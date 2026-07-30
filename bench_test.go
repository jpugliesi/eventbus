package eventbus_test

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpugliesi/eventbus"
	"github.com/stretchr/testify/require"
)

// benchPool creates a fresh isolated database on the shared container and returns
// a superuser pool over it. It mirrors the test harness but works with
// *testing.B (the harness helpers are *testing.T-bound).
func benchPool(b *testing.B) *pgxpool.Pool {
	b.Helper()
	ctx := context.Background()

	adminDSN, err := testContainer.ConnectionString(ctx, "sslmode=disable", "dbname=postgres")
	require.NoError(b, err)
	admin, err := pgx.Connect(ctx, adminDSN)
	require.NoError(b, err)
	var raw [8]byte
	_, _ = crand.Read(raw[:])
	dbName := "bench_" + hex.EncodeToString(raw[:])
	_, err = admin.Exec(ctx, "CREATE DATABASE "+dbName)
	require.NoError(b, err)
	require.NoError(b, admin.Close(ctx))

	endpoint, err := testContainer.PortEndpoint(ctx, "5432/tcp", "")
	require.NoError(b, err)
	cfg, err := pgxpool.ParseConfig(fmt.Sprintf("postgres://test:test@%s/%s?sslmode=disable", endpoint, dbName))
	require.NoError(b, err)
	cfg.MaxConns = 64 // don't let the default (~NumCPU) cap concurrency in benchmarks
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(b, err)
	b.Cleanup(func() {
		pool.Close()
		_ = testContainer.DropDatabase(dbName)
	})
	return pool
}

func silentLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// benchBacklog is the number of deliveries drained per benchmark iteration.
const benchBacklog = 2000

// seedBacklog inserts benchBacklog pending deliveries for one consumer. Keyed
// spreads them one-per-key across distinct partition keys (the ordered path);
// unkeyed leaves partition_key NULL (the parallel path). ANALYZE keeps plans
// honest.
func seedBacklog(tb testing.TB, pool *pgxpool.Pool, consumer string, keyed bool) {
	tb.Helper()
	ctx := context.Background()
	if keyed {
		_, err := pool.Exec(ctx, `
			INSERT INTO eventbus.partitions (namespace, consumer, partition_key)
			SELECT '', $1, 'k' || lpad(g::text, 6, '0')
			FROM generate_series(0, $2 - 1) g ON CONFLICT DO NOTHING`, consumer, benchBacklog)
		require.NoError(tb, err)
		_, err = pool.Exec(ctx, `
			INSERT INTO eventbus.deliveries (namespace, consumer, topic, partition_key, payload)
			SELECT '', $1, 't', 'k' || lpad(g::text, 6, '0'), '{}'::jsonb
			FROM generate_series(0, $2 - 1) g`, consumer, benchBacklog)
		require.NoError(tb, err)
	} else {
		_, err := pool.Exec(ctx, `
			INSERT INTO eventbus.deliveries (namespace, consumer, topic, payload)
			SELECT '', $1, 't', '{}'::jsonb FROM generate_series(1, $2) g`, consumer, benchBacklog)
		require.NoError(tb, err)
	}
	_, err := pool.Exec(ctx, `ANALYZE eventbus.deliveries, eventbus.partitions`)
	require.NoError(tb, err)
}

// BenchmarkDrain measures end-to-end drain throughput (deliveries/s) for the
// keyed (ordered) and unkeyed (parallel) paths at two concurrency levels. This is
// the primary performance metric the framework optimizes; the reported ns/op is
// the wall time to fully drain benchBacklog deliveries.
func BenchmarkDrain(b *testing.B) {
	scenarios := []struct {
		name        string
		keyed       bool
		concurrency int
	}{
		{"unkeyed/c8", false, 8},
		{"unkeyed/c32", false, 32},
		{"keyed/c8", true, 8},
		{"keyed/c32", true, 32},
	}
	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			pool := benchPool(b)
			ctx := context.Background()
			c := eventbus.NewClient(pool, eventbus.WithLogger(silentLogger()))
			require.NoError(b, c.EnsureSchema(ctx))
			c.Register(eventbus.Consumer{
				Name: "worker", Topic: "t", Concurrency: sc.concurrency,
				Handler: func(context.Context, eventbus.Delivery) error { return nil },
			})
			require.NoError(b, c.RegisterSubscriptions(ctx))

			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				seedBacklog(b, pool, "worker", sc.keyed)
				b.StartTimer()
				require.NoError(b, c.RunOnce(ctx))
			}
			b.ReportMetric(float64(benchBacklog)*float64(b.N)/b.Elapsed().Seconds(), "deliveries/s")
		})
	}
}

// BenchmarkClaimBatchSize sweeps the claim batch size to find the throughput knee
// (informs the WithClaimBatchSize default).
func BenchmarkClaimBatchSize(b *testing.B) {
	for _, bs := range []int{10, 50, 100, 250, 500, 1000} {
		b.Run(fmt.Sprintf("batch%d", bs), func(b *testing.B) {
			pool := benchPool(b)
			ctx := context.Background()
			c := eventbus.NewClient(pool, eventbus.WithLogger(silentLogger()), eventbus.WithClaimBatchSize(bs))
			require.NoError(b, c.EnsureSchema(ctx))
			c.Register(eventbus.Consumer{
				Name: "worker", Topic: "t", Concurrency: 16,
				Handler: func(context.Context, eventbus.Delivery) error { return nil },
			})
			require.NoError(b, c.RegisterSubscriptions(ctx))
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				seedBacklog(b, pool, "worker", false)
				b.StartTimer()
				require.NoError(b, c.RunOnce(ctx))
			}
			b.ReportMetric(float64(benchBacklog)*float64(b.N)/b.Elapsed().Seconds(), "deliveries/s")
		})
	}
}

// TestKeyedClaimUsesIndex runs EXPLAIN ANALYZE on the per-partition probe the
// keyed claim relies on, logs the plan, and asserts it is served by an index
// scan rather than a sequential scan of deliveries.
func TestKeyedClaimUsesIndex(t *testing.T) {
	t.Parallel()
	_, pool := newClient(t)
	ctx := t.Context()

	_, err := pool.Exec(ctx, `
		INSERT INTO eventbus.deliveries (namespace, consumer, topic, partition_key, payload)
		SELECT '', 'w', 't', 'k' || lpad(g::text, 6, '0'), '{}'::jsonb
		FROM generate_series(0, 3000) g`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ANALYZE eventbus.deliveries`)
	require.NoError(t, err)

	plan := explain(t, pool, `
		SELECT d.id FROM eventbus.deliveries d
		WHERE d.namespace = '' AND d.consumer = 'w' AND d.partition_key = 'k001500'
			AND d.state = 'pending' AND d.run_after <= now()
		ORDER BY d.id LIMIT 1`)
	t.Logf("keyed-claim probe plan:\n%s", plan)
	require.Contains(t, plan, "deliveries_claim", "keyed-claim probe must use the deliveries_claim index")
	require.NotContains(t, plan, "Seq Scan on deliveries", "keyed-claim probe must not seq-scan deliveries")
	require.NotContains(t, plan, "Rows Removed by Filter: 3000", "keyed-claim probe must not scan-and-filter the whole backlog")
}

// explain returns the EXPLAIN (ANALYZE, BUFFERS) plan text for a query.
func explain(t *testing.T, pool *pgxpool.Pool, query string) string {
	t.Helper()
	rows, err := pool.Query(t.Context(), "EXPLAIN (ANALYZE, BUFFERS) "+query)
	require.NoError(t, err)
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	require.NoError(t, rows.Err())
	return sb.String()
}
