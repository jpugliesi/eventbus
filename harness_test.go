package eventbus_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpugliesi/eventbus"
	"github.com/stretchr/testify/require"
)

// newPool creates a fresh, isolated database (no protodb migrations) and returns
// a superuser pool over it. The pool can run DDL and all eventbus operations.
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	db := testContainer.StartTestDatabaseWithoutMigrations(t)
	pool, err := pgxpool.New(t.Context(), db.AdminConnectionURL())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// testOptions are the knobs every test wants: zero backoff (retries are
// immediately re-claimable, so a drain converges in one RunOnce) and a silent
// logger.
func testOptions(extra ...eventbus.Option) []eventbus.Option {
	base := []eventbus.Option{
		eventbus.WithBackoff(func(int) time.Duration { return 0 }),
		eventbus.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}
	return append(base, extra...)
}

// newClient builds a client over a fresh isolated pool and ensures the schema.
func newClient(t *testing.T, opts ...eventbus.Option) (*eventbus.Client, *pgxpool.Pool) {
	t.Helper()
	pool := newPool(t)
	client := eventbus.NewClient(pool, testOptions(opts...)...)
	require.NoError(t, client.EnsureSchema(t.Context()))
	return client, pool
}

// newClientOn builds an additional client over an existing pool (e.g. a separate
// publisher process sharing the same database).
func newClientOn(t *testing.T, pool *pgxpool.Pool, opts ...eventbus.Option) *eventbus.Client {
	t.Helper()
	return eventbus.NewClient(pool, testOptions(opts...)...)
}

// recorder is an observing handler factory: it captures every delivery (at the
// moment the handler is entered, so per-key order reflects processing order) and
// tracks the peak number of concurrent in-flight handlers.
type recorder struct {
	mu       sync.Mutex
	received []eventbus.Delivery
	inFlight int
	peak     int
}

// handle wraps fn (the per-delivery decision; nil means succeed) with recording
// and concurrency tracking, returning an eventbus.Handler.
func (r *recorder) handle(fn func(d eventbus.Delivery) error) eventbus.Handler {
	return func(ctx context.Context, d eventbus.Delivery) error {
		r.mu.Lock()
		r.inFlight++
		if r.inFlight > r.peak {
			r.peak = r.inFlight
		}
		r.received = append(r.received, d)
		r.mu.Unlock()
		defer func() {
			r.mu.Lock()
			r.inFlight--
			r.mu.Unlock()
		}()
		if fn != nil {
			return fn(d)
		}
		return nil
	}
}

// count returns how many times the recorder's handler was entered.
func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.received)
}

// keysInOrder returns the payload-or-type sequence the recorder saw for a given
// partition key, in processing order.
func (r *recorder) typesForKey(key string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, d := range r.received {
		if d.PartitionKey == key {
			out = append(out, d.Type)
		}
	}
	return out
}

// peakInFlight returns the maximum observed concurrent handler count.
func (r *recorder) peakInFlight() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peak
}

// --- direct-SQL assertion helpers (read internal table state) ---

// countDeliveries returns the number of delivery rows for a consumer queue.
func countDeliveries(t *testing.T, pool *pgxpool.Pool, consumer string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(t.Context(),
		`SELECT count(*) FROM eventbus.deliveries WHERE consumer = $1`, consumer).Scan(&n)
	require.NoError(t, err)
	return n
}

// countState returns the number of delivery rows for a consumer in a given state.
func countState(t *testing.T, pool *pgxpool.Pool, consumer, state string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(t.Context(),
		`SELECT count(*) FROM eventbus.deliveries WHERE consumer = $1 AND state = $2`,
		consumer, state).Scan(&n)
	require.NoError(t, err)
	return n
}

// countSubscriptions returns how many subscription rows exist for a topic.
func countSubscriptions(t *testing.T, pool *pgxpool.Pool, topic string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(t.Context(),
		`SELECT count(*) FROM eventbus.subscriptions WHERE topic = $1`, topic).Scan(&n)
	require.NoError(t, err)
	return n
}

// partitionExists reports whether a partition row exists for a consumer/key.
func partitionExists(t *testing.T, pool *pgxpool.Pool, consumer, key string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM eventbus.partitions WHERE consumer = $1 AND partition_key = $2)`,
		consumer, key).Scan(&exists)
	require.NoError(t, err)
	return exists
}

// drain runs RunOnce and fails the test on error.
func drain(t *testing.T, c *eventbus.Client, names ...string) {
	t.Helper()
	require.NoError(t, c.RunOnce(t.Context(), names...))
}
