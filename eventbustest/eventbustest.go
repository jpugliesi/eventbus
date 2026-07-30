// Package eventbustest provides test doubles and helpers for exercising
// eventbus-backed code: a RecordingPublisher for publish-side unit tests, and
// pool-backed helpers (client construction, drain, delivery/subscription
// inspection) for integration tests. Bring your own Postgres pool — e.g. from
// internal/postgrestest — so this package stays free of container/Docker deps.
package eventbustest

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

// RecordingPublisher is an eventbus.Publisher that records events in memory
// instead of writing to a database — ideal for unit-testing publish-side code
// without a Postgres dependency. Safe for concurrent use.
type RecordingPublisher struct {
	// Err, when set, is returned by Publish/PublishBatch so error paths can be
	// exercised; events are not recorded when it fires.
	Err error

	mu     sync.Mutex
	events []eventbus.Event
}

// Publish records ev (or returns p.Err).
func (p *RecordingPublisher) Publish(_ context.Context, ev eventbus.Event, _ ...eventbus.PublishOption) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Err != nil {
		return p.Err
	}
	p.events = append(p.events, ev)
	return nil
}

// PublishBatch records evs (or returns p.Err).
func (p *RecordingPublisher) PublishBatch(_ context.Context, evs []eventbus.Event, _ ...eventbus.PublishOption) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Err != nil {
		return p.Err
	}
	p.events = append(p.events, evs...)
	return nil
}

// Events returns a copy of everything published so far, in order.
func (p *RecordingPublisher) Events() []eventbus.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]eventbus.Event(nil), p.events...)
}

var _ eventbus.Publisher = (*RecordingPublisher)(nil)

// MustClient builds an eventbus.Client over pool with test-friendly defaults —
// zero retry backoff (so a drain converges in one RunOnce) and a discard logger —
// ensures the schema, and fails the test on error. Extra options override the
// defaults.
func MustClient(tb testing.TB, pool *pgxpool.Pool, opts ...eventbus.Option) *eventbus.Client {
	tb.Helper()
	base := []eventbus.Option{
		eventbus.WithBackoff(func(int) time.Duration { return 0 }),
		eventbus.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}
	c := eventbus.NewClient(pool, append(base, opts...)...)
	require.NoError(tb, c.EnsureSchema(context.Background()))
	return c
}

// Drain drains the named consumers to exhaustion in one pass (RunOnce), failing
// the test on error. With no names it drains every registered consumer.
func Drain(tb testing.TB, c *eventbus.Client, names ...string) {
	tb.Helper()
	require.NoError(tb, c.RunOnce(context.Background(), names...))
}

// CountDeliveries returns the number of delivery rows for a consumer queue.
func CountDeliveries(tb testing.TB, pool *pgxpool.Pool, consumer string) int {
	tb.Helper()
	return count(tb, pool, `SELECT count(*) FROM eventbus.deliveries WHERE consumer = $1`, consumer)
}

// CountState returns the number of delivery rows for a consumer in a given state
// (e.g. "pending", "active", "failed").
func CountState(tb testing.TB, pool *pgxpool.Pool, consumer, state string) int {
	tb.Helper()
	return count(tb, pool, `SELECT count(*) FROM eventbus.deliveries WHERE consumer = $1 AND state = $2`, consumer, state)
}

// CountSubscriptions returns how many subscription rows exist for a topic.
func CountSubscriptions(tb testing.TB, pool *pgxpool.Pool, topic string) int {
	tb.Helper()
	return count(tb, pool, `SELECT count(*) FROM eventbus.subscriptions WHERE topic = $1`, topic)
}

func count(tb testing.TB, pool *pgxpool.Pool, query string, args ...any) int {
	tb.Helper()
	var n int
	require.NoError(tb, pool.QueryRow(context.Background(), query, args...).Scan(&n))
	return n
}
