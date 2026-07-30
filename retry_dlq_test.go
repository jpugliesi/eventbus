package eventbus_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpugliesi/eventbus"
	"github.com/stretchr/testify/require"
)

// A handler that fails a few times then succeeds is retried (zero backoff makes
// each retry immediately re-claimable) and ultimately completes.
func TestRetryThenSucceed(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(consumer("worker", "events", rec.handle(func(d eventbus.Delivery) error {
		if d.Attempts < 3 {
			return context.DeadlineExceeded // any error
		}
		return nil
	})))
	require.NoError(t, c.RegisterSubscriptions(t.Context()))
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "x"}))

	drain(t, c)
	require.Equal(t, 3, rec.count(), "handler retried until it succeeded on attempt 3")
	require.Equal(t, 0, countDeliveries(t, pool, "worker"), "completed delivery is deleted")
}

// A retriable failure reschedules the delivery into the future by the backoff
// delay; with a long backoff it is not re-claimed in the same drain pass.
func TestRetryBackoffAdvancesRunAfter(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t, eventbus.WithBackoff(func(int) time.Duration { return time.Hour }))
	c.Register(consumer("worker", "events", rec.handle(func(eventbus.Delivery) error {
		return context.DeadlineExceeded
	})))
	require.NoError(t, c.RegisterSubscriptions(t.Context()))
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "x"}))

	drain(t, c)
	require.Equal(t, 1, rec.count(), "long backoff means only one attempt this pass")

	state, attempts, runAfterFuture := deliveryState(t, pool, "worker")
	require.Equal(t, "pending", state)
	require.Equal(t, 1, attempts)
	require.True(t, runAfterFuture, "run_after should be scheduled into the future")
}

// When a delivery exhausts its attempts and a dead-letter queue is configured,
// it is copied there and removed from its own queue.
func TestDeadLetterRouting(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(eventbus.Consumer{
		Name: "worker", Topic: "events", MaxAttempts: 2, DeadLetter: "dead",
		Handler: rec.handle(func(eventbus.Delivery) error { return context.DeadlineExceeded }),
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "x", Payload: []byte(`{"a":1}`)}))

	drain(t, c)
	require.Equal(t, 2, rec.count(), "tried up to MaxAttempts")
	require.Equal(t, 0, countDeliveries(t, pool, "worker"), "exhausted delivery left its own queue")
	require.Equal(t, 1, countDeliveries(t, pool, "dead"), "copied to the dead-letter queue")
	require.Equal(t, 1, countState(t, pool, "dead", "pending"), "DLQ copy is a fresh pending delivery")
}

// A keyed delivery that dead-letters stays claimable in the DLQ: its partition
// row is seeded for the dead-letter consumer, so a worker draining the DLQ can
// process it. Regression for the keyed-DLQ-unclaimable bug.
func TestDeadLetterKeyedIsClaimable(t *testing.T) {
	t.Parallel()
	primary, dlq := &recorder{}, &recorder{}
	c, pool := newClient(t)
	c.Register(eventbus.Consumer{
		Name: "worker", Topic: "events", MaxAttempts: 1, DeadLetter: "dead",
		Handler: primary.handle(func(eventbus.Delivery) error { return context.DeadlineExceeded }),
	})
	c.Register(consumer("dead", "dead", dlq.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(t.Context()))
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "x", PartitionKey: "k", Payload: []byte(`{"a":1}`)}))

	drain(t, c, "worker") // exhaust the keyed delivery -> copy to the DLQ
	require.Equal(t, 1, primary.count())
	drain(t, c, "dead") // the keyed copy must be claimable
	require.Equal(t, 1, dlq.count(), "keyed DLQ copy was claimed and processed")
	require.Equal(t, "k", dlq.received[0].PartitionKey)
	require.Equal(t, 0, countDeliveries(t, pool, "dead"), "DLQ copy completed and deleted")
}

// With no dead-letter queue, an exhausted delivery is simply dropped (deleted),
// not retained or retried further.
func TestNoDeadLetterDrops(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(eventbus.Consumer{
		Name: "worker", Topic: "events", MaxAttempts: 2,
		Handler: rec.handle(func(eventbus.Delivery) error { return context.DeadlineExceeded }),
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "x"}))

	drain(t, c)
	require.Equal(t, 2, rec.count(), "tried up to MaxAttempts")
	require.Equal(t, 0, countDeliveries(t, pool, "worker"), "exhausted delivery is dropped, not retained")
	// Nothing left to claim: a second drain does nothing.
	drain(t, c)
	require.Equal(t, 2, rec.count())
}

// A panicking handler is recovered (it does not crash the worker) and treated as
// a failed attempt.
func TestHandlerPanicRecovered(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(eventbus.Consumer{
		Name: "worker", Topic: "events", MaxAttempts: 1,
		Handler: rec.handle(func(eventbus.Delivery) error { panic("boom") }),
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "x"}))

	require.NoError(t, c.RunOnce(t.Context()), "a panicking handler must not fail the drain")
	require.Equal(t, 1, rec.count())
	require.Equal(t, 0, countDeliveries(t, pool, "worker"), "exhausted (panicked) delivery is dropped")
}

// At-least-once safety: a delivery a worker claimed and then died on (left
// 'active') is not lost — the janitor rescues it to pending and it is reprocessed.
func TestCrashedDeliveryRescuedAndReprocessed(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t, eventbus.WithLeaseTimeout(time.Minute))
	c.Register(consumer("worker", "events", rec.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	// Simulate a crashed worker: an active delivery with a stale lock.
	_, err := pool.Exec(t.Context(), `
		INSERT INTO eventbus.deliveries (consumer, topic, type, state, locked_by, locked_at, attempts)
		VALUES ('worker', 'events', 'x', 'active', 'dead', now() - interval '1 hour', 1)`)
	require.NoError(t, err)

	// It is not claimable while active...
	drain(t, c)
	require.Equal(t, 0, rec.count())

	// ...the janitor rescues it...
	stats, err := c.Janitor(t.Context())
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats.Rescued, int64(1))

	// ...and the next drain processes it (reprocessed after the crash).
	drain(t, c)
	require.Equal(t, 1, rec.count())
	require.Equal(t, 0, countDeliveries(t, pool, "worker"))
}

// deliveryState reads the single delivery for a consumer: its state, attempts,
// and whether run_after is in the future.
func deliveryState(t *testing.T, pool *pgxpool.Pool, consumer string) (state string, attempts int, runAfterFuture bool) {
	t.Helper()
	err := pool.QueryRow(t.Context(), `
		SELECT state, attempts, run_after > now()
		FROM eventbus.deliveries WHERE consumer = $1
	`, consumer).Scan(&state, &attempts, &runAfterFuture)
	require.NoError(t, err)
	return state, attempts, runAfterFuture
}
