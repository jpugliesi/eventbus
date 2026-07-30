package eventbus_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jpugliesi/eventbus"
	"github.com/stretchr/testify/require"
)

// Deliveries that share a partition key are processed strictly in publish order
// within a consumer, even with many concurrent workers (the partition lock
// serializes the key).
func TestPartitionKeyOrdering(t *testing.T) {
	t.Parallel()
	const n = 12
	rec := &recorder{}
	c, _ := newClient(t)
	c.Register(eventbus.Consumer{
		Name: "worker", Topic: "events", Concurrency: 8,
		// A small delay would expose any accidental parallelism as reordering.
		Handler: rec.handle(func(eventbus.Delivery) error {
			time.Sleep(2 * time.Millisecond)
			return nil
		}),
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	want := make([]string, n)
	for i := range n {
		typ := fmt.Sprintf("e%02d", i)
		want[i] = typ
		require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: typ, PartitionKey: "K"}))
	}

	drain(t, c)
	require.Equal(t, want, rec.typesForKey("K"), "same-key deliveries must be handled in publish order")
	require.Equal(t, 1, rec.peakInFlight(), "same-key deliveries must never run concurrently, even at Concurrency=8")
}

// Deliveries with distinct partition keys (and unkeyed deliveries) run in
// parallel up to the consumer's concurrency.
func TestDistinctKeysRunInParallel(t *testing.T) {
	t.Parallel()
	const n = 6
	rec := &recorder{}
	release := make(chan struct{})
	c, _ := newClient(t)
	c.Register(eventbus.Consumer{
		Name: "worker", Topic: "events", Concurrency: n,
		Handler: rec.handle(func(eventbus.Delivery) error {
			// Hold until released so several handlers are provably in flight together.
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
			return nil
		}),
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	for i := range n {
		require.NoError(t, c.Publish(t.Context(), eventbus.Event{
			Topic: "events", Type: fmt.Sprintf("e%d", i), PartitionKey: fmt.Sprintf("k%d", i),
		}))
	}

	// Release the handlers shortly after the drain starts holding them.
	go func() {
		time.Sleep(200 * time.Millisecond)
		close(release)
	}()
	drain(t, c)
	require.Greater(t, rec.peakInFlight(), 1, "distinct keys should run concurrently")
}

// A consumer never runs more than Concurrency handlers at once.
func TestConcurrencyCap(t *testing.T) {
	t.Parallel()
	const cap = 3
	rec := &recorder{}
	c, _ := newClient(t)
	c.Register(eventbus.Consumer{
		Name: "worker", Topic: "events", Concurrency: cap,
		Handler: rec.handle(func(eventbus.Delivery) error {
			time.Sleep(20 * time.Millisecond)
			return nil
		}),
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	for i := range 15 {
		require.NoError(t, c.Publish(t.Context(), eventbus.Event{
			Topic: "events", Type: fmt.Sprintf("e%d", i), PartitionKey: fmt.Sprintf("k%d", i),
		}))
	}
	drain(t, c)
	require.LessOrEqual(t, rec.peakInFlight(), cap, "must not exceed configured concurrency")
	require.Equal(t, 15, rec.count())
}

// Ordering is per-(consumer, key) and independent across consumers: a slow,
// failing consumer does not perturb another consumer's ordering for the same key.
func TestPerConsumerKeyIndependence(t *testing.T) {
	t.Parallel()
	const n = 8
	fast := &recorder{}
	c, pool := newClient(t)
	c.Register(eventbus.Consumer{
		Name: "fast", Topic: "events", Concurrency: 4,
		Handler: fast.handle(nil),
	})
	// "slow" fails the first attempt of every delivery, forcing retries that
	// interleave with "fast"'s processing.
	tries := map[int64]int{}
	c.Register(eventbus.Consumer{
		Name: "slow", Topic: "events", Concurrency: 4, MaxAttempts: 5,
		Handler: func(_ context.Context, d eventbus.Delivery) error {
			tries[d.ID]++
			if tries[d.ID] < 2 {
				return fmt.Errorf("transient")
			}
			return nil
		},
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	want := make([]string, n)
	for i := range n {
		typ := fmt.Sprintf("e%02d", i)
		want[i] = typ
		require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: typ, PartitionKey: "K"}))
	}

	drain(t, c)
	require.Equal(t, want, fast.typesForKey("K"), "fast consumer keeps publish order despite slow consumer's retries")
	require.Equal(t, 0, countDeliveries(t, pool, "fast"))
	require.Equal(t, 0, countDeliveries(t, pool, "slow"))
}

// A worker that crashes mid-delivery on the head of a key must not let later
// deliveries jump ahead of the stuck one once the lock expires: the key stays
// blocked until the janitor rescues the head, then drains in publish order.
func TestCrashedKeyedDeliveryPreservesOrder(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t, eventbus.WithLeaseTimeout(time.Minute))
	c.Register(eventbus.Consumer{Name: "worker", Topic: "events", Concurrency: 4, Handler: rec.handle(nil)})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))
	for _, ty := range []string{"e0", "e1", "e2"} {
		require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: ty, PartitionKey: "K"}))
	}

	// Simulate a crashed worker mid-flight on the head delivery: the lowest-id
	// delivery is stuck 'active' with an expired lock, and its partition lock is
	// expired too.
	_, err := pool.Exec(t.Context(), `
		UPDATE eventbus.deliveries SET state = 'active', locked_at = now() - interval '1 hour'
		WHERE consumer = 'worker' AND id = (SELECT min(id) FROM eventbus.deliveries WHERE consumer = 'worker')`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `
		UPDATE eventbus.partitions SET locked_at = now() - interval '1 hour' WHERE consumer = 'worker' AND partition_key = 'K'`)
	require.NoError(t, err)

	// Even though the partition lock has expired, the stuck head blocks the key.
	drain(t, c)
	require.Equal(t, 0, rec.count(), "later deliveries must not jump the stuck head")

	// The janitor rescues the head, then the whole key drains in publish order.
	stats, err := c.Janitor(t.Context())
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats.Rescued, int64(1))

	drain(t, c)
	require.Equal(t, []string{"e0", "e1", "e2"}, rec.typesForKey("K"))
}

// A keyed delivery whose partition row is missing (as could briefly happen if a
// reap raced a publish) is invisible to claimKeyed — but it is not lost, and a
// later publish to the same key re-seeds the partition and recovers it in order.
// This is the self-healing property the partition-reap safety guard relies on.
func TestStrandedKeyedDeliveryRecoveredByRepublish(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(consumer("worker", "events", rec.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "e0", PartitionKey: "K"}))
	_, err := pool.Exec(t.Context(), `DELETE FROM eventbus.partitions WHERE consumer = 'worker' AND partition_key = 'K'`)
	require.NoError(t, err)

	drain(t, c)
	require.Equal(t, 0, rec.count(), "a keyed delivery with no partition row is not claimable")
	require.Equal(t, 1, countDeliveries(t, pool, "worker"), "but it is not lost")

	// A later publish to the same key re-seeds the partition; both drain in order.
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "e1", PartitionKey: "K"}))
	drain(t, c)
	require.Equal(t, []string{"e0", "e1"}, rec.typesForKey("K"))
}
