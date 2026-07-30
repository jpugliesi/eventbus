package eventbus_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jpugliesi/eventbus"
	"github.com/stretchr/testify/require"
)

// countObserver records metric counts for assertions.
type countObserver struct {
	mu     sync.Mutex
	counts map[string]int
}

func newObserver() *countObserver { return &countObserver{counts: map[string]int{}} }

func (o *countObserver) Count(_ context.Context, metric string, _ ...eventbus.Attr) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counts[metric]++
}

func (o *countObserver) get(metric string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.counts[metric]
}

func consumer(name, topic string, h eventbus.Handler) eventbus.Consumer {
	return eventbus.Consumer{Name: name, Topic: topic, Handler: h}
}

// Case 1: publishing to a topic nobody subscribes to drops the event and counts
// it; no delivery rows are created.
func TestPublishNoSubscriber(t *testing.T) {
	t.Parallel()
	obs := newObserver()
	c, pool := newClient(t, eventbus.WithObserver(obs))

	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "orphan", Type: "x"}))

	require.Equal(t, 0, countDeliveries(t, pool, "anyone"))
	var total int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM eventbus.deliveries`).Scan(&total))
	require.Equal(t, 0, total)
	require.Equal(t, 1, obs.get("published_to_empty_topic"))
}

// Case 1b: WithRequireSubscriber turns the drop into an error.
func TestPublishRequireSubscriber(t *testing.T) {
	t.Parallel()
	c, _ := newClient(t, eventbus.WithRequireSubscriber())
	err := c.Publish(t.Context(), eventbus.Event{Topic: "orphan", Type: "x"})
	require.ErrorIs(t, err, eventbus.ErrNoSubscriber)
}

// Case 2: one publisher, one topic, one consumer — exactly one delivery, handled
// once, row deleted on success.
func TestSingleConsumer(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(consumer("worker", "events", rec.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "hello", Payload: []byte(`{"k":"v"}`)}))
	require.Equal(t, 1, countDeliveries(t, pool, "worker"))

	drain(t, c)
	require.Equal(t, 1, rec.count())
	require.Equal(t, "hello", rec.received[0].Type)
	require.JSONEq(t, `{"k":"v"}`, string(rec.received[0].Payload))
	require.Equal(t, 1, rec.received[0].Attempts)
	require.Equal(t, 0, countDeliveries(t, pool, "worker")) // deleted on completion
}

// Case 3: one topic, two consumers — fan-out copies the event to each; a failure
// in one does not affect the other.
func TestFanOutTwoConsumers(t *testing.T) {
	t.Parallel()
	a, b := &recorder{}, &recorder{}
	c, pool := newClient(t)
	c.Register(consumer("a", "events", a.handle(nil)))
	c.Register(eventbus.Consumer{
		Name: "b", Topic: "events", MaxAttempts: 2,
		Handler: b.handle(func(eventbus.Delivery) error { return fmt.Errorf("b always fails") }),
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "hi"}))
	require.Equal(t, 1, countDeliveries(t, pool, "a"))
	require.Equal(t, 1, countDeliveries(t, pool, "b"))

	drain(t, c)
	require.Equal(t, 1, a.count()) // a handled its copy once
	require.Equal(t, 0, countDeliveries(t, pool, "a"))
	require.GreaterOrEqual(t, b.count(), 1)            // b retried independently
	require.Equal(t, 0, countDeliveries(t, pool, "a")) // a unaffected by b's failure
}

// Case 4: two publishers, two topics, one consumer each — no cross-delivery.
func TestTwoTopicsOneConsumerEach(t *testing.T) {
	t.Parallel()
	r1, r2 := &recorder{}, &recorder{}
	c, pool := newClient(t)
	c.Register(consumer("c1", "topic1", r1.handle(nil)))
	c.Register(consumer("c2", "topic2", r2.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "topic1", Type: "a"}))
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "topic2", Type: "b"}))

	drain(t, c)
	require.Equal(t, 1, r1.count())
	require.Equal(t, 1, r2.count())
	require.Equal(t, "a", r1.received[0].Type)
	require.Equal(t, "b", r2.received[0].Type)
	require.Equal(t, 0, countDeliveries(t, pool, "c1"))
	require.Equal(t, 0, countDeliveries(t, pool, "c2"))
}

// Case 5: two topics, two consumers each — 2x2 fan-out, each consumer drains only
// its topic.
func TestTwoTopicsTwoConsumersEach(t *testing.T) {
	t.Parallel()
	recs := map[string]*recorder{"a1": {}, "a2": {}, "b1": {}, "b2": {}}
	c, _ := newClient(t)
	c.Register(consumer("a1", "A", recs["a1"].handle(nil)))
	c.Register(consumer("a2", "A", recs["a2"].handle(nil)))
	c.Register(consumer("b1", "B", recs["b1"].handle(nil)))
	c.Register(consumer("b2", "B", recs["b2"].handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "A", Type: "a"}))
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "B", Type: "b"}))

	drain(t, c)
	require.Equal(t, 1, recs["a1"].count())
	require.Equal(t, 1, recs["a2"].count())
	require.Equal(t, 1, recs["b1"].count())
	require.Equal(t, 1, recs["b2"].count())
	require.Equal(t, "a", recs["a1"].received[0].Type)
	require.Equal(t, "b", recs["b1"].received[0].Type)
}

// Case 6: one consumer with concurrency and many replicas of work — each delivery
// is processed exactly once (SKIP LOCKED splits the work, no double-processing).
func TestCompetingConsumersExactlyOnce(t *testing.T) {
	t.Parallel()
	const n = 50
	var processed int64
	seen := &sync.Map{}
	c, pool := newClient(t)
	c.Register(eventbus.Consumer{
		Name: "worker", Topic: "events", Concurrency: 8,
		Handler: func(_ context.Context, d eventbus.Delivery) error {
			_, dup := seen.LoadOrStore(d.ID, true)
			require.False(t, dup, "delivery %d processed twice", d.ID)
			atomic.AddInt64(&processed, 1)
			return nil
		},
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))

	for i := range n {
		require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: fmt.Sprintf("e%d", i)}))
	}
	drain(t, c)
	require.Equal(t, int64(n), atomic.LoadInt64(&processed))
	require.Equal(t, 0, countDeliveries(t, pool, "worker"))
}

// Batched claiming + the fetcher's drain-confirmation must still deliver every
// message exactly once across a mix of keyed (multiple per key, requeued in order)
// and unkeyed work, even with a small batch size that exercises many claim rounds.
func TestBatchDrainMixedExactlyOnce(t *testing.T) {
	t.Parallel()
	const n = 300
	var processed int64
	seen := &sync.Map{}
	c, pool := newClient(t, eventbus.WithClaimBatchSize(7))
	c.Register(eventbus.Consumer{
		Name: "worker", Topic: "events", Concurrency: 6,
		Handler: func(_ context.Context, d eventbus.Delivery) error {
			_, dup := seen.LoadOrStore(d.ID, true)
			require.False(t, dup, "delivery %d processed twice", d.ID)
			atomic.AddInt64(&processed, 1)
			return nil
		},
	})
	require.NoError(t, c.RegisterSubscriptions(t.Context()))
	for i := range n {
		ev := eventbus.Event{Topic: "events", Type: fmt.Sprintf("e%d", i)}
		if i%2 == 0 {
			ev.PartitionKey = fmt.Sprintf("k%d", i%20) // half keyed across 20 keys (several per key)
		}
		require.NoError(t, c.Publish(t.Context(), ev))
	}
	drain(t, c)
	require.Equal(t, int64(n), atomic.LoadInt64(&processed))
	require.Equal(t, 0, countDeliveries(t, pool, "worker"))
}
