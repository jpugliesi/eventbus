package eventbus_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpugliesi/eventbus"
	"github.com/stretchr/testify/require"
)

// TestCrossProcessFanOut proves the subscriptions table is the cross-process
// source of truth for routing: a publisher that imports no consumer code fans
// out purely by reading the DB, then the consumer process drains its copy.
func TestCrossProcessFanOut(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Consumer process: declares and registers the "reco" subscription.
	rec := &recorder{}
	cConsumer, pool := newClient(t)
	cConsumer.Register(consumer("reco", "knowledge", rec.handle(nil)))
	require.NoError(t, cConsumer.RegisterSubscriptions(ctx))

	// Publisher process: a separate client on the same database with NO consumers
	// registered. It can only route by reading eventbus.subscriptions.
	pub := newClientOn(t, pool)
	require.NoError(t, pub.Publish(ctx, eventbus.Event{Topic: "knowledge", Type: "note", Payload: []byte(`{"k":"v"}`)}))

	// The publisher fanned out to "reco" without importing its handler.
	require.Equal(t, 1, countDeliveries(t, pool, "reco"))
	require.Equal(t, 0, rec.count()) // nothing drained yet

	// The consumer process drains its copy exactly once.
	require.NoError(t, cConsumer.RunOnce(ctx))
	require.Equal(t, 1, rec.count())
	require.Equal(t, "note", rec.received[0].Type)
	require.JSONEq(t, `{"k":"v"}`, string(rec.received[0].Payload))
	require.Equal(t, 0, countDeliveries(t, pool, "reco")) // deleted on completion
}

// TestSelfRegistrationStampsTime checks that RegisterSubscriptions stamps a
// fresh last_registered_at and that re-registering upserts (advances the stamp)
// rather than duplicating the row.
func TestSelfRegistrationStampsTime(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	c, pool := newClient(t)
	c.Register(consumer("reco", "knowledge", (&recorder{}).handle(nil)))

	require.NoError(t, c.RegisterSubscriptions(ctx))
	first := lastRegisteredAt(t, pool, "knowledge", "reco")
	require.WithinDuration(t, time.Now(), first, 10*time.Second)
	require.Equal(t, 1, countSubscriptions(t, pool, "knowledge"))

	// Re-registering refreshes the timestamp via the idempotent upsert.
	require.NoError(t, c.RegisterSubscriptions(ctx))
	second := lastRegisteredAt(t, pool, "knowledge", "reco")
	require.False(t, second.Before(first), "last_registered_at should advance (>= first)")
	require.Equal(t, 1, countSubscriptions(t, pool, "knowledge")) // upserted, not duplicated
}

// TestPublishBeforeSubscribeDrops shows that a publish to a topic whose consumer
// has not yet registered its subscription creates no deliveries; once the
// subscription is registered, the same publish fans out.
func TestPublishBeforeSubscribeDrops(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	c, pool := newClient(t)
	c.Register(consumer("reco", "knowledge", (&recorder{}).handle(nil)))

	// Registered in-process, but the global subscriptions table is still empty.
	require.NoError(t, c.Publish(ctx, eventbus.Event{Topic: "knowledge", Type: "early"}))
	require.Equal(t, 0, countDeliveries(t, pool, "reco"))

	// After self-registration the routing exists and the publish fans out.
	require.NoError(t, c.RegisterSubscriptions(ctx))
	require.NoError(t, c.Publish(ctx, eventbus.Event{Topic: "knowledge", Type: "late"}))
	require.Equal(t, 1, countDeliveries(t, pool, "reco"))
}

// TestSubscriptionReaping verifies the janitor reaps a subscription whose owner
// stopped refreshing it (past the TTL) and purges the deliveries routed to that
// now-dead queue.
func TestSubscriptionReaping(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	c, pool := newClient(t, eventbus.WithSubscriptionTTL(time.Minute))
	c.Register(consumer("stale", "t", (&recorder{}).handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(ctx))

	// A delivery exists for the soon-to-be-reaped consumer.
	require.NoError(t, c.Publish(ctx, eventbus.Event{Topic: "t", Type: "x"}))
	require.Equal(t, 1, countDeliveries(t, pool, "stale"))
	require.Equal(t, 1, countSubscriptions(t, pool, "t"))

	// Backdate the registration well past the one-minute TTL.
	_, err := pool.Exec(ctx,
		`UPDATE eventbus.subscriptions SET last_registered_at = now() - interval '1 hour' WHERE consumer = $1`,
		"stale")
	require.NoError(t, err)

	stats, err := c.Janitor(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats.SubscriptionsGCd, int64(1))
	require.GreaterOrEqual(t, stats.OrphansDeleted, int64(1))
	require.Equal(t, 0, countSubscriptions(t, pool, "t"))
	require.Equal(t, 0, countDeliveries(t, pool, "stale"))
}

// TestConsumerTopicChangePurgesStale: when a consumer keeps its Name but moves to
// a new Topic (a redeploy), re-registering drops the stale (old-topic, consumer)
// subscription and purges the deliveries already routed there — so old-topic
// publishers stop fanning into the queue and the handler never sees old-topic
// events. Regression for the reused-consumer-name misroute.
func TestConsumerTopicChangePurgesStale(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// v1: consumer "w" subscribes to topic "a" and a delivery is routed to it.
	c1, pool := newClient(t)
	c1.Register(consumer("w", "a", (&recorder{}).handle(nil)))
	require.NoError(t, c1.RegisterSubscriptions(ctx))
	require.NoError(t, c1.Publish(ctx, eventbus.Event{Topic: "a", Type: "x"}))
	require.Equal(t, 1, countSubscriptions(t, pool, "a"))
	require.Equal(t, 1, countDeliveries(t, pool, "w"))

	// v2 (a redeploy): the same consumer name now subscribes to topic "b".
	c2 := newClientOn(t, pool)
	c2.Register(consumer("w", "b", (&recorder{}).handle(nil)))
	require.NoError(t, c2.RegisterSubscriptions(ctx))

	require.Equal(t, 0, countSubscriptions(t, pool, "a"), "stale (a, w) subscription removed")
	require.Equal(t, 1, countSubscriptions(t, pool, "b"))
	require.Equal(t, 0, countDeliveries(t, pool, "w"), "old topic-a delivery purged")

	// A publish to the old topic no longer routes to w; the new topic does.
	require.NoError(t, c2.Publish(ctx, eventbus.Event{Topic: "a", Type: "x"}))
	require.Equal(t, 0, countDeliveries(t, pool, "w"), "topic a no longer routes to w")
	require.NoError(t, c2.Publish(ctx, eventbus.Event{Topic: "b", Type: "x"}))
	require.Equal(t, 1, countDeliveries(t, pool, "w"), "topic b now routes to w")
}

// lastRegisteredAt reads the subscription stamp for a (topic, consumer) pair
// directly from the table (last_registered_at is TIMESTAMPTZ).
func lastRegisteredAt(t *testing.T, pool *pgxpool.Pool, topic, consumer string) time.Time {
	t.Helper()
	var ts time.Time
	err := pool.QueryRow(t.Context(),
		`SELECT last_registered_at FROM eventbus.subscriptions WHERE topic = $1 AND consumer = $2`,
		topic, consumer).Scan(&ts)
	require.NoError(t, err)
	return ts
}
