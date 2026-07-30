package eventbus_test

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jpugliesi/eventbus"
	"github.com/stretchr/testify/require"
)

// These tests exercise the transactional-outbox guarantee: a Publish issued with
// WithTransaction(tx) writes its delivery row on the caller's transaction, so the
// event and the caller's business write share one atomic fate — both commit or
// neither does. There is no separate "publish the event" step that could succeed
// while the business write rolls back (or vice versa), which is the dual-write bug
// the outbox pattern exists to eliminate.

// TestOutboxAtomicCommit: a business row and its event delivery, written on the
// same tx, both become durable when the caller commits.
func TestOutboxAtomicCommit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(consumer("worker", "events", rec.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(ctx))

	_, err := pool.Exec(ctx, `CREATE TABLE widgets (id text primary key)`)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `INSERT INTO widgets (id) VALUES ($1)`, "w1")
	require.NoError(t, err)
	require.NoError(t, c.Publish(ctx,
		eventbus.Event{Topic: "events", Type: "widget.created", Payload: []byte(`{"id":"w1"}`)},
		eventbus.WithTransaction(tx)))

	require.NoError(t, tx.Commit(ctx))

	// The business write landed...
	var widgets int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM widgets WHERE id = $1`, "w1").Scan(&widgets))
	require.Equal(t, 1, widgets)
	// ...and so did the delivery, atomically, on the same commit.
	require.Equal(t, 1, countDeliveries(t, pool, "worker"))
}

// TestOutboxAtomicRollback: rolling back the caller's tx discards BOTH the
// business row and the delivery. The event is never committed on its own, which
// proves there is no second, independently-committed write.
func TestOutboxAtomicRollback(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(consumer("worker", "events", rec.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(ctx))

	_, err := pool.Exec(ctx, `CREATE TABLE widgets (id text primary key)`)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `INSERT INTO widgets (id) VALUES ($1)`, "w1")
	require.NoError(t, err)
	require.NoError(t, c.Publish(ctx,
		eventbus.Event{Topic: "events", Type: "widget.created", Payload: []byte(`{"id":"w1"}`)},
		eventbus.WithTransaction(tx)))

	require.NoError(t, tx.Rollback(ctx))

	// Neither the business row...
	var widgets int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM widgets WHERE id = $1`, "w1").Scan(&widgets))
	require.Equal(t, 0, widgets)
	// ...nor the delivery survived — no dual-write, no orphaned event.
	require.Equal(t, 0, countDeliveries(t, pool, "worker"))
}

// TestPublishStandalonePersists: without WithTransaction, Publish opens and
// commits its own tx, so the delivery is durable even though the caller does
// nothing else.
func TestPublishStandalonePersists(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(consumer("worker", "events", rec.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(ctx))

	require.NoError(t, c.Publish(ctx, eventbus.Event{Topic: "events", Type: "ping"}))

	require.Equal(t, 1, countDeliveries(t, pool, "worker"))
}

// TestOutboxSeamAcceptsRawPgxTx: the WithTransaction seam is typed on the generic
// DBTX interface, not on any project-specific (e.g. protodb) transaction type. A
// plain pgx.Tx straight from pool.Begin satisfies DBTX and works end-to-end —
// proving the package is not coupled to protodb and accepts any pgx-shaped tx.
func TestOutboxSeamAcceptsRawPgxTx(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rec := &recorder{}
	c, pool := newClient(t)
	c.Register(consumer("worker", "events", rec.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(ctx))

	// Explicitly a raw pgx.Tx — the only thing WithTransaction asks for is DBTX.
	var rawTx pgx.Tx
	rawTx, err := pool.Begin(ctx)
	require.NoError(t, err)

	require.NoError(t, c.Publish(ctx,
		eventbus.Event{Topic: "events", Type: "ping"},
		eventbus.WithTransaction(rawTx)))
	require.NoError(t, rawTx.Commit(ctx))

	require.Equal(t, 1, countDeliveries(t, pool, "worker"))
}

// TestOutboxBatchAtomic: PublishBatch shares the all-or-nothing fate of its tx —
// every delivery in the batch commits together, or none do.
func TestOutboxBatchAtomic(t *testing.T) {
	t.Parallel()

	events := []eventbus.Event{
		{Topic: "events", Type: "a"},
		{Topic: "events", Type: "b"},
		{Topic: "events", Type: "c"},
	}

	t.Run("commit", func(t *testing.T) {
		ctx := t.Context()
		rec := &recorder{}
		c, pool := newClient(t)
		c.Register(consumer("worker", "events", rec.handle(nil)))
		require.NoError(t, c.RegisterSubscriptions(ctx))

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, c.PublishBatch(ctx, events, eventbus.WithTransaction(tx)))
		require.NoError(t, tx.Commit(ctx))

		require.Equal(t, 3, countDeliveries(t, pool, "worker"))
	})

	t.Run("rollback", func(t *testing.T) {
		ctx := t.Context()
		rec := &recorder{}
		c, pool := newClient(t)
		c.Register(consumer("worker", "events", rec.handle(nil)))
		require.NoError(t, c.RegisterSubscriptions(ctx))

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, c.PublishBatch(ctx, events, eventbus.WithTransaction(tx)))
		require.NoError(t, tx.Rollback(ctx))

		require.Equal(t, 0, countDeliveries(t, pool, "worker"))
	})
}
