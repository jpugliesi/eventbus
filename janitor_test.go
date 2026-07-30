package eventbus_test

import (
	"testing"
	"time"

	"github.com/jpugliesi/eventbus"
	"github.com/stretchr/testify/require"
)

// TestJanitorRescuesStuckActive: an 'active' delivery whose worker died (locked_at
// older than the rescue window) is returned to 'pending' so another worker can
// take it.
func TestJanitorRescuesStuckActive(t *testing.T) {
	t.Parallel()
	c, pool := newClient(t, eventbus.WithLeaseTimeout(time.Minute))

	_, err := pool.Exec(t.Context(), `
		INSERT INTO eventbus.deliveries (consumer, topic, state, locked_by, locked_at)
		VALUES ('w', 't', 'active', 'dead', now() - interval '1 hour')`)
	require.NoError(t, err)

	stats, err := c.Janitor(t.Context())
	require.NoError(t, err)

	require.GreaterOrEqual(t, stats.Rescued, int64(1))
	require.Equal(t, 1, countState(t, pool, "w", "pending"))
	require.Equal(t, 0, countState(t, pool, "w", "active"))
}

// TestJanitorFreesExpiredPartitionLocks: a partition lock whose holder is gone
// (locked_at past the lock TTL) is released so the key does not starve forever.
func TestJanitorFreesExpiredPartitionLocks(t *testing.T) {
	t.Parallel()
	c, pool := newClient(t, eventbus.WithLeaseTimeout(time.Minute))

	_, err := pool.Exec(t.Context(), `
		INSERT INTO eventbus.partitions (consumer, partition_key, locked_by, locked_at)
		VALUES ('w', 'k', 'dead', now() - interval '1 hour')`)
	require.NoError(t, err)

	stats, err := c.Janitor(t.Context())
	require.NoError(t, err)

	require.GreaterOrEqual(t, stats.PartitionsFreed, int64(1))

	var lockedAtNull bool
	require.NoError(t, pool.QueryRow(t.Context(), `
		SELECT locked_at IS NULL FROM eventbus.partitions
		WHERE consumer = 'w' AND partition_key = 'k'`).Scan(&lockedAtNull))
	require.True(t, lockedAtNull, "expired partition lock should be released")
}

// TestJanitorDeletesUnrunPastRetention: undrained pending deliveries older than
// the unrun retention window are deleted; a recent pending row survives.
func TestJanitorDeletesUnrunPastRetention(t *testing.T) {
	t.Parallel()
	c, pool := newClient(t, eventbus.WithUnrunRetention(time.Minute))

	_, err := pool.Exec(t.Context(), `
		INSERT INTO eventbus.deliveries (consumer, topic, state, created_at)
		VALUES ('w', 't', 'pending', now() - interval '1 hour')`)
	require.NoError(t, err)
	// A fresh pending row inside the retention window must survive the sweep.
	_, err = pool.Exec(t.Context(), `
		INSERT INTO eventbus.deliveries (consumer, topic, state, created_at)
		VALUES ('w', 't', 'pending', now())`)
	require.NoError(t, err)

	stats, err := c.Janitor(t.Context())
	require.NoError(t, err)

	require.GreaterOrEqual(t, stats.UnrunDeleted, int64(1))
	require.Equal(t, 1, countDeliveries(t, pool, "w")) // only the recent pending remains
	require.Equal(t, 1, countState(t, pool, "w", "pending"))
}

// TestJanitorReapsIdlePartitions: an unlocked partition with no deliveries, idle
// past partitionRetention, is reaped; a partition that is busy (has a delivery),
// locked, or recently touched survives.
func TestJanitorReapsIdlePartitions(t *testing.T) {
	t.Parallel()
	c, pool := newClient(t, eventbus.WithPartitionRetention(time.Minute))
	ctx := t.Context()

	// idle: unlocked, old, no deliveries -> reaped.
	_, err := pool.Exec(ctx, `INSERT INTO eventbus.partitions (consumer, partition_key, touched_at)
		VALUES ('w', 'idle', now() - interval '1 hour')`)
	require.NoError(t, err)
	// busy: old but has a pending delivery -> survives (no-deliveries guard).
	_, err = pool.Exec(ctx, `INSERT INTO eventbus.partitions (consumer, partition_key, touched_at)
		VALUES ('w', 'busy', now() - interval '1 hour')`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO eventbus.deliveries (consumer, topic, partition_key, state)
		VALUES ('w', 't', 'busy', 'pending')`)
	require.NoError(t, err)
	// locked: old touched_at but currently locked -> survives.
	_, err = pool.Exec(ctx, `INSERT INTO eventbus.partitions (consumer, partition_key, locked_by, locked_at, touched_at)
		VALUES ('w', 'locked', 'holder', now(), now() - interval '1 hour')`)
	require.NoError(t, err)
	// fresh: idle but within retention -> survives.
	_, err = pool.Exec(ctx, `INSERT INTO eventbus.partitions (consumer, partition_key, touched_at)
		VALUES ('w', 'fresh', now())`)
	require.NoError(t, err)

	stats, err := c.Janitor(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.PartitionsReaped)
	require.False(t, partitionExists(t, pool, "w", "idle"), "idle partition should be reaped")
	require.True(t, partitionExists(t, pool, "w", "busy"), "partition with a delivery must survive")
	require.True(t, partitionExists(t, pool, "w", "locked"), "locked partition must survive")
	require.True(t, partitionExists(t, pool, "w", "fresh"), "recently-touched partition must survive")
}

// TestJanitorNoOpOnCleanState: a sweep over an empty schema changes nothing and
// reports zero-valued stats.
func TestJanitorNoOpOnCleanState(t *testing.T) {
	t.Parallel()
	c, _ := newClient(t)

	stats, err := c.Janitor(t.Context())
	require.NoError(t, err)
	require.Equal(t, eventbus.JanitorStats{}, stats)
}
