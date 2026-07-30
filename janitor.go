package eventbus

import (
	"context"
	"fmt"
)

// JanitorStats reports what one Janitor sweep changed.
type JanitorStats struct {
	UnrunDeleted     int64 // pending deliveries past unrunRetention
	SubscriptionsGCd int64 // subscriptions past subTTL
	OrphansDeleted   int64 // deliveries whose subscription was reaped
	Rescued          int64 // active deliveries past leaseTimeout returned to pending
	PartitionsFreed  int64 // partition locks past leaseTimeout released
	PartitionsReaped int64 // idle partition rows past partitionRetention deleted
}

// Janitor performs one garbage-collection sweep across every namespace. It is
// meant to run on a schedule (e.g. a cron/scheduler tick). Unlike claim/drain it
// is namespace-agnostic, so an embedder runs it on an admin connection that
// bypasses row-level security.
//
// The sweep, in order: rescue abandoned active deliveries past the lease and free
// their partition locks; delete undrained pending deliveries past unrunRetention;
// reap subscriptions whose owner stopped refreshing them (and purge their
// deliveries); reap idle partition rows past partitionRetention.
func (c *Client) Janitor(ctx context.Context) (JanitorStats, error) {
	if err := c.EnsureSchema(ctx); err != nil {
		return JanitorStats{}, err
	}
	var stats JanitorStats

	// Rescue: an active delivery whose holder is assumed dead (locked_at older than
	// the lease) goes back to pending so another worker can take it. This runs
	// before partitions are freed so a rescued keyed delivery is pending by the
	// time its key becomes claimable again — preserving per-key order.
	tag, err := c.db.Exec(ctx, `
		UPDATE eventbus.deliveries
		SET state = 'pending', locked_by = NULL, locked_at = NULL, heartbeat_at = NULL
		WHERE state = 'active' AND locked_at < now() - $1::interval
	`, c.leaseTimeout)
	if err != nil {
		return stats, fmt.Errorf("eventbus: janitor rescue: %w", err)
	}
	stats.Rescued = tag.RowsAffected()

	// Free partition locks whose holder is assumed dead (locked_at past the lease),
	// so a stuck key does not starve forever.
	tag, err = c.db.Exec(ctx, `
		UPDATE eventbus.partitions
		SET locked_by = NULL, locked_at = NULL, touched_at = now()
		WHERE locked_at IS NOT NULL AND locked_at < now() - $1::interval
	`, c.leaseTimeout)
	if err != nil {
		return stats, fmt.Errorf("eventbus: janitor free partitions: %w", err)
	}
	stats.PartitionsFreed = tag.RowsAffected()

	// Delete undrained pending deliveries past the unrun retention window (the
	// backstop for a queue nothing is draining).
	tag, err = c.db.Exec(ctx, `
		DELETE FROM eventbus.deliveries
		WHERE state = 'pending' AND created_at < now() - $1::interval
	`, c.unrunRetention)
	if err != nil {
		return stats, fmt.Errorf("eventbus: janitor delete unrun: %w", err)
	}
	stats.UnrunDeleted = tag.RowsAffected()

	// Reap subscriptions whose owning consumer stopped refreshing them, and purge
	// the deliveries routed to those now-dead queues.
	tag, err = c.db.Exec(ctx, `
		DELETE FROM eventbus.deliveries d
		USING (
			SELECT consumer FROM eventbus.subscriptions
			WHERE last_registered_at < now() - $1::interval
		) stale
		WHERE d.consumer = stale.consumer
	`, c.subTTL)
	if err != nil {
		return stats, fmt.Errorf("eventbus: janitor purge orphan deliveries: %w", err)
	}
	stats.OrphansDeleted = tag.RowsAffected()

	tag, err = c.db.Exec(ctx, `
		DELETE FROM eventbus.subscriptions
		WHERE last_registered_at < now() - $1::interval
	`, c.subTTL)
	if err != nil {
		return stats, fmt.Errorf("eventbus: janitor reap subscriptions: %w", err)
	}
	stats.SubscriptionsGCd = tag.RowsAffected()

	// Reap idle partition rows: unlocked, untouched past partitionRetention, and
	// with no remaining deliveries. The no-deliveries guard is what makes this
	// safe — an in-use key always has at least one delivery, so it is never reaped
	// out from under live work (and a future publish re-seeds the row anyway).
	tag, err = c.db.Exec(ctx, `
		DELETE FROM eventbus.partitions p
		WHERE p.locked_at IS NULL
			AND p.touched_at < now() - $1::interval
			AND NOT EXISTS (
				SELECT 1 FROM eventbus.deliveries d
				WHERE d.namespace = p.namespace AND d.consumer = p.consumer
					AND d.partition_key = p.partition_key
			)
	`, c.partitionRetention)
	if err != nil {
		return stats, fmt.Errorf("eventbus: janitor reap partitions: %w", err)
	}
	stats.PartitionsReaped = tag.RowsAffected()

	return stats, nil
}
