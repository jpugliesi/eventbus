package eventbus

import (
	"context"
	"fmt"
)

// subscription is one routing row: a consumer that drains a topic.
type subscription struct {
	consumer    string
	maxAttempts int
	deadLetter  string
}

// subscribersOf returns the consumers subscribed to a topic, read fresh from the
// global subscriptions table so a publisher in any process sees registrations
// written by a separate consumer process.
func (c *Client) subscribersOf(ctx context.Context, db DBTX, topic string) ([]subscription, error) {
	rows, err := db.Query(ctx, `
		SELECT consumer, max_attempts, COALESCE(dead_letter, '')
		FROM eventbus.subscriptions
		WHERE topic = $1
		ORDER BY consumer
	`, topic)
	if err != nil {
		return nil, fmt.Errorf("eventbus: read subscriptions: %w", err)
	}
	defer rows.Close()

	var subs []subscription
	for rows.Next() {
		var s subscription
		if err := rows.Scan(&s.consumer, &s.maxAttempts, &s.deadLetter); err != nil {
			return nil, fmt.Errorf("eventbus: scan subscription: %w", err)
		}
		subs = append(subs, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventbus: iterate subscriptions: %w", err)
	}
	return subs, nil
}

// RegisterSubscriptions publishes every registered consumer's routing to the
// subscriptions table and refreshes its last_registered_at. Run/RunOnce call it
// automatically; tests (and any publisher that must observe routing before the
// consumer first runs) may call it directly. It is idempotent.
func (c *Client) RegisterSubscriptions(ctx context.Context) error {
	if err := c.EnsureSchema(ctx); err != nil {
		return err
	}
	consumers, err := c.consumersFor(nil)
	if err != nil {
		return err
	}
	for _, cons := range consumers {
		if _, err := c.db.Exec(ctx, `
			INSERT INTO eventbus.subscriptions (topic, consumer, max_attempts, dead_letter, last_registered_at)
			VALUES ($1, $2, $3, $4, now())
			ON CONFLICT (topic, consumer) DO UPDATE
			SET max_attempts = excluded.max_attempts,
			    dead_letter = excluded.dead_letter,
			    last_registered_at = now()
		`, cons.Topic, cons.Name, cons.MaxAttempts, nullString(cons.DeadLetter)); err != nil {
			return fmt.Errorf("eventbus: register subscription %s: %w", cons.Name, err)
		}

		// A consumer name is exactly one queue: if it moved to a new topic (same
		// Name, different Topic), drop the stale subscription so old-topic
		// publishers stop fanning into this queue. This is cheap (tiny table) and
		// normally deletes nothing.
		tag, err := c.db.Exec(ctx, `
			DELETE FROM eventbus.subscriptions WHERE consumer = $1 AND topic <> $2
		`, cons.Name, cons.Topic)
		if err != nil {
			return fmt.Errorf("eventbus: prune stale subscription %s: %w", cons.Name, err)
		}
		// Only on an actual topic change (rare) purge the deliveries already routed
		// to the old topic, so the handler doesn't process events from a topic it no
		// longer subscribes to. Gated on the delete above so the drain hot path
		// never scans deliveries for this.
		if tag.RowsAffected() > 0 {
			if _, err := c.db.Exec(ctx, `
				DELETE FROM eventbus.deliveries WHERE consumer = $1 AND topic <> $2
			`, cons.Name, cons.Topic); err != nil {
				return fmt.Errorf("eventbus: purge stale deliveries %s: %w", cons.Name, err)
			}
		}
	}
	return nil
}
