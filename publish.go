package eventbus

import (
	"context"
	"fmt"
)

// publishConfig holds per-call publish options.
type publishConfig struct {
	tx DBTX
}

// PublishOption configures a single Publish/PublishBatch call.
type PublishOption func(*publishConfig)

// WithTransaction makes the publish run on the caller's transaction instead of
// its own, so the deliveries commit atomically with the caller's business write
// (the transactional outbox pattern). It is optional: omitting it makes Publish
// open and commit its own transaction. The argument is any DBTX — *protodb.Tx,
// pgx.Tx, and *pgxpool.Pool all satisfy it.
func WithTransaction(tx DBTX) PublishOption {
	return func(pc *publishConfig) { pc.tx = tx }
}

// Publish writes one delivery per consumer subscribed to ev.Topic. With no
// WithTransaction option it commits in its own transaction; with one it joins
// the caller's. Publishing to a topic with no subscribers drops the event and
// counts ft.eventbus.published_to_empty_topic (unless WithRequireSubscriber is
// set, in which case it returns ErrNoSubscriber).
func (c *Client) Publish(ctx context.Context, ev Event, opts ...PublishOption) error {
	return c.publishAll(ctx, []Event{ev}, opts)
}

// PublishBatch publishes several events. With no WithTransaction option the
// whole batch commits atomically in one transaction.
func (c *Client) PublishBatch(ctx context.Context, evs []Event, opts ...PublishOption) error {
	return c.publishAll(ctx, evs, opts)
}

func (c *Client) publishAll(ctx context.Context, evs []Event, opts []PublishOption) error {
	if err := c.EnsureSchema(ctx); err != nil {
		return err
	}
	var cfg publishConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	ns, err := c.namespace(ctx)
	if err != nil {
		return fmt.Errorf("eventbus: resolve namespace: %w", err)
	}

	// Caller-supplied transaction: insert directly, let the caller commit.
	if cfg.tx != nil {
		return c.insertDeliveries(ctx, cfg.tx, ns, evs)
	}

	// Standalone: open and commit our own transaction so a multi-event batch (and
	// its fan-out) is atomic.
	tx, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := c.insertDeliveries(ctx, tx, ns, evs); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("eventbus: commit publish tx: %w", err)
	}
	return nil
}

func (c *Client) insertDeliveries(ctx context.Context, db DBTX, ns string, evs []Event) error {
	// Cache the subscriber set per topic: a batch to the same topic reads routing
	// once.
	subsByTopic := make(map[string][]subscription)
	for _, ev := range evs {
		if ev.Topic == "" {
			return ErrEmptyTopic
		}
		subs, ok := subsByTopic[ev.Topic]
		if !ok {
			var err error
			if subs, err = c.subscribersOf(ctx, db, ev.Topic); err != nil {
				return err
			}
			subsByTopic[ev.Topic] = subs
		}
		if len(subs) == 0 {
			c.obs.Count(ctx, "published_to_empty_topic", Attr{"topic", ev.Topic})
			c.logger.DebugContext(ctx, "eventbus: publish to topic with no subscribers", "topic", ev.Topic)
			if c.requireSubAll {
				return fmt.Errorf("%w: %s", ErrNoSubscriber, ev.Topic)
			}
			continue
		}
		if err := c.fanOut(ctx, db, ns, ev, subs); err != nil {
			return err
		}
	}
	return nil
}

// fanOut writes one delivery per subscriber for a single event, seeding the
// partition row for keyed deliveries so the claim query has a row to lock.
func (c *Client) fanOut(ctx context.Context, db DBTX, ns string, ev Event, subs []subscription) error {
	payload := ev.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	for _, s := range subs {
		if _, err := db.Exec(ctx, `
			INSERT INTO eventbus.deliveries
				(namespace, consumer, topic, type, partition_key, payload, max_attempts, dead_letter)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, ns, s.consumer, ev.Topic, ev.Type, nullString(ev.PartitionKey), payload, s.maxAttempts, nullString(s.deadLetter)); err != nil {
			return fmt.Errorf("eventbus: insert delivery: %w", err)
		}
		if ev.PartitionKey != "" {
			if _, err := db.Exec(ctx, `
				INSERT INTO eventbus.partitions (namespace, consumer, partition_key)
				VALUES ($1, $2, $3)
				ON CONFLICT (namespace, consumer, partition_key) DO UPDATE SET touched_at = now()
			`, ns, s.consumer, ev.PartitionKey); err != nil {
				return fmt.Errorf("eventbus: seed partition: %w", err)
			}
		}
	}
	return nil
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
