package eventbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// claimed is a delivery handed to a worker, plus the policy fields the worker
// needs to finalize it (not exposed to the Handler).
type claimed struct {
	Delivery
	maxAttempts int
	deadLetter  string
}

// Run drains the named consumers (all, if none named) repeatedly until ctx is
// cancelled, sleeping WithPollInterval between passes. It is the long-lived
// worker entry point; a scheduled one-shot deployment calls RunOnce instead.
func (c *Client) Run(ctx context.Context, names ...string) error {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		if err := c.RunOnce(ctx, names...); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			c.logger.ErrorContext(ctx, "eventbus: drain pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// RunOnce registers the named consumers' subscriptions, then drains their queues
// to exhaustion once and returns. It does no internal polling, which makes it
// both the scheduled-one-shot primitive and the unit of work tests drive
// directly.
//
// Consumers are independent queues, so they drain concurrently — a backed-up
// consumer cannot head-of-line-block the others. A handler holds no database
// connection while it runs (only the brief claim and finalize transactions do),
// so the fan-out does not pin sum(Concurrency) connections.
func (c *Client) RunOnce(ctx context.Context, names ...string) error {
	if err := c.EnsureSchema(ctx); err != nil {
		return err
	}
	consumers, err := c.consumersFor(names)
	if err != nil {
		return err
	}
	if err := c.RegisterSubscriptions(ctx); err != nil {
		return err
	}
	if len(consumers) == 1 {
		return c.drainConsumer(ctx, consumers[0])
	}
	var wg sync.WaitGroup
	errs := make([]error, len(consumers))
	for i, cons := range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = c.drainConsumer(ctx, cons)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// drainConsumer drains one consumer to exhaustion in claim → process → finalize
// batches: each round claims up to claimBatchSize deliveries in one round-trip,
// runs their handlers concurrently (bounded by Concurrency), and finalizes the
// whole batch — successes in a single batched delete, the rare retry/dead-letter
// per delivery. Because a batch is fully finalized before the next claim, an
// empty claim means the queue is truly drained (no in-flight coordination, no
// termination race). A backed-up partition simply reappears in a later batch.
func (c *Client) drainConsumer(ctx context.Context, cons Consumer) error {
	for {
		batch, err := c.claimBatch(ctx, cons, c.claimBatchSize)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		if err := c.finalizeBatch(ctx, cons, c.processBatch(ctx, cons, batch)); err != nil {
			return err
		}
	}
}

// outcomeKind is how a processed delivery should be finalized.
type outcomeKind int

const (
	outcomeComplete outcomeKind = iota
	outcomeRetry
	outcomeDeadLetter
)

// outcome pairs a processed delivery with how to finalize it.
type outcome struct {
	cl   claimed
	kind outcomeKind
	err  error
}

// processBatch runs the handlers for a batch concurrently (at most Concurrency at
// once) and returns each delivery's outcome. Handlers hold no database connection.
func (c *Client) processBatch(ctx context.Context, cons Consumer, batch []claimed) []outcome {
	outcomes := make([]outcome, len(batch))
	sem := make(chan struct{}, cons.Concurrency)
	var wg sync.WaitGroup
	for i, cl := range batch {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int, cl claimed) {
			defer wg.Done()
			defer func() { <-sem }()
			outcomes[i] = c.classify(ctx, cons, cl)
		}(i, cl)
	}
	wg.Wait()
	return outcomes
}

// classify runs a delivery's handler and decides its fate without touching the
// database: complete on success, dead-letter when attempts are exhausted,
// otherwise retry.
func (c *Client) classify(ctx context.Context, cons Consumer, cl claimed) outcome {
	if err := c.runHandler(ctx, cons.Handler, cl.Delivery); err != nil {
		c.logger.WarnContext(ctx, "eventbus: handler failed",
			"consumer", cl.Consumer, "delivery", cl.ID, "attempt", cl.Attempts, "error", err)
		if cl.Attempts >= cl.maxAttempts {
			return outcome{cl: cl, kind: outcomeDeadLetter, err: err}
		}
		return outcome{cl: cl, kind: outcomeRetry, err: err}
	}
	return outcome{cl: cl, kind: outcomeComplete}
}

// finalizeBatch persists a processed batch: all successes (and their partition
// releases) in one batched round-trip, then the rarer retries and dead-letters
// per delivery (they carry per-delivery state).
func (c *Client) finalizeBatch(ctx context.Context, cons Consumer, outcomes []outcome) error {
	var completeIDs []int64
	var completeKeys []string
	ns := ""
	for _, o := range outcomes {
		if o.kind != outcomeComplete {
			continue
		}
		ns = o.cl.Namespace
		completeIDs = append(completeIDs, o.cl.ID)
		if o.cl.PartitionKey != "" {
			completeKeys = append(completeKeys, o.cl.PartitionKey)
		}
	}
	if len(completeIDs) > 0 {
		b := &pgx.Batch{}
		b.Queue(`DELETE FROM eventbus.deliveries WHERE namespace = $1 AND consumer = $2 AND id = ANY($3::bigint[])`,
			ns, cons.Name, completeIDs)
		if len(completeKeys) > 0 {
			b.Queue(`UPDATE eventbus.partitions SET locked_by = NULL, locked_at = NULL, touched_at = now()
				WHERE namespace = $1 AND consumer = $2 AND partition_key = ANY($3::text[])`,
				ns, cons.Name, completeKeys)
		}
		if err := c.sendBatch(ctx, b); err != nil {
			return fmt.Errorf("eventbus: batch complete: %w", err)
		}
	}

	for _, o := range outcomes {
		switch o.kind {
		case outcomeRetry:
			c.obs.Count(ctx, "retried", Attr{"consumer", cons.Name})
			if err := c.retry(ctx, o.cl, o.err); err != nil {
				return err
			}
		case outcomeDeadLetter:
			metric := "failed"
			if o.cl.deadLetter != "" {
				metric = "dead_lettered"
			}
			c.obs.Count(ctx, metric, Attr{"consumer", cons.Name})
			if err := c.deadLetter(ctx, o.cl, o.err); err != nil {
				return err
			}
		}
	}
	return nil
}

// claimBatch reserves up to n runnable deliveries in one transaction: keyed
// distinct-key heads first (one per key, preserving order), then unkeyed, to fill
// the requested count.
func (c *Client) claimBatch(ctx context.Context, cons Consumer, n int) ([]claimed, error) {
	if n <= 0 {
		return nil, nil
	}
	ns, err := c.namespace(ctx)
	if err != nil {
		return nil, fmt.Errorf("eventbus: resolve namespace: %w", err)
	}
	tx, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	out, err := c.claimKeyedBatch(ctx, tx, ns, cons.Name, n)
	if err != nil {
		return nil, err
	}
	if remaining := n - len(out); remaining > 0 {
		unkeyed, err := c.claimUnkeyedBatch(ctx, tx, ns, cons.Name, remaining)
		if err != nil {
			return nil, err
		}
		out = append(out, unkeyed...)
	}
	if len(out) == 0 {
		return nil, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("eventbus: commit claim tx: %w", err)
	}
	return out, nil
}

// claimColumns is the RETURNING list shared by the claim queries.
const claimColumns = `d.id, d.topic, d.type, COALESCE(d.partition_key, ''),
	d.payload, d.attempts, d.max_attempts, COALESCE(d.dead_letter, '')`

// claimKeyedBatch locks up to n free partitions that have runnable work and
// claims each partition's lowest-id pending delivery (DISTINCT ON, so at most one
// per key per batch — pg-boss's in-claim dedup). The partition locks outlive the
// transaction, keeping each key serialized until its delivery is finalized.
func (c *Client) claimKeyedBatch(ctx context.Context, tx DBTX, ns, consumer string, n int) ([]claimed, error) {
	rows, err := tx.Query(ctx, `
		WITH parts AS (
			SELECT p.partition_key
			FROM eventbus.partitions p
			WHERE p.namespace = $1 AND p.consumer = $2
				AND (p.locked_at IS NULL OR p.locked_at < now() - $3::interval)
				AND EXISTS (
					SELECT 1 FROM eventbus.deliveries d
					WHERE d.namespace = $1 AND d.consumer = $2 AND d.partition_key = p.partition_key
						AND d.state = 'pending' AND d.run_after <= now()
				)
				-- Never reclaim a key while one of its deliveries is still active
				-- (a crashed holder the janitor has not yet rescued): claiming a
				-- later delivery here would process it ahead of the stuck one.
				AND NOT EXISTS (
					SELECT 1 FROM eventbus.deliveries d
					WHERE d.namespace = $1 AND d.consumer = $2 AND d.partition_key = p.partition_key
						AND d.state = 'active'
				)
			ORDER BY p.partition_key
			FOR UPDATE OF p SKIP LOCKED
			LIMIT $5
		),
		locked AS (
			UPDATE eventbus.partitions p
			SET locked_by = $4, locked_at = now(), touched_at = now()
			FROM parts
			WHERE p.namespace = $1 AND p.consumer = $2 AND p.partition_key = parts.partition_key
			RETURNING p.partition_key
		),
		heads AS (
			SELECT DISTINCT ON (d.partition_key) d.id
			FROM eventbus.deliveries d JOIN locked ON d.partition_key = locked.partition_key
			WHERE d.namespace = $1 AND d.consumer = $2 AND d.state = 'pending' AND d.run_after <= now()
			ORDER BY d.partition_key, d.id
		)
		UPDATE eventbus.deliveries d
		SET state = 'active', locked_by = $4, locked_at = now(), heartbeat_at = now(), attempts = d.attempts + 1
		FROM heads
		WHERE d.namespace = $1 AND d.consumer = $2 AND d.id = heads.id
		RETURNING `+claimColumns,
		ns, consumer, c.leaseTimeout, c.owner, n)
	if err != nil {
		return nil, fmt.Errorf("eventbus: claim keyed: %w", err)
	}
	return scanClaims(rows, ns, consumer)
}

// claimUnkeyedBatch claims up to n pending unkeyed deliveries (lowest id first),
// locking the delivery rows directly.
func (c *Client) claimUnkeyedBatch(ctx context.Context, tx DBTX, ns, consumer string, n int) ([]claimed, error) {
	rows, err := tx.Query(ctx, `
		WITH candidate AS (
			SELECT d.id
			FROM eventbus.deliveries d
			WHERE d.namespace = $1 AND d.consumer = $2 AND d.partition_key IS NULL
				AND d.state = 'pending' AND d.run_after <= now()
			ORDER BY d.id
			FOR UPDATE OF d SKIP LOCKED
			LIMIT $3
		)
		UPDATE eventbus.deliveries d
		SET state = 'active', locked_by = $4, locked_at = now(), heartbeat_at = now(), attempts = d.attempts + 1
		FROM candidate
		WHERE d.namespace = $1 AND d.consumer = $2 AND d.id = candidate.id
		RETURNING `+claimColumns,
		ns, consumer, n, c.owner)
	if err != nil {
		return nil, fmt.Errorf("eventbus: claim unkeyed: %w", err)
	}
	return scanClaims(rows, ns, consumer)
}

// scanClaims scans claim RETURNING rows into delivery records.
func scanClaims(rows pgx.Rows, ns, consumer string) ([]claimed, error) {
	defer rows.Close()
	var out []claimed
	for rows.Next() {
		cl := claimed{Delivery: Delivery{Namespace: ns, Consumer: consumer}}
		if err := rows.Scan(&cl.ID, &cl.Topic, &cl.Type, &cl.PartitionKey,
			&cl.Payload, &cl.Attempts, &cl.maxAttempts, &cl.deadLetter); err != nil {
			return nil, fmt.Errorf("eventbus: scan claim: %w", err)
		}
		out = append(out, cl)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventbus: claim: %w", err)
	}
	return out, nil
}

// runHandler invokes the handler, converting a panic into an error so one bad
// delivery cannot take down the worker.
func (c *Client) runHandler(ctx context.Context, h Handler, d Delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("eventbus: handler panicked: %v", r)
		}
	}()
	return h(ctx, d)
}

// retry returns a single delivery to pending with a backoff delay (and releases
// its partition lock) in one batched round-trip.
func (c *Client) retry(ctx context.Context, cl claimed, cause error) error {
	delay := c.backoff(cl.Attempts)
	return c.finalize(ctx, cl, func(b *pgx.Batch) {
		b.Queue(`
			UPDATE eventbus.deliveries
			SET state = 'pending', run_after = now() + $4::interval,
			    locked_by = NULL, locked_at = NULL, heartbeat_at = NULL, last_error = $5
			WHERE namespace = $1 AND consumer = $2 AND id = $3`,
			cl.Namespace, cl.Consumer, cl.ID, delay, errString(cause))
	})
}

// deadLetter handles a delivery that has exhausted its attempts: copy it to the
// dead-letter queue if one is configured, then delete it. With no dead-letter
// queue the delivery is simply dropped.
func (c *Client) deadLetter(ctx context.Context, cl claimed, cause error) error {
	return c.finalize(ctx, cl, func(b *pgx.Batch) {
		if cl.deadLetter != "" {
			b.Queue(`
				INSERT INTO eventbus.deliveries
					(namespace, consumer, topic, type, partition_key, payload, last_error)
				VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				cl.Namespace, cl.deadLetter, cl.Topic, cl.Type,
				nullString(cl.PartitionKey), cl.Payload, errString(cause))
			// Seed the dead-letter consumer's partition row for a keyed copy, the
			// same way Publish does — keyed claims start from eventbus.partitions,
			// so without this the copy would be permanently unclaimable.
			if cl.PartitionKey != "" {
				b.Queue(`
					INSERT INTO eventbus.partitions (namespace, consumer, partition_key)
					VALUES ($1, $2, $3)
					ON CONFLICT (namespace, consumer, partition_key) DO UPDATE SET touched_at = now()`,
					cl.Namespace, cl.deadLetter, cl.PartitionKey)
			}
		}
		b.Queue(`DELETE FROM eventbus.deliveries WHERE namespace = $1 AND consumer = $2 AND id = $3`,
			cl.Namespace, cl.Consumer, cl.ID)
	})
}

// finalize runs one delivery's terminal/retry mutation and its partition-lock
// release as one atomic, single-round-trip batch. Used for the per-delivery
// retry and dead-letter paths; successful completions are batched in finalizeBatch.
func (c *Client) finalize(ctx context.Context, cl claimed, queue func(*pgx.Batch)) error {
	b := &pgx.Batch{}
	queue(b)
	if cl.PartitionKey != "" {
		b.Queue(`
			UPDATE eventbus.partitions
			SET locked_by = NULL, locked_at = NULL, touched_at = now()
			WHERE namespace = $1 AND consumer = $2 AND partition_key = $3`,
			cl.Namespace, cl.Consumer, cl.PartitionKey)
	}
	if err := c.sendBatch(ctx, b); err != nil {
		return fmt.Errorf("eventbus: finalize delivery: %w", err)
	}
	return nil
}

func errString(err error) *string {
	if err == nil {
		return nil
	}
	s := err.Error()
	if len(s) > 1000 {
		s = s[:1000]
	}
	return &s
}
