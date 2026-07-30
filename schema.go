package eventbus

import (
	"context"
	"fmt"
)

// ddlStatements creates the eventbus schema, tables, and indexes. Every
// statement is idempotent (IF NOT EXISTS), so EnsureSchema is safe to call
// concurrently across processes and repeatedly within one.
//
// The core schema is tenant-generic: namespace is an opaque string (default ”)
// and there is no row-level security here. An embedder may layer RLS on the
// namespace column separately.
var ddlStatements = []string{
	`CREATE SCHEMA IF NOT EXISTS eventbus`,

	// deliveries: one row per consumer copy of a published event. The queue a
	// row belongs to is "consumer"; workers claim WHERE consumer = $name.
	`CREATE TABLE IF NOT EXISTS eventbus.deliveries (
		id            BIGSERIAL,
		namespace     TEXT        NOT NULL DEFAULT '',
		consumer      TEXT        NOT NULL,
		topic         TEXT        NOT NULL,
		type          TEXT        NOT NULL DEFAULT '',
		partition_key TEXT,
		payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
		state         TEXT        NOT NULL DEFAULT 'pending',
		attempts      INTEGER     NOT NULL DEFAULT 0,
		max_attempts  INTEGER     NOT NULL DEFAULT 20,
		run_after     TIMESTAMPTZ NOT NULL DEFAULT now(),
		locked_by     TEXT,
		locked_at     TIMESTAMPTZ,
		heartbeat_at  TIMESTAMPTZ,
		dead_letter   TEXT,
		last_error    TEXT,
		created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (namespace, consumer, id)
	)`,
	// Fetch index for the claim path. partition_key is the third column so a keyed
	// claim probes a single key directly (the partitions "has work?" EXISTS and the
	// per-key "lowest pending id" lookup) instead of scanning the whole consumer's
	// backlog. The unkeyed claim seeks the partition_key IS NULL group. run_after
	// then id support the runnable filter and FIFO order.
	`CREATE INDEX IF NOT EXISTS deliveries_claim
		ON eventbus.deliveries (namespace, consumer, partition_key, run_after, id)
		WHERE state = 'pending'`,

	// partitions: per-key serial lock (graphile-worker's job_queues analog). A
	// row exists for every (namespace, consumer, partition_key) that has been
	// published; locked_at holds the key across a handler run; touched_at records
	// the last activity so the janitor can reap long-idle keys.
	`CREATE TABLE IF NOT EXISTS eventbus.partitions (
		namespace     TEXT        NOT NULL DEFAULT '',
		consumer      TEXT        NOT NULL,
		partition_key TEXT        NOT NULL,
		locked_by     TEXT,
		locked_at     TIMESTAMPTZ,
		touched_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (namespace, consumer, partition_key)
	)`,
	// Reap index: idle (unlocked) partition rows by age.
	`CREATE INDEX IF NOT EXISTS partitions_idle
		ON eventbus.partitions (touched_at)
		WHERE locked_at IS NULL`,

	// subscriptions: global routing config (NOT namespace-partitioned). The
	// cross-process source of truth for fan-out: a publisher reads it to learn
	// which consumers a topic copies to. Written only by consumer self-
	// registration; last_registered_at lets the janitor reap abandoned rows.
	`CREATE TABLE IF NOT EXISTS eventbus.subscriptions (
		topic              TEXT        NOT NULL,
		consumer           TEXT        NOT NULL,
		max_attempts       INTEGER     NOT NULL DEFAULT 20,
		dead_letter        TEXT,
		last_registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (topic, consumer)
	)`,
}

// EnsureSchema creates the eventbus schema and tables if they do not exist. The
// DDL runs once per Client on the first successful call; a transient failure is
// NOT latched, so a later call retries (every publish/drain entry point calls
// this, so a startup blip must not wedge the Client permanently).
func (c *Client) EnsureSchema(ctx context.Context) error {
	if c.schemaPrepared.Load() {
		return nil
	}
	c.schemaMu.Lock()
	defer c.schemaMu.Unlock()
	if c.schemaPrepared.Load() {
		return nil
	}
	for _, stmt := range ddlStatements {
		if _, err := c.db.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("eventbus: ensure schema: %w", err)
		}
	}
	c.schemaPrepared.Store(true)
	return nil
}
