# eventbus

[![Go Reference](https://pkg.go.dev/badge/github.com/jpugliesi/eventbus.svg)](https://pkg.go.dev/github.com/jpugliesi/eventbus)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Postgres-backed transactional event queue for Go. Publish an event inside
your own database transaction and it's guaranteed to be delivered: no
dual-write, no separate broker, no dropped events when a process crashes
mid-write.

`eventbus` gives you:

- **A real transactional outbox.** `Publish`/`PublishBatch` can join your
  existing `pgx` transaction (`WithTransaction`), so an event and the business
  row that triggered it commit or roll back together.
- **Competing-consumer drain** via `SELECT ... FOR UPDATE SKIP LOCKED`,
  batch-claimed for throughput.
- **Per-partition-key ordering.** Deliveries sharing a key are processed in
  publish order within a consumer; everything else runs in parallel up to the
  consumer's configured concurrency.
- **Copy-on-publish fan-out.** One `Publish` call becomes one delivery per
  subscribed consumer, and multiple independent consumers each get their own
  copy of every event.
- **Bounded retry with a dead-letter queue**, plus a **janitor** that
  garbage-collects terminal rows, stale partition locks, and abandoned
  subscriptions.

It depends on nothing but [`pgx/v5`](https://github.com/jackc/pgx) and the
standard library: no separate broker, no code generator, no CLI, no migration
tool. `~1,200` lines of production Go.

## Install

```sh
go get github.com/jpugliesi/eventbus
```

## Quick start

```go
package main

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpugliesi/eventbus"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://localhost/mydb")
	if err != nil {
		log.Fatal(err)
	}

	client := eventbus.NewClient(pool)

	client.Register(eventbus.Consumer{
		Name:        "email-sender",
		Topic:       "orders",
		Concurrency: 8,
		Handler: func(ctx context.Context, d eventbus.Delivery) error {
			return sendOrderConfirmation(ctx, d.Payload)
		},
	})
	if err := client.RegisterSubscriptions(ctx); err != nil {
		log.Fatal(err)
	}

	// Long-running worker process: polls and drains registered consumers
	// until ctx is cancelled.
	if err := client.Run(ctx, "email-sender"); err != nil {
		log.Fatal(err)
	}
}

func sendOrderConfirmation(ctx context.Context, payload []byte) error { return nil }
```

### Transactional outbox

An outbox exists so the event and the write that caused it share one atomic
fate.

```go
tx, err := pool.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, orderID, total); err != nil {
	return err
}

if err := client.Publish(ctx, eventbus.Event{
	Topic:        "orders",
	Type:         "order.created",
	PartitionKey: orderID, // all events for this order stay in order
	Payload:      payload,
}, eventbus.WithTransaction(tx)); err != nil {
	return err
}

return tx.Commit(ctx) // order row and event commit together, or neither does
```

### Testing

The `eventbustest` subpackage provides a Postgres-backed test client and an
in-memory `Publisher` double for unit-testing publish-side code without a
database at all.

```go
func TestOrderCreatedIsHandled(t *testing.T) {
	pool := myTestPool(t) // any *pgxpool.Pool
	client := eventbustest.MustClient(t, pool)

	var handled bool
	client.Register(eventbus.Consumer{
		Name:  "worker",
		Topic: "orders",
		Handler: func(context.Context, eventbus.Delivery) error {
			handled = true
			return nil
		},
	})
	require.NoError(t, client.RegisterSubscriptions(t.Context()))
	require.NoError(t, client.Publish(t.Context(), eventbus.Event{Topic: "orders", Type: "order.created"}))

	eventbustest.Drain(t, client, "worker") // RunOnce, fails the test on error
	require.True(t, handled)
}

func TestCreateOrderPublishesEvent(t *testing.T) {
	var pub eventbustest.RecordingPublisher // no database needed at all
	createOrder(&pub, order)                // your code takes an eventbus.Publisher
	require.Len(t, pub.Events(), 1)
	require.Equal(t, "order.created", pub.Events()[0].Type)
}
```

## How it works

Three tables, created and kept up to date automatically by `Client.EnsureSchema`:

- **`eventbus.deliveries`.** One row per `(consumer, event)` pair. A publish
  to a topic with two subscribed consumers writes two delivery rows, so each
  consumer's queue is independent of the others.
- **`eventbus.partitions`.** A serial lock per `(consumer, partition_key)`,
  the mechanism that gives keyed deliveries in-order processing.
- **`eventbus.subscriptions`.** The topic → consumer routing table, kept
  current by `RegisterSubscriptions`.

Drain is **claim → process → finalize**, all batched. `RunOnce` claims up to
`WithClaimBatchSize` (default 500) rows in one round trip. Keyed claims dedup
to one head-of-line delivery per free partition key, so ordering never
depends on processing order within the batch. Handlers then run concurrently
up to each consumer's `Concurrency`, and the whole batch finalizes (delete or
retry-schedule) in one more round trip.

Delivery is **at-least-once**: a handler can run more than once for the same
event if a worker dies after the handler succeeds but before its row is
deleted. Handlers must be idempotent. A lease (`WithLeaseTimeout`, default
15m) governs a claimed delivery and its partition lock together, so a crashed
worker can't have its keyed delivery reclaimed out of order; the `Janitor`
rescues timed-out deliveries and frees stale locks in the order that
preserves this.

There's no LISTEN/NOTIFY. `Run` is a plain poll loop (`WithPollInterval`,
default 1s); you can also call `RunOnce` yourself on whatever schedule you
like (a cron job, a Lambda invocation, a test). That trades a little latency
for a simpler operational model: no long-lived listening connection to keep
alive, no notification-payload size limit to work around.

## Performance

`BenchmarkDrain` (see `bench_test.go`) measures end-to-end throughput draining
a 2,000-delivery backlog. Batching the claim and finalize steps, rather than
claiming and finalizing one delivery at a time, is most of the win:

| path                    | naive (per-delivery) | batched  | speedup |
| ----------------------- | --------------------:| --------:| -------:|
| unkeyed (parallel)      |               827/s   | ~117,000/s | ~127×  |
| keyed (ordered)         |               757/s   | ~63,000/s  | ~84×   |

`BenchmarkClaimBatchSize` sweeps the batch size to find the throughput knee,
which is what the default of 500 is set from. `TestKeyedClaimUsesIndex` runs
`EXPLAIN (ANALYZE, BUFFERS)` on the per-partition claim query and asserts it
hits the `deliveries_claim` partial index rather than sequential-scanning the
table, so the claim path's cost doesn't grow with backlog size.

Run it yourself:

```sh
make bench
```

(Numbers above are from the benchmark's original development machine; yours
will vary by hardware. Run `make bench` to reproduce them locally.)

## Design notes and prior art

Two mechanisms are borrowed from existing Postgres-queue designs in the
Node.js ecosystem, credited by name in the source comments:

- The `eventbus.partitions` per-key serial lock table is the same idea as
  [graphile-worker](https://github.com/graphile/worker)'s `job_queues` table.
- The `DISTINCT ON (partition_key)` dedup used when claiming a keyed batch
  mirrors [pg-boss](https://github.com/timgit/pg-boss)'s in-claim dedup for
  fetching one job per key.

**How this differs from [River](https://github.com/riverqueue/river)** (the
most prominent Go/Postgres job-queue library). River and `eventbus` both use
`SKIP LOCKED` and both support enqueueing within a caller's existing
transaction, so "transactional outbox" isn't the differentiator: River has it
too, via `InsertTx`. The real difference is shape and scope:

- **Job queue vs. event bus.** River executes a job exactly once, picked up
  by one worker. `eventbus` is copy-on-publish: one `Publish` to a topic with
  three subscribed consumers produces three independent deliveries. It's
  built for "many things need to react to this event," not "run this one
  task."
- **Footprint.** River ships a full job-processing system: a CLI, a
  migration tool, cron/periodic jobs, unique jobs, batch insertion via
  `COPY`, and a web UI (River UI). `eventbus` is deliberately just the queue
  primitive, a library you embed with no surrounding tooling. Bring your own
  scheduler, dashboard, or telemetry.
- **Ordering.** Per-partition-key strict ordering is a first-class,
  benchmarked feature of `eventbus` (see Performance above); it isn't River's
  primary design point.

If you need River's fuller feature set (periodic jobs, a UI, a CLI), use
River. If you want a small, dependency-free primitive that bolts a
transactional outbox and a multi-consumer event bus onto a service already
running Postgres, that's what this is for.

## License

[MIT](LICENSE)
