// Package eventbus is a Postgres-backed transactional event queue.
//
// It provides durable publish, competing-consumer drain via
// SELECT ... FOR UPDATE SKIP LOCKED, per-partition-key ordering, copy-on-publish
// fan-out (one publish becomes one delivery per subscribed consumer), bounded
// retry with a dead-letter queue, and a janitor that garbage-collects terminal
// rows, stale partition locks, and abandoned subscriptions.
//
// # Dependencies
//
// The package depends only on github.com/jackc/pgx/v5 and the standard library,
// so it can be lifted into its own repository. It never imports protodb or any
// other firetiger package. The single database seam is [DBTX] (a structural
// subset of pgx); *pgxpool.Pool, pgx.Tx, and *protodb.Tx all satisfy it, so an
// embedder composes an outbox by passing its own transaction to [Client.Publish]
// via [WithTransaction].
//
// # Multi-tenancy
//
// The core treats the tenant as an opaque namespace string, resolved per
// operation by the function passed to [WithNamespace] (default: the empty
// namespace — single tenant). An embedder that wants per-tenant isolation sets
// namespace = its tenant id and may additionally layer row-level security on the
// tables; the core SQL never names "organization".
//
// # Delivery semantics
//
// Delivery is at-least-once: a handler may run more than once for the same
// delivery if a worker dies after the handler succeeds but before the row is
// deleted. Handlers must therefore be idempotent. Within a single consumer,
// deliveries sharing a non-empty PartitionKey are processed strictly in publish
// order; distinct keys (and empty keys) run in parallel up to the consumer's
// Concurrency. Distinct consumers each receive their own copy of every matching
// publish.
package eventbus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Errors returned by the package.
var (
	// ErrNoHandler is returned by Run/RunOnce when a requested consumer name was
	// never registered.
	ErrNoHandler = errors.New("eventbus: no consumer registered with that name")
	// ErrEmptyTopic is returned by Publish when an event has no topic.
	ErrEmptyTopic = errors.New("eventbus: event topic is empty")
	// ErrNoSubscriber is returned by Publish when WithRequireSubscriber is set and
	// no consumer is subscribed to the event's topic.
	ErrNoSubscriber = errors.New("eventbus: no consumer subscribed to topic")
)

// DBTX is the package's only database seam: a structural subset of pgx that
// *pgxpool.Pool, pgx.Tx, and *protodb.Tx all satisfy. It imports pgx (an
// external dependency, fine for extraction) but nothing from firetiger.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// beginner is the optional capability to open a transaction. *pgxpool.Pool and
// pgx.Tx satisfy it; a bare DBTX may not.
type beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// begin opens a transaction on the client's pool. Standalone publishes and all
// draining need it; a publish that supplies its own tx via WithTransaction does
// not. It is the single place the begin-capability of db is resolved.
func (c *Client) begin(ctx context.Context) (pgx.Tx, error) {
	b, ok := c.db.(beginner)
	if !ok {
		return nil, errors.New("eventbus: configured DBTX cannot open transactions")
	}
	return b.Begin(ctx)
}

// sendBatcher is the optional capability to pipeline a batch of statements in one
// round-trip. *pgxpool.Pool and pgx.Tx satisfy it.
type sendBatcher interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// sendBatch runs the queued statements as one atomic, single-round-trip implicit
// transaction (pgx sends them with a single trailing Sync, so a failure rolls the
// whole batch back). Used to finalize a delivery and release its partition lock
// in one trip instead of an explicit BEGIN/Exec/Exec/COMMIT.
func (c *Client) sendBatch(ctx context.Context, b *pgx.Batch) error {
	sb, ok := c.db.(sendBatcher)
	if !ok {
		return errors.New("eventbus: configured DBTX cannot send batches")
	}
	return sb.SendBatch(ctx, b).Close()
}

// Event is a message handed to Publish. One Publish fans out to one Delivery per
// consumer subscribed to Topic.
type Event struct {
	// Topic is the logical channel the event is published to (required).
	Topic string
	// Type is an application-defined event type carried through to the handler.
	Type string
	// PartitionKey, when non-empty, serializes deliveries that share it within
	// each consumer (ordering). Empty means unordered/parallel.
	PartitionKey string
	// Payload is the opaque event body, stored as JSONB.
	Payload []byte
}

// Publisher is the publish side of a Client. Code that only enqueues should
// depend on this narrow interface rather than the concrete *Client, so it stays
// trivially mockable — see the eventbustest package for a ready-made double.
type Publisher interface {
	Publish(ctx context.Context, ev Event, opts ...PublishOption) error
	PublishBatch(ctx context.Context, evs []Event, opts ...PublishOption) error
}

var _ Publisher = (*Client)(nil)

// Delivery is one consumer's copy of a published event, handed to its Handler.
type Delivery struct {
	// ID is the unique, monotonically increasing delivery id.
	ID int64
	// Namespace is the tenant the delivery belongs to.
	Namespace string
	// Consumer is the queue (consumer name) this copy was fanned out to.
	Consumer string
	// Topic is the topic the event was originally published to.
	Topic string
	// Type is the application-defined event type.
	Type string
	// PartitionKey is the ordering key, or "" if unordered.
	PartitionKey string
	// Payload is the opaque event body.
	Payload []byte
	// Attempts is how many times this delivery has been claimed, including the
	// current attempt (starts at 1 in the handler).
	Attempts int
}

// Handler processes a single delivery. Returning nil completes the delivery
// (the row is deleted); returning a non-nil error reschedules it with backoff
// until MaxAttempts is reached, after which it is dead-lettered or marked failed.
type Handler func(ctx context.Context, d Delivery) error

// Consumer declares a drain: a queue (Name) bound to a Topic, with its Handler.
// Subscription and handler are inseparable here by construction, so a queue that
// nothing drains cannot be declared. Registering a consumer publishes its
// routing to the subscriptions table on the first Run/RunOnce.
type Consumer struct {
	// Name is the consumer's queue. Deliveries fanned out to this consumer carry
	// Consumer == Name; workers claim WHERE consumer = Name (required, unique).
	Name string
	// Topic is the published topic this consumer subscribes to (required).
	Topic string
	// Concurrency is the maximum number of deliveries this consumer processes at
	// once. Zero means 1.
	Concurrency int
	// MaxAttempts caps retries before a delivery is dead-lettered/failed. Zero
	// means DefaultMaxAttempts.
	MaxAttempts int
	// DeadLetter, when non-empty, is the consumer queue terminal failures are
	// copied to. Empty means terminal failures are marked failed in place.
	DeadLetter string
	// Handler processes each delivery (required).
	Handler Handler
}

// DefaultMaxAttempts is used when Consumer.MaxAttempts is zero.
const DefaultMaxAttempts = 20

// BackoffFunc maps an attempt number (1-based) to the delay before the next
// attempt.
type BackoffFunc func(attempt int) time.Duration

// DefaultBackoff is an exponential backoff capped at one hour.
func DefaultBackoff(attempt int) time.Duration {
	d := time.Second * time.Duration(1<<min(attempt, 12))
	if d > time.Hour {
		return time.Hour
	}
	return d
}

// Observer receives operational counters. The default is a no-op; an embedder
// wires this to its metrics system, keeping the core dependency-free.
type Observer interface {
	Count(ctx context.Context, metric string, attrs ...Attr)
}

// Attr is a single metric attribute (label).
type Attr struct{ Key, Value string }

type noopObserver struct{}

func (noopObserver) Count(context.Context, string, ...Attr) {}

// Client is the entry point: it publishes events and drains consumers against a
// single pgx pool. It is safe for concurrent use.
type Client struct {
	db DBTX // the pool used for own-transaction publish and for claim/drain

	namespace          func(ctx context.Context) (string, error)
	backoff            BackoffFunc
	logger             *slog.Logger
	obs                Observer
	leaseTimeout       time.Duration // how long before a held delivery/partition is assumed dead
	unrunRetention     time.Duration // how long undrained pending deliveries are kept
	subTTL             time.Duration // how long an unrefreshed subscription is kept
	partitionRetention time.Duration // how long an idle partition row is kept before reaping
	pollInterval       time.Duration // Run loop cadence
	claimBatchSize     int           // deliveries claimed per round-trip while draining
	requireSubAll      bool          // Publish errors if a topic has no subscribers
	owner              string        // identifies this client in locked_by columns (host:pid)

	schemaMu       sync.Mutex
	schemaPrepared atomic.Bool

	mu        sync.RWMutex
	consumers map[string]Consumer
}

// Option configures a Client.
type Option func(*Client)

// WithNamespace sets the resolver that maps a context to the tenant namespace
// used for every publish and claim. The default returns the empty namespace.
func WithNamespace(fn func(ctx context.Context) (string, error)) Option {
	return func(c *Client) { c.namespace = fn }
}

// WithBackoff sets the retry backoff schedule. The default is DefaultBackoff.
func WithBackoff(fn BackoffFunc) Option { return func(c *Client) { c.backoff = fn } }

// WithLogger sets the structured logger. The default is slog.Default().
func WithLogger(l *slog.Logger) Option { return func(c *Client) { c.logger = l } }

// WithObserver sets the metrics observer. The default is a no-op.
func WithObserver(o Observer) Option { return func(c *Client) { c.obs = o } }

// WithLeaseTimeout sets the single lease/visibility window: how long a claimed
// delivery (and the partition it holds) may be held before the holder is assumed
// dead — at which point the janitor returns the delivery to pending and frees the
// key. It must exceed the slowest handler, since there is no in-flight heartbeat
// yet (a long handler would otherwise be rescued and reprocessed). One window
// governs both the delivery and its partition so a crashed key cannot be
// reclaimed out of order. Default 15m.
func WithLeaseTimeout(d time.Duration) Option { return func(c *Client) { c.leaseTimeout = d } }

// WithPartitionRetention sets how long an idle partition row (unlocked, with no
// remaining deliveries) is kept before the janitor reaps it. Purely a churn knob
// — reaping is always safe because an in-use key still has deliveries. Default 1h.
func WithPartitionRetention(d time.Duration) Option {
	return func(c *Client) { c.partitionRetention = d }
}

// WithUnrunRetention sets how long an undrained pending delivery is kept before
// the janitor deletes it (the backstop for a queue nothing drains). Must exceed
// the longest consumer run interval and any expected drain outage. Default 24h.
func WithUnrunRetention(d time.Duration) Option { return func(c *Client) { c.unrunRetention = d } }

// WithSubscriptionTTL sets how long an unrefreshed subscription (and its
// deliveries) is kept before the janitor reaps it. Must exceed the longest
// consumer run interval. Default 24h.
func WithSubscriptionTTL(d time.Duration) Option { return func(c *Client) { c.subTTL = d } }

// WithPollInterval sets the Run loop cadence. Default 1s.
func WithPollInterval(d time.Duration) Option { return func(c *Client) { c.pollInterval = d } }

// WithClaimBatchSize sets how many deliveries a consumer claims (and then
// finalizes) per database round-trip while draining — the dominant throughput
// knob. It is a cap: under light load only the pending deliveries are claimed, so
// a large value does not add latency to single-event flows. Under a backlog,
// larger batches amortize the per-round-trip cost but mark more deliveries active
// at once (a crash before they run leaves them to the lease/rescue) and a batch's
// completions are not persisted until its slowest handler returns. The default
// (500) is the measured throughput knee; lower it for very slow handlers or
// tight per-message latency. Default 500.
func WithClaimBatchSize(n int) Option { return func(c *Client) { c.claimBatchSize = n } }

// WithRequireSubscriber makes Publish return ErrNoSubscriber when a topic has no
// subscribers, instead of dropping the event (and counting it). Off by default.
func WithRequireSubscriber() Option { return func(c *Client) { c.requireSubAll = true } }

// NewClient constructs a Client over the given pool (or any DBTX). The pool is
// used both to open its own transactions for standalone publishes and to claim
// and drain deliveries.
func NewClient(db DBTX, opts ...Option) *Client {
	c := &Client{
		db:                 db,
		namespace:          func(context.Context) (string, error) { return "", nil },
		backoff:            DefaultBackoff,
		logger:             slog.Default(),
		obs:                noopObserver{},
		leaseTimeout:       15 * time.Minute,
		unrunRetention:     24 * time.Hour,
		subTTL:             24 * time.Hour,
		partitionRetention: time.Hour,
		pollInterval:       time.Second,
		claimBatchSize:     500,
		consumers:          map[string]Consumer{},
	}
	for _, opt := range opts {
		opt(c)
	}
	host, _ := os.Hostname()
	c.owner = fmt.Sprintf("eventbus@%s:%d", host, os.Getpid())
	return c
}

// Register declares a consumer. It panics if the consumer is invalid (empty
// Name/Topic/Handler) or its Name duplicates an existing registration, since
// these are programmer errors detected at startup.
func (c *Client) Register(consumer Consumer) {
	switch {
	case consumer.Name == "":
		panic("eventbus: consumer Name is empty")
	case consumer.Topic == "":
		panic("eventbus: consumer Topic is empty")
	case consumer.Handler == nil:
		panic("eventbus: consumer Handler is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, dup := c.consumers[consumer.Name]; dup {
		panic("eventbus: duplicate consumer name " + consumer.Name)
	}
	if consumer.Concurrency <= 0 {
		consumer.Concurrency = 1
	}
	if consumer.MaxAttempts <= 0 {
		consumer.MaxAttempts = DefaultMaxAttempts
	}
	c.consumers[consumer.Name] = consumer
}

// consumersFor returns the registered consumers matching names, or all of them
// when names is empty. It errors if a requested name is unknown.
func (c *Client) consumersFor(names []string) ([]Consumer, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(names) == 0 {
		out := make([]Consumer, 0, len(c.consumers))
		for _, cons := range c.consumers {
			out = append(out, cons)
		}
		return out, nil
	}
	out := make([]Consumer, 0, len(names))
	for _, name := range names {
		cons, ok := c.consumers[name]
		if !ok {
			return nil, ErrNoHandler
		}
		out = append(out, cons)
	}
	return out, nil
}
