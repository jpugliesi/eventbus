package eventbustest_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpugliesi/eventbus"
	"github.com/jpugliesi/eventbus/eventbustest"
	"github.com/jpugliesi/eventbus/internal/pgtest"
	"github.com/stretchr/testify/require"
)

var testContainer *pgtest.Container

func TestMain(m *testing.M) {
	var cleanup func()
	if c, cl, err := pgtest.SetupContainer(context.Background()); err == nil {
		testContainer, cleanup = c, cl
	}
	code := m.Run()
	if cleanup != nil {
		cleanup()
	}
	os.Exit(code)
}

var errBoom = errors.New("boom")

func TestRecordingPublisher(t *testing.T) {
	t.Parallel()
	var p eventbustest.RecordingPublisher
	require.NoError(t, p.Publish(context.Background(), eventbus.Event{Topic: "t", Type: "a"}))
	require.NoError(t, p.PublishBatch(context.Background(), []eventbus.Event{{Topic: "t", Type: "b"}, {Topic: "t", Type: "c"}}))

	got := p.Events()
	require.Len(t, got, 3)
	require.Equal(t, []string{"a", "b", "c"}, []string{got[0].Type, got[1].Type, got[2].Type})
}

func TestRecordingPublisherError(t *testing.T) {
	t.Parallel()
	p := eventbustest.RecordingPublisher{Err: errBoom}
	require.ErrorIs(t, p.Publish(context.Background(), eventbus.Event{Topic: "t"}), errBoom)
	require.ErrorIs(t, p.PublishBatch(context.Background(), []eventbus.Event{{Topic: "t"}}), errBoom)
	require.Empty(t, p.Events())
}

// TestPoolHelpers is the usage example for the DB-backed helpers: MustClient +
// Drain + the Count* inspectors against a real Postgres pool.
func TestPoolHelpers(t *testing.T) {
	if testContainer == nil {
		t.Skip("postgres container unavailable")
	}
	db := testContainer.StartTestDatabaseWithoutMigrations(t)
	pool, err := pgxpool.New(context.Background(), db.AdminConnectionURL())
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	c := eventbustest.MustClient(t, pool)

	var handled atomic.Int64
	c.Register(eventbus.Consumer{
		Name:  "worker",
		Topic: "events",
		Handler: func(context.Context, eventbus.Delivery) error {
			handled.Add(1)
			return nil
		},
	})
	require.NoError(t, c.RegisterSubscriptions(context.Background()))
	require.Equal(t, 1, eventbustest.CountSubscriptions(t, pool, "events"))

	require.NoError(t, c.Publish(context.Background(), eventbus.Event{Topic: "events", Type: "ping", Payload: []byte(`{}`)}))
	require.Equal(t, 1, eventbustest.CountDeliveries(t, pool, "worker"))

	eventbustest.Drain(t, c, "worker")
	require.Equal(t, int64(1), handled.Load())
	require.Equal(t, 0, eventbustest.CountDeliveries(t, pool, "worker")) // deleted on completion
}
