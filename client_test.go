package eventbus_test

import (
	"context"
	"testing"
	"time"

	"github.com/jpugliesi/eventbus"
	"github.com/stretchr/testify/require"
)

// RunOnce on an unregistered consumer name is a programmer error surfaced as
// ErrNoHandler.
func TestRunOnceUnknownConsumer(t *testing.T) {
	t.Parallel()
	c, _ := newClient(t)
	err := c.RunOnce(t.Context(), "ghost")
	require.ErrorIs(t, err, eventbus.ErrNoHandler)
}

// Publishing an event with no topic is rejected.
func TestPublishEmptyTopic(t *testing.T) {
	t.Parallel()
	c, _ := newClient(t)
	err := c.Publish(t.Context(), eventbus.Event{Type: "x"})
	require.ErrorIs(t, err, eventbus.ErrEmptyTopic)
}

// Register rejects invalid or duplicate consumers with a panic (startup-time
// programmer errors).
func TestRegisterValidation(t *testing.T) {
	t.Parallel()
	c, _ := newClient(t)
	noop := func(context.Context, eventbus.Delivery) error { return nil }

	require.Panics(t, func() { c.Register(eventbus.Consumer{Topic: "t", Handler: noop}) }, "empty name")
	require.Panics(t, func() { c.Register(eventbus.Consumer{Name: "n", Handler: noop}) }, "empty topic")
	require.Panics(t, func() { c.Register(eventbus.Consumer{Name: "n", Topic: "t"}) }, "nil handler")

	c.Register(eventbus.Consumer{Name: "dup", Topic: "t", Handler: noop})
	require.Panics(t, func() { c.Register(eventbus.Consumer{Name: "dup", Topic: "t", Handler: noop}) }, "duplicate name")
}

// The long-lived Run loop drains published work on its poll cadence and returns
// cleanly when its context is cancelled.
func TestRunDrainsThenCancels(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	c, pool := newClient(t, eventbus.WithPollInterval(10*time.Millisecond))
	c.Register(consumer("worker", "events", rec.handle(nil)))
	require.NoError(t, c.RegisterSubscriptions(t.Context()))
	require.NoError(t, c.Publish(t.Context(), eventbus.Event{Topic: "events", Type: "x"}))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Run drains the published delivery to completion on its poll cadence.
	require.Eventually(t, func() bool { return countDeliveries(t, pool, "worker") == 0 },
		2*time.Second, 10*time.Millisecond, "Run should drain the published delivery")
	require.Equal(t, 1, rec.count())

	// And returns cleanly once its context is cancelled.
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
