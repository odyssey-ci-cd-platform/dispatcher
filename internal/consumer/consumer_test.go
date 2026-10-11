package consumer_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/bus"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/consumer"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

// publish adds one event to the bus stream via the real publisher.
func publish(t *testing.T, client redis.UniversalClient, event domain.Event) {
	t.Helper()
	if err := bus.New(client).Publish(context.Background(), event); err != nil {
		t.Fatalf("publish %s: %v", event.Type, err)
	}
}

// collector records handled events with a Done channel per event.
type collector struct {
	mu      sync.Mutex
	events  []domain.Event
	seen    chan struct{}
	handler func(e domain.Event) error
}

func newCollector(n int) *collector {
	return &collector{seen: make(chan struct{}, n)}
}

func (c *collector) handle(_ context.Context, e domain.Event) error {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
	if c.handler != nil {
		if err := c.handler(e); err != nil {
			return err
		}
	}
	c.seen <- struct{}{}
	return nil
}

func (c *collector) recorded() []domain.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]domain.Event(nil), c.events...)
}

func TestConsumerDeliversAndAcks(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	// Events published before the consumer starts must all be delivered,
	// in stream order.
	for _, typ := range []string{"test.a", "test.b", "test.c"} {
		publish(t, client, domain.Event{Type: typ, Pipeline: "p"})
	}

	col := newCollector(3)
	c, err := consumer.New(client, col.handle, consumer.Options{Group: "svc", Consumer: "c1"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(runCtx) }()

	var got []string
	for i := 0; i < 3; i++ {
		select {
		case <-col.seen:
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for event %d; handled %v", i+1, col.recorded())
		}
	}
	for _, e := range col.recorded() {
		got = append(got, e.Type)
	}
	want := []string{"test.a", "test.b", "test.c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivered %v, want %v", got, want)
		}
	}

	// Successful handling must ack every entry.
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: bus.StreamEvents, Group: "svc", Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("XPendingExt: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending entries after successful handling = %d, want 0", len(pending))
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned error on shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

// TestConsumerHandlerFailureStaysPending asserts the at-least-once
// contract: a handler error must leave the entry pending (no ack) so it
// can be retried (ADR 0001).
func TestConsumerHandlerFailureStaysPending(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	publish(t, client, domain.Event{Type: "test.poison", Pipeline: "p"})

	attempts := make(chan struct{}, 4)
	handler := func(context.Context, domain.Event) error {
		attempts <- struct{}{}
		return errors.New("handler boom")
	}
	c, err := consumer.New(client, handler, consumer.Options{Group: "svc", Consumer: "c1"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(runCtx) }()

	select {
	case <-attempts:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never called")
	}
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned error on shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}

	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: bus.StreamEvents, Group: "svc", Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("XPendingExt: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending entries after failed handling = %d, want 1 (at-least-once)", len(pending))
	}
}

// TestConsumerCatchUpAfterRestart asserts the group cursor persists
// across restarts: a fresh instance of the same service receives events
// published while it was down, and does not redeliver acked entries.
func TestConsumerCatchUpAfterRestart(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	publish(t, client, domain.Event{Type: "test.first", Pipeline: "p"})

	col1 := newCollector(1)
	c1, err := consumer.New(client, col1.handle, consumer.Options{Group: "svc", Consumer: "c1"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = c1.Run(runCtx) }()
	select {
	case <-col1.seen:
	case <-time.After(5 * time.Second):
		t.Fatal("first consumer never received the event")
	}
	cancel()

	// Published while the service is down.
	publish(t, client, domain.Event{Type: "test.second", Pipeline: "p"})
	publish(t, client, domain.Event{Type: "test.third", Pipeline: "p"})

	col2 := newCollector(2)
	c2, err := consumer.New(client, col2.handle, consumer.Options{Group: "svc", Consumer: "c2"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runCtx2, cancel2 := context.WithCancel(ctx)
	go func() { _ = c2.Run(runCtx2) }()
	for i := 0; i < 2; i++ {
		select {
		case <-col2.seen:
		case <-time.After(5 * time.Second):
			t.Fatalf("restart catch-up timed out; handled %v", col2.recorded())
		}
	}
	cancel2()

	var got []string
	for _, e := range col2.recorded() {
		got = append(got, e.Type)
	}
	want := []string{"test.second", "test.third"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("catch-up delivered %v, want %v (no redelivery of acked entries)", got, want)
		}
	}
}

// TestConsumerDeadLettersAfterMaxAttempts asserts ADR 0001's recovery
// shape: a poison event is retried through claims (MinIdle), and once its
// attempts are exhausted it lands in the dead-letter stream with its
// envelope intact and leaves the group pending list empty — one poison
// event must not wedge the group.
func TestConsumerDeadLettersAfterMaxAttempts(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	publish(t, client, domain.Event{Type: "test.poison", Pipeline: "p"})

	attempts := make(chan struct{}, 16)
	handler := func(context.Context, domain.Event) error {
		attempts <- struct{}{}
		return errors.New("poison")
	}
	c, err := consumer.New(client, handler, consumer.Options{
		Group: "svc", Consumer: "c1",
		MinIdle: 10 * time.Millisecond, MaxAttempts: 3, Block: 50 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(runCtx) }()

	// Initial delivery + two claim retries = three failed attempts.
	for i := 0; i < 3; i++ {
		select {
		case <-attempts:
		case <-time.After(5 * time.Second):
			t.Fatalf("attempt %d never happened; poison wedged the group", i+1)
		}
	}

	// The dead-letter pass must land the envelope in odyssey:dead.
	deadline := time.Now().Add(5 * time.Second)
	for {
		n, err := client.XLen(ctx, bus.StreamDead).Result()
		if err == nil && n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dead-letter stream did not receive the poison event (len err: %v)", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	dead, err := client.XRange(ctx, bus.StreamDead, "-", "+").Result()
	if err != nil || len(dead) != 1 {
		t.Fatalf("dead-letter range: %v (%d entries)", err, len(dead))
	}
	if env, _ := dead[0].Values["envelope"].(string); !strings.Contains(env, "test.poison") {
		t.Errorf("dead-letter envelope = %q, want the original event JSON", env)
	}

	// Acking the dead-lettered entry unwedges the group.
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: bus.StreamEvents, Group: "svc", Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("XPendingExt: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending entries after dead-letter = %d, want 0", len(pending))
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned error on shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

func TestConsumerNewValidatesOptions(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	if _, err := consumer.New(client, func(context.Context, domain.Event) error { return nil }, consumer.Options{}, nil); err == nil {
		t.Error("New with empty Options: want error for missing group, got nil")
	}
	if _, err := consumer.New(client, nil, consumer.Options{Group: "g", Consumer: "c"}, nil); err == nil {
		t.Error("New with nil handler: want error, got nil")
	}
}

// TestConsumerDeadLettersMalformedImmediately asserts an entry whose
// envelope can never parse lands in odyssey:dead on first sight instead of
// cycling through MaxAttempts claims (AUD-013).
func TestConsumerDeadLettersMalformedImmediately(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	// Raw entry: the envelope value is not JSON, so handling can never
	// succeed. Default MinIdle (5m) means the old claim-cycle path would
	// take about 25 minutes to dead-letter it.
	if err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: bus.StreamEvents,
		Values: map[string]any{"envelope": "{not json"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	handler := func(context.Context, domain.Event) error { return nil }
	c, err := consumer.New(client, handler, consumer.Options{
		Group: "svc", Consumer: "c1", Block: 50 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(runCtx) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		n, err := client.XLen(ctx, bus.StreamDead).Result()
		if err == nil && n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("malformed entry not dead-lettered within 2s (dead len err: %v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: bus.StreamEvents, Group: "svc", Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("XPendingExt: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending entries after immediate dead-letter = %d, want 0", len(pending))
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned error on shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

// countingClient fails the first three XReadGroup calls, then delegates,
// counting every read to expose hot-looping (AUD-013).
type countingClient struct {
	redis.UniversalClient
	mu    sync.Mutex
	calls int
}

func (c *countingClient) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	// Every read fails: the point is to expose what the error path does —
	// hot-loop or back off (AUD-013).
	cmd := redis.NewXStreamSliceCmd(ctx)
	cmd.SetErr(errors.New("redis unavailable"))
	return cmd
}

func (c *countingClient) readCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// TestConsumerBacksOffWhenRedisUnavailable asserts read errors back off
// instead of hot-looping (AUD-013): a 300ms window must yield a handful of
// read attempts, not hundreds.
func TestConsumerBacksOffWhenRedisUnavailable(t *testing.T) {
	mr := miniredis.RunT(t)
	cc := &countingClient{UniversalClient: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()

	handler := func(context.Context, domain.Event) error { return nil }
	c, err := consumer.New(cc, handler, consumer.Options{
		Group: "svc", Consumer: "c1", Block: 50 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_ = c.Run(runCtx)

	if calls := cc.readCalls(); calls > 20 {
		t.Errorf("XReadGroup attempted %d times in 300ms — read errors hot-loop", calls)
	}
}

// TestConsumerRecoveryWalksBeyondFirstPage asserts recovery reaches every
// pending entry — the old pass inspected only the first 64 (AUD-013).
func TestConsumerRecoveryWalksBeyondFirstPage(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	const total = 70
	for i := 0; i < total; i++ {
		publish(t, client, domain.Event{Type: "test.recovery", Pipeline: "p"})
	}

	fail := true
	handler := func(context.Context, domain.Event) error {
		if fail {
			return errors.New("not yet")
		}
		return nil
	}
	c, err := consumer.New(client, handler, consumer.Options{
		Group: "svc", Consumer: "c1",
		MinIdle: time.Millisecond, MaxAttempts: 1000, Block: 20 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(runCtx) }()

	// Wait until the consumer has created the group and delivered the
	// entries before measuring recovery.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: bus.StreamEvents, Group: "svc", Start: "-", End: "+", Count: 200,
		}).Result()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer group never appeared: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Give recovery passes time to touch all entries, including those
	// beyond the first 64.
	retried := 0
	for time.Now().Before(deadline) {
		pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: bus.StreamEvents, Group: "svc", Start: "-", End: "+", Count: 200,
		}).Result()
		if err != nil {
			t.Fatalf("XPendingExt: %v", err)
		}
		retried = 0
		for _, p := range pending {
			if p.RetryCount >= 2 {
				retried++
			}
		}
		if retried == total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if retried != total {
		t.Errorf("only %d of %d entries were retried past the first delivery — recovery stops at the 64-entry page", retried, total)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned error on shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}
