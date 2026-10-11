package bus_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/bus"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

func TestPublisherPublishesJSONEnvelope(t *testing.T) {
	// Requirement (gh-52, ADR 0001): a published event lands on the
	// odyssey:events stream as a JSON envelope whose fields round-trip.
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	occurred := time.Now().UTC().Truncate(time.Millisecond)
	event := domain.Event{
		Type:       domain.EventJobFinished,
		OccurredAt: occurred,
		Pipeline:   "shop",
		Job:        "build-image",
		Step:       "lint",
		Payload:    map[string]string{"status": domain.StatusPassed.String()},
	}

	ctx := context.Background()
	pub := bus.New(client)
	if err := pub.Publish(ctx, event); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	entries, err := client.XRange(ctx, bus.StreamEvents, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d stream entries, want 1", len(entries))
	}
	if entries[0].ID == "" {
		t.Error("stream entry ID empty; the entry ID is the event's id per ADR 0001")
	}

	raw, ok := entries[0].Values["envelope"].(string)
	if !ok {
		t.Fatalf("entry values = %v, want an envelope field", entries[0].Values)
	}
	var got domain.Event
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("envelope is not valid JSON: %v", err)
	}
	if got.Type != event.Type {
		t.Errorf("type = %q, want %q", got.Type, event.Type)
	}
	if !got.OccurredAt.Equal(event.OccurredAt) {
		t.Errorf("occurred_at = %v, want %v", got.OccurredAt, event.OccurredAt)
	}
	if got.Pipeline != event.Pipeline {
		t.Errorf("pipeline = %q, want %q", got.Pipeline, event.Pipeline)
	}
	if got.Job != event.Job {
		t.Errorf("job = %q, want %q", got.Job, event.Job)
	}
	if got.Step != event.Step {
		t.Errorf("step = %q, want %q", got.Step, event.Step)
	}
	if got.Payload["status"] != event.Payload["status"] {
		t.Errorf("payload status = %q, want %q", got.Payload["status"], event.Payload["status"])
	}
}
