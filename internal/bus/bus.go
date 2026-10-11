// Package bus publishes lifecycle events to the event bus: a single Redis
// Stream, per ADR 0001.
package bus

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

const (
	// StreamEvents is the single stream all lifecycle events land on (ADR 0001).
	StreamEvents = "odyssey:events"

	// StreamDead receives events whose handling attempts were exhausted
	// (ADR 0001); ops inspects it manually.
	StreamDead = "odyssey:dead"

	// maxStreamLen bounds retention via approximate XADD trimming; the
	// results DB owns history, nothing replays from the bus (ADR 0001).
	maxStreamLen = 100000
)

// Publisher writes events onto the Redis Stream event bus.
type Publisher struct {
	client redis.UniversalClient
}

// New returns a Publisher that issues XADDs through client.
func New(client redis.UniversalClient) *Publisher {
	return &Publisher{client: client}
}

// Publish writes one event as a JSON envelope and returns any Redis error.
// Degradation policy is the caller's: the orchestrator logs emission
// failures and never fails a run over them (ADR 0001).
func (p *Publisher) Publish(ctx context.Context, event domain.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamEvents,
		MaxLen: maxStreamLen,
		Approx: true,
		Values: map[string]any{"envelope": string(body)},
	}).Err()
}
