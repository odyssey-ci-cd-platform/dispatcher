// Package consumer subscribes one service to the event bus: a Redis
// Streams consumer group with at-least-once delivery, per ADR 0001.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/bus"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

const (
	// defaultMinIdle bounds how long an entry sits unacknowledged before
	// it may be claimed by another consumer.
	// another instance claims it (ADR 0001: ~5 minutes).
	defaultMinIdle = 5 * time.Minute

	// defaultMaxAttempts dead-letters an entry after this many failed
	// handling attempts (ADR 0001: poison messages go to odyssey:dead).
	defaultMaxAttempts = 5

	// readBackoff paces retries after a failed read, so a down Redis
	// cannot hot-loop the consumer (AUD-013).
	readBackoff = 2 * time.Second

	// defaultBlock paces the XREADGROUP long poll; short enough that a
	// context cancel stops Run promptly.
	defaultBlock = time.Second
)

// Handler processes one event. A nil error acknowledges the entry; a
// non-nil error leaves it pending for redelivery — handlers must be
// idempotent (ADR 0001: at-least-once).
type Handler func(ctx context.Context, event domain.Event) error

// Options configures one subscribing service.
type Options struct {
	// Group is the per-service consumer group name (required).
	Group string
	// Consumer names this instance within the group (required).
	Consumer string
	// Stream overrides the bus stream; default bus.StreamEvents.
	Stream string
	// MinIdle is the claim threshold for stuck entries; default 5m.
	MinIdle time.Duration
	// MaxAttempts dead-letters an entry after this many handling
	// attempts; default 5.
	MaxAttempts int
	// Block is the XREADGROUP long-poll window; default 1s.
	Block time.Duration
}

// Consumer consumes lifecycle events from the event bus on behalf of one
// service (one consumer group).
type Consumer struct {
	client  redis.UniversalClient
	handler Handler
	opts    Options
	logger  *slog.Logger
}

// New returns a Consumer for one service. Group and Consumer must be set
// and handler must not be nil.
func New(client redis.UniversalClient, handler Handler, opts Options, logger *slog.Logger) (*Consumer, error) {
	if client == nil {
		return nil, errors.New("consumer: nil redis client")
	}
	if handler == nil {
		return nil, errors.New("consumer: nil handler")
	}
	if opts.Group == "" {
		return nil, errors.New("consumer: group must not be empty")
	}
	if opts.Consumer == "" {
		return nil, errors.New("consumer: consumer must not be empty")
	}
	if opts.Stream == "" {
		opts.Stream = bus.StreamEvents
	}
	if opts.MinIdle == 0 {
		opts.MinIdle = defaultMinIdle
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = defaultMaxAttempts
	}
	if opts.Block == 0 {
		opts.Block = defaultBlock
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Consumer{client: client, handler: handler, opts: opts, logger: logger}, nil
}

// Run consumes until ctx is cancelled and returns nil on shutdown. It
// creates the consumer group if missing — starting from the beginning of
// the stream, so a service never misses events that predate its first
// deploy — then delivers each entry to the handler and acknowledges
// handled entries. Failed handlers stay pending for redelivery; transient
// Redis errors are logged and retried.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.ensureGroup(ctx); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		c.recoverStuck(ctx)
		res, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.opts.Group,
			Consumer: c.opts.Consumer,
			Streams:  []string{c.opts.Stream, ">"},
			Count:    10,
			Block:    c.opts.Block,
		}).Result()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, redis.Nil) {
				continue
			}
			c.logger.Warn("event read failed", "err", err)
			// Back off so a down Redis cannot hot-loop the group (AUD-013).
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(readBackoff):
			}
			continue
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				c.handle(ctx, msg)
			}
		}
	}
}

// ensureGroup creates the consumer group, tolerating an existing one.
func (c *Consumer) ensureGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.opts.Stream, c.opts.Group, "0").Err()
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

// recoverStuck retries entries whose handler failed long enough ago
// (MinIdle) by claiming them, and dead-letters entries whose attempts are
// exhausted (ADR 0001). The claim scan is cursor-driven, so recovery
// reaches every pending entry, not just the first page (AUD-013). Redis
// errors are logged, never fatal.
func (c *Consumer) recoverStuck(ctx context.Context) {
	start := "0-0"
	for {
		claimed, next, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: c.opts.Stream, Group: c.opts.Group, Consumer: c.opts.Consumer,
			MinIdle: c.opts.MinIdle, Start: start, Count: 64,
		}).Result()
		if err != nil {
			if ctx.Err() == nil {
				c.logger.Warn("entry claim scan failed", "err", err)
			}
			return
		}
		for _, msg := range claimed {
			c.recoverOne(ctx, msg)
		}
		if next == "0-0" {
			return
		}
		start = next
	}
}

// recoverOne handles one claimed entry: dead-letter it when its attempts
// are exhausted, else hand it back to the handler. XAUTOCLAIM bumped the
// delivery count, which XPENDING reports as the retry count — no separate
// counter needed (ADR 0001).
func (c *Consumer) recoverOne(ctx context.Context, msg redis.XMessage) {
	pending, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: c.opts.Stream, Group: c.opts.Group, Start: msg.ID, End: msg.ID, Count: 1,
	}).Result()
	if err != nil {
		c.logger.Warn("pending check failed", "id", msg.ID, "err", err)
		return
	}
	// The claim bumped the delivery count, so the count already includes
	// the delivery being recovered; dead-letter only when handling has
	// actually exceeded MaxAttempts (gh-76 keeps the three-attempts contract).
	if len(pending) == 1 && pending[0].RetryCount > int64(c.opts.MaxAttempts) {
		c.deadLetter(ctx, msg.ID)
		return
	}
	c.handle(ctx, msg)
}

// deadLetterEntry copies one raw envelope to the dead-letter stream and
// acknowledges the entry, used for entries that can never succeed
// (AUD-013). Failures are logged; the entry stays pending and the regular
// recovery path retries the dead-letter later.
func (c *Consumer) deadLetterEntry(ctx context.Context, id, raw string) {
	if err := c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: bus.StreamDead,
		Values: map[string]any{"envelope": raw, "source_id": id},
	}).Err(); err != nil {
		c.logger.Warn("dead-letter write failed", "id", id, "err", err)
		return
	}
	if err := c.client.XAck(ctx, c.opts.Stream, c.opts.Group, id).Err(); err != nil {
		c.logger.Warn("dead-letter ack failed", "id", id, "err", err)
		return
	}
	c.logger.Warn("event dead-lettered", "id", id)
}

// deadLetter copies an exhausted entry's envelope to the dead-letter
// stream and acknowledges it, so one poison event cannot wedge the group
// (ADR 0001). An ack failure re-runs the whole pass later: at-least-once
// means a duplicate dead-letter entry is possible, never a lost one.
func (c *Consumer) deadLetter(ctx context.Context, id string) {
	entries, err := c.client.XRange(ctx, c.opts.Stream, id, id).Result()
	if err != nil {
		c.logger.Warn("dead-letter read failed", "id", id, "err", err)
		return
	}
	if len(entries) == 1 {
		raw, _ := entries[0].Values["envelope"].(string)
		if err := c.client.XAdd(ctx, &redis.XAddArgs{
			Stream: bus.StreamDead,
			Values: map[string]any{"envelope": raw, "source_id": id},
		}).Err(); err != nil {
			c.logger.Warn("dead-letter write failed", "id", id, "err", err)
			return
		}
	}
	if err := c.client.XAck(ctx, c.opts.Stream, c.opts.Group, id).Err(); err != nil {
		c.logger.Warn("dead-letter ack failed", "id", id, "err", err)
		return
	}
	c.logger.Warn("event dead-lettered", "id", id)
}

// handle parses one stream entry and processes it, acknowledging on
// success. A parse or handler failure leaves the entry pending — it is
// retried through the claim path — and is logged, never fatal.
func (c *Consumer) handle(ctx context.Context, msg redis.XMessage) {
	raw, ok := msg.Values["envelope"].(string)
	if !ok {
		// An entry with no envelope can never parse; dead-letter it now
		// instead of cycling claims forever (AUD-013).
		c.logger.Warn("event envelope missing", "id", msg.ID)
		c.deadLetterEntry(ctx, msg.ID, "")
		return
	}
	var event domain.Event
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		// Same for an envelope that does not parse (AUD-013).
		c.logger.Warn("event envelope malformed", "id", msg.ID, "err", err)
		c.deadLetterEntry(ctx, msg.ID, raw)
		return
	}
	if err := c.handler(ctx, event); err != nil {
		c.logger.Warn("event handling failed", "id", msg.ID, "type", event.Type, "err", err)
		return
	}
	if err := c.client.XAck(ctx, c.opts.Stream, c.opts.Group, msg.ID).Err(); err != nil {
		c.logger.Warn("event ack failed", "id", msg.ID, "err", err)
	}
}
