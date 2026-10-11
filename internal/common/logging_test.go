package common_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/common"
)

func TestContextLogger(t *testing.T) {
	fallback := slog.New(slog.DiscardHandler)
	attached := slog.New(slog.DiscardHandler)

	t.Run("round-trips the attached logger", func(t *testing.T) {
		ctx := common.ContextWithLogger(context.Background(), attached)
		if got := common.LoggerFromContext(ctx, fallback); got != attached {
			t.Error("LoggerFromContext did not return the attached logger")
		}
	})

	t.Run("falls back when no logger attached", func(t *testing.T) {
		if got := common.LoggerFromContext(context.Background(), fallback); got != fallback {
			t.Error("LoggerFromContext did not return the fallback logger")
		}
	})
}
