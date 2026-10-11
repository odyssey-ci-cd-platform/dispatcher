// events-logger is the reference event bus consumer: it subscribes one
// consumer group and prints every lifecycle event it handles. It exists
// to prove the fan-out mechanics end to end (RUNBOOK §1.5); the real
// services — logging, notification, viz — replace it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/consumer"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

func main() {
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	group := flag.String("group", "events-logger", "consumer group name (one per service)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	client := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer client.Close()

	c, err := consumer.New(client, printEvent, consumer.Options{
		Group:    *group,
		Consumer: instanceName(),
	}, logger)
	if err != nil {
		logger.Error("consumer setup failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := c.Run(ctx); err != nil {
		logger.Error("consumer stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("events-logger stopped")
}

// printEvent renders one handled event as a single line on stdout.
func printEvent(_ context.Context, e domain.Event) error {
	fmt.Printf("type=%s pipeline=%s job=%s step=%s\n", e.Type, e.Pipeline, e.Job, e.Step)
	return nil
}

// instanceName names this replica within the group: host and pid.
func instanceName() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
