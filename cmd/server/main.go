package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	odysseyv1 "github.com/odyssey-ci-cd-platform/dispatcher/gen/proto/v1"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/bus"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/consumer"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/orchestrator"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/results"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/runner"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/server"
)

// listenAddr returns the address the gRPC server binds. It defaults to
// localhost so an unconfigured server is not reachable from the network;
// ODYSSEY_ADDR is used verbatim, so both ":6000" and "host:6000" work
// (AUD-005).
func listenAddr(env string) string {
	if env != "" {
		return env
	}
	return "localhost:50051"
}

func main() {
	logger := newLogger()

	if err := godotenv.Load(); err != nil {
		logger.Warn(".env file not found, skipping", "error", err)
	}

	addr := listenAddr(os.Getenv("ODYSSEY_ADDR"))

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to listen", "addr", addr, "error", err)
		os.Exit(1)
	}

	// ADR 0001: the event bus is optional; the engine stays runnable
	// without Redis. Publish failures surface per event, logged never fatal.
	// ADR 0002: with the bus enabled, the results recorder runs in-process
	// and is the results database's only writer; it fails startup loudly —
	// a server that cannot record history must not pretend it will.
	var events orchestrator.EventSink
	if redisAddr := os.Getenv("ODYSSEY_REDIS_ADDR"); redisAddr != "" {
		redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
		defer redisClient.Close()
		events = bus.New(redisClient)
		logger.Info("event bus enabled", "redis", redisAddr)

		resultsPath := os.Getenv("ODYSSEY_RESULTS_DB")
		if resultsPath == "" {
			resultsPath = "odyssey-results.db"
		}
		resultsStore, err := results.Open(resultsPath)
		if err != nil {
			logger.Error("failed to open results database", "path", resultsPath, "error", err)
			os.Exit(1)
		}
		defer resultsStore.Close()

		recorder, err := consumer.New(redisClient, resultsStore.Record, consumer.Options{
			Group:    "results-recorder",
			Consumer: instanceName(),
		}, logger)
		if err != nil {
			logger.Error("failed to build results recorder", "error", err)
			os.Exit(1)
		}
		recorderCtx, recorderStop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer recorderStop()
		go func() {
			if err := recorder.Run(recorderCtx); err != nil {
				logger.Error("results recorder stopped", "error", err)
			}
		}()
		logger.Info("results recorder enabled", "db", resultsPath)
	} else {
		logger.Info("event bus disabled", "hint", "set ODYSSEY_REDIS_ADDR to enable")
	}

	// The runner is process-scoped: one client for the server's lifetime,
	// closed on shutdown (AUD-014).
	dockerRunner, err := runner.NewDockerRunner(logger)
	if err != nil {
		logger.Error("failed to create docker runner", "error", err)
		os.Exit(1)
	}
	defer dockerRunner.Close()

	grpcServer := grpc.NewServer()
	odysseyv1.RegisterOdysseyServiceServer(grpcServer, &server.Server{Logger: logger, Events: events, Runner: dockerRunner})

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		logger.Info("shutting down", "signal", sig.String())
		grpcServer.GracefulStop()
	}()

	logger.Info("server listening", "addr", addr)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

// instanceName names this replica within a consumer group: host and pid.
func instanceName() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// newLogger returns a structured logger whose format and level are
// controlled by ODYSSEY_ENV and ODYSSEY_LOG_FORMAT.
//
//	ODYSSEY_ENV=production  → JSON, Info  level
//	otherwise               → Text, Debug level
//
// ODYSSEY_LOG_FORMAT overrides the format: "text" or "json".
func newLogger() *slog.Logger {
	level := slog.LevelDebug
	if os.Getenv("ODYSSEY_ENV") == "production" {
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	// Both formats log to stderr; stdout stays reserved for data.
	if os.Getenv("ODYSSEY_LOG_FORMAT") == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}
