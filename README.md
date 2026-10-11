# Dispatcher

**Author:** Sarthak
**Date:** 2026-10-11 (platform content moved out; this README now covers only this repository)

## Overview

Dispatcher is a language-agnostic, configuration-driven CI/CD execution engine written in Go, exposed over a gRPC interface.

Given a resolved pipeline config, it runs jobs and steps, manages one container per job (Docker SDK: a long-running `tail -f /dev/null` container with each step executed via `exec`), captures stdout/stderr separately, and returns structured results.

It has no knowledge of orchestrators, queues, or the hosted platform — it is a pure execution library behind the gRPC surface, and a passive request/response executor: lifecycle concerns (heartbeats, retries, result streaming) belong to the caller, not the engine (gh-16).

Platform-level design is maintained outside this repository, currently as `odyssey-design.md` in the local odyssey workspace directory next to the checkouts.

## Design principles

- Minimal abstractions. Readable over micro-optimized.
- Functions over classes when no state is needed.
- Language-agnostic: no hardcoded toolchain assumptions baked into the engine.
- Config lives in a `.odyssey/` folder per project, in TOML.
- Prefer direct, slightly repetitive call sites over an abstraction that doesn't earn its weight.
- Schema fields should be minimal and orthogonal; contradictory combinations get rejected at validation time, not silently accepted.
- Fail-fast between stages: the first Failed or Errored stage ends the run, and later stages are skipped — recorded decision (gh-82, AUD-015).

## Architecture

### Run flow

- A pipeline is defined in `.odyssey/pipeline.toml`, loaded and validated by the config package.
- The orchestrator runs stages sequentially and jobs within a stage concurrently.
- The runner manages one Docker container per job and executes each step as a fresh shell invocation via `docker exec`.
- Results flow back as structured `JobResult` values with `Status` precedence applied.
- The gRPC server exposes the engine; when enabled, lifecycle events fan out to the event bus and a durable run history lands in the results database.

### Packages

- `internal/config` — TOML pipeline/env config loading (`.odyssey/pipeline.toml`, `.odyssey/env.toml`).
- `internal/domain` — core types: `Pipeline`, `JobResult`, `Status` (Error > Failed > Pending > Passed precedence).
- `internal/orchestrator` — runs stages sequentially, jobs within a stage concurrently.
- `internal/runner` — Docker execution (`docker.go`), cross-step env-file convention (`envfile.go`, `$ODYSSEY_ENV_FILE`).
- `internal/server` — gRPC server exposing the execution engine.
- `internal/bus` — event bus client and emission (Redis Streams, ADR 0001).
- `internal/results` — results recorder writing run/job/step history (embedded SQLite, ADR 0002).
- `internal/consumer` — event consumers (e.g. `events-logger`).
- `internal/common` — logging, shared utils.
- `api/v1/odyssey.proto` / `gen/proto/v1` — gRPC service definition and generated code (generated, never hand-edit `gen/`).

### Server environment variables

- `ODYSSEY_ADDR` — the address the gRPC server binds, used verbatim (`:50051`, `host:50051`). Unset, the server binds `localhost:50051`, so an unconfigured server is not reachable from the network (AUD-005).
- `ODYSSEY_PROJECT_ROOT` — when set, every request's `project_path` must resolve under this directory; requests outside it are rejected. Unset, paths are unrestricted, which is the local-development posture (AUD-005).
- `ODYSSEY_REDIS_ADDR` — enables the event bus when set; without it the server runs with the bus disabled (ADR 0001).
- `ODYSSEY_RESULTS_DB` — the results database file, used when the bus is enabled; the server's in-process recorder is the only writer (ADR 0002). Unset, `odyssey-results.db` in the working directory. An unopenable database fails startup: a server that cannot record history must not pretend it will.

## Execution model

- Each step (`exec_run`) is a fresh shell invocation inside the job's container; shell state does **not** persist between steps. Only filesystem state persists.
- Cross-step environment variable persistence uses the env-file convention: a step appends `KEY=value` lines to the file exposed as `$ODYSSEY_ENV_FILE` (`/tmp/.odyssey_env`), and subsequent steps receive those vars — modeled on GitHub Actions' `$GITHUB_ENV`. Later values win, matching shell append order.
- Stderr is **not** reliably "the error" — many tools write diagnostics to stderr on success. Execution returns exit code, stdout, stderr, and error as four separate values rather than conflating stderr with failure.
- Docker already handles local image caching; caching container instances is unnecessary.
- Deferred cleanup (container teardown) uses an independent context — `context.WithoutCancel(ctx)` plus its own `context.WithTimeout` — so a cancelled or timed-out parent context doesn't silently leak containers.

## Result model

- `StatusErrored` (infrastructure failure: setup/teardown broke) is distinct from `StatusFailed` (the step ran and exited nonzero) and `StatusPending`.
- `JobResult` carries a `SetupErr` field for infra failures specifically.
- Aggregation uses a `worstStatus` helper with precedence Error > Failed > Pending > Passed.

## Event bus vs. results DB

Deliberately two sinks with different semantics, not one box doing both jobs:

- Event bus: ephemeral, multi-consumer, real-time. Decided: Redis Streams for v1 — see [ADR 0001](docs/adr/0001-event-bus-technology.md) for the trade-off analysis (Kafka rejected as ops-heavy; NATS JetStream kept as the growth path).
- Results DB: durable, queryable, the source of truth for history over time. Decided: embedded SQLite for v1 — see [ADR 0002](docs/adr/0002-results-db-technology.md) for the trade-off analysis (Postgres deferred as the documented growth path; a single results-recorder consumer is the only writer).

The end-to-end smoke for both sinks is `scripts/e2e-event-bus.sh` (see RUNBOOK §1.5).

## Roadmap

### Mid-term

- `allowed_hosts` egress enforcement (DNS-filtering proxy; schema already settled, implementation deferred).

### Later / explicitly deferred

- `services` block: container-to-container networking, background processes.
- DAG-based job dependencies.

## Tech stack

- **Language**: Go (1.26+, uses `context.WithoutCancel`).
- **Containerization**: Docker SDK — long-running `tail -f /dev/null` container per job, `exec` for steps, `put_archive` for file copying.
- **Config format**: TOML (`.odyssey/pipeline.toml`, `.odyssey/env.toml`).
- **Interface**: gRPC (`api/v1/odyssey.proto`).
- **Event bus**: Redis Streams (ADR 0001); evaluated against Kafka and NATS JetStream.
- **Results DB**: embedded SQLite (ADR 0002); Postgres documented as the growth path.
