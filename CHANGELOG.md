# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Format

- Latest changes go at the top, under `[Unreleased]`, until a version is tagged.
- Group every entry under one of these headings, in this order:

  - **Added** — new features.
  - **Changed** — behavior changes to existing features.
  - **Deprecated** — soon-to-be-removed features.
  - **Removed** — removed features.
  - **Fixed** — bug fixes.
  - **Security** — vulnerability fixes.

- One entry per change: `- <summary>.` — sentence case, past tense.
- Reference the issue when one exists: `- <summary> (gh-<N>).`
- Only headings with entries appear in a release. Empty headings are omitted.
- Commits and branches keep the `gh-<N>` convention; entries summarize, not enumerate commits.

## [Unreleased]

### Added
- Second recorded repository audit, evaluated at ed81aa0 (gh-90).
- Results recording: with the bus enabled, the server runs an in-process `results-recorder` consumer — the only writer of the embedded SQLite results database (`ODYSSEY_RESULTS_DB`, default `odyssey-results.db`), WAL mode, migrations applied on startup, idempotent under at-least-once redelivery; events missing run identity dead-letter (gh-85).
- `internal/results`: SQLite store behind goose embedded migrations with normalized `runs`/`jobs`/`steps` and a JSON payload column reserved on `steps` (gh-85).
- Lifecycle events carry a run ID and finished events survive run cancellation (gh-67).
- Repository audit process (`audit/PROCESS.md`) and the first recorded audit, evaluated at 7d7be54 (gh-60).
- gRPC contract: step results carry stdout, stderr, error text, and duration separately; job results carry error and duration; stage, pipeline, and response carry durations (gh-66).
- Results DB technology decision record: embedded SQLite chosen for v1, normalized schema, single results-recorder writer, direct read access for analytics services (gh-17).
- Fan-out consumer skeleton: `internal/consumer` with consumer groups, at-least-once delivery, claim recovery, and dead-lettering to `odyssey:dead`, plus an `events-logger` reference consumer (gh-17).
- Step started/finished events emitted by the runner during job execution, payload carrying status only (gh-52).
- End-to-end event bus smoke script (`scripts/e2e-event-bus.sh`): live Redis in docker, real server and client, asserts the stream contents; documented as RUNBOOK §1.5.
- Hosted gRPC runs emit lifecycle events to the event bus when `ODYSSEY_REDIS_ADDR` is set; without it the server runs with the bus disabled (gh-52).
- Pipeline and job lifecycle events published to the Redis Streams event bus, with emission failures logged and never fatal (gh-52).
- Event bus technology decision record: Redis Streams chosen for v1 (gh-38).
- Process scaffolding ported from the cubicle project: issue-first workflow with solution-template and PR-link automation, one-shot ship/merge scripts, RUNBOOK.md with recorded baseline, gitignored STATE.md, and a `make check` gate.
- CI workflow running `make check` on every PR and push to main; branch protection on main requires the `check` status and pull requests.

### Changed
- README rewritten to cover only the dispatcher; the platform-wide Odyssey design document (component map, product goals, business model, cross-service roadmap) moved out of the repository to the local odyssey workspace as `odyssey-design.md`.
- Renamed the repository to `dispatcher` and aligned the Go module path to `github.com/odyssey-ci-cd-platform/dispatcher`, reversing the ODYS-7 keep-the-Bitbucket-path decision ahead of the pivot's dispatcher slices (gh-87).
- Step finished payloads add `exit_code` and `duration_ms`, and job started payloads add `stage`, so the recorder can fill its schema; stdout/stderr content stays off the bus (gh-85).
- The event bus E2E smoke also asserts the run lands in the results database, and passes a localhost `ODYSSEY_ADDR` — a bare port stopped parsing when gh-74 made the variable verbatim (gh-85).
- Pipeline TOML files with unknown keys are rejected at load, naming the file and keys; the undocumented job `name` key is gone from the example and fixtures (gh-68).
- A declared stage with no jobs now fails validation instead of aggregating to Pending forever (gh-69).
- Jobs within a stage are ordered deterministically by job key (gh-70).
- `make check` enforces gofmt cleanliness and module tidiness alongside vet and tests (gh-78).

### Fixed
- Step timeouts and run cancellations stop the running step promptly: the exec stream is closed so the runner no longer blocks in `stdcopy.StdCopy` until the process exits, and output received before the deadline is kept (gh-92).
- Step results carry the real process exit code; non-zero exits are no longer reported as infrastructure errors, and infrastructure faults mid-step surface as `StatusErrored` (gh-65).
- Status aggregation ranks every status explicitly; Running, Skipped, and Unknown no longer rank as Passed (gh-72).
- Status aggregation ranks every status explicitly; Running, Skipped, and Unknown no longer rank as Passed (gh-72).
- Stage, pipeline, and run durations are wall-clock measurements by the orchestrator instead of sums over concurrent work (gh-71).
- Step timeouts are validated milliseconds, documented in the example, and parent-cancellation is no longer reported as a step timeout; the goroutine/channel/select around step execution is gone (gh-73).
- Stages run fail-fast: the first Failed or Errored stage ends the run and later stages are skipped; the orchestrator no longer returns an always-nil error (gh-82).
- The server owns one Docker runner for its lifetime and closes it on shutdown; a missing runner is a configuration error instead of a per-request client (gh-75).
- Consumer: read errors back off instead of hot-looping, recovery walks every pending entry instead of the first 64, and entries that can never parse dead-letter immediately (gh-76).
- Docker integration tests ping the daemon and skip cleanly when it is down; tests locking in the old exit-code behavior were rewritten (gh-77).
- Code-level slips: structured logging replaces printf-style slog calls, the unused LocalRunner is removed, the client default address matches the server, the env-file variable is renamed to `ODYSSEY_ENV_FILE` so `ODYSSEY_ENV` alone means the log switch, the proto enum reserves the skipped field 6, status mapping switches on status values, and both log formats write to stderr (gh-79).
- Workflow automation: the issue template carries only problem, outcomes, and constraints; `ship-pr.sh` derives the issue only from feature/N branches, cleans its temp body, and polls for checks instead of a fixed sleep; the PR-link workflow links only closing references (gh-80).
- Documentation drift repaired: stale `README.org` removed, README corrected (`.odyssey` path, `StatusErrored`, the actual container model, Go 1.26, decision-first event-bus wording), and the example documents step timeouts (gh-81).
- The agent rulebook (`AGENTS.md`, `.agents/`) is version-controlled and CI runs the docs check on every PR; the checker now counts violations (it previously printed findings but always exited zero) and skips YAML frontmatter (gh-83).
### Security
- The gRPC server binds localhost by default, honors a verbatim `ODYSSEY_ADDR`, and confines `project_path` under `ODYSSEY_PROJECT_ROOT` when set (gh-74).

