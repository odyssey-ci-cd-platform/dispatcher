# dispatcher

Language-agnostic, configuration-driven CI/CD execution engine (Go rewrite) plus a thin gRPC agent daemon. **Read `README.md` first** for architecture, design principles, roadmap, and open questions — it's a maintained design doc, not stale docs. Don't duplicate its content here; this file is operational notes only.

## Commands

- Gate: `make check` (vet + test) — the single gate every change must leave green; the recorded baseline lives in RUNBOOK.md §3.1.
- Build: `go build ./...`
- Test: `go test ./...`
- Vet: `go vet ./...`
- Regenerate gRPC/protobuf code after editing `api/v1/odyssey.proto`: `make proto` (requires `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` on `PATH`). Use the `/proto` slash command.

## Workflow

- Multi-commit work needs a GitHub issue first (template: Work item). An issue describes the work needed — the outcome or business requirement — never the code change. One-commit tasks get no issue.
- A workflow posts the solution template as the first issue comment automatically; the first actionable is filling it in — edit the posted comment, keep every header, N/A where not applicable.
- Branch per work item: `feature/<issue-number>`, or issueless `type/slug`.
  All changes land through PRs — main is protected (required `check`
  status, no direct pushes, admins included).
- Tests come before changes: the intent test is red first, then the change turns it green.
- One-shot PR flow: `scripts/ship-pr.sh <branch> <title> [body-file]` (push, open PR, watch CI), `scripts/merge-pr.sh <branch>` (merge, delete branch, sync main).
- Intermediate session state lives in `STATE.md` (repo root, gitignored, never committed).
- Operational procedures, incidents, and the recorded baseline live in `RUNBOOK.md`; update it in the same commit that changes what a procedure does.

## Testing notes

- `internal/runner/docker_test.go` spins up real containers against a live Docker daemon. If Docker isn't running, those tests skip (requireDocker pings the daemon) — a failure is a code regression, not a daemon problem.
- Convention: table-driven tests, external test packages (`package config_test`, not `package config`), `t.Helper()` in shared fixtures. Match this in new tests.

## Package map

- `internal/config` — TOML pipeline/env config loading (`.odyssey/pipeline.toml`, `.odyssey/env.toml`).
- `internal/domain` — core types: `Pipeline`, `JobResult`, `Status` (`Error` > `Failed` > `Pending` > `Passed` precedence).
- `internal/orchestrator` — runs stages sequentially, jobs within a stage concurrently.
- `internal/runner` — Docker execution (`docker.go`), cross-step env-file convention (`envfile.go`, `$ODYSSEY_ENV_FILE`).
- `internal/server` — gRPC server exposing the execution engine.
- `internal/common` — logging, shared utils.
- `api/v1/odyssey.proto` / `gen/proto/v1` — gRPC service definition and generated code (generated, don't hand-edit `gen/`).

## Design principles (from README, worth restating)

- Minimal abstractions; functions over classes when no state is needed.
- Prefer direct, slightly repetitive call sites over an abstraction that doesn't earn its weight.
- Language-agnostic engine — no hardcoded toolchain assumptions.
- Schema fields minimal and orthogonal; reject contradictory combinations at validation/parse time, not silently.
- Stderr is not reliably "the error" — keep stdout/stderr separate in results, don't conflate with failure.
- Deferred cleanup (container teardown) uses an independent context (`context.WithoutCancel` + its own timeout) so a cancelled/timed-out parent context can't leak containers.

## Gotchas

- Module path is `github.com/odyssey-ci-cd-platform/dispatcher`, matching the GitHub repo `dispatcher` (`odyssey-ci-cd-platform` org) — aligned by gh-87. This reverses the recorded ODYS-7 decision to keep the legacy `bitbucket.org/odyssey-ci/odyssey-core-agent` path; the old path no longer appears anywhere in the repo.

## Conventions

- Branches: `feature/<issue-number>`. Commits: `gh-<issue-number>: <summary>`, imperative mood.
- Service seams take injected interfaces with test fakes: the consumer package owns the small interface, fakes mirror real signatures, and both use the shared shapes from `internal/domain` (the equivalent of a single models module). No fake hand-rolls its own data shape.
- Guard clauses and negative checks over nesting; flatten before adding a level.
- Names and doc comments say exactly what a thing does, at its own level of abstraction.

## Documentation rules

- In Markdown files (README, docs/, CHANGELOG, etc.), never hard-wrap a line mid-sentence. Break lines only at sentence boundaries; one sentence per line is the norm.
- Long lines are fine. Markdown soft-wraps in every editor and renders identically regardless of where the source line breaks. Wrapping at ~80 columns midsentence produces noisy diffs and broken search.
- Exceptions where line length is structural: code blocks, tables, HTML/frontmatter.
- Update CHANGELOG.md on every change that lands on main, in the same PR. Add one entry under `[Unreleased]` following the format at the top of that file. Purely mechanical commits (formatting, typos) may share one entry.
- Before committing docs changes, run `.agents/bin/check-docs.sh` to catch mid-sentence wraps in modified Markdown files.
