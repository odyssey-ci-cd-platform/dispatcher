package results_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/results"
)

// runEvents returns the lifecycle event sequence the orchestrator emits
// for one passing run of a single-stage, single-job, single-step pipeline
// (payload fields as extended for the recorder).
func runEvents(t *testing.T) []domain.Event {
	t.Helper()
	started := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	return []domain.Event{
		{Type: domain.EventPipelineStarted, OccurredAt: started, RunID: "run-1", Pipeline: "demo"},
		{Type: domain.EventJobStarted, OccurredAt: started.Add(time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Payload: map[string]string{"stage": "first"}},
		{Type: domain.EventStepStarted, OccurredAt: started.Add(2 * time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Step: "compile"},
		{Type: domain.EventStepFinished, OccurredAt: started.Add(3 * time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Step: "compile", Payload: map[string]string{"status": "passed", "exit_code": "0", "duration_ms": "1200"}},
		{Type: domain.EventJobFinished, OccurredAt: started.Add(4 * time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Payload: map[string]string{"status": "passed"}},
		{Type: domain.EventPipelineFinished, OccurredAt: started.Add(5 * time.Second), RunID: "run-1", Pipeline: "demo", Payload: map[string]string{"status": "passed"}},
	}
}

// recordAll feeds every event to the store in stream order.
func recordAll(t *testing.T, store *results.Store, events []domain.Event) {
	t.Helper()
	for _, event := range events {
		if err := store.Record(context.Background(), event); err != nil {
			t.Fatalf("Record(%s): unexpected error: %v", event.Type, err)
		}
	}
}

func queryRows(t *testing.T, db *sql.DB, query string) [][]string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var out [][]string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			switch v := v.(type) {
			case nil:
				row[i] = "<nil>"
			case []byte:
				row[i] = string(v)
			case string:
				row[i] = v
			default:
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func TestOpenMigratesAndSetsWAL(t *testing.T) {
	path := t.TempDir() + "/results.db"

	store, err := results.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open for inspection: %v", err)
	}
	defer db.Close()

	if got := queryRows(t, db, "PRAGMA journal_mode"); len(got) != 1 || got[0][0] != "wal" {
		t.Errorf("journal_mode = %v, want [[wal]]", got)
	}

	// Migrations applied: the three normalized tables exist, and a
	// second Open over the same file is a no-op, not an error.
	wantTables := []string{"jobs", "runs", "steps"}
	gotTables := queryRows(t, db, "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('runs','jobs','steps') ORDER BY name")
	if len(gotTables) != len(wantTables) {
		t.Fatalf("tables = %v, want exactly %v", gotTables, wantTables)
	}
	for i, row := range gotTables {
		if row[0] != wantTables[i] {
			t.Errorf("table[%d] = %s, want %s", i, row[0], wantTables[i])
		}
	}

	reopened, err := results.Open(path)
	if err != nil {
		t.Fatalf("second Open on migrated database: %v", err)
	}
	reopened.Close()
}

func TestRecordFullRunLandsRows(t *testing.T) {
	store, err := results.Open(t.TempDir() + "/results.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	recordAll(t, store, runEvents(t))

	db, err := sql.Open("sqlite", store.Path())
	if err != nil {
		t.Fatalf("open for inspection: %v", err)
	}
	defer db.Close()

	runs := queryRows(t, db, "SELECT run_id, pipeline, status, started_at, finished_at FROM runs")
	if len(runs) != 1 {
		t.Fatalf("runs rows = %d, want 1", len(runs))
	}
	want := []string{"run-1", "demo", "passed", "2026-10-09T12:00:00Z", "2026-10-09T12:00:05Z"}
	for i := range want {
		if runs[0][i] != want[i] {
			t.Errorf("runs[0][%d] = %q, want %q", i, runs[0][i], want[i])
		}
	}

	jobs := queryRows(t, db, "SELECT r.run_id, j.name, j.stage, j.status FROM jobs j JOIN runs r ON r.id = j.run_id")
	if len(jobs) != 1 {
		t.Fatalf("jobs rows = %d, want 1", len(jobs))
	}
	wantJob := []string{"run-1", "build", "first", "passed"}
	for i := range wantJob {
		if jobs[0][i] != wantJob[i] {
			t.Errorf("jobs[0][%d] = %q, want %q", i, jobs[0][i], wantJob[i])
		}
	}

	steps := queryRows(t, db, "SELECT s.name, s.status, s.exit_code, s.duration_ms, s.payload FROM steps s")
	if len(steps) != 1 {
		t.Fatalf("steps rows = %d, want 1", len(steps))
	}
	wantStep := []string{"compile", "passed", "0", "1200", "{}"}
	for i := range wantStep {
		if steps[0][i] != wantStep[i] {
			t.Errorf("steps[0][%d] = %q, want %q", i, steps[0][i], wantStep[i])
		}
	}
}

func TestRecordIsIdempotentUnderRedelivery(t *testing.T) {
	store, err := results.Open(t.TempDir() + "/results.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	events := runEvents(t)
	recordAll(t, store, events)
	recordAll(t, store, events) // at-least-once redelivery of every entry

	db, err := sql.Open("sqlite", store.Path())
	if err != nil {
		t.Fatalf("open for inspection: %v", err)
	}
	defer db.Close()

	for table, want := range map[string]string{"runs": "1", "jobs": "1", "steps": "1"} {
		got := queryRows(t, db, "SELECT COUNT(*) FROM "+table)
		if got[0][0] != want {
			t.Errorf("COUNT(*) FROM %s = %s, want %s (redelivery must not duplicate)", table, got[0][0], want)
		}
	}
}

func TestRecordIgnoresUnknownEventType(t *testing.T) {
	store, err := results.Open(t.TempDir() + "/results.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	err = store.Record(context.Background(), domain.Event{Type: "future.thing", OccurredAt: time.Now(), RunID: "run-1", Pipeline: "demo"})
	if err != nil {
		t.Errorf("Record(unknown type): %v, want nil (new event types must not poison the recorder)", err)
	}

	db, err := sql.Open("sqlite", store.Path())
	if err != nil {
		t.Fatalf("open for inspection: %v", err)
	}
	defer db.Close()
	if got := queryRows(t, db, "SELECT COUNT(*) FROM runs"); got[0][0] != "0" {
		t.Errorf("COUNT(*) FROM runs = %s, want 0", got[0][0])
	}
}

func TestRecordPartialRunKeepsRunPending(t *testing.T) {
	store, err := results.Open(t.TempDir() + "/results.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	events := runEvents(t)[:1] // pipeline.started only: the run never finished
	recordAll(t, store, events)

	db, err := sql.Open("sqlite", store.Path())
	if err != nil {
		t.Fatalf("open for inspection: %v", err)
	}
	defer db.Close()

	runs := queryRows(t, db, "SELECT status, finished_at FROM runs")
	if len(runs) != 1 {
		t.Fatalf("runs rows = %d, want 1", len(runs))
	}
	if runs[0][0] != "pending" || runs[0][1] != "<nil>" {
		t.Errorf("unfinished run = (%s, %s), want (pending, <nil>)", runs[0][0], runs[0][1])
	}
}

// TestRecordFinishedWithoutParentRowFails asserts a finished event whose
// parent row is unknown returns an error, so the consumer dead-letters it
// per the at-least-once contract instead of acking a silently lost final
// status (AUD-025, ADR 0002 decision 4).
func TestRecordFinishedWithoutParentRowFails(t *testing.T) {
	started := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		setup    []domain.Event
		finished domain.Event
		wantErr  string
	}{
		{
			name:     "pipeline.finished without pipeline.started",
			setup:    nil,
			finished: domain.Event{Type: domain.EventPipelineFinished, OccurredAt: started.Add(time.Second), RunID: "run-1", Pipeline: "demo", Payload: map[string]string{"status": "passed"}},
			wantErr:  `unknown run "run-1"`,
		},
		{
			name: "job.finished without job.started",
			setup: []domain.Event{
				{Type: domain.EventPipelineStarted, OccurredAt: started, RunID: "run-1", Pipeline: "demo"},
			},
			finished: domain.Event{Type: domain.EventJobFinished, OccurredAt: started.Add(time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Payload: map[string]string{"status": "passed"}},
			wantErr:  `unknown job "build"`,
		},
		{
			name: "step.finished without step.started",
			setup: []domain.Event{
				{Type: domain.EventPipelineStarted, OccurredAt: started, RunID: "run-1", Pipeline: "demo"},
				{Type: domain.EventJobStarted, OccurredAt: started.Add(time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Payload: map[string]string{"stage": "first"}},
			},
			finished: domain.Event{Type: domain.EventStepFinished, OccurredAt: started.Add(2 * time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Step: "compile", Payload: map[string]string{"status": "passed", "exit_code": "0"}},
			wantErr:  `unknown step "compile"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := results.Open(t.TempDir() + "/results.db")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer store.Close()

			recordAll(t, store, tt.setup)

			err = store.Record(context.Background(), tt.finished)
			if err == nil {
				t.Fatalf("Record(%s) = nil, want error (a silent ack loses the final status)", tt.finished.Type)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Record(%s) error = %v, want error containing %q", tt.finished.Type, err, tt.wantErr)
			}
		})
	}
}

// TestRecordStepCarriesTimestampsAndError asserts the steps row keeps the
// step's started/finished timestamps and the payload's error text (AUD-025):
// the outcome history flakiness analytics read.
func TestRecordStepCarriesTimestampsAndError(t *testing.T) {
	started := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	base := []domain.Event{
		{Type: domain.EventPipelineStarted, OccurredAt: started, RunID: "run-1", Pipeline: "demo"},
		{Type: domain.EventJobStarted, OccurredAt: started.Add(time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Payload: map[string]string{"stage": "first"}},
		{Type: domain.EventStepStarted, OccurredAt: started.Add(2 * time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Step: "compile"},
	}
	tests := []struct {
		name            string
		finishedPayload map[string]string
		wantError       string
	}{
		{
			name:            "error text from the payload",
			finishedPayload: map[string]string{"status": "errored", "error": "container exec failed"},
			wantError:       "container exec failed",
		},
		{
			name:            "no error key records NULL",
			finishedPayload: map[string]string{"status": "passed", "exit_code": "0", "duration_ms": "1200"},
			wantError:       "<nil>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := results.Open(t.TempDir() + "/results.db")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer store.Close()

			events := slices.Concat(base, []domain.Event{{
				Type: domain.EventStepFinished, OccurredAt: started.Add(3 * time.Second), RunID: "run-1", Pipeline: "demo", Job: "build", Step: "compile", Payload: tt.finishedPayload,
			}})
			recordAll(t, store, events)

			db, err := sql.Open("sqlite", store.Path())
			if err != nil {
				t.Fatalf("open for inspection: %v", err)
			}
			defer db.Close()

			steps := queryRows(t, db, "SELECT started_at, finished_at, error FROM steps")
			if len(steps) != 1 {
				t.Fatalf("steps rows = %d, want 1", len(steps))
			}
			want := []string{"2026-10-09T12:00:02Z", "2026-10-09T12:00:03Z", tt.wantError}
			for i := range want {
				if steps[0][i] != want[i] {
					t.Errorf("steps[0][%d] = %q, want %q", i, steps[0][i], want[i])
				}
			}
		})
	}
}
