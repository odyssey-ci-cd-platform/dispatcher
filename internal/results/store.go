// Package results durably records pipeline lifecycle events into the
// embedded SQLite results DB (ADR 0002). The recorder consumer is the
// database's only writer; everything else reads.
package results

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"time"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store is the embedded SQLite results database: WAL, migrated on open,
// written only through Record.
type Store struct {
	db   *sql.DB
	path string
}

// Open opens (creating if needed) the database file at path, enables WAL,
// and applies pending migrations, so the binary migrates its own database
// on startup (ADR 0002).
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("results: open %s: %w", path, err)
	}
	// The recorder is the only writer (ADR 0002): one connection removes
	// cross-connection lock contention by construction.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("results: enable WAL on %s: %w", path, err)
	}
	// goose collects migrations from the FS root; the embedded files
	// live one level down.
	migrationFiles, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("results: migration filesystem: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrationFiles)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("results: migration provider: %w", err)
	}
	if _, err := provider.Up(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("results: migrate %s: %w", path, err)
	}
	return &Store{db: db, path: path}, nil
}

// Path returns the database file path the store was opened on.
func (s *Store) Path() string { return s.path }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// Record applies one lifecycle event. Redelivered events are no-ops
// (at-least-once delivery, ADR 0001); unknown event types are ignored so
// new producers cannot poison the recorder. Events missing their run
// identity fail loudly — they dead-letter instead of recording rows that
// could never be told apart.
func (s *Store) Record(ctx context.Context, event domain.Event) error {
	switch event.Type {
	case domain.EventPipelineStarted:
		return s.recordPipelineStarted(ctx, event)
	case domain.EventPipelineFinished:
		return s.recordPipelineFinished(ctx, event)
	case domain.EventJobStarted:
		return s.recordJobStarted(ctx, event)
	case domain.EventJobFinished:
		return s.recordJobFinished(ctx, event)
	case domain.EventStepStarted:
		return s.recordStepStarted(ctx, event)
	case domain.EventStepFinished:
		return s.recordStepFinished(ctx, event)
	default:
		return nil
	}
}

// stamp renders an event time as UTC RFC 3339, SQLite's sortable ISO-8601 text.
func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func (s *Store) recordPipelineStarted(ctx context.Context, event domain.Event) error {
	if event.RunID == "" {
		return errors.New("results: pipeline.started without run id")
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO runs (run_id, pipeline, status, started_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT (run_id) DO NOTHING`,
		event.RunID, event.Pipeline, domain.StatusPending.String(), stamp(event.OccurredAt))
	if err != nil {
		return fmt.Errorf("results: record run %s: %w", event.RunID, err)
	}
	return nil
}

func (s *Store) recordPipelineFinished(ctx context.Context, event domain.Event) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status = ?, finished_at = ? WHERE run_id = ?`,
		event.Payload["status"], stamp(event.OccurredAt), event.RunID)
	if err != nil {
		return fmt.Errorf("results: finish run %s: %w", event.RunID, err)
	}
	return nil
}

func (s *Store) recordJobStarted(ctx context.Context, event domain.Event) error {
	runRow, err := s.runRow(ctx, event.RunID)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO jobs (run_id, name, stage, status, started_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (run_id, name) DO NOTHING`,
		runRow, event.Job, event.Payload["stage"], domain.StatusPending.String(), stamp(event.OccurredAt))
	if err != nil {
		return fmt.Errorf("results: record job %s/%s: %w", event.RunID, event.Job, err)
	}
	return nil
}

func (s *Store) recordJobFinished(ctx context.Context, event domain.Event) error {
	runRow, err := s.runRow(ctx, event.RunID)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE jobs SET status = ?, finished_at = ? WHERE run_id = ? AND name = ?`,
		event.Payload["status"], stamp(event.OccurredAt), runRow, event.Job)
	if err != nil {
		return fmt.Errorf("results: finish job %s/%s: %w", event.RunID, event.Job, err)
	}
	return nil
}

func (s *Store) recordStepStarted(ctx context.Context, event domain.Event) error {
	jobRow, err := s.jobRow(ctx, event.RunID, event.Job)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO steps (job_id, name, status) VALUES (?, ?, ?)
		 ON CONFLICT (job_id, name) DO NOTHING`,
		jobRow, event.Step, domain.StatusPending.String())
	if err != nil {
		return fmt.Errorf("results: record step %s/%s/%s: %w", event.RunID, event.Job, event.Step, err)
	}
	return nil
}

func (s *Store) recordStepFinished(ctx context.Context, event domain.Event) error {
	jobRow, err := s.jobRow(ctx, event.RunID, event.Job)
	if err != nil {
		return err
	}
	// A step whose process never produced an exit code (ExitNone) records
	// NULL, not a sentinel value.
	var exitCode, duration any
	if raw, ok := event.Payload["exit_code"]; ok {
		code, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("results: step %s/%s/%s bad exit_code %q: %w", event.RunID, event.Job, event.Step, raw, err)
		}
		exitCode = code
	}
	if raw, ok := event.Payload["duration_ms"]; ok {
		ms, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("results: step %s/%s/%s bad duration_ms %q: %w", event.RunID, event.Job, event.Step, raw, err)
		}
		duration = ms
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE steps SET status = ?, exit_code = ?, duration_ms = ? WHERE job_id = ? AND name = ?`,
		event.Payload["status"], exitCode, duration, jobRow, event.Step)
	if err != nil {
		return fmt.Errorf("results: finish step %s/%s/%s: %w", event.RunID, event.Job, event.Step, err)
	}
	return nil
}

// runRow resolves a run's row id, failing events whose parent run is
// unknown — stream order (ADR 0002) guarantees pipeline.started arrives
// first, so a missing row means corruption, not a race.
func (s *Store) runRow(ctx context.Context, runID string) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `SELECT id FROM runs WHERE run_id = ?`, runID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("results: event references unknown run %q", runID)
	}
	return id, err
}

// jobRow resolves a job's row id the same way runRow does for runs.
func (s *Store) jobRow(ctx context.Context, runID string, job string) (int64, error) {
	runRow, err := s.runRow(ctx, runID)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.db.QueryRowContext(ctx, `SELECT id FROM jobs WHERE run_id = ? AND name = ?`, runRow, job).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("results: event references unknown job %q in run %q", job, runID)
	}
	return id, err
}
