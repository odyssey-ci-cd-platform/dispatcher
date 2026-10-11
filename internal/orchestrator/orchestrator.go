package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/common"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/runner"
	"golang.org/x/sync/errgroup"
)

// EventSink publishes lifecycle events to the event bus. Nil means the
// event bus is disabled; the engine stays runnable without Redis (ADR 0001).
type EventSink interface {
	Publish(ctx context.Context, event domain.Event) error
}

// Orchestrator executes a Pipeline by running stages sequentially and
// jobs within each stage concurrently.
type Orchestrator struct {
	runner runner.Runner
	sink   EventSink
	logger *slog.Logger
}

// New returns an Orchestrator that delegates job execution to r and emits
// lifecycle events to sink (may be nil to disable emission).
func New(r runner.Runner, sink EventSink, logger *slog.Logger) *Orchestrator {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Orchestrator{runner: r, sink: sink, logger: logger}
}

// emit publishes one lifecycle event. Emission failures are logged, never
// fatal — pipeline execution proceeds unaffected (ADR 0001).
func (o *Orchestrator) emit(ctx context.Context, event domain.Event) {
	if o.sink == nil {
		return
	}
	if err := o.sink.Publish(ctx, event); err != nil {
		o.logger.Warn("event emit failed", "type", event.Type, "pipeline", event.Pipeline, "err", err)
	}
}

// stepSink returns the sink handed to the runner: nil when the bus is
// disabled, otherwise a view stamping runner-built events with the pipeline
// name and run ID before emission — the runner knows steps, not which
// pipeline or run it serves (ADR 0001: envelopes carry the pipeline).
func (o *Orchestrator) stepSink(pipelineName string, runID string) runner.StepSink {
	if o.sink == nil {
		return nil
	}
	return &stampedSink{sink: o.sink, pipeline: pipelineName, runID: runID}
}

// stampedSink adapts the orchestrator's EventSink to runner.StepSink,
// filling in the pipeline name and run ID on every event that passes
// through.
type stampedSink struct {
	sink     EventSink
	pipeline string
	runID    string
}

func (s *stampedSink) Publish(ctx context.Context, event domain.Event) error {
	event.Pipeline = s.pipeline
	event.RunID = s.runID
	return s.sink.Publish(ctx, event)
}

// newRunID returns a unique identifier for one pipeline run: a timestamp
// for ordering plus random bytes for uniqueness across concurrent runs.
func newRunID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// The timestamp alone still distinguishes sequential runs.
		return fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("run-%d-%s", time.Now().UnixNano(), hex.EncodeToString(b[:]))
}

// Run executes every stage in the pipeline. Jobs within a stage run
// concurrently. Stages run fail-fast: the first Failed or Errored stage
// ends the run and later stages are skipped (recorded decision, gh-82).
// Run never fails — the result carries what happened, including cancelled
// runs, so there is no error return (AUD-015).
func (o *Orchestrator) Run(ctx context.Context, pipeline domain.Pipeline, projectPath string) domain.PipelineResult {
	runID := newRunID()
	result := domain.PipelineResult{
		RunID:        runID,
		PipelineName: pipeline.Name,
		StageResults: make([]domain.StageResult, 0, len(pipeline.Stages)),
	}
	o.emit(ctx, domain.Event{Type: domain.EventPipelineStarted, OccurredAt: time.Now(), RunID: runID, Pipeline: pipeline.Name})
	runStart := time.Now()
	for _, stage := range pipeline.Stages {
		stageResult := o.runStage(ctx, stage, pipeline.Name, runID, projectPath)
		result.StageResults = append(result.StageResults, stageResult)
		// Fail-fast: a failed or errored stage ends the run (gh-82).
		if worst := stageResult.Status(); worst == domain.StatusFailed || worst == domain.StatusErrored {
			break
		}
	}
	result.Duration = time.Since(runStart)
	// Finished events are the record that a run ended; they must not die
	// with a cancelled context (AUD-012).
	finishedCtx := context.WithoutCancel(ctx)
	o.emit(finishedCtx, domain.Event{
		Type:       domain.EventPipelineFinished,
		OccurredAt: time.Now(),
		RunID:      runID,
		Pipeline:   pipeline.Name,
		Payload:    map[string]string{"status": result.Status().String()},
	})
	return result
}

// runStage executes all jobs in a stage concurrently and returns the
// aggregated StageResult.
func (o *Orchestrator) runStage(ctx context.Context, stage domain.Stage, pipelineName string, runID string, projectPath string) domain.StageResult {
	stageLogger := o.logger.With("stage", stage.Name)
	stageLogger.Info("stage started")
	start := time.Now()

	jobResults := make([]domain.JobResult, len(stage.Jobs))
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)

	for i, job := range stage.Jobs {
		g.Go(func() error {
			jobLogger := stageLogger.With("job", job.Name)
			jobCtx := common.ContextWithLogger(ctx, jobLogger)

			o.emit(jobCtx, domain.Event{Type: domain.EventJobStarted, OccurredAt: time.Now(), RunID: runID, Pipeline: pipelineName, Job: job.Name, Payload: map[string]string{"stage": stage.Name}})
			jobResult, err := o.runner.Run(jobCtx, job, projectPath, o.stepSink(pipelineName, runID))
			// The finished event is the record that the job ended; it must
			// not die with a cancelled context (AUD-012).
			o.emit(context.WithoutCancel(jobCtx), domain.Event{
				Type:       domain.EventJobFinished,
				OccurredAt: time.Now(),
				RunID:      runID,
				Pipeline:   pipelineName,
				Job:        job.Name,
				Payload:    map[string]string{"status": jobResult.Status().String()},
			})
			if err != nil {
				jobLogger.Error("job failed",
					"job", job.Name,
					"stepCount", len(jobResult.StepResults),
					"error", err,
				)
			}
			mu.Lock()
			jobResults[i] = jobResult
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	stageResult := domain.StageResult{
		StageName:  stage.Name,
		JobResults: jobResults,
		Duration:   time.Since(start),
	}
	stageLogger.Info("stage finished",
		"status", stageResult.Status().String(),
		"elapsed", time.Since(start).String(),
	)
	return stageResult
}
