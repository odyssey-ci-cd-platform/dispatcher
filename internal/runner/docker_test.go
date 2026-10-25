package runner_test

import (
	"context"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/runner"
)

// recordingSink is a StepSink that records events in memory.
type recordingSink struct {
	mu     sync.Mutex
	events []domain.Event
}

func (s *recordingSink) Publish(_ context.Context, event domain.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *recordingSink) recorded() []domain.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.Event(nil), s.events...)
}

// TestDockerRunnerEmitsStepEvents asserts every executed step yields exactly
// one started and one finished event, in order, tagged with the job, with
// only the status in the finished payload — never stdout/stderr content
// (gh-52; ADR 0001 keeps output content off the bus).
func TestDockerRunnerEmitsStepEvents(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "events-job",
		Image: "alpine",
		Steps: []domain.Step{
			{Name: "good", Run: "echo ok"},
			{Name: "bad", Run: "false"},
		},
	}
	sink := &recordingSink{}

	result, err := r.Run(context.Background(), job, dir, sink)
	// AUD-003: the failing second step is a process exit, not an error.
	if err != nil {
		t.Fatalf("Run: unexpected error for the failing second step: %v", err)
	}
	if result.Status() != domain.StatusFailed {
		t.Fatalf("job status = %v, want failed (second step fails)", result.Status())
	}

	var types []string
	for _, e := range sink.recorded() {
		if e.Job != "events-job" {
			t.Errorf("event %q tagged job %q, want events-job", e.Type, e.Job)
			continue
		}
		types = append(types, e.Type+"("+e.Step+")")
	}
	want := []string{
		"step.started(good)", "step.finished(good)",
		"step.started(bad)", "step.finished(bad)",
	}
	if !slices.Equal(types, want) {
		t.Errorf("step events = %v, want %v", types, want)
	}

	for _, e := range sink.recorded() {
		if e.Type != domain.EventStepFinished {
			continue
		}
		wantStatus := "passed"
		if e.Step == "bad" {
			wantStatus = "failed"
		}
		if e.Payload["status"] != wantStatus {
			t.Errorf("step %q finished payload status = %q, want %q", e.Step, e.Payload["status"], wantStatus)
		}
		for _, leak := range []string{"stdout", "stderr"} {
			if _, has := e.Payload[leak]; has {
				t.Errorf("step %q finished payload leaks %s", e.Step, leak)
			}
		}
	}
}

func TestNewDockerRunner(t *testing.T) {
	r, err := runner.NewDockerRunner(nil)
	if err != nil {
		t.Skipf("Docker client not available (this is fine): %v", err)
	}
	if r == nil {
		t.Error("NewDockerRunner() returned nil runner with no error")
	}
}

// requireDocker returns a DockerRunner, skipping the test if Docker
// is not available. A successful client constructor is not enough — the
// daemon itself is pinged, so a stopped daemon skips instead of failing
// or hanging (AUD-017).
func requireDocker(t *testing.T) *runner.DockerRunner {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	r, err := runner.NewDockerRunner(nil)
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Ping(pingCtx); err != nil {
		r.Close()
		t.Skipf("Docker daemon not reachable: %v", err)
	}
	return r
}

// alpineContainerIDs returns the IDs of the cancel-repro test's containers
// via the odyssey.job label, running or stopped.
func alpineContainerIDs(t *testing.T) map[string]bool {
	t.Helper()
	out, err := exec.Command("docker", "ps", "-a", "--filter", "label=odyssey.job=cancel-repro", "-q").Output()
	if err != nil {
		t.Fatalf("docker ps failed: %v", err)
	}
	ids := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			ids[line] = true
		}
	}
	return ids
}

// TestDockerRunnerCancelledRunRemovesContainer asserts a cancelled run does
// not leak its container: teardown must outlive the run's context (gh-63,
// AUD-001). Teardown on a cancelled context races the daemon request, so
// the leak only appears on some runs — the scenario loops five times.
func TestDockerRunnerCancelledRunRemovesContainer(t *testing.T) {
	r := requireDocker(t)
	before := alpineContainerIDs(t)

	job := domain.Job{
		Name:  "cancel-repro",
		Image: "alpine:latest",
		Steps: []domain.Step{{Name: "hang", Run: "sleep 10"}},
	}
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(1200 * time.Millisecond)
			cancel()
		}()

		_, err := r.Run(ctx, job, t.TempDir(), nil)
		if err == nil {
			t.Error("Run() expected an error when the context is cancelled mid-step")
		}
		for id := range alpineContainerIDs(t) {
			if !before[id] {
				t.Errorf("cancelled run leaked its container %s", id)
			}
		}
	}
}

// TestDockerRunnerInfraFailureDuringStepIsErrored asserts an infrastructure
// fault mid-step (the container vanishing under the running exec) yields
// StatusErrored — not Failed, which is reserved for process exits (AUD-003).
func TestDockerRunnerInfraFailureDuringStepIsErrored(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "infra-repro",
		Image: "alpine:latest",
		Steps: []domain.Step{{Name: "hang", Run: "sleep 10"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	go func() {
		time.Sleep(2500 * time.Millisecond)
		out, err := exec.Command("docker", "ps", "-aq", "--filter", "label=odyssey.job=infra-repro").Output()
		if err != nil {
			t.Logf("container listing failed (will re-check): %v", err)
			return
		}
		for _, id := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if id == "" {
				continue
			}
			if rmErr := exec.Command("docker", "rm", "-f", id).Run(); rmErr != nil {
				t.Logf("container removal failed (will re-check): %v", rmErr)
			}
		}
	}()

	result, err := r.Run(ctx, job, dir, nil)
	if err == nil {
		t.Fatal("Run() expected an error when the container is removed mid-step, got nil")
	}
	if result.Status() != domain.StatusErrored {
		t.Errorf("expected StatusErrored for an infrastructure fault, got %v", result.Status())
	}
	if len(result.StepResults) == 0 {
		t.Fatal("expected at least one step result")
	}
}

// TestDockerRunnerCancelledStepReportsCancellation asserts a parent-context
// cancellation mid-step stops the runner promptly — Run returns within a
// small margin of the cancel, and the cancellation is reported as cancelled,
// not as "timed out" (AUD-011; promptness gh-92, AUD-022).
func TestDockerRunnerCancelledStepReportsCancellation(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "cancel-report",
		Image: "alpine:latest",
		Steps: []domain.Step{{Name: "hang", Run: "sleep 20"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cancelAt atomic.Int64
	go func() {
		// Cancel only once the sleep process is verifiably running: a fixed
		// delay or the step-started event alone can land the cancel in the
		// pre-step env-read exec instead of the step, proving nothing about
		// interrupting a running exec (AUD-022).
		deadline := time.After(30 * time.Second)
		for {
			if sleepRunning(t) {
				cancelAt.Store(time.Now().UnixNano())
				cancel()
				return
			}
			select {
			case <-deadline:
				cancelAt.Store(time.Now().UnixNano())
				cancel()
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()

	result, err := r.Run(ctx, job, dir, nil)
	cancel()
	if err == nil {
		t.Fatal("Run() expected an error when the run is cancelled, got nil")
	}
	cancelledAt := time.Unix(0, cancelAt.Load())
	if cancelledAt.IsZero() {
		t.Fatal("the run was never cancelled")
	}
	// Promptness is the point: the hung step must not keep Run blocked until
	// the process exits on its own — sleep 20 would take ~20s.
	if since := time.Since(cancelledAt); since > 5*time.Second {
		t.Errorf("Run returned %v after cancellation, want within ~5s", since)
	}
	if len(result.StepResults) == 0 {
		t.Fatal("expected the in-flight step result to be recorded")
	}
	sr := result.StepResults[0]
	if sr.Err == nil {
		t.Fatal("expected the cancellation on the step result")
	}
	if strings.Contains(sr.Err.Error(), "timed out") {
		t.Errorf("cancellation reported as a timeout: %v", sr.Err)
	}
}

// sleepRunning reports whether a `sleep 20` process is live inside the
// cancel-report test's container, found via the odyssey.job label.
func sleepRunning(t *testing.T) bool {
	t.Helper()
	id, err := exec.Command("docker", "ps", "-q", "--filter", "label=odyssey.job=cancel-report").Output()
	if err != nil || strings.TrimSpace(string(id)) == "" {
		return false
	}
	out, err := exec.Command("docker", "exec", strings.TrimSpace(string(id)), "pgrep", "-f", "sleep 20").Output()
	return err == nil && strings.TrimSpace(string(out)) != ""
}

// TestDockerRunnerStopsJobAfterFailingStep asserts the job stops at the
// first non-zero step exit: the step after a failing one neither runs nor
// emits events — in-job fail-fast, mirroring the stage-level rule (gh-93,
// AUD-023).
func TestDockerRunnerStopsJobAfterFailingStep(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "fail-fast-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			{Name: "test", Run: "exit 3"},
			{Name: "deploy", Run: "echo deployed"},
		},
	}
	sink := &recordingSink{}

	result, err := r.Run(context.Background(), job, dir, sink)
	// A process exit is a result, not a run error (AUD-003).
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}
	if result.Status() != domain.StatusFailed {
		t.Errorf("Status() = %v, want StatusFailed", result.Status())
	}

	// Only the failing step is recorded; the deploy step never ran.
	if len(result.StepResults) != 1 {
		t.Fatalf("expected 1 step result (job stops at the failure), got %d", len(result.StepResults))
	}
	if got := result.StepResults[0].ExitCode; got != domain.ExitCode(3) {
		t.Errorf("first step exit code = %v, want 3", got)
	}
	for _, e := range sink.recorded() {
		if e.Step == "deploy" {
			t.Errorf("step after the failure emitted event %q", e.Type)
		}
	}
}

func TestDockerRunnerRunEcho(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "echo-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			{Name: "hello", Run: "echo hello-world"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	status := result.Status()
	if status != domain.StatusPassed {
		t.Errorf("expected StatusPassed, got %v", status)
	}

	if len(result.StepResults) != 1 {
		t.Fatalf("expected 1 step result, got %d", len(result.StepResults))
	}

	step := result.StepResults[0]
	if step.ExitCode != domain.ExitSuccess {
		t.Errorf("expected exit success, got %v", step.ExitCode)
	}

	if !strings.Contains(step.Stdout, "hello-world") {
		t.Errorf("expected stdout to contain 'hello-world', got %q", step.Stdout)
	}
}

func TestDockerRunnerRunFailingCommand(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "fail-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			{Name: "failing step", Run: "exit 42"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	// AUD-003: a non-zero exit is the process's own result — the real exit
	// code with no run error; the job is Failed, not Errored.
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	status := result.Status()
	if status != domain.StatusFailed {
		t.Errorf("expected StatusFailed, got %v", status)
	}

	if len(result.StepResults) != 1 {
		t.Fatalf("expected 1 step result, got %d", len(result.StepResults))
	}

	step := result.StepResults[0]
	if step.ExitCode != domain.ExitCode(42) {
		t.Errorf("expected the real exit code 42, got %v", step.ExitCode)
	}
	if step.Err != nil {
		t.Errorf("expected no step error for a process failure, got %v", step.Err)
	}
}

func TestDockerRunnerRunWithEnvVars(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "env-test",
		Image: "alpine:latest",
		Env: map[string]string{
			"MY_VAR": "hello-env",
		},
		Steps: []domain.Step{
			{Name: "print env", Run: "echo $MY_VAR"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	if len(result.StepResults) != 1 {
		t.Fatalf("expected 1 step result, got %d", len(result.StepResults))
	}

	if !strings.Contains(result.StepResults[0].Stdout, "hello-env") {
		t.Errorf("expected stdout to contain 'hello-env', got %q", result.StepResults[0].Stdout)
	}
}

func TestDockerRunnerRunWithSetup(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "setup-test",
		Image: "alpine:latest",
		Setup: []string{
			"echo 'setup-ran' > /app/setup-marker.txt",
		},
		Steps: []domain.Step{
			{Name: "check setup", Run: "cat /app/setup-marker.txt"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	if len(result.StepResults) != 1 {
		t.Fatalf("expected 1 step result, got %d", len(result.StepResults))
	}

	if !strings.Contains(result.StepResults[0].Stdout, "setup-ran") {
		t.Errorf("expected stdout to contain 'setup-ran', got %q", result.StepResults[0].Stdout)
	}
}

func TestDockerRunnerRunMultipleSteps(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "multi-step-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			{Name: "step one", Run: "echo one"},
			{Name: "step two", Run: "echo two"},
			{Name: "step three", Run: "echo three"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	if len(result.StepResults) != 3 {
		t.Fatalf("expected 3 step results, got %d", len(result.StepResults))
	}

	for i, want := range []string{"one", "two", "three"} {
		if !strings.Contains(result.StepResults[i].Stdout, want) {
			t.Errorf("step %d: expected stdout to contain %q, got %q", i+1, want, result.StepResults[i].Stdout)
		}
		if result.StepResults[i].ExitCode != domain.ExitSuccess {
			t.Errorf("step %d: expected exit success, got %v", i+1, result.StepResults[i].ExitCode)
		}
	}
}

func TestDockerRunnerExportsEnvBetweenSteps(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "export-env-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			{Name: "export", Run: `echo "SHARED=from-step-one" >> "$ODYSSEY_ENV_FILE"`},
			{Name: "consume", Run: "echo $SHARED"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}
	if len(result.StepResults) != 2 {
		t.Fatalf("expected 2 step results, got %d", len(result.StepResults))
	}
	if got := result.StepResults[1].Stdout; !strings.Contains(got, "from-step-one") {
		t.Errorf("expected second step to see exported var, got %q", got)
	}
}

func TestDockerRunnerExportedEnvOverridesJobEnv(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "override-env-test",
		Image: "alpine:latest",
		Env:   map[string]string{"SHARED": "job-level"},
		Steps: []domain.Step{
			{Name: "before override", Run: "echo $SHARED"},
			{Name: "override", Run: `echo "SHARED=step-level" >> "$ODYSSEY_ENV_FILE"`},
			{Name: "after override", Run: "echo $SHARED"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}
	if len(result.StepResults) != 3 {
		t.Fatalf("expected 3 step results, got %d", len(result.StepResults))
	}
	if got := result.StepResults[0].Stdout; !strings.Contains(got, "job-level") {
		t.Errorf("first step: expected job-level value, got %q", got)
	}
	if got := result.StepResults[2].Stdout; !strings.Contains(got, "step-level") {
		t.Errorf("last step: expected exported value to override job env, got %q", got)
	}
}

func TestDockerRunnerRunSetupError(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "bad-setup-test",
		Image: "alpine:latest",
		Setup: []string{
			"exit 1",
		},
		Steps: []domain.Step{
			{Name: "should not run", Run: "echo nope"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err == nil {
		t.Error("Run() expected error for failed setup, got nil")
	}

	if result.SetupErr == nil {
		t.Error("expected SetupErr to be set")
	}

	if result.Status() != domain.StatusErrored {
		t.Errorf("expected StatusErrored, got %v", result.Status())
	}

	if len(result.StepResults) != 0 {
		t.Errorf("expected 0 step results when setup fails, got %d", len(result.StepResults))
	}
}

func TestDockerRunnerRunImagePullError(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "bad-image-test",
		Image: "alpine:odyssey-no-such-tag",
		Steps: []domain.Step{
			{Name: "should not run", Run: "echo nope"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err == nil {
		t.Fatal("Run() expected an error for a missing image, got nil")
	}
	if result.SetupErr == nil {
		t.Error("expected SetupErr to be set when image pull fails")
	}
	if result.Status() != domain.StatusErrored {
		t.Errorf("expected StatusErrored, got %v", result.Status())
	}
}

func TestDockerRunnerRunMountError(t *testing.T) {
	r := requireDocker(t)

	job := domain.Job{
		Name:  "bad-mount-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			{Name: "should not run", Run: "echo nope"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A nonexistent project path makes mountArchive fail after the container
	// is created, surfacing as a setup error.
	result, err := r.Run(ctx, job, "/nonexistent/odyssey/project", nil)
	if err == nil {
		t.Fatal("Run() expected an error for a missing project path, got nil")
	}
	if result.SetupErr == nil {
		t.Error("expected SetupErr to be set when mounting the project fails")
	}
	if result.Status() != domain.StatusErrored {
		t.Errorf("expected StatusErrored, got %v", result.Status())
	}
}

func TestDockerRunnerRunExportedEnvReadError(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "env-read-error-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			// Removing the env file makes the after-step readExportedEnv fail,
			// which is an infrastructure fault and must surface as Errored,
			// not as a process failure (AUD-003).
			{Name: "remove env file", Run: `rm -f "$ODYSSEY_ENV_FILE"`},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err == nil {
		t.Fatal("Run() expected an error when the env file can't be read, got nil")
	}
	if result.Status() != domain.StatusErrored {
		t.Errorf("expected StatusErrored, got %v", result.Status())
	}
}

func TestDockerRunnerRunStepDuration(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "duration-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			{Name: "quick", Run: "echo quick"},
			{Name: "slow", Run: "sleep 1"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	if len(result.StepResults) != 2 {
		t.Fatalf("expected 2 step results, got %d", len(result.StepResults))
	}

	for _, sr := range result.StepResults {
		if sr.Duration <= 0 {
			t.Errorf("step %q: expected positive duration, got %v", sr.StepName, sr.Duration)
		}
	}

	slowDuration := result.StepResults[1].Duration
	if slowDuration < time.Second {
		t.Errorf("slow step duration %v is less than 1s", slowDuration)
	}

	expectedTotal := result.StepResults[0].Duration + result.StepResults[1].Duration
	if result.Duration() != expectedTotal {
		t.Errorf("job Duration() = %v, want %v", result.Duration(), expectedTotal)
	}
}

// TestDockerRunnerRunStepTimeout asserts a step timeout stops the runner
// promptly: Run returns within the timeout plus a small margin, the error
// names the timeout, and output produced before the deadline is kept —
// not discarded (gh-92, AUD-022).
func TestDockerRunnerRunStepTimeout(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "timeout-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			{Name: "slow", Run: "echo partial-output && sleep 20", Timeout: 1000},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	result, err := r.Run(ctx, job, dir, nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Run() expected an error for a step exceeding its timeout, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected timeout error, got: %v", err)
	}
	// Promptness is the point: the hung step must not hold the runner until
	// the process exits on its own — sleep 20 would take ~20s.
	if elapsed > 5*time.Second {
		t.Errorf("Run returned after %v, want within ~5s of the 1s timeout", elapsed)
	}

	// The timed-out step is still recorded, and output that arrived before
	// the deadline survives.
	if len(result.StepResults) != 1 {
		t.Fatalf("expected the timed-out step to be recorded, got %d step results", len(result.StepResults))
	}
	if got := result.StepResults[0].Stdout; !strings.Contains(got, "partial-output") {
		t.Errorf("output received before the deadline was discarded, got %q", got)
	}
}

func TestDockerRunnerRunStepWithinTimeout(t *testing.T) {
	r := requireDocker(t)
	dir := t.TempDir()

	job := domain.Job{
		Name:  "within-timeout-test",
		Image: "alpine:latest",
		Steps: []domain.Step{
			{Name: "quick", Run: "echo done", Timeout: 5000},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := r.Run(ctx, job, dir, nil)
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	if len(result.StepResults) != 1 {
		t.Fatalf("expected 1 step result, got %d", len(result.StepResults))
	}
	if !strings.Contains(result.StepResults[0].Stdout, "done") {
		t.Errorf("expected stdout to contain 'done', got %q", result.StepResults[0].Stdout)
	}
}
