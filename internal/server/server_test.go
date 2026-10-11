// Internal test package (exception to the external-package convention):
// the proto mapping functions under test are unexported by design (the
// mapping is a server-internal concern), so the assertions must live
// inside the package (AUD-017 disposition).
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	odysseyv1 "github.com/odyssey-ci-cd-platform/dispatcher/gen/proto/v1"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/runner"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeRunner returns pre-configured results keyed by job name.
type fakeRunner struct {
	results map[string]domain.JobResult
	errs    map[string]error

	mu    sync.Mutex
	calls []string
}

func (f *fakeRunner) Run(_ context.Context, job domain.Job, _ string, _ runner.StepSink) (domain.JobResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, job.Name)
	f.mu.Unlock()
	return f.results[job.Name], f.errs[job.Name]
}

func newPassedJob(name string) domain.JobResult {
	return domain.JobResult{
		JobName: name,
		StepResults: []domain.StepResult{
			{StepName: "echo", Stdout: "hello", ExitCode: domain.ExitSuccess},
		},
	}
}

func newFailedJob(name string) domain.JobResult {
	return domain.JobResult{
		JobName: name,
		StepResults: []domain.StepResult{
			{StepName: "fail", Stderr: "ouch", ExitCode: domain.ExitFailure},
		},
	}
}

func newErroredJob(name string) domain.JobResult {
	return domain.JobResult{
		JobName:  name,
		SetupErr: errors.New("container creation failed"),
	}
}

// fakeSink is an EventSink that records events in memory.
type fakeSink struct {
	mu     sync.Mutex
	events []domain.Event
}

func (f *fakeSink) Publish(_ context.Context, event domain.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	return nil
}

func (f *fakeSink) recorded() []domain.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.Event(nil), f.events...)
}

func TestDomainStatusToProto(t *testing.T) {
	tests := []struct {
		name string
		s    domain.Status
		want odysseyv1.Status
	}{
		{name: "passed", s: domain.StatusPassed, want: odysseyv1.Status_STATUS_PASSED},
		{name: "failed", s: domain.StatusFailed, want: odysseyv1.Status_STATUS_FAILED},
		{name: "errored", s: domain.StatusErrored, want: odysseyv1.Status_STATUS_ERRORED},
		{name: "pending", s: domain.StatusPending, want: odysseyv1.Status_STATUS_PENDING},
		{name: "running", s: domain.StatusRunning, want: odysseyv1.Status_STATUS_RUNNING},
		{name: "skipped", s: domain.StatusSkipped, want: odysseyv1.Status_STATUS_SKIPPED},
		{name: "unknown slug returns unspecified", s: domain.StatusUnknown, want: odysseyv1.Status_STATUS_UNSPECIFIED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domainStatusToProto(tt.s)
			if got != tt.want {
				t.Errorf("domainStatusToProto(%v) = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}

// TestRunPipelineRequiresInjectedRunner asserts a missing Runner is a
// configuration error — the server no longer builds a DockerRunner per
// request, which leaked a client on every call (AUD-014).
func TestRunPipelineRequiresInjectedRunner(t *testing.T) {
	s := &Server{}
	dir := t.TempDir()
	odysseyDir := filepath.Join(dir, ".odyssey")
	if err := os.MkdirAll(odysseyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pipelineTOML := `[pipeline]
name = "ci"
stages = ["build"]

[jobs.compile]
stage = "build"
image = "alpine:latest"
steps = [{ name = "run", run = "echo hi" }]
`
	if err := os.WriteFile(filepath.Join(odysseyDir, "pipeline.toml"), []byte(pipelineTOML), 0o644); err != nil {
		t.Fatal(err)
	}

	resp, err := s.RunPipeline(context.Background(), &odysseyv1.RunPipelineRequest{ProjectPath: dir})
	if status.Code(err) != codes.Internal {
		t.Errorf("code = %v, want Internal", status.Code(err))
	}
	if resp != nil && resp.PipelineName != "" {
		t.Errorf("expected an empty response, got pipeline %q", resp.PipelineName)
	}
}

func TestDomainStepResultToProto(t *testing.T) {
	tests := []struct {
		name string
		r    domain.StepResult
		want *odysseyv1.StepResult
	}{
		{
			name: "passed step with stdout only",
			r:    domain.StepResult{StepName: "build", Stdout: "compiled", ExitCode: domain.ExitSuccess},
			want: &odysseyv1.StepResult{
				StepName: "build",
				Stdout:   "compiled",
				ExitCode: 0,
				Status:   odysseyv1.Status_STATUS_PASSED,
			},
		},
		{
			name: "failed step with stderr only",
			r:    domain.StepResult{StepName: "test", Stderr: "assertion failed", ExitCode: domain.ExitFailure},
			want: &odysseyv1.StepResult{
				StepName: "test",
				Stderr:   "assertion failed",
				ExitCode: 1,
				Status:   odysseyv1.Status_STATUS_FAILED,
			},
		},
		{
			name: "step with stdout and stderr keeps the streams separate",
			r:    domain.StepResult{StepName: "lint", Stdout: "ok", Stderr: "warning", ExitCode: domain.ExitSuccess},
			want: &odysseyv1.StepResult{
				StepName: "lint",
				Stdout:   "ok",
				Stderr:   "warning",
				ExitCode: 0,
				Status:   odysseyv1.Status_STATUS_PASSED,
			},
		},
		{
			name: "infrastructure fault carries the error text and no fabricated output",
			r:    domain.StepResult{StepName: "build", ExitCode: domain.ExitNone, Err: errors.New("exec failed")},
			want: &odysseyv1.StepResult{
				StepName: "build",
				Error:    "exec failed",
				ExitCode: -1,
				Status:   odysseyv1.Status_STATUS_ERRORED,
			},
		},
		{
			name: "duration round-trips in milliseconds",
			r:    domain.StepResult{StepName: "build", Stdout: "ok", ExitCode: domain.ExitSuccess, Duration: 1500 * time.Millisecond},
			want: &odysseyv1.StepResult{
				StepName:   "build",
				Stdout:     "ok",
				ExitCode:   0,
				Status:     odysseyv1.Status_STATUS_PASSED,
				DurationMs: 1500,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domainStepResultToProto(tt.r)
			if got.StepName != tt.want.StepName {
				t.Errorf("StepName = %q, want %q", got.StepName, tt.want.StepName)
			}
			if got.Stdout != tt.want.Stdout {
				t.Errorf("Stdout = %q, want %q", got.Stdout, tt.want.Stdout)
			}
			if got.Stderr != tt.want.Stderr {
				t.Errorf("Stderr = %q, want %q", got.Stderr, tt.want.Stderr)
			}
			if got.Error != tt.want.Error {
				t.Errorf("Error = %q, want %q", got.Error, tt.want.Error)
			}
			if got.ExitCode != tt.want.ExitCode {
				t.Errorf("ExitCode = %d, want %d", got.ExitCode, tt.want.ExitCode)
			}
			if got.Status != tt.want.Status {
				t.Errorf("Status = %v, want %v", got.Status, tt.want.Status)
			}
			if got.DurationMs != tt.want.DurationMs {
				t.Errorf("DurationMs = %d, want %d", got.DurationMs, tt.want.DurationMs)
			}
		})
	}
}

func TestDomainJobResultToProto(t *testing.T) {
	passedStep := domain.StepResult{StepName: "s", Stdout: "ok", ExitCode: domain.ExitSuccess}

	tests := []struct {
		name string
		r    domain.JobResult
		want func(*odysseyv1.JobResult)
	}{
		{
			name: "job with passed steps",
			r: domain.JobResult{
				JobName:     "build",
				StepResults: []domain.StepResult{passedStep, passedStep},
			},
			want: func(got *odysseyv1.JobResult) {
				if len(got.StepResults) != 2 {
					t.Errorf("expected 2 step results, got %d", len(got.StepResults))
				}
				if got.Status != odysseyv1.Status_STATUS_PASSED {
					t.Errorf("Status = %v, want STATUS_PASSED", got.Status)
				}
			},
		},
		{
			name: "job with setup error returns errored and empty steps",
			r: domain.JobResult{
				JobName:  "dead",
				SetupErr: errors.New("image pull failed"),
			},
			want: func(got *odysseyv1.JobResult) {
				if len(got.StepResults) != 0 {
					t.Errorf("expected 0 step results, got %d", len(got.StepResults))
				}
				if got.Status != odysseyv1.Status_STATUS_ERRORED {
					t.Errorf("Status = %v, want STATUS_ERRORED", got.Status)
				}
			},
		},
		{
			name: "empty job with no setup error returns pending",
			r:    domain.JobResult{JobName: "noop"},
			want: func(got *odysseyv1.JobResult) {
				if len(got.StepResults) != 0 {
					t.Errorf("expected 0 step results, got %d", len(got.StepResults))
				}
				if got.Status != odysseyv1.Status_STATUS_PENDING {
					t.Errorf("Status = %v, want STATUS_PENDING", got.Status)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domainJobResultToProto(tt.r)
			if got.JobName != tt.r.JobName {
				t.Errorf("JobName = %q, want %q", got.JobName, tt.r.JobName)
			}
			tt.want(got)
		})
	}
}

func TestDomainStageResultToProto(t *testing.T) {
	t.Run("empty stage returns pending", func(t *testing.T) {
		r := domain.StageResult{StageName: "empty-stage"}
		got := domainStageResultToProto(r)
		if len(got.JobResults) != 0 {
			t.Errorf("expected 0 job results, got %d", len(got.JobResults))
		}
		if got.Status != odysseyv1.Status_STATUS_PENDING {
			t.Errorf("Status = %v, want STATUS_PENDING", got.Status)
		}
	})

	t.Run("stage with passed and failed jobs", func(t *testing.T) {
		r := domain.StageResult{
			StageName: "test-stage",
			JobResults: []domain.JobResult{
				newPassedJob("lint"),
				newFailedJob("unit"),
			},
		}
		got := domainStageResultToProto(r)
		if len(got.JobResults) != 2 {
			t.Fatalf("expected 2 job results, got %d", len(got.JobResults))
		}
		// Worst status = Failed
		if got.Status != odysseyv1.Status_STATUS_FAILED {
			t.Errorf("Status = %v, want STATUS_FAILED", got.Status)
		}
	})

	t.Run("errored job takes precedence", func(t *testing.T) {
		r := domain.StageResult{
			StageName: "mixed",
			JobResults: []domain.JobResult{
				newPassedJob("good"),
				newErroredJob("dead"),
				newFailedJob("bad"),
			},
		}
		got := domainStageResultToProto(r)
		if len(got.JobResults) != 3 {
			t.Fatalf("expected 3 job results, got %d", len(got.JobResults))
		}
		if got.Status != odysseyv1.Status_STATUS_ERRORED {
			t.Errorf("Status = %v, want STATUS_ERRORED", got.Status)
		}
	})
}

func TestDomainPipelineResultToProto(t *testing.T) {
	t.Run("empty pipeline returns pending", func(t *testing.T) {
		r := domain.PipelineResult{PipelineName: "noop"}
		got := domainPipelineResultToProto(r)
		if got.PipelineName != "noop" {
			t.Errorf("PipelineName = %q, want %q", got.PipelineName, "noop")
		}
		if len(got.StageResults) != 0 {
			t.Errorf("expected 0 stage results, got %d", len(got.StageResults))
		}
		if got.Status != odysseyv1.Status_STATUS_PENDING {
			t.Errorf("Status = %v, want STATUS_PENDING", got.Status)
		}
	})

	t.Run("multi-stage pipeline round-trips names", func(t *testing.T) {
		r := domain.PipelineResult{
			PipelineName: "ci",
			StageResults: []domain.StageResult{
				{
					StageName: "build-stage",
					JobResults: []domain.JobResult{
						{JobName: "compile", StepResults: []domain.StepResult{
							{StepName: "go-build", ExitCode: domain.ExitSuccess},
						}},
					},
				},
				{
					StageName: "test-stage",
					JobResults: []domain.JobResult{
						{JobName: "unit", StepResults: []domain.StepResult{
							{StepName: "go-test", ExitCode: domain.ExitFailure},
						}},
					},
				},
			},
		}
		got := domainPipelineResultToProto(r)

		if len(got.StageResults) != 2 {
			t.Fatalf("expected 2 stage results, got %d", len(got.StageResults))
		}
		if got.StageResults[0].StageName != "build-stage" {
			t.Errorf("stage 0 name = %q", got.StageResults[0].StageName)
		}
		if got.StageResults[1].StageName != "test-stage" {
			t.Errorf("stage 1 name = %q", got.StageResults[1].StageName)
		}
		if got.Status != odysseyv1.Status_STATUS_FAILED {
			t.Errorf("Status = %v, want STATUS_FAILED", got.Status)
		}
	})
}

const bufSize = 1024 * 1024

// writeODysseyConfig writes a minimal .odyssey/pipeline.toml to dir.
func writeODysseyConfig(t *testing.T, dir string) {
	t.Helper()
	odysseyDir := filepath.Join(dir, ".odyssey")
	if err := os.Mkdir(odysseyDir, 0o755); err != nil {
		t.Fatalf("mkdir .odyssey: %v", err)
	}
	cfg := fmt.Sprintf(`[pipeline]
name = "test-pipeline"
stages = ["%s"]

[jobs.%s]
stage = "%s"
image = "alpine"
steps = [{ name = "echo", run = "echo hi" }]
`, "test-stage", "test-job", "test-stage")
	if err := os.WriteFile(filepath.Join(odysseyDir, "pipeline.toml"), []byte(cfg), 0o644); err != nil {
		t.Fatalf("write pipeline.toml: %v", err)
	}
}

// bufDialer returns a ContextDialer that connects to the bufconn listener.
func bufDialer(lis *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	}
}

// TestServerRunPipeline_EmitsLifecycleEvents asserts that a server with a
// configured event sink emits pipeline started and finished events when a
// pipeline runs (gh-52): hosted runs must flow lifecycle events onto the bus.
func TestServerRunPipeline_EmitsLifecycleEvents(t *testing.T) {
	dir := t.TempDir()
	writeODysseyConfig(t, dir)

	fake := &fakeRunner{
		results: map[string]domain.JobResult{
			"test-job": newPassedJob("test-job"),
		},
		errs: map[string]error{},
	}
	sink := &fakeSink{}

	srv := Server{Runner: fake, Events: sink}
	resp, err := srv.RunPipeline(context.Background(), &odysseyv1.RunPipelineRequest{ProjectPath: dir})
	if err != nil {
		t.Fatalf("RunPipeline returned error: %v", err)
	}
	if resp.Status != odysseyv1.Status_STATUS_PASSED {
		t.Errorf("Status = %v, want STATUS_PASSED", resp.Status)
	}

	types := make([]string, 0, len(sink.recorded()))
	for _, e := range sink.recorded() {
		if e.Pipeline != "test-pipeline" {
			t.Errorf("event %q carries pipeline %q, want %q", e.Type, e.Pipeline, "test-pipeline")
		}
		types = append(types, e.Type)
	}
	// One job in the fixture, so the sequence is deterministic (gh-52 emits
	// both pipeline and job lifecycle events).
	want := []string{
		domain.EventPipelineStarted,
		domain.EventJobStarted,
		domain.EventJobFinished,
		domain.EventPipelineFinished,
	}
	if !slices.Equal(types, want) {
		t.Errorf("emitted events = %v, want %v", types, want)
	}
}

func TestServerRunPipeline_Success(t *testing.T) {
	dir := t.TempDir()
	writeODysseyConfig(t, dir)

	fake := &fakeRunner{
		results: map[string]domain.JobResult{
			"test-job": newPassedJob("test-job"),
		},
		errs: map[string]error{},
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	odysseyv1.RegisterOdysseyServiceServer(srv, &Server{Runner: fake})
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()
	t.Cleanup(srv.Stop)

	ctx := context.Background()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(bufDialer(lis)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := odysseyv1.NewOdysseyServiceClient(conn)
	resp, err := client.RunPipeline(ctx, &odysseyv1.RunPipelineRequest{
		ProjectPath: dir,
	})
	if err != nil {
		t.Fatalf("RunPipeline returned error: %v", err)
	}

	if resp.PipelineName != "test-pipeline" {
		t.Errorf("PipelineName = %q, want %q", resp.PipelineName, "test-pipeline")
	}
	if resp.Status != odysseyv1.Status_STATUS_PASSED {
		t.Errorf("Status = %v, want STATUS_PASSED", resp.Status)
	}
	if len(resp.StageResults) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(resp.StageResults))
	}
	if len(resp.StageResults[0].JobResults) != 1 {
		t.Fatalf("expected 1 job, got %d", len(resp.StageResults[0].JobResults))
	}
	if resp.StageResults[0].JobResults[0].JobName != "test-job" {
		t.Errorf("JobName = %q, want %q", resp.StageResults[0].JobResults[0].JobName, "test-job")
	}
}

func TestServerRunPipeline_InvalidPath(t *testing.T) {
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	odysseyv1.RegisterOdysseyServiceServer(srv, &Server{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx := context.Background()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(bufDialer(lis)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := odysseyv1.NewOdysseyServiceClient(conn)
	_, err = client.RunPipeline(ctx, &odysseyv1.RunPipelineRequest{
		ProjectPath: "/nonexistent/path",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent project path, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("gRPC code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestServerRunPipeline_RunnerCreationFails(t *testing.T) {
	dir := t.TempDir()
	writeODysseyConfig(t, dir)

	// No "://" in DOCKER_HOST makes NewDockerRunner fail at construction,
	// exercising the codes.Internal path in RunPipeline.
	t.Setenv("DOCKER_HOST", "invalid-host")

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	odysseyv1.RegisterOdysseyServiceServer(srv, &Server{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx := context.Background()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(bufDialer(lis)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := odysseyv1.NewOdysseyServiceClient(conn)
	_, err = client.RunPipeline(ctx, &odysseyv1.RunPipelineRequest{
		ProjectPath: dir,
	})
	if err == nil {
		t.Fatal("expected error when runner creation fails, got nil")
	}
	if status.Code(err) != codes.Internal {
		t.Errorf("gRPC code = %v, want Internal", status.Code(err))
	}
}
