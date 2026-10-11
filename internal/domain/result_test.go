package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

func TestStepResultStatus(t *testing.T) {
	tests := []struct {
		name string
		r    domain.StepResult
		want domain.Status
	}{
		{
			name: "success exit code returns passed",
			r:    domain.StepResult{ExitCode: domain.ExitSuccess},
			want: domain.StatusPassed,
		},
		{
			name: "failure exit code returns failed",
			r:    domain.StepResult{ExitCode: domain.ExitFailure},
			want: domain.StatusFailed,
		},
		{
			name: "infrastructure fault returns errored regardless of exit code",
			r:    domain.StepResult{ExitCode: domain.ExitNone, Err: errors.New("exec failed")},
			want: domain.StatusErrored,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.r.Status()
			if got != tt.want {
				t.Errorf("Status() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobResultStatus(t *testing.T) {
	tests := []struct {
		name string
		r    domain.JobResult
		want domain.Status
	}{
		{
			name: "setup error returns errored",
			r:    domain.JobResult{SetupErr: errors.New("container creation failed")},
			want: domain.StatusErrored,
		},
		{
			name: "setup error wins even when steps present",
			r: domain.JobResult{
				SetupErr:    errors.New("image pull failed"),
				StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}},
			},
			want: domain.StatusErrored,
		},
		{
			name: "no steps and no error returns pending",
			r:    domain.JobResult{},
			want: domain.StatusPending,
		},
		{
			name: "all steps passed returns passed",
			r: domain.JobResult{
				StepResults: []domain.StepResult{
					{ExitCode: domain.ExitSuccess},
					{ExitCode: domain.ExitSuccess},
				},
			},
			want: domain.StatusPassed,
		},
		{
			name: "any step failed returns failed",
			r: domain.JobResult{
				StepResults: []domain.StepResult{
					{ExitCode: domain.ExitSuccess},
					{ExitCode: domain.ExitFailure},
				},
			},
			want: domain.StatusFailed,
		},
		{
			name: "first step failed returns failed",
			r: domain.JobResult{
				StepResults: []domain.StepResult{
					{ExitCode: domain.ExitFailure},
					{ExitCode: domain.ExitSuccess},
				},
			},
			want: domain.StatusFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.r.Status()
			if got != tt.want {
				t.Errorf("Status() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobResultDuration(t *testing.T) {
	tests := []struct {
		name string
		r    domain.JobResult
		want time.Duration
	}{
		{
			name: "no steps returns zero",
			r:    domain.JobResult{},
			want: 0,
		},
		{
			name: "sums step durations",
			r: domain.JobResult{
				StepResults: []domain.StepResult{
					{Duration: 1 * time.Second},
					{Duration: 2500 * time.Millisecond},
					{Duration: 500 * time.Millisecond},
				},
			},
			want: 4 * time.Second,
		},
		{
			name: "single step returns its duration",
			r: domain.JobResult{
				StepResults: []domain.StepResult{
					{Duration: 3 * time.Second},
				},
			},
			want: 3 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.r.Duration()
			if got != tt.want {
				t.Errorf("Duration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStageResultStatus(t *testing.T) {
	tests := []struct {
		name string
		r    domain.StageResult
		want domain.Status
	}{
		{
			name: "no jobs returns pending",
			r:    domain.StageResult{},
			want: domain.StatusPending,
		},
		{
			name: "all jobs passed returns passed",
			r: domain.StageResult{
				JobResults: []domain.JobResult{
					{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}},
					{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}},
				},
			},
			want: domain.StatusPassed,
		},
		{
			name: "any job failed returns failed over passed",
			r: domain.StageResult{
				JobResults: []domain.JobResult{
					{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}},
					{StepResults: []domain.StepResult{{ExitCode: domain.ExitFailure}}},
				},
			},
			want: domain.StatusFailed,
		},
		{
			name: "errored takes precedence over failed",
			r: domain.StageResult{
				JobResults: []domain.JobResult{
					{StepResults: []domain.StepResult{{ExitCode: domain.ExitFailure}}},
					{SetupErr: errors.New("setup failed")},
				},
			},
			want: domain.StatusErrored,
		},
		{
			name: "errored takes precedence over passed",
			r: domain.StageResult{
				JobResults: []domain.JobResult{
					{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}},
					{SetupErr: errors.New("image pull failed")},
				},
			},
			want: domain.StatusErrored,
		},
		{
			name: "pending beats passed",
			r: domain.StageResult{
				JobResults: []domain.JobResult{
					{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}},
					{}, // empty job result → Pending
				},
			},
			want: domain.StatusPending,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.r.Status()
			if got != tt.want {
				t.Errorf("Status() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStageResultDuration(t *testing.T) {
	// Duration is a wall-clock field the orchestrator measures (AUD-009);
	// it is no longer computed by summing concurrent jobs.
	r := domain.StageResult{Duration: 1500 * time.Millisecond}
	if r.Duration != 1500*time.Millisecond {
		t.Errorf("Duration = %v, want the measured field value", r.Duration)
	}
}

func TestPipelineResultStatus(t *testing.T) {
	tests := []struct {
		name string
		r    domain.PipelineResult
		want domain.Status
	}{
		{
			name: "no stages returns pending",
			r:    domain.PipelineResult{},
			want: domain.StatusPending,
		},
		{
			name: "all stages passed returns passed",
			r: domain.PipelineResult{
				StageResults: []domain.StageResult{
					{JobResults: []domain.JobResult{{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}}}},
					{JobResults: []domain.JobResult{{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}}}},
				},
			},
			want: domain.StatusPassed,
		},
		{
			name: "any stage failed returns failed over passed",
			r: domain.PipelineResult{
				StageResults: []domain.StageResult{
					{JobResults: []domain.JobResult{{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}}}},
					{JobResults: []domain.JobResult{{StepResults: []domain.StepResult{{ExitCode: domain.ExitFailure}}}}},
				},
			},
			want: domain.StatusFailed,
		},
		{
			name: "errored takes precedence over failed",
			r: domain.PipelineResult{
				StageResults: []domain.StageResult{
					{JobResults: []domain.JobResult{{StepResults: []domain.StepResult{{ExitCode: domain.ExitFailure}}}}},
					{JobResults: []domain.JobResult{{SetupErr: errors.New("setup failed")}}},
				},
			},
			want: domain.StatusErrored,
		},
		{
			name: "errored takes precedence over passed",
			r: domain.PipelineResult{
				StageResults: []domain.StageResult{
					{JobResults: []domain.JobResult{{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}}}},
					{JobResults: []domain.JobResult{{SetupErr: errors.New("image pull failed")}}},
				},
			},
			want: domain.StatusErrored,
		},
		{
			name: "pending beats passed",
			r: domain.PipelineResult{
				StageResults: []domain.StageResult{
					{JobResults: []domain.JobResult{{StepResults: []domain.StepResult{{ExitCode: domain.ExitSuccess}}}}},
					{}, // empty stage result → Pending
				},
			},
			want: domain.StatusPending,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.r.Status()
			if got != tt.want {
				t.Errorf("Status() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPipelineResultDuration(t *testing.T) {
	// Duration is a wall-clock field the orchestrator measures (AUD-009);
	// it is no longer computed by summing concurrent stages.
	r := domain.PipelineResult{Duration: 2 * time.Second}
	if r.Duration != 2*time.Second {
		t.Errorf("Duration = %v, want the measured field value", r.Duration)
	}
}
