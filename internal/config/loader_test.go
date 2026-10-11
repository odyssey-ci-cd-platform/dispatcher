package config_test

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/config"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

// writeOdysseyConfig creates a .odyssey directory under dir, writes
// pipeline.toml with content, and optionally writes env.toml if envContent
// is non-empty.
func writeOdysseyConfig(t *testing.T, dir, pipelineContent, envContent string) {
	t.Helper()
	odysseyDir := filepath.Join(dir, ".odyssey")
	if err := os.MkdirAll(odysseyDir, 0o755); err != nil {
		t.Fatalf("failed to create .odyssey dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(odysseyDir, "pipeline.toml"), []byte(pipelineContent), 0o644); err != nil {
		t.Fatalf("failed to write pipeline.toml: %v", err)
	}
	if envContent != "" {
		if err := os.WriteFile(filepath.Join(odysseyDir, "env.toml"), []byte(envContent), 0o644); err != nil {
			t.Fatalf("failed to write env.toml: %v", err)
		}
	}
}

// TestLoadRejectsUnknownKeys asserts a typo'd configuration key fails the
// load naming the key, instead of being silently ignored (AUD-006).
func TestLoadRejectsUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	writeOdysseyConfig(t, dir, `[pipeline]
name = "ci"
stages = ["build"]

[jobs.compile]
stage = "build"
image = "alpine:latest"

[[jobs.compile.steps]]
name = "run"
run = "echo hi"
timout = 500
`, "")

	_, err := config.Load(dir)
	if err == nil {
		t.Fatal("Load() expected an error for an unknown key, got nil")
	}
	if !strings.Contains(err.Error(), "timout") {
		t.Errorf("error should name the unknown key, got: %v", err)
	}
}

// TestLoadRejectsStageWithoutJobs asserts a declared stage with no jobs
// fails validation instead of leaving the pipeline Pending forever (AUD-007).
func TestLoadRejectsStageWithoutJobs(t *testing.T) {
	dir := t.TempDir()
	writeOdysseyConfig(t, dir, `[pipeline]
name = "ci"
stages = ["build", "test"]

[jobs.compile]
stage = "build"
image = "alpine:latest"
steps = [{ name = "run", run = "echo hi" }]
`, "")

	_, err := config.Load(dir)
	if err == nil {
		t.Fatal("Load() expected an error for a stage with no jobs, got nil")
	}
	if !strings.Contains(err.Error(), "test") || !strings.Contains(err.Error(), "no jobs") {
		t.Errorf("error should name the empty stage, got: %v", err)
	}
}

// TestLoadJobOrderIsDeterministic asserts jobs within a stage come back in
// a stable order (sorted by key) regardless of map iteration (AUD-008).
func TestLoadJobOrderIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	writeOdysseyConfig(t, dir, `[pipeline]
name = "ci"
stages = ["build"]

[jobs.zebra]
stage = "build"
image = "alpine:latest"
steps = [{ name = "run", run = "echo zebra" }]

[jobs.aardvark]
stage = "build"
image = "alpine:latest"
steps = [{ name = "run", run = "echo aardvark" }]
`, "")

	pipeline, err := config.Load(dir)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	got := []string{pipeline.Stages[0].Jobs[0].Name, pipeline.Stages[0].Jobs[1].Name}
	want := []string{"aardvark", "zebra"}
	if !slices.Equal(got, want) {
		t.Errorf("job order = %v, want sorted %v", got, want)
	}
}

// TestLoadRejectsNegativeStepTimeout asserts a negative step timeout fails
// validation — the unit is milliseconds and negatives are meaningless
// (AUD-011).
func TestLoadRejectsNegativeStepTimeout(t *testing.T) {
	dir := t.TempDir()
	writeOdysseyConfig(t, dir, `[pipeline]
name = "ci"
stages = ["build"]

[jobs.compile]
stage = "build"
image = "alpine:latest"
steps = [{ name = "run", run = "echo hi", timeout = -5 }]
`, "")

	_, err := config.Load(dir)
	if err == nil {
		t.Fatal("Load() expected an error for a negative timeout, got nil")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("error should mention the timeout, got: %v", err)
	}
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name         string
		pipelineTOML string
		envTOML      string
		want         domain.Pipeline
		wantErr      bool
		errContains  []string
	}{
		{
			name: "valid minimal pipeline",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = [
  { name = "run tests", run = "go test ./..." },
]
`),
			want: domain.Pipeline{
				Name: "ci",
				Stages: []domain.Stage{
					{
						Name: "test",
						Jobs: []domain.Job{
							{
								Name:  "unit",
								Image: "alpine:latest",
								Env:   map[string]string{},
								Steps: []domain.Step{
									{Name: "run tests", Run: "go test ./..."},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "valid multi-stage pipeline",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "full-ci"
stages = ["build", "test", "deploy"]

[jobs.compile]
stage = "build"
image = "golang:1.21"
steps = [
  { name = "build binary", run = "go build -o app ./cmd/server" },
]

[jobs.unit-tests]
stage = "test"
image = "golang:1.21"
setup = ["go mod download"]
env = { DB_HOST = "localhost" }
steps = [
  { name = "unit tests", run = "go test ./..." },
  { name = "race tests", run = "go test -race ./..." },
]

[jobs.deploy-app]
stage = "deploy"
image = "alpine:latest"
steps = [
  { name = "deploy", run = "echo deployed" },
]
`),
			want: domain.Pipeline{
				Name: "full-ci",
				Stages: []domain.Stage{
					{
						Name: "build",
						Jobs: []domain.Job{
							{
								Name:  "compile",
								Image: "golang:1.21",
								Env:   map[string]string{},
								Steps: []domain.Step{
									{Name: "build binary", Run: "go build -o app ./cmd/server"},
								},
							},
						},
					},
					{
						Name: "test",
						Jobs: []domain.Job{
							{
								Name:  "unit-tests",
								Image: "golang:1.21",
								Setup: []string{"go mod download"},
								Env:   map[string]string{"DB_HOST": "localhost"},
								Steps: []domain.Step{
									{Name: "unit tests", Run: "go test ./..."},
									{Name: "race tests", Run: "go test -race ./..."},
								},
							},
						},
					},
					{
						Name: "deploy",
						Jobs: []domain.Job{
							{
								Name:  "deploy-app",
								Image: "alpine:latest",
								Env:   map[string]string{},
								Steps: []domain.Step{
									{Name: "deploy", Run: "echo deployed"},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "env merging - job overrides global",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
env = { FOO = "job-value", BAR = "from-job" }
steps = [
  { name = "step", run = "echo hello" },
]
`),
			envTOML: strings.TrimSpace(`
[env]
FOO = "global-value"
BAZ = "from-global"
`),
			want: domain.Pipeline{
				Name: "ci",
				Stages: []domain.Stage{
					{
						Name: "test",
						Jobs: []domain.Job{
							{
								Name:  "unit",
								Image: "alpine:latest",
								Env: map[string]string{
									"FOO": "job-value",
									"BAR": "from-job",
									"BAZ": "from-global",
								},
								Steps: []domain.Step{
									{Name: "step", Run: "echo hello"},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "env.toml only - no job env",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = [
  { name = "step", run = "echo hello" },
]
`),
			envTOML: strings.TrimSpace(`
[env]
SHARED_SECRET = "secret123"
`),
			want: domain.Pipeline{
				Name: "ci",
				Stages: []domain.Stage{
					{
						Name: "test",
						Jobs: []domain.Job{
							{
								Name:  "unit",
								Image: "alpine:latest",
								Env:   map[string]string{"SHARED_SECRET": "secret123"},
								Steps: []domain.Step{
									{Name: "step", Run: "echo hello"},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "without env.toml - jobs work fine",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = [
  { name = "step", run = "echo hello" },
]
`),
			want: domain.Pipeline{
				Name: "ci",
				Stages: []domain.Stage{
					{
						Name: "test",
						Jobs: []domain.Job{
							{
								Name:  "unit",
								Image: "alpine:latest",
								Env:   map[string]string{},
								Steps: []domain.Step{
									{Name: "step", Run: "echo hello"},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "empty stages in pipeline",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = []

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = [
  { name = "step", run = "echo hello" },
]
`),
			wantErr:     true,
			errContains: []string{"at least one stage"},
		},
		{
			name: "duplicate stage names",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test", "test"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = [
  { name = "step", run = "echo hello" },
]
`),
			wantErr:     true,
			errContains: []string{`duplicate stage name: "test"`},
		},
		{
			name: "no jobs defined",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]
`),
			wantErr:     true,
			errContains: []string{"at least one job"},
		},
		{
			name: "job references undeclared stage",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["build"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = [
  { name = "step", run = "echo hello" },
]
`),
			wantErr:     true,
			errContains: []string{`stage "test" is not declared`},
		},
		{
			name: "job with empty stage",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = ""
image = "alpine:latest"
steps = [
  { name = "step", run = "echo hello" },
]
`),
			wantErr:     true,
			errContains: []string{`stage must not be empty`},
		},
		{
			name: "job with empty image",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = ""
steps = [
  { name = "step", run = "echo hello" },
]
`),
			wantErr:     true,
			errContains: []string{`image must not be empty`},
		},
		{
			name: "job with no steps",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = []
`),
			wantErr:     true,
			errContains: []string{`must define at least one step`},
		},
		{
			name: "step with empty name",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = [
  { name = "", run = "echo hello" },
]
`),
			wantErr:     true,
			errContains: []string{`name must not be empty`},
		},
		{
			name: "step with empty run command",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = [
  { name = "step", run = "" },
]
`),
			wantErr:     true,
			errContains: []string{`run must not be empty`},
		},
		{
			name:         "missing pipeline.toml",
			pipelineTOML: "",
			wantErr:      true,
			errContains:  []string{"failed to read pipeline.toml"},
		},
		{
			name:         "invalid TOML syntax",
			pipelineTOML: `this is not valid toml {{{`,
			wantErr:      true,
			errContains:  []string{"failed to read pipeline.toml"},
		},
		{
			name: "malformed env.toml",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = "alpine:latest"
steps = [
  { name = "step", run = "echo hello" },
]
`),
			envTOML:     "this is not valid toml {{{",
			wantErr:     true,
			errContains: []string{"failed to read env.toml"},
		},
		{
			name: "multiple validation errors reported together",
			pipelineTOML: strings.TrimSpace(`
[pipeline]
name = "ci"
stages = ["test"]

[jobs.unit]
stage = "test"
image = ""
steps = []
`),
			wantErr:     true,
			errContains: []string{"image must not be empty", "must define at least one step"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()

			// For the missing file case, don't create anything
			if tt.pipelineTOML != "" {
				writeOdysseyConfig(t, dir, tt.pipelineTOML, tt.envTOML)
			}

			got, err := config.Load(dir)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Load() unexpected error: %v", err)
					return
				}
				for _, want := range tt.errContains {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("Load() error = %v, want error containing %q", err, want)
					}
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Load() succeeded unexpectedly")
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Load() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
