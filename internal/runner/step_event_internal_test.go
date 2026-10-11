package runner

import (
	"errors"
	"testing"
	"time"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

func TestStepFinishedEventCarriesOutcomeMetadata(t *testing.T) {
	// Requirement (gh-85, ADR 0002): the finished payload carries exit code
	// and duration so the results recorder can fill its steps table —
	// stdout/stderr content stays off the bus (ADR 0001).
	event := stepFinishedEvent("build", domain.StepResult{
		StepName: "compile",
		ExitCode: 3,
		Duration: 1500 * time.Millisecond,
	})

	if got := event.Payload["status"]; got != domain.StatusFailed.String() {
		t.Errorf("status = %q, want %q", got, domain.StatusFailed.String())
	}
	if got := event.Payload["exit_code"]; got != "3" {
		t.Errorf("exit_code = %q, want \"3\"", got)
	}
	if got := event.Payload["duration_ms"]; got != "1500" {
		t.Errorf("duration_ms = %q, want \"1500\"", got)
	}
}

func TestStepFinishedEventOmitsAbsentExitCode(t *testing.T) {
	// A step whose process never produced an exit code (ExitNone) omits the
	// key, so the recorded column stays NULL instead of a sentinel value.
	event := stepFinishedEvent("build", domain.StepResult{
		StepName: "compile",
		ExitCode: domain.ExitNone,
		Err:      errors.New("exec failed"),
		Duration: time.Second,
	})

	if got := event.Payload["status"]; got != domain.StatusErrored.String() {
		t.Errorf("status = %q, want %q", got, domain.StatusErrored.String())
	}
	if _, ok := event.Payload["exit_code"]; ok {
		t.Errorf("exit_code present for a step with no exit code, want omitted")
	}
	if got := event.Payload["duration_ms"]; got != "1000" {
		t.Errorf("duration_ms = %q, want \"1000\"", got)
	}
}
