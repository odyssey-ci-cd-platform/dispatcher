package server

import (
	odysseyv1 "github.com/odyssey-ci-cd-platform/dispatcher/gen/proto/v1"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

// domainStatusToProto maps a domain.Status to the corresponding protobuf
// Status enum. Unknown statuses produce STATUS_UNSPECIFIED.
func domainStatusToProto(s domain.Status) odysseyv1.Status {
	switch s {
	case domain.StatusPassed:
		return odysseyv1.Status_STATUS_PASSED
	case domain.StatusFailed:
		return odysseyv1.Status_STATUS_FAILED
	case domain.StatusErrored:
		return odysseyv1.Status_STATUS_ERRORED
	case domain.StatusPending:
		return odysseyv1.Status_STATUS_PENDING
	case domain.StatusRunning:
		return odysseyv1.Status_STATUS_RUNNING
	case domain.StatusSkipped:
		return odysseyv1.Status_STATUS_SKIPPED
	default:
		return odysseyv1.Status_STATUS_UNSPECIFIED
	}
}

// domainStepResultToProto converts a single domain step result to its
// protobuf form. Stdout and stderr stay separate; an infrastructure fault
// travels in Error (AUD-004).
func domainStepResultToProto(r domain.StepResult) *odysseyv1.StepResult {
	p := &odysseyv1.StepResult{
		StepName:   r.StepName,
		Stdout:     r.Stdout,
		Stderr:     r.Stderr,
		ExitCode:   int32(r.ExitCode),
		Status:     domainStatusToProto(r.Status()),
		DurationMs: r.Duration.Milliseconds(),
	}
	if r.Err != nil {
		p.Error = r.Err.Error()
	}
	return p
}

// domainJobResultToProto converts a single domain job result to its
// protobuf form. If SetupErr is set on the domain result, Status will be
// STATUS_ERRORED and StepResults will be empty.
func domainJobResultToProto(r domain.JobResult) *odysseyv1.JobResult {
	protoStepResults := make([]*odysseyv1.StepResult, 0, len(r.StepResults))
	for _, sr := range r.StepResults {
		protoStepResults = append(protoStepResults, domainStepResultToProto(sr))
	}
	p := &odysseyv1.JobResult{
		JobName:     r.JobName,
		Status:      domainStatusToProto(r.Status()),
		StepResults: protoStepResults,
		// Steps run sequentially within a job, so the summed duration is the
		// job's wall time.
		DurationMs: r.Duration().Milliseconds(),
	}
	if r.SetupErr != nil {
		p.Error = r.SetupErr.Error()
	}
	return p
}

// domainStageResultToProto converts a single domain stage result to its
// protobuf form, including all nested job results.
func domainStageResultToProto(r domain.StageResult) *odysseyv1.StageResult {
	protoJobResults := make([]*odysseyv1.JobResult, 0, len(r.JobResults))
	for _, jr := range r.JobResults {
		protoJobResults = append(protoJobResults, domainJobResultToProto(jr))
	}
	return &odysseyv1.StageResult{
		StageName:  r.StageName,
		Status:     domainStatusToProto(r.Status()),
		JobResults: protoJobResults,
		DurationMs: r.Duration.Milliseconds(),
	}
}

// domainPipelineResultToProto converts a complete domain pipeline result
// (including all nested stages, jobs, and steps) into the protobuf
// RunPipelineResponse returned to the caller.
func domainPipelineResultToProto(r domain.PipelineResult) odysseyv1.RunPipelineResponse {
	protoStageResults := make([]*odysseyv1.StageResult, 0, len(r.StageResults))
	for _, sr := range r.StageResults {
		protoStageResults = append(protoStageResults, domainStageResultToProto(sr))
	}
	return odysseyv1.RunPipelineResponse{
		PipelineName: r.PipelineName,
		Status:       domainStatusToProto(r.Status()),
		StageResults: protoStageResults,
		DurationMs:   r.Duration.Milliseconds(),
	}
}
