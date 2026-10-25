package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/common"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

const workDir = "/app"

// shortID truncates a Docker container ID to 12 characters for logging.
func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

type DockerRunner struct {
	client *client.Client
	logger *slog.Logger
}

func NewDockerRunner(logger *slog.Logger) (*DockerRunner, error) {
	c, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &DockerRunner{client: c, logger: logger}, nil
}

// Close releases the Docker client's connections. The runner is
// process-scoped on a server, so Close belongs to shutdown (AUD-014).
func (r *DockerRunner) Close() error {
	return r.client.Close()
}

// Ping verifies the Docker daemon is reachable. Tests use it to skip
// cleanly instead of failing or hanging when the daemon is down (AUD-017).
func (r *DockerRunner) Ping(ctx context.Context) error {
	_, err := r.client.Ping(ctx, client.PingOptions{})
	return err
}

// loggerFromCtx returns the logger attached to ctx, falling back to the
// runner's own logger when none is present.
func (r *DockerRunner) loggerFromCtx(ctx context.Context) *slog.Logger {
	return common.LoggerFromContext(ctx, r.logger)
}

func (r *DockerRunner) Run(ctx context.Context, job domain.Job, projectPath string, events StepSink) (domain.JobResult, error) {
	jobResult := domain.JobResult{JobName: job.Name}

	// Pull image
	err := r.pullImage(ctx, job.Image)
	if err != nil {
		err = fmt.Errorf("failed to pull image %q: %w", job.Image, err)
		jobResult.SetupErr = err
		return jobResult, err
	}
	r.loggerFromCtx(ctx).Info("docker image pulled", "image", job.Image)

	// Create container
	containerID, err := r.createContainer(ctx, job, projectPath)
	if err != nil {
		err = fmt.Errorf("failed to create container: %w", err)
		jobResult.SetupErr = err
		return jobResult, err
	}
	r.loggerFromCtx(ctx).Info("container created", "containerID", shortID(containerID))

	defer func() {
		// Teardown must not inherit the run's fate (AUD-001): a cancelled or
		// timed-out run still removes its container, under its own deadline.
		teardownCtx, teardownCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer teardownCancel()
		if err := r.removeContainer(teardownCtx, containerID); err != nil {
			r.loggerFromCtx(ctx).Error("remove container failed", "containerID", shortID(containerID), "error", err)
		}
	}()

	// Start container
	if _, err := r.client.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		err = fmt.Errorf("failed to start container: %w", err)
		jobResult.SetupErr = err
		return jobResult, err
	}
	r.loggerFromCtx(ctx).Info("container started", "containerID", shortID(containerID))

	// Run setup commands. Reported via JobResult.SetupErr
	if err := r.runSetup(ctx, containerID, job.Setup); err != nil {
		err = fmt.Errorf("job setup failed: %w", err)
		jobResult.SetupErr = err
		return jobResult, err
	}
	r.loggerFromCtx(ctx).Info("setup commands ran", "containerID", shortID(containerID))

	// Run steps
	stepResults, stepRunErr := r.runSteps(ctx, containerID, job, events)
	if stepRunErr != nil {
		stepRunErr = fmt.Errorf("failed to run steps: %w", stepRunErr)
	}
	r.loggerFromCtx(ctx).Info("steps completed", "containerID", shortID(containerID))

	jobResult.StepResults = stepResults
	return jobResult, stepRunErr
}

// pullImage pulls the docker image and discards the response body.
func (r *DockerRunner) pullImage(ctx context.Context, img string) error {
	imagePullResponse, err := r.client.ImagePull(ctx, img, client.ImagePullOptions{})
	if err != nil {
		return err
	}
	defer func() {
		pullImageRespCloseErr := imagePullResponse.Close()
		if pullImageRespCloseErr != nil {
			r.loggerFromCtx(ctx).Warn("failed to close image pull response", "error", pullImageRespCloseErr)
		}
	}()
	_, err = io.Copy(io.Discard, imagePullResponse)
	return err
}

// createContainer creates a container for the job without starting it.
// It also mounts the projectPath to the container.
func (r *DockerRunner) createContainer(ctx context.Context, job domain.Job, projectPath string) (string, error) {
	env := make([]string, 0, len(job.Env)+1)
	for k, v := range job.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	// Expose the env-file path so steps can export vars to later steps.
	env = append(env, fmt.Sprintf("%s=%s", envFileVar, envFilePath))

	resp, err := r.client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image: job.Image,
			Env:   env,
			// Labels mark the container as this job's so operators and tests
			// can attribute containers to runs without guessing by image.
			Labels:     map[string]string{"odyssey.job": job.Name},
			WorkingDir: workDir,
			Cmd:        []string{"sh", "-c", "tail -f /dev/null"},
		},
	})
	if err != nil {
		return "", err
	}
	containerID := resp.ID
	if err := r.mountArchive(ctx, containerID, projectPath, workDir); err != nil {
		removeErr := r.removeContainer(ctx, containerID)
		if removeErr != nil {
			err = fmt.Errorf("%w; additionally, failed to remove container: %w", err, removeErr)
		}
		return "", fmt.Errorf("failed to mount project path to container: %w", err)
	}
	return containerID, nil
}

// removeContainer removes a container by its ID.
func (r *DockerRunner) removeContainer(ctx context.Context, containerID string) error {
	_, err := r.client.ContainerRemove(ctx, containerID, client.ContainerRemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	})
	if err == nil {
		r.loggerFromCtx(ctx).Info("container removed", "containerID", shortID(containerID))
	}
	return err
}

// runSetup runs setup command for the Job.
func (r *DockerRunner) runSetup(ctx context.Context, containerID string, setupCmd []string) error {
	for _, command := range setupCmd {
		code, _, _, err := r.execCommand(ctx, containerID, command, nil)
		if err != nil {
			return err
		}
		// A setup command exiting non-zero means the job cannot proceed;
		// it is a setup failure (Errored), not a step result.
		if code != domain.ExitSuccess {
			return fmt.Errorf("setup command %q exited with code %d", command, code)
		}
	}
	// Start each job with an empty env file so a step can append to it and
	// later steps can read it back, even if no earlier step wrote anything.
	if _, _, _, err := r.execCommand(ctx, containerID, ": > "+envFilePath, nil); err != nil {
		return fmt.Errorf("failed to initialize env file: %w", err)
	}
	return nil
}

func (r *DockerRunner) runSteps(ctx context.Context, containerID string, job domain.Job, events StepSink) ([]domain.StepResult, error) {
	stepResults := make([]domain.StepResult, 0, len(job.Steps))

	for _, step := range job.Steps {
		stepStartTime := time.Now()
		r.emitStep(ctx, events, domain.Event{Type: domain.EventStepStarted, OccurredAt: time.Now(), Job: job.Name, Step: step.Name})

		// Vars exported by earlier steps, injected as this exec's env. Exec env
		// overrides the container's job-level env.
		exported, err := r.readExportedEnv(ctx, containerID)
		if err != nil {
			stepResults = append(stepResults, domain.StepResult{
				StepName: step.Name,
				ExitCode: domain.ExitNone,
				Err:      err,
				Duration: time.Since(stepStartTime),
			})
			r.emitStep(ctx, events, stepFinishedEvent(job.Name, stepResults[len(stepResults)-1]))
			return stepResults, fmt.Errorf("failed to read exported env before step %q: %w", step.Name, err)
		}

		type stepResult struct {
			ExitCode domain.ExitCode
			Stdout   string
			Stderr   string
			RunErr   error
		}

		stepCtx := ctx
		var cancel context.CancelFunc
		if step.Timeout > 0 {
			stepCtx, cancel = context.WithTimeout(ctx, time.Duration(step.Timeout)*time.Millisecond)
		}

		// execCommand honors its context: a step deadline or a run
		// cancellation interrupts the exec and surfaces as its error (AUD-022).
		exitCode, stdout, stderr, runErr := r.execCommand(stepCtx, containerID, step.Run, envSlice(exported))
		if cancel != nil {
			cancel()
		}

		stepRes := domain.StepResult{
			StepName: step.Name,
			Stdout:   stdout,
			Stderr:   stderr,
			ExitCode: exitCode,
			Duration: time.Since(stepStartTime),
		}

		switch {
		case runErr != nil && ctx.Err() != nil:
			// The run itself was cancelled or timed out — the step did not
			// time out; report the cancellation honestly (AUD-011).
			stepRes.ExitCode = domain.ExitNone
			stepRes.Err = fmt.Errorf("run cancelled: %w", ctx.Err())
			stepResults = append(stepResults, stepRes)
			r.emitStep(ctx, events, stepFinishedEvent(job.Name, stepRes))
			return stepResults, ctx.Err()
		case runErr != nil && step.Timeout > 0 && errors.Is(runErr, context.DeadlineExceeded):
			timeoutErr := fmt.Errorf("step %s timed out after %dms", step.Name, step.Timeout)
			r.loggerFromCtx(ctx).Error(timeoutErr.Error())
			stepRes.ExitCode = domain.ExitFailure
			stepResults = append(stepResults, stepRes)
			r.emitStep(ctx, events, stepFinishedEvent(job.Name, stepRes))
			return stepResults, timeoutErr
		case runErr != nil:
			stepRes.Err = runErr
			stepResults = append(stepResults, stepRes)
			r.emitStep(ctx, events, stepFinishedEvent(job.Name, stepRes))
			return stepResults, runErr
		}

		stepResults = append(stepResults, stepRes)
		r.emitStep(ctx, events, stepFinishedEvent(job.Name, stepRes))

		// Fail-fast within a job: the first non-zero exit ends it, mirroring
		// the stage-level fail-fast rule (gh-82) — nothing may run against
		// code that just failed (AUD-023). A process exit is a result, not
		// an error (AUD-003), so the run error stays nil.
		if stepRes.ExitCode != domain.ExitSuccess {
			return stepResults, nil
		}

		// Surface which vars this step exported for later steps. Values are
		// omitted since they may hold secrets; a future UI can show them.
		after, err := r.readExportedEnv(ctx, containerID)
		if err != nil {
			stepResults = append(stepResults, domain.StepResult{
				StepName: step.Name,
				ExitCode: domain.ExitNone,
				Err:      err,
				Duration: time.Since(stepStartTime),
			})
			r.emitStep(ctx, events, stepFinishedEvent(job.Name, stepResults[len(stepResults)-1]))
			return stepResults, fmt.Errorf("failed to read exported env after step %q: %w", step.Name, err)
		}
		if keys := newlyExportedKeys(exported, after); len(keys) > 0 {
			r.loggerFromCtx(ctx).Info("step exported env vars", "step", step.Name, "keys", keys)
		}
	}
	return stepResults, nil
}

// emitStep publishes one step lifecycle event. Emission failures are
// logged, never fatal — step execution proceeds unaffected (ADR 0001).
func (r *DockerRunner) emitStep(ctx context.Context, events StepSink, event domain.Event) {
	if events == nil {
		return
	}
	if err := events.Publish(ctx, event); err != nil {
		r.loggerFromCtx(ctx).Warn("event emit failed", "type", event.Type, "step", event.Step, "err", err)
	}
}

// stepFinishedEvent builds the finished envelope for an appended step
// result. The payload carries the status, exit code, and duration the
// results recorder needs for its steps table — never stdout/stderr
// content (ADR 0001 keeps output off the bus). A step whose process never
// produced an exit code omits the key, so the recorded column stays NULL.
func stepFinishedEvent(jobName string, result domain.StepResult) domain.Event {
	payload := map[string]string{
		"status":      result.Status().String(),
		"duration_ms": strconv.FormatInt(result.Duration.Milliseconds(), 10),
	}
	if result.ExitCode != domain.ExitNone {
		payload["exit_code"] = strconv.Itoa(int(result.ExitCode))
	}
	return domain.Event{
		Type:       domain.EventStepFinished,
		OccurredAt: time.Now(),
		Job:        jobName,
		Step:       result.StepName,
		Payload:    payload,
	}
}

// readExportedEnv reads the in-container env file and parses its KEY=value
// lines into a map. See parseEnvFile for the parsing rules.
func (r *DockerRunner) readExportedEnv(ctx context.Context, containerID string) (map[string]string, error) {
	code, stdout, _, err := r.execCommand(ctx, containerID, "cat "+envFilePath, nil)
	if err != nil {
		return nil, err
	}
	if code != domain.ExitSuccess {
		return nil, fmt.Errorf("env file read exited with code %d", code)
	}
	return parseEnvFile(stdout), nil
}

// execCommand runs a command inside a container and returns its exit code,
// stdout, stderr, and an error.
func (r *DockerRunner) execCommand(ctx context.Context, containerID string, command string, env []string) (domain.ExitCode, string, string, error) {
	execConfig := client.ExecCreateOptions{
		Cmd:          []string{"sh", "-c", command},
		Env:          env,
		AttachStdout: true,
		AttachStderr: true,
	}
	execCreateResult, err := r.client.ExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return domain.ExitNone, "", "", err
	}

	execID := execCreateResult.ID
	resp, err := r.client.ExecAttach(ctx, execID, client.ExecAttachOptions{})
	if err != nil {
		return domain.ExitNone, "", "", err
	}
	defer resp.Close()

	// StdCopy blocks on the hijacked stream until the process exits and
	// cannot be interrupted by the context, so the copy runs aside and a
	// context done closes the stream — the only way to unblock the read.
	// The exec itself keeps running; Run's teardown removes the container
	// and nothing waits on the process (AUD-022).
	var stdout, stderr bytes.Buffer
	copyDone := make(chan error, 1)
	go func() {
		_, err := stdcopy.StdCopy(&stdout, &stderr, resp.Reader)
		copyDone <- err
	}()

	select {
	case copyErr := <-copyDone:
		if copyErr != nil {
			// The stream broke (e.g. the container vanished) — keep whatever
			// output arrived before the break.
			return domain.ExitNone, stdout.String(), stderr.String(), copyErr
		}
	case <-ctx.Done():
		resp.Close()
		<-copyDone
		return domain.ExitNone, stdout.String(), stderr.String(), ctx.Err()
	}

	inspect, err := r.client.ExecInspect(ctx, execID, client.ExecInspectOptions{})
	if err != nil {
		return domain.ExitNone, stdout.String(), stderr.String(), err
	}

	// A non-zero exit is the process's own result, not an infrastructure
	// error: report the real code and no error (AUD-003).
	return domain.ExitCode(inspect.ExitCode), stdout.String(), stderr.String(), nil
}

// mountArchive creates a tar of the project directory and copies it into destinationPath inside the container
func (r *DockerRunner) mountArchive(ctx context.Context, containerID string, projectPath string, destinationPath string) error {
	archive, err := common.CreateTar(projectPath)
	if err != nil {
		return err
	}
	if len(archive) == 0 {
		return fmt.Errorf("cannot mount an empty archive")
	}
	reader := bytes.NewReader(archive)
	if _, err := r.client.CopyToContainer(ctx, containerID, client.CopyToContainerOptions{
		DestinationPath: destinationPath,
		Content:         reader,
	}); err != nil {
		return err
	}
	return nil
}
