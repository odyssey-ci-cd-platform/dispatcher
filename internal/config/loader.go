package config

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/common"
	"github.com/odyssey-ci-cd-platform/dispatcher/internal/domain"
)

const (
	odysseyDir   = ".odyssey"
	pipelineFile = "pipeline.toml"
	envFile      = "env.toml"
)

// Load reads and parses the .odyssey/ config directory at projectPath,
// validates it, and returns a domain.Pipeline ready for execution.
func Load(projectPath string) (domain.Pipeline, error) {
	odysseyPath := filepath.Join(projectPath, odysseyDir)

	root, err := common.ReadToml[RootConfig](filepath.Join(odysseyPath, pipelineFile))
	if err != nil {
		return domain.Pipeline{}, fmt.Errorf("failed to read pipeline.toml: %w", err)
	}

	if err := root.Validate(); err != nil {
		return domain.Pipeline{}, fmt.Errorf("invalid pipeline config: %w", err)
	}

	env, err := loadEnv(odysseyPath)
	if err != nil {
		return domain.Pipeline{}, fmt.Errorf("failed to read env.toml: %w", err)
	}

	return translate(root, env), nil
}

// loadEnv reads env.toml if it exists. Return nil map is file is absent.
func loadEnv(odysseyPath string) (map[string]string, error) {
	envPath := filepath.Join(odysseyPath, envFile)

	if _, err := os.Stat(envPath); os.IsNotExist(err) {
		return nil, nil
	}
	envConfig, err := common.ReadToml[EnvConfig](envPath)
	if err != nil {
		return nil, err
	}
	return envConfig.Env, nil
}

// translate converts a validated RootConfig into a domain.Pipeline.
// Global env is merged into each job's env, with job-level values taking precedence.
func translate(root RootConfig, globalEnv map[string]string) domain.Pipeline {
	// Group jobs by stage, preserving declared stage order. Jobs are taken
	// in sorted key order so the resulting job, result, and event order is
	// deterministic across runs (AUD-008).
	jobsByStage := make(map[string][]domain.Job)
	for _, jobKey := range slices.Sorted(maps.Keys(root.Jobs)) {
		jobCfg := root.Jobs[jobKey]
		job := domain.Job{
			Name:  jobKey,
			Image: jobCfg.Image,
			Setup: jobCfg.Setup,
			Env:   mergeEnv(globalEnv, jobCfg.Env),
		}
		for _, stepCfg := range jobCfg.Steps {
			job.Steps = append(job.Steps, domain.Step{
				Name:    stepCfg.Name,
				Run:     stepCfg.Run,
				Timeout: stepCfg.Timeout,
			})
		}
		jobsByStage[jobCfg.Stage] = append(jobsByStage[jobCfg.Stage], job)
	}

	// Build stages in declared order
	stages := make([]domain.Stage, 0, len(root.Pipeline.Stages))
	for _, stageName := range root.Pipeline.Stages {
		stages = append(stages, domain.Stage{
			Name: stageName,
			Jobs: jobsByStage[stageName],
		})
	}
	return domain.Pipeline{
		Name:   root.Pipeline.Name,
		Stages: stages,
	}
}

// mergeEnv merges global and job-level env maps.
// Job-level values take precedence over global values.
func mergeEnv(global, jobLevel map[string]string) map[string]string {
	merged := make(map[string]string)
	maps.Copy(merged, global)
	maps.Copy(merged, jobLevel)

	return merged
}
