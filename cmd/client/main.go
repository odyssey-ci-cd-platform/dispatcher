package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	odysseyv1 "github.com/odyssey-ci-cd-platform/dispatcher/gen/proto/v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC server address")
	timeout := flag.Duration("timeout", 120*time.Second, "pipeline timeout")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: odyssey-client [-addr HOST:PORT] <project-path>\n")
		os.Exit(2)
	}
	projectPath := flag.Arg(0)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := odysseyv1.NewOdysseyServiceClient(conn)
	resp, err := client.RunPipeline(ctx, &odysseyv1.RunPipelineRequest{
		ProjectPath: projectPath,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Print the result tree.
	fmt.Printf("Pipeline: %s  Status: %s\n", resp.PipelineName, resp.Status)
	for _, stage := range resp.StageResults {
		fmt.Printf("  Stage: %s  Status: %s\n", stage.StageName, stage.Status)
		for _, job := range stage.JobResults {
			fmt.Printf("    Job: %s  Status: %s\n", job.JobName, job.Status)
			for _, step := range job.StepResults {
				fmt.Printf("      Step: %s  Exit: %d  Status: %s\n",
					step.StepName, step.ExitCode, step.Status)
				if step.Stdout != "" {
					fmt.Printf("        stdout: %s\n", step.Stdout)
				}
				if step.Stderr != "" {
					fmt.Printf("        stderr: %s\n", step.Stderr)
				}
				if step.Error != "" {
					fmt.Printf("        error: %s\n", step.Error)
				}
			}
		}
	}
}
