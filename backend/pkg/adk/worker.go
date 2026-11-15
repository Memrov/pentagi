package adk

import (
	"context"
	"fmt"

	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// WorkerPlugin registers the integration's InvokeModel activity (and the MCP
// activities) at worker start, with our openai-compatible model behind the
// activity boundary. The key is checked here rather than in Config.Validate so
// commands that never build a model ("run", "help") do not require it.
func WorkerPlugin(cfg *Config) (worker.Plugin, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("ADK_API_KEY is required")
	}
	return googleadk.NewPlugin(googleadk.Config{
		Models: map[string]googleadk.ModelFactory{
			cfg.ModelName: ModelFactory(cfg),
		},
	})
}

// RegisterWorkflow puts the workflow on the worker under its stable name.
func RegisterWorkflow(w worker.Worker) {
	w.RegisterWorkflowWithOptions(PentesterWorkflow, workflow.RegisterOptions{Name: PentesterWorkflowName})
}

// StartPentest executes the workflow and returns the run handle; callers read
// the result from run.Get.
func StartPentest(ctx context.Context, c client.Client, taskQueue string, req PentestRequest) (client.WorkflowRun, error) {
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, PentesterWorkflowName, req)
	if err != nil {
		return nil, fmt.Errorf("failed to start pentester workflow: %w", err)
	}
	return run, nil
}
