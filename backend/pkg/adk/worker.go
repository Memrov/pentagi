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
// commands that never build a model ("run", "help") do not require it — and
// only when no resolver is wired: a production worker resolves keys per flow,
// so an empty ADK_API_KEY there only degrades non-flow runs to a provider
// 401 instead of blocking the whole worker.
//
// The plugin's Models map matches by name, and a workflow never sends a model
// name other than cfg.ModelName or the adviser's marked handle — the real
// model name comes out of the resolved credentials inside the activity, so
// every provider-configured model is served by these two registrations.
func WorkerPlugin(cfg *Config, resolver ProviderResolver) (worker.Plugin, error) {
	if cfg.APIKey == "" && resolver == nil {
		return nil, fmt.Errorf("ADK_API_KEY is required")
	}
	factory := ModelFactory(cfg, resolver)
	return googleadk.NewPlugin(googleadk.Config{
		Models: map[string]googleadk.ModelFactory{
			cfg.ModelName: factory,
			agentModelName(AgentTypeAdviser, cfg.ModelName): factory,
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
