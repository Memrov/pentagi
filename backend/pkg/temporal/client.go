package temporal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"pentagi/pkg/adk"
	"pentagi/pkg/config"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Client starts and signals flow workflows and hosts the worker that runs
// them. It satisfies controller.FlowEngine.
type Client struct {
	client    client.Client
	worker    worker.Worker
	adkWorker worker.Worker
	taskQueue string
	idPrefix  string
	policy    ActivityPolicy
	adk       *ADKPolicy
	// engineOf reads the flow row's engine where a start or signal must stamp
	// it onto the workflow; a missing or failing lookup yields legacy, the
	// engine every pre-column row runs.
	engineOf func(context.Context, int64) (string, error)
}

// Dial connects to the Temporal frontend. The task queue and workflow ids
// carry the tenant prefix, so tenants sharing one Temporal namespace never
// pick up each other's flows.
func Dial(cfg *config.Config) (*Client, error) {
	c, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHostPort,
		Namespace: cfg.TemporalNamespace,
		Logger:    log.NewStructuredLogger(slog.Default()),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to temporal at %s: %w", cfg.TemporalHostPort, err)
	}

	return &Client{
		client:    c,
		taskQueue: cfg.ScopedName(cfg.TemporalTaskQueue),
		idPrefix:  cfg.TenantPrefix(),
		policy: ActivityPolicy{
			StartToClose: time.Duration(cfg.TemporalActivityTimeout) * time.Second,
			Heartbeat:    time.Duration(cfg.TemporalHeartbeatTimeout) * time.Second,
			MaxAttempts:  int32(cfg.TemporalMaxAttempts),
		},
	}, nil
}

// SetEngineLookup installs the row-level engine resolver; without it every
// flow is stamped legacy.
func (c *Client) SetEngineLookup(fn func(context.Context, int64) (string, error)) {
	c.engineOf = fn
}

// SetADKPolicy configures child dispatch for engine=adk flows; the task queue
// must already carry the tenant prefix.
func (c *Client) SetADKPolicy(policy ADKPolicy) {
	c.adk = &policy
}

func (c *Client) WorkflowID(flowID int64) string {
	return fmt.Sprintf("%spentagi-flow-%d", c.idPrefix, flowID)
}

// StartWorker begins polling the task queue. Start it only after the flows
// are loaded, otherwise the first activities fail with ErrFlowNotLoaded and
// wait for a retry.
func (c *Client) StartWorker(runner FlowRunner) error {
	w := worker.New(c.client, c.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(FlowWorkflow, workflow.RegisterOptions{Name: FlowWorkflowName})

	acts := NewActivities(runner, max(c.policy.Heartbeat/3, time.Second))
	if prompter, ok := runner.(AgentPrompter); ok {
		acts.prompter = prompter
	}
	w.RegisterActivityWithOptions(acts.ResumeFlow, activity.RegisterOptions{Name: ResumeFlowActivity})
	w.RegisterActivityWithOptions(acts.ProcessFlowInput, activity.RegisterOptions{Name: ProcessInputActivity})
	w.RegisterActivityWithOptions(acts.RenderAgentPrompt, activity.RegisterOptions{Name: RenderPromptActivity})

	if err := w.Start(); err != nil {
		return fmt.Errorf("failed to start temporal worker on %s: %w", c.taskQueue, err)
	}
	c.worker = w

	return nil
}

// StartADKWorker polls the ADK queue in the same process: the pentester child
// workflow, its tool activities, and the model activities the plugins carry.
func (c *Client) StartADKWorker(acts *adk.Activities, plugins ...worker.Plugin) error {
	if c.adk == nil || c.adk.TaskQueue == "" {
		return fmt.Errorf("adk dispatch is not configured")
	}
	w := worker.New(c.client, c.adk.TaskQueue, worker.Options{Plugins: plugins})
	adk.RegisterWorkflow(w)
	acts.Register(w)

	if err := w.Start(); err != nil {
		return fmt.Errorf("failed to start adk worker on %s: %w", c.adk.TaskQueue, err)
	}
	c.adkWorker = w

	return nil
}

func (c *Client) Close() {
	if c.worker != nil {
		c.worker.Stop()
	}
	if c.adkWorker != nil {
		c.adkWorker.Stop()
	}
	c.client.Close()
}

// StartFlow resumes the unfinished tasks of a flow in a new workflow, or does
// nothing if its workflow is already running.
func (c *Client) StartFlow(ctx context.Context, flowID int64) error {
	_, err := c.client.ExecuteWorkflow(ctx, c.startOptions(flowID), FlowWorkflowName, c.params(ctx, flowID))
	if err != nil {
		return fmt.Errorf("failed to start workflow for flow %d: %w", flowID, err)
	}
	return nil
}

// SignalInput returns once the input is in the workflow history, which makes
// it durable; it does not wait for the input to be processed.
func (c *Client) SignalInput(ctx context.Context, flowID int64, input string) error {
	_, err := c.client.SignalWithStartWorkflow(ctx, c.WorkflowID(flowID), SignalInput, input,
		c.startOptions(flowID), FlowWorkflowName, c.params(ctx, flowID))
	if err != nil {
		return fmt.Errorf("failed to signal input to flow %d: %w", flowID, err)
	}
	return nil
}

func (c *Client) FinishFlow(ctx context.Context, flowID int64) error {
	err := c.client.SignalWorkflow(ctx, c.WorkflowID(flowID), "", SignalFinish, nil)
	var notFound *serviceerror.NotFound
	if err != nil && !errors.As(err, &notFound) {
		return fmt.Errorf("failed to signal finish to flow %d: %w", flowID, err)
	}
	return nil
}

func (c *Client) QueryState(ctx context.Context, flowID int64) (FlowState, error) {
	var state FlowState
	val, err := c.client.QueryWorkflow(ctx, c.WorkflowID(flowID), "", QueryState)
	if err != nil {
		return state, fmt.Errorf("failed to query flow %d: %w", flowID, err)
	}
	if err := val.Get(&state); err != nil {
		return state, fmt.Errorf("failed to decode flow %d state: %w", flowID, err)
	}
	return state, nil
}

func (c *Client) startOptions(flowID int64) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                       c.WorkflowID(flowID),
		TaskQueue:                c.taskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}
}

func (c *Client) params(ctx context.Context, flowID int64) FlowParams {
	params := FlowParams{FlowID: flowID, Resume: true, Engine: EngineLegacy, Policy: c.policy}
	if c.engineOf != nil {
		engine, err := c.engineOf(ctx, flowID)
		if err != nil {
			slog.Warn("failed to read flow engine, defaulting to legacy", "flow_id", flowID, "error", err)
		} else if engine != "" {
			params.Engine = engine
		}
	}
	if params.Engine == EngineAdk && c.adk != nil {
		params.ADK = *c.adk
	}
	return params
}
