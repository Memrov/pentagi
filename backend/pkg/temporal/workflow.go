package temporal

import (
	"encoding/json"
	"fmt"
	"time"

	"pentagi/pkg/adk"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// FlowWorkflow feeds the inputs of one flow to the runner one at a time, in
// the order they were signalled, until it receives the finish signal.
func FlowWorkflow(ctx workflow.Context, params FlowParams) error {
	logger := workflow.GetLogger(ctx)
	inputCh := workflow.GetSignalChannel(ctx, SignalInput)
	finishCh := workflow.GetSignalChannel(ctx, SignalFinish)

	pending := append([]string(nil), params.Pending...)
	state := FlowState{
		FlowID:     params.FlowID,
		Engine:     params.Engine,
		Processed:  params.Processed,
		LastResult: params.LastResult,
	}
	finished := false

	err := workflow.SetQueryHandler(ctx, QueryState, func() (FlowState, error) {
		state.Pending = len(pending)
		return state, nil
	})
	if err != nil {
		return err
	}

	onInput := func(c workflow.ReceiveChannel, _ bool) {
		var input string
		c.Receive(ctx, &input)
		pending = append(pending, input)
	}
	onFinish := func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(ctx, nil)
		finished = true
	}

	// A finish signal cancels the work in flight and waits for it to
	// unwind, so the run is not left going on a flow the user has closed.
	// await returns false when the finish signal won, so a caller chaining
	// steps can stop before scheduling the next one.
	await := func(schedule func(workflow.Context) workflow.Future, out any) bool {
		cctx, cancel := workflow.WithCancel(ctx)
		defer cancel()

		state.Running = true
		defer func() { state.Running = false }()

		done := false
		sel := workflow.NewSelector(ctx)
		fut := schedule(cctx)
		sel.AddFuture(fut, func(f workflow.Future) {
			done = true
			if err := f.Get(ctx, out); err != nil {
				logger.Error("flow work failed", "flow_id", params.FlowID, "error", err)
			}
		})
		sel.AddReceive(inputCh, onInput)
		sel.AddReceive(finishCh, onFinish)
		for !done && !finished {
			sel.Select(ctx)
		}
		if !done {
			cancel()
			_ = fut.Get(ctx, out)
		}
		return done
	}

	run := func(name string, arg any) {
		await(func(c workflow.Context) workflow.Future {
			return workflow.ExecuteActivity(
				workflow.WithActivityOptions(c, params.Policy.options()), name, arg)
		}, nil)
	}

	// One adk input is a child workflow on the adk queue with a deterministic
	// id built from the processed counter: a replayed parent re-executes the
	// same id and Temporal answers it from the recorded command instead of
	// starting a second run of the input, and a child cannot outlive the
	// parent under the default close policy, so the id can never collide
	// with a still-running one. The prompts are rendered in an activity
	// first because the prompter reads the DB.
	//
	// The child emits its session events back through EmitFlowEvent on this
	// queue — the flows queue is where the flow's log workers live. Around
	// the child the parent emits the flow's status itself: the child never
	// sees a finished/cancelled end to mark waiting from, so the boundary
	// belongs to the side that knows the run is over.
	runAdkInput := func(input string) {
		if params.ADK.TaskQueue == "" {
			logger.Error("adk engine flow has no adk task queue configured; dropping input",
				"flow_id", params.FlowID)
			return
		}

		emitStatus := func(c workflow.Context, status string) workflow.Future {
			return workflow.ExecuteActivity(
				workflow.WithActivityOptions(c, adk.EmitActivityOptions(workflow.GetInfo(ctx).TaskQueueName)),
				adk.EmitFlowEventActivity,
				adk.EmitFlowEventInput{FlowID: params.FlowID, Events: []adk.FlowEvent{
					{Op: adk.FlowEventSetStatus, Status: status},
				}})
		}

		req := adk.PentestRequest{
			Task:               input,
			Model:              params.ADK.Model,
			ToolTimeoutSeconds: params.ADK.ToolTimeoutSeconds,
			FlowID:             params.FlowID,
			EmitEvents:         true,
			EmitTaskQueue:      workflow.GetInfo(ctx).TaskQueueName,
		}

		var rendered RenderedPrompt
		if !await(func(c workflow.Context) workflow.Future {
			return workflow.ExecuteActivity(
				workflow.WithActivityOptions(c, params.Policy.options()),
				RenderPromptActivity,
				PromptRenderRequest{FlowID: params.FlowID, AgentType: AgentTypePentester, Input: input})
		}, &rendered) {
			return
		}
		req.Instruction = rendered.Instruction
		req.Question = rendered.Question

		if !await(func(c workflow.Context) workflow.Future { return emitStatus(c, "running") }, nil) {
			return
		}
		// An emit failure is logged inside await; the run continues because a
		// missing status row is cheaper than a stalled pentest.
		defer func() {
			await(func(c workflow.Context) workflow.Future { return emitStatus(c, "waiting") }, nil)
		}()

		childID := fmt.Sprintf("%s-input-%d", workflow.GetInfo(ctx).WorkflowExecution.ID, state.Processed)
		var res adk.PentestResult
		if !await(func(c workflow.Context) workflow.Future {
			return workflow.ExecuteChildWorkflow(
				workflow.WithChildOptions(c, workflow.ChildWorkflowOptions{
					WorkflowID:          childID,
					TaskQueue:           params.ADK.TaskQueue,
					WaitForCancellation: true,
				}),
				adk.PentesterWorkflowName, req)
		}, &res) {
			return
		}
		state.LastResult = adkResultSummary(res)
	}

	if params.Resume {
		run(ResumeFlowActivity, params.FlowID)
	}

	idle := workflow.NewSelector(ctx)
	idle.AddReceive(inputCh, onInput)
	idle.AddReceive(finishCh, onFinish)

	for !finished {
		if len(pending) == 0 {
			if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
				// Signals still buffered in the channels would be dropped
				// with this run.
				for idle.HasPending() {
					idle.Select(ctx)
				}
				if finished {
					break
				}
				if len(pending) > 0 {
					continue
				}
				return workflow.NewContinueAsNewError(ctx, FlowWorkflowName, FlowParams{
					FlowID:     params.FlowID,
					Engine:     params.Engine,
					Processed:  state.Processed,
					LastResult: state.LastResult,
					Policy:     params.Policy,
					ADK:        params.ADK,
				})
			}
			idle.Select(ctx)
			continue
		}

		input := pending[0]
		pending = pending[1:]
		if params.Engine == EngineAdk {
			runAdkInput(input)
		} else {
			run(ProcessInputActivity, ProcessInputArgs{FlowID: params.FlowID, Input: input})
		}
		state.Processed++
	}

	return nil
}

// adkResultSummary is the compact record FlowState keeps per child run; the
// answer text stays out of workflow history on purpose.
func adkResultSummary(res adk.PentestResult) string {
	summary := struct {
		EventCount        int  `json:"event_count"`
		HackResult        bool `json:"hack_result"`
		HackResultSuccess bool `json:"hack_result_success"`
	}{
		EventCount:        res.EventCount,
		HackResult:        res.HackResult != nil,
		HackResultSuccess: res.HackResult != nil && res.HackResult.Success,
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return fmt.Sprintf(`{"event_count":%d}`, res.EventCount)
	}
	return string(raw)
}

func (p ActivityPolicy) options() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: p.StartToClose,
		HeartbeatTimeout:    p.Heartbeat,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        time.Minute,
			MaximumAttempts:        p.MaxAttempts,
			NonRetryableErrorTypes: []string{errTypeFlowClosed},
		},
	}
}
