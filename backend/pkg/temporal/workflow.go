package temporal

import (
	"time"

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
	state := FlowState{FlowID: params.FlowID}
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

	// A finish signal cancels the activity in flight and waits for it to
	// unwind, so the run is not left going on a flow the user has closed.
	run := func(name string, arg any) {
		actx, cancel := workflow.WithCancel(workflow.WithActivityOptions(ctx, params.Policy.options()))
		defer cancel()

		state.Running = true
		defer func() { state.Running = false }()

		done := false
		sel := workflow.NewSelector(ctx)
		fut := workflow.ExecuteActivity(actx, name, arg)
		sel.AddFuture(fut, func(f workflow.Future) {
			done = true
			if err := f.Get(ctx, nil); err != nil {
				logger.Error("flow activity failed", "activity", name, "flow_id", params.FlowID, "error", err)
			}
		})
		sel.AddReceive(inputCh, onInput)
		sel.AddReceive(finishCh, onFinish)
		for !done && !finished {
			sel.Select(ctx)
		}
		if !done {
			cancel()
			_ = fut.Get(ctx, nil)
		}
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
					FlowID: params.FlowID,
					Policy: params.Policy,
				})
			}
			idle.Select(ctx)
			continue
		}

		input := pending[0]
		pending = pending[1:]
		run(ProcessInputActivity, ProcessInputArgs{FlowID: params.FlowID, Input: input})
		state.Processed++
	}

	return nil
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
