package temporal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"pentagi/pkg/adk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

var testPolicy = ActivityPolicy{StartToClose: time.Hour, Heartbeat: time.Minute, MaxAttempts: 1}

func newFlowEnv(t *testing.T, resume, process any) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(FlowWorkflow, workflow.RegisterOptions{Name: FlowWorkflowName})
	env.RegisterActivityWithOptions(resume, activity.RegisterOptions{Name: ResumeFlowActivity})
	env.RegisterActivityWithOptions(process, activity.RegisterOptions{Name: ProcessInputActivity})

	return env
}

func TestWorkflow_FlowWorkflow_ProcessesInputsInOrderUntilFinish(t *testing.T) {
	var (
		resumed int
		inputs  []string
	)
	env := newFlowEnv(t,
		func(_ context.Context, flowID int64) error {
			assert.Equal(t, int64(7), flowID)
			resumed++
			return nil
		},
		func(_ context.Context, args ProcessInputArgs) error {
			assert.Equal(t, int64(7), args.FlowID)
			inputs = append(inputs, args.Input)
			return nil
		},
	)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalInput, "one")
		env.SignalWorkflow(SignalInput, "two")
	}, time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalFinish, nil) }, time.Hour)

	env.ExecuteWorkflow(FlowWorkflowName, FlowParams{FlowID: 7, Resume: true, Pending: []string{"zero"}, Policy: testPolicy})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, 1, resumed)
	assert.Equal(t, []string{"zero", "one", "two"}, inputs)
}

func TestWorkflow_FlowWorkflow_FailedInputDoesNotStopTheFlow(t *testing.T) {
	var inputs []string
	env := newFlowEnv(t,
		func(context.Context, int64) error { return nil },
		func(_ context.Context, args ProcessInputArgs) error {
			inputs = append(inputs, args.Input)
			if args.Input == "bad" {
				return errors.New("provider unavailable")
			}
			return nil
		},
	)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalInput, "good") }, time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalFinish, nil) }, time.Hour)

	env.ExecuteWorkflow(FlowWorkflowName, FlowParams{FlowID: 7, Pending: []string{"bad"}, Policy: testPolicy})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, []string{"bad", "good"}, inputs)
}

func TestWorkflow_FlowWorkflow_FinishCancelsRunningInput(t *testing.T) {
	cancelled := make(chan error, 1)
	env := newFlowEnv(t,
		func(context.Context, int64) error { return nil },
		func(ctx context.Context, _ ProcessInputArgs) error {
			deadline := time.After(10 * time.Second)
			for {
				activity.RecordHeartbeat(ctx)
				select {
				case <-ctx.Done():
					cancelled <- ctx.Err()
					return ctx.Err()
				case <-deadline:
					cancelled <- nil
					return nil
				case <-time.After(10 * time.Millisecond):
				}
			}
		},
	)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalFinish, nil) }, time.Millisecond)

	// The SDK throttles heartbeats to most of the heartbeat timeout, and the
	// cancellation reaches the activity only on a heartbeat it sends.
	policy := testPolicy
	policy.Heartbeat = 200 * time.Millisecond
	env.ExecuteWorkflow(FlowWorkflowName, FlowParams{FlowID: 7, Pending: []string{"scan"}, Policy: policy})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	select {
	case err := <-cancelled:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("the running input was never cancelled")
	}
}

func TestWorkflow_FlowWorkflow_QueryReportsQueuedInputs(t *testing.T) {
	release := make(chan struct{})
	env := newFlowEnv(t,
		func(context.Context, int64) error { return nil },
		func(ctx context.Context, _ ProcessInputArgs) error {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		},
	)

	var state FlowState
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalInput, "queued") }, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryState)
		require.NoError(t, err)
		require.NoError(t, val.Get(&state))
		close(release)
		env.SignalWorkflow(SignalFinish, nil)
	}, time.Second)

	env.ExecuteWorkflow(FlowWorkflowName, FlowParams{FlowID: 7, Pending: []string{"running"}, Policy: testPolicy})

	require.True(t, env.IsWorkflowCompleted())
	assert.Equal(t, FlowState{FlowID: 7, Running: true, Pending: 1}, state)
}

type childRun struct {
	id  string
	req adk.PentestRequest
}

// newAdkEnv registers the flow workflow, the prompt render activity stub and
// a pentester child stub whose runs land in the returned slice. The emit
// activity is captured too: the parent emits the flow's status around each
// child run, so an env without it would pass with a noisy dead emit.
func newAdkEnv(
	t *testing.T,
	render func(context.Context, PromptRenderRequest) (RenderedPrompt, error),
	child func(workflow.Context, adk.PentestRequest) (adk.PentestResult, error),
) (*testsuite.TestWorkflowEnvironment, *[]childRun) {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "pentagi-flow-7"})
	env.RegisterWorkflowWithOptions(FlowWorkflow, workflow.RegisterOptions{Name: FlowWorkflowName})
	env.RegisterActivityWithOptions(func(context.Context, int64) error { return nil },
		activity.RegisterOptions{Name: ResumeFlowActivity})
	env.RegisterActivityWithOptions(render, activity.RegisterOptions{Name: RenderPromptActivity})

	var (
		mu   sync.Mutex
		runs []childRun
	)
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, req adk.PentestRequest) (adk.PentestResult, error) {
		res, err := child(ctx, req)
		mu.Lock()
		runs = append(runs, childRun{workflow.GetInfo(ctx).WorkflowExecution.ID, req})
		mu.Unlock()
		return res, err
	}, workflow.RegisterOptions{Name: adk.PentesterWorkflowName})

	emits = &emitRecorder{}
	env.RegisterActivityWithOptions(emits.record,
		activity.RegisterOptions{Name: adk.EmitFlowEventActivity})

	return env, &runs
}

// emitRecorder captures the parent's EmitFlowEvent calls; tests reach it
// through the package-level emits the env installs before registering.
type emitRecorder struct {
	mu     sync.Mutex
	inputs []adk.EmitFlowEventInput
}

func (r *emitRecorder) record(_ context.Context, in adk.EmitFlowEventInput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inputs = append(r.inputs, in)
	return nil
}

func (r *emitRecorder) statuses() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, in := range r.inputs {
		for _, ev := range in.Events {
			if ev.Op == adk.FlowEventSetStatus {
				out = append(out, ev.Status)
			}
		}
	}
	return out
}

var emits *emitRecorder

func renderOK(_ context.Context, req PromptRenderRequest) (RenderedPrompt, error) {
	return RenderedPrompt{Instruction: "rendered instruction", Question: "rendered " + req.Input}, nil
}

func adkParams(pending ...string) FlowParams {
	return FlowParams{
		FlowID:  7,
		Engine:  EngineAdk,
		Pending: pending,
		Policy:  testPolicy,
		ADK:     ADKPolicy{TaskQueue: "pentagi-adk", Model: "adk-model", ToolTimeoutSeconds: 60},
	}
}

func TestWorkflow_FlowWorkflow_AdkEngineDispatchesInputsToChildWorkflows(t *testing.T) {
	env, runs := newAdkEnv(t, renderOK, func(ctx workflow.Context, _ adk.PentestRequest) (adk.PentestResult, error) {
		return adk.PentestResult{EventCount: 4, HackResult: &adk.HackResult{Success: true, Result: "done"}}, nil
	})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalFinish, nil) }, time.Minute)

	env.ExecuteWorkflow(FlowWorkflowName, adkParams("scan target"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, *runs, 1)
	run := (*runs)[0]
	assert.Equal(t, "pentagi-flow-7-input-0", run.id)
	assert.Equal(t, adk.PentestRequest{
		Task:               "scan target",
		Model:              "adk-model",
		Instruction:        "rendered instruction",
		Question:           "rendered scan target",
		ToolTimeoutSeconds: 60,
		FlowID:             7,
		EmitEvents:         true,
		EmitTaskQueue:      "default-test-taskqueue",
	}, run.req)
	assert.Equal(t, []string{"running", "waiting"}, emits.statuses(),
		"the parent brackets the child run with the flow's status")
}

func TestWorkflow_FlowWorkflow_AdkEngineRecordsResultSummaryInState(t *testing.T) {
	env, _ := newAdkEnv(t, renderOK, func(ctx workflow.Context, _ adk.PentestRequest) (adk.PentestResult, error) {
		if err := workflow.Sleep(ctx, 30*time.Second); err != nil {
			return adk.PentestResult{}, err
		}
		return adk.PentestResult{EventCount: 4, HackResult: &adk.HackResult{Success: true, Result: "done"}}, nil
	})

	var state FlowState
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryState)
		require.NoError(t, err)
		require.NoError(t, val.Get(&state))
		env.SignalWorkflow(SignalFinish, nil)
	}, time.Minute)

	env.ExecuteWorkflow(FlowWorkflowName, adkParams("scan target"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, "adk", state.Engine)
	assert.Equal(t, 1, state.Processed)
	assert.Equal(t, `{"event_count":4,"hack_result":true,"hack_result_success":true}`, state.LastResult)
}

func TestWorkflow_FlowWorkflow_AdkEngineBuffersInputsWhileChildRuns(t *testing.T) {
	env, runs := newAdkEnv(t, renderOK, func(ctx workflow.Context, _ adk.PentestRequest) (adk.PentestResult, error) {
		if err := workflow.Sleep(ctx, 30*time.Second); err != nil {
			return adk.PentestResult{}, err
		}
		return adk.PentestResult{EventCount: 1}, nil
	})

	var state FlowState
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalInput, "two") }, 15*time.Second)
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryState)
		require.NoError(t, err)
		require.NoError(t, val.Get(&state))
	}, 20*time.Second)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalFinish, nil) }, 2*time.Minute)

	env.ExecuteWorkflow(FlowWorkflowName, adkParams("one"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, *runs, 2)
	assert.Equal(t, "pentagi-flow-7-input-0", (*runs)[0].id)
	assert.Equal(t, "one", (*runs)[0].req.Task)
	assert.Equal(t, "pentagi-flow-7-input-1", (*runs)[1].id)
	assert.Equal(t, "two", (*runs)[1].req.Task)
	assert.True(t, state.Running)
	assert.Equal(t, 1, state.Pending)
}

func TestWorkflow_FlowWorkflow_AdkEngineFinishCancelsRunningChild(t *testing.T) {
	cancelled := make(chan error, 1)
	env, runs := newAdkEnv(t, renderOK, func(ctx workflow.Context, _ adk.PentestRequest) (adk.PentestResult, error) {
		ctx.Done().Receive(ctx, nil)
		err := ctx.Err()
		cancelled <- err
		return adk.PentestResult{}, err
	})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalFinish, nil) }, 30*time.Second)

	env.ExecuteWorkflow(FlowWorkflowName, adkParams("run"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, *runs, 1)
	select {
	case err := <-cancelled:
		assert.True(t, temporal.IsCanceledError(err), "expected a canceled error, got %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the running child workflow was never cancelled")
	}
}

func TestWorkflow_FlowWorkflow_AdkEngineRenderFailureFallsBackToChildDefaults(t *testing.T) {
	env, runs := newAdkEnv(t,
		func(context.Context, PromptRenderRequest) (RenderedPrompt, error) {
			return RenderedPrompt{}, errors.New("template exploded")
		},
		func(_ workflow.Context, _ adk.PentestRequest) (adk.PentestResult, error) {
			return adk.PentestResult{EventCount: 1}, nil
		})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalFinish, nil) }, time.Minute)

	env.ExecuteWorkflow(FlowWorkflowName, adkParams("scan"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, *runs, 1)
	assert.Equal(t, "", (*runs)[0].req.Instruction)
	assert.Equal(t, "", (*runs)[0].req.Question)
}
