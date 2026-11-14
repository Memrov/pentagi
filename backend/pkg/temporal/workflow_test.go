package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
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
