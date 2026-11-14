package temporal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type fakeRunner struct {
	mx       sync.Mutex
	last     int64
	err      error
	requests []InputRequest
	resumed  []int64
}

func (r *fakeRunner) ResumeFlow(ctx context.Context, flowID int64) error {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.resumed = append(r.resumed, flowID)
	return r.err
}

func (r *fakeRunner) InputMark(ctx context.Context, _ int64) (InputMark, error) {
	return InputMark{LastTaskID: r.last, WaitingTaskID: 4}, ctx.Err()
}

func (r *fakeRunner) ProcessFlowInput(_ context.Context, req InputRequest) error {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.requests = append(r.requests, req)
	return r.err
}

func newActivityEnv(runner FlowRunner) *testsuite.TestActivityEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	acts := NewActivities(runner, time.Second)
	env.RegisterActivityWithOptions(acts.ProcessFlowInput, activity.RegisterOptions{Name: ProcessInputActivity})
	env.RegisterActivityWithOptions(acts.ResumeFlow, activity.RegisterOptions{Name: ResumeFlowActivity})
	return env
}

func TestActivities_ProcessFlowInput_FirstAttemptHeartbeatsLastTaskID(t *testing.T) {
	runner := &fakeRunner{last: 5}
	env := newActivityEnv(runner)

	var marks []InputMark
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		var mark InputMark
		if details.HasValues() && details.Get(&mark) == nil {
			marks = append(marks, mark)
		}
	})

	_, err := env.ExecuteActivity(ProcessInputActivity, ProcessInputArgs{FlowID: 3, Input: "scan"})

	require.NoError(t, err)
	assert.Equal(t, []InputRequest{{FlowID: 3, Input: "scan"}}, runner.requests)
	require.NotEmpty(t, marks)
	assert.Equal(t, InputMark{LastTaskID: 5, WaitingTaskID: 4}, marks[0])
}

func TestActivities_ProcessFlowInput_RetryRecoversFromHeartbeat(t *testing.T) {
	runner := &fakeRunner{last: 9}
	env := newActivityEnv(runner)
	env.SetHeartbeatDetails(InputMark{LastTaskID: 5, WaitingTaskID: 4})

	_, err := env.ExecuteActivity(ProcessInputActivity, ProcessInputArgs{FlowID: 3, Input: "scan"})

	require.NoError(t, err)
	assert.Equal(t, []InputRequest{{FlowID: 3, Input: "scan", Recover: true, LastTaskID: 5, WaitingTaskID: 4}}, runner.requests)
}

func TestActivities_ProcessFlowInput_ClosedFlowIsNotRetried(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		nonRetryable bool
	}{
		{"closed flow", ErrFlowClosed, true},
		{"flow not loaded yet", ErrFlowNotLoaded, false},
		{"transient failure", errors.New("database unavailable"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newActivityEnv(&fakeRunner{err: tc.err})

			_, err := env.ExecuteActivity(ProcessInputActivity, ProcessInputArgs{FlowID: 3, Input: "scan"})

			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr)
			assert.Equal(t, tc.nonRetryable, appErr.NonRetryable())
		})
	}
}

func TestActivities_ResumeFlow_PassesFlowID(t *testing.T) {
	runner := &fakeRunner{}
	env := newActivityEnv(runner)

	_, err := env.ExecuteActivity(ResumeFlowActivity, int64(3))

	require.NoError(t, err)
	assert.Equal(t, []int64{3}, runner.resumed)
}
