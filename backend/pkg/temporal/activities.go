package temporal

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type Activities struct {
	runner    FlowRunner
	heartbeat time.Duration
}

// NewActivities heartbeats every interval while a run is in progress; it
// must be well below the heartbeat timeout of the activity options.
func NewActivities(runner FlowRunner, interval time.Duration) *Activities {
	return &Activities{runner: runner, heartbeat: interval}
}

type inputMark struct {
	LastTaskID int64
}

func (a *Activities) ResumeFlow(ctx context.Context, flowID int64) error {
	defer a.keepAlive(ctx, nil)()
	return classify(a.runner.ResumeFlow(ctx, flowID))
}

// ProcessFlowInput records the newest task id in its first heartbeat, so a
// retry after a crash can find the task the interrupted attempt created and
// resume it instead of creating a second one from the same input.
func (a *Activities) ProcessFlowInput(ctx context.Context, args ProcessInputArgs) error {
	req := InputRequest{FlowID: args.FlowID, Input: args.Input}

	var mark inputMark
	if activity.HasHeartbeatDetails(ctx) && activity.GetHeartbeatDetails(ctx, &mark) == nil {
		req.Recover = true
		req.LastTaskID = mark.LastTaskID
	} else {
		last, err := a.runner.LastTaskID(ctx, args.FlowID)
		if err != nil {
			return classify(err)
		}
		mark.LastTaskID = last
	}

	defer a.keepAlive(ctx, mark)()
	return classify(a.runner.ProcessFlowInput(ctx, req))
}

func (a *Activities) keepAlive(ctx context.Context, details any) (stop func()) {
	record := func() {
		if details == nil {
			activity.RecordHeartbeat(ctx)
		} else {
			activity.RecordHeartbeat(ctx, details)
		}
	}
	record()

	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(a.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				record()
			}
		}
	}()

	return func() {
		close(done)
		<-stopped
	}
}

func classify(err error) error {
	if errors.Is(err, ErrFlowClosed) {
		return temporal.NewNonRetryableApplicationError(err.Error(), errTypeFlowClosed, err)
	}
	return err
}
