//go:build temporal_e2e

package temporal

import (
	"context"
	"sync"
	"testing"
	"time"

	"pentagi/pkg/config"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

type e2eRunner struct {
	mx    sync.Mutex
	block bool
	seen  chan InputRequest
}

func (r *e2eRunner) ResumeFlow(context.Context, int64) error          { return nil }
func (r *e2eRunner) LastTaskID(context.Context, int64) (int64, error) { return 5, nil }
func (r *e2eRunner) ProcessFlowInput(ctx context.Context, req InputRequest) error {
	r.seen <- req
	if r.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func TestE2E_DevServer_RetryRecoversAfterWorkerLoss(t *testing.T) {
	cfg := &config.Config{TemporalHostPort: "localhost:7233", TemporalNamespace: "default",
		TemporalTaskQueue: "e2e-" + time.Now().Format("150405"), TemporalActivityTimeout: 600,
		TemporalHeartbeatTimeout: 3, TemporalMaxAttempts: 5}
	ctx := context.Background()
	flowID := time.Now().UnixNano() % 1000000

	a, err := Dial(cfg)
	require.NoError(t, err)
	ra := &e2eRunner{block: true, seen: make(chan InputRequest, 4)}
	require.NoError(t, a.StartWorker(ra))
	require.NoError(t, a.StartFlow(ctx, flowID))
	require.NoError(t, a.SignalInput(ctx, flowID, "scan example.com"))

	first := <-ra.seen
	t.Logf("worker A got %+v", first)
	require.False(t, first.Recover)
	time.Sleep(2 * time.Second)

	st, err := a.QueryState(ctx, flowID)
	require.NoError(t, err)
	t.Logf("state while running: %+v", st)
	require.True(t, st.Running)

	a.Close()
	t.Log("worker A stopped mid-run")

	b, err := Dial(cfg)
	require.NoError(t, err)
	defer b.Close()
	rb := &e2eRunner{seen: make(chan InputRequest, 4)}
	require.NoError(t, b.StartWorker(rb))

	select {
	case retry := <-rb.seen:
		t.Logf("worker B got %+v", retry)
		require.True(t, retry.Recover)
		require.Equal(t, int64(5), retry.LastTaskID)
		require.Equal(t, "scan example.com", retry.Input)
	case <-time.After(60 * time.Second):
		t.Fatal("no retry reached worker B")
	}

	require.NoError(t, b.FinishFlow(ctx, flowID))
	require.Eventually(t, func() bool {
		d, err := b.client.DescribeWorkflowExecution(ctx, b.WorkflowID(flowID), "")
		return err == nil && d.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 30*time.Second, 500*time.Millisecond)
	t.Logf("workflow %s completed", b.WorkflowID(flowID))
}
