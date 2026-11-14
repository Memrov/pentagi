// Package temporal runs each flow as a Temporal workflow, so the queue of
// user input and the run in progress survive a backend restart and an
// interrupted run is resumed by a retried activity instead of being lost.
//
// The workflow only orders work; the agent itself still runs inside this
// process through FlowRunner, which is why the activity worker must live in
// the same process that holds the loaded flows.
package temporal

import (
	"context"
	"errors"
	"time"
)

const (
	FlowWorkflowName     = "pentagi.FlowWorkflow"
	ResumeFlowActivity   = "pentagi.ResumeFlow"
	ProcessInputActivity = "pentagi.ProcessFlowInput"

	SignalInput  = "input"
	SignalFinish = "finish"
	QueryState   = "state"

	errTypeFlowClosed = "FlowClosed"
)

var (
	// ErrFlowClosed is returned by a FlowRunner for a flow that was finished
	// or stopped for good; the activity is not retried.
	ErrFlowClosed = errors.New("flow is closed")
	// ErrFlowNotLoaded is returned by a FlowRunner for a flow this process
	// has not loaded yet; the activity is retried.
	ErrFlowNotLoaded = errors.New("flow is not loaded")
)

// InputRequest is one user input handed to a FlowRunner. Recover is set when
// an earlier attempt of the same input may have started work before it was
// interrupted; LastTaskID is then the newest task id seen before that first
// attempt began, so the runner can tell the task it created from older ones.
type InputRequest struct {
	FlowID     int64
	Input      string
	Recover    bool
	LastTaskID int64
}

// FlowRunner executes flow work inside the process that owns the flow.
// Calls for the same flow must be safe to run concurrently: a retry may
// start while a timed-out attempt is still unwinding.
type FlowRunner interface {
	ResumeFlow(ctx context.Context, flowID int64) error
	LastTaskID(ctx context.Context, flowID int64) (int64, error)
	ProcessFlowInput(ctx context.Context, req InputRequest) error
}

// FlowParams is the workflow input. Pending carries inputs across
// continue-as-new.
type FlowParams struct {
	FlowID  int64
	Resume  bool
	Pending []string
	Policy  ActivityPolicy
}

// ActivityPolicy is part of the workflow input rather than read from the
// environment, because workflow code must stay deterministic on replay.
type ActivityPolicy struct {
	StartToClose time.Duration
	Heartbeat    time.Duration
	MaxAttempts  int32
}

type FlowState struct {
	FlowID    int64
	Running   bool
	Pending   int
	Processed int
}

type ProcessInputArgs struct {
	FlowID int64
	Input  string
}
