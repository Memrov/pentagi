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

	"pentagi/pkg/adk"
)

const (
	FlowWorkflowName     = "pentagi.FlowWorkflow"
	ResumeFlowActivity   = "pentagi.ResumeFlow"
	ProcessInputActivity = "pentagi.ProcessFlowInput"
	// RenderPromptActivity renders an agent's prompt pair where the DB and
	// the user prompter live; workflow code cannot reach either.
	RenderPromptActivity = "pentagi.RenderAgentPrompt"

	SignalInput  = "input"
	SignalFinish = "finish"
	QueryState   = "state"

	errTypeFlowClosed = "FlowClosed"
)

// Engine values mirror the FLOW_ENGINE enum on the flows row.
const (
	EngineLegacy = "legacy"
	EngineAdk    = "adk"
)

// Agent type names RenderPromptActivity serves; pentester maps to the
// pentester/question_pentester pair, adviser to the adviser system prompt
// (its question template needs per-call args, so only the instruction is
// rendered ahead of time).
const (
	AgentTypePentester = "pentester"
	AgentTypeAdviser   = "adviser"
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
// attempt began, so the runner can tell the task it created from older ones,
// and WaitingTaskID the task that was waiting for this input, if any.
type InputRequest struct {
	FlowID        int64
	Input         string
	Recover       bool
	LastTaskID    int64
	WaitingTaskID int64
}

// InputMark is what a flow looked like before the first attempt of an input
// ran; zero ids mean none.
type InputMark struct {
	LastTaskID    int64
	WaitingTaskID int64
}

// FlowRunner executes flow work inside the process that owns the flow.
// Calls for the same flow must be safe to run concurrently: a retry may
// start while a timed-out attempt is still unwinding.
type FlowRunner interface {
	ResumeFlow(ctx context.Context, flowID int64) error
	InputMark(ctx context.Context, flowID int64) (InputMark, error)
	ProcessFlowInput(ctx context.Context, req InputRequest) error
}

// FlowParams is the workflow input. Pending carries queued inputs and
// Processed the child-id counter across continue-as-new.
type FlowParams struct {
	FlowID  int64
	Resume  bool
	Engine  string
	Pending []string
	// Processed feeds the deterministic child workflow id, so it must never
	// restart at zero on a continued run.
	Processed  int
	LastResult string
	Policy     ActivityPolicy
	ADK        ADKPolicy
}

// ActivityPolicy is part of the workflow input rather than read from the
// environment, because workflow code must stay deterministic on replay.
type ActivityPolicy struct {
	StartToClose time.Duration
	Heartbeat    time.Duration
	MaxAttempts  int32
}

// ADKPolicy is the engine=adk dispatch config, carried in params for the same
// determinism reason as ActivityPolicy.
type ADKPolicy struct {
	TaskQueue          string
	Model              string
	ToolTimeoutSeconds int
}

// PromptRenderRequest asks the flows worker for an agent's prompt pair.
// IncludeAdvice tells the pentester render to name the advice tool in the
// toolset variables, matching the tools the child workflow will register;
// it is ignored for other agent types.
type PromptRenderRequest struct {
	FlowID        int64
	AgentType     string
	Input         string
	IncludeAdvice bool
}

type RenderedPrompt struct {
	Instruction string
	Question    string
}

// AgentPrompter is the render capability behind RenderPromptActivity; it is
// usually the same runner that serves FlowRunner.
type AgentPrompter interface {
	RenderAgentPrompt(ctx context.Context, req PromptRenderRequest) (RenderedPrompt, error)
}

// FlowEventer is the persistence capability behind adk.EmitFlowEventActivity:
// translated session events land on the flow's own log workers, which only
// the process holding the loaded flow can reach. It is usually the same
// runner that serves FlowRunner.
type FlowEventer interface {
	EmitFlowEvent(ctx context.Context, in adk.EmitFlowEventInput) error
}

type FlowState struct {
	FlowID     int64
	Engine     string
	Running    bool
	Pending    int
	Processed  int
	LastResult string
}

type ProcessInputArgs struct {
	FlowID int64
	Input  string
}
