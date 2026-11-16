package adk

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/adk/v2/session"
)

// EmitFlowEventActivity is the flows-queue activity the pentester workflow
// calls to persist one translated session event. It must run where the flow's
// in-memory log workers live, so the child schedules it on the parent's queue
// (PentestRequest.EmitTaskQueue), not its own.
const EmitFlowEventActivity = "pentagi.adk.EmitFlowEvent"

// FlowEventOp discriminates the fields of FlowEvent that carry meaning; the
// union stays flat so the DTO crosses the activity boundary as plain JSON.
type FlowEventOp string

const (
	FlowEventToolCallStart FlowEventOp = "tool_call_start"
	FlowEventToolCallEnd   FlowEventOp = "tool_call_end"
	FlowEventMessage       FlowEventOp = "message"
	FlowEventTermLog       FlowEventOp = "term_log"
	FlowEventSetStatus     FlowEventOp = "flow_status"
)

// MessageKind mirrors the database.MsglogType values a sink needs without
// importing the database package into workflow code.
type MessageKind string

const (
	MessageKindThoughts MessageKind = "thoughts"
	MessageKindTerminal MessageKind = "terminal"
	MessageKindFile     MessageKind = "file"
	MessageKindSearch   MessageKind = "search"
	MessageKindAnswer   MessageKind = "answer"
	MessageKindDone     MessageKind = "done"
	MessageKindReport   MessageKind = "report"
)

// FlowEvent is one translated op of a session event: a tool_call row's two
// halves, a standalone message, a terminal-log line or a flow status change.
type FlowEvent struct {
	Op        FlowEventOp     `json:"op"`
	CallID    string          `json:"call_id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Args      json.RawMessage `json:"args,omitempty"`
	Result    string          `json:"result,omitempty"`
	OK        bool            `json:"ok,omitempty"`
	Duration  float64         `json:"duration_seconds,omitempty"`
	Kind      MessageKind     `json:"kind,omitempty"`
	Headline  string          `json:"headline,omitempty"`
	Thinking  string          `json:"thinking,omitempty"`
	Direction string          `json:"direction,omitempty"`
	Text      string          `json:"text,omitempty"`
	Status    string          `json:"status,omitempty"`
}

// EmitFlowEventInput is the activity argument: the flow the rows belong to
// plus the event's ops, applied in order.
type EmitFlowEventInput struct {
	FlowID int64       `json:"flow_id"`
	Events []FlowEvent `json:"events"`
}

// TermLogSink writes one terminal-log line for a flow's primary container;
// direction is the termlog row type (stdin|stdout|stderr).
type TermLogSink interface {
	TermLog(ctx context.Context, direction, text string) error
}

// EventSink is the worker-side persistence seam behind EmitFlowEventActivity:
// the controller adapter implements it over the flow's own log workers, which
// publish every write into the UI subscriptions.
type EventSink interface {
	TermLogSink
	ToolCallStart(ctx context.Context, callID, name string, args json.RawMessage, thinking string) error
	ToolCallEnd(ctx context.Context, callID, result string, ok bool, durationSeconds float64) error
	Message(ctx context.Context, kind MessageKind, headline, result, thinking string) error
	SetFlowStatus(ctx context.Context, status string) error
}

// EmitActivityOptions bounds the observability write the way the other
// flows-queue calls are bounded; a failing emit is logged workflow-side and
// never fails the run, so the retry budget stays small. The parent flow
// workflow uses the same policy for its own flow_status emits.
func EmitActivityOptions(taskQueue string) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:           taskQueue,
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	}
}

// eventEmitter translates session events and fires EmitFlowEvent once per
// event. calls holds the function-call timestamps the response event's
// duration is measured against; both live in workflow memory, so replay
// rebuilds them from recorded history.
type eventEmitter struct {
	ctx     workflow.Context
	req     PentestRequest
	calls   map[string]time.Time
	seq     int
	enabled bool
}

func newEventEmitter(ctx workflow.Context, req PentestRequest) *eventEmitter {
	return &eventEmitter{ctx: ctx, req: req, calls: map[string]time.Time{}, enabled: req.EmitEvents}
}

// emit translates ev into ops and schedules one EmitFlowEvent for them. An
// emit failure is observability loss, never a run failure: the workflow logs
// and continues, keeping a dead flows queue from stalling the agent.
func (e *eventEmitter) emit(ev *session.Event) {
	if !e.enabled {
		return
	}
	ops := e.translate(ev)
	if len(ops) == 0 {
		return
	}
	ectx := workflow.WithActivityOptions(e.ctx, EmitActivityOptions(e.req.EmitTaskQueue))
	err := workflow.ExecuteActivity(ectx, EmitFlowEventActivity, EmitFlowEventInput{
		FlowID: e.req.FlowID,
		Events: ops,
	}).Get(ectx, nil)
	if err != nil {
		workflow.GetLogger(e.ctx).Warn("adk flow event emit failed",
			"flow_id", e.req.FlowID, "ops", len(ops), "error", err)
	}
}

// translate maps one session event's parts onto ops: reasoning parts fold
// into the sibling ops' thinking field, calls open toolcall+message rows,
// responses close them with the folded result, and plain text lands as a
// thoughts message. A hack_result call additionally closes the flow's
// transcript the way production's done barrier does.
func (e *eventEmitter) translate(ev *session.Event) []FlowEvent {
	if ev == nil || ev.Content == nil || ev.Author == "user" {
		return nil
	}

	var thinking []string
	for _, p := range ev.Content.Parts {
		if p != nil && p.Thought && p.Text != "" {
			thinking = append(thinking, p.Text)
		}
	}
	reasoning := strings.Join(thinking, "\n")

	var ops []FlowEvent
	for _, p := range ev.Content.Parts {
		switch {
		case p == nil:
		case p.FunctionCall != nil:
			fc := p.FunctionCall
			id := fc.ID
			if id == "" {
				// ADK fills empty call ids before emitting, but a caller-built
				// event may still lack one; the event's own position in the
				// stream keeps the response pairing deterministic.
				e.seq++
				id = fmt.Sprintf("%s#%d", fc.Name, e.seq)
			}
			args, err := json.Marshal(fc.Args)
			if err != nil {
				args = []byte("{}")
			}
			ops = append(ops, FlowEvent{
				Op: FlowEventToolCallStart, CallID: id, Name: fc.Name, Args: args, Thinking: reasoning,
			})
			e.calls[id] = ev.Timestamp
			if fc.Name == HackResultToolName {
				if done := hackResultDoneEvent(fc.Args, reasoning); done != nil {
					ops = append(ops, *done)
				}
			}
		case p.FunctionResponse != nil:
			fr := p.FunctionResponse
			id := fr.ID
			if id == "" {
				id = fr.Name
			}
			result, ok := foldToolResult(fr.Response)
			var dur float64
			if started, has := e.calls[id]; has {
				dur = ev.Timestamp.Sub(started).Seconds()
				delete(e.calls, id)
			}
			ops = append(ops, FlowEvent{
				Op: FlowEventToolCallEnd, CallID: id, Result: result, OK: ok, Duration: dur,
			})
		case p.Text != "" && !p.Thought:
			ops = append(ops, FlowEvent{
				Op: FlowEventMessage, Kind: MessageKindThoughts, Headline: p.Text, Thinking: reasoning,
			})
		}
	}
	return ops
}

// hackResultDoneEvent builds the closing message for a hack_result call: the
// report the model placed in the call's args. Production writes the same
// payload as a report msglog once the done barrier fires; here it is one done
// row, because the ADK barrier never runs a summarization turn.
func hackResultDoneEvent(args map[string]any, thinking string) *FlowEvent {
	result, _ := args["result"].(string)
	if result == "" {
		return nil
	}
	headline, _ := args["message"].(string)
	if headline == "" {
		headline = HackResultToolName
	}
	return &FlowEvent{
		Op: FlowEventMessage, Kind: MessageKindDone, Headline: headline, Result: result, Thinking: thinking,
	}
}

// foldToolResult renders the toolcall row's result the way the production
// executor records it: the output text with the machine fields appended as
// markers. Terminal output already carries its "[exit code: N]" footer, so
// the fold only appends what the output does not already say; a failed call
// surfaces ADK's {"error": ...} payload as the failure text.
func foldToolResult(resp map[string]any) (result string, ok bool) {
	if len(resp) == 0 {
		return "", true
	}
	if errText, has := resp["error"]; has {
		return fmt.Sprintf("%v", errText), false
	}
	output, _ := resp["output"].(string)
	if output == "" {
		if text, has := resp["result"].(string); has {
			output = text
		}
	}
	if output == "" && len(resp) > 0 {
		if raw, err := json.Marshal(resp); err == nil {
			output = string(raw)
		}
	}
	result = output
	if code, has := numericField(resp["exit_code"]); has && code != 0 && !strings.Contains(output, "[exit code:") {
		result += fmt.Sprintf("\n[exit code: %d]", code)
	}
	if uri, _ := resp["payload_uri"].(string); uri != "" {
		result += "\n[payload: " + uri + "]"
	}
	if timed, _ := resp["timed_out"].(bool); timed && !strings.Contains(result, "timeout") {
		result += "\n[timed out]"
	}
	return result, true
}

// numericField reads a JSON-decoded number; the Temporal converter hands
// activity results back as float64 while a direct call may keep an int.
func numericField(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	}
	return 0, false
}
