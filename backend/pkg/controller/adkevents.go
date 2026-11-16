package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"pentagi/pkg/adk"
	"pentagi/pkg/database"
	"pentagi/pkg/temporal"
)

// adkCallRef correlates one function call's two writes: the toolcalls row a
// ToolCallStart opened and the msglogs row the same call's headline lives in,
// so a ToolCallEnd can close both the way the production executor does.
type adkCallRef struct {
	logID  int64
	msgID  int64
	format database.MsglogResultFormat
}

// adkEventSink lands translated ADK session events on a flow's own log
// workers — the same rows and subscription events the legacy executor
// produces, so the UI needs nothing new. The calls map is the correlation the
// worker-level API lacks: emits arrive one activity invocation at a time, so
// start and end of a call meet only here. It is per-flow-worker state and
// does not survive a restart; a close whose start predates the restart falls
// back to the call-id index in the toolcalls table.
type adkEventSink struct {
	flowCtx   *FlowContext
	setStatus func(ctx context.Context, status database.FlowStatus) error

	mx        sync.Mutex
	calls     map[string]adkCallRef
	container int64
	haveCnt   bool
}

func newADKEventSink(flowCtx *FlowContext, setStatus func(context.Context, database.FlowStatus) error) *adkEventSink {
	return &adkEventSink{flowCtx: flowCtx, setStatus: setStatus, calls: map[string]adkCallRef{}}
}

var _ adk.EventSink = (*adkEventSink)(nil)

// emit applies one translated event's ops in order. A failure aborts the
// batch and the activity retries it — the calls map makes the retry
// idempotent for the start half of a call, so a repeated batch cannot open a
// second running row for the same call id.
func (s *adkEventSink) emit(ctx context.Context, events []adk.FlowEvent) error {
	for _, ev := range events {
		var err error
		switch ev.Op {
		case adk.FlowEventToolCallStart:
			err = s.ToolCallStart(ctx, ev.CallID, ev.Name, ev.Args, ev.Thinking)
		case adk.FlowEventToolCallEnd:
			err = s.ToolCallEnd(ctx, ev.CallID, ev.Result, ev.OK, ev.Duration)
		case adk.FlowEventMessage:
			err = s.Message(ctx, ev.Kind, ev.Headline, ev.Result, ev.Thinking)
		case adk.FlowEventTermLog:
			err = s.TermLog(ctx, ev.Direction, ev.Text)
		case adk.FlowEventSetStatus:
			err = s.SetFlowStatus(ctx, ev.Status)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// ToolCallStart opens the toolcall row and the paired msglog headline, the
// two rows the production executor writes around every call. The msglog type
// comes from the tool name; the headline is the call's message arg where the
// tool carries one. The whole pair runs under the sink mutex: a retry whose
// earlier attempt is still mid-flight must not open the call twice.
func (s *adkEventSink) ToolCallStart(ctx context.Context, callID, name string, args json.RawMessage, thinking string) error {
	callID = database.SanitizeUTF8(callID)
	name = database.SanitizeUTF8(name)
	args = json.RawMessage(database.SanitizeUTF8(string(args)))

	s.mx.Lock()
	defer s.mx.Unlock()
	if _, ok := s.calls[callID]; ok {
		return nil
	}

	logID, err := s.flowCtx.ToolCallLog.PutLog(ctx, callID, name, args, nil, nil)
	if err != nil {
		return err
	}
	msgID, err := s.flowCtx.MsgLog.PutFlowMsg(ctx, adkMsglogType(name), thinking, adkHeadline(name, args))
	if err != nil {
		return err
	}

	s.calls[callID] = adkCallRef{logID: logID, msgID: msgID, format: adkResultFormat(name)}
	return nil
}

// ToolCallEnd closes both rows the start opened. A call id this process never
// saw — a restart between the two emits — still reaches the toolcalls row
// through its call-id index; the paired msglog has no such index and is left
// at its headline, which is the same loss production accepts for a crash
// mid-call.
func (s *adkEventSink) ToolCallEnd(ctx context.Context, callID, result string, ok bool, durationSeconds float64) error {
	callID = database.SanitizeUTF8(callID)
	result = database.SanitizeUTF8(result)

	s.mx.Lock()
	ref, found := s.calls[callID]
	if found {
		delete(s.calls, callID)
	}
	s.mx.Unlock()

	if !found {
		tc, err := s.flowCtx.DB.GetCallToolcall(ctx, callID)
		if err != nil || tc.FlowID != s.flowCtx.FlowID {
			return nil
		}
		ref = adkCallRef{logID: tc.ID, format: adkResultFormat(tc.Name)}
	}
	if ref.msgID != 0 {
		if err := s.flowCtx.MsgLog.UpdateMsgResult(ctx, ref.msgID, 0, result, ref.format); err != nil {
			return err
		}
	}
	if ok {
		return s.flowCtx.ToolCallLog.UpdateLogSuccess(ctx, ref.logID, result, durationSeconds)
	}
	return s.flowCtx.ToolCallLog.UpdateLogFailed(ctx, ref.logID, result, durationSeconds)
}

// Message writes a standalone msglog row — reasoning text and the closing
// report the workflow emits outside any call pairing.
func (s *adkEventSink) Message(ctx context.Context, kind adk.MessageKind, headline, result, thinking string) error {
	msgType := adkMessageType(kind)
	if result == "" {
		_, err := s.flowCtx.MsgLog.PutFlowMsg(ctx, msgType, thinking, headline)
		return err
	}
	_, err := s.flowCtx.MsgLog.PutFlowMsgResult(ctx, msgType, thinking, headline, result,
		database.MsglogResultFormatMarkdown)
	return err
}

// TermLog writes one terminal-log row to the flow's primary container — the
// seam the ADK terminal activity calls directly, because an activity cannot
// schedule another activity.
func (s *adkEventSink) TermLog(ctx context.Context, direction, text string) error {
	containerID, err := s.primaryContainer(ctx)
	if err != nil {
		return err
	}
	var msgType database.TermlogType
	switch direction {
	case string(database.TermlogTypeStdin):
		msgType = database.TermlogTypeStdin
	case string(database.TermlogTypeStderr):
		msgType = database.TermlogTypeStderr
	default:
		msgType = database.TermlogTypeStdout
	}
	_, err = s.flowCtx.TermLog.PutMsg(ctx, msgType, text, containerID, nil, nil)
	return err
}

// SetFlowStatus applies the flow-status ops the parent workflow brackets a
// child run with — the running/waiting pair the legacy task worker maintains
// itself. Other statuses stay unreachable: finished and failed keep their own
// dedicated paths.
func (s *adkEventSink) SetFlowStatus(ctx context.Context, status string) error {
	switch database.FlowStatus(status) {
	case database.FlowStatusRunning, database.FlowStatusWaiting:
	default:
		return fmt.Errorf("adk flow status %q is not settable through the event sink", status)
	}
	if s.setStatus == nil {
		return nil
	}
	return s.setStatus(ctx, database.FlowStatus(status))
}

// primaryContainer resolves the flow's primary container id once; termlog
// rows are container-scoped and the ADK exec target is always the primary.
func (s *adkEventSink) primaryContainer(ctx context.Context) (int64, error) {
	s.mx.Lock()
	defer s.mx.Unlock()
	if s.haveCnt {
		return s.container, nil
	}
	cnt, err := s.flowCtx.DB.GetFlowPrimaryContainer(ctx, s.flowCtx.FlowID)
	if err != nil {
		return 0, fmt.Errorf("failed to get flow %d primary container: %w", s.flowCtx.FlowID, err)
	}
	s.container, s.haveCnt = cnt.ID, true
	return cnt.ID, nil
}

// adkMsglogType is the tool-name half of the production registry's
// getMessageType, reduced to the tools the ADK set ships.
func adkMsglogType(name string) database.MsglogType {
	switch name {
	case adk.TerminalToolName:
		return database.MsglogTypeTerminal
	case adk.FileToolName:
		return database.MsglogTypeFile
	case adk.NucleiToolName, adk.HttpxToolName:
		return database.MsglogTypeSearch
	default:
		return database.MsglogTypeThoughts
	}
}

// adkResultFormat is the matching half of getMessageResultFormat: terminal
// output renders raw, file contents stay plain, everything else is markdown.
func adkResultFormat(name string) database.MsglogResultFormat {
	switch name {
	case adk.TerminalToolName:
		return database.MsglogResultFormatTerminal
	case adk.FileToolName:
		return database.MsglogResultFormatPlain
	default:
		return database.MsglogResultFormatMarkdown
	}
}

// adkMessageType keeps the workflow-side kind strings honest against the
// database enum; anything unmapped degrades to a thoughts row rather than an
// insert the enum would reject.
func adkMessageType(kind adk.MessageKind) database.MsglogType {
	switch kind {
	case adk.MessageKindAnswer:
		return database.MsglogTypeAnswer
	case adk.MessageKindDone:
		return database.MsglogTypeDone
	case adk.MessageKindReport:
		return database.MsglogTypeReport
	case adk.MessageKindTerminal:
		return database.MsglogTypeTerminal
	case adk.MessageKindFile:
		return database.MsglogTypeFile
	case adk.MessageKindSearch:
		return database.MsglogTypeSearch
	default:
		return database.MsglogTypeThoughts
	}
}

// adkHeadline is the msglog's headline column: the call's one-line message
// arg where the schema carries it, the tool name otherwise.
func adkHeadline(name string, args json.RawMessage) string {
	var fields struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(args, &fields); err == nil && fields.Message != "" {
		return fields.Message
	}
	return name
}

// EmitFlowEvent resolves the flow the events belong to and applies them —
// the FlowEventer half of durableRunner. Log rows are in-memory per loaded
// flow, so the same registry claim the run itself takes applies here.
func (r durableRunner) EmitFlowEvent(ctx context.Context, in adk.EmitFlowEventInput) error {
	w, err := r.worker(ctx, in.FlowID)
	if err != nil {
		return err
	}
	if w.events == nil {
		return fmt.Errorf("flow %d has no adk event sink", in.FlowID)
	}
	return w.events.emit(ctx, in.Events)
}

// NewADKTermLogResolver binds adk terminal exec to the flow's terminal log
// through the same worker the sandbox resolver finds — the activity supplies
// the flow id parsed from its child workflow id, and a flow this process has
// not loaded reports itself unloaded, which keeps the CLI path quiet.
func NewADKTermLogResolver(fc FlowController) func(context.Context, int64) (adk.TermLogSink, error) {
	return func(ctx context.Context, flowID int64) (adk.TermLogSink, error) {
		ctl, ok := fc.(*flowController)
		if !ok {
			return nil, fmt.Errorf("unsupported flow controller %T", fc)
		}
		fw, err := ctl.GetFlow(ctx, flowID)
		if err != nil || fw == nil {
			return nil, fmt.Errorf("flow %d: %w", flowID, temporal.ErrFlowNotLoaded)
		}
		w, ok := fw.(*flowWorker)
		if !ok {
			return nil, fmt.Errorf("flow %d has unsupported worker %T", flowID, fw)
		}
		if w.events == nil {
			return nil, fmt.Errorf("flow %d has no adk event sink", flowID)
		}
		return w.events, nil
	}
}
