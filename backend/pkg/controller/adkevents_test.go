package controller

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"

	"pentagi/pkg/adk"
	"pentagi/pkg/database"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// adkSinkFakes holds the in-memory workers the sink delegates to; each fake
// records its calls the way the real workers record rows.
type adkSinkFakes struct {
	msg     *fakeMsgLog
	tool    *fakeToolCallLog
	term    *fakeTermLog
	querier *adkEventQuerier
	status  []database.FlowStatus
}

type msgLogCall struct {
	method string
	typ    database.MsglogType
	think  string
	msg    string
	result string
	format database.MsglogResultFormat
	id     int64
}

type fakeMsgLog struct {
	mx     sync.Mutex
	calls  []msgLogCall
	nextID int64
}

func (f *fakeMsgLog) PutMsg(context.Context, database.MsglogType, *int64, *int64, int64, string, string) (int64, error) {
	panic("unimplemented")
}

func (f *fakeMsgLog) PutFlowMsg(_ context.Context, typ database.MsglogType, thinking, msg string) (int64, error) {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.nextID++
	f.calls = append(f.calls, msgLogCall{method: "put", typ: typ, think: thinking, msg: msg, id: f.nextID})
	return f.nextID, nil
}

func (f *fakeMsgLog) PutFlowMsgResult(
	_ context.Context, typ database.MsglogType, thinking, msg, result string, format database.MsglogResultFormat,
) (int64, error) {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.nextID++
	f.calls = append(f.calls, msgLogCall{
		method: "put_result", typ: typ, think: thinking, msg: msg, result: result, format: format, id: f.nextID,
	})
	return f.nextID, nil
}

func (f *fakeMsgLog) PutTaskMsg(context.Context, database.MsglogType, int64, string, string) (int64, error) {
	panic("unimplemented")
}

func (f *fakeMsgLog) PutTaskMsgResult(
	context.Context, database.MsglogType, int64, string, string, string, database.MsglogResultFormat,
) (int64, error) {
	panic("unimplemented")
}

func (f *fakeMsgLog) PutSubtaskMsg(context.Context, database.MsglogType, int64, int64, string, string) (int64, error) {
	panic("unimplemented")
}

func (f *fakeMsgLog) PutSubtaskMsgResult(
	context.Context, database.MsglogType, int64, int64, string, string, string, database.MsglogResultFormat,
) (int64, error) {
	panic("unimplemented")
}

func (f *fakeMsgLog) UpdateMsgResult(
	_ context.Context, msgID, _ int64, result string, format database.MsglogResultFormat,
) error {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.calls = append(f.calls, msgLogCall{method: "update", id: msgID, result: result, format: format})
	return nil
}

func (f *fakeMsgLog) recorded() []msgLogCall {
	f.mx.Lock()
	defer f.mx.Unlock()
	return slices.Clone(f.calls)
}

type toolCallRecord struct {
	method   string
	callID   string
	name     string
	args     json.RawMessage
	id       int64
	result   string
	duration float64
}

type fakeToolCallLog struct {
	mx     sync.Mutex
	calls  []toolCallRecord
	nextID int64
}

func (f *fakeToolCallLog) PutLog(
	_ context.Context, callID, name string, args json.RawMessage, _, _ *int64,
) (int64, error) {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.nextID++
	f.calls = append(f.calls, toolCallRecord{
		method: "put", callID: callID, name: name, args: args, id: f.nextID,
	})
	return f.nextID, nil
}

func (f *fakeToolCallLog) UpdateLogSuccess(_ context.Context, id int64, result string, dur float64) error {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.calls = append(f.calls, toolCallRecord{method: "success", id: id, result: result, duration: dur})
	return nil
}

func (f *fakeToolCallLog) UpdateLogFailed(_ context.Context, id int64, result string, dur float64) error {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.calls = append(f.calls, toolCallRecord{method: "failed", id: id, result: result, duration: dur})
	return nil
}

func (f *fakeToolCallLog) GetLog(context.Context, int64) (database.Toolcall, error) {
	panic("unimplemented")
}

func (f *fakeToolCallLog) recorded() []toolCallRecord {
	f.mx.Lock()
	defer f.mx.Unlock()
	return slices.Clone(f.calls)
}

type termLogRecord struct {
	typ       database.TermlogType
	text      string
	container int64
}

type fakeTermLog struct {
	mx    sync.Mutex
	calls []termLogRecord
}

func (f *fakeTermLog) PutMsg(
	_ context.Context, typ database.TermlogType, msg string, containerID int64, _, _ *int64,
) (int64, error) {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.calls = append(f.calls, termLogRecord{typ: typ, text: msg, container: containerID})
	return int64(len(f.calls)), nil
}

func (f *fakeTermLog) GetMsg(context.Context, int64) (database.Termlog, error) {
	panic("unimplemented")
}
func (f *fakeTermLog) GetContainers(context.Context) ([]database.Container, error) {
	panic("unimplemented")
}
func (f *fakeTermLog) ContainerNotRunning(context.Context, int64, *int64, *int64) error {
	panic("unimplemented")
}
func (f *fakeTermLog) ContainerRunning(context.Context, int64, *int64, *int64) error {
	panic("unimplemented")
}

func (f *fakeTermLog) recorded() []termLogRecord {
	f.mx.Lock()
	defer f.mx.Unlock()
	return slices.Clone(f.calls)
}

type adkEventQuerier struct {
	database.Querier

	primary   database.Container
	primaryEr error
	byCallID  map[string]database.Toolcall
}

func (q *adkEventQuerier) GetFlowPrimaryContainer(context.Context, int64) (database.Container, error) {
	if q.primaryEr != nil {
		return database.Container{}, q.primaryEr
	}
	return q.primary, nil
}

func (q *adkEventQuerier) GetCallToolcall(_ context.Context, callID string) (database.Toolcall, error) {
	if tc, ok := q.byCallID[callID]; ok {
		return tc, nil
	}
	return database.Toolcall{}, errors.New("no rows")
}

func newADKSinkFixture() (*adkEventSink, *adkSinkFakes) {
	f := &adkSinkFakes{
		msg:     &fakeMsgLog{},
		tool:    &fakeToolCallLog{},
		term:    &fakeTermLog{},
		querier: &adkEventQuerier{primary: database.Container{ID: 9, FlowID: 7}},
	}
	flowCtx := &FlowContext{
		DB:          f.querier,
		FlowID:      7,
		MsgLog:      f.msg,
		TermLog:     f.term,
		ToolCallLog: f.tool,
	}
	sink := newADKEventSink(flowCtx, func(_ context.Context, status database.FlowStatus) error {
		f.status = append(f.status, status)
		return nil
	})
	return sink, f
}

func TestADKEventSink_ToolCallStartOpensToolCallAndTypedMsgLog(t *testing.T) {
	sink, f := newADKSinkFixture()

	err := sink.ToolCallStart(context.Background(), "call-1", adk.TerminalToolName,
		json.RawMessage(`{"input":"id","message":"whoami"}`), "")
	require.NoError(t, err)

	toolCalls := f.tool.recorded()
	require.Len(t, toolCalls, 1)
	assert.Equal(t, "put", toolCalls[0].method)
	assert.Equal(t, "call-1", toolCalls[0].callID)
	assert.Equal(t, adk.TerminalToolName, toolCalls[0].name)
	assert.JSONEq(t, `{"input":"id","message":"whoami"}`, string(toolCalls[0].args))

	msgs := f.msg.recorded()
	require.Len(t, msgs, 1)
	assert.Equal(t, "put", msgs[0].method)
	assert.Equal(t, database.MsglogTypeTerminal, msgs[0].typ)
	assert.Equal(t, "whoami", msgs[0].msg, "the message arg is the headline")
}

func TestADKEventSink_ToolCallEndClosesBothRows(t *testing.T) {
	sink, f := newADKSinkFixture()
	require.NoError(t, sink.ToolCallStart(context.Background(), "call-1", adk.TerminalToolName,
		json.RawMessage(`{"input":"id"}`), ""))

	err := sink.ToolCallEnd(context.Background(), "call-1", "uid=0\n[exit code: 0]", true, 1.5)
	require.NoError(t, err)

	toolCalls := f.tool.recorded()
	require.Len(t, toolCalls, 2)
	assert.Equal(t, "success", toolCalls[1].method)
	assert.Equal(t, toolCalls[0].id, toolCalls[1].id, "the end closes the row the start opened")
	assert.Equal(t, "uid=0\n[exit code: 0]", toolCalls[1].result)
	assert.Equal(t, 1.5, toolCalls[1].duration)

	msgs := f.msg.recorded()
	require.Len(t, msgs, 2)
	assert.Equal(t, "update", msgs[1].method)
	assert.Equal(t, msgs[0].id, msgs[1].id, "the paired msglog gets the result")
	assert.Equal(t, "uid=0\n[exit code: 0]", msgs[1].result)
	assert.Equal(t, database.MsglogResultFormatTerminal, msgs[1].format)
}

func TestADKEventSink_ToolCallEndFailedUsesFailedUpdate(t *testing.T) {
	sink, f := newADKSinkFixture()
	require.NoError(t, sink.ToolCallStart(context.Background(), "call-1", adk.NucleiToolName,
		json.RawMessage(`{"target":"https://t"}`), ""))

	require.NoError(t, sink.ToolCallEnd(context.Background(), "call-1", "activity timeout", false, 0.5))

	toolCalls := f.tool.recorded()
	require.Len(t, toolCalls, 2)
	assert.Equal(t, "failed", toolCalls[1].method)
	assert.Equal(t, "activity timeout", toolCalls[1].result)

	msgs := f.msg.recorded()
	require.Len(t, msgs, 2)
	assert.Equal(t, database.MsglogTypeSearch, msgs[0].typ)
	assert.Equal(t, "nuclei_scan", msgs[0].msg, "no message arg falls back to the tool name")
	assert.Equal(t, database.MsglogResultFormatMarkdown, msgs[1].format)
}

func TestADKEventSink_ToolCallEndUnknownCallDropsSilently(t *testing.T) {
	sink, f := newADKSinkFixture()

	require.NoError(t, sink.ToolCallEnd(context.Background(), "gone", "x", true, 0))
	assert.Empty(t, f.tool.recorded())
	assert.Empty(t, f.msg.recorded())
}

func TestADKEventSink_MessageWritesStandaloneRows(t *testing.T) {
	sink, f := newADKSinkFixture()

	require.NoError(t, sink.Message(context.Background(), adk.MessageKindThoughts, "thinking out loud", "", "reasoning"))
	require.NoError(t, sink.Message(context.Background(), adk.MessageKindDone, "pentest complete", "root flag", ""))

	msgs := f.msg.recorded()
	require.Len(t, msgs, 2)
	assert.Equal(t, "put", msgs[0].method)
	assert.Equal(t, database.MsglogTypeThoughts, msgs[0].typ)
	assert.Equal(t, "thinking out loud", msgs[0].msg)
	assert.Equal(t, "reasoning", msgs[0].think)

	assert.Equal(t, "put_result", msgs[1].method)
	assert.Equal(t, database.MsglogTypeDone, msgs[1].typ)
	assert.Equal(t, "pentest complete", msgs[1].msg)
	assert.Equal(t, "root flag", msgs[1].result)
	assert.Equal(t, database.MsglogResultFormatMarkdown, msgs[1].format)
}

func TestADKEventSink_TermLogWritesToPrimaryContainer(t *testing.T) {
	sink, f := newADKSinkFixture()

	require.NoError(t, sink.TermLog(context.Background(), "stdin", "/work $ id\r\n"))
	require.NoError(t, sink.TermLog(context.Background(), "stdout", "uid=0\r\n"))
	require.NoError(t, sink.TermLog(context.Background(), "stderr", "warn\r\n"))

	terms := f.term.recorded()
	require.Len(t, terms, 3)
	assert.Equal(t, database.TermlogTypeStdin, terms[0].typ)
	assert.Equal(t, int64(9), terms[0].container, "termlog rows bind to the primary container")
	assert.Equal(t, database.TermlogTypeStdout, terms[1].typ)
	assert.Equal(t, database.TermlogTypeStderr, terms[2].typ)
}

func TestADKEventSink_SetFlowStatusOnlyTakesRunningAndWaiting(t *testing.T) {
	sink, f := newADKSinkFixture()

	require.NoError(t, sink.SetFlowStatus(context.Background(), "running"))
	require.NoError(t, sink.SetFlowStatus(context.Background(), "waiting"))
	assert.Equal(t, []database.FlowStatus{database.FlowStatusRunning, database.FlowStatusWaiting}, f.status)
	assert.Error(t, sink.SetFlowStatus(context.Background(), "finished"))
}

func TestADKEventSink_SanitizesInvalidUTF8(t *testing.T) {
	sink, f := newADKSinkFixture()

	require.NoError(t, sink.ToolCallStart(context.Background(), "call-\xff\x00", "term\x00inal",
		json.RawMessage("{\"input\":\"i\x00d\"}"), "th\x00ink"))

	toolCalls := f.tool.recorded()
	require.Len(t, toolCalls, 1)
	assert.Equal(t, "call-�", toolCalls[0].callID)
	assert.Equal(t, "terminal", toolCalls[0].name)
	assert.JSONEq(t, `{"input":"id"}`, string(toolCalls[0].args))

	require.NoError(t, sink.ToolCallEnd(context.Background(), "call-�", "out\x00put", true, 0))
	require.Equal(t, "output", f.tool.recorded()[1].result)
}

// A duplicate ToolCallStart — the emit retry after a mid-batch failure —
// must not open a second pair of rows for the same call.
func TestADKEventSink_ToolCallStartIsIdempotentPerCallID(t *testing.T) {
	sink, f := newADKSinkFixture()
	args := json.RawMessage(`{"input":"id"}`)

	require.NoError(t, sink.ToolCallStart(context.Background(), "call-1", adk.TerminalToolName, args, ""))
	require.NoError(t, sink.ToolCallStart(context.Background(), "call-1", adk.TerminalToolName, args, ""))

	assert.Len(t, f.tool.recorded(), 1)
	assert.Len(t, f.msg.recorded(), 1)
}

func TestADKEventSink_EmitAppliesOpsInOrder(t *testing.T) {
	sink, f := newADKSinkFixture()

	err := sink.emit(context.Background(), []adk.FlowEvent{
		{Op: adk.FlowEventToolCallStart, CallID: "c1", Name: adk.FileToolName,
			Args: json.RawMessage(`{"path":"/work/x","message":"read x"}`)},
		{Op: adk.FlowEventToolCallEnd, CallID: "c1", Result: "contents", OK: true, Duration: 0.2},
		{Op: adk.FlowEventMessage, Kind: adk.MessageKindThoughts, Headline: "done"},
	})
	require.NoError(t, err)

	msgs := f.msg.recorded()
	require.Len(t, msgs, 3)
	assert.Equal(t, database.MsglogTypeFile, msgs[0].typ)
	assert.Equal(t, "read x", msgs[0].msg)
	assert.Equal(t, database.MsglogResultFormatPlain, msgs[1].format, "file results stay plain")
	assert.Equal(t, database.MsglogTypeThoughts, msgs[2].typ)

	toolCalls := f.tool.recorded()
	require.Len(t, toolCalls, 2)
	assert.Equal(t, "success", toolCalls[1].method)
}
