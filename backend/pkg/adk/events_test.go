package adk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/session"
	"google.golang.org/genai"
)

func translateParts(t *testing.T, e *eventEmitter, timestamp time.Time, parts ...*genai.Part) []FlowEvent {
	t.Helper()
	return e.translate(&session.Event{
		LLMResponse: model.LLMResponse{Content: &genai.Content{Role: genai.RoleModel, Parts: parts}},
		Timestamp:   timestamp,
	})
}

func newTestEmitter() *eventEmitter {
	return &eventEmitter{calls: map[string]time.Time{}}
}

func TestTranslateEvent_FunctionCallOpensCallAndKeepsArgs(t *testing.T) {
	e := newTestEmitter()
	ops := translateParts(t, e, time.Now(), &genai.Part{
		FunctionCall: &genai.FunctionCall{
			ID:   "call-1",
			Name: TerminalToolName,
			Args: map[string]any{"input": "nmap -sV t", "message": "scan"},
		},
	})

	require.Len(t, ops, 1)
	op := ops[0]
	assert.Equal(t, FlowEventToolCallStart, op.Op)
	assert.Equal(t, "call-1", op.CallID)
	assert.Equal(t, TerminalToolName, op.Name)
	var args map[string]any
	require.NoError(t, json.Unmarshal(op.Args, &args))
	assert.Equal(t, "nmap -sV t", args["input"])
}

func TestTranslateEvent_FunctionResponseClosesWithDuration(t *testing.T) {
	e := newTestEmitter()
	start := time.Now()
	translateParts(t, e, start, &genai.Part{
		FunctionCall: &genai.FunctionCall{ID: "call-1", Name: TerminalToolName, Args: map[string]any{"input": "id"}},
	})

	ops := translateParts(t, e, start.Add(1500*time.Millisecond), &genai.Part{
		FunctionResponse: &genai.FunctionResponse{
			ID:       "call-1",
			Name:     TerminalToolName,
			Response: map[string]any{"output": "uid=0\n[exit code: 0]", "exit_code": 0},
		},
	})

	require.Len(t, ops, 1)
	op := ops[0]
	assert.Equal(t, FlowEventToolCallEnd, op.Op)
	assert.Equal(t, "call-1", op.CallID)
	assert.Equal(t, "uid=0\n[exit code: 0]", op.Result)
	assert.True(t, op.OK)
	assert.InDelta(t, 1.5, op.Duration, 0.001)
}

func TestTranslateEvent_TextLandsAsThoughts(t *testing.T) {
	e := newTestEmitter()
	ops := translateParts(t, e, time.Now(), &genai.Part{Text: "Found SSH exposed"})

	require.Len(t, ops, 1)
	assert.Equal(t, FlowEventMessage, ops[0].Op)
	assert.Equal(t, MessageKindThoughts, ops[0].Kind)
	assert.Equal(t, "Found SSH exposed", ops[0].Headline)
	assert.Empty(t, ops[0].Result)
}

func TestTranslateEvent_ThoughtPartFoldsIntoSiblingOps(t *testing.T) {
	e := newTestEmitter()
	ops := translateParts(t, e, time.Now(),
		&genai.Part{Text: "checking ssh first", Thought: true},
		&genai.Part{FunctionCall: &genai.FunctionCall{ID: "c1", Name: TerminalToolName, Args: map[string]any{}}},
	)

	require.Len(t, ops, 1)
	assert.Equal(t, "checking ssh first", ops[0].Thinking)
}

func TestTranslateEvent_HackResultCallAddsDoneMessage(t *testing.T) {
	e := newTestEmitter()
	ops := translateParts(t, e, time.Now(), &genai.Part{
		FunctionCall: &genai.FunctionCall{
			ID:   "call-9",
			Name: HackResultToolName,
			Args: map[string]any{"success": true, "result": "domain admin obtained", "message": "pentest complete"},
		},
	})

	require.Len(t, ops, 2)
	assert.Equal(t, FlowEventToolCallStart, ops[0].Op)
	assert.Equal(t, HackResultToolName, ops[0].Name)
	assert.Equal(t, FlowEventMessage, ops[1].Op)
	assert.Equal(t, MessageKindDone, ops[1].Kind)
	assert.Equal(t, "pentest complete", ops[1].Headline)
	assert.Equal(t, "domain admin obtained", ops[1].Result)
}

func TestTranslateEvent_UnknownAndEmptyShapesDropQuietly(t *testing.T) {
	e := newTestEmitter()
	assert.Empty(t, e.translate(nil))
	assert.Empty(t, e.translate(&session.Event{}))
	assert.Empty(t, translateParts(t, e, time.Now()))
	assert.Empty(t, translateParts(t, e, time.Now(), &genai.Part{}))
}

func TestTranslateEvent_EmptyCallIDFallsBackDeterministically(t *testing.T) {
	e := newTestEmitter()
	ops := translateParts(t, e, time.Now(), &genai.Part{
		FunctionCall: &genai.FunctionCall{Name: FileToolName, Args: map[string]any{"path": "/work/x"}},
	})
	require.Len(t, ops, 1)
	assert.Equal(t, FileToolName+"#1", ops[0].CallID)

	// A response without an id pairs on the name, which is what ADK's own
	// matching assumes once it has populated the call id.
	end := translateParts(t, e, time.Now(), &genai.Part{
		FunctionResponse: &genai.FunctionResponse{Name: FileToolName, Response: map[string]any{"output": "x"}},
	})
	require.Len(t, end, 1)
	assert.Equal(t, FileToolName, end[0].CallID)
}

func TestFoldToolResult_TerminalOutputKeepsItsFooter(t *testing.T) {
	result, ok := foldToolResult(map[string]any{"output": "uid=0\n[exit code: 0]", "exit_code": float64(0)})
	assert.True(t, ok)
	assert.Equal(t, "uid=0\n[exit code: 0]", result)
}

func TestFoldToolResult_NonzeroExitWithoutFooterGetsMarker(t *testing.T) {
	result, ok := foldToolResult(map[string]any{"output": "boom", "exit_code": float64(2)})
	assert.True(t, ok)
	assert.Equal(t, "boom\n[exit code: 2]", result)
}

func TestFoldToolResult_PayloadURIAndTimeoutFoldIn(t *testing.T) {
	result, ok := foldToolResult(map[string]any{
		"output":      "head of output",
		"payload_uri": "s3://bucket/key.log",
		"timed_out":   true,
	})
	assert.True(t, ok)
	assert.Equal(t, "head of output\n[payload: s3://bucket/key.log]\n[timed out]", result)
}

func TestFoldToolResult_ErrorPayloadFailsTheCall(t *testing.T) {
	result, ok := foldToolResult(map[string]any{"error": "activity timeout"})
	assert.False(t, ok)
	assert.Equal(t, "activity timeout", result)
}

func TestFoldToolResult_AckTextAndEmptyResponse(t *testing.T) {
	result, ok := foldToolResult(map[string]any{"result": "hack result successfully processed"})
	assert.True(t, ok)
	assert.Equal(t, "hack result successfully processed", result)

	result, ok = foldToolResult(nil)
	assert.True(t, ok)
	assert.Empty(t, result)
}
