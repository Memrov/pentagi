package adk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/session"
	"google.golang.org/genai"
)

func callEvent(name string, args map[string]any) *session.Event {
	return &session.Event{
		LLMResponse: modelResponseWithCall(name, args),
	}
}

func modelResponseWithCall(name string, args map[string]any) model.LLMResponse {
	return model.LLMResponse{
		Content: &genai.Content{
			Role:  genai.RoleModel,
			Parts: []*genai.Part{{FunctionCall: &genai.FunctionCall{Name: name, Args: args}}},
		},
	}
}

func TestExtractHackResult_ReadsCallArgs(t *testing.T) {
	cases := []struct {
		name   string
		events []*session.Event
		want   *HackResult
	}{
		{"success true", []*session.Event{
			callEvent(HackResultToolName, map[string]any{"success": true, "result": "pwned"}),
		}, &HackResult{Success: true, Result: "pwned"}},
		{"success false", []*session.Event{
			callEvent(HackResultToolName, map[string]any{"success": false, "result": "failed"}),
		}, &HackResult{Success: false, Result: "failed"}},
		{"success omitted defaults false", []*session.Event{
			callEvent(HackResultToolName, map[string]any{"result": "partial"}),
		}, &HackResult{Result: "partial"}},
		{"other tool ignored", []*session.Event{
			callEvent(TerminalToolName, map[string]any{"input": "id"}),
		}, nil},
		{"nil events skipped", []*session.Event{nil, {}}, nil},
		{"empty stream", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, extractHackResult(tc.events))
		})
	}
}

func TestNewHackResultTool_DeclaresBarrierSchema(t *testing.T) {
	tl, err := newHackResultTool()
	require.NoError(t, err)
	require.Equal(t, HackResultToolName, tl.Name())
	assert.Equal(t, hackResultToolDesc, tl.Description())

	props := schemaProps(t, tl)
	assert.Contains(t, props, "result")
	assert.Contains(t, props, "success")
	required, _ := schemaJSON(t, tl)["required"].([]any)
	assert.Contains(t, required, "result")
	assert.NotContains(t, required, "success",
		"production's hack_result has no success field; the ADK extension stays optional")
}
