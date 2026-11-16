package adk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/googleadk"
	"google.golang.org/genai"
)

// The advice tool must face the model with the same contract as production's
// AskAdvice: name, the fields the chain hands the mentor, and which of them
// are required.
func TestNewAdviceTool_DeclarationMatchesProductionArgs(t *testing.T) {
	tl, err := newAdviceTool(nil, "mentor instruction")
	require.NoError(t, err)
	assert.Equal(t, AdviceToolName, tl.Name())
	assert.Equal(t, adviceToolDesc, tl.Description())

	declarer, ok := tl.(interface {
		Declaration() *genai.FunctionDeclaration
	})
	require.True(t, ok, "agenttool exposes a function declaration")
	decl := declarer.Declaration()
	require.NotNil(t, decl)
	require.NotNil(t, decl.Parameters)
	assert.Equal(t, []string{"question", "message"}, decl.Parameters.Required)
	for _, field := range []string{"question", "code", "output", "message"} {
		assert.Contains(t, decl.Parameters.Properties, field)
	}
}

// Delegation end to end in-process: the parent's advice call runs the adviser
// agent on the same model, the mentor's text comes back as the tool result,
// and the parent keeps working until hack_result — advice is not a barrier.
func TestRunDirect_AdviceDelegatesAndParentContinues(t *testing.T) {
	fm := newRecordingModel("fake-model",
		googleadk.FunctionCallResponse("call-1", AdviceToolName, map[string]any{
			"question": "which services matter", "message": "asking the mentor"}),
		googleadk.TextResponse("Check SMB signing first, then look at Kerberos."),
		googleadk.FunctionCallResponse("call-2", HackResultToolName, map[string]any{
			"success": true, "result": "smb signing disabled", "message": "done"}),
	)

	res, err := runDirect(context.Background(), fm, "pentest dc.corp", directOptions{
		sb: &fakeSandbox{}, threshold: 1024, toolTimeout: DefaultToolTimeout,
		includeAdvice: true, adviceInstruction: "MENTOR OVERRIDE",
	})
	require.NoError(t, err)
	require.NotNil(t, res.HackResult)
	assert.Equal(t, &HackResult{Success: true, Result: "smb signing disabled"}, res.HackResult)

	reqs := fm.requests()
	require.Len(t, reqs, 3, "parent call, adviser call, parent's closing call")

	// The parent saw advice in its tool list.
	var adviceDecl bool
	for _, gt := range reqs[0].Config.Tools {
		for _, decl := range gt.FunctionDeclarations {
			if decl.Name == AdviceToolName {
				adviceDecl = true
			}
		}
	}
	assert.True(t, adviceDecl, "the pentester request must declare the advice tool")

	// The second call is the adviser's: it got the mentor instruction, and its
	// user message is the tool args serialized as JSON.
	require.NotNil(t, reqs[1].Config.SystemInstruction)
	assert.Contains(t, reqs[1].Config.SystemInstruction.Parts[0].Text, "MENTOR OVERRIDE")
	require.NotEmpty(t, reqs[1].Contents)
	assert.Contains(t, reqs[1].Contents[0].Parts[0].Text, "which services matter")

	// And the parent's closing call saw the mentor's answer as the advice
	// tool's function response.
	var adviceResult string
	for _, c := range reqs[2].Contents {
		for _, p := range c.Parts {
			if p != nil && p.FunctionResponse != nil && p.FunctionResponse.Name == AdviceToolName {
				adviceResult, _ = p.FunctionResponse.Response["result"].(string)
			}
		}
	}
	assert.Contains(t, adviceResult, "SMB signing",
		"the adviser's text must reach the parent as the tool result")
}
