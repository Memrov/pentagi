package adk

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/googleadk"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/tool"
)

func TestRunDirect_ToolCallThenAnswer(t *testing.T) {
	fm := googleadk.NewFakeModel(
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{
			"input": "echo recon", "cwd": "/work", "detach": false, "timeout": 60, "message": "recon",
		}),
		googleadk.TextResponse("recon says hi"),
	).WithName("fake-model")

	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "recon\n", ExitCode: 0}, nil
	}}

	res, err := runDirect(context.Background(), fm, "do recon", directOptions{
		sb: sb, threshold: 1024, toolTimeout: DefaultToolTimeout,
	})
	require.NoError(t, err)
	assert.Equal(t, "recon says hi", res.Answer)

	calls := sb.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"sh", "-c", sandboxCommandSh, "echo recon"}, calls[0])
}

// hack_result is the loop barrier in direct mode the same as in the workflow:
// the model is never asked to summarize after it fires.
func TestRunDirect_HackResultEndsLoop(t *testing.T) {
	fm := newRecordingModel("fake-model",
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{
			"input": "id", "cwd": "/work", "detach": false, "timeout": 60, "message": "whoami",
		}),
		googleadk.FunctionCallResponse("call-2", HackResultToolName, map[string]any{
			"success": true, "result": "domain admin obtained", "message": "done"}),
		googleadk.TextResponse("must never be asked"),
	)

	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "uid=0"}, nil
	}}

	res, err := runDirect(context.Background(), fm, "pentest dc.corp", directOptions{
		sb: sb, threshold: 1024, toolTimeout: DefaultToolTimeout,
	})
	require.NoError(t, err)
	require.NotNil(t, res.HackResult)
	assert.Equal(t, &HackResult{Success: true, Result: "domain admin obtained"}, res.HackResult)
	assert.NotEqual(t, "must never be asked", res.Answer)
	assert.Len(t, fm.requests(), 2, "no model call may follow hack_result")
}

func TestRunDirect_InstructionAndQuestionOverride(t *testing.T) {
	fm := newRecordingModel("fake-model",
		googleadk.TextResponse("ok"),
		googleadk.FunctionCallResponse("call-2", HackResultToolName, map[string]any{
			"success": true, "result": "ok", "message": "done"}),
	)

	_, err := runDirect(context.Background(), fm, "task text", directOptions{
		sb: &fakeSandbox{}, threshold: 1024, toolTimeout: DefaultToolTimeout,
		instruction: "OVERRIDE INSTRUCTION", question: "override question",
	})
	require.NoError(t, err)

	reqs := fm.requests()
	require.Len(t, reqs, 2, "the answer pass, then the nudged hack_result pass")
	require.NotNil(t, reqs[0].Config)
	require.NotNil(t, reqs[0].Config.SystemInstruction)
	assert.True(t, strings.HasPrefix(reqs[0].Config.SystemInstruction.Parts[0].Text, "OVERRIDE INSTRUCTION"),
		"the provider text leads; ADK appends the agent-identity line behind it")
	require.NotEmpty(t, reqs[0].Contents)
	assert.Equal(t, "override question", reqs[0].Contents[0].Parts[0].Text)
}

// The scope-guard seam, exercised through the public run path: a guard that
// answers instead of the tool keeps the sandbox untouched while the model
// still gets a tool response.
func TestRunDirect_ScopeGuardShortCircuitsTool(t *testing.T) {
	fm := googleadk.NewFakeModel(
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{
			"input": "nmap target", "cwd": "/work", "detach": false, "timeout": 60, "message": "scan",
		}),
		googleadk.TextResponse("call was blocked"),
	).WithName("fake-model")

	sb := &fakeSandbox{}
	guard := ScopeGuard(func(ctx agent.Context, tl tool.Tool, args map[string]any) (map[string]any, error) {
		if tl.Name() == TerminalToolName {
			return map[string]any{"error": "out of scope"}, nil
		}
		return nil, nil
	})

	res, err := runDirect(context.Background(), fm, "scan", directOptions{
		sb: sb, threshold: 1024, toolTimeout: DefaultToolTimeout, guard: guard,
	})
	require.NoError(t, err)
	assert.Equal(t, "call was blocked", res.Answer)
	assert.Empty(t, sb.calls(), "the guard must run before the sandbox")
}

// A model that never calls hack_result gets nudged like the workflow does,
// and after the budget is spent the result carries the same synthetic
// failure the workflow writes.
func TestRunDirect_MissingBarrierYieldsSyntheticFailure(t *testing.T) {
	fm := newRecordingModel("fake-model", googleadk.TextResponse("no closing call"))

	res, err := runDirect(context.Background(), fm, "scan", directOptions{
		sb: &fakeSandbox{}, threshold: 1024, toolTimeout: DefaultToolTimeout,
	})
	require.NoError(t, err)
	require.NotNil(t, res.HackResult)
	assert.False(t, res.HackResult.Success)
	assert.Contains(t, res.HackResult.Result, "after 3 turns")
	assert.Len(t, fm.requests(), maxBarrierRuns)
}
