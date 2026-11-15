package adk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/googleadk"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/tool"
)

func TestRunDirect_ToolCallThenAnswer(t *testing.T) {
	fm := googleadk.NewFakeModel(
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{"command": "echo recon"}),
		googleadk.TextResponse("recon says hi"),
	).WithName("fake-model")

	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "recon\n", ExitCode: 0}, nil
	}}

	res, err := runDirect(context.Background(), fm, sb, nil, "do recon", 1024, nil)
	require.NoError(t, err)
	assert.Equal(t, "recon says hi", res.Answer)

	calls := sb.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"sh", "-c", "echo recon"}, calls[0])
}

// The scope-guard seam, exercised through the public run path: a guard that
// answers instead of the tool keeps the sandbox untouched while the model
// still gets a tool response.
func TestRunDirect_ScopeGuardShortCircuitsTool(t *testing.T) {
	fm := googleadk.NewFakeModel(
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{"command": "nmap target"}),
		googleadk.TextResponse("call was blocked"),
	).WithName("fake-model")

	sb := &fakeSandbox{}
	guard := ScopeGuard(func(ctx agent.Context, tl tool.Tool, args map[string]any) (map[string]any, error) {
		if tl.Name() == TerminalToolName {
			return map[string]any{"error": "out of scope"}, nil
		}
		return nil, nil
	})

	res, err := runDirect(context.Background(), fm, sb, nil, "scan", 1024, guard)
	require.NoError(t, err)
	assert.Equal(t, "call was blocked", res.Answer)
	assert.Empty(t, sb.calls(), "the guard must run before the sandbox")
}
