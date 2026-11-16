package adk

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/googleadk"
	"google.golang.org/adk/v2/tool"
)

// namedTool is the smallest tool.Tool the guard callbacks need — they only
// read the name for the repeat key.
type namedTool struct{ name string }

func (t namedTool) Name() string        { return t.name }
func (t namedTool) Description() string { return t.name }
func (t namedTool) IsLongRunning() bool { return false }

// call simulates one model-issued tool call through the callback pair the way
// ADK's callTool does: before decides, and whatever the call finally produced
// feeds after.
func call(t *testing.T, d *repeatDetector, tl tool.Tool, args map[string]any, toolResult map[string]any) map[string]any {
	t.Helper()
	res, err := d.before(nil, tl, args)
	require.NoError(t, err)
	if res == nil {
		res = toolResult
	}
	_, err = d.after(nil, tl, args, res, nil)
	require.NoError(t, err)
	return res
}

// Production semantics: calls 1-2 execute, the 3rd..6th replay the cached
// result with a notice, and the 7th identical call aborts the chain.
func TestRepeatDetector_SoftReplayThenAbort(t *testing.T) {
	d := &repeatDetector{}
	tl := namedTool{TerminalToolName}
	args := map[string]any{"input": "id", "cwd": "/work"}
	real := map[string]any{"output": "uid=0"}

	for i := 1; i <= 2; i++ {
		res := call(t, d, tl, args, real)
		assert.Equal(t, real, res, "call %d must execute", i)
	}
	for i := 3; i <= 6; i++ {
		res := call(t, d, tl, args, map[string]any{"output": "DIFFERENT — must never run"})
		assert.Equal(t, "uid=0", res["output"], "call %d replays the cached result", i)
		assert.Contains(t, res["repeat_notice"], fmt.Sprintf("%d times", i))
	}

	res, err := d.before(nil, tl, args)
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repeated 7 times")

	// The abort surfaces on the next model turn, not the tool response.
	g := &runGuards{repeat: d}
	resp, err := g.beforeModel(nil, nil)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, d.abortErr)
}

// Any change to the tool or to the normalized args restarts the count, the
// way production's detector does.
func TestRepeatDetector_ResetsOnNewCall(t *testing.T) {
	d := &repeatDetector{}
	tl := namedTool{TerminalToolName}
	other := namedTool{FileToolName}
	real := map[string]any{"output": "ok"}

	for i := 0; i < 3; i++ {
		call(t, d, tl, map[string]any{"input": "id"}, real)
	}
	// A different tool name is a different call.
	res := call(t, d, other, map[string]any{"input": "id"}, real)
	assert.Equal(t, real, res)
	// Changed args are a different call even on the same tool.
	res = call(t, d, tl, map[string]any{"input": "whoami"}, real)
	assert.Equal(t, real, res)
	// And the fresh sequence needs its own three to start replaying.
	res = call(t, d, tl, map[string]any{"input": "whoami"}, real)
	assert.Equal(t, real, res)
	res = call(t, d, tl, map[string]any{"input": "whoami"}, map[string]any{"output": "unused"})
	assert.Equal(t, "ok", res["output"], "the third identical call replays")
}

// The volatile "message" commentary arg is stripped before comparison — a
// call that only rephrases its log line still counts as the same call.
func TestRepeatDetector_MessageArgIgnored(t *testing.T) {
	d := &repeatDetector{}
	tl := namedTool{TerminalToolName}
	real := map[string]any{"output": "uid=0"}

	call(t, d, tl, map[string]any{"input": "id", "message": "whoami"}, real)
	call(t, d, tl, map[string]any{"input": "id", "message": "checking the user"}, real)
	res := call(t, d, tl, map[string]any{"input": "id", "message": "third time"}, map[string]any{"output": "unused"})
	assert.Equal(t, "uid=0", res["output"], "only the message changed — still the same call")
	assert.Contains(t, res["repeat_notice"], "3 times")
}

func TestRunGuards_IterationCapBoundary(t *testing.T) {
	g := newRunGuards(3)
	for i := 1; i <= 3; i++ {
		resp, err := g.beforeModel(nil, nil)
		require.NoError(t, err)
		assert.Nil(t, resp, "model call %d must proceed", i)
	}
	resp, err := g.beforeModel(nil, nil)
	require.NoError(t, err)
	require.NotNil(t, resp, "call past the cap is answered, not made")
	require.NotNil(t, resp.Content)
	assert.Contains(t, resp.Content.Parts[0].Text, "Iteration limit (3)")
}

func TestRunGuards_DefaultCap(t *testing.T) {
	assert.Equal(t, defaultMaxIterations, newRunGuards(0).maxIterations)
	assert.Equal(t, 20, newRunGuards(20).maxIterations)
}

// End to end in direct mode: a model stuck reissuing one terminal call runs
// it twice, gets replays, then the run dies on the abort error.
func TestRunDirect_RepeatingCallAborts(t *testing.T) {
	fm := newRecordingModel("fake-model",
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{
			"input": "id", "cwd": "/work", "detach": false, "timeout": 60, "message": "whoami",
		}),
	)
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "uid=0"}, nil
	}}

	_, err := runDirect(context.Background(), fm, "loop", directOptions{
		sb: sb, threshold: 1024, toolTimeout: DefaultToolTimeout,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repeated 7 times")
	assert.Len(t, sb.calls(), 2, "only the first two identical calls reached the sandbox")
	assert.Len(t, fm.requests(), repeatCallAbortAt, "the abort lands before another model call")
}

// The iteration cap ends each pass with a guard answer; the workflow-level
// nudge then reruns until the barrier budget is spent. Distinct scripted
// calls keep the repeat detector out of the way.
func TestRunDirect_IterationCapBoundsEachPass(t *testing.T) {
	fm := newRecordingModel("fake-model",
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{"input": "cmd-1", "cwd": "/work", "detach": false, "timeout": 60, "message": "one"}),
		googleadk.FunctionCallResponse("call-2", TerminalToolName, map[string]any{"input": "cmd-2", "cwd": "/work", "detach": false, "timeout": 60, "message": "two"}),
		googleadk.FunctionCallResponse("call-3", TerminalToolName, map[string]any{"input": "cmd-3", "cwd": "/work", "detach": false, "timeout": 60, "message": "three"}),
	)
	sb := &fakeSandbox{}

	res, err := runDirect(context.Background(), fm, "loop", directOptions{
		sb: sb, threshold: 1024, toolTimeout: DefaultToolTimeout, maxIterations: 2,
	})
	require.NoError(t, err)
	require.NotNil(t, res.HackResult)
	assert.False(t, res.HackResult.Success)
	assert.Contains(t, res.HackResult.Result, "after 3 turns")
	assert.Contains(t, res.Answer, "Iteration limit (2)")
	// Three passes, each bounded at two model calls before the guard answers.
	assert.Len(t, fm.requests(), maxBarrierRuns*2)
	// The last scripted call replays on exhaustion, so the second call of each
	// pass is identical to the first of the next — but each pass gets a fresh
	// detector, so every call still executes.
	assert.Len(t, sb.calls(), maxBarrierRuns*2)
}
