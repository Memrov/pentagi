package adk

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/runner"
	"google.golang.org/adk/v2/session"
	"google.golang.org/genai"
)

// activityCounter records how many times each activity type ran.
type activityCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newActivityCounter() *activityCounter { return &activityCounter{counts: map[string]int{}} }

func (c *activityCounter) record(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[name]++
}

func (c *activityCounter) get(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[name]
}

// newPentestEnv wires the workflow under test with a scripted model and a
// fakeSandbox behind the real Activities, mirroring how production registration
// goes through googleadk.NewActivities and Activities.Register.
func newPentestEnv(
	t *testing.T,
	modelName string,
	mdl model.LLM,
	sandbox Sandbox,
	store PayloadStore,
) (*testsuite.TestWorkflowEnvironment, *activityCounter) {
	t.Helper()

	gacts, err := googleadk.NewActivities(googleadk.Config{
		Models: map[string]googleadk.ModelFactory{
			modelName: func(context.Context, string) (model.LLM, error) { return mdl, nil },
			agentModelName(AgentTypeAdviser, modelName): func(context.Context, string) (model.LLM, error) {
				return mdl, nil
			},
		},
	})
	require.NoError(t, err)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(PentesterWorkflow, workflow.RegisterOptions{Name: PentesterWorkflowName})
	gacts.Register(testRegistry{env})

	acts := NewActivities(sandbox, store, 1024, 0)
	acts.Register(env)

	counter := newActivityCounter()
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		counter.record(info.ActivityType.Name)
	})
	return env, counter
}

func newFakePentestEnv(
	t *testing.T,
	modelName string,
	responses []*model.LLMResponse,
	sandbox Sandbox,
	store PayloadStore,
) (*testsuite.TestWorkflowEnvironment, *activityCounter) {
	t.Helper()
	return newPentestEnv(t, modelName,
		googleadk.NewFakeModel(responses...).WithName(modelName), sandbox, store)
}

func TestPentesterWorkflow_ToolCallThenAnswer(t *testing.T) {
	const modelName = "fake-model"
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "22/tcp open  ssh", ExitCode: 0}, nil
	}}
	env, counter := newFakePentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{
			"input": "nmap -sV scanme.sh", "cwd": "/work", "detach": false, "timeout": 60, "message": "scan",
		}),
		googleadk.TextResponse("Found SSH exposed on scanme.sh; recommend key-only auth."),
		// A bare text answer ends the pass without the barrier, so the
		// workflow nudges once and the model calls hack_result.
		googleadk.FunctionCallResponse("call-2", HackResultToolName, map[string]any{
			"success": true, "result": "ssh open", "message": "done"}),
	}, sb, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "scan scanme.sh", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	assert.Contains(t, res.Answer, "SSH")

	calls := sb.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"sh", "-c", sandboxCommandSh, "nmap -sV scanme.sh"}, calls[0])

	assert.Equal(t, 3, counter.get(googleadk.InvokeModelActivityName), "tool call, answer, then the nudged hack_result")
	assert.Equal(t, 1, counter.get(TerminalToolName))
}

func TestPentesterWorkflow_MultiToolSequence(t *testing.T) {
	const modelName = "fake-model"
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: `{"host":"scanme.sh","status":200}`, ExitCode: 0}, nil
	}}
	env, counter := newFakePentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.FunctionCallResponse("call-1", HttpxToolName, map[string]any{"target": "https://scanme.sh", "tech_detect": true}),
		googleadk.FunctionCallResponse("call-2", NucleiToolName, map[string]any{"target": "https://scanme.sh", "severity": []any{"high"}}),
		googleadk.TextResponse("probe and scan complete"),
		googleadk.FunctionCallResponse("call-3", HackResultToolName, map[string]any{
			"success": true, "result": "scan complete", "message": "done"}),
	}, sb, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "probe and scan", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	calls := sb.calls()
	require.Len(t, calls, 2)
	assert.Equal(t, "httpx", calls[0][0])
	assert.Equal(t, "nuclei", calls[1][0])
	assert.Equal(t, 1, counter.get(HttpxToolName))
	assert.Equal(t, 1, counter.get(NucleiToolName))
	assert.Equal(t, 4, counter.get(googleadk.InvokeModelActivityName))
}

// hack_result is the loop barrier: its SkipSummarization flag ends the agent
// turn without another model call, and the call's args land on the result.
func TestPentesterWorkflow_HackResultEndsLoop(t *testing.T) {
	const modelName = "fake-model"
	fm := newRecordingModel(modelName,
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
	env, _ := newPentestEnv(t, modelName, fm, sb, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "pentest dc.corp", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.NotNil(t, res.HackResult)
	assert.Equal(t, &HackResult{Success: true, Result: "domain admin obtained"}, res.HackResult)
	assert.NotEqual(t, "must never be asked", res.Answer)
	assert.Len(t, fm.requests(), 2, "no model call may follow hack_result")
}

func TestPentesterWorkflow_InstructionAndQuestionOverride(t *testing.T) {
	const modelName = "fake-model"
	fm := newRecordingModel(modelName,
		googleadk.TextResponse("ok"),
		googleadk.FunctionCallResponse("call-2", HackResultToolName, map[string]any{
			"success": true, "result": "ok", "message": "done"}),
	)
	env, _ := newPentestEnv(t, modelName, fm, &fakeSandbox{}, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{
		Task: "task text", Model: modelName,
		Instruction: "OVERRIDE INSTRUCTION", Question: "override question",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	reqs := fm.requests()
	require.Len(t, reqs, 2, "the answer pass, then the nudged hack_result pass")
	require.NotNil(t, reqs[0].Config)
	require.NotNil(t, reqs[0].Config.SystemInstruction)
	assert.True(t, strings.HasPrefix(reqs[0].Config.SystemInstruction.Parts[0].Text, "OVERRIDE INSTRUCTION"),
		"the provider text leads; ADK appends the agent-identity line behind it")
	require.NotEmpty(t, reqs[0].Contents)
	assert.Equal(t, "override question", reqs[0].Contents[0].Parts[0].Text)
}

// A validation failure is non-retryable: the activity runs exactly once, the
// error is handed to the model per ADK's tool-error contract, and the model
// answers on the next turn.
func TestPentesterWorkflow_ToolValidationErrorFeedsBackOnce(t *testing.T) {
	const modelName = "fake-model"
	env, counter := newFakePentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.FunctionCallResponse("call-1", NucleiToolName, map[string]any{"target": "scanme.sh"}),
		googleadk.TextResponse("the target needs an http(s) scheme"),
		googleadk.FunctionCallResponse("call-2", HackResultToolName, map[string]any{
			"success": false, "result": "could not scan", "message": "done"}),
	}, &fakeSandbox{}, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "scan scanme.sh", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	assert.Contains(t, res.Answer, "http(s) scheme")
	assert.Equal(t, 1, counter.get(NucleiToolName), "non-retryable means a single attempt")
	assert.Equal(t, 3, counter.get(googleadk.InvokeModelActivityName))
}

// emitCapture stands in for EmitFlowEventActivity: the ops the workflow
// translated land here exactly once per session event.
type emitCapture struct {
	mu     sync.Mutex
	inputs []EmitFlowEventInput
	err    error
}

func (c *emitCapture) emit(_ context.Context, in EmitFlowEventInput) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inputs = append(c.inputs, in)
	return c.err
}

func (c *emitCapture) all() []EmitFlowEventInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]EmitFlowEventInput(nil), c.inputs...)
}

func TestPentesterWorkflow_EmitEventsTranslatesEverySessionEvent(t *testing.T) {
	const modelName = "fake-model"
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "uid=0", ExitCode: 0}, nil
	}}
	env, _ := newFakePentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{
			"input": "id", "cwd": "/work", "detach": false, "timeout": 60, "message": "whoami",
		}),
		googleadk.FunctionCallResponse("call-2", HackResultToolName, map[string]any{
			"success": true, "result": "root flag", "message": "pwned"}),
	}, sb, nil)

	capture := &emitCapture{}
	env.RegisterActivityWithOptions(capture.emit, activity.RegisterOptions{Name: EmitFlowEventActivity})

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{
		Task: "get root", Model: modelName,
		FlowID: 7, EmitEvents: true, EmitTaskQueue: "flows-queue",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	inputs := capture.all()
	var ops []FlowEvent
	for _, in := range inputs {
		assert.Equal(t, int64(7), in.FlowID)
		ops = append(ops, in.Events...)
	}

	var starts, ends, dones int
	for _, op := range ops {
		switch {
		case op.Op == FlowEventToolCallStart:
			starts++
		case op.Op == FlowEventToolCallEnd:
			ends++
		case op.Op == FlowEventMessage && op.Kind == MessageKindDone:
			dones++
		}
	}
	assert.Equal(t, 2, starts, "terminal and hack_result calls")
	assert.Equal(t, 2, ends, "each call's response closes it")
	assert.Equal(t, 1, dones, "hack_result emits the closing message")
	assert.Equal(t, 4, len(inputs), "one emit activity per session event")
}

func TestPentesterWorkflow_EmitFailureDoesNotFailTheRun(t *testing.T) {
	const modelName = "fake-model"
	env, _ := newFakePentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.TextResponse("scan finished"),
	}, &fakeSandbox{}, nil)

	capture := &emitCapture{err: errors.New("flows worker gone")}
	env.RegisterActivityWithOptions(capture.emit, activity.RegisterOptions{Name: EmitFlowEventActivity})

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{
		Task: "scan", Model: modelName,
		FlowID: 7, EmitEvents: true, EmitTaskQueue: "flows-queue",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	assert.Equal(t, "scan finished", res.Answer)
	assert.NotEmpty(t, capture.all(), "the emit was still attempted")
}

func TestPentesterWorkflow_EmitEventsOffByDefault(t *testing.T) {
	const modelName = "fake-model"
	env, _ := newFakePentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.TextResponse("quiet run"),
		googleadk.FunctionCallResponse("call-1", HackResultToolName, map[string]any{
			"success": true, "result": "quiet", "message": "done"}),
	}, &fakeSandbox{}, nil)

	// No EmitFlowEvent registration at all: a CLI-style run must not reach for
	// the flows queue — an unexpected lookup fails the workflow if it fires.
	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "scan", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

// A run whose model never calls hack_result is not a success: the workflow
// nudges it back like production's reflector, and after the budget is spent
// the workflow itself reports a failed HackResult.
func TestPentesterWorkflow_MissingBarrierNudgesThenFails(t *testing.T) {
	const modelName = "fake-model"
	env, counter := newFakePentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.TextResponse("I checked the ports but refuse to close out."),
	}, &fakeSandbox{}, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "scan", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.NotNil(t, res.HackResult, "a text-only end must not silently succeed")
	assert.False(t, res.HackResult.Success)
	assert.Contains(t, res.HackResult.Result, "after 3 turns")
	assert.Equal(t, maxBarrierRuns, counter.get(googleadk.InvokeModelActivityName),
		"one model call per barrier-less pass")
}

// The retry count is request state, not run state: a continued-as-new run
// resumes the budget instead of restarting it, so retries seeded at 2 leave
// room for exactly one more barrier-less pass.
func TestPentesterWorkflow_BarrierRetriesSeedFromRequest(t *testing.T) {
	const modelName = "fake-model"
	env, counter := newFakePentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.TextResponse("still no barrier"),
	}, &fakeSandbox{}, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{
		Task: "scan", Model: modelName, BarrierRetries: maxBarrierRuns - 1,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.NotNil(t, res.HackResult)
	assert.False(t, res.HackResult.Success)
	assert.Equal(t, 1, counter.get(googleadk.InvokeModelActivityName))
}

// The repeat detector runs inside the agent: a model reissuing one call gets
// the cached answer, and the seventh identical call aborts the workflow with
// the non-retryable guard error.
func TestPentesterWorkflow_RepeatingCallAbortsChain(t *testing.T) {
	const modelName = "fake-model"
	fm := newRecordingModel(modelName,
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{
			"input": "id", "cwd": "/work", "detach": false, "timeout": 60, "message": "whoami",
		}),
	)
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "uid=0"}, nil
	}}
	env, _ := newPentestEnv(t, modelName, fm, sb, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "loop", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repeated 7 times")
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "the abort must be an application error, got %T", err)
	assert.Equal(t, repeatAbortErrType, appErr.Type())
	assert.Len(t, sb.calls(), 2, "calls three on got replays; the sandbox saw only calls one and two")
	assert.Len(t, fm.requests(), repeatCallAbortAt)
}

// advice is a delegated agent, not an activity: the parent's call runs the
// adviser on the same model activity, its answer returns as the tool result,
// and the parent closes through hack_result. The emit bridge records the
// delegation as an ordinary toolcall row pair.
func TestPentesterWorkflow_AdviceDelegatesThroughWorkflow(t *testing.T) {
	const modelName = "fake-model"
	fm := newRecordingModel(modelName,
		googleadk.FunctionCallResponse("call-1", AdviceToolName, map[string]any{
			"question": "which services matter", "message": "asking the mentor"}),
		googleadk.TextResponse("Check SMB signing first, then look at Kerberos."),
		googleadk.FunctionCallResponse("call-2", HackResultToolName, map[string]any{
			"success": true, "result": "smb signing disabled", "message": "done"}),
	)
	env, counter := newPentestEnv(t, modelName, fm, &fakeSandbox{}, nil)

	capture := &emitCapture{}
	env.RegisterActivityWithOptions(capture.emit, activity.RegisterOptions{Name: EmitFlowEventActivity})

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{
		Task: "pentest dc.corp", Model: modelName,
		IncludeAdvice: true, AdviceInstruction: "MENTOR OVERRIDE",
		FlowID: 9, EmitEvents: true, EmitTaskQueue: "flows-queue",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	assert.Equal(t, &HackResult{Success: true, Result: "smb signing disabled"}, res.HackResult)

	reqs := fm.requests()
	require.Len(t, reqs, 3, "parent call, nested adviser call, parent's closing call")
	assert.Equal(t, 3, counter.get(googleadk.InvokeModelActivityName),
		"the adviser's model call goes through the same InvokeModel activity")

	require.NotNil(t, reqs[1].Config.SystemInstruction)
	assert.Contains(t, reqs[1].Config.SystemInstruction.Parts[0].Text, "MENTOR OVERRIDE")
	require.NotEmpty(t, reqs[1].Contents)
	assert.Contains(t, reqs[1].Contents[0].Parts[0].Text, "which services matter")

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

	var sawAdviceStart, sawAdviceEnd bool
	for _, in := range capture.all() {
		for _, op := range in.Events {
			if op.Op == FlowEventToolCallStart && op.Name == AdviceToolName {
				sawAdviceStart = true
			}
			if op.Op == FlowEventToolCallEnd && op.CallID == "call-1" {
				sawAdviceEnd = true
			}
		}
	}
	assert.True(t, sawAdviceStart, "the delegation opens a toolcall row like any tool")
	assert.True(t, sawAdviceEnd, "the delegation's row closes with the adviser's answer")
}

// Temporal's continue-as-new suggestion exports the ADK session into the next
// run's request: the imported history lets the model pick up at the tool
// response the first run ended on, and the run finishes there. The test env
// cannot chain runs itself, so each continue-as-new request is handed to a
// fresh environment — the same handoff the server performs.
func TestPentesterWorkflow_ContinueAsNewPreservesSession(t *testing.T) {
	const modelName = "fake-model"
	fm := newRecordingModel(modelName,
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{
			"input": "id", "cwd": "/work", "detach": false, "timeout": 60, "message": "whoami",
		}),
		googleadk.FunctionCallResponse("call-2", HackResultToolName, map[string]any{
			"success": true, "result": "uid=0 obtained", "message": "done"}),
	)
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "uid=0"}, nil
	}}

	req := PentestRequest{Task: "pentest", Model: modelName}
	var res PentestResult
	for run := 0; run < 4; run++ {
		env, _ := newPentestEnv(t, modelName, fm, sb, nil)
		env.SetContinueAsNewSuggested(true)
		env.ExecuteWorkflow(PentesterWorkflow, req)
		require.True(t, env.IsWorkflowCompleted())

		err := env.GetWorkflowError()
		if err == nil {
			require.NoError(t, env.GetWorkflowResult(&res))
			break
		}
		var canErr *workflow.ContinueAsNewError
		require.True(t, errors.As(err, &canErr), "run %d ended for a reason other than continue-as-new: %v", run, err)
		assert.Equal(t, PentesterWorkflowName, canErr.WorkflowType.Name)
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(canErr.Input, &req),
			"the carried request must decode")
		require.NotNil(t, req.Session, "the next run must carry the exported session")
	}
	require.NotNil(t, res.HackResult, "the run chain must close on hack_result, not exhaust CAN attempts")
	assert.Equal(t, &HackResult{Success: true, Result: "uid=0 obtained"}, res.HackResult)

	assert.Len(t, sb.calls(), 1, "the terminal call ran once, in the first run")
	reqs := fm.requests()
	require.Len(t, reqs, 2, "one model call per workflow run")

	// The second run's request carries the imported transcript — the task,
	// the terminal call and its response — proving the session crossed.
	var sawTerminalCall bool
	for _, c := range reqs[1].Contents {
		for _, p := range c.Parts {
			if p != nil && p.FunctionCall != nil && p.FunctionCall.Name == TerminalToolName {
				sawTerminalCall = true
			}
		}
	}
	assert.True(t, sawTerminalCall, "the resumed run's model request must include the imported history")
}

// MaxIterations bounds each pass: the guard answers the third model call in
// the model's place, the pass ends, and the barrier nudge reruns until the
// budget is spent — three passes of two calls each.
func TestPentesterWorkflow_IterationCapBoundsEachPass(t *testing.T) {
	const modelName = "fake-model"
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "ok"}, nil
	}}
	env, counter := newFakePentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{"input": "cmd-1", "cwd": "/work", "detach": false, "timeout": 60, "message": "one"}),
		googleadk.FunctionCallResponse("call-2", TerminalToolName, map[string]any{"input": "cmd-2", "cwd": "/work", "detach": false, "timeout": 60, "message": "two"}),
		googleadk.FunctionCallResponse("call-3", TerminalToolName, map[string]any{"input": "cmd-3", "cwd": "/work", "detach": false, "timeout": 60, "message": "three"}),
	}, sb, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "loop", Model: modelName, MaxIterations: 2})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.NotNil(t, res.HackResult)
	assert.False(t, res.HackResult.Success)
	assert.Contains(t, res.Answer, "Iteration limit (2)")
	assert.Equal(t, maxBarrierRuns*2, counter.get(googleadk.InvokeModelActivityName))
	assert.Len(t, sb.calls(), maxBarrierRuns*2)
}

// The workflow's export/import pair is exercised directly: events appended by
// one run reappear under the same session in a fresh service, which is what
// the next continue-as-new run resumes against.
func TestSessionSnapshotExportImportPreservesConversation(t *testing.T) {
	ctx := context.Background()
	svc := session.InMemoryService()

	fm := newRecordingModel("fake-model",
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{
			"input": "id", "cwd": "/work", "detach": false, "timeout": 60, "message": "whoami",
		}),
		googleadk.TextResponse("done"),
	)
	tools, err := DirectTools(&fakeSandbox{}, nil, 1024, DefaultToolTimeout, false)
	require.NoError(t, err)
	pentester, err := NewPentesterAgent(fm, tools, "", nil, newRunGuards(0))
	require.NoError(t, err)
	r, err := runner.New(runner.Config{
		AppName: appName, Agent: pentester, SessionService: svc, AutoCreateSession: true,
	})
	require.NoError(t, err)

	msg := genai.NewContentFromText("who am i", genai.RoleUser)
	for _, err := range r.Run(ctx, "pentagi", "sess-1", msg, agent.RunConfig{MaxLLMCalls: -1}) {
		require.NoError(t, err)
	}

	snap, err := googleadk.ExportSession(ctx, svc, appName, "pentagi", "sess-1")
	require.NoError(t, err)
	require.Equal(t, "sess-1", snap.SessionID)
	require.NotEmpty(t, snap.Events)

	svc2 := session.InMemoryService()
	imported, err := googleadk.ImportSession(ctx, svc2, snap)
	require.NoError(t, err)
	assert.Equal(t, "sess-1", imported.ID())
	assert.Equal(t, len(snap.Events), imported.Events().Len(),
		"every recorded event re-appends on import")
}
