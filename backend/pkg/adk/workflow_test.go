package adk

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/adk/v2/model"
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

// newPentestEnv wires the workflow under test with a scripted FakeModel and a
// fakeSandbox behind the real Activities, mirroring how production registration
// goes through googleadk.NewActivities and Activities.Register.
func newPentestEnv(
	t *testing.T,
	modelName string,
	responses []*model.LLMResponse,
	sandbox Sandbox,
	store PayloadStore,
) (*testsuite.TestWorkflowEnvironment, *activityCounter) {
	t.Helper()

	fm := googleadk.NewFakeModel(responses...).WithName(modelName)
	gacts, err := googleadk.NewActivities(googleadk.Config{
		Models: map[string]googleadk.ModelFactory{
			modelName: func(context.Context, string) (model.LLM, error) { return fm, nil },
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

func TestPentesterWorkflow_ToolCallThenAnswer(t *testing.T) {
	const modelName = "fake-model"
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "22/tcp open  ssh", ExitCode: 0}, nil
	}}
	env, counter := newPentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.FunctionCallResponse("call-1", TerminalToolName, map[string]any{"command": "nmap -sV scanme.sh"}),
		googleadk.TextResponse("Found SSH exposed on scanme.sh; recommend key-only auth."),
	}, sb, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "scan scanme.sh", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	assert.Contains(t, res.Answer, "SSH")

	calls := sb.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"sh", "-c", "nmap -sV scanme.sh"}, calls[0])

	assert.Equal(t, 2, counter.get(googleadk.InvokeModelActivityName), "one model call per turn: the tool call and the final answer")
	assert.Equal(t, 1, counter.get(TerminalToolName))
}

func TestPentesterWorkflow_MultiToolSequence(t *testing.T) {
	const modelName = "fake-model"
	var ran [][]string
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		ran = append(ran, argv)
		return ExecResult{Output: `{"host":"scanme.sh","status":200}`, ExitCode: 0}, nil
	}}
	env, counter := newPentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.FunctionCallResponse("call-1", HttpxToolName, map[string]any{"target": "https://scanme.sh", "tech_detect": true}),
		googleadk.FunctionCallResponse("call-2", NucleiToolName, map[string]any{"target": "https://scanme.sh", "severity": []any{"high"}}),
		googleadk.TextResponse("probe and scan complete"),
	}, sb, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "probe and scan", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.Len(t, ran, 2)
	assert.Equal(t, "httpx", ran[0][0])
	assert.Equal(t, "nuclei", ran[1][0])
	assert.Equal(t, 1, counter.get(HttpxToolName))
	assert.Equal(t, 1, counter.get(NucleiToolName))
	assert.Equal(t, 3, counter.get(googleadk.InvokeModelActivityName))
}

// A validation failure is non-retryable: the activity runs exactly once, the
// error is handed to the model per ADK's tool-error contract, and the model
// answers on the next turn.
func TestPentesterWorkflow_ToolValidationErrorFeedsBackOnce(t *testing.T) {
	const modelName = "fake-model"
	env, counter := newPentestEnv(t, modelName, []*model.LLMResponse{
		googleadk.FunctionCallResponse("call-1", NucleiToolName, map[string]any{"target": "scanme.sh"}),
		googleadk.TextResponse("the target needs an http(s) scheme"),
	}, &fakeSandbox{}, nil)

	env.ExecuteWorkflow(PentesterWorkflow, PentestRequest{Task: "scan scanme.sh", Model: modelName})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res PentestResult
	require.NoError(t, env.GetWorkflowResult(&res))
	assert.Contains(t, res.Answer, "http(s) scheme")
	assert.Equal(t, 1, counter.get(NucleiToolName), "non-retryable means a single attempt")
	assert.Equal(t, 2, counter.get(googleadk.InvokeModelActivityName))
}
