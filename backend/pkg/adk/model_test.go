package adk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/adk/v2/model"
	"google.golang.org/genai"
)

// chatCompletionsFixture serves one canned chat completion and records how it
// was called: path, headers and the model the client asked for.
type chatCompletionsFixture struct {
	srv         *httptest.Server
	path        string
	auth        string
	userAgent   string
	session     string
	model       string
	lastRequest map[string]any
}

func newChatCompletionsFixture(t *testing.T, response map[string]any) *chatCompletionsFixture {
	f := &chatCompletionsFixture{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.path = r.URL.Path
		f.auth = r.Header.Get("Authorization")
		f.userAgent = r.Header.Get("User-Agent")
		f.session = r.Header.Get("x-opencode-session")
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&f.lastRequest))
		f.model, _ = f.lastRequest["model"].(string)
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(response))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func TestModelFactory_ChatCompletions_RoundTrip(t *testing.T) {
	fixture := newChatCompletionsFixture(t, map[string]any{
		"id":     "chatcmpl-1",
		"object": "chat.completion",
		"choices": []any{
			map[string]any{
				"index":         0,
				"message":       map[string]any{"role": "assistant", "content": "port 22 is open"},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]any{"prompt_tokens": 9, "completion_tokens": 5, "total_tokens": 14},
	})

	cfg := &Config{APIKey: "zen-key-123", BaseURL: fixture.srv.URL, ModelAPI: "chat_completions"}
	llm, err := ModelFactory(cfg, nil)(context.Background(), "deepseek-v4.1-flash")
	require.NoError(t, err)
	assert.Equal(t, "deepseek-v4.1-flash", llm.Name())

	req := &model.LLMRequest{
		Model:    "deepseek-v4.1-flash",
		Contents: []*genai.Content{genai.NewContentFromText("check port 22", genai.RoleUser)},
	}
	var got *model.LLMResponse
	for resp, err := range llm.GenerateContent(context.Background(), req, false) {
		require.NoError(t, err)
		got = resp
	}
	require.NotNil(t, got)
	require.NotNil(t, got.Content)
	require.NotEmpty(t, got.Content.Parts)
	assert.Equal(t, "port 22 is open", got.Content.Parts[0].Text)

	assert.Equal(t, "/chat/completions", fixture.path)
	assert.Equal(t, "Bearer zen-key-123", fixture.auth)
	assert.Equal(t, "deepseek-v4.1-flash", fixture.model)
	assert.Equal(t, "pentagi-adk/1.0", fixture.userAgent)
	assert.True(t, strings.HasPrefix(fixture.session, "pentagi-"), "session %q", fixture.session)
}

func TestSplitAgentModelName_DecodesAgentHandles(t *testing.T) {
	for _, tc := range []struct {
		name, agent, model string
	}{
		{"deepseek-v4.1-flash", AgentTypePentester, "deepseek-v4.1-flash"},
		{"adviser/deepseek-v4.1-flash", AgentTypeAdviser, "deepseek-v4.1-flash"},
		{"pentester/gpt-4o", AgentTypePentester, "gpt-4o"},
		// A slash inside a model name is not an agent handle.
		{"openai/gpt-4o", AgentTypePentester, "openai/gpt-4o"},
		{"adviser", AgentTypePentester, "adviser"},
	} {
		agent, model := splitAgentModelName(tc.name)
		assert.Equal(t, tc.agent, agent, tc.name)
		assert.Equal(t, tc.model, model, tc.name)
	}
}

// runFactoryInWorkflow executes one factory call as an activity inside a
// workflow carrying the given id, the same way InvokeModel does; the built
// model's name becomes the workflow result.
func runFactoryInWorkflow(
	workflowID, handle string,
	factory googleadk.ModelFactory,
) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	if workflowID != "" {
		env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: workflowID})
	}
	build := func(ctx context.Context, name string) (string, error) {
		llm, err := factory(ctx, name)
		if err != nil {
			return "", err
		}
		return llm.Name(), nil
	}
	env.RegisterActivity(build)
	env.ExecuteWorkflow(func(ctx workflow.Context) (string, error) {
		var name string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx,
			workflow.ActivityOptions{StartToCloseTimeout: time.Minute}),
			build, handle).Get(ctx, &name)
		return name, err
	})
	return env
}

func TestModelFactory_ResolverSuppliesFlowCredentials(t *testing.T) {
	var gotFlow int64
	var gotAgent string
	resolver := func(_ context.Context, flowID int64, agentType string) (ModelCredentials, error) {
		gotFlow, gotAgent = flowID, agentType
		return ModelCredentials{
			ModelName: "user-picked-model",
			BaseURL:   "https://llm.example/v1",
			APIKey:    "per-user-key",
			APIStyle:  APIStyleChatCompletions,
		}, nil
	}
	cfg := &Config{APIKey: "env-key", BaseURL: "https://env.example/v1", ModelAPI: "chat_completions"}

	env := runFactoryInWorkflow("t-pentagi-flow-42-input-0", "env-model", ModelFactory(cfg, resolver))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var name string
	require.NoError(t, env.GetWorkflowResult(&name))
	assert.Equal(t, "user-picked-model", name,
		"the built model must be the resolved one, not the registered handle")
	assert.Equal(t, int64(42), gotFlow)
	assert.Equal(t, AgentTypePentester, gotAgent)
}

func TestModelFactory_AgentHandleReachesResolver(t *testing.T) {
	var gotAgent string
	resolver := func(_ context.Context, _ int64, agentType string) (ModelCredentials, error) {
		gotAgent = agentType
		return ModelCredentials{
			ModelName: "adviser-model", BaseURL: "https://llm.example/v1",
			APIKey: "k", APIStyle: APIStyleChatCompletions,
		}, nil
	}
	cfg := &Config{APIKey: "env-key", BaseURL: "https://env.example/v1", ModelAPI: "chat_completions"}

	env := runFactoryInWorkflow("pentagi-flow-7-input-0", "adviser/env-model", ModelFactory(cfg, resolver))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, AgentTypeAdviser, gotAgent)
	var name string
	require.NoError(t, env.GetWorkflowResult(&name))
	assert.Equal(t, "adviser-model", name)
}

func TestModelFactory_ResolverErrorFailsTheAttempt(t *testing.T) {
	resolver := func(context.Context, int64, string) (ModelCredentials, error) {
		return ModelCredentials{}, errors.New("provider row missing")
	}
	cfg := &Config{APIKey: "env-key", BaseURL: "https://env.example/v1", ModelAPI: "chat_completions"}

	env := runFactoryInWorkflow("pentagi-flow-7-input-0", "env-model", ModelFactory(cfg, resolver))

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

// The environment config is the fallback for anything that is not a flow
// child — a CLI run, a hand-typed id — and when no resolver is wired at all.
func TestModelFactory_NonFlowWorkflowKeepsEnvCredentials(t *testing.T) {
	resolver := func(context.Context, int64, string) (ModelCredentials, error) {
		return ModelCredentials{}, errors.New("must not be consulted for an unparseable workflow id")
	}
	cfg := &Config{APIKey: "env-key", BaseURL: "https://env.example/v1", ModelAPI: "chat_completions"}

	env := runFactoryInWorkflow("", "env-model", ModelFactory(cfg, resolver))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var name string
	require.NoError(t, env.GetWorkflowResult(&name))
	assert.Equal(t, "env-model", name, "no flow means the requested handle names the model")
}

func TestBuildModel_GeminiStyleBuildsGeminiModel(t *testing.T) {
	llm, err := buildModel(context.Background(), ModelCredentials{
		ModelName: "gemini-3.1-pro-preview",
		APIKey:    "gem-key",
		APIStyle:  APIStyleGemini,
	}, http.DefaultClient, "session")
	require.NoError(t, err)
	assert.Equal(t, "gemini-3.1-pro-preview", llm.Name())
}

func TestBuildModel_UnknownStyleIsRefused(t *testing.T) {
	_, err := buildModel(context.Background(), ModelCredentials{
		ModelName: "m", APIStyle: "grpc",
	}, http.DefaultClient, "session")
	assert.ErrorContains(t, err, "unsupported model api style")
}
