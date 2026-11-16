package adk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/openai/openai-go/v3/option"
	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/activity"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/model/gemini"
	"google.golang.org/adk/v2/model/openaimodel"
	"google.golang.org/genai"
)

// ModelCredentials carries everything one model construction needs that
// production keeps off the workflow: which model, which endpoint, which key,
// which wire API. It is resolved and consumed inside the InvokeModel activity,
// so the key never enters workflow history.
type ModelCredentials struct {
	ModelName string
	BaseURL   string
	APIKey    string
	APIStyle  string
}

// APIStyle values buildModel serves: the two OpenAI-compatible styles
// openaimodel speaks, plus the native Gemini door.
const (
	APIStyleChatCompletions = string(openaimodel.APIChatCompletions)
	APIStyleResponses       = string(openaimodel.APIResponses)
	APIStyleGemini          = "gemini"
)

// ProviderResolver answers whose credentials an InvokeModel call uses: flowID
// is parsed from the attempt's workflow id, agentType names the agent whose
// turn it is. It runs inside the activity — never returning credentials to
// workflow code — so model names and answers stay the only things history
// sees.
type ProviderResolver func(ctx context.Context, flowID int64, agentType string) (ModelCredentials, error)

// Agent-type names a model handle can carry. The strings match
// temporal.AgentType* and pconfig's option names, so the controller resolver
// can take them verbatim.
const (
	AgentTypePentester = "pentester"
	AgentTypeAdviser   = "adviser"
)

// agentModelName gives a non-primary agent its own model handle by prefixing
// the agent type, e.g. "adviser/deepseek-v4.1-flash". The pentester keeps the
// bare model name, so the registration the CLI has always made still serves it.
func agentModelName(agentType, model string) string {
	if agentType == "" || agentType == AgentTypePentester {
		return model
	}
	return agentType + "/" + model
}

// splitAgentModelName is agentModelName's inverse: only a first segment that
// names a known agent type is consumed as one; anything else — including
// model names that already contain a slash — is the pentester's bare name.
func splitAgentModelName(name string) (agentType, modelName string) {
	if i := strings.IndexByte(name, '/'); i > 0 {
		switch name[:i] {
		case AgentTypeAdviser, AgentTypePentester:
			return name[:i], name[i+1:]
		}
	}
	return AgentTypePentester, name
}

// ModelFactory rebuilds the real model inside the InvokeModel activity, so the
// API key is captured in the worker-side closure and never serialized into the
// workflow. openai-go retries are disabled: Temporal's RetryPolicy is the
// single source of truth for retries.
//
// The plugin resolves the factory on every InvokeModel activity, so the
// transport is built once here and shared across turns — per-call clients
// would pay a fresh TLS handshake per LLM turn.
//
// A set resolver redirects the call to the flow's own provider: the attempt's
// workflow id yields the flow, the requested handle the agent type, and the
// resolved credentials replace the environment ones. Everything else — a run
// whose workflow id is not a flow child, or no resolver at all — keeps the
// ADK_* environment config, which is also what the CLI uses.
//
// OpenCode Go's terms require a client-specific user agent and a stable
// x-opencode-session per conversation; the hashed workflow ID gives every turn
// of a run the same session value, including across activity retries.
func ModelFactory(cfg *Config, resolver ProviderResolver) googleadk.ModelFactory {
	httpClient := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	return func(ctx context.Context, name string) (model.LLM, error) {
		agentType, modelName := splitAgentModelName(name)
		creds := ModelCredentials{
			ModelName: modelName,
			BaseURL:   cfg.BaseURL,
			APIKey:    cfg.APIKey,
			APIStyle:  cfg.ModelAPI,
		}
		if resolver != nil && activity.IsActivity(ctx) {
			if flowID, ok := FlowIDFromWorkflowID(activity.GetInfo(ctx).WorkflowExecution.ID); ok {
				resolved, err := resolver(ctx, flowID, agentType)
				if err != nil {
					return nil, err
				}
				creds = resolved
			}
		}
		return buildModel(ctx, creds, httpClient, sessionID(ctx))
	}
}

func buildModel(ctx context.Context, creds ModelCredentials, httpClient *http.Client, session string) (model.LLM, error) {
	style := creds.APIStyle
	if style == "" {
		style = APIStyleChatCompletions
	}
	switch style {
	case APIStyleGemini:
		return gemini.NewModel(ctx, creds.ModelName, &genai.ClientConfig{
			APIKey:      creds.APIKey,
			Backend:     genai.BackendGeminiAPI,
			HTTPClient:  httpClient,
			HTTPOptions: genai.HTTPOptions{BaseURL: creds.BaseURL},
		})
	case APIStyleChatCompletions, APIStyleResponses:
		return openaimodel.NewModel(ctx, creds.ModelName, &openaimodel.ClientConfig{
			APIKey:     creds.APIKey,
			BaseURL:    creds.BaseURL,
			API:        openaimodel.API(style),
			HTTPClient: httpClient,
			Options: []option.RequestOption{
				option.WithMaxRetries(0),
				option.WithHeader("User-Agent", "pentagi-adk/1.0"),
				option.WithHeader("x-opencode-session", session),
			},
		})
	default:
		return nil, fmt.Errorf("unsupported model api style %q", creds.APIStyle)
	}
}

// Inside InvokeModel the activity context carries the workflow ID; direct mode
// has none, so each in-process run gets one fresh session ID of its own. The
// ID is hashed so meaningful workflow IDs don't leak internal naming to the
// inference provider.
func sessionID(ctx context.Context) string {
	if activity.IsActivity(ctx) {
		if id := activity.GetInfo(ctx).WorkflowExecution.ID; id != "" {
			sum := sha256.Sum256([]byte(id))
			return "pentagi-" + hex.EncodeToString(sum[:])[:32]
		}
	}
	return "pentagi-" + uuid.NewString()
}
