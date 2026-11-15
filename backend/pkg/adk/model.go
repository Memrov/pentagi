package adk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"

	"github.com/google/uuid"
	"github.com/openai/openai-go/v3/option"
	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/activity"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/model/openaimodel"
)

// ModelFactory rebuilds the real model inside the InvokeModel activity, so the
// API key is captured in the worker-side closure and never serialized into the
// workflow. openai-go retries are disabled: Temporal's RetryPolicy is the
// single source of truth for retries.
//
// The plugin resolves the factory on every InvokeModel activity, so the
// transport is built once here and shared across turns — per-call clients
// would pay a fresh TLS handshake per LLM turn.
//
// OpenCode Go's terms require a client-specific user agent and a stable
// x-opencode-session per conversation; the hashed workflow ID gives every turn
// of a run the same session value, including across activity retries.
func ModelFactory(cfg *Config) googleadk.ModelFactory {
	httpClient := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	return func(ctx context.Context, name string) (model.LLM, error) {
		return openaimodel.NewModel(ctx, name, &openaimodel.ClientConfig{
			APIKey:     cfg.APIKey,
			BaseURL:    cfg.BaseURL,
			API:        openaimodel.API(cfg.ModelAPI),
			HTTPClient: httpClient,
			Options: []option.RequestOption{
				option.WithMaxRetries(0),
				option.WithHeader("User-Agent", "pentagi-adk/1.0"),
				option.WithHeader("x-opencode-session", sessionID(ctx)),
			},
		})
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
