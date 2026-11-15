package adk

import (
	"context"

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
// OpenCode Go's terms require a client-specific user agent and a stable
// x-opencode-session per conversation; the workflow ID gives every turn of a
// run the same session value, including across activity retries.
func ModelFactory(cfg *Config) googleadk.ModelFactory {
	return func(ctx context.Context, name string) (model.LLM, error) {
		return openaimodel.NewModel(ctx, name, &openaimodel.ClientConfig{
			APIKey:  cfg.APIKey,
			BaseURL: cfg.BaseURL,
			API:     openaimodel.API(cfg.ModelAPI),
			Options: []option.RequestOption{
				option.WithMaxRetries(0),
				option.WithHeader("User-Agent", "pentagi-adk/1.0"),
				option.WithHeader("x-opencode-session", sessionID(ctx)),
			},
		})
	}
}

// Inside InvokeModel the activity context carries the workflow ID; direct mode
// has none, so each in-process run gets one fresh session ID of its own.
func sessionID(ctx context.Context) string {
	if activity.IsActivity(ctx) {
		if id := activity.GetInfo(ctx).WorkflowExecution.ID; id != "" {
			return "pentagi-" + id
		}
	}
	return "pentagi-" + uuid.NewString()
}
