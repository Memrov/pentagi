package adk

import (
	"context"

	"github.com/openai/openai-go/v3/option"
	"go.temporal.io/googleadk"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/model/openaimodel"
)

// ModelFactory rebuilds the real model inside the InvokeModel activity, so the
// API key is captured in the worker-side closure and never serialized into the
// workflow. openai-go retries are disabled: Temporal's RetryPolicy is the
// single source of truth for retries.
func ModelFactory(cfg *Config) googleadk.ModelFactory {
	return func(ctx context.Context, name string) (model.LLM, error) {
		return openaimodel.NewModel(ctx, name, &openaimodel.ClientConfig{
			APIKey:  cfg.APIKey,
			BaseURL: cfg.BaseURL,
			API:     openaimodel.API(cfg.ModelAPI),
			Options: []option.RequestOption{option.WithMaxRetries(0)},
		})
	}
}
