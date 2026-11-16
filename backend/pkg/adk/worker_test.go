package adk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ADK_API_KEY gates the env-only path: a CLI worker needs it, while a
// production worker with a resolver wired gets keys per flow and must boot
// without one.
func TestWorkerPlugin_APIKeyGate(t *testing.T) {
	cfg := &Config{ModelName: "m", BaseURL: "https://llm.example/v1", ModelAPI: "chat_completions"}

	_, err := WorkerPlugin(cfg, nil)
	assert.ErrorContains(t, err, "ADK_API_KEY is required")

	resolver := func(context.Context, int64, string) (ModelCredentials, error) {
		return ModelCredentials{}, nil
	}
	plugin, err := WorkerPlugin(cfg, resolver)
	require.NoError(t, err)
	require.NotNil(t, plugin)
}
