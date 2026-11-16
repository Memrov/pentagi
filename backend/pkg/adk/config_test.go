package adk

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unsetenv(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			t.Cleanup(func() { _ = os.Setenv(k, v) })
		}
		_ = os.Unsetenv(k)
	}
}

func TestConfig_LoadConfig_Defaults(t *testing.T) {
	unsetenv(t,
		"ADK_BASE_URL", "ADK_MODEL_NAME", "ADK_MODEL_API", "ADK_TASK_QUEUE",
		"ADK_SANDBOX", "ADK_SANDBOX_CONTAINER", "ADK_PAYLOAD_BUCKET",
		"ADK_PAYLOAD_PREFIX", "ADK_PAYLOAD_THRESHOLD", "ADK_TOOL_TIMEOUT",
		"ADK_FLOW_BOUND")
	t.Setenv("ADK_API_KEY", "test-key")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "https://opencode.ai/inference/go/openai/v1", cfg.BaseURL)
	assert.Equal(t, "deepseek-v4.1-flash", cfg.ModelName)
	assert.Equal(t, "chat_completions", cfg.ModelAPI)
	assert.Equal(t, "local", cfg.Sandbox)
	assert.Equal(t, 16384, cfg.PayloadThreshold)
	assert.Equal(t, "pentagi-adk", cfg.TaskQueue)
	assert.False(t, cfg.FlowBound)
}

func TestConfig_LoadConfig_FlowBoundEnv(t *testing.T) {
	unsetenv(t, "ADK_FLOW_BOUND")
	t.Setenv("ADK_API_KEY", "test-key")
	t.Setenv("ADK_FLOW_BOUND", "true")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.True(t, cfg.FlowBound)
}

func TestConfig_Validate_RejectsInvalidFields(t *testing.T) {
	valid := func() *Config {
		return &Config{
			Sandbox: "local", PayloadThreshold: 1, ToolTimeoutSeconds: 60,
			ModelName: "m", ModelAPI: "chat_completions", BaseURL: "https://x.test",
		}
	}
	cases := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"bad sandbox", func(c *Config) { c.Sandbox = "podman" }, "ADK_SANDBOX"},
		{"no payload threshold", func(c *Config) { c.PayloadThreshold = 0 }, "ADK_PAYLOAD_THRESHOLD"},
		{"no tool timeout", func(c *Config) { c.ToolTimeoutSeconds = 0 }, "ADK_TOOL_TIMEOUT"},
		{"no model name", func(c *Config) { c.ModelName = "" }, "ADK_MODEL_NAME"},
		{"bad model api", func(c *Config) { c.ModelAPI = "chat" }, "ADK_MODEL_API"},
		{"plaintext base url", func(c *Config) { c.BaseURL = "http://api.example.com" }, "ADK_BASE_URL"},
		{"unparseable base url", func(c *Config) { c.BaseURL = "not a url" }, "ADK_BASE_URL"},
		// The key is checked where the model is built, not here.
		{"api key not required", func(c *Config) { c.APIKey = "" }, ""},
		{"localhost http allowed", func(c *Config) { c.BaseURL = "http://localhost:8080" }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid()
			tc.mut(cfg)
			err := cfg.Validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestConfig_Validate_DockerAllowsEmptyContainer(t *testing.T) {
	cfg := &Config{
		Sandbox: "docker", PayloadThreshold: 1, ToolTimeoutSeconds: 60,
		ModelName: "m", ModelAPI: "chat_completions", BaseURL: "https://x.test",
	}
	assert.NoError(t, cfg.Validate())
}
