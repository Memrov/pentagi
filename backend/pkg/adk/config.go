package adk

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v10"
	"google.golang.org/adk/v2/model/openaimodel"
)

const (
	// DefaultBaseURL is OpenCode Go's OpenAI-compatible base; /chat/completions
	// or /responses is appended by the client. Console translates every text
	// model across APIs, so the chat-completions path serves all of them.
	DefaultBaseURL   = "https://opencode.ai/inference/go/openai/v1"
	DefaultModelName = "deepseek-v4.1-flash"
)

// Config carries the spike's own environment; Temporal connection settings stay
// in pkg/config and are composed by the caller. The API key must only ever come
// from the environment or the secrets store — it is read worker-side and never
// crosses into a workflow.
type Config struct {
	APIKey    string `env:"ADK_API_KEY"`
	BaseURL   string `env:"ADK_BASE_URL" envDefault:"https://opencode.ai/inference/go/openai/v1"`
	ModelName string `env:"ADK_MODEL_NAME" envDefault:"deepseek-v4.1-flash"`
	ModelAPI  string `env:"ADK_MODEL_API" envDefault:"chat_completions"`
	TaskQueue string `env:"ADK_TASK_QUEUE" envDefault:"pentagi-adk"`

	// Sandbox selects the command backend: "local" runs on the host (development
	// only, unsandboxed), "docker" execs into the container named by
	// SandboxContainer.
	Sandbox          string `env:"ADK_SANDBOX" envDefault:"local"`
	SandboxContainer string `env:"ADK_SANDBOX_CONTAINER"`

	PayloadBucket    string `env:"ADK_PAYLOAD_BUCKET"`
	PayloadPrefix    string `env:"ADK_PAYLOAD_PREFIX" envDefault:"adk-payloads"`
	PayloadThreshold int    `env:"ADK_PAYLOAD_THRESHOLD" envDefault:"16384"`

	ToolTimeoutSeconds int `env:"ADK_TOOL_TIMEOUT" envDefault:"600"`
}

func LoadConfig() (*Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse adk config: %w", err)
	}
	return &cfg, cfg.Validate()
}

func (c *Config) Validate() error {
	if c.APIKey == "" {
		return fmt.Errorf("ADK_API_KEY is required")
	}
	switch c.Sandbox {
	case "local", "docker":
	default:
		return fmt.Errorf("ADK_SANDBOX must be local or docker, got %q", c.Sandbox)
	}
	if c.Sandbox == "docker" && c.SandboxContainer == "" {
		return fmt.Errorf("ADK_SANDBOX_CONTAINER is required when ADK_SANDBOX=docker")
	}
	if c.PayloadThreshold <= 0 {
		return fmt.Errorf("ADK_PAYLOAD_THRESHOLD must be positive, got %d", c.PayloadThreshold)
	}
	switch openaimodel.API(c.ModelAPI) {
	case openaimodel.APIChatCompletions, openaimodel.APIResponses:
	default:
		return fmt.Errorf("ADK_MODEL_API must be %s or %s, got %q",
			openaimodel.APIChatCompletions, openaimodel.APIResponses, c.ModelAPI)
	}
	return nil
}

func (c *Config) ToolTimeout() time.Duration {
	return time.Duration(c.ToolTimeoutSeconds) * time.Second
}
