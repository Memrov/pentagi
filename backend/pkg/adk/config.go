package adk

import (
	"fmt"
	"net/url"
	"time"

	"github.com/caarlos0/env/v10"
	"google.golang.org/adk/v2/model/openaimodel"
)

// DefaultModelName is the fallback when a request names no model; the
// canonical BaseURL default lives in the envDefault tag on Config.BaseURL.
const DefaultModelName = "deepseek-v4.1-flash"

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
	// only, unsandboxed), "docker" execs into a container — SandboxContainer is
	// the fallback for runs whose workflow id does not resolve to a flow.
	Sandbox          string `env:"ADK_SANDBOX" envDefault:"local"`
	SandboxContainer string `env:"ADK_SANDBOX_CONTAINER"`

	PayloadBucket    string `env:"ADK_PAYLOAD_BUCKET"`
	PayloadPrefix    string `env:"ADK_PAYLOAD_PREFIX" envDefault:"adk-payloads"`
	PayloadThreshold int    `env:"ADK_PAYLOAD_THRESHOLD" envDefault:"16384"`

	// FlowBound marks the worker's terminal commands as owned by its flow — the
	// production stop sweep kills only commands carrying the mark, so an
	// unmarked command would survive a flow stop.
	FlowBound bool `env:"ADK_FLOW_BOUND"`

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
	// APIKey is deliberately not required here — `run` never builds a model;
	// WorkerPlugin and RunDirect check it at the point the model is built.
	switch c.Sandbox {
	case "local", "docker":
	default:
		return fmt.Errorf("ADK_SANDBOX must be local or docker, got %q", c.Sandbox)
	}
	if c.PayloadThreshold <= 0 {
		return fmt.Errorf("ADK_PAYLOAD_THRESHOLD must be positive, got %d", c.PayloadThreshold)
	}
	if c.ToolTimeoutSeconds <= 0 {
		return fmt.Errorf("ADK_TOOL_TIMEOUT must be positive, got %d", c.ToolTimeoutSeconds)
	}
	if c.ModelName == "" {
		return fmt.Errorf("ADK_MODEL_NAME is required")
	}
	switch openaimodel.API(c.ModelAPI) {
	case openaimodel.APIChatCompletions, openaimodel.APIResponses:
	default:
		return fmt.Errorf("ADK_MODEL_API must be %s or %s, got %q",
			openaimodel.APIChatCompletions, openaimodel.APIResponses, c.ModelAPI)
	}
	if u, err := url.Parse(c.BaseURL); err != nil || !isSecureOrLocal(u) {
		return fmt.Errorf("ADK_BASE_URL must be an https URL (http allowed only for localhost), got %q", c.BaseURL)
	}
	return nil
}

// The API key is sent as a bearer token to BaseURL, so anything but https —
// or a non-parseable value — would ship it in cleartext.
func isSecureOrLocal(u *url.URL) bool {
	if u.Scheme == "https" {
		return true
	}
	if u.Scheme != "http" {
		return false
	}
	switch u.Hostname() {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	return false
}

func (c *Config) ToolTimeout() time.Duration {
	return time.Duration(c.ToolTimeoutSeconds) * time.Second
}
