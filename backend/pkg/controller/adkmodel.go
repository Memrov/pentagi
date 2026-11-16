package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"pentagi/pkg/adk"
	"pentagi/pkg/config"
	"pentagi/pkg/database"
	"pentagi/pkg/providers"
	"pentagi/pkg/providers/pconfig"
	"pentagi/pkg/providers/provider"

	sdktemporal "go.temporal.io/sdk/temporal"
)

// NewADKModelResolver answers an InvokeModel activity with the credentials of
// the flow's own provider: the flow row picks the provider, the provider
// config picks the agent's model (the same ModelWithPrefix the production
// call path puts on the wire), and the server config supplies that provider
// type's endpoint and key — the same pair its langchaingo client is built
// from. Everything runs inside the activity attempt, so the key never
// crosses into workflow history.
func NewADKModelResolver(
	provs providers.ProviderController,
	db database.Querier,
	cfg *config.Config,
) adk.ProviderResolver {
	return func(ctx context.Context, flowID int64, agentType string) (adk.ModelCredentials, error) {
		opt := pconfig.ProviderOptionsType(agentType)
		if !slices.Contains(pconfig.AllAgentTypes, opt) {
			return adk.ModelCredentials{}, nonRetryableModel(
				fmt.Sprintf("unknown agent type %q", agentType))
		}

		flow, err := db.GetFlow(ctx, flowID)
		if err != nil {
			return adk.ModelCredentials{}, fmt.Errorf("failed to get flow %d: %w", flowID, err)
		}
		prv, err := provs.GetProvider(ctx, provider.ProviderName(flow.ModelProviderName), flow.UserID)
		if err != nil {
			return adk.ModelCredentials{}, fmt.Errorf("failed to get provider %q for flow %d: %w",
				flow.ModelProviderName, flowID, err)
		}

		creds, err := adkModelCredentials(prv.Type(), cfg, prv.ModelWithPrefix(opt))
		if err != nil {
			return adk.ModelCredentials{}, nonRetryableModel(err.Error())
		}
		return creds, nil
	}
}

// adkModelCredentials maps a provider type to the endpoint and key the server
// config holds for it. Providers without an OpenAI-compatible or native
// Gemini door are refused by name — silently serving them through the wrong
// API would surface as a confusing mid-run failure.
func adkModelCredentials(
	ptype provider.ProviderType,
	cfg *config.Config,
	modelName string,
) (adk.ModelCredentials, error) {
	creds := adk.ModelCredentials{ModelName: modelName, APIStyle: adk.APIStyleChatCompletions}
	keyOptional := false

	switch ptype {
	case provider.ProviderOpenAI:
		creds.BaseURL, creds.APIKey = cfg.OpenAIServerURL, cfg.OpenAIKey
	case provider.ProviderCustom:
		creds.BaseURL, creds.APIKey = cfg.LLMServerURL, cfg.LLMServerKey
	case provider.ProviderDeepSeek:
		creds.BaseURL, creds.APIKey = cfg.DeepSeekServerURL, cfg.DeepSeekAPIKey
	case provider.ProviderGLM:
		creds.BaseURL, creds.APIKey = cfg.GLMServerURL, cfg.GLMAPIKey
	case provider.ProviderKimi:
		creds.BaseURL, creds.APIKey = cfg.KimiServerURL, cfg.KimiAPIKey
	case provider.ProviderQwen:
		creds.BaseURL, creds.APIKey = cfg.QwenServerURL, cfg.QwenAPIKey
	case provider.ProviderMiniMax:
		creds.BaseURL, creds.APIKey = cfg.MiniMaxServerURL, cfg.MiniMaxAPIKey
	case provider.ProviderMistral:
		creds.BaseURL, creds.APIKey = cfg.MistralServerURL, cfg.MistralAPIKey
	case provider.ProviderXAI:
		creds.BaseURL, creds.APIKey = cfg.XAIServerURL, cfg.XAIAPIKey
	case provider.ProviderOllama:
		// Ollama's OpenAI-compatible door sits under /v1 beside the native API.
		creds.BaseURL = strings.TrimRight(cfg.OllamaServerURL, "/") + "/v1"
		creds.APIKey = cfg.OllamaServerAPIKey
		keyOptional = true
	case provider.ProviderGemini:
		creds.APIStyle = adk.APIStyleGemini
		creds.BaseURL, creds.APIKey = cfg.GeminiServerURL, cfg.GeminiAPIKey
	default:
		return adk.ModelCredentials{}, fmt.Errorf(
			"provider type %q is not supported by the adk engine", ptype)
	}

	if creds.APIKey == "" && !keyOptional {
		return adk.ModelCredentials{}, fmt.Errorf(
			"no api key configured for provider type %q", ptype)
	}
	return creds, nil
}

// nonRetryableModel marks the InvokeModel activity's configuration failures —
// an unknown agent type, an unsupported provider, a missing key — so a
// misconfigured flow fails fast instead of burning the model call's retry
// budget on an attempt that can never succeed.
func nonRetryableModel(msg string) error {
	return sdktemporal.NewNonRetryableApplicationError(msg, "ModelConfig", nil)
}
