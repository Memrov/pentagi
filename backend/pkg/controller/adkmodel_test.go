package controller

import (
	"context"
	"errors"
	"testing"

	"pentagi/pkg/adk"
	"pentagi/pkg/config"
	"pentagi/pkg/database"
	"pentagi/pkg/providers"
	"pentagi/pkg/providers/pconfig"
	"pentagi/pkg/providers/provider"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktemporal "go.temporal.io/sdk/temporal"
)

type modelResolverProvs struct {
	providers.ProviderController

	prv  provider.Provider
	err  error
	name provider.ProviderName
	user int64
}

func (p *modelResolverProvs) GetProvider(
	_ context.Context, prvname provider.ProviderName, userID int64,
) (provider.Provider, error) {
	p.name, p.user = prvname, userID
	return p.prv, p.err
}

type modelResolverProvider struct {
	provider.Provider

	ptype provider.ProviderType
	model string
	opt   pconfig.ProviderOptionsType
}

func (p *modelResolverProvider) Type() provider.ProviderType { return p.ptype }
func (p *modelResolverProvider) ModelWithPrefix(opt pconfig.ProviderOptionsType) string {
	p.opt = opt
	return p.model
}

func adkResolverConfig() *config.Config {
	return &config.Config{
		OpenAIKey: "openai-key", OpenAIServerURL: "https://api.openai.com/v1",
		DeepSeekAPIKey: "deepseek-key", DeepSeekServerURL: "https://api.deepseek.com",
		GeminiAPIKey: "gemini-key", GeminiServerURL: "https://generativelanguage.googleapis.com",
		OllamaServerURL: "http://ollama.local:11434/",
	}
}

func newModelResolver(
	flow database.Flow,
	prv provider.Provider,
) (adk.ProviderResolver, *modelResolverProvs) {
	provs := &modelResolverProvs{prv: prv}
	return NewADKModelResolver(provs, &finishFakeQuerier{flow: flow}, adkResolverConfig()), provs
}

func TestNewADKModelResolver_ResolvesFlowProvider(t *testing.T) {
	prv := &modelResolverProvider{ptype: provider.ProviderOpenAI, model: "gpt-5.2"}
	resolve, provs := newModelResolver(
		database.Flow{ID: 7, UserID: 3, ModelProviderName: "openai"}, prv)

	creds, err := resolve(context.Background(), 7, adk.AgentTypePentester)
	require.NoError(t, err)

	assert.Equal(t, adk.ModelCredentials{
		ModelName: "gpt-5.2",
		BaseURL:   "https://api.openai.com/v1",
		APIKey:    "openai-key",
		APIStyle:  adk.APIStyleChatCompletions,
	}, creds)
	assert.Equal(t, provider.ProviderName("openai"), provs.name)
	assert.Equal(t, int64(3), provs.user)
	assert.Equal(t, pconfig.OptionsTypePentester, prv.opt)
}

// The adviser's handle names its own options type, so a provider config that
// gives it a different model resolves differently from the pentester's.
func TestNewADKModelResolver_AdviserGetsOwnAgentConfig(t *testing.T) {
	prv := &modelResolverProvider{ptype: provider.ProviderOpenAI, model: "gpt-5-mini"}
	resolve, _ := newModelResolver(
		database.Flow{ID: 7, UserID: 3, ModelProviderName: "openai"}, prv)

	creds, err := resolve(context.Background(), 7, adk.AgentTypeAdviser)
	require.NoError(t, err)
	assert.Equal(t, "gpt-5-mini", creds.ModelName)
	assert.Equal(t, pconfig.OptionsTypeAdviser, prv.opt)
}

func TestNewADKModelResolver_GeminiUsesNativeStyle(t *testing.T) {
	prv := &modelResolverProvider{ptype: provider.ProviderGemini, model: "gemini-3.1-pro-preview"}
	resolve, _ := newModelResolver(
		database.Flow{ID: 7, UserID: 3, ModelProviderName: "gemini"}, prv)

	creds, err := resolve(context.Background(), 7, adk.AgentTypePentester)
	require.NoError(t, err)
	assert.Equal(t, adk.ModelCredentials{
		ModelName: "gemini-3.1-pro-preview",
		BaseURL:   "https://generativelanguage.googleapis.com",
		APIKey:    "gemini-key",
		APIStyle:  adk.APIStyleGemini,
	}, creds)
}

func TestNewADKModelResolver_OllamaPointsAtV1AndNeedsNoKey(t *testing.T) {
	prv := &modelResolverProvider{ptype: provider.ProviderOllama, model: "qwen3:32b"}
	resolve, _ := newModelResolver(
		database.Flow{ID: 7, UserID: 3, ModelProviderName: "ollama"}, prv)

	creds, err := resolve(context.Background(), 7, adk.AgentTypePentester)
	require.NoError(t, err)
	assert.Equal(t, "http://ollama.local:11434/v1", creds.BaseURL)
	assert.Equal(t, adk.APIStyleChatCompletions, creds.APIStyle)
}

// Providers with no OpenAI-compatible or native Gemini door fail non-retryably
// and name the type — a retry could never make them work.
func TestNewADKModelResolver_UnsupportedTypeFailsNonRetryably(t *testing.T) {
	for _, ptype := range []provider.ProviderType{
		provider.ProviderAnthropic, provider.ProviderBedrock,
	} {
		prv := &modelResolverProvider{ptype: ptype, model: "any"}
		resolve, _ := newModelResolver(
			database.Flow{ID: 7, UserID: 3, ModelProviderName: string(ptype)}, prv)

		_, err := resolve(context.Background(), 7, adk.AgentTypePentester)
		require.Error(t, err)
		assert.Contains(t, err.Error(), string(ptype))
		var appErr *sdktemporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		assert.True(t, appErr.NonRetryable())
	}
}

func TestNewADKModelResolver_MissingKeyFailsNonRetryably(t *testing.T) {
	prv := &modelResolverProvider{ptype: provider.ProviderMistral, model: "mistral-large"}
	resolve, _ := newModelResolver(
		database.Flow{ID: 7, UserID: 3, ModelProviderName: "mistral"}, prv)

	_, err := resolve(context.Background(), 7, adk.AgentTypePentester)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no api key")
	var appErr *sdktemporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	assert.True(t, appErr.NonRetryable())
}

func TestNewADKModelResolver_UnknownAgentTypeFailsNonRetryably(t *testing.T) {
	resolve, _ := newModelResolver(
		database.Flow{ID: 7, UserID: 3, ModelProviderName: "openai"},
		&modelResolverProvider{ptype: provider.ProviderOpenAI, model: "m"})

	_, err := resolve(context.Background(), 7, "not-an-agent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not-an-agent")
	var appErr *sdktemporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	assert.True(t, appErr.NonRetryable())
}

// Lookup failures — the flow row, the provider row — stay ordinary errors so
// the activity's retry policy can ride out a transient database blip.
func TestNewADKModelResolver_LookupErrorsStayRetryable(t *testing.T) {
	resolve := NewADKModelResolver(
		&modelResolverProvs{},
		&finishFakeQuerier{flowErr: errors.New("db down")},
		adkResolverConfig())

	_, err := resolve(context.Background(), 7, adk.AgentTypePentester)
	require.Error(t, err)
	var appErr *sdktemporal.ApplicationError
	require.False(t, errors.As(err, &appErr) && appErr.NonRetryable())

	resolve = NewADKModelResolver(
		&modelResolverProvs{err: errors.New("provider gone")},
		&finishFakeQuerier{flow: database.Flow{ID: 7, ModelProviderName: "openai"}},
		adkResolverConfig())

	_, err = resolve(context.Background(), 7, adk.AgentTypePentester)
	require.Error(t, err)
	assert.ErrorContains(t, err, "provider gone")
}
