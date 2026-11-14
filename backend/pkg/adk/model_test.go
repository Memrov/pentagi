package adk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/adk/v2/model"
	"google.golang.org/genai"
)

// chatCompletionsFixture serves one canned chat completion and records how it
// was called: path, headers and the model the client asked for.
type chatCompletionsFixture struct {
	srv         *httptest.Server
	path        string
	auth        string
	userAgent   string
	session     string
	model       string
	lastRequest map[string]any
}

func newChatCompletionsFixture(t *testing.T, response map[string]any) *chatCompletionsFixture {
	f := &chatCompletionsFixture{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.path = r.URL.Path
		f.auth = r.Header.Get("Authorization")
		f.userAgent = r.Header.Get("User-Agent")
		f.session = r.Header.Get("x-opencode-session")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&f.lastRequest))
		f.model, _ = f.lastRequest["model"].(string)
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(response))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func TestModelFactory_ChatCompletions_RoundTrip(t *testing.T) {
	fixture := newChatCompletionsFixture(t, map[string]any{
		"id":     "chatcmpl-1",
		"object": "chat.completion",
		"choices": []any{
			map[string]any{
				"index":         0,
				"message":       map[string]any{"role": "assistant", "content": "port 22 is open"},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]any{"prompt_tokens": 9, "completion_tokens": 5, "total_tokens": 14},
	})

	cfg := &Config{APIKey: "zen-key-123", BaseURL: fixture.srv.URL, ModelAPI: "chat_completions"}
	llm, err := ModelFactory(cfg)(context.Background(), "deepseek-v4.1-flash")
	require.NoError(t, err)
	assert.Equal(t, "deepseek-v4.1-flash", llm.Name())

	req := &model.LLMRequest{
		Model:    "deepseek-v4.1-flash",
		Contents: []*genai.Content{genai.NewContentFromText("check port 22", genai.RoleUser)},
	}
	var got *model.LLMResponse
	for resp, err := range llm.GenerateContent(context.Background(), req, false) {
		require.NoError(t, err)
		got = resp
	}
	require.NotNil(t, got)
	require.NotNil(t, got.Content)
	require.NotEmpty(t, got.Content.Parts)
	assert.Equal(t, "port 22 is open", got.Content.Parts[0].Text)

	assert.Equal(t, "/chat/completions", fixture.path)
	assert.Equal(t, "Bearer zen-key-123", fixture.auth)
	assert.Equal(t, "deepseek-v4.1-flash", fixture.model)
	assert.Equal(t, "pentagi-adk/1.0", fixture.userAgent)
	assert.True(t, strings.HasPrefix(fixture.session, "pentagi-"), "session %q", fixture.session)
}
