package controller

import (
	"context"
	"errors"
	"testing"

	"pentagi/pkg/config"
	"pentagi/pkg/database"
	"pentagi/pkg/graphiti"
	"pentagi/pkg/providers"
	"pentagi/pkg/templates"
	"pentagi/pkg/temporal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type prompterFakeQuerier struct {
	database.Querier

	prompts []database.Prompt
	err     error
}

func (f *prompterFakeQuerier) GetUserPrompts(context.Context, int64) ([]database.Prompt, error) {
	return f.prompts, f.err
}

func TestPrompter_NewUserPrompter_OverlaysTheUsersPromptsOnTheDefaults(t *testing.T) {
	dbErr := errors.New("db connection lost")
	defaults := templates.NewDefaultPrompter()

	for _, tc := range []struct {
		name        string
		prompts     []database.Prompt
		dbErr       error
		wantCustom  map[templates.PromptType]string
		wantDefault []templates.PromptType
	}{
		{
			name:        "no user prompts",
			wantDefault: []templates.PromptType{templates.PromptTypePrimaryAgent, templates.PromptTypeAssistant},
		},
		{
			name:        "one override",
			prompts:     []database.Prompt{{Type: database.PromptTypePrimaryAgent, Prompt: "custom primary"}},
			wantCustom:  map[templates.PromptType]string{templates.PromptTypePrimaryAgent: "custom primary"},
			wantDefault: []templates.PromptType{templates.PromptTypeAssistant, templates.PromptTypePentester},
		},
		{
			name: "several overrides",
			prompts: []database.Prompt{
				{Type: database.PromptTypePrimaryAgent, Prompt: "custom primary"},
				{Type: database.PromptTypeCoder, Prompt: "custom coder"},
				{Type: database.PromptTypeReporter, Prompt: "custom reporter"},
			},
			wantCustom: map[templates.PromptType]string{
				templates.PromptTypePrimaryAgent: "custom primary",
				templates.PromptTypeCoder:        "custom coder",
				templates.PromptTypeReporter:     "custom reporter",
			},
			wantDefault: []templates.PromptType{
				templates.PromptTypeAssistant, templates.PromptTypePentester, templates.PromptTypeSearcher,
			},
		},
		{
			name:        "an empty body, which would surface deep in agent rendering",
			prompts:     []database.Prompt{{Type: database.PromptTypePrimaryAgent, Prompt: ""}},
			wantDefault: []templates.PromptType{templates.PromptTypePrimaryAgent},
		},
		{
			name: "an override using a variable the backend no longer provides",
			prompts: []database.Prompt{
				{Type: database.PromptTypeSearcher, Prompt: "Search with {{.GoogleToolName}}"},
				{Type: database.PromptTypeCoder, Prompt: "custom coder"},
			},
			wantCustom:  map[templates.PromptType]string{templates.PromptTypeCoder: "custom coder"},
			wantDefault: []templates.PromptType{templates.PromptTypeSearcher},
		},
		{name: "a database that cannot be read", dbErr: dbErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prompter, err := newUserPrompter(context.Background(), &prompterFakeQuerier{prompts: tc.prompts, err: tc.dbErr}, 42)

			if tc.dbErr != nil {
				assert.ErrorIs(t, err, tc.dbErr, "a session is not built on defaults the user did not choose")
				assert.Nil(t, prompter)

				return
			}
			require.NoError(t, err)
			for pt, want := range tc.wantCustom {
				got, err := prompter.GetTemplate(pt)
				require.NoError(t, err)
				assert.Equal(t, want, got, "prompt %s", pt)
			}
			for _, pt := range tc.wantDefault {
				got, err := prompter.GetTemplate(pt)
				require.NoError(t, err)
				want, err := defaults.GetTemplate(pt)
				require.NoError(t, err)
				assert.Equal(t, want, got, "prompt %s keeps its default", pt)
			}
		})
	}
}

type renderFakeQuerier struct {
	database.Querier

	flow    database.Flow
	prompts []database.Prompt
	cnt     database.Container
	cntErr  error
}

func (q *renderFakeQuerier) GetFlow(context.Context, int64) (database.Flow, error) {
	return q.flow, nil
}

func (q *renderFakeQuerier) GetUserPrompts(context.Context, int64) ([]database.Prompt, error) {
	return q.prompts, nil
}

func (q *renderFakeQuerier) GetFlowPrimaryContainer(context.Context, int64) (database.Container, error) {
	return q.cnt, q.cntErr
}

type renderFakeProviders struct {
	providers.ProviderController
}

func (renderFakeProviders) GraphitiClient() *graphiti.Client { return nil }

func TestPrompter_RenderAgentPrompt_RendersThePentesterPair(t *testing.T) {
	q := &renderFakeQuerier{
		flow: database.Flow{ID: 7, UserID: 42, Language: "English"},
		cnt:  database.Container{Image: "vxcontrol/kali-linux", Name: "worker-7"},
	}
	runner := durableRunner{fc: &flowController{db: q, provs: renderFakeProviders{}, cfg: &config.Config{}}}

	res, err := runner.RenderAgentPrompt(context.Background(), temporal.PromptRenderRequest{
		FlowID: 7, AgentType: temporal.AgentTypePentester, Input: "scan https://target.test",
	})

	require.NoError(t, err)
	assert.Contains(t, res.Instruction, "hack_result")
	assert.Contains(t, res.Instruction, "terminal")
	assert.Contains(t, res.Instruction, "English")
	assert.Contains(t, res.Question, "scan https://target.test")
}

func TestPrompter_RenderAgentPrompt_RendersTheAdviserPrompt(t *testing.T) {
	q := &renderFakeQuerier{
		flow: database.Flow{ID: 7, UserID: 42, Language: "English"},
		cnt:  database.Container{Image: "vxcontrol/kali-linux", Name: "worker-7"},
	}
	runner := durableRunner{fc: &flowController{db: q, provs: renderFakeProviders{}, cfg: &config.Config{}}}

	res, err := runner.RenderAgentPrompt(context.Background(), temporal.PromptRenderRequest{
		FlowID: 7, AgentType: temporal.AgentTypeAdviser,
	})

	require.NoError(t, err)
	assert.NotEmpty(t, res.Instruction, "the adviser system prompt must render")
	assert.Contains(t, res.Instruction, "advice")
	assert.Empty(t, res.Question, "the question template wraps per-call args, not a render-time input")
}

func TestPrompter_RenderAgentPrompt_RejectsUnknownAgentType(t *testing.T) {
	runner := durableRunner{fc: &flowController{}}

	_, err := runner.RenderAgentPrompt(context.Background(), temporal.PromptRenderRequest{
		FlowID: 7, AgentType: "coder", Input: "x",
	})

	assert.ErrorContains(t, err, `unsupported agent type "coder"`)
}
