package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"pentagi/pkg/adk"
	"pentagi/pkg/csum"
	"pentagi/pkg/database"
	"pentagi/pkg/docker"
	"pentagi/pkg/flowfiles"
	"pentagi/pkg/providers"
	"pentagi/pkg/templates"
	"pentagi/pkg/templates/validator"
	"pentagi/pkg/temporal"

	"github.com/sirupsen/logrus"
)

// newUserPrompter loads the user's custom prompts from the database and
// overlays them onto the compiled default templates. Prompt types that
// the user has not customized continue to use the defaults. A database
// error is returned to the caller so that session creation fails
// explicitly instead of silently falling back to defaults.
func newUserPrompter(ctx context.Context, db database.Querier, userID int64) (templates.Prompter, error) {
	userPrompts, err := db.GetUserPrompts(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to load user prompts: %w", err)
	}

	defaults, err := templates.LoadDefaultPromptsMap()
	if err != nil {
		return nil, fmt.Errorf("failed to load default templates: %w", err)
	}

	return buildUserPrompter(defaults, userPrompts), nil
}

// buildUserPrompter overlays each non-empty user override that still validates
// onto defaults and returns a Prompter backed by that map. It mutates defaults:
// callers must pass a fresh map (e.g., from templates.LoadDefaultPromptsMap)
// so the embedded defaults are not modified.
func buildUserPrompter(defaults templates.PromptsMap, userPrompts []database.Prompt) templates.Prompter {
	for _, p := range userPrompts {
		if p.Prompt == "" {
			// The Prompts UI uses delete (or reset, which writes the
			// default body back) to remove a customization, so an empty
			// body is unexpected. Skip it instead of clobbering the
			// default with an empty string that would later surface as
			// ErrTemplateNotFound deep inside agent rendering.
			continue
		}

		// Saving validates against the variables declared at that time, so a stored
		// override can name one that was removed since; text/template would render
		// the missing key as "<no value>" without an error and point the agent at a
		// tool that does not exist.
		promptType := templates.PromptType(p.Type)
		if err := validator.ValidatePrompt(promptType, p.Prompt); err != nil {
			logrus.WithError(err).WithField("prompt_type", p.Type).
				Warn("custom prompt no longer validates, using the default")
			continue
		}
		defaults[promptType] = p.Prompt
	}

	return templates.NewFlowPrompter(defaults)
}

// RenderAgentPrompt backs pentagi.RenderAgentPrompt: the pentester pair
// rendered over the flow's real environment — the durable engine asks for it
// through an activity because the user prompter and the container row live
// worker-side. A flow still preparing has no container row; the image facts
// then stay empty instead of failing the render.
func (r durableRunner) RenderAgentPrompt(ctx context.Context, req temporal.PromptRenderRequest) (temporal.RenderedPrompt, error) {
	if req.AgentType != temporal.AgentTypePentester {
		return temporal.RenderedPrompt{}, fmt.Errorf("unsupported agent type %q", req.AgentType)
	}
	flow, err := r.fc.db.GetFlow(ctx, req.FlowID)
	if err != nil {
		return temporal.RenderedPrompt{}, fmt.Errorf("failed to load flow %d: %w", req.FlowID, err)
	}
	prompter, err := newUserPrompter(ctx, r.fc.db, flow.UserID)
	if err != nil {
		return temporal.RenderedPrompt{}, err
	}

	var image string
	if cnt, err := r.fc.db.GetFlowPrimaryContainer(ctx, req.FlowID); err == nil {
		image = cnt.Image
	}
	graphiti := r.fc.provs.GraphitiClient()

	vars := adk.PentesterPromptVars(adk.PentesterPromptInput{
		Question: req.Input,
		Tools: []string{
			adk.HackResultToolName, adk.TerminalToolName, adk.FileToolName,
			adk.NucleiToolName, adk.HttpxToolName,
		},
		GraphitiEnabled:         graphiti != nil && graphiti.IsEnabled(),
		IsDefaultDockerImage:    providers.IsPentestImage(r.fc.cfg, image),
		DockerImage:             image,
		Cwd:                     docker.WorkFolderPathInContainer,
		ContainerPorts:          providers.ContainerPortsDescription(r.fc.cfg, req.FlowID),
		Lang:                    flow.Language,
		CurrentTime:             time.Now().Format("2006-01-02 15:04:05"),
		ToolPlaceholder:         providers.ToolPlaceholder,
		UserFiles:               flowfiles.FileListingForPrompt(r.fc.cfg.DataDir, uint64(flow.ID)),
		SummarizedContentPrefix: strings.ReplaceAll(csum.SummarizedContentPrefix, "\n", "\\n"),
	})

	instruction, err := prompter.RenderTemplate(templates.PromptTypePentester, vars)
	if err != nil {
		return temporal.RenderedPrompt{}, fmt.Errorf("failed to render pentester prompt: %w", err)
	}
	question, err := prompter.RenderTemplate(templates.PromptTypeQuestionPentester, map[string]any{"Question": req.Input})
	if err != nil {
		return temporal.RenderedPrompt{}, fmt.Errorf("failed to render question prompt: %w", err)
	}

	return temporal.RenderedPrompt{Instruction: instruction, Question: question}, nil
}
