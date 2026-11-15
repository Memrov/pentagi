package adk

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/runner"
	"google.golang.org/adk/v2/session"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/adk/v2/tool/functiontool"
	"google.golang.org/adk/v2/tool/toolconfirmation"
	"google.golang.org/genai"
)

// DirectTools builds the same three tools as in-process function tools, for
// running the agent without Temporal; tool results are identical in shape, and
// the timeout must match the durable path or the A/B comparison measures
// different tool budgets.
func DirectTools(sb Sandbox, payload PayloadStore, threshold int, toolTimeout time.Duration) ([]tool.Tool, error) {
	acts := NewActivities(sb, payload, threshold, toolTimeout)

	terminal, err := functiontool.New(functiontool.Config{
		Name:        TerminalToolName,
		Description: terminalToolDesc,
	}, func(ctx agent.Context, in TerminalExecInput) (ToolResult, error) {
		return acts.TerminalExec(ctx, in)
	})
	if err != nil {
		return nil, err
	}

	nuclei, err := functiontool.New(functiontool.Config{
		Name:        NucleiToolName,
		Description: nucleiToolDesc,
	}, func(ctx agent.Context, in NucleiScanInput) (ToolResult, error) {
		return acts.NucleiScan(ctx, in)
	})
	if err != nil {
		return nil, err
	}

	httpx, err := functiontool.New(functiontool.Config{
		Name:        HttpxToolName,
		Description: httpxToolDesc,
	}, func(ctx agent.Context, in HttpxProbeInput) (ToolResult, error) {
		return acts.HttpxProbe(ctx, in)
	})
	if err != nil {
		return nil, err
	}

	return []tool.Tool{terminal, nuclei, httpx}, nil
}

// RunDirect executes the pentester agent in-process, no Temporal: the fast path
// for comparing the ADK loop against the existing one before wiring durability.
// A tool confirmation pauses nothing here — pending requests surface as an
// error since there is no signal to resume on.
func RunDirect(ctx context.Context, cfg *Config, sb Sandbox, payload PayloadStore, task string) (PentestResult, error) {
	if cfg.APIKey == "" {
		return PentestResult{}, fmt.Errorf("ADK_API_KEY is required")
	}
	mdl, err := ModelFactory(cfg)(ctx, cfg.ModelName)
	if err != nil {
		return PentestResult{}, fmt.Errorf("failed to build model: %w", err)
	}
	return runDirect(ctx, mdl, task, directOptions{
		sb:          sb,
		payload:     payload,
		threshold:   cfg.PayloadThreshold,
		toolTimeout: cfg.ToolTimeout(),
	})
}

type directOptions struct {
	sb          Sandbox
	payload     PayloadStore
	threshold   int
	toolTimeout time.Duration
	guard       ScopeGuard
}

func runDirect(ctx context.Context, mdl model.LLM, task string, opts directOptions) (PentestResult, error) {
	tools, err := DirectTools(opts.sb, opts.payload, opts.threshold, opts.toolTimeout)
	if err != nil {
		return PentestResult{}, err
	}
	pentester, err := NewPentesterAgent(mdl, tools, opts.guard)
	if err != nil {
		return PentestResult{}, err
	}
	r, err := runner.New(runner.Config{
		AppName:           appName,
		Agent:             pentester,
		SessionService:    session.InMemoryService(),
		AutoCreateSession: true,
	})
	if err != nil {
		return PentestResult{}, err
	}

	msg := genai.NewContentFromText(task, genai.RoleUser)
	var res PentestResult
	for ev, err := range r.Run(ctx, "pentagi", "direct", msg, agent.RunConfig{}) {
		if err != nil {
			return res, err
		}
		if ev == nil {
			continue
		}
		res.EventCount++
		if ev.Content == nil {
			continue
		}
		for _, p := range ev.Content.Parts {
			if p != nil {
				if p.Text != "" {
					res.Answer = p.Text
				}
				if p.FunctionCall != nil && p.FunctionCall.Name == toolconfirmation.FunctionCallName {
					return res, errors.New("tool requested confirmation, which direct mode cannot answer")
				}
			}
		}
	}
	return res, nil
}
