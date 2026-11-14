package adk

import (
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/agent/llmagent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/tool"
)

// The production prompt port is owned outside this spike; this instruction only
// needs to exercise the loop honestly enough to compare it with the existing
// langchaingo pentester.
const pentesterInstruction = `You are a penetration-testing specialist on an authorized assessment.
Work the task you are given and only that task.

Tools: terminal runs shell commands in a sandbox, nuclei_scan runs template
scans against one URL, httpx_probe fetches HTTP metadata for one URL.
Prefer targeted probes over broad scans, and say briefly what each action is
for before you take it.

When you finish, report concrete findings: what you tested, what you found,
the evidence for each finding, and suggested next steps.`

// ScopeGuard is the seam reserved for the authorization checks the production
// port wires in. The spike leaves it unset; an installed guard must default to
// allowing the call so nothing here changes behavior on its own.
type ScopeGuard func(ctx agent.Context, t tool.Tool, args map[string]any) (map[string]any, error)

func NewPentesterAgent(mdl model.LLM, tools []tool.Tool, guard ScopeGuard) (agent.Agent, error) {
	cfg := llmagent.Config{
		Name:        "pentester",
		Description: "executes penetration-testing tasks with sandboxed tools",
		Model:       mdl,
		Instruction: pentesterInstruction,
		Tools:       tools,
	}
	if guard != nil {
		cfg.BeforeToolCallbacks = []llmagent.BeforeToolCallback{llmagent.BeforeToolCallback(guard)}
	}
	return llmagent.New(cfg)
}
