package adk

import (
	"cmp"

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

Tools: terminal runs shell commands in a sandbox, file reads and writes files
under the sandbox, nuclei_scan runs template scans against one URL, httpx_probe
fetches HTTP metadata for one URL.
Prefer targeted probes over broad scans, and say briefly what each action is
for before you take it.

Finish by calling hack_result with what you tested, what you found, the
evidence for each finding, and suggested next steps — that call ends the run.`

// ScopeGuard is the seam reserved for the authorization checks the production
// port wires in. The spike leaves it unset; an installed guard must default to
// allowing the call so nothing here changes behavior on its own.
type ScopeGuard func(ctx agent.Context, t tool.Tool, args map[string]any) (map[string]any, error)

// NewPentesterAgent builds the loop agent; an empty instruction keeps the
// spike default. The text goes through InstructionProvider rather than the
// Instruction field: llmagent expands {name} placeholders in that field
// against session state, and a rendered production prompt carries literal
// {target_ip}-style markers that must reach the model untouched.
func NewPentesterAgent(mdl model.LLM, tools []tool.Tool, instruction string, guard ScopeGuard) (agent.Agent, error) {
	instruction = cmp.Or(instruction, pentesterInstruction)
	cfg := llmagent.Config{
		Name:        "pentester",
		Description: "executes penetration-testing tasks with sandboxed tools",
		Model:       mdl,
		InstructionProvider: func(agent.ReadonlyContext) (string, error) {
			return instruction, nil
		},
		Tools: tools,
	}
	if guard != nil {
		cfg.BeforeToolCallbacks = []llmagent.BeforeToolCallback{llmagent.BeforeToolCallback(guard)}
	}
	return llmagent.New(cfg)
}
