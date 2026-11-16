package adk

import (
	"cmp"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/agent/llmagent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/adk/v2/tool/agenttool"
	"google.golang.org/genai"
)

// adviceToolDesc is verbatim from tools.registryDefinitions — the production
// prompts were written against exactly this string.
const adviceToolDesc = "Get more complex answer from the mentor about some issue or difficult situation"

// Fallback when no rendered adviser prompt arrives, mirroring the pentester
// default: the production adviser.tmpl render stays worker-side and a flow
// still preparing may not supply one.
const adviserInstruction = `You are the senior technical mentor for an authorized penetration test.
Answer the specialist's question with concrete, prioritized recommendations.
The question arrives as a JSON object: question is the ask; code and output are
optional verbatim excerpts; message is the asker's engagement-log line.`

// adviceInputSchema mirrors tools.AskAdvice field-for-field, so the model
// faces the same argument contract as production's advice tool.
var adviceInputSchema = &genai.Schema{
	Type: "OBJECT",
	Properties: map[string]*genai.Schema{
		"question": {
			Type:        "STRING",
			Description: "Technical-channel payload — detailed question for the senior mentor describing the issue and what you need to figure out. Always written in English; never translated.",
		},
		"code": {
			Type:        "STRING",
			Description: "Technical-channel excerpt — optional code snippet relevant to the question. Preserve verbatim; do not translate code or comments.",
		},
		"output": {
			Type:        "STRING",
			Description: "Technical-channel excerpt — optional stdout/stderr excerpt relevant to the question. Preserve verbatim.",
		},
		"message": {
			Type:        "STRING",
			Description: "Engagement-log entry — a 1-2 short sentence running commentary explaining what you are asking the mentor and why. Written in the engagement language declared by your system prompt.",
		},
	},
	Required: []string{"question", "message"},
}

// newAdviceTool builds the first specialist delegation: an adviser llmagent
// wrapped as an agenttool, so the parent's tool call runs a nested agent run
// and its final text returns as the tool result. SkipSummarization must stay
// false — true marks the sub-agent's result the parent's final event and ends
// the whole run on the advice, while the pentester is expected to keep working
// and finish through hack_result.
//
// The adviser gets no tools: production's chain is a single-shot call. The
// InputSchema makes agenttool validate the model's args and forward them as
// the user message; the rendered adviser system prompt carries the engagement
// context. Production routes the question through an enricher agent first —
// deferred until memory/search tools exist.
func newAdviceTool(mdl model.LLM, instruction string) (tool.Tool, error) {
	instruction = cmp.Or(instruction, adviserInstruction)
	adviser, err := llmagent.New(llmagent.Config{
		Name:        AdviceToolName,
		Description: adviceToolDesc,
		Model:       mdl,
		InstructionProvider: func(agent.ReadonlyContext) (string, error) {
			return instruction, nil
		},
		InputSchema: adviceInputSchema,
	})
	if err != nil {
		return nil, err
	}
	return agenttool.New(adviser, &agenttool.Config{SkipSummarization: false}), nil
}
