package adk

import (
	"encoding/json"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/session"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/adk/v2/tool/functiontool"
)

// hackResultStateKey is where the closing call leaves its payload on the
// event's state delta: later phases read it back from the exported session
// rather than reparsing the transcript.
const hackResultStateKey = "pentagi.hack_result"

// hackResultProcessed mirrors the acknowledgement the production handler
// returns; the model never sees it because the loop ends on this response.
const hackResultProcessed = "hack result successfully processed"

// HackResult is the pentester's closing call extracted into the workflow
// result — production stores the same payload on the msgchain result.
type HackResult struct {
	Success bool   `json:"success"`
	Result  string `json:"result"`
}

// newHackResultTool builds the barrier tool that replaces production's
// executor barrier map entry. Setting SkipSummarization marks the function
// response as the agent's final event (session.Event.IsFinalResponse), so the
// ADK loop ends on this call exactly the way performAgentChain ends on
// hack_result — the model gets no summarization turn afterwards. It must stay
// an in-workflow function tool: an activity round-trip would defer the barrier
// behind Temporal retries.
func newHackResultTool() (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        HackResultToolName,
		Description: hackResultToolDesc,
	}, func(ctx agent.Context, in HackResultInput) (map[string]any, error) {
		success := in.Success != nil && *in.Success
		if err := ctx.State().Set(hackResultStateKey, map[string]any{
			"success": success,
			"result":  in.Result,
		}); err != nil {
			return nil, err
		}
		ctx.Actions().SkipSummarization = true
		return map[string]any{"result": hackResultProcessed}, nil
	})
}

// extractHackResult pulls the hack_result call out of the event stream. The
// function call's args carry everything state does, and scanning events works
// identically in-workflow and direct — reading session state back needs the
// session object, which the runner owns.
func extractHackResult(events []*session.Event) *HackResult {
	for _, ev := range events {
		if ev == nil || ev.Content == nil {
			continue
		}
		for _, p := range ev.Content.Parts {
			if p == nil || p.FunctionCall == nil || p.FunctionCall.Name != HackResultToolName {
				continue
			}
			raw, err := json.Marshal(p.FunctionCall.Args)
			if err != nil {
				continue
			}
			var args struct {
				Success *bool  `json:"success"`
				Result  string `json:"result"`
			}
			if json.Unmarshal(raw, &args) != nil {
				continue
			}
			out := &HackResult{Result: args.Result}
			if args.Success != nil {
				out.Success = *args.Success
			}
			return out
		}
	}
	return nil
}
