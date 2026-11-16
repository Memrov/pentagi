package adk

import (
	"encoding/json"
	"fmt"
	"maps"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/genai"
)

const (
	// defaultMaxIterations mirrors production maxGeneralAgentChainIterations.
	defaultMaxIterations = 100
	// repeatCallThreshold is production's RepeatingToolCallThreshold: the Nth
	// consecutive identical call is answered from the cached result instead of
	// running the tool again.
	repeatCallThreshold = 3
	// repeatCallAbortAt is RepeatingToolCallThreshold +
	// maxSoftDetectionsBeforeAbort: the consecutive repeat that kills the chain.
	repeatCallAbortAt = repeatCallThreshold + 4
	// maxBarrierRuns caps runs that may end without hack_result; production's
	// reflector stops nudging at maxReflectorCallsPerChain.
	maxBarrierRuns = 3
)

const (
	repeatNoticeText    = "This exact call already ran %d times with these arguments; the result will not change. Alter arguments or answer from what you have."
	repeatAbortErrType  = "RepeatingToolCallAbort"
	iterationLimitText  = "Iteration limit (%d) reached. Report findings via hack_result now."
	missingBarrierNudge = "You must finish by calling hack_result with success and your findings."
)

// runGuards holds the agent-side guardrails for one invocation: the repeat
// detector and the model-turn counter. Both are per-run state — a reflector
// re-run or a continued-as-new execution builds a fresh set, matching
// production where each agent chain constructs its own repeatingDetector. The
// callbacks never block and never read the environment, so they stay
// deterministic inside a workflow.
type runGuards struct {
	repeat        *repeatDetector
	maxIterations int
	modelCalls    int
}

func newRunGuards(maxIterations int) *runGuards {
	if maxIterations <= 0 {
		maxIterations = defaultMaxIterations
	}
	return &runGuards{repeat: &repeatDetector{}, maxIterations: maxIterations}
}

// beforeModel counts turns for this invocation. Past the cap it answers in the
// model's place with a text-only response — a final response in ADK, so the
// run ends and the workflow's missing-barrier path takes over — rather than
// aborting the way production's hard error does.
func (g *runGuards) beforeModel(_ agent.Context, _ *model.LLMRequest) (*model.LLMResponse, error) {
	if err := g.repeat.abortErr; err != nil {
		return nil, err
	}
	if g.modelCalls >= g.maxIterations {
		return &model.LLMResponse{
			Content: &genai.Content{
				Role:  genai.RoleModel,
				Parts: []*genai.Part{{Text: fmt.Sprintf(iterationLimitText, g.maxIterations)}},
			},
			TurnComplete: true,
		}, nil
	}
	g.modelCalls++
	return nil, nil
}

func (g *runGuards) beforeTool(ctx agent.Context, t tool.Tool, args map[string]any) (map[string]any, error) {
	return g.repeat.before(ctx, t, args)
}

func (g *runGuards) afterTool(ctx agent.Context, t tool.Tool, args, result map[string]any, err error) (map[string]any, error) {
	return g.repeat.after(ctx, t, args, result, err)
}

// repeatDetector tracks consecutive identical tool calls, the way production's
// repeatingDetector does per agent chain. Arguments are canonicalized the same
// way — the volatile "message" commentary arg is dropped and the rest compared
// by content — so a call that only rephrases its log line still counts.
type repeatDetector struct {
	lastKey     string
	consecutive int
	lastResult  map[string]any
	abortErr    error
}

func (d *repeatDetector) before(_ agent.Context, t tool.Tool, args map[string]any) (map[string]any, error) {
	key := callKey(t.Name(), args)
	if key == d.lastKey {
		d.consecutive++
	} else {
		d.lastKey = key
		d.consecutive = 1
		d.lastResult = nil
	}
	switch {
	case d.consecutive >= repeatCallAbortAt:
		d.abortErr = temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("tool %q repeated %d times consecutively, aborting chain", t.Name(), d.consecutive),
			repeatAbortErrType, nil)
		// The callback error still becomes the call's {"error": ...} payload,
		// so the toolcall row records the abort; the next model turn never
		// runs because beforeModel propagates abortErr out of the run.
		return nil, d.abortErr
	case d.consecutive >= repeatCallThreshold:
		replay := make(map[string]any, len(d.lastResult)+1)
		maps.Copy(replay, d.lastResult)
		replay["repeat_notice"] = fmt.Sprintf(repeatNoticeText, d.consecutive)
		return replay, nil
	default:
		return nil, nil
	}
}

// after caches the result of calls that actually executed, for the replay on a
// later repeat. A response carrying repeat_notice came from the callback, not
// the tool — storing it would echo the notice back instead of the real result.
// A call outside the current consecutive run is ignored: parallel fan-out can
// interleave callbacks of different calls.
func (d *repeatDetector) after(_ agent.Context, t tool.Tool, args, result map[string]any, err error) (map[string]any, error) {
	if err != nil || callKey(t.Name(), args) != d.lastKey {
		return nil, nil
	}
	if _, replayed := result["repeat_notice"]; !replayed {
		d.lastResult = maps.Clone(result)
	}
	return nil, nil
}

// callKey identifies a call by tool name plus canonical JSON of the args with
// "message" removed — Go's json.Marshal sorts map keys, which gives the
// sorted-key canonicalization production does by hand.
func callKey(name string, args map[string]any) string {
	stripped := maps.Clone(args)
	delete(stripped, "message")
	raw, err := json.Marshal(stripped)
	if err != nil {
		return name
	}
	return name + "\x00" + string(raw)
}
