package adk

import (
	"cmp"
	"fmt"
	"time"

	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/runner"
	"google.golang.org/adk/v2/session"
	"google.golang.org/genai"
)

type PentestRequest struct {
	Task      string `json:"task"`
	Model     string `json:"model,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	// Instruction replaces the pentester's system prompt; Question replaces the
	// user message the runner opens with (Task is the fallback). Both exist so
	// the production prompt port can inject its rendered template and
	// per-request text without going through Task.
	Instruction string `json:"instruction,omitempty"`
	Question    string `json:"question,omitempty"`
	// ToolTimeoutSeconds overrides the activity deadline for each tool call.
	ToolTimeoutSeconds int `json:"tool_timeout_seconds,omitempty"`
	// MaxIterations caps model calls per run; 0 falls back to the production
	// general-agent chain cap.
	MaxIterations int `json:"max_iterations,omitempty"`
	// IncludeAdvice adds the adviser delegation tool; AdviceInstruction is its
	// rendered system prompt, produced beside the pentester pair worker-side.
	IncludeAdvice     bool   `json:"include_advice,omitempty"`
	AdviceInstruction string `json:"advice_instruction,omitempty"`
	// Session and BarrierRetries carry a run across continue-as-new: Session is
	// the exported ADK session (conversation history plus session-scoped
	// state), BarrierRetries how many completed turns already ended without
	// hack_result, so the reflector budget continues rather than restarting.
	Session        *googleadk.SessionSnapshot `json:"session,omitempty"`
	BarrierRetries int                        `json:"barrier_retries,omitempty"`
	// FlowID, EmitEvents and EmitTaskQueue switch on the UI log bridge: each
	// session event is translated and persisted through EmitFlowEventActivity
	// on the flows queue — the queue the flow's log workers live on, which is
	// why the child cannot just inherit its own. CLI runs leave them zero.
	FlowID        int64  `json:"flow_id,omitempty"`
	EmitEvents    bool   `json:"emit_events,omitempty"`
	EmitTaskQueue string `json:"emit_task_queue,omitempty"`
}

type PentestResult struct {
	Answer     string      `json:"answer"`
	HackResult *HackResult `json:"hack_result,omitempty"`
	EventCount int         `json:"event_count"`
}

// modelActivityOptions bounds InvokeModel the way tools are bounded: a
// heartbeat timeout (the plugin heartbeats on a timer once it is set) and a
// bounded retry, so a wedged model call is detected and a persistent provider
// failure fails the workflow instead of retrying forever.
var modelActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 10 * time.Minute,
	HeartbeatTimeout:    30 * time.Second,
	RetryPolicy: &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    3,
	},
}

// PentesterWorkflow drives the ADK pentester for one task. Each pass over
// runner.Run ends either with an answer or with tool calls paused on human
// confirmation; in the latter case the workflow waits durably on a signal
// before resuming, so a confirmation can sit outstanding across restarts.
// A pass that ends without the hack_result barrier is nudged back into the
// agent, up to the production reflector budget; after the last pass the
// workflow reports a failed HackResult itself.
func PentesterWorkflow(ctx workflow.Context, req PentestRequest) (PentestResult, error) {
	toolTimeout := DefaultToolTimeout
	if req.ToolTimeoutSeconds > 0 && req.ToolTimeoutSeconds <= int((24*time.Hour)/time.Second) {
		toolTimeout = time.Duration(req.ToolTimeoutSeconds) * time.Second
	}

	tools, err := DurableTools(toolTimeout)
	if err != nil {
		return PentestResult{}, err
	}

	modelName := cmp.Or(req.Model, DefaultModelName)
	mdl := googleadk.NewModel(modelName, googleadk.WithModelActivityOptions(modelActivityOptions))

	if req.IncludeAdvice {
		adviceTool, err := newAdviceTool(mdl, req.AdviceInstruction)
		if err != nil {
			return PentestResult{}, err
		}
		tools = append(tools, adviceTool)
	}

	userID := cmp.Or(req.UserID, "pentagi")
	sessionID := cmp.Or(req.SessionID, workflow.GetInfo(ctx).WorkflowExecution.ID)

	svc := session.InMemoryService()
	adkCtx := googleadk.NewContext(ctx)
	if req.Session != nil {
		if _, err := googleadk.ImportSession(adkCtx, svc, req.Session); err != nil {
			return PentestResult{}, fmt.Errorf("failed to import session: %w", err)
		}
		// The imported session lives under the snapshot's own id — coincides
		// with the workflow id on a real continue-as-new, but deriving it from
		// the snapshot cannot silently create an empty session instead.
		sessionID = req.Session.SessionID
	}

	// A run resumed across continue-as-new seeds no user message: the imported
	// history already ends at a tool-response boundary, which is the turn the
	// model picks up. Everything else opens with the question.
	var msg *genai.Content
	if req.Session == nil {
		msg = genai.NewContentFromText(cmp.Or(req.Question, req.Task), genai.RoleUser)
	}

	var res PentestResult
	emitter := newEventEmitter(ctx, req)
	attempts := req.BarrierRetries
	for {
		// Guards are rebuilt per run: production's repeat detector and
		// iteration counter are per-chain state, and a nudged run must start
		// them over. The runner rebuilds with them because callbacks are bound
		// at agent construction; the session service survives and keeps the
		// conversation across runs.
		guards := newRunGuards(req.MaxIterations)
		pentester, err := NewPentesterAgent(mdl, tools, req.Instruction, nil, guards)
		if err != nil {
			return PentestResult{}, err
		}
		r, err := runner.New(runner.Config{
			AppName:           appName,
			Agent:             pentester,
			SessionService:    svc,
			AutoCreateSession: true,
		})
		if err != nil {
			return PentestResult{}, err
		}

		var events []*session.Event
		// MaxLLMCalls -1 disables ADK's own budget: our iteration cap owns the
		// limit, and the default path reads an env var, which is a
		// nondeterminism hazard in workflow code.
		for ev, err := range r.Run(adkCtx, userID, sessionID, msg, agent.RunConfig{MaxLLMCalls: -1}) {
			if err != nil {
				return res, err
			}
			if ev == nil {
				continue
			}
			events = append(events, ev)
			res.EventCount++
			emitter.emit(ev)
			if ev.Content == nil {
				continue
			}
			for _, p := range ev.Content.Parts {
				if p != nil && p.Text != "" {
					res.Answer = p.Text
				}
			}
			// The safe continue-as-new boundary is a completed function
			// response: call/response pairs are fully recorded, so the exported
			// history has no dangling call the next run would choke on. The
			// hack_result response is the exception — the run ends on it, so
			// continuing as new there would replay the barrier forever.
			if hasFunctionResponse(ev) && !hasBarrierResponse(ev) && workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
				snap, err := googleadk.ExportSession(adkCtx, svc, appName, userID, sessionID)
				if err != nil {
					return res, err
				}
				req.Session = snap
				req.BarrierRetries = attempts
				return res, workflow.NewContinueAsNewError(ctx, PentesterWorkflow, req)
			}
		}

		if hr := extractHackResult(events); hr != nil {
			res.HackResult = hr
		}

		pending := googleadk.PendingConfirmations(events)
		if len(pending) > 0 {
			var decision googleadk.ConfirmationDecision
			workflow.GetSignalChannel(ctx, googleadk.ConfirmationSignalName).Receive(ctx, &decision)
			if decision.FunctionCallID == "" {
				decision.FunctionCallID = pending[0].FunctionCallID
			}
			msg = googleadk.ConfirmationResponse(decision)
			continue
		}

		if res.HackResult != nil {
			return res, nil
		}

		attempts++
		if attempts >= maxBarrierRuns {
			res.HackResult = &HackResult{
				Success: false,
				Result:  fmt.Sprintf("agent ended without hack_result after %d turns", attempts),
			}
			return res, nil
		}
		msg = genai.NewContentFromText(missingBarrierNudge, genai.RoleUser)
	}
}

// hasFunctionResponse reports whether the event carries a completed tool
// result — the only event shape that closes a call/response pair.
func hasFunctionResponse(ev *session.Event) bool {
	if ev == nil || ev.Content == nil {
		return false
	}
	for _, p := range ev.Content.Parts {
		if p != nil && p.FunctionResponse != nil {
			return true
		}
	}
	return false
}

// hasBarrierResponse reports whether the event carries hack_result's function
// response — the moment the run terminates, not a continue-as-new boundary.
func hasBarrierResponse(ev *session.Event) bool {
	if ev == nil || ev.Content == nil {
		return false
	}
	for _, p := range ev.Content.Parts {
		if p != nil && p.FunctionResponse != nil && p.FunctionResponse.Name == HackResultToolName {
			return true
		}
	}
	return false
}
