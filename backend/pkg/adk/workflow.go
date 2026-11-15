package adk

import (
	"cmp"
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
	// ToolTimeoutSeconds overrides the activity deadline for each tool call.
	ToolTimeoutSeconds int `json:"tool_timeout_seconds,omitempty"`
}

type PentestResult struct {
	Answer     string `json:"answer"`
	EventCount int    `json:"event_count"`
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
	pentester, err := NewPentesterAgent(googleadk.NewModel(modelName,
		googleadk.WithModelActivityOptions(modelActivityOptions)), tools, nil)
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

	userID := cmp.Or(req.UserID, "pentagi")
	sessionID := cmp.Or(req.SessionID, workflow.GetInfo(ctx).WorkflowExecution.ID)

	adkCtx := googleadk.NewContext(ctx)
	msg := genai.NewContentFromText(req.Task, genai.RoleUser)

	var res PentestResult
	for {
		var events []*session.Event
		for ev, err := range r.Run(adkCtx, userID, sessionID, msg, agent.RunConfig{}) {
			if err != nil {
				return res, err
			}
			if ev == nil {
				continue
			}
			events = append(events, ev)
			res.EventCount++
			if ev.Content == nil {
				continue
			}
			for _, p := range ev.Content.Parts {
				if p != nil && p.Text != "" {
					res.Answer = p.Text
				}
			}
		}

		pending := googleadk.PendingConfirmations(events)
		if len(pending) == 0 {
			return res, nil
		}

		var decision googleadk.ConfirmationDecision
		workflow.GetSignalChannel(ctx, googleadk.ConfirmationSignalName).Receive(ctx, &decision)
		if decision.FunctionCallID == "" {
			decision.FunctionCallID = pending[0].FunctionCallID
		}
		msg = googleadk.ConfirmationResponse(decision)
	}
}
