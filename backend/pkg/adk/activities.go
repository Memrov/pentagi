package adk

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

const (
	// toolHeartbeatInterval paces RecordHeartbeat from each activity;
	// toolHeartbeatTimeout is the workflow-side bound it must fit inside, so a
	// dead worker is detected in tens of seconds instead of waiting out the
	// full StartToCloseTimeout.
	toolHeartbeatInterval = 5 * time.Second
	toolHeartbeatTimeout  = 30 * time.Second
)

// Activities are the worker-side implementations behind the durable tools;
// every method executes inside one sandboxed activity attempt.
type Activities struct {
	sandbox    Sandbox
	payload    PayloadStore
	threshold  int
	defTimeout time.Duration
}

func NewActivities(sandbox Sandbox, payload PayloadStore, threshold int, defTimeout time.Duration) *Activities {
	if threshold <= 0 {
		threshold = 16 << 10
	}
	return &Activities{
		sandbox:    sandbox,
		payload:    payload,
		threshold:  threshold,
		defTimeout: defTimeout,
	}
}

// Register exposes each tool under the same name ActivityAsTool dispatches.
func (a *Activities) Register(r interface {
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}) {
	r.RegisterActivityWithOptions(a.TerminalExec, activity.RegisterOptions{Name: TerminalToolName})
	r.RegisterActivityWithOptions(a.NucleiScan, activity.RegisterOptions{Name: NucleiToolName})
	r.RegisterActivityWithOptions(a.HttpxProbe, activity.RegisterOptions{Name: HttpxToolName})
}

func (a *Activities) TerminalExec(ctx context.Context, in TerminalExecInput) (ToolResult, error) {
	if strings.TrimSpace(in.Command) == "" {
		return ToolResult{}, nonRetryable("terminal command is required")
	}
	defer heartbeat(ctx)()
	res, err := a.sandbox.Exec(ctx, []string{"sh", "-c", in.Command}, ExecOptions{
		Cwd:     in.Cwd,
		Timeout: boundedTimeout(in.TimeoutSeconds, a.defTimeout),
	})
	if err != nil {
		return ToolResult{}, err
	}
	return a.finish(ctx, res), nil
}

func (a *Activities) NucleiScan(ctx context.Context, in NucleiScanInput) (ToolResult, error) {
	if err := validateTarget(in.Target); err != nil {
		return ToolResult{}, err
	}
	argv := []string{"nuclei", "-u", in.Target, "-jsonl", "-silent", "-nc"}
	if len(in.Severity) > 0 {
		argv = append(argv, "-severity", strings.Join(in.Severity, ","))
	}
	if len(in.Tags) > 0 {
		argv = append(argv, "-tags", strings.Join(in.Tags, ","))
	}
	defer heartbeat(ctx)()
	res, err := a.sandbox.Exec(ctx, argv, ExecOptions{Timeout: a.defTimeout})
	if err != nil {
		return ToolResult{}, err
	}
	return a.finish(ctx, res), nil
}

func (a *Activities) HttpxProbe(ctx context.Context, in HttpxProbeInput) (ToolResult, error) {
	if err := validateTarget(in.Target); err != nil {
		return ToolResult{}, err
	}
	argv := []string{"httpx", "-u", in.Target, "-json", "-nc"}
	if in.TechDetect {
		argv = append(argv, "-tech-detect")
	}
	if in.StatusCode {
		argv = append(argv, "-status-code")
	}
	if in.Title {
		argv = append(argv, "-title")
	}
	if in.FollowRedirect {
		argv = append(argv, "-follow-redirects")
	}
	defer heartbeat(ctx)()
	res, err := a.sandbox.Exec(ctx, argv, ExecOptions{Timeout: a.defTimeout})
	if err != nil {
		return ToolResult{}, err
	}
	return a.finish(ctx, res), nil
}

// finish caps what the model sees: output over the threshold is handed to the
// payload store when one is configured, otherwise hard-truncated.
func (a *Activities) finish(ctx context.Context, res ExecResult) ToolResult {
	out := ToolResult{
		Output:    res.Output,
		ExitCode:  res.ExitCode,
		TimedOut:  res.TimedOut,
		Truncated: res.Truncated,
	}
	if len(out.Output) <= a.threshold {
		return out
	}
	out.Truncated = true
	if a.payload == nil {
		out.Output = out.Output[:a.threshold]
		return out
	}
	uri, err := a.payload.Put(ctx, payloadKey(ctx), []byte(out.Output))
	if err != nil {
		out.Output = out.Output[:a.threshold]
		return out
	}
	out.PayloadURI = uri
	out.Output = out.Output[:a.threshold]
	return out
}

func payloadKey(ctx context.Context) string {
	if !activity.IsActivity(ctx) {
		return ""
	}
	if info := activity.GetInfo(ctx); info.ActivityID != "" {
		return fmt.Sprintf("%s/%s-attempt%d.log", info.WorkflowExecution.ID, info.ActivityID, info.Attempt)
	}
	return ""
}

// heartbeat pings the Temporal service while the sandboxed command runs; the
// returned func stops the ticker and is meant to be deferred. Outside an
// activity (direct mode, unit tests) it is a no-op.
func heartbeat(ctx context.Context) func() {
	if !activity.IsActivity(ctx) {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(toolHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return func() { close(done) }
}

func boundedTimeout(seconds int, def time.Duration) time.Duration {
	if seconds > 0 {
		if d := time.Duration(seconds) * time.Second; d < def {
			return d
		}
	}
	return def
}

func validateTarget(target string) error {
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		return nonRetryable("target must be an http(s) URL, got %q", target)
	}
	return nil
}

func nonRetryable(format string, args ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, args...), "Validation", nil)
}
