package adk

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	return a.exec(ctx, []string{"sh", "-c", in.Command}, ExecOptions{
		Cwd:     in.Cwd,
		Timeout: boundedTimeout(in.TimeoutSeconds, a.defTimeout),
	})
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
	return a.exec(ctx, argv, ExecOptions{Timeout: a.defTimeout})
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
	return a.exec(ctx, argv, ExecOptions{Timeout: a.defTimeout})
}

// exec is the shared attempt tail every tool takes: heartbeat for the
// activity's whole duration, the sandboxed call, then output capping. The exec
// timeout is clamped to the remaining activity budget — a command that would
// outlive StartToClose gets its graceful TimedOut result instead of being torn
// down mid-write and retried into the same wall.
func (a *Activities) exec(ctx context.Context, argv []string, opts ExecOptions) (ToolResult, error) {
	defer startHeartbeat(ctx)()
	if activity.IsActivity(ctx) {
		if d := activity.GetInfo(ctx).Deadline; !d.IsZero() {
			if remaining := time.Until(d) - 2*toolHeartbeatInterval; opts.Timeout <= 0 || remaining < opts.Timeout {
				opts.Timeout = remaining
			}
		}
	}
	res, err := a.sandbox.Exec(ctx, argv, opts)
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
	out.Output = strings.Clone(out.Output[:a.threshold])
	if a.payload == nil {
		return out
	}
	uri, err := a.payload.Put(ctx, payloadKey(ctx), []byte(res.Output))
	if err != nil {
		if activity.IsActivity(ctx) {
			activity.GetLogger(ctx).Warn("payload offload failed; truncating", "error", err)
		}
		return out
	}
	out.PayloadURI = uri
	return out
}

// Outside an activity a fresh key keeps every oversized result distinct
// instead of overwriting one shared "" key.
func payloadKey(ctx context.Context) string {
	if !activity.IsActivity(ctx) {
		return uuid.NewString() + ".log"
	}
	if info := activity.GetInfo(ctx); info.ActivityID != "" {
		return fmt.Sprintf("%s/%s-attempt%d.log", info.WorkflowExecution.ID, info.ActivityID, info.Attempt)
	}
	return uuid.NewString() + ".log"
}

// startHeartbeat pings the Temporal service while the sandboxed command runs,
// carrying the elapsed time so a stuck activity shows live progress in the UI.
// The returned func stops and joins the ticker goroutine; it is safe to defer
// and safe to call twice. Outside an activity (direct mode, unit tests) it is
// a no-op.
func startHeartbeat(ctx context.Context) func() {
	if !activity.IsActivity(ctx) {
		return func() {}
	}
	start := time.Now()
	done, stopped := make(chan struct{}), make(chan struct{})
	var once sync.Once
	go func() {
		defer close(stopped)
		activity.RecordHeartbeat(ctx, map[string]any{"elapsed_s": 0})
		ticker := time.NewTicker(toolHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, map[string]any{"elapsed_s": int(time.Since(start).Seconds())})
			}
		}
	}()
	return func() { once.Do(func() { close(done); <-stopped }) }
}

// Compare before multiplying: a model-supplied count past MaxInt64/time.Second
// would wrap the duration negative and silently disable the exec timeout.
func boundedTimeout(seconds int, fallback time.Duration) time.Duration {
	if seconds > 0 && time.Duration(seconds) <= fallback/time.Second {
		return time.Duration(seconds) * time.Second
	}
	return fallback
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
