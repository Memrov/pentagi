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

// Terminal timeout bounds, kept verbatim from pkg/tools/terminal.go: an
// explicit timeout earns the extra grace on top, and anything non-positive or
// past the bound lands on the configured default plus the same grace.
const (
	maxExplicitExecCommandTimeout = 3 * time.Hour
	defaultExtraExecTimeout       = 5 * time.Second
)

// The styling the UI terminal replays, verbatim from pkg/tools/terminal.go:
// commands are framed cyan, command output green.
const (
	ansiColorInputCmd  = "\033[96m"
	ansiColorSystemMsg = "\033[92m"
	ansiColorReset     = "\033[0m"
	ansiLineEnd        = "\r\n"
	defaultWorkDir     = "/work"
)

// Activities are the worker-side implementations behind the durable tools;
// every method executes inside one sandboxed activity attempt.
type Activities struct {
	sandbox    Sandbox
	payload    PayloadStore
	threshold  int
	defTimeout time.Duration
	// FlowBound wraps terminal argv in docker.FlowCommand so the flow's
	// stop-command sweep can kill what this workflow started; unset keeps
	// every command unowned via docker.SandboxCommand.
	FlowBound bool
	// SandboxFor resolves the sandbox for an activity attempt whose workflow
	// id parses as a FlowWorkflow child; nil — or an unparseable id — keeps
	// the shared sandbox, which is what binds production tools to the flow's
	// primary container without this package knowing flow rows.
	SandboxFor func(ctx context.Context, flowID int64) (Sandbox, error)
	// TermLogFor resolves the flow's terminal-log sink on the same workflow-id
	// key; nil keeps terminal calls unlogged, which is the CLI path.
	TermLogFor func(ctx context.Context, flowID int64) (TermLogSink, error)
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
// payload_read is registered unconditionally — the toolset only advertises it
// when a store exists, and a registered-but-unadvertised activity is inert.
func (a *Activities) Register(r interface {
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}) {
	r.RegisterActivityWithOptions(a.TerminalExec, activity.RegisterOptions{Name: TerminalToolName})
	r.RegisterActivityWithOptions(a.FileExec, activity.RegisterOptions{Name: FileToolName})
	r.RegisterActivityWithOptions(a.NucleiScan, activity.RegisterOptions{Name: NucleiToolName})
	r.RegisterActivityWithOptions(a.HttpxProbe, activity.RegisterOptions{Name: HttpxToolName})
	r.RegisterActivityWithOptions(a.PayloadRead, activity.RegisterOptions{Name: PayloadReadToolName})
}

func (a *Activities) TerminalExec(ctx context.Context, in TerminalExecInput) (ToolResult, error) {
	if strings.TrimSpace(in.Input) == "" {
		return ToolResult{}, nonRetryable("terminal command is required")
	}
	// Production normalises twice — once in the handler, once in the executor
	// — because the executor is also a barrier target for tools that skip the
	// handler. Here the path is terminal-only, but the same two steps keep the
	// arithmetic an exact copy.
	timeout := normalizeExecTimeout(time.Duration(in.Timeout)*time.Second, a.defTimeout)
	if timeout > 0 {
		timeout += defaultExtraExecTimeout
	}
	timeout = normalizeExecTimeout(timeout, a.defTimeout)

	argv := sandboxCommand(in.Input)
	if a.FlowBound {
		argv = flowCommand(in.Input)
	}
	tl := a.termLog(ctx)
	if tl != nil {
		a.putTermLog(ctx, tl, "stdin", stdinPrompt(in.Cwd, in.Input))
	}
	res, err := a.execSandboxed(ctx, argv, ExecOptions{
		Cwd:     in.Cwd,
		Timeout: timeout,
		Detach:  in.Detach,
	})
	if err != nil {
		return ToolResult{}, err
	}
	out := a.terminalResult(ctx, res, timeout)
	if tl != nil && out.Output != "" {
		a.putTermLog(ctx, tl, "stdout",
			fmt.Sprintf("%s%s%s%s", ansiColorSystemMsg, out.Output, ansiColorReset, ansiLineEnd))
	}
	return out, nil
}

// stdinPrompt renders the line the UI terminal shows before the command's
// output — the same "cwd $ command" frame the production terminal writes into
// the termlog.
func stdinPrompt(cwd, command string) string {
	if cwd == "" {
		cwd = defaultWorkDir
	}
	return fmt.Sprintf("%s $ %s%s%s%s", cwd, ansiColorInputCmd, command, ansiColorReset, ansiLineEnd)
}

// termLog resolves the per-flow sink the same way execSandboxed resolves the
// per-flow sandbox: through the activity attempt's workflow id. A sink error
// downgrades the exec's observability, so it is logged and skipped.
func (a *Activities) termLog(ctx context.Context) TermLogSink {
	if a.TermLogFor == nil || !activity.IsActivity(ctx) {
		return nil
	}
	flowID, ok := FlowIDFromWorkflowID(activity.GetInfo(ctx).WorkflowExecution.ID)
	if !ok {
		return nil
	}
	tl, err := a.TermLogFor(ctx, flowID)
	if err != nil {
		activity.GetLogger(ctx).Warn("terminal log sink resolution failed", "error", err)
		return nil
	}
	return tl
}

// putTermLog is the write tail: a row the flow's termlog worker rejects does
// not fail the command the row described.
func (a *Activities) putTermLog(ctx context.Context, tl TermLogSink, direction, text string) {
	if err := tl.TermLog(ctx, direction, text); err != nil {
		activity.GetLogger(ctx).Warn("terminal log write failed", "direction", direction, "error", err)
	}
}

// terminalResult applies the production formatting contract: a started
// detached command gets the "still running" text, a timed-out command the
// error with the detach/timeout HINT (returned whole — capping it could cut
// the HINT off the tail), and a finished command the output-plus-exit-code
// text capped like any oversized result.
func (a *Activities) terminalResult(ctx context.Context, res ExecResult, timeout time.Duration) ToolResult {
	if res.StillRunning {
		return ToolResult{Output: fmt.Sprintf(
			"Command started in background with timeout %s (still running)", timeout)}
	}
	if res.TimedOut {
		return ToolResult{Output: commandTimeoutText(timeout, res.Output), TimedOut: true}
	}
	out := ToolResult{
		Output:    execOutputString(res),
		ExitCode:  res.ExitCode,
		Truncated: res.Truncated,
	}
	display, uri, capped := a.capOutput(ctx, out.Output)
	out.Output, out.PayloadURI = display, uri
	out.Truncated = out.Truncated || capped
	return out
}

// commandTimeoutText is the production timeout error verbatim; the trailing
// HINT is what teaches the model detach or the shell timeout wrapper.
func commandTimeoutText(timeout time.Duration, partial string) string {
	suggestedTimeout := max(int(timeout.Seconds())-10, 10)
	return fmt.Sprintf(
		"command execution timeout (%v). Partial output: %s. "+
			"HINT: If this is an interactive command (shell/REPL/listener), use detach=true. "+
			"For long batch commands, wrap with shell timeout utility: 'timeout %d <command>' to ensure clean completion",
		context.DeadlineExceeded,
		truncateString(partial, resultSizeLimit),
		suggestedTimeout,
	)
}

// normalizeExecTimeout replicates terminal.normalizeExecTimeout: the accepted
// bound is the configured default plus its grace, so only a requested timeout
// inside (0, bound] survives; the configured default itself caps at 3h.
func normalizeExecTimeout(timeout, defTimeout time.Duration) time.Duration {
	if defTimeout <= 0 || defTimeout > maxExplicitExecCommandTimeout {
		defTimeout = maxExplicitExecCommandTimeout
	}
	bound := defTimeout + defaultExtraExecTimeout
	if timeout > 0 && timeout <= bound {
		return timeout
	}
	return bound
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

// execSandboxed is the shared attempt tail every tool takes: heartbeat for
// the activity's whole duration, then the sandboxed call. The exec timeout is
// clamped to the remaining activity budget — a command that would outlive
// StartToClose gets its graceful TimedOut result instead of being torn down
// mid-write and retried into the same wall. Detached commands are exempt:
// their point is to outlive the call that started them.
func (a *Activities) execSandboxed(ctx context.Context, argv []string, opts ExecOptions) (ExecResult, error) {
	defer startHeartbeat(ctx)()
	sb := a.sandbox
	if activity.IsActivity(ctx) && !opts.Detach {
		if d := activity.GetInfo(ctx).Deadline; !d.IsZero() {
			if remaining := time.Until(d) - 2*toolHeartbeatInterval; opts.Timeout <= 0 || remaining < opts.Timeout {
				opts.Timeout = remaining
			}
		}
	}
	if a.SandboxFor != nil && activity.IsActivity(ctx) {
		if flowID, ok := FlowIDFromWorkflowID(activity.GetInfo(ctx).WorkflowExecution.ID); ok {
			resolved, err := a.SandboxFor(ctx, flowID)
			if err != nil {
				return ExecResult{}, err
			}
			sb = resolved
		}
	}
	return sb.Exec(ctx, argv, opts)
}

// exec is the generic tool tail: sandboxed call plus the unformatted output
// cap (nuclei/httpx keep their raw-output result, as before).
func (a *Activities) exec(ctx context.Context, argv []string, opts ExecOptions) (ToolResult, error) {
	res, err := a.execSandboxed(ctx, argv, opts)
	if err != nil {
		return ToolResult{}, err
	}
	out := ToolResult{
		ExitCode:  res.ExitCode,
		TimedOut:  res.TimedOut,
		Truncated: res.Truncated,
	}
	out.Output, out.PayloadURI, out.Truncated = a.capOutput(ctx, res.Output)
	out.Truncated = out.Truncated || res.Truncated
	return out, nil
}

// payloadReadMaxBytes bounds one fetch window: both the default when the
// caller asks for nothing and the cap when it asks for too much — a tool
// result stays a result, not a blob dump.
const payloadReadMaxBytes = 16 << 10

// PayloadRead is the activity behind payload_read: the model hands back the
// URI a capped tool result carried in its payload_uri field and gets a window
// of the stored blob. Fetch failures are reported in the content — the
// swallow-and-report convention — never as an activity error.
func (a *Activities) PayloadRead(ctx context.Context, in PayloadReadInput) (PayloadReadOutput, error) {
	if a.payload == nil {
		return PayloadReadOutput{Content: "payload store is not configured on this worker"}, nil
	}
	uri := strings.TrimSpace(in.URI)
	if uri == "" {
		return PayloadReadOutput{Content: "uri is required"}, nil
	}
	data, err := a.payload.Get(ctx, uri)
	if err != nil {
		return PayloadReadOutput{Content: fmt.Sprintf("failed to fetch payload: %v", err)}, nil
	}
	return windowPayload(data, in.OffsetBytes, in.MaxBytes), nil
}

// windowPayload slices one read out of the blob; an offset past the end is an
// empty, untruncated answer rather than an error, so a model that follows a
// stale next_offset learns there is nothing left.
func windowPayload(data []byte, offset, maxBytes int) PayloadReadOutput {
	if offset < 0 {
		offset = 0
	}
	if maxBytes <= 0 || maxBytes > payloadReadMaxBytes {
		maxBytes = payloadReadMaxBytes
	}
	total := len(data)
	if offset > total {
		offset = total
	}
	end := min(offset+maxBytes, total)
	return PayloadReadOutput{
		Content:    string(data[offset:end]),
		TotalBytes: total,
		NextOffset: end,
		Truncated:  end < total,
	}
}

// capOutput caps what the model sees: output over the threshold is handed to
// the payload store when one is configured, otherwise hard-truncated.
func (a *Activities) capOutput(ctx context.Context, full string) (display, uri string, capped bool) {
	if len(full) <= a.threshold {
		return full, "", false
	}
	display = strings.Clone(full[:a.threshold])
	if a.payload == nil {
		return display, "", true
	}
	u, err := a.payload.Put(ctx, payloadKey(ctx), []byte(full))
	if err != nil {
		if activity.IsActivity(ctx) {
			activity.GetLogger(ctx).Warn("payload offload failed; truncating", "error", err)
		}
		return display, "", true
	}
	return display, u, true
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

func validateTarget(target string) error {
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		return nonRetryable("target must be an http(s) URL, got %q", target)
	}
	return nil
}

func nonRetryable(format string, args ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, args...), "Validation", nil)
}
