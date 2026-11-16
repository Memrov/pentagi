package adk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"go.temporal.io/sdk/activity"
)

// maxExecOutput bounds captured command output; anything past it is discarded
// and marked truncated rather than growing the activity result unboundedly.
const maxExecOutput = 4 << 20 // 4 MiB

// dockerJoinTimeout bounds the wait for the copy goroutine once the hijacked
// stream is closed; a stuck copy must not deadlock the activity.
const dockerJoinTimeout = 5 * time.Second

// defaultQuickCheckTimeout is how long a detached command gets to die early
// before the started verdict is returned — the production terminal's 500ms.
const defaultQuickCheckTimeout = 500 * time.Millisecond

// errToolTimeout distinguishes the tool's own deadline from a parent cancel:
// only the former produces the graceful TimedOut result.
var errToolTimeout = errors.New("tool timeout")

// Spawned tools see only what they need. The worker's environment carries the
// model and Temporal credentials, and an inherited env would let any spawned
// command read them back into workflow history.
var sandboxAllowEnv = []string{"PATH=", "HOME=", "LANG=", "LC_ALL=", "TERM=", "TMPDIR=", "USER=", "SHELL="}

func sandboxEnv() []string {
	env := make([]string, 0, len(sandboxAllowEnv))
	for _, e := range os.Environ() {
		for _, p := range sandboxAllowEnv {
			if strings.HasPrefix(e, p) {
				env = append(env, e)
				break
			}
		}
	}
	return env
}

type ExecOptions struct {
	Cwd     string
	Timeout time.Duration
	// Detach asks for the started-not-finished verdict: the sandbox starts the
	// command on a cancellation-proof context, gives it a short grace to die
	// early, then reports StillRunning instead of waiting it out.
	Detach bool
}

type ExecResult struct {
	Output    string
	ExitCode  int
	TimedOut  bool
	Truncated bool
	// StillRunning marks a detached command that survived its early grace;
	// the command may still be running after the call returns.
	StillRunning bool
	// ContainerStopped marks a command whose exit above 128 coincided with the
	// sandbox container dying — the interrupted marker's trigger.
	ContainerStopped bool
}

// Sandbox runs one command line. argv is passed to the kernel without a shell,
// so callers add "sh", "-c" themselves when they want shell semantics.
type Sandbox interface {
	Exec(ctx context.Context, argv []string, opts ExecOptions) (ExecResult, error)
}

// cappedBuffer keeps the first n bytes and reports whether anything was dropped.
type cappedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	n := min(len(p), b.limit-b.buf.Len())
	_, _ = b.buf.Write(p[:n])
	if n < len(p) {
		b.truncated = true
	}
	return len(p), nil
}

// LocalSandbox execs on the host: development and tests only, there is no
// isolation. Commands run in their own process group so cancellation kills the
// whole tree, not just the direct child. Production wiring must hand the tools
// a DockerSandbox bound to the flow's worker container.
type LocalSandbox struct{}

func (LocalSandbox) Exec(ctx context.Context, argv []string, opts ExecOptions) (ExecResult, error) {
	if len(argv) == 0 {
		return ExecResult{}, nonRetryable("empty argv")
	}
	if opts.Detach {
		return localDetach(ctx, argv, opts)
	}
	execCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeoutCause(ctx, opts.Timeout, errToolTimeout)
		defer cancel()
	}

	cmd := exec.CommandContext(execCtx, argv[0], argv[1:]...)
	cmd.Env = sandboxEnv()
	setPgid(cmd)
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	out := &cappedBuffer{limit: maxExecOutput}
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	res := ExecResult{Output: out.buf.String(), Truncated: out.truncated}
	if cmd.ProcessState != nil {
		res.ExitCode = cmd.ProcessState.ExitCode()
	}
	if err := context.Cause(execCtx); err != nil {
		if errors.Is(err, errToolTimeout) {
			res.TimedOut = true
			return res, nil
		}
		return res, ctx.Err()
	}
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		if errors.Is(err, exec.ErrNotFound) {
			return res, nonRetryable("executable not found: %s", argv[0])
		}
		return res, fmt.Errorf("failed to run %q: %w", argv[0], err)
	}
	return res, nil
}

// localDetach starts the command and returns at once: the caller gets the
// started verdict while a detached context — with its own timeout — keeps the
// process alive past a cancelled call, matching the production detach
// semantics. Output is discarded by contract (detached calls capture none).
func localDetach(ctx context.Context, argv []string, opts ExecOptions) (ExecResult, error) {
	detached := context.WithoutCancel(ctx)
	cancel := context.CancelFunc(func() {})
	if opts.Timeout > 0 {
		detached, cancel = context.WithTimeout(detached, opts.Timeout)
	}

	cmd := exec.CommandContext(detached, argv[0], argv[1:]...)
	cmd.Env = sandboxEnv()
	setPgid(cmd)
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		cancel()
		if errors.Is(err, exec.ErrNotFound) {
			return ExecResult{}, nonRetryable("executable not found: %s", argv[0])
		}
		return ExecResult{}, fmt.Errorf("failed to run %q: %w", argv[0], err)
	}

	go func() { _ = cmd.Wait(); cancel() }()
	return ExecResult{StillRunning: true}, nil
}

// DockerExecClient is the exec slice of docker.DockerClient, so the PentAGI client
// satisfies it; mobyExec adapts the bare moby client to the same shape.
type DockerExecClient interface {
	ContainerExecCreate(ctx context.Context, container string, config client.ExecCreateOptions) (client.ExecCreateResult, error)
	ContainerExecAttach(ctx context.Context, execID string, config client.ExecAttachOptions) (client.HijackedResponse, error)
	ContainerExecInspect(ctx context.Context, execID string) (client.ExecInspectResult, error)
	ContainerInspect(ctx context.Context, container string, options client.ContainerInspectOptions) (client.ContainerInspectResult, error)
}

type mobyExec struct{ cli *client.Client }

func (m mobyExec) ContainerExecCreate(ctx context.Context, container string, opts client.ExecCreateOptions) (client.ExecCreateResult, error) {
	return m.cli.ExecCreate(ctx, container, opts)
}

func (m mobyExec) ContainerExecAttach(ctx context.Context, execID string, opts client.ExecAttachOptions) (client.HijackedResponse, error) {
	res, err := m.cli.ExecAttach(ctx, execID, opts)
	return res.HijackedResponse, err
}

func (m mobyExec) ContainerExecInspect(ctx context.Context, execID string) (client.ExecInspectResult, error) {
	return m.cli.ExecInspect(ctx, execID, client.ExecInspectOptions{})
}

func (m mobyExec) ContainerInspect(ctx context.Context, container string, opts client.ContainerInspectOptions) (client.ContainerInspectResult, error) {
	return m.cli.ContainerInspect(ctx, container, opts)
}

// DockerSandbox execs inside a running container. The exec wrapper records its
// PID in a marker file so cancellation can kill the whole process group —
// closing the attach stream only detaches the client, the command keeps
// running in the container.
type DockerSandbox struct {
	Client    DockerExecClient
	Container string
}

func NewDockerSandbox(container string) (*DockerSandbox, error) {
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize docker client: %w", err)
	}
	return &DockerSandbox{Client: mobyExec{cli: cli}, Container: container}, nil
}

func (s *DockerSandbox) Exec(ctx context.Context, argv []string, opts ExecOptions) (ExecResult, error) {
	if len(argv) == 0 {
		return ExecResult{}, nonRetryable("empty argv")
	}
	if s.Client == nil || s.Container == "" {
		return ExecResult{}, nonRetryable("docker sandbox requires a client and container")
	}
	execCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeoutCause(ctx, opts.Timeout, errToolTimeout)
		defer cancel()
	}

	marker := "/tmp/.adk-exec-" + uuid.NewString()
	createResp, err := s.Client.ContainerExecCreate(execCtx, s.Container, client.ExecCreateOptions{
		Cmd: append([]string{
			"sh", "-c", "echo $$ > " + marker + " 2>/dev/null || true; exec \"$@\"", "sh",
		}, argv...),
		AttachStdout: true,
		AttachStderr: true,
		WorkingDir:   opts.Cwd,
		TTY:          true,
	})
	if err != nil {
		return ExecResult{}, fmt.Errorf("failed to create exec process: %w", err)
	}

	resp, err := s.Client.ContainerExecAttach(execCtx, createResp.ID, client.ExecAttachOptions{TTY: true})
	if err != nil {
		return ExecResult{}, fmt.Errorf("failed to attach to exec process: %w", err)
	}

	st := &execStream{
		resp: resp,
		out:  &cappedBuffer{limit: maxExecOutput},
		done: make(chan struct{}),
	}
	go func() {
		_, st.copyErr = io.Copy(st.out, resp.Reader)
		close(st.done)
	}()

	if opts.Detach {
		return s.awaitDetach(ctx, createResp.ID, marker, st, opts.Timeout)
	}
	defer resp.Close()

	return s.awaitExec(ctx, execCtx, createResp.ID, marker, st)
}

// execStream bundles the attach side of one docker exec: the hijacked stream,
// the capped sink the copy goroutine feeds, and the signal that copy ended —
// shared by the blocking wait and the detached quick check.
type execStream struct {
	resp    client.HijackedResponse
	out     *cappedBuffer
	done    chan struct{}
	copyErr error
}

// awaitExec is the blocking wait shared by the foreground and the detached
// waiter's early-exit path: copy to EOF or the exec context ends, then inspect
// for the exit code. The ctx/execCtx split mirrors the caller's — execCtx
// carries the tool deadline, ctx the parent's cancellation.
func (s *DockerSandbox) awaitExec(ctx, execCtx context.Context, execID, marker string, st *execStream) (ExecResult, error) {
	res := ExecResult{ExitCode: -1}
	select {
	case <-st.done:
		if st.copyErr != nil && !errors.Is(st.copyErr, io.EOF) {
			return res, fmt.Errorf("failed to copy output: %w", st.copyErr)
		}
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
	case <-execCtx.Done():
		st.resp.Close()
		// The copy goroutine exits on the closed connection; its error is
		// expected here, only the partial output matters.
		select {
		case <-st.done:
		case <-time.After(dockerJoinTimeout):
			logActivity(ctx).Warn("docker output copy did not stop after stream close")
		}
		killCtx, killCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		s.killExec(killCtx, marker)
		killCancel()
		res.Output = st.out.buf.String()
		res.Truncated = st.out.truncated
		if !errors.Is(context.Cause(execCtx), errToolTimeout) {
			return res, ctx.Err()
		}
		res.TimedOut = true
		return res, nil
	}

	res.Output = st.out.buf.String()
	res.Truncated = st.out.truncated
	inspect, err := s.Client.ContainerExecInspect(execCtx, execID)
	if err != nil {
		return res, fmt.Errorf("failed to inspect exec process: %w", err)
	}
	res.ExitCode = inspect.ExitCode
	if res.ExitCode > 128 && !s.containerRunning(execCtx) {
		res.ContainerStopped = true
	}
	return res, nil
}

// awaitDetach is the production detach path: the waiter gets a
// cancellation-proof context with its own timeout so a cancelled caller cannot
// take the command down with it, and the started verdict comes back once the
// quick check expires without an early exit. The waiter keeps streaming and
// reapplies the timeout kill in the background after we return.
func (s *DockerSandbox) awaitDetach(ctx context.Context, execID, marker string, st *execStream, timeout time.Duration) (ExecResult, error) {
	waitCtx := context.WithoutCancel(ctx)
	cancel := context.CancelFunc(func() {})
	if timeout > 0 {
		waitCtx, cancel = context.WithTimeout(waitCtx, timeout)
	}

	type outcome struct {
		res ExecResult
		err error
	}
	resultChan := make(chan outcome, 1)
	go func() {
		defer cancel()
		defer st.resp.Close()
		res, err := s.awaitExec(waitCtx, waitCtx, execID, marker, st)
		resultChan <- outcome{res: res, err: err}
	}()

	select {
	case o := <-resultChan:
		return o.res, o.err
	case <-time.After(defaultQuickCheckTimeout):
		return ExecResult{StillRunning: true}, nil
	}
}

// containerRunning gives the verdict production checks before tagging an
// exit>128 as interrupted. A daemon error other than not-found leaves the
// state unknown — and unknown counts as running, never as interrupted.
func (s *DockerSandbox) containerRunning(ctx context.Context) bool {
	res, err := s.Client.ContainerInspect(ctx, s.Container, client.ContainerInspectOptions{})
	if err != nil {
		return !cerrdefs.IsNotFound(err)
	}
	if res.Container.State == nil {
		return false
	}
	return res.Container.State.Running
}

// killExec is best-effort: group kill first (the exec'd process leads its own
// group when a TTY is allocated), direct-PID kill as fallback, then the marker
// is removed. The cleanup itself must not fail the call — the activity already
// reports a timeout.
func (s *DockerSandbox) killExec(ctx context.Context, marker string) {
	script := fmt.Sprintf(
		`kill -KILL -"$(cat %[1]s 2>/dev/null)" 2>/dev/null || kill -KILL "$(cat %[1]s 2>/dev/null)" 2>/dev/null; rm -f %[1]s`,
		marker)
	create, err := s.Client.ContainerExecCreate(ctx, s.Container, client.ExecCreateOptions{
		Cmd: []string{"sh", "-c", script},
	})
	if err != nil {
		logActivity(ctx).Warn("docker cleanup exec create failed", "error", err)
		return
	}
	resp, err := s.Client.ContainerExecAttach(ctx, create.ID, client.ExecAttachOptions{})
	if err != nil {
		logActivity(ctx).Warn("docker cleanup exec attach failed", "error", err)
		return
	}
	resp.Close()
}

// logActivity returns the activity logger inside an activity and a discard
// logger everywhere else, so sandbox cleanup failures surface where a Temporal
// UI can show them without threading a logger through the API.
func logActivity(ctx context.Context) interface {
	Warn(msg string, keyvals ...interface{})
} {
	if activity.IsActivity(ctx) {
		return activity.GetLogger(ctx)
	}
	return discardLogger{}
}

type discardLogger struct{}

func (discardLogger) Warn(string, ...interface{}) {}
