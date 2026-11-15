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
}

type ExecResult struct {
	Output    string
	ExitCode  int
	TimedOut  bool
	Truncated bool
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

// DockerExecClient is the exec slice of docker.DockerClient, so the PentAGI client
// satisfies it; mobyExec adapts the bare moby client to the same shape.
type DockerExecClient interface {
	ContainerExecCreate(ctx context.Context, container string, config client.ExecCreateOptions) (client.ExecCreateResult, error)
	ContainerExecAttach(ctx context.Context, execID string, config client.ExecAttachOptions) (client.HijackedResponse, error)
	ContainerExecInspect(ctx context.Context, execID string) (client.ExecInspectResult, error)
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
	defer resp.Close()

	out := &cappedBuffer{limit: maxExecOutput}
	done := make(chan struct{})
	var copyErr error
	go func() {
		_, copyErr = io.Copy(out, resp.Reader)
		close(done)
	}()

	res := ExecResult{ExitCode: -1}
	select {
	case <-done:
		if copyErr != nil && !errors.Is(copyErr, io.EOF) {
			return res, fmt.Errorf("failed to copy output: %w", copyErr)
		}
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
	case <-execCtx.Done():
		resp.Close()
		// The copy goroutine exits on the closed connection; its error is
		// expected here, only the partial output matters.
		select {
		case <-done:
		case <-time.After(dockerJoinTimeout):
			logActivity(ctx).Warn("docker output copy did not stop after stream close")
		}
		killCtx, killCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		s.killExec(killCtx, marker)
		killCancel()
		res.Output = out.buf.String()
		res.Truncated = out.truncated
		if !errors.Is(context.Cause(execCtx), errToolTimeout) {
			return res, ctx.Err()
		}
		res.TimedOut = true
		return res, nil
	}

	res.Output = out.buf.String()
	res.Truncated = out.truncated
	inspect, err := s.Client.ContainerExecInspect(execCtx, createResp.ID)
	if err != nil {
		return res, fmt.Errorf("failed to inspect exec process: %w", err)
	}
	res.ExitCode = inspect.ExitCode
	return res, nil
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
