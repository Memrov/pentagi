package adk

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// optsRecordingSandbox captures the ExecOptions each call ran under.
type optsRecordingSandbox struct {
	inner *fakeSandbox
	seen  []ExecOptions
}

func (o *optsRecordingSandbox) Exec(ctx context.Context, argv []string, opts ExecOptions) (ExecResult, error) {
	o.seen = append(o.seen, opts)
	return o.inner.Exec(ctx, argv, opts)
}

func TestActivities_TerminalExec_ShellWrapsAndBoundsTimeout(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "uid=0"}, nil
	}}
	rec := &optsRecordingSandbox{inner: sb}
	acts := NewActivities(rec, nil, 1024, time.Minute)

	res, err := acts.TerminalExec(context.Background(), TerminalExecInput{Command: "id", Cwd: "/work", TimeoutSeconds: 10})
	require.NoError(t, err)
	assert.Equal(t, "uid=0", res.Output)

	calls := sb.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"sh", "-c", "id"}, calls[0])
	require.Len(t, rec.seen, 1)
	assert.Equal(t, "/work", rec.seen[0].Cwd)
	assert.Equal(t, 10*time.Second, rec.seen[0].Timeout)
}

func TestActivities_TerminalExec_RejectsEmptyCommandNonRetryable(t *testing.T) {
	acts := NewActivities(&fakeSandbox{}, nil, 1024, time.Minute)
	_, err := acts.TerminalExec(context.Background(), TerminalExecInput{Command: "   "})
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable())
}

func TestActivities_NucleiScan_BuildsArgvWithoutShell(t *testing.T) {
	sb := &fakeSandbox{}
	acts := NewActivities(sb, nil, 1024, time.Minute)

	_, err := acts.NucleiScan(context.Background(), NucleiScanInput{
		Target:   "https://scanme.sh",
		Severity: []string{"critical", "high"},
		Tags:     []string{"cve"},
	})
	require.NoError(t, err)

	calls := sb.calls()
	require.Len(t, calls, 1)
	argv := calls[0]
	assert.Equal(t, "nuclei", argv[0])
	assert.Contains(t, argv, "-u")
	assert.Contains(t, argv, "https://scanme.sh")
	assert.Contains(t, argv, "critical,high")
	assert.Contains(t, argv, "cve")
	for _, arg := range argv {
		assert.NotContains(t, arg, " ")
	}
}

func TestActivities_ScanTools_RejectNonHTTPTargets(t *testing.T) {
	acts := NewActivities(&fakeSandbox{}, nil, 1024, time.Minute)
	for _, target := range []string{"scanme.sh", "ftp://x", "file:///etc/passwd", ""} {
		_, err := acts.NucleiScan(context.Background(), NucleiScanInput{Target: target})
		require.Error(t, err, target)
		var appErr *temporal.ApplicationError
		require.True(t, errors.As(err, &appErr))
		assert.True(t, appErr.NonRetryable())

		_, err = acts.HttpxProbe(context.Background(), HttpxProbeInput{Target: target})
		require.Error(t, err, target)
	}
}

func TestActivities_HttpxProbe_MapsFlags(t *testing.T) {
	sb := &fakeSandbox{}
	acts := NewActivities(sb, nil, 1024, time.Minute)

	_, err := acts.HttpxProbe(context.Background(), HttpxProbeInput{
		Target: "https://scanme.sh", TechDetect: true, StatusCode: true, Title: true, FollowRedirect: true,
	})
	require.NoError(t, err)

	argv := sb.calls()[0]
	for _, flag := range []string{"-tech-detect", "-status-code", "-title", "-follow-redirects"} {
		assert.Contains(t, argv, flag)
	}
}

func TestActivities_Finish_OffloadsOversizedOutput(t *testing.T) {
	store := NewMemoryPayloadStore()
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: strings.Repeat("x", 5000)}, nil
	}}
	acts := NewActivities(sb, store, 1024, time.Minute)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(acts.TerminalExec, activity.RegisterOptions{Name: TerminalToolName})

	val, err := env.ExecuteActivity(acts.TerminalExec, TerminalExecInput{Command: "big-scan"})
	require.NoError(t, err)

	var res ToolResult
	require.NoError(t, val.Get(&res))
	assert.True(t, res.Truncated)
	assert.Len(t, res.Output, 1024)
	assert.True(t, strings.HasPrefix(res.PayloadURI, "mem://"))

	key := strings.TrimPrefix(res.PayloadURI, "mem://")
	blob, ok := store.Get(key)
	require.True(t, ok, "payload store should hold the full output under the activity-scoped key")
	assert.Len(t, blob, 5000)
}

func TestActivities_Finish_TruncatesWithoutStore(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: strings.Repeat("y", 5000)}, nil
	}}
	acts := NewActivities(sb, nil, 1024, time.Minute)

	res, err := acts.TerminalExec(context.Background(), TerminalExecInput{Command: "big"})
	require.NoError(t, err)
	assert.True(t, res.Truncated)
	assert.Empty(t, res.PayloadURI)
	assert.Len(t, res.Output, 1024)
}

func TestActivities_Exec_PropagatesSandboxError(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{}, errors.New("container gone")
	}}
	acts := NewActivities(sb, nil, 1024, time.Minute)

	_, err := acts.TerminalExec(context.Background(), TerminalExecInput{Command: "id"})
	assert.ErrorContains(t, err, "container gone")
}
