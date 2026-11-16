package adk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// optsRecordingSandbox captures the ExecOptions each call ran under.
type optsRecordingSandbox struct {
	inner *fakeSandbox
	mu    sync.Mutex
	opts  []ExecOptions
}

func (o *optsRecordingSandbox) Exec(ctx context.Context, argv []string, opts ExecOptions) (ExecResult, error) {
	o.mu.Lock()
	o.opts = append(o.opts, opts)
	o.mu.Unlock()
	return o.inner.Exec(ctx, argv, opts)
}

func (o *optsRecordingSandbox) seen() []ExecOptions {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]ExecOptions(nil), o.opts...)
}

func TestActivities_TerminalExec_WrapsAndExecutes(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "uid=0"}, nil
	}}
	rec := &optsRecordingSandbox{inner: sb}
	acts := NewActivities(rec, nil, 1024, time.Minute)

	res, err := acts.TerminalExec(context.Background(), TerminalExecInput{Input: "id", Cwd: "/work", Timeout: 10})
	require.NoError(t, err)
	assert.Equal(t, "uid=0\n[exit code: 0]", res.Output)

	calls := sb.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"sh", "-c", sandboxCommandSh, "id"}, calls[0])
	seen := rec.seen()
	require.Len(t, seen, 1)
	assert.Equal(t, "/work", seen[0].Cwd)
	assert.Equal(t, 15*time.Second, seen[0].Timeout)
	assert.False(t, seen[0].Detach)
}

// The production arithmetic: an explicit request inside the bound keeps its
// seconds plus the 5s grace; anything else lands on the configured default
// plus the grace, and the default itself is capped at 3h.
func TestActivities_TerminalExec_NormalizesTimeout(t *testing.T) {
	cases := []struct {
		name     string
		def      time.Duration
		request  int64
		wantExec time.Duration
	}{
		{"zero falls back", 10 * time.Minute, 0, 605 * time.Second},
		{"negative falls back", 10 * time.Minute, -30, 605 * time.Second},
		{"inside bound earns grace", 10 * time.Minute, 10, 15 * time.Second},
		{"at default earns grace", 10 * time.Minute, 600, 605 * time.Second},
		{"just past bound clamps", 10 * time.Minute, 606, 605 * time.Second},
		{"way past bound clamps", 10 * time.Minute, 99999, 605 * time.Second},
		{"zero default caps at ceiling", 0, 0, 3*time.Hour + 5*time.Second},
		{"over-ceiling default caps", 4 * time.Hour, 0, 3*time.Hour + 5*time.Second},
		{"request honoured under ceiling default", 0, 100, 105 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &optsRecordingSandbox{inner: &fakeSandbox{}}
			acts := NewActivities(rec, nil, 1024, tc.def)

			_, err := acts.TerminalExec(context.Background(), TerminalExecInput{Input: "id", Timeout: tc.request})
			require.NoError(t, err)

			seen := rec.seen()
			require.Len(t, seen, 1)
			assert.Equal(t, tc.wantExec, seen[0].Timeout)
		})
	}
}

func TestActivities_TerminalExec_FlowBoundMarksCommand(t *testing.T) {
	cases := []struct {
		name       string
		flowBound  bool
		wantScript string
		wantMarker bool
	}{
		{"unowned sandbox command", false, sandboxCommandSh, false},
		{"flow-owned command", true, markFlowCommandSh, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sb := &fakeSandbox{}
			acts := NewActivities(sb, nil, 1024, time.Minute)
			acts.FlowBound = tc.flowBound

			_, err := acts.TerminalExec(context.Background(), TerminalExecInput{Input: "id"})
			require.NoError(t, err)

			calls := sb.calls()
			require.Len(t, calls, 1)
			require.Len(t, calls[0], 4)
			assert.Equal(t, tc.wantScript, calls[0][2])
			assert.Equal(t, "id", calls[0][3])
			assert.Equal(t, tc.wantMarker, strings.Contains(calls[0][2], "oom_score_adj"))
		})
	}
}

func TestActivities_TerminalExec_FormatsResult(t *testing.T) {
	cases := []struct {
		name string
		res  ExecResult
		want string
	}{
		{"output and exit code", ExecResult{Output: "uid=0", ExitCode: 0}, "uid=0\n[exit code: 0]"},
		{"empty output", ExecResult{ExitCode: 1}, "[no output]\n[exit code: 1]"},
		{"interrupted with dead container", ExecResult{Output: "x", ExitCode: 137, ContainerStopped: true},
			"x\n[exit code: 137]\n[interrupted: the sandbox container stopped]"},
		{"high exit code while container lives", ExecResult{Output: "x", ExitCode: 137},
			"x\n[exit code: 137]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) { return tc.res, nil }}
			acts := NewActivities(sb, nil, 1024, time.Minute)

			res, err := acts.TerminalExec(context.Background(), TerminalExecInput{Input: "id"})
			require.NoError(t, err)
			assert.Equal(t, tc.want, res.Output)
			assert.Equal(t, tc.res.ExitCode, res.ExitCode)
		})
	}
}

func TestActivities_TerminalExec_DetachReturnsStartedString(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{StillRunning: true}, nil
	}}
	rec := &optsRecordingSandbox{inner: sb}
	acts := NewActivities(rec, nil, 1024, 10*time.Minute)

	res, err := acts.TerminalExec(context.Background(), TerminalExecInput{Input: "nc -l 4444", Detach: true, Timeout: 30})
	require.NoError(t, err)
	assert.Equal(t, "Command started in background with timeout 35s (still running)", res.Output)
	assert.False(t, res.TimedOut)

	seen := rec.seen()
	require.Len(t, seen, 1)
	assert.True(t, seen[0].Detach)
}

// The error text is part of the model contract: the trailing HINT is what the
// model reads to learn detach and the shell timeout wrapper.
func TestActivities_TerminalExec_TimeoutTextCarriesHint(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{TimedOut: true, Output: "partial-bits"}, nil
	}}
	acts := NewActivities(sb, nil, 1024, 10*time.Minute)

	res, err := acts.TerminalExec(context.Background(), TerminalExecInput{Input: "nmap -T1 scanme.sh"})
	require.NoError(t, err)
	assert.True(t, res.TimedOut)
	assert.Equal(t, "command execution timeout (context deadline exceeded). "+
		"Partial output: partial-bits. "+
		"HINT: If this is an interactive command (shell/REPL/listener), use detach=true. "+
		"For long batch commands, wrap with shell timeout utility: 'timeout 595 <command>' to ensure clean completion",
		res.Output)
}

func TestActivities_TerminalExec_RejectsEmptyCommandNonRetryable(t *testing.T) {
	acts := NewActivities(&fakeSandbox{}, nil, 1024, time.Minute)
	_, err := acts.TerminalExec(context.Background(), TerminalExecInput{Input: "   "})
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

func TestActivities_CapOutput_OffloadsOversizedOutput(t *testing.T) {
	store := NewMemoryPayloadStore()
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: strings.Repeat("x", 5000)}, nil
	}}
	acts := NewActivities(sb, store, 1024, time.Minute)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(acts.NucleiScan, activity.RegisterOptions{Name: NucleiToolName})

	val, err := env.ExecuteActivity(acts.NucleiScan, NucleiScanInput{Target: "https://scanme.sh"})
	require.NoError(t, err)

	var res ToolResult
	require.NoError(t, val.Get(&res))
	assert.True(t, res.Truncated)
	assert.Len(t, res.Output, 1024)
	assert.True(t, strings.HasPrefix(res.PayloadURI, "mem://"))

	blob, err := store.Get(context.Background(), res.PayloadURI)
	require.NoError(t, err, "payload store should hold the full output under the activity-scoped key")
	assert.Len(t, blob, 5000)
}

func TestActivities_CapOutput_TruncatesWithoutStore(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: strings.Repeat("y", 5000)}, nil
	}}
	acts := NewActivities(sb, nil, 1024, time.Minute)

	res, err := acts.NucleiScan(context.Background(), NucleiScanInput{Target: "https://scanme.sh"})
	require.NoError(t, err)
	assert.True(t, res.Truncated)
	assert.Empty(t, res.PayloadURI)
	assert.Len(t, res.Output, 1024)
}

// The terminal formats its result before the cap sees it: the stored blob is
// the rendered text, exit-code footer included.
func TestActivities_TerminalExec_OffloadsFormattedOutput(t *testing.T) {
	store := NewMemoryPayloadStore()
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: strings.Repeat("x", 5000)}, nil
	}}
	acts := NewActivities(sb, store, 1024, time.Minute)

	res, err := acts.TerminalExec(context.Background(), TerminalExecInput{Input: "big-scan"})
	require.NoError(t, err)
	assert.True(t, res.Truncated)
	assert.Len(t, res.Output, 1024)

	blob, err := store.Get(context.Background(), res.PayloadURI)
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s\n[exit code: 0]", strings.Repeat("x", 5000)), string(blob))
}

func TestActivities_Exec_PropagatesSandboxError(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{}, errors.New("container gone")
	}}
	acts := NewActivities(sb, nil, 1024, time.Minute)

	_, err := acts.TerminalExec(context.Background(), TerminalExecInput{Input: "id"})
	assert.ErrorContains(t, err, "container gone")
}

// runTerminalInWorkflow executes TerminalExec as an activity inside a workflow
// whose id is the one passed to SetStartWorkflowOptions, so the resolution
// path sees the workflow id a FlowWorkflow child would carry.
func runTerminalInWorkflow(workflowID string, acts *Activities) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	if workflowID != "" {
		env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: workflowID})
	}
	env.RegisterActivity(acts.TerminalExec)
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		var res ToolResult
		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx,
			workflow.ActivityOptions{StartToCloseTimeout: time.Minute}),
			acts.TerminalExec, TerminalExecInput{Input: "id"}).Get(ctx, &res)
	})
	return env
}

func TestActivities_ExecSandboxed_ResolvesFlowSandboxFromWorkflowID(t *testing.T) {
	base := &fakeSandbox{}
	resolved := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "bound"}, nil
	}}
	acts := NewActivities(base, nil, 1024, time.Minute)
	flows := make(chan int64, 1)
	acts.SandboxFor = func(_ context.Context, flowID int64) (Sandbox, error) {
		flows <- flowID
		return resolved, nil
	}

	env := runTerminalInWorkflow("t-pentagi-flow-42-input-0", acts)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	select {
	case flowID := <-flows:
		assert.Equal(t, int64(42), flowID)
	default:
		t.Fatal("the sandbox resolver was never consulted")
	}
	assert.Empty(t, base.calls())
	assert.Len(t, resolved.calls(), 1)
}

func TestActivities_ExecSandboxed_FallsBackForNonFlowWorkflowID(t *testing.T) {
	base := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "base"}, nil
	}}
	acts := NewActivities(base, nil, 1024, time.Minute)
	acts.SandboxFor = func(context.Context, int64) (Sandbox, error) {
		return nil, errors.New("must not be consulted for an unparseable workflow id")
	}

	env := runTerminalInWorkflow("", acts)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.Len(t, base.calls(), 1)
}

func TestActivities_ExecSandboxed_PropagatesResolverError(t *testing.T) {
	acts := NewActivities(&fakeSandbox{}, nil, 1024, time.Minute)
	acts.SandboxFor = func(context.Context, int64) (Sandbox, error) {
		return nil, errors.New("no container row")
	}

	env := runTerminalInWorkflow("pentagi-flow-7-input-0", acts)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

// fakeTermLog records the (direction, text) pairs the sink seam writes.
type fakeTermLog struct {
	mu      sync.Mutex
	entries []string
	err     error
}

func (f *fakeTermLog) TermLog(_ context.Context, direction, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, direction+":"+text)
	return f.err
}

func (f *fakeTermLog) all() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.entries...)
}

func TestActivities_TerminalExec_WritesTermLogAroundExec(t *testing.T) {
	tl := &fakeTermLog{}
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		// By the time the command runs the stdin row must already be logged:
		// the production ordering keeps the prompt above its output.
		assert.Equal(t, 1, len(tl.all()), "stdin must be logged before exec")
		return ExecResult{Output: "uid=0", ExitCode: 0}, nil
	}}
	acts := NewActivities(sb, nil, 1024, time.Minute)
	acts.TermLogFor = func(_ context.Context, flowID int64) (TermLogSink, error) {
		assert.Equal(t, int64(42), flowID)
		return tl, nil
	}

	env := runTerminalInWorkflow("pentagi-flow-42-input-0", acts)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	entries := tl.all()
	require.Len(t, entries, 2)
	assert.Equal(t, "stdin:/work $ \033[96mid\033[0m\r\n", entries[0])
	assert.Equal(t, "stdout:\033[92muid=0\n[exit code: 0]\033[0m\r\n", entries[1])
}

func TestActivities_TerminalExec_NoTermLogResolverSkipsLogging(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "ok"}, nil
	}}
	acts := NewActivities(sb, nil, 1024, time.Minute)

	env := runTerminalInWorkflow("pentagi-flow-42-input-0", acts)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.Len(t, sb.calls(), 1)
}

func TestActivities_TerminalExec_TermLogErrorsDoNotFailExec(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "ok"}, nil
	}}
	acts := NewActivities(sb, nil, 1024, time.Minute)
	acts.TermLogFor = func(context.Context, int64) (TermLogSink, error) {
		return &fakeTermLog{err: errors.New("db down")}, nil
	}

	env := runTerminalInWorkflow("pentagi-flow-42-input-0", acts)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.Len(t, sb.calls(), 1)
}

func TestActivities_TerminalExec_TermLogSkippedOutsideFlowWorkflow(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "ok"}, nil
	}}
	acts := NewActivities(sb, nil, 1024, time.Minute)
	acts.TermLogFor = func(context.Context, int64) (TermLogSink, error) {
		return nil, errors.New("must not be consulted for an unparseable workflow id")
	}

	env := runTerminalInWorkflow("", acts)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.Len(t, sb.calls(), 1)
}

func TestActivities_PayloadRead_ServesWindows(t *testing.T) {
	store := NewMemoryPayloadStore()
	uri, err := store.Put(context.Background(), "w/1.log", []byte("0123456789abcdef"))
	require.NoError(t, err)
	acts := NewActivities(&fakeSandbox{}, store, 1024, time.Minute)

	out, err := acts.PayloadRead(context.Background(), PayloadReadInput{URI: uri})
	require.NoError(t, err)
	assert.Equal(t, PayloadReadOutput{
		Content: "0123456789abcdef", TotalBytes: 16, NextOffset: 16, Truncated: false,
	}, out)

	out, err = acts.PayloadRead(context.Background(), PayloadReadInput{URI: uri, OffsetBytes: 4, MaxBytes: 6})
	require.NoError(t, err)
	assert.Equal(t, PayloadReadOutput{
		Content: "456789", TotalBytes: 16, NextOffset: 10, Truncated: true,
	}, out)
}

// maxBytes is both the default and the ceiling: a read never exceeds 16 KiB.
func TestActivities_PayloadRead_ClampsMaxBytes(t *testing.T) {
	blob := make([]byte, 20<<10)
	for i := range blob {
		blob[i] = 'a'
	}
	store := NewMemoryPayloadStore()
	uri, err := store.Put(context.Background(), "w/big.log", blob)
	require.NoError(t, err)
	acts := NewActivities(&fakeSandbox{}, store, 1024, time.Minute)

	out, err := acts.PayloadRead(context.Background(), PayloadReadInput{URI: uri, MaxBytes: 1 << 20})
	require.NoError(t, err)
	assert.Len(t, out.Content, 16<<10)
	assert.Equal(t, 16<<10, out.NextOffset)
	assert.True(t, out.Truncated)
}

// An offset past the end is an empty untruncated answer, not an error — a
// stale next_offset must not crash the read.
func TestActivities_PayloadRead_OffsetPastEndReadsEmpty(t *testing.T) {
	store := NewMemoryPayloadStore()
	uri, err := store.Put(context.Background(), "w/1.log", []byte("short"))
	require.NoError(t, err)
	acts := NewActivities(&fakeSandbox{}, store, 1024, time.Minute)

	out, err := acts.PayloadRead(context.Background(), PayloadReadInput{URI: uri, OffsetBytes: 999})
	require.NoError(t, err)
	assert.Equal(t, "", out.Content)
	assert.Equal(t, 5, out.NextOffset)
	assert.False(t, out.Truncated)
}

func TestActivities_PayloadRead_ReportsMissingURIAsContent(t *testing.T) {
	acts := NewActivities(&fakeSandbox{}, NewMemoryPayloadStore(), 1024, time.Minute)

	out, err := acts.PayloadRead(context.Background(), PayloadReadInput{URI: "mem://gone.log"})
	require.NoError(t, err)
	assert.Contains(t, out.Content, "not found")
}

func TestActivities_PayloadRead_RequiresURIAndStore(t *testing.T) {
	acts := NewActivities(&fakeSandbox{}, NewMemoryPayloadStore(), 1024, time.Minute)
	out, err := acts.PayloadRead(context.Background(), PayloadReadInput{})
	require.NoError(t, err)
	assert.Contains(t, out.Content, "uri is required")

	acts = NewActivities(&fakeSandbox{}, nil, 1024, time.Minute)
	out, err = acts.PayloadRead(context.Background(), PayloadReadInput{URI: "mem://x"})
	require.NoError(t, err)
	assert.Contains(t, out.Content, "not configured")
}
