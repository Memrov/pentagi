package adk

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActivities_FileExec_ReadPassesPathPositionally(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "file-body\n"}, nil
	}}
	acts := NewActivities(sb, nil, 1024, 0)

	res, err := acts.FileExec(context.Background(), FileExecInput{Action: "read_file", Path: "/work/notes.txt"})
	require.NoError(t, err)
	assert.Equal(t, "file-body\n", res.Output)

	calls := sb.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"sh", "-c", `cat -- "$0"`, "/work/notes.txt"}, calls[0])
}

// The content travels base64-encoded as a positional arg: no quoting the
// model supplies can reach the shell unescaped.
func TestActivities_FileExec_WriteEncodesContentPositionally(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{}, nil
	}}
	acts := NewActivities(sb, nil, 1024, 0)

	res, err := acts.FileExec(context.Background(), FileExecInput{
		Action: "write_file", Path: "/work/poc.txt", Content: "hello"})
	require.NoError(t, err)
	assert.Equal(t, "Successfully wrote 5 bytes to /work/poc.txt", res.Output)

	calls := sb.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"sh", "-c", `echo "$0" | base64 -d > "$1"`, "aGVsbG8=", "/work/poc.txt"}, calls[0])
}

func TestActivities_FileExec_EditReturnsUnsupported(t *testing.T) {
	sb := &fakeSandbox{}
	acts := NewActivities(sb, nil, 1024, 0)

	res, err := acts.FileExec(context.Background(), FileExecInput{
		Action: "edit_file", Path: "/work/x", Diff: "@@ -1 +1 @@"})
	require.NoError(t, err)
	assert.Equal(t, "edit_file is not supported yet; use write_file", res.Output)
	assert.Empty(t, sb.calls(), "an unsupported action must not reach the sandbox")
}

// The production handler infers a missing action from whichever payload field
// carries data; a call that works there must work here.
func TestActivities_FileExec_InfersActionFromPayload(t *testing.T) {
	cases := []struct {
		name       string
		in         FileExecInput
		wantArgvAt int
		wantOutput string
	}{
		{"diff means edit", FileExecInput{Path: "/work/x", Diff: "d"}, -1, "edit_file is not supported yet; use write_file"},
		{"content means write", FileExecInput{Path: "/work/x", Content: "c"}, 2, "Successfully wrote 1 bytes to /work/x"},
		{"empty means read", FileExecInput{Path: "/work/x"}, 2, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) { return ExecResult{}, nil }}
			acts := NewActivities(sb, nil, 1024, 0)

			res, err := acts.FileExec(context.Background(), tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.wantOutput, res.Output)
			if tc.wantArgvAt >= 0 {
				calls := sb.calls()
				require.Len(t, calls, 1)
				assert.Equal(t, tc.in.Path, calls[0][len(calls[0])-1])
			}
		})
	}
}

func TestActivities_FileExec_RejectsShellExpansionPaths(t *testing.T) {
	acts := NewActivities(&fakeSandbox{}, nil, 1024, 0)
	for _, path := range []string{"", "$(id)", "`id`", "$HOME/x", "${HOME}/x", "/work/$x"} {
		res, err := acts.FileExec(context.Background(), FileExecInput{Action: "read_file", Path: path})
		require.NoError(t, err, path)
		assert.NotEmpty(t, res.Output, path)
		assert.Empty(t, acts.sandbox.(*fakeSandbox).calls(), path)
	}
}

func TestActivities_FileExec_RejectsUnknownAction(t *testing.T) {
	acts := NewActivities(&fakeSandbox{}, nil, 1024, 0)

	res, err := acts.FileExec(context.Background(), FileExecInput{Action: "delete_file", Path: "/work/x"})
	require.NoError(t, err)
	assert.Equal(t, `unknown file action "delete_file": expected one of read_file, write_file or edit_file`, res.Output)
}

// Paths that are legal but hostile to sloppy quoting must reach the exec
// unchanged, still positional.
func TestActivities_FileExec_HostilePathsStayPositional(t *testing.T) {
	paths := []string{"/work/has space.txt", `/work/"quoted".txt`, "/work/x;rm -rf /", "/work/a\nb"}
	for _, path := range paths {
		sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) { return ExecResult{}, nil }}
		acts := NewActivities(sb, nil, 1024, 0)

		_, err := acts.FileExec(context.Background(), FileExecInput{Action: "read_file", Path: path})
		require.NoError(t, err, path)

		calls := sb.calls()
		require.Len(t, calls, 1, path)
		assert.Equal(t, `cat -- "$0"`, calls[0][2], path)
		assert.Equal(t, path, calls[0][3], path)
	}
}

// Same truncation contract the executor applies: head and tail of 16KiB each
// with the exact byte-range markers between them.
func TestActivities_FileExec_ReadTruncatesAtLimit(t *testing.T) {
	full := strings.Repeat("a", 40*1024)
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: full}, nil
	}}
	acts := NewActivities(sb, nil, 1024, 0)

	res, err := acts.FileExec(context.Background(), FileExecInput{Action: "read_file", Path: "/work/big"})
	require.NoError(t, err)
	assert.True(t, res.Truncated)
	assert.Equal(t,
		strings.Repeat("a", 16384)+"\n[0:16384 bytes]\n... [truncated] ...\n[24576:40960 bytes]\n"+strings.Repeat("a", 16384),
		res.Output)
}

func TestActivities_FileExec_ReadAtLimitBoundary(t *testing.T) {
	cases := []struct {
		name string
		size int
		want bool
	}{
		{"exactly twice the limit passes", 32 * 1024, false},
		{"one byte over truncates", 32*1024 + 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
				return ExecResult{Output: strings.Repeat("a", tc.size)}, nil
			}}
			acts := NewActivities(sb, nil, 1024, 0)

			res, err := acts.FileExec(context.Background(), FileExecInput{Action: "read_file", Path: "/work/big"})
			require.NoError(t, err)
			assert.Equal(t, tc.want, res.Truncated)
		})
	}
}

func TestActivities_FileExec_ExitFailureFormatsResult(t *testing.T) {
	sb := &fakeSandbox{next: func(argv []string) (ExecResult, error) {
		return ExecResult{Output: "cat: /work/nope: No such file", ExitCode: 1}, nil
	}}
	acts := NewActivities(sb, nil, 1024, 0)

	res, err := acts.FileExec(context.Background(), FileExecInput{Action: "read_file", Path: "/work/nope"})
	require.NoError(t, err)
	assert.Equal(t, "cat: /work/nope: No such file\n[exit code: 1]", res.Output)
	assert.Equal(t, 1, res.ExitCode)
}

func TestFlowCommand_MarksOomScore(t *testing.T) {
	assert.Equal(t,
		[]string{"sh", "-c", markFlowCommandSh, "nmap x"},
		flowCommand("nmap x"))
	assert.Contains(t, markFlowCommandSh, "oom_score_adj")
	assert.NotContains(t, sandboxCommandSh, "oom_score_adj")
}
