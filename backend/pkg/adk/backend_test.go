package adk

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalSandbox_Exec_CapturesOutputAndExitCode(t *testing.T) {
	res, err := LocalSandbox{}.Exec(context.Background(), []string{"sh", "-c", "echo hello; exit 3"}, ExecOptions{})
	require.NoError(t, err)
	assert.Equal(t, "hello\n", res.Output)
	assert.Equal(t, 3, res.ExitCode)
	assert.False(t, res.TimedOut)
}

func TestLocalSandbox_Exec_KillsOnTimeout(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	res, err := LocalSandbox{}.Exec(ctx, []string{"sleep", "30"}, ExecOptions{Timeout: 100 * time.Millisecond})
	require.NoError(t, err)
	assert.True(t, res.TimedOut)
	assert.Less(t, time.Since(start), 5*time.Second, "the kill path must not wait out the command")
}

func TestLocalSandbox_Exec_PropagatesParentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	res, err := LocalSandbox{}.Exec(ctx, []string{"sleep", "30"}, ExecOptions{Timeout: 30 * time.Second})
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, res.TimedOut, "parent cancellation is not the tool's own timeout")
}

// Detached means started-not-finished: the call comes back at once with the
// StillRunning verdict while the command keeps running on its own context.
func TestLocalSandbox_Exec_DetachReturnsStarted(t *testing.T) {
	start := time.Now()
	res, err := LocalSandbox{}.Exec(context.Background(),
		[]string{"sh", "-c", "sleep 30"}, ExecOptions{Detach: true, Timeout: 500 * time.Millisecond})
	require.NoError(t, err)
	assert.True(t, res.StillRunning)
	assert.Empty(t, res.Output)
	assert.Less(t, time.Since(start), 5*time.Second, "detach must not wait for the command")
}

func TestLocalSandbox_Exec_ErrorsOnlyOnSpawnFailure(t *testing.T) {
	_, err := LocalSandbox{}.Exec(context.Background(), []string{"definitely-not-a-binary-xyz"}, ExecOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "definitely-not-a-binary-xyz")

	res, err := LocalSandbox{}.Exec(context.Background(), nil, ExecOptions{})
	require.Error(t, err)
	_ = res
}

func TestCappedBuffer_Write_DropsPastLimit(t *testing.T) {
	b := &cappedBuffer{limit: 5}
	_, _ = b.Write([]byte("hello"))
	_, _ = b.Write([]byte("world"))
	assert.Equal(t, "hello", b.buf.String())
	assert.True(t, b.truncated)

	b2 := &cappedBuffer{limit: 5}
	_, _ = b2.Write([]byte("hi"))
	assert.False(t, b2.truncated)

	b3 := &cappedBuffer{limit: 5}
	_, _ = b3.Write([]byte("hello"))
	assert.False(t, b3.truncated, "a write that fills the buffer exactly drops nothing")
}

func TestLocalSandbox_Exec_TruncatesHugeOutput(t *testing.T) {
	res, err := LocalSandbox{}.Exec(context.Background(),
		[]string{"sh", "-c", "yes abcdefghij | head -c 6000000"}, ExecOptions{Timeout: 30 * time.Second})
	require.NoError(t, err)
	assert.True(t, res.Truncated)
	assert.LessOrEqual(t, len(res.Output), maxExecOutput)
	assert.True(t, strings.HasPrefix(res.Output, "abcdefghij"))
}
