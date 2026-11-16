package adk

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"unicode/utf8"
)

// resultSizeLimit mirrors tools.DefaultResultSizeLimit: the file read and the
// terminal timeout texts are capped exactly where the executor caps a tool
// result, so a result the production prompt was written against comes back in
// the same shape here.
const resultSizeLimit = 16 << 10

// File actions, matching tools.ReadFile/WriteFile/EditFile.
const (
	actionReadFile  = "read_file"
	actionWriteFile = "write_file"
	actionEditFile  = "edit_file"
)

// edit_file needs the container copy plumbing (read, apply diff, write back)
// that the exec-only sandbox does not have; the model gets the refusal as a
// result string, the way production hands it tool errors.
const editFileUnsupported = "edit_file is not supported yet; use write_file"

var shellExpansion = regexp.MustCompile("\\$\\(|\\$\\{|`|\\$[A-Za-z_][A-Za-z0-9_]*")

// Command wrappers, verbatim from pkg/docker/flow_commands.go — importing the
// package would drag its database and observability dependencies into the
// spike for the sake of two shell constants. SandboxCommand leaves the exec
// unowned so a flow stop never kills it; FlowCommand bumps oom_score_adj by
// one, the mark the stop sweep greps /proc for.
const (
	ignoreHangupSh    = `trap "" HUP`
	sandboxCommandSh  = ignoreHangupSh + `; exec sh -c "$0"`
	markFlowCommandSh = ignoreHangupSh + `; read -r adj 2>/dev/null </proc/self/oom_score_adj && ` +
		`echo $((adj + 1)) 2>/dev/null >/proc/self/oom_score_adj; exec sh -c "$0"`
)

func flowCommand(command string) []string    { return []string{"sh", "-c", markFlowCommandSh, command} }
func sandboxCommand(command string) []string { return []string{"sh", "-c", sandboxCommandSh, command} }

func validateFilePath(path string) error {
	if path == "" {
		return fmt.Errorf("path is required and cannot be empty: give an absolute path such as /work/notes.txt")
	}

	if token := shellExpansion.FindString(path); token != "" {
		return fmt.Errorf(
			"path must be a literal filename, but it contains %q: this field is not run through a shell, "+
				"so the expression is taken as part of the name. Use the terminal tool to resolve it "+
				"first, then pass the result here",
			token)
	}

	return nil
}

// FileExec is the activity behind the file tool: read_file and write_file map
// onto the sandbox exec surface (there is no container copy API here), with
// the same path validation and result truncation production applies. Path and
// content travel as positional args, never interpolated into the script, so
// quoting cannot be broken by model input.
func (a *Activities) FileExec(ctx context.Context, in FileExecInput) (ToolResult, error) {
	action := in.Action
	if action == "" {
		// Models omit 'action' often enough that production infers it from the
		// populated payload fields; a call that succeeds there should succeed
		// here too.
		switch {
		case in.Diff != "":
			action = actionEditFile
		case in.Content != "":
			action = actionWriteFile
		default:
			action = actionReadFile
		}
	}

	switch action {
	case actionEditFile:
		return ToolResult{Output: editFileUnsupported}, nil
	case actionReadFile, actionWriteFile:
	default:
		return ToolResult{Output: fmt.Sprintf("unknown file action %q: expected one of %s, %s or %s",
			action, actionReadFile, actionWriteFile, actionEditFile)}, nil
	}

	if err := validateFilePath(in.Path); err != nil {
		return ToolResult{Output: err.Error()}, nil
	}

	var argv []string
	switch action {
	case actionReadFile:
		argv = []string{"sh", "-c", `cat -- "$0"`, in.Path}
	case actionWriteFile:
		argv = []string{"sh", "-c", `echo "$0" | base64 -d > "$1"`,
			base64.StdEncoding.EncodeToString([]byte(in.Content)), in.Path}
	}

	res, err := a.execSandboxed(ctx, argv, ExecOptions{Timeout: a.defTimeout})
	if err != nil {
		return ToolResult{}, err
	}
	switch {
	case res.TimedOut:
		return ToolResult{Output: res.Output, TimedOut: true}, nil
	case res.ExitCode != 0:
		return ToolResult{Output: execOutputString(res), ExitCode: res.ExitCode}, nil
	}

	if action == actionReadFile {
		out := truncateResult(res.Output)
		return ToolResult{Output: out, Truncated: res.Truncated || out != res.Output}, nil
	}

	return ToolResult{Output: fmt.Sprintf("Successfully wrote %d bytes to %s", len(in.Content), in.Path)}, nil
}

// truncateResult keeps head and tail of an oversized result with the exact
// markers the executor emits, rune-boundary adjustments included.
func truncateResult(result string) string {
	if len(result) <= resultSizeLimit*2 {
		return result
	}

	head := resultSizeLimit
	for head > 0 && !utf8.RuneStart(result[head]) {
		head--
	}
	tail := len(result) - resultSizeLimit
	for tail < len(result) && !utf8.RuneStart(result[tail]) {
		tail++
	}

	return fmt.Sprintf("%s\n[0:%d bytes]\n... [truncated] ...\n[%d:%d bytes]\n%s",
		result[:head],
		head,
		tail,
		len(result),
		result[tail:],
	)
}

// truncateString is the executor's flat cut used inside the terminal timeout
// text: the first maxLen bytes plus the full-size marker.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "... [truncated full size is " + strconv.Itoa(len(s)) + " bytes]"
}

// execOutputString renders the production terminal result contract: captured
// output — or [no output] — then the exit-code footer, and the interrupted
// marker when the command died with its container.
func execOutputString(res ExecResult) string {
	out := res.Output
	if out == "" {
		out = "[no output]"
	}
	out = fmt.Sprintf("%s\n[exit code: %d]", out, res.ExitCode)
	if res.ExitCode > 128 && res.ContainerStopped {
		out += "\n[interrupted: the sandbox container stopped]"
	}
	return out
}
