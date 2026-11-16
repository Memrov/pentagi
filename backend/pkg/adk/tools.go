package adk

import (
	"context"
	"time"

	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/adk/v2/tool"
)

// TerminalExecInput mirrors tools.TerminalAction field-for-field: the model's
// prompt and its jsonschema descriptions were written against these names, so
// the ADK tool must accept the same arguments. Go names stay idiomatic.
type TerminalExecInput struct {
	Input   string `json:"input"`
	Cwd     string `json:"cwd"`
	Detach  bool   `json:"detach"`
	Timeout int64  `json:"timeout"`
	Message string `json:"message"`
}

// FileExecInput mirrors tools.FileAction; edit_file is declared because the
// production schema advertises it, but FileExec turns it away until diff
// support lands.
type FileExecInput struct {
	Action  string `json:"action"`
	Content string `json:"content,omitempty"`
	Diff    string `json:"diff,omitempty"`
	Path    string `json:"path"`
	Message string `json:"message"`
}

// HackResultInput mirrors tools.HackResult plus the optional success flag the
// other closing tools carry: production's struct has no success field, but the
// workflow result needs an explicit success/failure signal, so the ADK schema
// accepts it as optional.
type HackResultInput struct {
	Success *bool  `json:"success,omitempty"`
	Result  string `json:"result"`
	Message string `json:"message"`
}

type NucleiScanInput struct {
	Target   string   `json:"target"`
	Severity []string `json:"severity,omitempty"`
	Tags     []string `json:"tags,omitempty"`
}

type HttpxProbeInput struct {
	Target         string `json:"target"`
	TechDetect     bool   `json:"tech_detect,omitempty"`
	StatusCode     bool   `json:"status_code,omitempty"`
	Title          bool   `json:"title,omitempty"`
	FollowRedirect bool   `json:"follow_redirect,omitempty"`
}

type ToolResult struct {
	Output     string `json:"output"`
	ExitCode   int    `json:"exit_code"`
	TimedOut   bool   `json:"timed_out,omitempty"`
	Truncated  bool   `json:"truncated,omitempty"`
	PayloadURI string `json:"payload_uri,omitempty"`
}

// PayloadReadInput fetches one window of a stored payload. offset_bytes
// continues from a previous next_offset; max_bytes caps the window.
type PayloadReadInput struct {
	URI         string `json:"uri"`
	OffsetBytes int    `json:"offset_bytes,omitempty"`
	MaxBytes    int    `json:"max_bytes,omitempty"`
}

// PayloadReadOutput is the window: the slice plus the cursor state the model
// needs to decide whether to keep reading.
type PayloadReadOutput struct {
	Content    string `json:"content"`
	TotalBytes int    `json:"total_bytes"`
	NextOffset int    `json:"next_offset"`
	Truncated  bool   `json:"truncated"`
}

// Signature anchors for ActivityAsTool, which infers the model-facing schema
// from the argument type but dispatches by name only — the registered
// Activities method is what runs, so these bodies never execute.
func terminalExecShape(context.Context, TerminalExecInput) (ToolResult, error) {
	return ToolResult{}, nil
}

func fileExecShape(context.Context, FileExecInput) (ToolResult, error) { return ToolResult{}, nil }

func nucleiScanShape(context.Context, NucleiScanInput) (ToolResult, error) { return ToolResult{}, nil }
func httpxProbeShape(context.Context, HttpxProbeInput) (ToolResult, error) { return ToolResult{}, nil }
func payloadReadShape(context.Context, PayloadReadInput) (PayloadReadOutput, error) {
	return PayloadReadOutput{}, nil
}

var (
	_ func(context.Context, TerminalExecInput) (ToolResult, error)       = (*Activities)(nil).TerminalExec
	_ func(context.Context, FileExecInput) (ToolResult, error)           = (*Activities)(nil).FileExec
	_ func(context.Context, NucleiScanInput) (ToolResult, error)         = (*Activities)(nil).NucleiScan
	_ func(context.Context, HttpxProbeInput) (ToolResult, error)         = (*Activities)(nil).HttpxProbe
	_ func(context.Context, PayloadReadInput) (PayloadReadOutput, error) = (*Activities)(nil).PayloadRead
)

// Model-facing tool descriptions, shared by the durable and direct tool sets
// so the A/B comparison exercises the same agent instructions. The terminal,
// file and hack_result texts are verbatim from tools.registryDefinitions: the
// production prompts were written against exactly these strings.
const (
	terminalToolDesc = "Calls a terminal command in blocking mode. " +
		"Use timeout=0 or a negative value to apply the configured server default timeout. " +
		"Explicit positive values are accepted up to 10800 seconds (3 hours); values outside this range are replaced by the server default. " +
		"Only one command can be executed at a time"
	fileToolDesc = "Reads, writes, or edits local files (see 'action'). " +
		"Prefer edit_file for targeted changes to an existing file; use write_file only for a new file or a full rewrite."
	hackResultToolDesc = "Send the penetration test result to the user with detailed report"
	nucleiToolDesc     = "Run a nuclei template scan against one target URL and return the JSONL findings. " +
		"Prefer severity and tags filters over broad scans."
	httpxToolDesc = "Probe a target URL with httpx and return the JSON probe result " +
		"(status, title, technologies, redirect chain)."
	payloadReadToolDesc = "Fetch a stored tool-output payload by URI. " +
		"A tool result with truncated=true carries a payload_uri field; pass that URI here to read " +
		"the full blob in windows — offset_bytes to continue from a previous next_offset, " +
		"max_bytes to bound one read (16384 default and maximum)."
)

// DurableTools builds the workflow-side tool set: terminal, file and the two
// scanners become Temporal activities dispatched under the same name they are
// registered with, while hack_result is an in-workflow function tool — its
// result is the loop barrier and must never wait out an activity round-trip.
// payload_read joins them only when the worker has a payload store to read
// through, so an advertised tool always answers.
func DurableTools(timeout time.Duration, payloadRead bool) ([]tool.Tool, error) {
	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		HeartbeatTimeout:    toolHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}

	hack, err := newHackResultTool()
	if err != nil {
		return nil, err
	}

	terminal, err := googleadk.ActivityAsTool(terminalExecShape, googleadk.ActivityToolOptions{
		Name:            TerminalToolName,
		Description:     terminalToolDesc,
		ActivityOptions: actOpts,
	})
	if err != nil {
		return nil, err
	}

	file, err := googleadk.ActivityAsTool(fileExecShape, googleadk.ActivityToolOptions{
		Name:            FileToolName,
		Description:     fileToolDesc,
		ActivityOptions: actOpts,
	})
	if err != nil {
		return nil, err
	}

	nuclei, err := googleadk.ActivityAsTool(nucleiScanShape, googleadk.ActivityToolOptions{
		Name:            NucleiToolName,
		Description:     nucleiToolDesc,
		ActivityOptions: actOpts,
	})
	if err != nil {
		return nil, err
	}

	httpx, err := googleadk.ActivityAsTool(httpxProbeShape, googleadk.ActivityToolOptions{
		Name:            HttpxToolName,
		Description:     httpxToolDesc,
		ActivityOptions: actOpts,
	})
	if err != nil {
		return nil, err
	}

	tools := []tool.Tool{hack, terminal, file, nuclei, httpx}

	if payloadRead {
		read, err := googleadk.ActivityAsTool(payloadReadShape, googleadk.ActivityToolOptions{
			Name:            PayloadReadToolName,
			Description:     payloadReadToolDesc,
			ActivityOptions: actOpts,
		})
		if err != nil {
			return nil, err
		}
		tools = append(tools, read)
	}

	return tools, nil
}
