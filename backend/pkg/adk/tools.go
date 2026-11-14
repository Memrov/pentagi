package adk

import (
	"context"
	"time"

	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/adk/v2/tool"
)

type TerminalExecInput struct {
	Command        string `json:"command"`
	Cwd            string `json:"cwd,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
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

// Signature anchors for ActivityAsTool, which infers the model-facing schema
// from the argument type but dispatches by name only — the registered
// Activities method is what runs, so these bodies never execute.
func terminalExecShape(context.Context, TerminalExecInput) (ToolResult, error) {
	return ToolResult{}, nil
}

func nucleiScanShape(context.Context, NucleiScanInput) (ToolResult, error) { return ToolResult{}, nil }
func httpxProbeShape(context.Context, HttpxProbeInput) (ToolResult, error) { return ToolResult{}, nil }

var (
	_ func(context.Context, TerminalExecInput) (ToolResult, error) = (*Activities)(nil).TerminalExec
	_ func(context.Context, NucleiScanInput) (ToolResult, error)   = (*Activities)(nil).NucleiScan
	_ func(context.Context, HttpxProbeInput) (ToolResult, error)   = (*Activities)(nil).HttpxProbe
)

// DurableTools builds the workflow-side tool set: each tool call becomes a
// Temporal activity dispatched under the same name it is registered with.
func DurableTools(timeout time.Duration) ([]tool.Tool, error) {
	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}

	terminal, err := googleadk.ActivityAsTool(terminalExecShape, googleadk.ActivityToolOptions{
		Name: TerminalToolName,
		Description: "Run a shell command in the assessment sandbox. Use for reconnaissance, " +
			"exploitation steps, and general tooling that is not covered by the dedicated scanners.",
		ActivityOptions: actOpts,
	})
	if err != nil {
		return nil, err
	}

	nuclei, err := googleadk.ActivityAsTool(nucleiScanShape, googleadk.ActivityToolOptions{
		Name: NucleiToolName,
		Description: "Run a nuclei template scan against one target URL and return the JSONL findings. " +
			"Prefer severity and tags filters over broad scans.",
		ActivityOptions: actOpts,
	})
	if err != nil {
		return nil, err
	}

	httpx, err := googleadk.ActivityAsTool(httpxProbeShape, googleadk.ActivityToolOptions{
		Name: HttpxToolName,
		Description: "Probe a target URL with httpx and return the JSON probe result " +
			"(status, title, technologies, redirect chain).",
		ActivityOptions: actOpts,
	})
	if err != nil {
		return nil, err
	}

	return []tool.Tool{terminal, nuclei, httpx}, nil
}
