// Package adk wires a Google ADK pentester agent into PentAGI's Temporal
// durability layer, as a spike for evaluating ADK as the agent loop. The agent
// orchestration runs inside a workflow through go.temporal.io/googleadk: model
// calls and the sandbox tools run as activities, while the real model client
// and its API key stay worker-side and never enter workflow history.
package adk

import "time"

const (
	PentesterWorkflowName = "pentagi.adk.PentesterWorkflow"

	TerminalToolName = "terminal"
	NucleiToolName   = "nuclei_scan"
	HttpxToolName    = "httpx_probe"

	appName = "pentagi-adk"

	// DefaultToolTimeout bounds one tool activity attempt; scans may still
	// retry under the activity RetryPolicy.
	DefaultToolTimeout = 10 * time.Minute
)
