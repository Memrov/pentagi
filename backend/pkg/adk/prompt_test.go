package adk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPentesterPromptVars_CarriesCallerContextVerbatim(t *testing.T) {
	vars := PentesterPromptVars(PentesterPromptInput{
		Question:                "test the vpn gateway",
		GraphitiEnabled:         true,
		IsDefaultDockerImage:    true,
		DockerImage:             "pentagi/pentester:latest",
		Cwd:                     "/work",
		ContainerPorts:          "80,443",
		ExecutionContext:        "subtask 3 of task 9",
		Lang:                    "English",
		CurrentTime:             "2025-01-01 12:00 UTC",
		ToolPlaceholder:         "Execute operations via function invocation - textual responses are not acceptable for task completion.",
		UserFiles:               "notes.txt",
		SummarizedContentPrefix: "**summarized content:**\\n",
	})

	assert.Equal(t, map[string]any{
		"Question":                "test the vpn gateway",
		"GraphitiEnabled":         true,
		"IsDefaultDockerImage":    true,
		"DockerImage":             "pentagi/pentester:latest",
		"Cwd":                     "/work",
		"ContainerPorts":          "80,443",
		"ExecutionContext":        "subtask 3 of task 9",
		"Lang":                    "English",
		"CurrentTime":             "2025-01-01 12:00 UTC",
		"ToolPlaceholder":         "Execute operations via function invocation - textual responses are not acceptable for task completion.",
		"UserFiles":               "notes.txt",
		"SummarizedContentPrefix": "**summarized content:**\\n",
	}, vars, "no tool names means exactly the non-tool variables")
}

func TestPentesterPromptVars_EmitsToolVarsOnlyForRegisteredTools(t *testing.T) {
	cases := []struct {
		tool string
		var_ string
	}{
		{"hack_result", "HackResultToolName"},
		{"terminal", "TerminalToolName"},
		{"file", "FileToolName"},
		{"web_search", "WebSearchToolName"},
		{"search_guide", "SearchGuideToolName"},
		{"store_guide", "StoreGuideToolName"},
		{"graphiti_search", "GraphitiSearchToolName"},
		{"search", "SearchToolName"},
		{"coder", "CoderToolName"},
		{"advice", "AdviceToolName"},
		{"memorist", "MemoristToolName"},
		{"maintenance", "MaintenanceToolName"},
		{"execute_task_and_return_summary", "SummarizationToolName"},
	}
	for _, tc := range cases {
		t.Run(tc.tool, func(t *testing.T) {
			vars := PentesterPromptVars(PentesterPromptInput{Tools: []string{tc.tool}})
			assert.Equal(t, tc.tool, vars[tc.var_])

			for _, other := range cases {
				if other.var_ != tc.var_ {
					assert.NotContains(t, vars, other.var_, "absent tools must not leak their variable")
				}
			}
		})
	}
}

// A tool the seam does not know is still settable — the variable name comes
// from the mapping only when we have one; unknown names are skipped silently
// so a tool without a template slot does not invent one.
func TestPentesterPromptVars_IgnoresUnmappedToolNames(t *testing.T) {
	vars := PentesterPromptVars(PentesterPromptInput{Tools: []string{"nuclei_scan", "httpx_probe"}})
	for key := range vars {
		assert.NotContains(t, key, "ToolName", "spike scanner tools have no production prompt variable")
	}
}
