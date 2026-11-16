package adk

// PentesterPromptInput is everything the production pentester template needs
// that the caller owns: the task text, the environment facts and the toolset
// in play. Phase 2 renders the template through templates.Prompter with the
// map PentesterPromptVars returns, which is what lets this package stay free
// of the templates dependency.
type PentesterPromptInput struct {
	Question                string
	Tools                   []string
	GraphitiEnabled         bool
	IsDefaultDockerImage    bool
	DockerImage             string
	Cwd                     string
	ContainerPorts          string
	ExecutionContext        string
	Lang                    string
	CurrentTime             string
	ToolPlaceholder         string
	UserFiles               string
	SummarizedContentPrefix string
}

// promptToolVars maps each tool name to the template variable the production
// prompt addresses it by — the same keys flowProvider.GetPentesterHandler
// fills. Names without a ported constant are literals until their tool lands.
var promptToolVars = map[string]string{
	HackResultToolName:                "HackResultToolName",
	"web_search":                      "WebSearchToolName",
	"search_guide":                    "SearchGuideToolName",
	"store_guide":                     "StoreGuideToolName",
	"graphiti_search":                 "GraphitiSearchToolName",
	"search":                          "SearchToolName",
	"coder":                           "CoderToolName",
	"advice":                          "AdviceToolName",
	"memorist":                        "MemoristToolName",
	"maintenance":                     "MaintenanceToolName",
	TerminalToolName:                  "TerminalToolName",
	FileToolName:                      "FileToolName",
	"execute_task_and_return_summary": "SummarizationToolName",
}

// PentesterPromptVars builds the template variables exactly as the production
// handler's system context names them. A tool-name variable is emitted only
// when its tool is registered, so a reduced toolset renders a prompt that
// never names tools the agent cannot call; everything else passes through
// verbatim — the function stays pure and reads neither clock nor environment.
func PentesterPromptVars(input PentesterPromptInput) map[string]any {
	vars := map[string]any{
		"Question":                input.Question,
		"GraphitiEnabled":         input.GraphitiEnabled,
		"IsDefaultDockerImage":    input.IsDefaultDockerImage,
		"DockerImage":             input.DockerImage,
		"Cwd":                     input.Cwd,
		"ContainerPorts":          input.ContainerPorts,
		"ExecutionContext":        input.ExecutionContext,
		"Lang":                    input.Lang,
		"CurrentTime":             input.CurrentTime,
		"ToolPlaceholder":         input.ToolPlaceholder,
		"UserFiles":               input.UserFiles,
		"SummarizedContentPrefix": input.SummarizedContentPrefix,
	}
	for _, name := range input.Tools {
		if key, ok := promptToolVars[name]; ok {
			vars[key] = name
		}
	}
	return vars
}
