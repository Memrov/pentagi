# ADK Go spike: durable pentester agent

`pkg/adk` evaluates replacing the langchaingo agent loop with Google ADK Go 2.0 while keeping Temporal as the only durability and orchestration layer. It is a spike: nothing in production code paths calls it, and it is exercised through `cmd/adkpentest` and its own tests.

## Architecture

The integration is `go.temporal.io/googleadk`. The agent loop runs inside `pentagi.adk.PentesterWorkflow`; each LLM call is dispatched to the plugin's `InvokeModel` activity and each tool call to one of our activity implementations, so every step is retried, timed out and replayable under Temporal rather than the agent framework.

- Workflow side (`workflow.go`, `tools.go`): builds the `llmagent` pentester with `googleadk.NewModel(name)` — a name-only model handle — plus the tool surface. `terminal`, `file`, `nuclei_scan` and `httpx_probe` are exposed through `googleadk.ActivityAsTool`, which infers the model-facing schema from the activity's argument struct and dispatches the activity by name, so the tool name and the registered activity name are one constant. `hack_result` is an in-workflow `functiontool`, deliberately not an activity: it is the loop barrier and must never wait out an activity round-trip.
- Worker side (`worker.go`, `model.go`, `activities.go`, `flowtools.go`, `backend.go`, `payload.go`): `WorkerPlugin` registers `InvokeModel` with a `ModelFactory` that rebuilds the real `openaimodel` inside the activity boundary over one shared HTTP client. The API key lives in the factory closure and never enters workflow history. `Activities` runs each tool inside a `Sandbox` — `DockerSandbox` for a container or `LocalSandbox` for development — and hands oversized output to a `PayloadStore` (S3, or a bounded in-memory store when no bucket is configured) instead of bloating history.
- Retries are owned by Temporal alone: the openai-go client is constructed with `WithMaxRetries(0)` so a transient failure is not retried twice over.

The model is any OpenAI-compatible chat-completions endpoint. The default target is **OpenCode Go** (the subscription tier): `ADK_BASE_URL=https://opencode.ai/inference/go/openai/v1`, `ADK_MODEL_NAME=deepseek-v4.1-flash` (verified in the live Go catalog; Console translates every text model across APIs, so the chat-completions path serves all of them). `ADK_MODEL_API` selects `chat_completions` (default) or `responses` for providers that speak the Responses API. Go's terms require a client-specific user agent and a stable `x-opencode-session` per conversation — `ModelFactory` sends `pentagi-adk/1.0` and derives the session from the workflow ID, so every turn of a run shares one value including across retries. DeepSeek models require Settings › Privacy › Regions: Global on the OpenCode workspace.

## Tool surface and production parity

`tools.go` carries the production argument structs field-for-field: `terminal` takes `input`/`cwd`/`detach`/`timeout`/`message`, `file` takes `action`/`content`/`diff`/`path`/`message`, `hack_result` takes `result`/`message` plus an optional `success` flag (production's `HackResult` struct has no `success`; the workflow result needs the explicit signal, so the ADK schema accepts it as optional). Model-facing descriptions are verbatim from `tools.registryDefinitions` — the production prompts were written against exactly these strings.

`activities.go` replicates the terminal contract from `pkg/tools/terminal.go`: timeout normalization (non-positive or above the bound falls back to the configured default plus a five-second grace, the default itself capped at three hours), the `output + "\n[exit code: N]"` result shape with `[no output]` and `[interrupted: the sandbox container stopped]` variants, the detach quick check returning `Command started in background with timeout %s (still running)`, and the timeout error text with the `detach=true`/`timeout %d <command>` HINT. `detach` runs the command on a cancellation-proof context and returns after a ~500 ms early-exit check.

`flowtools.go` adds the `file` activity over the exec surface: `read_file` cats the path, `write_file` base64-decodes content into it — both passed positionally so no model-supplied quoting reaches the shell — with the production shell-expansion path validation and the executor's 16 KiB head/tail truncation markers. `edit_file` returns `edit_file is not supported yet; use write_file` (no container copy plumbing here); a missing `action` is inferred from `diff`/`content` the way the production handler does.

`hack_result` (in `hackresult.go`) sets `pentagi.hack_result` on the event state delta and `Actions().SkipSummarization = true`, which ends the ADK turn without a summarization model call — the same barrier `performAgentChain` gets from its result map. `PentestResult.HackResult` carries the extracted `{success, result}` for both the workflow and direct paths.

Commands run through the production wrappers replicated in `flowtools.go` (the real `pkg/docker` package pulls in database and observability dependencies the spike avoids): `sandboxCommandSh` leaves an exec unowned, `markFlowCommandSh` bumps `oom_score_adj` so the flow's stop sweep kills it. `ADK_FLOW_BOUND` / `--flow-bound` on `Activities.FlowBound` selects the flow-owned wrapper for when the spike binds to a real flow's container.

## Prompt seam

`prompt.go` holds the clean seam for the production pentester prompt: `PentesterPromptVars(input)` returns the template variables `flowProvider.GetPentesterHandler` fills — question, docker image/cwd/ports, execution context, language, current time, tool placeholder, user files — plus every `*ToolName` variable gated on the tools actually registered, so a reduced toolset renders a prompt that never names tools the agent cannot call. The function is pure and imports nothing from `pkg/templates`; Phase 2 renders the template through `templates.Prompter` and injects the result via `PentestRequest.Instruction`/`Question` (or `directOptions` in direct mode). The instruction goes through `InstructionProvider`, not `llmagent.Config.Instruction`, because ADK expands `{name}` placeholders in the static field and a rendered production prompt carries literal `{target_ip}`-style markers that must reach the model untouched.

## Human-in-the-loop and scope seam

`PendingConfirmations`/`ConfirmationResponse` drive the durable HITL loop in the workflow: a pass either completes or pauses on an `adk_request_confirmation` call, and the workflow waits on a Temporal signal for the decision, surviving worker restarts. None of the current tools request confirmation — activity-backed tools cannot — so the loop runs exactly one pass today.

`ScopeGuard` in `agent.go` is the isolated seam for the authorization checks the production port will wire; it is unset and must default to allowing. The existing prompts and the scope/authorization guards elsewhere are untouched on purpose.

## Running

Credentials live outside the repository: export them in the shell or keep a `~/.pentagi-secrets.env` file — a `.env` in the tree is gitignored but outside it cannot be committed at all. Nothing secret goes to GitHub; the repo is public.

```bash
cd backend
# in-process, no Temporal — the fast comparison path
ADK_API_KEY=... go run ./cmd/adkpentest direct "probe https://scanme.sh and summarize"

# durable path: dev server plus worker plus a run
temporal server start-dev &
ADK_API_KEY=... go run ./cmd/adkpentest worker
ADK_API_KEY=... go run ./cmd/adkpentest run "probe https://scanme.sh and summarize"

# mark the worker's terminal commands as owned by its flow (env or flag)
ADK_FLOW_BOUND=true go run ./cmd/adkpentest worker
go run ./cmd/adkpentest worker --flow-bound

# Temporal Cloud instead of a local server
export TEMPORAL_HOST_PORT=<namespace>.tmprl.cloud:7233
export TEMPORAL_NAMESPACE=<namespace>.<account>
export TEMPORAL_API_KEY=...   # enables TLS + API-key auth
```

`ADK_SANDBOX=local` (default) executes on the host — development only, unsandboxed, but the spawned command sees only a small env allowlist (`PATH`, `HOME`, locale basics) so the worker's `ADK_API_KEY`/`TEMPORAL_API_KEY`/AWS credentials cannot be read back by the tools. `ADK_SANDBOX=docker` with `ADK_SANDBOX_CONTAINER=<name>` execs into a running container through the same exec calls `docker.DockerClient` makes; binding the sandbox to the flow's primary container is the production wiring left for the port. `ADK_PAYLOAD_BUCKET` enables S3 offload for outputs over `ADK_PAYLOAD_THRESHOLD` (16 KiB default, matching `tools.DefaultResultSizeLimit`); without it a bounded in-memory store holds payloads for the run.

## Verified so far

- Unit tests cover the whole loop offline: `googleadk.NewFakeModel` scripts tool-call-then-answer turns through the real `PentesterWorkflow` in the Temporal test environment, asserting per-activity dispatch counts (one `InvokeModel` per turn, one attempt for non-retryable validation errors) and that tool errors reach the model per ADK's contract. A scripted `hack_result` call ends the workflow with `HackResult` populated and no further `InvokeModel` activity — the same barrier in direct mode.
- The chat-completions adapter is exercised against an httptest server asserting path, `Authorization: Bearer`, model name and response decoding.
- `LocalSandbox` semantics: exit codes, timeout kill, output cap, and the detached `StillRunning` start verdict.
- Terminal parity: timeout normalization across the production bound table, the `still running` detach string, the exact timeout-HINT text, the exit-code/interrupted result format, and the flow-bound `oom_score_adj` command marker.
- File activity: positional read/write argv, the `edit_file` refusal, missing-action inference, shell-expansion path rejection, hostile-but-legal paths staying positional, and the executor's exact truncation markers at the 32 KiB boundary.
- The prompt seam: `PentesterPromptVars` keys match the production map, tool-name variables appear only for registered tools, and instruction/question overrides reach the model request in both modes.
- Tool activities heartbeat every 5 s (with elapsed-time details) against a 30 s `HeartbeatTimeout`, and `InvokeModel` gets the same heartbeat timeout plus a bounded retry via `WithModelActivityOptions`, so a dead worker or wedged provider is detected in tens of seconds instead of waiting out `StartToCloseTimeout`; a live worker-restart run on Temporal Cloud confirmed the retry-onto-new-worker path. The exec timeout is clamped to the activity's remaining deadline (detached commands exempt — their point is to outlive the call), and parent cancellation propagates as `context.Canceled` while the tool's own deadline produces the graceful `timed_out` result. Cancellation also kills the whole process group — locally via `Setpgid`, in Docker via a PID marker file and a cleanup exec.

## Open gaps for the port

Phase 1 closed: the production `terminal`/`file`/`hack_result` tool surface with verbatim schemas and descriptions, terminal timeout/detach/result parity, the `hack_result` loop barrier in both modes, flow-bound command marking, and the rendered-prompt injection seam.

Remaining:

- Sub-agent tools (`pentester`, `coder`, `searcher`, `advice`, `memorist`, `maintenance`, `search`/`web_search`, guide stores, `graphiti_search`) — `PentesterPromptVars` already maps their variable names; the activities and handlers are not ported.
- `edit_file` diff application (needs container copy plumbing beyond the exec surface) and `ask`/`ask_user` for durable user interaction beyond the confirmation signal.
- Event bridge to production observability: ADK events are collected but not yet published as flow logs/termlogs/Langfuse observations.
- The fixer and reflector chains — the tool-call fixer on malformed calls and the post-run reflector are production prompt-driven flows with no ADK equivalent yet.
- Session carry-over across continue-as-new (`googleadk.ExportSession`/`ImportSession`) for long tasks — the workflow is single-run today, and `pentagi.hack_result` state is extracted from events rather than read back from an exported session.
- Fetching `payload_uri` blobs back when the model needs more than the preview.
- Binding `DockerSandbox` to the flow's primary container (the flow-bound wrapper is wired; the lifecycle is not), plus mapping PentAGI's task/subtask lifecycle and message chains onto ADK sessions.
- The authorization guard wiring — `ScopeGuard` stays the isolated seam.
