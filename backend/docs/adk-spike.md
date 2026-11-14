# ADK Go spike: durable pentester agent

`pkg/adk` evaluates replacing the langchaingo agent loop with Google ADK Go 2.0 while keeping Temporal as the only durability and orchestration layer. It is a spike: nothing in production code paths calls it, and it is exercised through `cmd/adkpentest` and its own tests.

## Architecture

The integration is `go.temporal.io/googleadk`. The agent loop runs inside `pentagi.adk.PentesterWorkflow`; each LLM call is dispatched to the plugin's `InvokeModel` activity and each tool call to one of our activity implementations, so every step is retried, timed out and replayable under Temporal rather than the agent framework.

- Workflow side (`workflow.go`, `tools.go`): builds the `llmagent` pentester with `googleadk.NewModel(name)` — a name-only model handle — plus three tools exposed through `googleadk.ActivityAsTool`. `ActivityAsTool` infers the model-facing schema from the activity's argument struct and dispatches the activity by name, so the tool name and the registered activity name are one constant.
- Worker side (`worker.go`, `model.go`, `activities.go`, `backend.go`, `payload.go`): `WorkerPlugin` registers `InvokeModel` with a `ModelFactory` that rebuilds the real `openaimodel` inside the activity boundary. The API key lives in the factory closure and never enters workflow history. `Activities` runs each tool inside a `Sandbox` — `DockerSandbox` for a container or `LocalSandbox` for development — and hands oversized output to a `PayloadStore` (S3 URI today, in-memory for tests) instead of bloating history.
- Retries are owned by Temporal alone: the openai-go client is constructed with `WithMaxRetries(0)` so a transient failure is not retried twice over.

The model is any OpenAI-compatible chat-completions endpoint. Default target is OpenCode Zen: `ADK_BASE_URL=https://opencode.ai/zen/v1`, `ADK_MODEL_NAME=deepseek-v4.1-flash` (the current DeepSeek Flash; Zen model IDs change, check the live catalog). `ADK_MODEL_API` selects `chat_completions` (default, what Zen's DeepSeek models serve) or `responses` for providers that speak the Responses API.

## Human-in-the-loop and scope seam

`PendingConfirmations`/`ConfirmationResponse` drive the durable HITL loop in the workflow: a pass either completes or pauses on an `adk_request_confirmation` call, and the workflow waits on a Temporal signal for the decision, surviving worker restarts. None of the current tools request confirmation — activity-backed tools cannot — so the loop runs exactly one pass today.

`ScopeGuard` in `agent.go` is the isolated seam for the authorization checks the production port will wire; it is unset and must default to allowing. The existing prompts and the scope/authorization guards elsewhere are untouched on purpose.

## Running

```bash
cd backend
# in-process, no Temporal — the fast comparison path
ADK_API_KEY=... go run ./cmd/adkpentest direct "probe https://scanme.sh and summarize"

# durable path: dev server plus worker plus a run
temporal server start-dev &
ADK_API_KEY=... go run ./cmd/adkpentest worker
ADK_API_KEY=... go run ./cmd/adkpentest run "probe https://scanme.sh and summarize"
```

`ADK_SANDBOX=local` (default) executes on the host — development only, unsandboxed. `ADK_SANDBOX=docker` with `ADK_SANDBOX_CONTAINER=<name>` execs into a running container through the same exec calls `docker.DockerClient` makes; binding the sandbox to the flow's primary container is the production wiring left for the port. `ADK_PAYLOAD_BUCKET` enables S3 offload for outputs over `ADK_PAYLOAD_THRESHOLD` (16 KiB default, matching `tools.DefaultResultSizeLimit`).

## Verified so far

- Unit tests cover the whole loop offline: `googleadk.NewFakeModel` scripts tool-call-then-answer turns through the real `PentesterWorkflow` in the Temporal test environment, asserting per-activity dispatch counts (one `InvokeModel` per turn, one attempt for non-retryable validation errors) and that tool errors reach the model per ADK's contract.
- The chat-completions adapter is exercised against an httptest server asserting path, `Authorization: Bearer`, model name and response decoding.
- `LocalSandbox` semantics: exit codes, timeout kill, output cap.

## Open gaps for the port

- Binding `DockerSandbox` to the flow's primary container and syncing `pkg/docker`'s flow markers for the kill sweep.
- Fetching `payload_uri` blobs back when the model needs more than the preview.
- Session carry-over across continue-as-new (`googleadk.ExportSession`/`ImportSession`) for long tasks — the workflow is single-run today.
- Mapping PentAGI's task/subtask lifecycle, message chains and Langfuse observations onto ADK sessions and plugin telemetry.
- The production pentester prompt and the authorization guard are user-owned; the spike instruction is deliberately minimal.
