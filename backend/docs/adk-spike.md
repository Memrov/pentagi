# ADK Go spike: durable pentester agent

`pkg/adk` evaluates replacing the langchaingo agent loop with Google ADK Go 2.0 while keeping Temporal as the only durability and orchestration layer. It is a spike: nothing in production code paths calls it, and it is exercised through `cmd/adkpentest` and its own tests.

## Architecture

The integration is `go.temporal.io/googleadk`. The agent loop runs inside `pentagi.adk.PentesterWorkflow`; each LLM call is dispatched to the plugin's `InvokeModel` activity and each tool call to one of our activity implementations, so every step is retried, timed out and replayable under Temporal rather than the agent framework.

- Workflow side (`workflow.go`, `tools.go`): builds the `llmagent` pentester with `googleadk.NewModel(name)` — a name-only model handle — plus three tools exposed through `googleadk.ActivityAsTool`. `ActivityAsTool` infers the model-facing schema from the activity's argument struct and dispatches the activity by name, so the tool name and the registered activity name are one constant.
- Worker side (`worker.go`, `model.go`, `activities.go`, `backend.go`, `payload.go`): `WorkerPlugin` registers `InvokeModel` with a `ModelFactory` that rebuilds the real `openaimodel` inside the activity boundary over one shared HTTP client. The API key lives in the factory closure and never enters workflow history. `Activities` runs each tool inside a `Sandbox` — `DockerSandbox` for a container or `LocalSandbox` for development — and hands oversized output to a `PayloadStore` (S3, or a bounded in-memory store when no bucket is configured) instead of bloating history.
- Retries are owned by Temporal alone: the openai-go client is constructed with `WithMaxRetries(0)` so a transient failure is not retried twice over.

The model is any OpenAI-compatible chat-completions endpoint. The default target is **OpenCode Go** (the subscription tier): `ADK_BASE_URL=https://opencode.ai/inference/go/openai/v1`, `ADK_MODEL_NAME=deepseek-v4.1-flash` (verified in the live Go catalog; Console translates every text model across APIs, so the chat-completions path serves all of them). `ADK_MODEL_API` selects `chat_completions` (default) or `responses` for providers that speak the Responses API. Go's terms require a client-specific user agent and a stable `x-opencode-session` per conversation — `ModelFactory` sends `pentagi-adk/1.0` and derives the session from the workflow ID, so every turn of a run shares one value including across retries. DeepSeek models require Settings › Privacy › Regions: Global on the OpenCode workspace.

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

# Temporal Cloud instead of a local server
export TEMPORAL_HOST_PORT=<namespace>.tmprl.cloud:7233
export TEMPORAL_NAMESPACE=<namespace>.<account>
export TEMPORAL_API_KEY=...   # enables TLS + API-key auth
```

`ADK_SANDBOX=local` (default) executes on the host — development only, unsandboxed, but the spawned command sees only a small env allowlist (`PATH`, `HOME`, locale basics) so the worker's `ADK_API_KEY`/`TEMPORAL_API_KEY`/AWS credentials cannot be read back by the tools. `ADK_SANDBOX=docker` with `ADK_SANDBOX_CONTAINER=<name>` execs into a running container through the same exec calls `docker.DockerClient` makes; binding the sandbox to the flow's primary container is the production wiring left for the port. `ADK_PAYLOAD_BUCKET` enables S3 offload for outputs over `ADK_PAYLOAD_THRESHOLD` (16 KiB default, matching `tools.DefaultResultSizeLimit`); without it a bounded in-memory store holds payloads for the run.

## Verified so far

- Unit tests cover the whole loop offline: `googleadk.NewFakeModel` scripts tool-call-then-answer turns through the real `PentesterWorkflow` in the Temporal test environment, asserting per-activity dispatch counts (one `InvokeModel` per turn, one attempt for non-retryable validation errors) and that tool errors reach the model per ADK's contract.
- The chat-completions adapter is exercised against an httptest server asserting path, `Authorization: Bearer`, model name and response decoding.
- `LocalSandbox` semantics: exit codes, timeout kill, output cap.
- Tool activities heartbeat every 5 s (with elapsed-time details) against a 30 s `HeartbeatTimeout`, and `InvokeModel` gets the same heartbeat timeout plus a bounded retry via `WithModelActivityOptions`, so a dead worker or wedged provider is detected in tens of seconds instead of waiting out `StartToCloseTimeout`; a live worker-restart run on Temporal Cloud confirmed the retry-onto-new-worker path. The exec timeout is clamped to the activity's remaining deadline, and parent cancellation propagates as `context.Canceled` while the tool's own deadline produces the graceful `timed_out` result. Cancellation also kills the whole process group — locally via `Setpgid`, in Docker via a PID marker file and a cleanup exec.

## Open gaps for the port

- Binding `DockerSandbox` to the flow's primary container; cancellation uses its own per-exec PID markers, not yet synced with `pkg/docker`'s kill sweep.
- Fetching `payload_uri` blobs back when the model needs more than the preview.
- Session carry-over across continue-as-new (`googleadk.ExportSession`/`ImportSession`) for long tasks — the workflow is single-run today.
- Mapping PentAGI's task/subtask lifecycle, message chains and Langfuse observations onto ADK sessions and plugin telemetry.
- The production pentester prompt and the authorization guard are user-owned; the spike instruction is deliberately minimal.
