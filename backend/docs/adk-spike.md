# ADK Go spike: durable pentester agent

`pkg/adk` evaluates replacing the langchaingo agent loop with Google ADK Go 2.0 while keeping Temporal as the only durability and orchestration layer. Phase 2 wires it into production behind a per-flow flag: a flow whose `engine` column reads `adk` runs each input as a child `pentagi.adk.PentesterWorkflow` under `pentagi.FlowWorkflow`, while `legacy` flows — the default and every pre-migration row — take the unchanged `pentagi.ProcessFlowInput` path. Phase 4 adds the production guardrails (repeat detection, iteration cap, barrier recovery), the first delegated sub-agent tool (`advice`), and continue-as-new across workflow history rollover. The `cmd/adkpentest` CLI remains the standalone exercise path.

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

`PendingConfirmations`/`ConfirmationResponse` drive the durable HITL loop in the workflow: a pass either completes or pauses on an `adk_request_confirmation` call, and the workflow waits on a Temporal signal for the decision, surviving worker restarts. None of the current tools request confirmation — activity-backed tools cannot — so today a pass ends either on `hack_result` or on a text answer the workflow nudges back into the agent (see Phase 4).

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
- The Phase 3 bridge: scripted runs assert one `EmitFlowEvent` per session event with the translated ops (call start/end pairs, the hack_result `done` row), an emit failure leaves the workflow green, the adapter tests verify toolcall/msglog/termlog/status writes against fake workers including UTF-8 sanitization and start-idempotency, and the termlog seam is proven before-and-after exec through the activity env.
- The Phase 4 guardrails: the repeat detector's threshold/replay/abort sequence and its reset on changed calls are unit-tested, a reissued `terminal` call aborts the workflow with the non-retryable `RepeatingToolCallAbort` error after exactly two real executions, the iteration cap ends each pass at the boundary and seeds the synthetic failure, a text-only end is nudged `maxBarrierRuns` times then reported as a failed `HackResult`, and the seeded `BarrierRetries` count proves retries survive continue-as-new. `advice` is proven end to end — the parent's call runs the adviser on the same `InvokeModel` activity, the rendered mentor instruction and JSON args reach it, and its answer returns to the parent as the tool result before `hack_result` closes the run; the emit bridge records the delegation as one toolcall row pair. Continue-as-new is proven by running each continued request in a fresh test environment and asserting the resumed model request carries the imported transcript, plus a direct `ExportSession`/`ImportSession` round-trip.

## Production wiring (Phase 2)

The `flows.engine` column (`FLOW_ENGINE` enum: `legacy`, `adk`, default `legacy`) picks the executor per flow. `FLOW_ENGINE` in `pkg/config` stamps new rows at `reserveFlow` time — unknown values normalize to `legacy` — and `cmd/pentagi` resolves the row's engine into `FlowParams` whenever a workflow is started or signalled, falling back to `legacy` when the lookup fails.

`pentagi.FlowWorkflow` stays the durable input sequencer for both engines. For `engine=adk` each dequeued input runs as a child workflow instead of the `ProcessFlowInput` activity:

- `pentagi.RenderAgentPrompt` runs first on the flows queue — the user prompter and the container row are worker-side, so the pentester/`question_pentester` pair renders in an activity through the same `newUserPrompter` path the controller uses. A render failure is non-retryable: the input still dispatches and the child falls back to its built-in default prompt.
- The child runs on the scoped `ADK_TASK_QUEUE` (`<tenant->pentagi-adk`) as `pentagi.adk.PentesterWorkflow` with the rendered `Instruction`/`Question`, the flow's model and the tool timeout from `FlowParams.ADK` — config values live in params because workflow code cannot read env on replay.
- The child id is deterministic: `<tenant->pentagi-flow-<flowID>-input-<seq>` where `seq` is the workflow's processed-input counter carried across continue-as-new. Reattachment works through history dedup, not a conflict policy — `workflow.ChildWorkflowOptions` in SDK v1.51 exposes no `WorkflowIDConflictPolicy`, and none is needed: a replayed parent re-executes the same `ExecuteChildWorkflow` command and Temporal answers it from the recorded event, while the default parent-close policy terminates children with their parent so a same-id child cannot still be running when a new parent run arrives.
- A finish signal cancels the in-flight child context and `WaitForCancellation` waits out its unwind; inputs signalled during a child run buffer in `pending` exactly as they do for the legacy path. A completed child leaves a compact `{"event_count":…,"hack_result":…}` summary on `FlowState.LastResult` — answer text deliberately stays out of history.
- `durableRecover` short-circuits for adk flows: the InputMark/task-id recovery exists for the legacy task chain only, and child workflows own their own inputs.

The second worker shares the server process: when `TEMPORAL_ENABLED` and `ADK_WORKER_ENABLED` (default `true`) are set, `cmd/pentagi` loads `adk.Config`, registers `PentesterWorkflow`, the four tool activities and the `WorkerPlugin` model activities on the scoped adk queue, and publishes `FlowParams.ADK` only once the worker can actually serve it — a misconfigured `ADK_*` setup logs a warning and leaves adk inputs dropped with a workflow error rather than parked on a queue nobody polls. The model API key never enters workflow history: it lives worker-side in the plugin factory.

Tool activities resolve their sandbox per attempt: an activity whose workflow id parses as a flow child id (`adk.FlowIDFromWorkflowID`) binds a `DockerSandbox` to the flow's primary container via `controller.NewADKSandboxResolver`, and anything else — a CLI spike run, a hand-typed id — falls back to `ADK_SANDBOX_CONTAINER`. `ADK_SANDBOX=docker` is required for that binding; `local` keeps everything on the host for development.

## Event bridge (Phase 3)

Phase 3 makes an adk run visible in the existing UI without any new GraphQL types or frontend work: the child workflow translates each `session.Event` into `adk.FlowEvent` ops and executes `pentagi.adk.EmitFlowEvent` once per event on the **flows** queue — the queue where the flow's in-memory log workers live, which is why the child cannot inherit its own queue. `EmitEvents`/`FlowID`/`EmitTaskQueue` ride on `PentestRequest`; the parent sets them for engine=adk inputs and CLI runs leave them zero. The emit call is one short activity (60 s, 3 attempts) whose failure is logged and swallowed — observability is non-critical, a dead flows queue must not stall a pentest.

The flows-queue activity delegates to `durableRunner.EmitFlowEvent`, which resolves the flow through the same `fc.GetFlow` registry the run itself uses, then to `adkEventSink` — a per-flow-worker adapter (`controller/adkevents.go`) writing through the existing workers, so persist→publish→subscription is exactly the legacy pipeline. Event-to-row mapping:

| ADK event shape | Ops emitted | Rows written |
| --- | --- | --- |
| `FunctionCall` part | `tool_call_start` (+ `message` when the call is `hack_result`) | `toolcalls` row `running` (call_id, name, args) + paired `msglogs` row — type from the tool (`terminal`→terminal, `file`→file, `nuclei_scan`/`httpx_probe`→search, else thoughts), headline = args' `message` or the tool name |
| `FunctionResponse` part | `tool_call_end` | `toolcalls` row `finished`/`failed` with folded result text (`output`, plus `[exit code: N]`/`[payload: …]`/`[timed out]`/`{error: …}` markers) and duration; paired `msglogs` row gets the same result with format terminal/markdown/plain like production |
| `hack_result` call | extra `message` op | one `done` row: headline = `message` arg, result = the report — production's report barrier row |
| text part | `message` (thoughts) | `msglogs` thoughts row |
| `Thought` part | folded into the sibling ops | lands on the paired row's `thinking` column |
| run boundary | `flow_status` | `flows.status` running before the child, waiting after — the pair the legacy task worker maintains itself; emitted by the **parent** because a cancelled child never sees its own end |

Start/end correlation is `callID → {toolcall id, msglog id}` in adapter memory; a close whose start predates a backend restart falls back to the toolcalls `call_id` index (the msglog row has no such index and keeps its headline — the same loss a mid-call crash costs production). `ToolCallStart` is idempotent per call id so a retried emit cannot double-open rows. Everything crosses `database.SanitizeUTF8` at the adapter; the workers' own 2048-char headline truncation and sanitization are reused, not duplicated.

Terminal output reaches the termlog on a second seam: `Activities.TermLogFor` resolves the same per-flow sink by the activity's workflow id (nil resolver → the CLI path stays quiet), and `TerminalExec` writes styled `stdin` before exec and `stdout` after, matching the production `/work $ cmd` framing and green-output styling. The sink is called directly — activities cannot schedule activities — and its failures downgrade to warnings, never a failed command.

## Guardrails, delegation and rollover (Phase 4)

Phase 4 closes the loop-safety gap between the spike and `performAgentChain`, adds the first sub-agent delegation, and survives Temporal history rollover.

| Production behavior | ADK equivalent | Where |
| --- | --- | --- |
| `RepeatingToolCallThreshold = 3`, soft detections at calls 3–6 replaying the previous answer, abort at 7 with `maxSoftDetectionsBeforeAbort` | `repeatDetector` in a `BeforeToolCallback` — identical normalized call #3+ returns the cached result plus a `repeat_notice`; call #7 sets a non-retryable `RepeatingToolCallAbort` application error that `BeforeModelCallback` propagates out of the run | `guards.go` |
| Comparison strips the volatile `message` arg and canonicalizes argument ordering | `callKey` drops `message` and marshals the rest (Go's `json.Marshal` sorts map keys) | `guards.go` |
| Detector resets when the tool or normalized args change; per-chain detector | `lastKey`/`consecutive` reset per change; `newRunGuards` is rebuilt per `runner.Run` pass because callbacks bind at agent construction | `guards.go`, `workflow.go` |
| `maxGeneralAgentChainIterations = 100` | `runGuards.beforeModel` counts model calls per pass (`PentestRequest.MaxIterations`, default 100); past the cap it answers for the model with an "Iteration limit" text, ending the pass without another activity | `guards.go` |
| Reflector decides whether a chain that ended without result gets rerun (≤ `maxReflectorCallsPerChain`) | The workflow nudges a barrier-less pass with a `hack_result` instruction; after `maxBarrierRuns` (3) passes it returns a synthetic `HackResult{Success:false}` — a text-only answer can no longer pass for success | `workflow.go`, `direct.go` |
| `advice` delegates to a mentor chain | `agenttool.New(adviser)` wraps an `llmagent` adviser; `SkipSummarization:false` keeps it an ordinary tool so the parent continues to `hack_result`. The input schema mirrors `tools.AskAdvice` (`question`+`message` required, `code`/`output` optional); `pentagi.RenderAgentPrompt` renders `adviser.tmpl` worker-side under `AgentTypeAdviser`, and the nested run rides the same `InvokeModel` activity | `advice.go`, `prompter.go` |
| Long conversations must not blow up workflow history | On `GetContinueAsNewSuggested()` the workflow exports the session with `googleadk.ExportSession` at a function-response boundary (never on the `hack_result` response, which would replay the barrier), carries the snapshot plus `BarrierRetries` in the next run's `PentestRequest`, and `ImportSession` rebuilds it before `runner.Run` resumes with no new user message | `workflow.go` |

Deliberate simplifications against production: the iteration cap is per pass rather than absolute across reflector reruns (each ADK invocation is one chain, matching production's per-chain counters); the missing-barrier nudge is a fixed instruction, not the production reflector's prompt-driven verdict; `advice` skips the enricher sub-agent that rewrites the question in production, which needs the unported search/memory tools; and a repeated-call replay returns the cached result plus a notice field rather than production's exact refusal text.

## Open gaps for the port

Phase 1 closed: the production `terminal`/`file`/`hack_result` tool surface with verbatim schemas and descriptions, terminal timeout/detach/result parity, the `hack_result` loop barrier in both modes, flow-bound command marking, and the rendered-prompt injection seam.

Phase 2 closed: the per-flow engine flag end to end (migration, `FLOW_ENGINE`, row stamping, engine lookup into `FlowParams`), child dispatch on the scoped adk queue with deterministic ids, the `pentagi.RenderAgentPrompt` activity on the production prompt path, the in-process adk worker, flow-bound sandbox resolution, and the adk bypass of task-level recovery.

Phase 3 closed: the `pentagi.adk.EmitFlowEvent` activity on the flows queue, the `session.Event → FlowEvent` translation with toolcall/msglog/flow-status rows through the existing workers, the terminal `TermLogFor` sink seam, and the running/waiting status bracket the parent emits around each child.

Phase 4 closed: the repeat-call detector with replay and non-retryable abort, the per-pass iteration cap, workflow-level missing-barrier nudges with the reflector budget and synthetic failure, the `advice` sub-agent tool via `agenttool` with the rendered adviser prompt, and continue-as-new over an exported/imported ADK session snapshot.

Remaining:

- Sub-agent tools (`pentester`, `coder`, `searcher`, `memorist`, `maintenance`, `search`/`web_search`, guide stores, `graphiti_search`) — `PentesterPromptVars` already maps their variable names; `advice` is the pattern they follow. The production adviser also passes through a question-enricher agent, deferred until the memory/search tools exist.
- agentlog and searchlog rows: the bridge writes msglogs, toolcalls and termlogs only — the `agentlogs`/`searchlogs` feeds production fills for sub-agent turns and search results have no ADK source events yet (nested agent-tool runs emit no parent-session events, so `advice` lands as one toolcall pair like production's tool row).
- `edit_file` diff application (needs container copy plumbing beyond the exec surface) and `ask`/`ask_user` for durable user interaction beyond the confirmation signal.
- The fixer and full reflector chains — the tool-call fixer on malformed calls is unnecessary under ADK's `{"error": …}` tool-error contract, and the missing-barrier nudge covers the reflector's retry role; the prompt-driven reflector verdict itself is not ported.
- Per-user provider resolution: the adk model is one global `ADK_*` config rather than the flow's configured provider and model, so an adk flow ignores its provider row.
- Session carry-over across continue-as-new (`googleadk.ExportSession`/`ImportSession`) for long tasks — the workflow is single-run today, and `pentagi.hack_result` state is extracted from events rather than read back from an exported session.
- Fetching `payload_uri` blobs back when the model needs more than the preview (the marker already folds into the toolcall/msglog result text).
- Mapping PentAGI's task/subtask lifecycle and message chains onto ADK sessions — bridge rows are flow-level with nil task/subtask ids.
- The authorization guard wiring — `ScopeGuard` stays the isolated seam.
