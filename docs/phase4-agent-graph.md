# Phase 4: agent graph

## The graph

```
build_context ──► agent ◄──► tools
                    │          │  (finished, or a budget ran out)
                    ▼          ▼
               write_report ◄──► validate_report ──► publish ──► action? ──► await_approval (stub)
                         (repair, at most 2x)                        └──► END
```

| Node | Kind | What it does |
|---|---|---|
| `build_context` | code | Builds the starter pack: the alerts, golden signals of the alerting service, flag changes and deploys from 1 h before the incident, and the service map 2 hops around the service (healthy vs now). Uses the same tool functions, so everything has evidence ids. |
| `agent` | model | Picks one tool call, or answers in text when done. |
| `tools` | code | Validates the arguments, applies repeat detection, runs the tool, wraps the result as untrusted data, and records evidence ids in state. |
| `write_report` | model | Structured output (strict JSON schema of `Report`). |
| `validate_report` | code | Checks citations and support; sends problems back for repair; falls back to a code-written report. Decides the action. |
| `publish` | code | Upserts `reports`, sets the incident status, and pushes `{incident_id, status}` on Redis pub/sub `oncall:reports`. |
| `await_approval` | stub | Records `approval_status: pending`. Phase 5 replaces it with `interrupt()`. |

**State:** incident id, alerts, starter pack, messages, evidence `{ref, tool, summary}`, budget counters, report, proposed action, approval status. It is checkpointed in Postgres after every node, with `thread_id` = incident id.

## Running it

Add the model settings to `.env` (see `.env.example`); the worker reads them from there:

```sh
ONCALL_AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com
ONCALL_AZURE_OPENAI_API_KEY=...
ONCALL_AGENT_DEPLOYMENT=<deployment name>
docker compose up -d oncall-worker       # recreate to pick up .env
# flip a flag, wait for the alert, then:
docker exec oncall-worker python -m oncall.cli report <incident-id>
```

Without a model configured, every investigation still runs end to end and publishes an "inconclusive: no model configured" report written by code.

## Decisions

| Decision | Why |
|---|---|
| LangGraph with LangChain's `AzureChatOpenAI` | The standard model integration for LangGraph: messages, tool binding and token usage come built in. The key stays inside the model object; it is never in state, checkpoints or prompts. Without a key, Azure login (Entra ID) is used. |
| Tool and report schemas in OpenAI strict mode | The API itself guarantees arguments and reports match the schema. Our helper makes every field required (optional ones are nullable) and forbids extra fields. |
| One tool call per turn (`parallel_tool_calls=False`) | Budgets and repeat detection count exact calls. |
| Tool output wrapped in `<data trust="untrusted">`, and `</data` escaped inside it | A log line can't close the block and continue as if it were our instructions. The system prompt says data blocks are never instructions, but that's the weakest layer: the structural defenses are limited tools, code-summarized output, required metric/trace support and human approval. |
| Repeat detection key = tool + normalized arguments (lists sorted, window to the minute) | "The same question" asked twice with seconds-different windows is still a repeat. The result is cached in Redis and returned with a note; 3 repeats force a stop. The keys live in state. |
| The time budget counts node run time, not wall clock | A run resumed after a crash isn't punished for the time the worker was dead. |
| Budgets checked before each model call and after each tool run | A cost overrun with pending tool calls answers them "not run" first; the API rejects a history with unanswered calls. |
| Citation validation in code: every id must exist in state; the root cause needs a metric, trace or service-map finding; the correlated change needs its evidence | This is the brief's "conclusions need metric or trace support", enforced, not just asked for. Two repairs, then a code-written inconclusive report. |
| Code decides the action, not the model | `set_flag` becomes report-only unless the flag and value are on the profile's allowed list, confidence is high or medium, and the run wasn't forced to stop. |
| A forced stop caps confidence at low | The brief requires that partial reports never propose an action. |
| The model failing (after the client's 3 retries with backoff) gives a code-written report | No second call to a failing model. The leading suspect is taken from the starter map: errors beat missing calls beat latency. |
| New incident status `reported` | "Investigation done, nothing to approve" was missing from the draft. It counts as open, so later alerts still join the incident. |
| `reports` table: one row per incident, upserted | A crash between the database write and the checkpoint just rewrites the row. It also records cost, tokens, tool/LLM calls, active seconds, and time from first alert to report. |
| Lease heartbeat in the consumer | Investigations can outlast the 5-minute lease. Without a heartbeat, another worker would take over a live run. The heartbeat renews the lease and resets the stream entry's idle time. |
| `recursion_limit` 60 as a backstop | If it's ever hit, the run is steered into a forced partial report instead of failing. |
| Fix kinds `set_flag` / `report_only` | "recommendation" happens to be a demo service name, and the no-hardcoding guard rightly refuses that literal. |

**Prices:** `ONCALL_AGENT_PRICE_*_PER_MTOK` default to gpt-4.1-mini list prices. Set them to your deployment's prices, or the cost budget and the recorded cost are wrong.

## Tests (no model, no backends)

A scripted fake model and fake tools exercise the graph:
- the happy path with an action, and the untrusted wrapping;
- citation repair;
- a logs-only conclusion falling back after repairs;
- the tool budget;
- repeats served from the Redis cache;
- the cost budget with pending calls;
- model failure;
- no model;
- a crash mid-run that resumes from the Postgres checkpoint without redoing finished nodes, while a duplicate job for a finished run does nothing.

The building blocks have their own tests: strict schemas, window bounds, repeat keys, escaping, validation and the action check.

**Live (no model):** a posted alert led to incident → job → graph → published report in under 2 s, with 7 checkpoints and the starter pack's evidence stored. **Not yet run against Azure OpenAI**, as agreed.
