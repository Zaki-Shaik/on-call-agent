# Phase 2: worker, adapters and tools

## Layers

```
tools/         query_metrics  get_traces  search_logs  list_recent_changes
               (backend-neutral: baselines, onsets, patterns, critical path, evidence)
                  │ structured calls: (service, signal, window), never a query language
telemetry/     MetricsBackend   TraceBackend   LogBackend      flags/ FlagProvider
               PrometheusMetrics JaegerTraces  OpenSearchLogs         FlagdUiProvider
                  │ HTTP with per-backend timeouts; failures raise BackendError
backends       Prometheus (span metrics + docker_stats)  Jaeger  OpenSearch  flagd-ui
```

- **Adapters own the query language.** Tools only ever ask for, say, `(payment, error_rate, window)`.
- **Adapters return neutral types** (`Series`, `Trace`/`Span`, `LogRecord`, `FlagState`).
- **A second backend** is one adapter class plus one entry in `telemetry/factory.py`.

## Tool outputs

| Tool | Returns | Size on the live incident |
|---|---|---|
| `query_metrics` | Per signal: baseline (median of the previous hour), current, change %, direction, robust z-score, onset time | ~1.6 KB for all 7 signals |
| `get_traces` | Per operation: calls, errors, p50/p95, top error messages. Downstream/upstream edges with errors and an example error trace. Critical-path share by service | ~3.7 KB |
| `search_logs` | Severity counts (window vs baseline), top 10 patterns with count, first/last seen, new-vs-baseline, example trace | ~1.5 KB |
| `list_recent_changes` | Flag changes and deploys: time, type, service, actor, description | ~0.4 KB |

Every finding is saved as evidence with a short per-incident id (`E1`, `E2`, ...) and a deep link.
The model sees only the id; links stay in Postgres for the report.

**Statuses are explicit.**
- `ok`.
- `no_data`: the backend answered and has nothing. Never reported as 0.
- `error`: the backend failed or timed out, with the reason. A failure is not stored as evidence.

## Decisions

| Decision | Why |
|---|---|
| Baseline = median of the previous hour; noise = MAD (median absolute deviation) | Robust to the spikes an incident window contains; no fixed thresholds per signal |
| Onset = first point of 3 consecutive points beyond 3 robust sigmas (noise floor 5% of baseline) | Statistical "departs from normal", not a severity judgement; one spike or a last-minute blip is not an onset |
| No z-score against a flat zero baseline | Any non-zero value would score as "infinitely" unusual; "baseline 0 -> 0.5" is the honest description |
| Error rate = `(errors or vector(0)) / total` | Span metrics only create the error series after the first error. Without this, a service that never failed has "no baseline" instead of 0% |
| `new` is `null` when the baseline hour has no logs | "New" can't be claimed without something to compare with |
| Critical path computed from spans (walk back from the end, last-finishing child wins) | Shows where the time of the slowest requests actually goes, not just which spans are slow |
| Kafka consumers: caller taken from span links / destination | Phase 0 found consumers link to the producer instead of being its child |
| Short evidence ids from an atomic per-incident counter | Cheaper in tokens and harder for the model to mistype than UUIDs; no duplicate ids under concurrency |
| Worker claims an incident with a lease (`lease_expires_at`) | Redis redelivers a crashed worker's job (`XAUTOCLAIM` after the lease); the lease makes sure only one worker runs it, and a finished run is never repeated |
| Jobs failing 3 times are dropped (logged) | A poison job must not loop forever. Phase 4 adds the forced partial report for LLM failures |
| Sweeper + change recorder run in exactly one worker (Postgres advisory lock as leader election) | No duplicate change events or re-enqueues with several workers; leadership moves automatically if the leader dies |
| Flag history: poll every 5 s and diff | Your choice. Accurate to 5 s, works for any provider |
| Deploys: a new `service.version` per service | Your choice. Generic OTel; services first seen are stored silently (startup is not a deploy) |

## Schema changes (migration 002)

- `incidents`:
  - `lease_owner` and `lease_expires_at`, for crash recovery;
  - `evidence_seq`, the counter behind `E1`, `E2`, ....
- `evidence`: as in the draft, plus `ref` (`E1`...) and `data` (the structured finding).
- `change_events`: new. Flag changes and deploys in one shape.
- `observed_state`: new. The last flag values and versions the recorder diffs against.

## Live verification (incident `37563b3b`, `paymentFailure` → 50%)

| Check | Result |
|---|---|
| Change recorder | Flag change recorded 5 s after the flip, and again when reset |
| `query_metrics payment` | error_rate 0 → 0.5, onset 03:50:30; latency, traffic and memory flat |
| `get_traces checkout` | checkout → payment: 8/39 errors, with the real error message; all other dependencies clean |
| `search_logs payment` | New WARN pattern "Payment request failed. Invalid token...", first seen 03:48:45 |
| `list_recent_changes` | The flag change at 03:47:42 |
| Evidence links | All 36 resolve to data: Grafana (Prometheus and OpenSearch queries run through Grafana's API), Jaeger traces and searches, flags page |
| Backend down | OpenSearch stopped → `search_logs` returns `status: error` with the reason |

Known limits:
- A freshly started stack has no baseline for an hour. The tool says so per signal; evals must
  start with a warm stack.
- At the default load, payment gets about 2 calls per minute, so rates are coarse.
