# Phase 1: alert ingestion

## Flow

```
Prometheus rules ──► Alertmanager ──► POST /webhooks/alertmanager ─┐
Grafana alerting ─────────────────► POST /webhooks/grafana ───────┤
                                                                   ▼
                        source adapter (parse + convert to internal Alert)
                                                                   ▼
               per alert, one transaction: dedupe ─► create or attach incident
                                                                   ▼
                      after commit, only for a new incident: XADD oncall:investigations
```

## Decisions

| Decision | Why |
|---|---|
| One endpoint, `/webhooks/{source}`, with a source registry | A new alert source is one class; nothing else changes. Grafana sends the Alertmanager format plus extra fields, so both share a parser. |
| Dedupe key is `(source, fingerprint, starts_at)`, not the fingerprint alone | Alertmanager's fingerprint is a hash of the label set, so it **repeats** when the same alert fires again later. `starts_at` tells episodes apart; re-sends of one episode keep the same `starts_at`. |
| `INSERT ... ON CONFLICT DO NOTHING` for dedupe | Atomic in the database, so no read-then-write race between concurrent deliveries. |
| Grouping: join any open incident of the same system whose `opened_at` is within ±10 min of the alert's `starts_at` | One failure cascades (payment fails, so checkout fails), and per-service grouping opened two incidents for one flag flip. Interim rule; Phase 3 narrows it to the service-map neighborhood. |
| Window measured on alert time (`starts_at`), not wall clock | Replaying recorded alerts for the eval groups exactly as it did live. |
| `pg_advisory_xact_lock` per system around grouping | Two alerts arriving together can't both open an incident. Without the lock the concurrency test fails 3 out of 3 runs. |
| Enqueue only after commit; on Redis failure, log and leave the incident `queued` | The dual-write gap from the design. The Phase 2 sweeper re-enqueues stuck `queued` incidents, and the worker must be idempotent. |
| Resolved alerts update the alert row only | They never create an incident or a job. Incident status is not driven by alert resolution: an incident can be resolved by the fix, not just by alerts going quiet. |
| Alerts without a service label are stored, not routed | Kept for the record without starting an investigation we can't anchor. |
| 503 on database errors | Alertmanager retries 5xx, and the retry is deduplicated. |
| Optional bearer token (`ONCALL_WEBHOOK_TOKEN`) | Off in local dev; compared in constant time when set. |
| Plain numbered SQL migrations, applied at startup under an advisory lock | Transparent and dependency-free. Alembic would add little for a handful of tables. |

## Data model changes vs the draft

- `incidents`:
  - no `thread_id` column, because the incident id *is* the thread id;
  - `service` means the first alerting service (others are on the alerts);
  - `opened_at` is the first alert's `starts_at`;
  - added `created_at` and `updated_at`.
- `alerts`:
  - surrogate `id`, plus a unique key on `(source, fingerprint, starts_at)`;
  - added `ends_at`, `resolved_payload` (the resolve notification, kept separately so
    `raw_payload` stays the first payload unchanged) and `updated_at`;
  - `raw_payload` is the single alert object exactly as received.

## Alert rules (`deploy/alerting/golden-signals.rules.yaml`)

Generic: span-metric names and `service_name` only, with no service names in the rules.
Thresholds are relative to each service's own baseline (the previous hour, offset 15 min)
wherever possible.

| Rule | Fires when |
|---|---|
| ServiceErrorRateHigh | >5% of server/consumer spans fail over 5 min, and at least 3 errors |
| ServiceLatencyHigh | p95 > 2× baseline p95, and > 100 ms |
| ServiceTrafficDrop | rate < 20% of baseline, for services with real baseline traffic |
| ServiceTrafficSpike | rate > 3× the baseline *median*, only if the baseline has no gaps |
| ServiceMemorySaturation | memory > 90% of limit **and** up more than 10 points on baseline |

Tuned live:
- **Memory.** Grafana always runs at about 95% of its limit, so an absolute threshold paged on it.
  Added the "risen vs baseline" condition.
- **Traffic spike.** After the stack had been stopped, the baseline hour contained a gap, so
  normal traffic looked like 3–14× spikes on 6 services. Switched to the baseline median and
  required a gap-free baseline.

Known limits:
- Rules that need a baseline stay silent for about 75 min after a cold start.
- The demo's default load (5 users, about 2 checkouts per minute) is close to the floor of the
  error rule.
- No CPU rule yet: `container_cpu_utilization_ratio` needs calibrating.

## Live verification (2026-10-22)

| Check | Result |
|---|---|
| `paymentFailure` → 50% | Error alerts for payment and checkout, **1 incident** (both alerts attached), **1 job** |
| Re-send of the same payload | `duplicate`; counts unchanged |
| Flag back to `off` | Both alerts marked `resolved`; no new incident or job |
| Grafana | Real Grafana 13.1 notification (receiver test) parsed and ingested |

Test fixtures in `tests/fixtures/alerts/` are these recorded payloads.
