# Phase 0 checks

Target: OpenTelemetry Demo **3.1.0** (tag `3.1.0`, released 2026-09-18), full stack plus the
observability layer, which is what `make start` runs. Online Boutique checked at `main` (2026-09-18).

Status legend: **Verified (code)** means confirmed from the demo source or config.
**Pending (live)** means it still needs confirmation against the running stack.

---

## 1. What does the demo use for logs?

**OpenSearch 3.7.0.** Verified (code).

- Services send logs over OTLP to the Collector. The `opensearch` exporter writes them to the
  index `otel-logs-yyyy-MM-dd` (`src/otel-collector/otelcol-config-observability.yml`).
- Grafana has an OpenSearch datasource (uid `webstore-logs`), so log deep links can go
  through Grafana Explore.
- Security plugin disabled and single node, so plain HTTP with no auth on `opensearch:9200`.
- Only logs sent over OTLP reach OpenSearch; anything a service writes only to stdout does not.
- **Verified (live):** the index holds logs from 15 demo services plus `kafka` and the Collector
  (`otelcol-contrib`). Service field: `resource.service.name.keyword`.
  - No logs from `flagd`, `flagd-ui` or `image-provider`.
  - **Upstream bug: the Collector drops some logs.** The OpenSearch exporter hits a mapping
    conflict (`attributes.http` is text in some records and an object in others). Every log
    with `http.request.method` is rejected; 177 dropped in the first few minutes. `frontend`
    is almost entirely missing (2 docs) as a result.
  - Consequence: the logs backend is lossy, so "no logs" is never evidence. This is consistent
    with the brief's rule that conclusions need metric or trace support. A fix (index template
    or a Collector transform) is possible later if `search_logs` needs frontend logs.

**Impact:** the `search_logs` adapter targets OpenSearch (`_search` with aggregations on
`otel-logs-*`). Recorded in the profile as `backends.logs.type: opensearch`.

## 2. Does Jaeger keep traces only in memory?

**Yes.** Verified (code). Jaeger 2.19 uses the `memory` backend with `MEMORY_MAX_TRACES=25000`.
At demo traffic that is minutes to tens of minutes of history, and everything is lost on restart.

**Done:** `deploy/demo/jaeger/config.yml` swaps the store for **Badger** (embedded on-disk
key-value store) on the named volume `jaeger-data`, with a 7-day span TTL. That matches
Prometheus's 7-day retention, so a replayed incident has both traces and metrics.

Related gaps found and fixed for the same replay reason: Prometheus and OpenSearch had no named
volumes, so their data was lost on `docker compose down`. Both now have named volumes.

**Verified (live):** Jaeger starts with Badger at `/badger`. A trace written before
`docker compose up --force-recreate jaeger` is still retrievable afterwards.

## 3. Does flagd keep a history of flag changes?

**No.** Verified (code).

- flagd (v0.16.0) reads `demo.flagd.json` (`--uri file:...`) and reloads it when the file changes.
  It keeps no history.
- flagd-ui (Phoenix app) is the only writer. Its `POST /api/write` **replaces the entire file**
  (write-then-rename), keeps no audit trail, and the write is fire-and-forget (`GenServer.cast`):
  the HTTP 200 does not mean the write happened.
- **Verified (live):** after a write through flagd-ui's API, flagd serves the new variant
  within **0.5–1 s** (checked through flagd's OFREP endpoint).
- flagd-ui rewrites the flag file on every change. The demo bind-mounts that file from the
  repo, so we now keep it on the `flagd-data` volume (seeded once from the upstream copy by a
  `flagd-init` container). The copied file stays untouched, and flag state survives restarts.
  Reset it with `docker compose down -v` or by setting flags back to `off`.
- flagd-ui also has a **Scheduler** tab that can flip flags randomly. It must stay off during
  our runs, or it will create unlabelled incidents.

**Proposal: a flag change recorder** that polls the flag provider and stores diffs as change events.

- A background loop calls the flag provider adapter's `read_flags()` every 5 s (configurable).
  It computes each flag's *effective* variant, diffs against the previous snapshot, and writes one
  change event per difference: time, `type=flag_change`, flag, old→new value, `actor=unknown`,
  link to the flags UI.
- When our own approval step sets a flag, it writes the change event itself with
  `actor=<approver>`. The poller then sees the change as already recorded and skips it.
- At startup it stores a baseline snapshot and records no event.
- Trade-offs:
  - Timestamps are accurate to the poll interval (±5 s), which is fine against 60 s metric resolution.
  - A flip that is reverted within one poll interval is missed.
  - Polling works for any provider, unlike flagd's gRPC sync stream (push-based, but flagd-specific).
- Needs a `change_events` table, which is not in the draft data model. `list_recent_changes`
  reads from it.

Two details the flag provider adapter must handle:
- **Targeting rules.** `productCatalogFailure` is switched on through `targeting.if[1]`, not
  `defaultVariant` (flagd-ui does the same). The adapter must report the effective variant, and
  setting a value must reset the targeting branch as well.
- **Whole-file writes.** Setting a flag is read-modify-write of the whole file, last writer wins.
  The approval step re-checks state right before writing (already in the design) and must
  **read back** afterwards to confirm, because the write API's 200 proves nothing.

## 4. How do Kafka producer/consumer spans connect?

**Verified (live): span links, for both consumers.** Neither consumer's span is a child of the
producer. Each consumed message starts a new trace whose consumer span carries a span link
(shown by Jaeger as `FOLLOWS_FROM` into another trace) to checkout's `publish orders` span.
All sampled links resolved to that producer span.

- fraud-detection: `receive orders` (consumer, root of its own trace) → child `process orders`
  (consumer), which holds the link.
- accounting: manual `order-consumed` (internal, trace root) → child `receive orders`
  (consumer), which holds the link.

So **checkout → fraud-detection and checkout → accounting edges exist only through span links,
across traces.** A map builder that only follows parent/child relations misses Kafka entirely.

What the code showed, for reference:

| Side | Service | Instrumentation | What the code shows |
|---|---|---|---|
| Producer | checkout (Go, sarama) | Manual `PRODUCER` span `orders publish` with `messaging.system=kafka`, `messaging.destination.name=orders` | Injects W3C trace context into the Kafka message headers |
| Consumer | fraud-detection (Kotlin) | Java agent, with `OTEL_INSTRUMENTATION_MESSAGING_EXPERIMENTAL_RECEIVE_TELEMETRY_ENABLED=true` | With receive telemetry on, the agent creates `receive` spans, and per-record `process` spans carry a **span link** to the producer span rather than a parent |
| Consumer | accounting (.NET) | .NET auto-instrumentation for Confluent.Kafka, plus a manual `INTERNAL` span `order-consumed` started *before* `Consume()` | The manual span cannot be parented by the message context. The auto-instrumented consumer span should carry the producer context |

**Impact on the map builder (Phase 3):** follow span links on consumer spans. Resolve the
linked span (other trace) to its service, and fall back to `messaging.destination.name` when
the linked trace is not available. Parent/child handling still covers synchronous calls.
This fits the 2–3 h timebox.

## 5. Which signal reveals each failure flag?

Signals in scope: latency (p50/p95/p99), traffic, error rate, saturation (CPU, memory).
Mapping from reading each flag's code (Verified (code)). Confirm live during Phase 6 injector work.

| Flag | Mechanism (from code) | Root-cause service | Revealing signal(s) | Visibility |
|---|---|---|---|---|
| `adFailure` | 10% of `GetAds` throw `UNAVAILABLE` | ad | error rate (ad, frontend client calls) | Good |
| `adHighCpu` | CPU burn on each request | ad | saturation CPU, latency | Good |
| `adManualGc` | Full GC on each request | ad | latency (p95/p99), CPU | Good |
| `cartFailure` | n% of `EmptyCart` use a broken store | cart | error rate (cart, checkout upstream) | Good at ≥25% |
| `emailMemoryLeak` | Sent mails retained, bodies inflated ×n | email | saturation memory (slow ramp) | Good, but gradual onset |
| `failedReadinessProbe` | cart health check returns Unhealthy | cart | **none**: nothing in Docker Compose acts on readiness | **Invisible** |
| `imageSlowLoad` | Envoy fault-delay header on browser image requests | frontend-proxy (as observed) | latency on frontend-proxy image route | Weak: only browser users (1 of 10 weight) |
| `intlShippingSlowdown` | 5–10 s sleep for non-US addresses | shipping | latency (shipping, checkout upstream) | Good at p95/p99 if enough intl orders |
| `kafkaQueueProblems` | checkout publishes 100 extra msgs; fraud-detection sleeps 1 s/record | checkout + fraud-detection | traffic (publish rate), latency of consumer processing | Partial: consumer lag is the real symptom and is not in our signal list |
| `loadGeneratorFloodHomepage` | Load generator hits `/` 100× per task | load-generator (cause), frontend (symptom) | traffic (frontend), latency | Good |
| `paymentFailure` | n% of `Charge` throw | payment | error rate (payment, checkout upstream) | Good |
| `paymentUnreachable` | checkout dials `badAddress:50051` | checkout (config) / payment (named) | error rate (checkout); payment traffic drops; checkout→payment edge disappears | Good, and a good service-map case |
| `productCatalogFailure` | Fails one product id (`OLJCESPC7Z`) | product-catalog | error rate (partial) | Moderate: one product only |
| `productCatalogLockContention` | Long-held DB lock every 10 s | product-catalog | latency (p95/p99) | Good |
| `recommendationCacheFailure` | Cache list grows on each miss | recommendation | saturation memory, latency (growing) | Good, gradual |

**No signal can see:** `failedReadinessProbe`. Not failures: `emitRawPii`. Not running in our
stack: `aiRunawayAgent`, `aiSlowResponse` (they need the demo's optional agent layer).

**Live flip test (`paymentFailure` → `50%` at 00:43:23 UTC):**
- flagd served the new value 0.5 s after the write.
- Jaeger showed payment server spans with errors from the same minute.
- Prometheus span metrics went from 0 errors to 6 errors against 10 successes over the next
  ~5 minutes. That's visible in Grafana as payment's error rate rising from zero.
- Reset to `off` afterwards.

Caveats for Phase 2:
- **Traffic is low.** With the demo default of 5 simulated users, payment gets roughly
  3–4 calls per minute. At that rate a 10% failure flag is close to noise, and error-rate
  baselines are statistically thin. Raising `LOCUST_USERS` (e.g. to 20–30, via `demo.env`)
  would make the eval more reliable. Your call; it costs CPU.
- **Span metrics lag about 1 minute** behind Jaeger (connector flush plus Prometheus ingest).
- **Span metric names** in this deployment: `traces_span_metrics_calls_total` and
  `traces_span_metrics_duration_milliseconds_*`, with labels `service_name`, `span_kind`,
  `span_name`, `status_code`. The demo services also emit SDK metrics such as
  `rpc_server_call_duration_seconds_*`.
- **`service.version` was wrong.** Every service reported `3.0.0` because the demo's `.env`
  expands it from its own `IMAGE_VERSION` before our override loads. Fixed in `demo.env`.
  This matters later if version changes are used as deploy events.
- **Metric resolution.** The demo does not set `OTEL_METRIC_EXPORT_INTERVAL`, so SDK metrics are
  exported every **60 s** (SDK default). `docker_stats` and `kafkametrics` collect every 10 s.
  Change onset will be accurate to about a minute. If that's too coarse, an env override could
  lower it to 15 s (your call).
- **Saturation source.** CPU and memory come from the Collector's `docker_stats` receiver, keyed
  by container name, not `service.name`. In this demo the two are equal. A system where they
  differ needs a mapping in the profile.

## 6. Does Online Boutique emit enough OTel metrics for a held-out test?

**No, not as shipped.** Verified (code). There are also two bigger issues for Phase 7.

- **Metrics:** none of its services use the OTel metrics SDK. Its only OTel telemetry is
  **traces**, behind `ENABLE_TRACING=1`, and only some services have it (frontend, checkout,
  product catalog, currency, payment, email, recommendation). Logs go to stdout only.
- **What would make it meaningful with no code change on our side:**
  - Run it with tracing on into a Collector that has the `spanmetrics` connector, which derives
    latency, traffic and error rate from spans. The OTel Demo's Collector already has this connector.
  - Add `docker_stats` for CPU and memory.
  - Add `filelog` or Docker log collection into OpenSearch.

  All of that is deployment config plus a new profile.
  - Caveat: services without tracing (cart, ad, shipping) appear only as the callee of other
    services' client spans.
  - Caveat: if our metrics adapter relies on semantic-convention SDK metrics
    (`rpc.server.duration` etc.), span-derived metrics have *different names*. Either
    `metric_overrides` covers this, or the adapter should use span metrics as its default source
    for both systems. **Decision for Phase 2.**
- **No feature flags.** Online Boutique has no flag system, so held-out incidents can only be
  no-change (killed container, load spike) or other change types. The flag-caused category cannot
  be tested on it.
- **Kubernetes-first.** It ships Kubernetes manifests, Helm and Kustomize, and **no Docker
  Compose file**. Running it locally means writing our own Compose file for it, or running a local
  Kubernetes cluster (kind). The brief rejected Kubernetes events as a change source because the
  demo runs on Compose. A Compose file for Boutique keeps everything consistent.

These need your input before Phase 7, not now.
