An On call agent that takes metrics from a opentelemetry compatible project and points to the service causing the isssue.

## Running it

The agent is system-agnostic: it monitors any OpenTelemetry-instrumented system described by a
profile in `profiles/`. `compose.yaml` holds only the agent's own services (API, worker,
Postgres, Redis).

### Local dev with the OpenTelemetry Demo

Requires Docker Desktop (about 8 GB of memory for the full stack).

```sh
# once: clone the demo next to this repo, at the release the profile was written for
git clone --branch 3.1.0 --depth 1 https://github.com/open-telemetry/opentelemetry-demo.git ../opentelemetry-demo
cp .env.example .env        # sets COMPOSE_FILE=compose.demo.yaml and OTEL_DEMO_DIR

docker compose up -d
```

`compose.demo.yaml` includes the demo's compose files unchanged; our changes to its services
(persistent trace, metric and log storage; pinned images) are in `deploy/demo/`.

| What | URL |
|---|---|
| Demo webstore | http://localhost:8080 |
| Grafana | http://localhost:8080/grafana/ |
| Jaeger | http://localhost:8080/jaeger/ui |
| Feature flags | http://localhost:8080/feature/ |
| Load generator | http://localhost:8080/loadgen/ |
| On-call API | http://localhost:8000/healthz, http://localhost:8000/readyz |
| On-call Postgres / Redis | localhost:5433 / localhost:6380 |

## Layout

```
compose.yaml           the agent's services (system-agnostic)
compose.demo.yaml      local dev: OpenTelemetry Demo + compose.yaml
deploy/demo/           our changes to demo services
profiles/              one system profile per monitored system
src/oncall/            api (FastAPI), worker, profile loader
tests/                 pytest, with recorded fixtures
docs/                  phase notes and check results
```

## Tests

```sh
pip install -e ".[dev]"
pytest
```
