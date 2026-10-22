An On call agent that takes metrics from a opentelemetry compatible project and points to the service causing the isssue.

## Running it

The agent is system-agnostic: it monitors any OpenTelemetry-instrumented system described by a
profile in `profiles/`. `compose.yaml` holds only the agent's own services (API, worker,
Postgres, Redis).

### Local dev with the OpenTelemetry Demo

Requires Docker Desktop (about 8 GB of memory for the full stack).

```sh
cp .env.example .env        # once: sets COMPOSE_FILE=compose.demo.yaml
docker compose up -d
```

After changing our code, rebuild only our image: `docker compose build oncall-api && docker compose up -d`.
Don't use `up --build`: it tries to build the demo's services too, whose source isn't here.

The demo runs from its published images. Only its compose and config files are copied, unmodified,
into `deploy/demo/upstream/` (git-ignored; see `deploy/demo/README.md`). Our changes to its services (persistent trace, metric and log
storage; pinned images) are in `deploy/demo/compose.override.yaml` and `deploy/demo/demo.env`.

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
deploy/demo/upstream/  demo compose + config files (git-ignored copy, 3.1.0)
deploy/demo/           our changes to demo services
profiles/              one system profile per monitored system
src/oncall/            api (FastAPI), worker, profile loader
tests/                 pytest, with recorded fixtures
docs/                  phase notes and check results
```

## Calling the agent's tools by hand

```sh
docker exec oncall-worker python -m oncall.cli incidents
docker exec oncall-worker python -m oncall.cli metrics  <incident-id> --service payment
docker exec oncall-worker python -m oncall.cli traces   <incident-id> --service checkout
docker exec oncall-worker python -m oncall.cli logs     <incident-id> --service payment --min-severity warn
docker exec oncall-worker python -m oncall.cli changes  <incident-id>
docker exec oncall-worker python -m oncall.cli evidence <incident-id> [E3]
```

The window defaults to 15 minutes before the incident opened until now; override with `--start`/`--end`.

## Tests

```sh
pip install -e ".[dev]"
pytest
```
