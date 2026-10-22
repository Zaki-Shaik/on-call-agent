# Local dev stack: OpenTelemetry Demo

The demo is only our local test system; nothing in the agent depends on it.

- `upstream/` (git-ignored): unmodified copies of the demo's compose and config files from
  https://github.com/open-telemetry/opentelemetry-demo at tag `3.1.0` (Apache-2.0).
  Services run from the published images (`ghcr.io/open-telemetry/demo:3.1.0-*`), so no
  source code or Dockerfiles are needed.
- `compose.override.yaml`, `demo.env`, `jaeger/config.yml`: our changes to demo services:
  persistent trace, metric and log storage; flag state on a volume; pinned versions.

To recreate `upstream/`, copy these paths from the demo repo at `3.1.0`, keeping the
structure and using LF line endings:

```
LICENSE  .env  compose.yaml  compose.full.yaml  compose.observability.yaml
compose.extras.yaml  otel-config.yml
src/otel-collector/otelcol-config*.yml  src/prometheus/prometheus-config.yaml
src/grafana/grafana.ini  src/grafana/provisioning/  src/jaeger/config.yml
src/jaeger/ui-config.json  src/flagd/demo.flagd.json  src/postgresql/init.sql
```
