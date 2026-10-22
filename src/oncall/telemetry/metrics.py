"""Metrics adapters. The tool asks for (service, signal, window); the adapter owns the query language.

PrometheusMetrics reads span metrics (OpenTelemetry Collector spanmetrics connector) for
latency, traffic and errors, and container metrics (docker_stats receiver) for saturation.
Metric names default to what those components produce in Prometheus and can be overridden
per system through the profile's metric_overrides.
"""

from datetime import datetime, timezone
from math import isfinite
from typing import Protocol

import httpx

from oncall.profile import Backend
from oncall.telemetry.http import request_json
from oncall.telemetry.models import BackendError, Series, Signal

DEFAULT_METRIC_NAMES = {
    "calls": "traces_span_metrics_calls_total",
    "duration": "traces_span_metrics_duration_milliseconds",
    "cpu": "container_cpu_utilization_ratio",
    "memory": "container_memory_usage_total_bytes",
}

# Spans that represent work a service does for a caller (not its outgoing calls).
_INBOUND = 'span_kind=~"SPAN_KIND_SERVER|SPAN_KIND_CONSUMER"'


class MetricsBackend(Protocol):
    def series(self, service: str, signal: Signal, start: datetime, end: datetime, step_s: int) -> Series:
        """Time series for one signal of one service. Empty means no data. Raises BackendError."""
        ...

    def query_text(self, service: str, signal: Signal) -> str:
        """The backend query behind a signal, for human-facing deep links only."""
        ...

    def service_versions(self, at: datetime, lookback_s: int) -> dict[str, set[str]]:
        """service -> versions reported recently (OTel service.version). Raises BackendError."""
        ...


def _quote(value: str) -> str:
    """Quote a PromQL label value."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


class PrometheusMetrics:
    def __init__(
        self,
        config: Backend,
        overrides: dict[str, str] | None = None,
        service_containers: dict[str, str] | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self._url = config.url.rstrip("/")
        self._names = DEFAULT_METRIC_NAMES | (overrides or {})
        self._containers = service_containers or {}
        self._client = client or httpx.Client(timeout=config.timeout_seconds)

    def query_text(self, service: str, signal: Signal) -> str:
        calls, duration = self._names["calls"], self._names["duration"]
        svc = f"service_name={_quote(service)}, {_INBOUND}"
        rate_window = "2m"
        match signal:
            case Signal.TRAFFIC:
                return f"sum(rate({calls}{{{svc}}}[{rate_window}]))"
            case Signal.ERROR_RATE:
                # The error series only exists after a service's first error; until then the
                # error rate is 0 (when there is traffic), not missing.
                return (
                    f'(sum(rate({calls}{{{svc}, status_code="STATUS_CODE_ERROR"}}[{rate_window}])) or vector(0))'
                    f" / sum(rate({calls}{{{svc}}}[{rate_window}]))"
                )
            case Signal.LATENCY_P50 | Signal.LATENCY_P95 | Signal.LATENCY_P99:
                q = {Signal.LATENCY_P50: "0.5", Signal.LATENCY_P95: "0.95", Signal.LATENCY_P99: "0.99"}[signal]
                return f"histogram_quantile({q}, sum by (le) (rate({duration}_bucket{{{svc}}}[{rate_window}])))"
            case Signal.CPU | Signal.MEMORY:
                container = _quote(self._containers.get(service, service))
                metric = self._names["cpu" if signal is Signal.CPU else "memory"]
                return f"max({metric}{{container_name={container}}})"
        raise ValueError(f"unsupported signal {signal}")

    def series(self, service: str, signal: Signal, start: datetime, end: datetime, step_s: int) -> Series:
        body = request_json(
            self._client, "GET", f"{self._url}/api/v1/query_range", backend="prometheus",
            params={"query": self.query_text(service, signal), "start": start.timestamp(),
                    "end": end.timestamp(), "step": step_s},
        )
        results = _results(body)
        if not results:
            return []
        # Queries aggregate to one series; extra series would mean a misconfigured name.
        return [
            (datetime.fromtimestamp(float(t), tz=timezone.utc), float(v))
            for t, v in results[0].get("values", [])
            if isfinite(float(v))  # NaN (e.g. 0/0 error rate with no traffic) is a gap, not a value
        ]

    def service_versions(self, at: datetime, lookback_s: int) -> dict[str, set[str]]:
        calls = self._names["calls"]
        body = request_json(
            self._client, "GET", f"{self._url}/api/v1/query", backend="prometheus",
            params={"query": f"count by (service_name, service_version) "
                             f"(increase({calls}[{lookback_s}s]) > 0)",
                    "time": at.timestamp()},
        )
        versions: dict[str, set[str]] = {}
        for r in _results(body):
            labels = r.get("metric", {})
            if labels.get("service_name") and labels.get("service_version"):
                versions.setdefault(labels["service_name"], set()).add(labels["service_version"])
        return versions


def _results(body: dict) -> list[dict]:
    if body.get("status") != "success":
        raise BackendError(f"prometheus: {body.get('errorType', 'error')}: {body.get('error', '')}")
    return body.get("data", {}).get("result", [])
