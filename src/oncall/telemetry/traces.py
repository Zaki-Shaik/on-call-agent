"""Trace adapters. Return backend-neutral Trace/Span objects; aggregation happens in the tool."""

import json
from datetime import datetime
from typing import Any, Protocol

import httpx

from oncall.profile import Backend
from oncall.telemetry.http import request_json
from oncall.telemetry.models import BackendError, Span, Trace

# Span attributes worth keeping for summaries; the rest is dropped to keep traces small.
_KEPT_ATTRIBUTES = (
    "peer.service", "server.address", "rpc.method", "rpc.service", "http.route",
    "http.response.status_code", "messaging.destination.name", "messaging.operation.type",
    "db.system", "db.system.name", "exception.type",
)


class TraceBackend(Protocol):
    def find_traces(
        self, service: str, start: datetime, end: datetime, limit: int,
        operation: str | None = None, errors_only: bool = False,
    ) -> list[Trace]:
        """Traces containing spans of `service` in the window. Raises BackendError."""
        ...

    def get_trace(self, trace_id: str) -> Trace | None:
        ...


class JaegerTraces:
    """Jaeger query API (the JSON API its UI uses)."""

    def __init__(self, config: Backend, client: httpx.Client | None = None) -> None:
        self._url = config.url.rstrip("/")
        self._client = client or httpx.Client(timeout=config.timeout_seconds)

    def find_traces(
        self, service: str, start: datetime, end: datetime, limit: int,
        operation: str | None = None, errors_only: bool = False,
    ) -> list[Trace]:
        params: dict[str, Any] = {
            "service": service, "limit": limit,
            "start": int(start.timestamp() * 1_000_000), "end": int(end.timestamp() * 1_000_000),
        }
        if operation:
            params["operation"] = operation
        if errors_only:
            params["tags"] = json.dumps({"error": "true"})
        body = request_json(self._client, "GET", f"{self._url}/api/traces", backend="jaeger", params=params)
        return [parse_jaeger_trace(t) for t in _data(body)]

    def get_trace(self, trace_id: str) -> Trace | None:
        try:
            body = request_json(self._client, "GET", f"{self._url}/api/traces/{trace_id}", backend="jaeger")
        except BackendError as e:
            if "HTTP 404" in str(e):
                return None
            raise
        traces = _data(body)
        return parse_jaeger_trace(traces[0]) if traces else None


def _data(body: Any) -> list[dict]:
    if not isinstance(body, dict) or body.get("errors"):
        raise BackendError(f"jaeger: {body.get('errors') if isinstance(body, dict) else 'bad response'}")
    return body.get("data") or []


def parse_jaeger_trace(raw: dict) -> Trace:
    processes = {pid: p.get("serviceName", "unknown") for pid, p in raw.get("processes", {}).items()}
    spans = []
    for s in raw.get("spans", []):
        tags = {t["key"]: t.get("value") for t in s.get("tags", [])}
        parent, links = None, []
        for ref in s.get("references", []):
            if ref.get("refType") == "CHILD_OF" and ref.get("traceID") == s["traceID"] and parent is None:
                parent = ref["spanID"]
            else:
                # Jaeger shows OTel span links as FOLLOWS_FROM (or CHILD_OF into another trace).
                links.append((ref["traceID"], ref["spanID"]))
        error = tags.get("error") in (True, "true") or tags.get("otel.status_code") == "ERROR"
        spans.append(Span(
            trace_id=s["traceID"],
            span_id=s["spanID"],
            parent_id=parent,
            service=processes.get(s.get("processID"), "unknown"),
            operation=s.get("operationName", ""),
            kind=str(tags.get("span.kind", "internal")),
            start_us=int(s.get("startTime", 0)),
            duration_us=int(s.get("duration", 0)),
            error=error,
            status_message=str(tags.get("otel.status_description", ""))[:300],
            links=tuple(links),
            attributes={k: str(tags[k]) for k in _KEPT_ATTRIBUTES if k in tags},
        ))
    return Trace(trace_id=raw.get("traceID", spans[0].trace_id if spans else ""), spans=tuple(spans))
