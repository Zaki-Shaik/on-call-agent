"""Log adapters. Return counts and a bounded sample of records; pattern grouping happens in the tool."""

from datetime import datetime
from typing import Any, Protocol

import httpx

from oncall.profile import Backend
from oncall.telemetry.http import request_json
from oncall.telemetry.models import BackendError, LogRecord

# OpenTelemetry severity numbers: 1-4 TRACE, 5-8 DEBUG, 9-12 INFO, 13-16 WARN, 17-20 ERROR, 21-24 FATAL.
SEVERITY_MIN = {"debug": 5, "info": 9, "warn": 13, "error": 17, "fatal": 21}

# Field names written by the OpenTelemetry Collector's OpenSearch exporter. Overridable via
# the profile's logs backend options.
DEFAULT_FIELDS = {
    "service": "resource.service.name.keyword",
    "timestamp": "@timestamp",
    "body": "body",
    "severity_number": "severity.number",
    "severity_text": "severity.text",
    "trace_id": "traceId",
}


class LogBackend(Protocol):
    def counts_by_severity(self, service: str, start: datetime, end: datetime) -> dict[str, int]:
        """Raises BackendError."""
        ...

    def sample(
        self, service: str, start: datetime, end: datetime, size: int,
        min_severity: str | None = None, text: str | None = None,
    ) -> tuple[int, list[LogRecord]]:
        """(total matching, newest `size` records). Raises BackendError."""
        ...

    def query_text(self, service: str, min_severity: str | None = None, text: str | None = None) -> str:
        """The backend query, for human-facing deep links only."""
        ...


class OpenSearchLogs:
    def __init__(self, config: Backend, client: httpx.Client | None = None) -> None:
        self._url = config.url.rstrip("/")
        self._index = config.options.get("index_pattern", "logs-*")
        self._f = DEFAULT_FIELDS | config.options.get("fields", {})
        self._client = client or httpx.Client(timeout=config.timeout_seconds)

    def _filters(self, service: str, start: datetime, end: datetime,
                 min_severity: str | None = None, text: str | None = None) -> list[dict]:
        filters: list[dict[str, Any]] = [
            {"term": {self._f["service"]: service}},
            {"range": {self._f["timestamp"]: {"gte": start.isoformat(), "lt": end.isoformat()}}},
        ]
        if min_severity:
            filters.append({"range": {self._f["severity_number"]: {"gte": SEVERITY_MIN[min_severity]}}})
        if text:
            # Plain phrase match: the caller passes words, never a query language.
            filters.append({"match_phrase": {self._f["body"]: text}})
        return filters

    def _search(self, body: dict) -> dict:
        result = request_json(self._client, "POST", f"{self._url}/{self._index}/_search",
                              backend="opensearch", json=body)
        if "error" in result:
            raise BackendError(f"opensearch: {result['error']}")
        return result

    def counts_by_severity(self, service: str, start: datetime, end: datetime) -> dict[str, int]:
        result = self._search({
            "size": 0,
            "query": {"bool": {"filter": self._filters(service, start, end)}},
            "aggs": {"sev": {"terms": {"field": self._f["severity_text"] + ".keyword", "size": 20}}},
        })
        return {b["key"]: b["doc_count"] for b in result.get("aggregations", {}).get("sev", {}).get("buckets", [])}

    def sample(
        self, service: str, start: datetime, end: datetime, size: int,
        min_severity: str | None = None, text: str | None = None,
    ) -> tuple[int, list[LogRecord]]:
        result = self._search({
            "size": size,
            "track_total_hits": True,
            "query": {"bool": {"filter": self._filters(service, start, end, min_severity, text)}},
            "sort": [{self._f["timestamp"]: "desc"}],
        })
        hits = result.get("hits", {})
        records = [self._record(h.get("_source", {})) for h in hits.get("hits", [])]
        return hits.get("total", {}).get("value", len(records)), records

    def _record(self, src: dict) -> LogRecord:
        return LogRecord(
            timestamp=datetime.fromisoformat(_get(src, self._f["timestamp"])),
            severity_text=str(_get(src, self._f["severity_text"]) or ""),
            severity_number=int(_get(src, self._f["severity_number"]) or 0),
            body=str(_get(src, self._f["body"]) or ""),
            trace_id=_get(src, self._f["trace_id"]) or None,
        )

    def query_text(self, service: str, min_severity: str | None = None, text: str | None = None) -> str:
        field = self._f["service"].removesuffix(".keyword")
        parts = [f'{field}:"{service}"']
        if min_severity:
            parts.append(f"{self._f['severity_number']}:>={SEVERITY_MIN[min_severity]}")
        if text:
            parts.append(f'{self._f["body"]}:"{text}"')
        return " AND ".join(parts)


def _get(doc: dict, dotted: str) -> Any:
    """Read a dotted field from a document whose keys may themselves contain dots."""
    if dotted in doc:
        return doc[dotted]
    head, _, rest = dotted.partition(".")
    if rest and isinstance(doc.get(head), dict):
        return _get(doc[head], rest)
    return None
