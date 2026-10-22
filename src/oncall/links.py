"""Deep links from evidence to the UIs an engineer would open (Grafana Explore, Jaeger)."""

import json
from datetime import datetime
from urllib.parse import quote, urlencode

from oncall.profile import Links

_DATASOURCE_TYPES = {
    "metrics": "prometheus",
    "logs": "grafana-opensearch-datasource",
    "traces": "jaeger",
}


class Linker:
    def __init__(self, links: Links) -> None:
        self._links = links

    def metrics(self, query: str, start: datetime, end: datetime) -> str | None:
        return self._explore("metrics", {"expr": query}, start, end)

    def logs(self, lucene: str, start: datetime, end: datetime) -> str | None:
        return self._explore("logs", {
            "query": lucene, "queryType": "lucene", "timeField": "@timestamp",
            "metrics": [{"id": "1", "type": "logs"}], "bucketAggs": [],
        }, start, end)

    def trace(self, trace_id: str) -> str | None:
        return f"{self._links.jaeger.rstrip('/')}/trace/{trace_id}" if self._links.jaeger else None

    def trace_search(self, service: str, start: datetime, end: datetime,
                     operation: str | None = None, errors_only: bool = False) -> str | None:
        if not self._links.jaeger:
            return None
        params = {"service": service, "start": int(start.timestamp() * 1e6),
                  "end": int(end.timestamp() * 1e6), "limit": 20}
        if operation:
            params["operation"] = operation
        if errors_only:
            params["tags"] = json.dumps({"error": "true"})
        return f"{self._links.jaeger.rstrip('/')}/search?{urlencode(params)}"

    def flags(self) -> str | None:
        return self._links.flags

    def _explore(self, role: str, query: dict, start: datetime, end: datetime) -> str | None:
        uid = self._links.grafana_datasources.get(role)
        if not uid:
            return None
        pane = {
            "datasource": uid,
            "queries": [{"refId": "A", "datasource": {"type": _DATASOURCE_TYPES[role], "uid": uid}, **query}],
            "range": {"from": str(int(start.timestamp() * 1000)), "to": str(int(end.timestamp() * 1000))},
        }
        panes = quote(json.dumps({"a": pane}, separators=(",", ":")))
        return f"{self._links.grafana.rstrip('/')}/explore?schemaVersion=1&orgId=1&panes={panes}"
