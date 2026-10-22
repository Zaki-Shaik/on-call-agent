"""The starter pack: what code gathers before the model starts, so its first call is informed."""

from datetime import timedelta
from typing import Any

from oncall.telemetry.models import TimeWindow
from oncall.tools.base import ToolContext
from oncall.tools.changes import list_recent_changes
from oncall.tools.metrics import query_metrics
from oncall.tools.servicemap import get_service_map

# Incident window: from shortly before the first alert (failures start before alerts fire) to now.
LEAD = timedelta(minutes=15)
# Changes are looked for further back: a bad deploy or flag flip can take a while to show.
CHANGES_LEAD = timedelta(hours=1)
MAP_HOPS = 2


def build_starter_pack(ctx: ToolContext, incident: dict[str, Any], alerts: list[dict[str, Any]],
                       window: TimeWindow) -> dict[str, Any]:
    service = incident["service"]
    changes_window = TimeWindow(start=incident["opened_at"] - CHANGES_LEAD, end=window.end)

    def dump(result) -> dict[str, Any]:
        return result.model_dump(mode="json", exclude_none=True)

    return {
        "alerts": [{"service": a["service"], "severity": a["severity"], "status": a["status"],
                    "summary": a["summary"], "starts_at": a["starts_at"].isoformat()} for a in alerts],
        "incident_window": window.model_dump(mode="json"),
        "golden_signals": dump(query_metrics(ctx, service, window)),
        "recent_changes": dump(list_recent_changes(ctx, changes_window)),
        "service_map": dump(get_service_map(ctx, window, service, MAP_HOPS)),
    }
