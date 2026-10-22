"""get_service_map: the dependency map around a service, healthy vs now.

The healthy map comes from stored snapshots; the incident side is sampled from traces of the
services in view during the window. Each edge reports both sides and what changed.
"""

from datetime import datetime

from pydantic import BaseModel

from oncall.evidence import EvidenceDraft
from oncall.servicemap.compare import EdgeNumbers, EdgeView, NodeView, compare
from oncall.servicemap.model import NodeType, neighborhood
from oncall.telemetry.models import BackendError, TimeWindow
from oncall.tools.base import Status, ToolContext

MAX_HOPS = 3


class ServiceMapResult(BaseModel):
    tool: str = "get_service_map"
    service: str | None = None  # None: the whole map
    hops: int | None = None
    window: TimeWindow
    status: Status
    healthy_built_at: datetime | None = None
    healthy_window: TimeWindow | None = None
    nodes: list[NodeView] = []
    edges: list[EdgeView] = []
    note: str | None = None


def _describe(n: EdgeNumbers | None) -> str:
    if n is None:
        return "no calls"
    lag = f", lag p95 {n.lag_p95_ms} ms" if n.lag_p95_ms is not None else ""
    return f"{n.errors}/{n.calls} errors, p95 {n.p95_ms} ms{lag}"


def get_service_map(ctx: ToolContext, window: TimeWindow, service: str | None = None,
                    hops: int = 2) -> ServiceMapResult:
    if not 1 <= hops <= MAX_HOPS:
        raise ValueError(f"hops must be between 1 and {MAX_HOPS}")
    hops_out = hops if service else None
    if ctx.maps is None:
        return ServiceMapResult(service=service, hops=hops_out, window=window, status=Status.ERROR,
                                note="no service map configured")
    try:
        snapshot = ctx.maps.healthy()
    except BackendError as e:
        return ServiceMapResult(service=service, hops=hops_out, window=window, status=Status.ERROR,
                                note=f"no healthy service map: {e}")
    full = snapshot.map
    notes = []
    if snapshot.stale:
        notes.append(f"healthy map is from {snapshot.built_at:%Y-%m-%d %H:%M}Z; rebuilding it failed")
    if service and full.node(service) is None:
        notes.append(f"{service} is not in the healthy map")
    area = neighborhood(full, service, hops) if service else full
    services = {n.name for n in area.nodes if n.type is NodeType.SERVICE}
    if service and full.node(service) is None:
        services.add(service)  # unknown to the healthy map: maybe a new service, so look for it
    try:
        view = ctx.maps.incident_view(services, window, full)
    except BackendError as e:
        return ServiceMapResult(service=service, hops=hops_out, window=window, status=Status.ERROR, note=str(e))
    if view.failed:
        notes.append(f"trace queries failed for {', '.join(sorted(view.failed))}")
    nodes, edges = compare(area, view.observations, view.queried, full, view.before)
    if not edges and not any(n.incident_traces for n in nodes):
        return ServiceMapResult(service=service, hops=hops_out, window=window, status=Status.NO_DATA,
                                healthy_built_at=snapshot.built_at, healthy_window=snapshot.window,
                                note="; ".join(notes) or None)

    inputs = {"service": service, "hops": hops_out, "window": window.model_dump(mode="json")}
    drafts = []
    for e in edges:
        example = view.error_examples.get((e.source, e.target, e.type))
        searched = e.source if e.source in services else e.target
        link = ctx.links.trace(example) if example else ctx.links.trace_search(searched, window.start, window.end)
        drafts.append(EvidenceDraft(
            "get_service_map", inputs,
            f"{e.source} -> {e.target} ({e.type}): healthy {_describe(e.healthy)};"
            f" hour before {_describe(e.before)}; window {_describe(e.incident)}"
            + (f"; {', '.join(e.changes)}" if e.changes else ""),
            link, e.model_dump(mode="json", exclude={"evidence"})))
    changed_nodes = [n for n in nodes if n.changes]
    for n in changed_nodes:
        drafts.append(EvidenceDraft(
            "get_service_map", inputs,
            f"{n.name}: no traces in the window (healthy map: {n.healthy_traces} sampled traces)",
            ctx.links.trace_search(n.name, window.start, window.end), n.model_dump(mode="json", exclude={"evidence"})))
    refs = ctx.save(drafts)
    for target, ref in zip([*edges, *changed_nodes], refs):
        target.evidence = ref

    notes.append("calls are counts in sampled traces, not rates: compare error shares and latency, not call counts")
    return ServiceMapResult(
        service=service, hops=hops_out, window=window, status=Status.OK,
        healthy_built_at=snapshot.built_at, healthy_window=snapshot.window,
        nodes=nodes, edges=edges, note="; ".join(notes),
    )
