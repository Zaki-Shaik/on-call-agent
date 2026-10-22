"""get_traces: aggregated slow or failing operations and a critical-path summary, not raw spans."""

from collections import Counter, defaultdict
from statistics import quantiles

from pydantic import BaseModel

from oncall.evidence import EvidenceDraft
from oncall.telemetry.models import BackendError, Span, TimeWindow, Trace
from oncall.tools.base import Status, ToolContext, sig

SAMPLE_LIMIT = 100
ERROR_SAMPLE_LIMIT = 50
CRITICAL_PATH_TRACES = 5
TOP = 8


class OperationStats(BaseModel):
    operation: str
    kind: str
    calls: int
    errors: int
    p50_ms: float
    p95_ms: float
    top_errors: list[str] = []
    evidence: str | None = None


class EdgeStats(BaseModel):
    peer: str  # the other service, or the address/destination when it is not traced
    calls: int
    errors: int
    p95_ms: float
    example_error_trace: str | None = None
    evidence: str | None = None


class CriticalPathShare(BaseModel):
    service: str
    share_pct: float


class TracesResult(BaseModel):
    tool: str = "get_traces"
    service: str
    window: TimeWindow
    status: Status
    traces_sampled: int = 0
    operations: list[OperationStats] = []
    downstream: list[EdgeStats] = []
    upstream: list[EdgeStats] = []
    critical_path: list[CriticalPathShare] = []
    critical_path_evidence: str | None = None
    note: str | None = None


def _ms(us: float) -> float:
    return sig(us / 1000) or 0.0


def _p(values: list[int], q: int) -> float:
    if len(values) == 1:
        return values[0]
    return quantiles(values, n=100, method="inclusive")[q - 1]


def critical_path(trace: Trace) -> dict[str, int]:
    """Microseconds each service spends on the trace's critical path (the chain that sets its duration)."""
    children: dict[str | None, list[Span]] = defaultdict(list)
    ids = {s.span_id for s in trace.spans}
    for s in trace.spans:
        children[s.parent_id if s.parent_id in ids else None].append(s)
    totals: dict[str, int] = defaultdict(int)

    def walk(span: Span, end_limit: int) -> None:
        start, cursor = span.start_us, min(span.start_us + span.duration_us, end_limit)
        # Walk back from the end: the child that finished last is what the span waited on.
        for child in sorted(children.get(span.span_id, []), key=lambda c: c.start_us + c.duration_us, reverse=True):
            child_end = min(child.start_us + child.duration_us, cursor)
            if child_end <= start or child.start_us >= cursor:
                continue
            totals[span.service] += cursor - child_end
            walk(child, child_end)
            cursor = max(child.start_us, start)
        totals[span.service] += max(cursor - start, 0)

    for root in children[None]:
        walk(root, root.start_us + root.duration_us)
    return dict(totals)


def _peer(span: Span, by_parent: dict[str, list[Span]]) -> str:
    called = {c.service for c in by_parent.get(span.span_id, []) if c.service != span.service}
    if called:
        return sorted(called)[0]
    a = span.attributes
    return (a.get("peer.service") or a.get("messaging.destination.name") or a.get("server.address")
            or a.get("db.system.name") or a.get("db.system") or span.operation)


def get_traces(ctx: ToolContext, service: str, window: TimeWindow,
               operation: str | None = None, errors_only: bool = False) -> TracesResult:
    inputs = {"service": service, "window": window.model_dump(mode="json"),
              "operation": operation, "errors_only": errors_only}
    try:
        traces = {t.trace_id: t for t in ctx.backends.traces.find_traces(
            service, window.start, window.end, ERROR_SAMPLE_LIMIT, operation, errors_only=True)}
        if not errors_only:
            for t in ctx.backends.traces.find_traces(service, window.start, window.end, SAMPLE_LIMIT, operation):
                traces.setdefault(t.trace_id, t)
    except BackendError as e:
        return TracesResult(service=service, window=window, status=Status.ERROR, note=str(e))
    if not traces:
        return TracesResult(service=service, window=window, status=Status.NO_DATA)

    ops: dict[tuple[str, str], list[Span]] = defaultdict(list)
    down: dict[str, list[Span]] = defaultdict(list)
    up: dict[str, list[Span]] = defaultdict(list)
    for t in traces.values():
        by_id = {s.span_id: s for s in t.spans}
        by_parent: dict[str, list[Span]] = defaultdict(list)
        for s in t.spans:
            if s.parent_id:
                by_parent[s.parent_id].append(s)
        for s in t.spans:
            if s.service != service:
                continue
            ops[(s.operation, s.kind)].append(s)
            if s.kind in ("client", "producer"):
                down[_peer(s, by_parent)].append(s)
            elif s.kind in ("server", "consumer"):
                parent = by_id.get(s.parent_id) if s.parent_id else None
                caller = parent.service if parent and parent.service != service else (
                    s.attributes.get("messaging.destination.name") if s.links else None)
                if caller:
                    up[caller].append(s)

    operations = sorted(
        (OperationStats(
            operation=op, kind=kind, calls=len(spans), errors=sum(s.error for s in spans),
            p50_ms=_ms(_p([s.duration_us for s in spans], 50)), p95_ms=_ms(_p([s.duration_us for s in spans], 95)),
            top_errors=[m for m, _ in Counter(s.status_message[:120] for s in spans if s.error and s.status_message).most_common(2)],
        ) for (op, kind), spans in ops.items()),
        key=lambda o: (o.errors, o.p95_ms), reverse=True,
    )[:TOP]

    def edges(groups: dict[str, list[Span]]) -> list[EdgeStats]:
        return sorted(
            (EdgeStats(
                peer=peer, calls=len(spans), errors=sum(s.error for s in spans),
                p95_ms=_ms(_p([s.duration_us for s in spans], 95)),
                example_error_trace=next((s.trace_id for s in spans if s.error), None),
            ) for peer, spans in groups.items()),
            key=lambda e: (e.errors, e.p95_ms), reverse=True,
        )[:TOP]

    downstream, upstream = edges(down), edges(up)

    # Critical path over the slowest traces that involve the service.
    slowest = sorted(traces.values(), key=lambda t: max((s.start_us + s.duration_us for s in t.spans), default=0)
                     - min((s.start_us for s in t.spans), default=0), reverse=True)[:CRITICAL_PATH_TRACES]
    shares: dict[str, float] = defaultdict(float)
    for t in slowest:
        per_service = critical_path(t)
        total = sum(per_service.values()) or 1
        for svc, us in per_service.items():
            shares[svc] += us / total / len(slowest) * 100
    path = [CriticalPathShare(service=s, share_pct=round(p, 1))
            for s, p in sorted(shares.items(), key=lambda kv: kv[1], reverse=True) if p >= 1][:TOP]

    # Evidence: each operation and edge row, plus the critical path.
    drafts: list[EvidenceDraft] = []
    targets: list = []
    for o in operations:
        drafts.append(EvidenceDraft("get_traces", inputs,
                                    f"{service} {o.kind} {o.operation}: {o.calls} calls, {o.errors} errors, p95 {o.p95_ms} ms",
                                    ctx.links.trace_search(service, window.start, window.end, o.operation, o.errors > 0),
                                    o.model_dump(mode="json")))
        targets.append(o)
    for direction, rows in (("calls", downstream), ("called by", upstream)):
        for e in rows:
            link = ctx.links.trace(e.example_error_trace) if e.example_error_trace else \
                ctx.links.trace_search(service, window.start, window.end)
            drafts.append(EvidenceDraft("get_traces", inputs,
                                        f"{service} {direction} {e.peer}: {e.calls} spans, {e.errors} errors, p95 {e.p95_ms} ms",
                                        link, e.model_dump(mode="json")))
            targets.append(e)
    if path:
        drafts.append(EvidenceDraft(
            "get_traces", inputs,
            "critical path of slowest traces: " + ", ".join(f"{p.service} {p.share_pct}%" for p in path),
            ctx.links.trace(slowest[0].trace_id), {"critical_path": [p.model_dump() for p in path]}))
    refs = ctx.save(drafts)
    for target, ref in zip(targets, refs):
        target.evidence = ref

    return TracesResult(
        service=service, window=window, status=Status.OK, traces_sampled=len(traces),
        operations=operations, downstream=downstream, upstream=upstream, critical_path=path,
        critical_path_evidence=refs[-1] if path else None,
        note=f"sample of up to {ERROR_SAMPLE_LIMIT} error traces and {SAMPLE_LIMIT} recent traces",
    )
