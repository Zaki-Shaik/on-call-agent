"""The service dependency map, discovered from traces.

Edges come only from OpenTelemetry span kinds and semantic-convention attributes:
- call: a span whose child belongs to another service. A client span with no traced child goes
  to the peer its attributes name (a database, a cache, or a traced service that is down).
- publish: a producer span to its messaging destination (a topic node).
- consume: a topic to a consumer service. Consumers point at the producer with a span link,
  not as its child, so the linked producer span is used only to measure lag.
"""

import ipaddress
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from statistics import quantiles

from pydantic import BaseModel

from oncall.telemetry.models import Span, Trace
from oncall.tools.base import sig

DESTINATION = "messaging.destination.name"
OPERATIONS_KEPT = 5

SpanRef = tuple[str, str]  # (trace_id, span_id)


class NodeType(StrEnum):
    SERVICE = "service"
    EXTERNAL = "external"  # not traced itself: a database, cache or third-party host
    TOPIC = "topic"


class EdgeType(StrEnum):
    CALL = "call"
    PUBLISH = "publish"
    CONSUME = "consume"


class Node(BaseModel):
    name: str
    type: NodeType
    traces: int = 0  # sampled traces the service appears in (0 for externals and topics)


class Edge(BaseModel):
    source: str
    target: str
    type: EdgeType
    calls: int  # observed in the sampled traces, not a rate
    errors: int
    p50_ms: float
    p95_ms: float
    lag_p95_ms: float | None = None  # consume edges: publish end to consume start
    operations: list[str] = []  # caller-side operation names, most frequent first
    example_trace: str | None = None
    example_error_trace: str | None = None

    @property
    def key(self) -> tuple[str, str, EdgeType]:
        return (self.source, self.target, self.type)


class ServiceMap(BaseModel):
    nodes: list[Node]
    edges: list[Edge]
    traces_sampled: int

    def node(self, name: str) -> Node | None:
        return next((n for n in self.nodes if n.name == name), None)

    def services(self) -> set[str]:
        return {n.name for n in self.nodes if n.type is NodeType.SERVICE}

    def known_operations(self) -> dict[tuple[str, str], str]:
        """(caller, operation) -> callee, to name the target of a client span that has no traced child."""
        return {(e.source, op): e.target for e in self.edges
                if e.type is EdgeType.CALL for op in e.operations}


def percentile(values: list[int], q: int) -> float:
    if len(values) == 1:
        return values[0]
    return quantiles(values, n=100, method="inclusive")[q - 1]


def to_ms(us: float) -> float:
    return sig(us / 1000) or 0.0


@dataclass
class EdgeSamples:
    durations_us: list[int] = field(default_factory=list)
    lags_us: list[int] = field(default_factory=list)
    errors: int = 0
    operations: Counter = field(default_factory=Counter)
    example_trace: str | None = None
    example_error_trace: str | None = None

    @property
    def calls(self) -> int:
        return len(self.durations_us)

    def summary(self, source: str, target: str, type_: EdgeType) -> Edge:
        return Edge(
            source=source, target=target, type=type_, calls=self.calls, errors=self.errors,
            p50_ms=to_ms(percentile(self.durations_us, 50)), p95_ms=to_ms(percentile(self.durations_us, 95)),
            lag_p95_ms=to_ms(percentile(self.lags_us, 95)) if self.lags_us else None,
            operations=[op for op, _ in self.operations.most_common(OPERATIONS_KEPT)],
            example_trace=self.example_trace, example_error_trace=self.example_error_trace,
        )


@dataclass
class Observations:
    """Raw per-edge samples from a set of traces. `to_map()` summarizes them."""

    traces: int = 0
    services: Counter = field(default_factory=Counter)  # service -> traces it appears in
    node_types: dict[str, NodeType] = field(default_factory=dict)
    edges: dict[tuple[str, str, EdgeType], EdgeSamples] = field(default_factory=lambda: defaultdict(EdgeSamples))

    def add(self, source: str, target: str, type_: EdgeType, span: Span, duration_us: int, error: bool,
            target_type: NodeType = NodeType.SERVICE, source_type: NodeType = NodeType.SERVICE,
            lag_us: int | None = None) -> None:
        self.node_types.setdefault(source, source_type)
        self.node_types.setdefault(target, target_type)
        samples = self.edges[(source, target, type_)]
        samples.durations_us.append(duration_us)
        samples.errors += error
        samples.operations[span.operation] += 1
        samples.example_trace = samples.example_trace or span.trace_id
        if error:
            samples.example_error_trace = samples.example_error_trace or span.trace_id
        if lag_us is not None:
            samples.lags_us.append(max(lag_us, 0))  # clocks of different hosts can disagree slightly

    def to_map(self) -> ServiceMap:
        nodes = {s: Node(name=s, type=NodeType.SERVICE, traces=n) for s, n in self.services.items()}
        for name, type_ in self.node_types.items():
            nodes.setdefault(name, Node(name=name, type=type_))
        return ServiceMap(
            nodes=sorted(nodes.values(), key=lambda n: (n.type != NodeType.SERVICE, n.name)),
            edges=sorted((s.summary(*key) for key, s in self.edges.items()), key=lambda e: e.key),
            traces_sampled=self.traces,
        )


def _is_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def _peer_name(span: Span) -> str | None:
    """Name of an untraced peer from its attributes. IP addresses change, so they don't name nodes."""
    a = span.attributes
    for key in ("peer.service", "server.address"):
        if a.get(key) and not _is_ip(a[key]):
            return a[key]
    return a.get("db.system.name") or a.get("db.system")


def extract(
    traces: Iterable[Trace],
    known_services: set[str] = frozenset(),
    known_operations: Mapping[tuple[str, str], str] | None = None,
    linked: Mapping[SpanRef, Span] | None = None,
) -> Observations:
    """Edge observations from traces.

    known_services: traced services, so a call to one that is down still points at it.
    known_operations: from a healthy map, to name callees when the client span only has an IP.
    linked: spans from other traces that consumer spans link to (their producers).
    """
    traces = list(traces)
    spans_by_ref: dict[SpanRef, Span] = dict(linked or {})
    spans_by_ref.update({(s.trace_id, s.span_id): s for t in traces for s in t.spans})
    obs = Observations()
    for t in traces:
        obs.traces += 1
        obs.services.update({s.service for s in t.spans})
        by_id = {s.span_id: s for s in t.spans}
        children: dict[str, list[Span]] = defaultdict(list)
        for s in t.spans:
            if s.parent_id in by_id:
                children[s.parent_id].append(s)
        consumed: dict[tuple[str, str], list[Span]] = defaultdict(list)
        for s in t.spans:
            if s.kind in ("client", "producer"):
                _outgoing(obs, s, children[s.span_id], known_services, known_operations or {})
            elif s.kind in ("server", "consumer"):
                parent = by_id.get(s.parent_id) if s.parent_id else None
                # A call without a client span on the caller's side (context propagated directly).
                if parent and parent.service != s.service and parent.kind not in ("client", "producer"):
                    obs.add(parent.service, s.service, EdgeType.CALL, s, s.duration_us, s.error)
            if s.kind == "consumer":
                consumed[(s.service, s.attributes.get(DESTINATION, ""))].append(s)
        for (service, topic), spans in consumed.items():
            _consumed(obs, service, topic, spans, spans_by_ref)
    return obs


def _outgoing(obs: Observations, span: Span, children: list[Span], known_services: set[str],
              known_operations: Mapping[tuple[str, str], str]) -> None:
    callees = [c for c in children if c.service != span.service]
    for c in callees:
        # With retries a client span has several server children; then each child's own time counts.
        duration = span.duration_us if len(callees) == 1 else c.duration_us
        obs.add(span.service, c.service, EdgeType.CALL, span, duration, span.error or c.error)
    if callees:
        return
    if span.kind == "producer":
        topic = span.attributes.get(DESTINATION) or _peer_name(span)
        if topic:
            obs.add(span.service, topic, EdgeType.PUBLISH, span, span.duration_us, span.error, NodeType.TOPIC)
        return
    target = _peer_name(span) or known_operations.get((span.service, span.operation))
    if target and target != span.service:
        target_type = NodeType.SERVICE if target in known_services else NodeType.EXTERNAL
        obs.add(span.service, target, EdgeType.CALL, span, span.duration_us, span.error, target_type)


def _consumed(obs: Observations, service: str, topic: str, spans: list[Span],
              spans_by_ref: Mapping[SpanRef, Span]) -> None:
    # One observation per consumer service and destination per trace: a "receive" span and its
    # "process" child describe the same message. Prefer the span that carries the link.
    span = next((s for s in spans if s.links), spans[0])
    producer = next((spans_by_ref[ref] for ref in span.links if ref in spans_by_ref), None)
    source = topic or (producer.service if producer else None)
    if not source or source == service:
        return
    lag = span.start_us - (producer.start_us + producer.duration_us) if producer else None
    obs.add(source, service, EdgeType.CONSUME, span, span.duration_us, any(s.error for s in spans),
            source_type=NodeType.TOPIC if topic else NodeType.SERVICE, lag_us=lag)


def neighborhood(m: ServiceMap, service: str, hops: int) -> ServiceMap:
    """Edges within `hops` of `service`, followed downstream (callees) and upstream (callers)."""
    outgoing: dict[str, list[Edge]] = defaultdict(list)
    incoming: dict[str, list[Edge]] = defaultdict(list)
    for e in m.edges:
        outgoing[e.source].append(e)
        incoming[e.target].append(e)
    kept: dict[tuple[str, str, EdgeType], Edge] = {}
    for index, far_end in ((outgoing, "target"), (incoming, "source")):
        frontier, seen = {service}, {service}
        for _ in range(hops):
            nxt = set()
            for name in frontier:
                for e in index[name]:
                    kept[e.key] = e
                    other = getattr(e, far_end)
                    if other not in seen:
                        seen.add(other)
                        nxt.add(other)
            frontier = nxt
    names = {service} | {e.source for e in kept.values()} | {e.target for e in kept.values()}
    return ServiceMap(nodes=[n for n in m.nodes if n.name in names],
                      edges=sorted(kept.values(), key=lambda e: e.key), traces_sampled=m.traces_sampled)
