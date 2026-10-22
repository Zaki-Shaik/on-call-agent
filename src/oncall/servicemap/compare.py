"""Compare edges during an incident with the healthy map and the hour before.

Every change is a significance test, not a fixed threshold: "this many errors (or slow calls,
or missing calls) would happen by chance less than 1 time in 1000 if nothing had changed".

- Errors, missing and new edges are judged against the healthy map: they are rare events, so
  they need its long sample.
- Latency and consumer lag are judged against the hour before the window, as query_metrics does:
  they drift with load and warm-up, so a map built over a day is the wrong yardstick.
"""

from enum import StrEnum
from math import exp, lgamma, log, log1p

from pydantic import BaseModel

from oncall.servicemap.model import (
    Edge, EdgeSamples, EdgeType, NodeType, Observations, ServiceMap, percentile, to_ms,
)

ALPHA = 0.001
MIN_ERRORS = 2  # one failed call is not a pattern, however clean the healthy edge was
MIN_SLOW = 3
MIN_SLOWDOWN = 1.5  # p95 must also grow by half: a statistically real 10% shift is not worth a flag
MIN_BEFORE_CALLS = 10  # fewer calls in the hour before give no usable p95
SLOW_SHARE = 0.05  # by definition, 5% of healthy calls are slower than the healthy p95


class EdgeChange(StrEnum):
    ERRORS_UP = "errors_up"
    SLOWER = "slower"
    LAG_UP = "lag_up"
    MISSING = "missing"  # the caller is active but no longer makes this call
    NEW = "new"  # not in the healthy map, though its sample should have caught it


class NodeChange(StrEnum):
    NOT_SEEN = "not_seen"  # no traces of the service in the window


class EdgeNumbers(BaseModel):
    calls: int
    errors: int
    p95_ms: float
    lag_p95_ms: float | None = None


class EdgeView(BaseModel):
    source: str
    target: str
    type: EdgeType
    healthy: EdgeNumbers | None = None
    before: EdgeNumbers | None = None  # the hour before the window
    incident: EdgeNumbers | None = None
    changes: list[EdgeChange] = []
    evidence: str | None = None


class NodeView(BaseModel):
    """A service whose traces were sampled for the window."""

    name: str
    healthy_traces: int = 0
    incident_traces: int = 0
    changes: list[NodeChange] = []
    evidence: str | None = None


def binomial_sf(k: int, n: int, p: float) -> float:
    """P(X >= k) for X ~ Binomial(n, p)."""
    if k <= 0:
        return 1.0
    if k > n or p <= 0:
        return 0.0
    if p >= 1:
        return 1.0
    log_terms = (lgamma(n + 1) - lgamma(i + 1) - lgamma(n - i + 1) + i * log(p) + (n - i) * log1p(-p)
                 for i in range(k, n + 1))
    return min(sum(exp(t) for t in log_terms), 1.0)


def _numbers(edge: Edge | EdgeSamples) -> EdgeNumbers:
    if isinstance(edge, Edge):
        return EdgeNumbers(calls=edge.calls, errors=edge.errors, p95_ms=edge.p95_ms, lag_p95_ms=edge.lag_p95_ms)
    return EdgeNumbers(calls=edge.calls, errors=edge.errors, p95_ms=to_ms(percentile(edge.durations_us, 95)),
                       lag_p95_ms=to_ms(percentile(edge.lags_us, 95)) if edge.lags_us else None)


def _slower(values: list[int], reference: list[int]) -> bool:
    """More values above the reference p95 than chance allows, and a p95 that moved enough to matter."""
    if not values or len(reference) < MIN_BEFORE_CALLS:
        return False
    p95 = percentile(reference, 95)
    above = sum(v > p95 for v in values)
    return (above >= MIN_SLOW and binomial_sf(above, len(values), SLOW_SHARE) < ALPHA
            and percentile(values, 95) >= MIN_SLOWDOWN * p95)


def edge_changes(healthy: Edge | None, incident: EdgeSamples | None, expected_calls: float | None,
                 before: EdgeSamples | None = None) -> list[EdgeChange]:
    """Changes of one edge.

    expected_calls: for an edge seen on one side only, how many calls the other side's sample
    should have contained if nothing changed. None when that can't be estimated (the calling
    service was not sampled), and then absence proves nothing.
    before: the edge in the hour before the window, the reference for latency and lag.
    """
    if not healthy or not incident or not incident.calls:
        if expected_calls is None or exp(-expected_calls) >= ALPHA:  # Poisson: P(0 calls) = e^-expected
            return []
        return [EdgeChange.NEW] if healthy is None else [EdgeChange.MISSING]
    changes = []
    # Add-half smoothing: a healthy edge with 0 errors in n calls has an error rate somewhere
    # below ~1/n, not exactly zero, so one error is not infinitely surprising.
    p_error = (healthy.errors + 0.5) / (healthy.calls + 1)
    if incident.errors >= MIN_ERRORS and binomial_sf(incident.errors, incident.calls, p_error) < ALPHA:
        changes.append(EdgeChange.ERRORS_UP)
    if before and _slower(incident.durations_us, before.durations_us):
        changes.append(EdgeChange.SLOWER)
    if before and _slower(incident.lags_us, before.lags_us):
        changes.append(EdgeChange.LAG_UP)
    return changes


def _expected_calls(key: tuple[str, str, EdgeType], h: Edge | None, i: EdgeSamples | None,
                    full: ServiceMap, incident: Observations, queried: set[str]) -> float | None:
    """For an edge seen on one side only: the calls the other side should have shown.

    Scaled by the traces of the edge's service end (the caller, or the consumer of a topic),
    assuming the edge keeps the same share of that service's traces.
    """
    source = full.node(key[0])
    source_type = source.type if source else incident.node_types.get(key[0], NodeType.SERVICE)
    anchor = key[0] if source_type is NodeType.SERVICE else key[1]
    healthy_traces = full.node(anchor).traces if full.node(anchor) else 0
    now_traces = incident.services[anchor] if anchor in queried else 0
    if h and not (i and i.calls):
        return h.calls / healthy_traces * now_traces if healthy_traces and now_traces else None
    if i and i.calls and h is None:
        if not now_traces:
            return None
        if not healthy_traces:
            return float("inf")  # the service itself is new
        return i.calls / now_traces * healthy_traces
    return None


def compare(healthy: ServiceMap, incident: Observations, queried: set[str], full: ServiceMap | None = None,
            before: Observations | None = None) -> tuple[list[NodeView], list[EdgeView]]:
    """Compare `healthy` (usually a neighborhood of `full`) with what traces of `queried` services show now.

    Nodes are listed for the queried services only; other nodes are visible as edge ends.
    """
    full = full or healthy
    healthy_edges = {e.key: e for e in healthy.edges}
    known_elsewhere = {e.key for e in full.edges} - healthy_edges.keys()
    names = {n.name for n in healthy.nodes} | queried
    # Incident edges in the area, plus new ones touching it. Known edges outside the area stay out.
    incident_edges = {k: s for k, s in incident.edges.items()
                      if (k[0] in names or k[1] in names) and k not in known_elsewhere}

    edges = []
    for key in sorted(healthy_edges.keys() | incident_edges.keys()):
        h, i = healthy_edges.get(key), incident_edges.get(key)
        b = before.edges.get(key) if before else None
        edges.append(EdgeView(
            source=key[0], target=key[1], type=key[2], healthy=_numbers(h) if h else None,
            before=_numbers(b) if b and b.calls else None, incident=_numbers(i) if i and i.calls else None,
            changes=edge_changes(h, i, _expected_calls(key, h, i, full, incident, queried), b),
        ))
    edges.sort(key=lambda e: (not e.changes, -(e.incident.calls if e.incident else 0)))

    nodes = []
    for name in sorted(queried):
        h = full.node(name)
        seen = incident.services[name]
        nodes.append(NodeView(
            name=name, healthy_traces=h.traces if h else 0, incident_traces=seen,
            changes=[NodeChange.NOT_SEEN] if seen == 0 and h and h.traces else [],
        ))
    nodes.sort(key=lambda n: (not n.changes, n.name))
    return nodes, edges
