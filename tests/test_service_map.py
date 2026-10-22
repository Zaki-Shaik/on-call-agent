"""Service map: edge extraction, neighborhoods, comparison and sampling windows (no backends)."""

from datetime import datetime, timedelta, timezone

import pytest

from oncall.servicemap.compare import EdgeChange, NodeChange, binomial_sf, compare, edge_changes
from oncall.servicemap.model import EdgeSamples, EdgeType, NodeType, Observations, extract, neighborhood
from oncall.servicemap.store import sample_windows
from oncall.telemetry.models import Span, TimeWindow, Trace

T0 = datetime(2026, 10, 22, 3, 0, tzinfo=timezone.utc)


def span(trace, span_id, parent, service, kind, start=0, duration=100, error=False, op=None, links=(), **attrs):
    return Span(trace_id=trace, span_id=span_id, parent_id=parent, service=service, operation=op or f"op-{span_id}",
                kind=kind, start_us=start, duration_us=duration, error=error, links=tuple(links), attributes=attrs)


def edges_of(obs: Observations) -> dict:
    return {(e.source, e.target, e.type): e for e in obs.to_map().edges}


def test_client_span_with_traced_child_is_a_call() -> None:
    t = Trace("t1", (
        span("t1", "a", None, "frontend", "server", 0, 100),
        span("t1", "b", "a", "frontend", "client", 10, 50, error=True, op="GetCart"),
        span("t1", "c", "b", "cart", "server", 15, 40, error=True),
    ))
    [edge] = extract([t]).to_map().edges
    assert (edge.source, edge.target, edge.type) == ("frontend", "cart", EdgeType.CALL)
    # Caller-side duration and operation; an error on either side counts.
    assert (edge.calls, edge.errors, edge.p95_ms, edge.operations) == (1, 1, 0.05, ["GetCart"])
    assert edge.example_error_trace == "t1"


def test_untraced_peers_are_named_from_attributes_but_not_by_ip() -> None:
    t = Trace("t1", (
        span("t1", "a", None, "cart", "server"),
        span("t1", "b", "a", "cart", "client", **{"server.address": "valkey-cart", "db.system": "redis"}),
        span("t1", "c", "a", "cart", "client", **{"db.system": "redis"}),
        span("t1", "d", "a", "cart", "client", **{"server.address": "10.0.0.7"}),
    ))
    obs = extract([t])
    assert set(edges_of(obs)) == {("cart", "valkey-cart", EdgeType.CALL), ("cart", "redis", EdgeType.CALL)}
    assert obs.to_map().node("valkey-cart").type is NodeType.EXTERNAL


def test_call_to_a_traced_service_that_is_down_still_points_at_it() -> None:
    # The callee produced no span (it is down); the client span names it or matches a known operation.
    t = Trace("t1", (
        span("t1", "a", None, "checkout", "server"),
        span("t1", "b", "a", "checkout", "client", error=True, **{"server.address": "cart"}),
        span("t1", "c", "a", "checkout", "client", error=True, op="Charge", **{"server.address": "10.0.0.9"}),
    ))
    obs = extract([t], known_services={"cart", "payment", "checkout"},
                  known_operations={("checkout", "Charge"): "payment"})
    edges = edges_of(obs)
    assert edges[("checkout", "cart", EdgeType.CALL)].errors == 1
    assert edges[("checkout", "payment", EdgeType.CALL)].errors == 1
    assert obs.to_map().node("cart").type is NodeType.SERVICE


def test_server_span_under_a_non_client_parent_is_a_call() -> None:
    t = Trace("t1", (
        span("t1", "a", None, "frontend", "internal"),
        span("t1", "b", "a", "ad", "server", duration=30),
    ))
    assert ("frontend", "ad", EdgeType.CALL) in edges_of(extract([t]))


def test_messaging_publish_and_consume_with_lag_from_the_linked_producer() -> None:
    producer = Trace("p", (
        span("p", "a", None, "checkout", "server", 0, 1000),
        span("p", "b", "a", "checkout", "producer", 100, 50, **{"messaging.destination.name": "orders"}),
    ))
    # Two consumer spans for one message (receive + process): one observation, from the linked span.
    consumer = Trace("c", (
        span("c", "x", None, "fraud", "consumer", 400, 30, **{"messaging.destination.name": "orders"}),
        span("c", "y", "x", "fraud", "consumer", 410, 20, links=[("p", "b")],
             **{"messaging.destination.name": "orders"}),
    ))
    obs = extract([producer, consumer])
    edges = edges_of(obs)
    assert edges[("checkout", "orders", EdgeType.PUBLISH)].calls == 1
    consume = edges[("orders", "fraud", EdgeType.CONSUME)]
    assert (consume.calls, consume.lag_p95_ms) == (1, 0.26)  # 410 - (100 + 50) us
    assert obs.to_map().node("orders").type is NodeType.TOPIC


def test_consumer_without_destination_links_to_the_producer_service() -> None:
    producer_span = span("p", "b", None, "checkout", "producer", 100, 50)
    consumer = Trace("c", (span("c", "y", None, "accounting", "consumer", 300, 20, links=[("p", "b")]),))
    obs = extract([consumer], linked={("p", "b"): producer_span})
    assert edges_of(obs)[("checkout", "accounting", EdgeType.CONSUME)].lag_p95_ms == 0.15


def test_recorded_jaeger_traces() -> None:
    import json
    from pathlib import Path

    from oncall.telemetry.traces import parse_jaeger_trace

    fixtures = Path(__file__).parent / "fixtures" / "backends"
    raw = json.loads((fixtures / "jaeger_map_traces.json").read_text())
    traces = [parse_jaeger_trace(t) for t in raw["data"]]
    services = json.loads((fixtures / "jaeger_services.json").read_text())["data"]

    edges = edges_of(extract(traces, set(services)))

    assert ("checkout", "payment", EdgeType.CALL) in edges
    assert ("checkout", "orders", EdgeType.PUBLISH) in edges
    assert edges[("orders", "fraud-detection", EdgeType.CONSUME)].lag_p95_ms is not None
    assert ("orders", "accounting", EdgeType.CONSUME) in edges
    assert not any(target[0].isdigit() for _, target, _ in edges)  # no IP-named nodes


def test_neighborhood_follows_both_directions_for_n_hops() -> None:
    traces = [Trace("t1", (
        span("t1", "a", None, "lb", "client"),
        span("t1", "b", "a", "web", "server"),
        span("t1", "c", "b", "web", "client"),
        span("t1", "d", "c", "checkout", "server"),
        span("t1", "e", "d", "checkout", "client"),
        span("t1", "f", "e", "payment", "server"),
        span("t1", "g", "f", "payment", "client"),
        span("t1", "h", "g", "bank", "server"),
        span("t1", "i", "b", "web", "client"),
        span("t1", "j", "i", "ads", "server"),
    ))]
    m = extract(traces).to_map()
    one = neighborhood(m, "checkout", 1)
    assert {(e.source, e.target) for e in one.edges} == {("web", "checkout"), ("checkout", "payment")}
    two = neighborhood(m, "checkout", 2)
    # Two hops reach lb (upstream) and bank (downstream), but not web's other callee.
    assert {(e.source, e.target) for e in two.edges} == {
        ("lb", "web"), ("web", "checkout"), ("checkout", "payment"), ("payment", "bank")}
    assert "ads" not in {n.name for n in two.nodes}


def test_binomial_tail() -> None:
    assert binomial_sf(0, 10, 0.5) == 1.0
    assert binomial_sf(10, 10, 0.5) == pytest.approx(0.5 ** 10)
    assert binomial_sf(2, 3, 0.5) == pytest.approx(0.5)
    assert binomial_sf(1, 5, 0.0) == 0.0


def samples(calls: int, errors: int = 0, duration_us: int = 1000, slow: int = 0, slow_us: int = 50_000):
    s = EdgeSamples()
    s.durations_us = [slow_us] * slow + [duration_us] * (calls - slow)
    s.errors = errors
    return s


def healthy_edge(calls=200, errors=0, p95_ms=2.0):
    return samples(calls, errors, 1500).summary("checkout", "payment", EdgeType.CALL).model_copy(
        update={"p95_ms": p95_ms})


@pytest.mark.parametrize(("errors", "calls", "flagged"), [
    (1, 20, False),   # a single error is never a pattern
    (2, 40, False),   # within what a clean edge could produce by chance
    (8, 39, True),    # paymentFailure at ~20%
])
def test_errors_up_is_a_significance_test(errors, calls, flagged) -> None:
    changes = edge_changes(healthy_edge(), samples(calls, errors), None)
    assert (EdgeChange.ERRORS_UP in changes) is flagged


def test_noisy_healthy_edge_needs_more_errors() -> None:
    noisy = healthy_edge(calls=200, errors=20)  # 10% errors when healthy
    assert edge_changes(noisy, samples(40, 6), None) == []
    assert EdgeChange.ERRORS_UP in edge_changes(noisy, samples(40, 20), None)


def test_slower_counts_calls_above_the_p95_of_the_hour_before() -> None:
    before = samples(40, duration_us=1000)
    assert edge_changes(healthy_edge(), samples(40, slow=2), None, before) == []
    assert edge_changes(healthy_edge(), samples(40, slow=12), None, before) == [EdgeChange.SLOWER]
    # Every call a little slower: statistically certain, but too small a change to flag.
    assert edge_changes(healthy_edge(), samples(40, duration_us=1300), None, before) == []
    # Without a usable hour before (e.g. the system just started), latency is not judged.
    assert edge_changes(healthy_edge(), samples(40, slow=12), None, samples(5)) == []
    assert edge_changes(healthy_edge(), samples(40, slow=12), None, None) == []


def test_consumer_lag_up() -> None:
    before, now = samples(30), samples(30)
    before.lags_us = [2000] * 30
    now.lags_us = [2000] * 20 + [900_000] * 10  # a backlog builds up
    assert edge_changes(healthy_edge(), now, None, before) == [EdgeChange.LAG_UP]


def test_missing_and_new_edges() -> None:
    h = healthy_edge()
    assert edge_changes(h, None, expected_calls=10) == [EdgeChange.MISSING]
    assert edge_changes(h, None, expected_calls=3) == []  # zero calls is plausible by chance
    assert edge_changes(h, None, expected_calls=None) == []  # caller not sampled: unknown
    # A new edge is only news if the healthy sample should have caught it.
    assert edge_changes(None, samples(5), expected_calls=10) == [EdgeChange.NEW]
    assert edge_changes(None, samples(5), expected_calls=3) == []  # rare edge the healthy sample missed
    assert edge_changes(None, samples(5), expected_calls=None) == []


def test_compare_marks_failing_edge_missing_edge_and_unseen_service() -> None:
    def call(trace, caller, callee, error=False):
        return (span(trace, f"{trace}c", None, caller, "client", error=error),
                span(trace, f"{trace}s", f"{trace}c", callee, "server", error=error))

    healthy = extract([Trace(f"h{i}", (*call(f"h{i}", "checkout", "payment"), *call(f"h{i}x", "checkout", "email"),
                                       *call(f"h{i}y", "frontend", "ad")))
                       for i in range(50)]).to_map()
    # Now: payment fails half the time, checkout no longer calls email but calls a new fallback
    # in every trace, ad is gone. frontend -> ad is known but outside the area, so it stays out.
    incident = extract([Trace(f"i{i}", (*call(f"i{i}", "checkout", "payment", error=i % 2 == 0),
                                        *call(f"i{i}z", "checkout", "fallback")))
                        for i in range(30)])

    area = neighborhood(healthy, "checkout", 1)
    nodes, edges = compare(area, incident, {"checkout", "payment", "email", "ad"}, full=healthy)

    by_edge = {(e.source, e.target): e for e in edges}
    assert by_edge[("checkout", "payment")].changes == [EdgeChange.ERRORS_UP]
    assert by_edge[("checkout", "email")].changes == [EdgeChange.MISSING]
    assert by_edge[("checkout", "fallback")].changes == [EdgeChange.NEW]
    assert edges[0].changes  # changed edges first
    assert ("frontend", "ad") not in by_edge
    by_node = {n.name: n for n in nodes}
    assert by_node["ad"].changes == [NodeChange.NOT_SEEN]
    assert by_node["checkout"].changes == []


def test_sample_windows_skip_excluded_periods() -> None:
    window = TimeWindow(start=T0, end=T0 + timedelta(hours=4))
    excluded = [TimeWindow(start=T0 + timedelta(minutes=30), end=T0 + timedelta(hours=2, minutes=50))]

    slices = sample_windows(window, excluded, slices=4)

    assert [(w.start - T0, w.end - T0) for w in slices] == [
        (timedelta(0), timedelta(minutes=30)),
        (timedelta(hours=2, minutes=50), timedelta(hours=3)),
        (timedelta(hours=3), timedelta(hours=4)),
    ]
