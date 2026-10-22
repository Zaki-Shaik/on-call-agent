"""Service map snapshots in Postgres and the get_service_map tool, with a fake trace backend."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from oncall.evidence import EvidenceStore
from oncall.links import Linker
from oncall.profile import load_profile
from oncall.servicemap.compare import EdgeChange, NodeChange
from oncall.servicemap.model import EdgeType
from oncall.servicemap.store import MAX_OPEN_EXCLUSION, ServiceMaps
from oncall.telemetry.factory import Backends
from oncall.telemetry.models import BackendError, Span, TimeWindow, Trace
from oncall.tools.base import Status, ToolContext
from oncall.tools.servicemap import get_service_map

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
INCIDENT = TimeWindow(start=NOW - timedelta(minutes=20), end=NOW)


def us(t: datetime) -> int:
    return int(t.timestamp() * 1_000_000)


def checkout_trace(trace_id: str, at: datetime, payment_error: bool = False, email: bool = True) -> Trace:
    """frontend -> checkout -> payment (+ email)."""
    t0 = us(at)
    spans = [
        Span(trace_id, "a", None, "frontend", "PlaceOrder", "client", t0, 900, False),
        Span(trace_id, "b", "a", "checkout", "PlaceOrder", "server", t0 + 10, 800, payment_error),
        Span(trace_id, "c", "b", "checkout", "Charge", "client", t0 + 20, 100, payment_error),
        Span(trace_id, "d", "c", "payment", "Charge", "server", t0 + 30, 80, payment_error),
    ]
    if email:
        spans += [Span(trace_id, "e", "b", "checkout", "SendConfirmation", "client", t0 + 200, 50, False),
                  Span(trace_id, "f", "e", "email", "SendConfirmation", "server", t0 + 210, 30, False)]
    return Trace(trace_id, tuple(spans))


class FakeTraceBackend:
    def __init__(self, traces: list[Trace], fail: bool = False) -> None:
        self.traces = traces
        self.fail = fail
        self.searches = 0

    def services(self) -> list[str]:
        if self.fail:
            raise BackendError("jaeger: unreachable")
        return sorted({s.service for t in self.traces for s in t.spans})

    def find_traces(self, service, start, end, limit, operation=None, errors_only=False):
        self.searches += 1
        if self.fail:
            raise BackendError("jaeger: unreachable")
        found = [t for t in self.traces
                 if any(s.service == service for s in t.spans)
                 and us(start) <= min(s.start_us for s in t.spans) < us(end)
                 and (not errors_only or any(s.error for s in t.spans))]
        return sorted(found, key=lambda t: -min(s.start_us for s in t.spans))[:limit]

    def get_trace(self, trace_id):
        return next((t for t in self.traces if t.trace_id == trace_id), None)


def healthy_traces() -> list[Trace]:
    # Every 10 minutes over the last day.
    return [checkout_trace(f"h{i}", NOW - timedelta(minutes=10 * i)) for i in range(3, 144)]


def add_incident(pool, opened_at: datetime, resolved_at: datetime | None) -> None:
    with pool.connection() as conn:
        incident = conn.execute(
            "INSERT INTO incidents (status, service, system_profile, opened_at)"
            " VALUES ('investigating', 'payment', 'otel-demo', %s) RETURNING id", (opened_at,)).fetchone()["id"]
        conn.execute(
            "INSERT INTO alerts (source, fingerprint, starts_at, ends_at, status, service, severity, summary,"
            " incident_id, raw_payload) VALUES ('alertmanager', 'f1', %s, %s, %s, 'payment', 'critical', 's', %s, '{}')",
            (opened_at, resolved_at, "resolved" if resolved_at else "firing", incident))


def maps(pool, backend, now=NOW) -> ServiceMaps:
    return ServiceMaps(pool, "otel-demo", backend, clock=lambda: now)


def test_build_stores_a_snapshot_without_incident_periods(pool) -> None:
    opened = NOW - timedelta(hours=6)
    add_incident(pool, opened, resolved_at=opened + timedelta(hours=1))
    # Errors during that incident must not make it into the healthy map.
    failing = [checkout_trace(f"x{i}", opened + timedelta(minutes=i), payment_error=True) for i in range(0, 60, 2)]
    backend = FakeTraceBackend(healthy_traces() + failing)

    snapshot = maps(pool, backend).build_healthy()

    assert snapshot.excluded == [TimeWindow(start=opened - timedelta(minutes=30),
                                            end=opened + timedelta(hours=1, minutes=10))]
    edges = {e.key: e for e in snapshot.map.edges}
    payment = edges[("checkout", "payment", EdgeType.CALL)]
    assert payment.calls > 0 and payment.errors == 0
    assert maps(pool, backend).latest().map == snapshot.map


def test_open_incident_exclusion_is_capped(pool) -> None:
    opened = NOW - timedelta(hours=20)
    add_incident(pool, opened, resolved_at=None)  # its resolve notification never came
    [period] = maps(pool, FakeTraceBackend([])).incident_periods(TimeWindow(start=NOW - timedelta(hours=24), end=NOW))
    assert period.end == opened + MAX_OPEN_EXCLUSION + timedelta(minutes=10)


def test_healthy_reuses_a_fresh_map_and_rebuilds_a_stale_one(pool) -> None:
    backend = FakeTraceBackend(healthy_traces())
    first = maps(pool, backend).build_healthy()
    searches = backend.searches

    assert maps(pool, backend, NOW + timedelta(minutes=90)).healthy().id == first.id
    assert backend.searches == searches

    rebuilt = maps(pool, backend, NOW + timedelta(hours=3)).healthy()
    assert rebuilt.id != first.id and not rebuilt.stale


def test_healthy_falls_back_to_a_stale_map_when_rebuilding_fails(pool) -> None:
    first = maps(pool, FakeTraceBackend(healthy_traces())).build_healthy()

    later = maps(pool, FakeTraceBackend([], fail=True), NOW + timedelta(hours=3)).healthy()

    assert later.id == first.id and later.stale
    with pytest.raises(BackendError):
        maps(pool, FakeTraceBackend([], fail=True)).build_healthy()


@pytest.fixture
def incident_id(pool):
    with pool.connection() as conn:
        return conn.execute("INSERT INTO incidents (status, service, system_profile, opened_at)"
                            " VALUES ('investigating', 'payment', 'otel-demo', %s) RETURNING id",
                            (INCIDENT.start,)).fetchone()["id"]


def ctx(pool, incident_id, service_maps) -> ToolContext:
    profile = load_profile(ROOT / "profiles" / "otel-demo.yaml", env={})
    return ToolContext(profile, Backends(None, service_maps._traces, None, None), Linker(profile.links),
                       EvidenceStore(pool), pool, incident_id, service_maps)


def test_get_service_map_highlights_the_failing_edge(pool, incident_id) -> None:
    # During the incident: payment fails half the time, and orders that fail send no email.
    now_traces = [checkout_trace(f"i{i}", INCIDENT.start + timedelta(seconds=20 * i),
                                 payment_error=i % 2 == 0, email=i % 2 == 1) for i in range(60)]
    backend = FakeTraceBackend(healthy_traces() + now_traces)
    service_maps = maps(pool, backend)
    service_maps.build_healthy()

    result = get_service_map(ctx(pool, incident_id, service_maps), INCIDENT, "payment", hops=2)

    assert result.status is Status.OK
    first = result.edges[0]
    assert (first.source, first.target, first.changes) == ("checkout", "payment", [EdgeChange.ERRORS_UP])
    assert first.healthy.errors == 0 and first.incident.errors > 0
    with pool.connection() as conn:
        row = conn.execute("SELECT summary, link FROM evidence WHERE incident_id = %s AND ref = %s",
                           (incident_id, first.evidence)).fetchone()
    assert "checkout -> payment (call)" in row["summary"] and "errors_up" in row["summary"]
    assert "/trace/i" in row["link"]  # an example error trace
    # Two hops from payment reach checkout's caller, not checkout's other callee.
    assert {(e.source, e.target) for e in result.edges} == {("checkout", "payment"), ("frontend", "checkout")}


def test_get_service_map_unseen_service_and_errors(pool, incident_id) -> None:
    backend = FakeTraceBackend(healthy_traces())
    service_maps = maps(pool, backend)
    service_maps.build_healthy()

    # Nothing at all in the window: every service in view went quiet.
    result = get_service_map(ctx(pool, incident_id, service_maps), INCIDENT, "email", hops=1)
    assert result.status is Status.OK
    email = next(n for n in result.nodes if n.name == "email")
    assert email.changes == [NodeChange.NOT_SEEN] and email.evidence

    backend.fail = True
    failed = get_service_map(ctx(pool, incident_id, service_maps), INCIDENT, "email", hops=1)
    assert failed.status is Status.ERROR

    c = ctx(pool, incident_id, service_maps)
    no_maps = ToolContext(c.profile, c.backends, c.links, c.evidence, pool, incident_id)
    assert get_service_map(no_maps, INCIDENT, "email").status is Status.ERROR
    with pytest.raises(ValueError):
        get_service_map(no_maps, INCIDENT, "email", hops=5)
