"""The four tools with fake backends and the real evidence store."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from oncall.evidence import EvidenceStore
from oncall.links import Linker
from oncall.profile import load_profile
from oncall.telemetry.factory import Backends
from oncall.telemetry.models import BackendError, LogRecord, Signal, TimeWindow
from oncall.telemetry.traces import parse_jaeger_trace
from oncall.tools.base import Status, ToolContext
from oncall.tools.changes import list_recent_changes
from oncall.tools.logs import search_logs
from oncall.tools.metrics import query_metrics
from oncall.tools.traces import get_traces

ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 10, 22, 3, 30, tzinfo=timezone.utc)
WINDOW = TimeWindow(start=T0, end=T0 + timedelta(minutes=20))


class FakeMetrics:
    def series(self, service, signal, start, end, step_s):
        if signal is Signal.CPU:
            raise BackendError("prometheus: timed out")
        if signal is Signal.MEMORY:
            return []
        points, t = [], start
        while t <= end:
            points.append((t, 0.5 if signal is Signal.ERROR_RATE and t >= T0 + timedelta(minutes=5) else 0.0))
            t += timedelta(seconds=step_s)
        return points

    def query_text(self, service, signal):
        return f"q_{signal.value}"


class FakeLogs:
    def __init__(self, fail=False):
        self.fail = fail

    def counts_by_severity(self, service, start, end):
        if self.fail:
            raise BackendError("opensearch: unreachable")
        return {"ERROR": 2, "INFO": 3} if start >= T0 else {"INFO": 50}

    def sample(self, service, start, end, size, min_severity=None, text=None):
        if start >= T0:
            return 5, [LogRecord(T0 + timedelta(minutes=i), "ERROR" if i < 2 else "INFO", 17 if i < 2 else 9,
                                 f"Payment request failed token {i}" if i < 2 else f"charged {i}", "t1")
                       for i in range(5)]
        return 1, [LogRecord(T0 - timedelta(minutes=5), "INFO", 9, "charged 0")]

    def query_text(self, service, min_severity=None, text=None):
        return f'resource.service.name:"{service}"'


class FakeTraces:
    def __init__(self, traces):
        self.traces = traces

    def find_traces(self, service, start, end, limit, operation=None, errors_only=False):
        return self.traces


@pytest.fixture
def incident_id(pool):
    with pool.connection() as conn:
        return conn.execute("INSERT INTO incidents (status, service, system_profile, opened_at)"
                            " VALUES ('investigating', 'payment', 'otel-demo', %s) RETURNING id", (T0,)).fetchone()["id"]


def ctx(pool, incident_id, metrics=None, logs=None, traces=None) -> ToolContext:
    profile = load_profile(ROOT / "profiles" / "otel-demo.yaml", env={})
    return ToolContext(profile, Backends(metrics or FakeMetrics(), traces or FakeTraces([]), logs, None),
                       Linker(profile.links), EvidenceStore(pool), pool, incident_id)


def stored(pool, incident_id) -> dict[str, dict]:
    with pool.connection() as conn:
        return {r["ref"]: r for r in conn.execute("SELECT * FROM evidence WHERE incident_id = %s", (incident_id,))}


def test_query_metrics_statuses_and_evidence(pool, incident_id) -> None:
    result = query_metrics(ctx(pool, incident_id), "payment", WINDOW,
                           [Signal.ERROR_RATE, Signal.TRAFFIC, Signal.CPU, Signal.MEMORY])
    by = {s.signal: s for s in result.signals}

    err = by[Signal.ERROR_RATE]
    assert err.status is Status.OK and err.baseline == 0.0 and err.current == 0.5 and err.direction == "up"
    assert err.onset == T0 + timedelta(minutes=5)
    assert by[Signal.TRAFFIC].direction == "flat"
    assert by[Signal.CPU].status is Status.ERROR and "timed out" in by[Signal.CPU].note
    assert by[Signal.CPU].evidence is None  # a failure is not evidence
    assert by[Signal.MEMORY].status is Status.NO_DATA and by[Signal.MEMORY].current is None

    evidence = stored(pool, incident_id)
    assert [err.evidence, by[Signal.TRAFFIC].evidence, by[Signal.MEMORY].evidence] == ["E1", "E2", "E3"]
    assert "baseline 0.0 -> current 0.5" in evidence["E1"]["summary"]
    assert evidence["E1"]["link"].startswith("http://localhost:8080/grafana/explore?")
    assert result.baseline_window.end == WINDOW.start


def test_search_logs_patterns_new_and_counts(pool, incident_id) -> None:
    result = search_logs(ctx(pool, incident_id, logs=FakeLogs()), "payment", WINDOW)

    assert result.status is Status.OK
    assert result.severity_counts == {"ERROR": 2, "INFO": 3}
    assert result.baseline_severity_counts == {"INFO": 50}
    top = result.patterns[0]
    assert (top.pattern, top.severity, top.count, top.new) == ("Payment request failed token <*>", "ERROR", 2, True)
    assert result.patterns[1].new is False  # "charged <*>" also appears in the baseline
    assert set(stored(pool, incident_id)) == {"E1", "E2"}


def test_search_logs_backend_failure_is_explicit(pool, incident_id) -> None:
    result = search_logs(ctx(pool, incident_id, logs=FakeLogs(fail=True)), "payment", WINDOW)
    assert result.status is Status.ERROR and "unreachable" in result.note
    assert stored(pool, incident_id) == {}


def test_search_logs_without_logs_backend(pool, incident_id) -> None:
    assert search_logs(ctx(pool, incident_id), "payment", WINDOW).status is Status.ERROR


def test_get_traces_on_recorded_error_trace(pool, incident_id) -> None:
    raw = json.loads((ROOT / "tests" / "fixtures" / "backends" / "jaeger_checkout_error_traces.json").read_text())
    traces = [parse_jaeger_trace(t) for t in raw["data"]]

    result = get_traces(ctx(pool, incident_id, traces=FakeTraces(traces)), "checkout", WINDOW)

    assert result.status is Status.OK and result.traces_sampled == 1
    payment_edge = next(e for e in result.downstream if e.peer == "payment")
    assert payment_edge.errors >= 1 and payment_edge.example_error_trace == traces[0].trace_id
    assert any(e.peer for e in result.upstream)
    assert abs(sum(p.share_pct for p in result.critical_path) - 100) < 5
    refs = stored(pool, incident_id)
    assert payment_edge.evidence in refs
    assert refs[payment_edge.evidence]["link"].endswith(f"/trace/{traces[0].trace_id}")


def test_get_traces_no_data_and_error(pool, incident_id) -> None:
    assert get_traces(ctx(pool, incident_id), "checkout", WINDOW).status is Status.NO_DATA

    class Failing:
        def find_traces(self, *a, **kw):
            raise BackendError("jaeger: unreachable")

    result = get_traces(ctx(pool, incident_id, traces=Failing()), "checkout", WINDOW)
    assert result.status is Status.ERROR and "unreachable" in result.note


def test_list_recent_changes_filters_window_and_services(pool, incident_id) -> None:
    with pool.connection() as conn:
        for minutes, type_, service, desc in [(-30, "flag_change", None, "too early"),
                                              (2, "flag_change", None, "flag paymentFailure: off -> 50%"),
                                              (3, "deploy", "cart", "cart version 1 -> 2"),
                                              (4, "deploy", "email", "email version 1 -> 2")]:
            conn.execute("INSERT INTO change_events (system_profile, occurred_at, type, service, actor, description, details)"
                         " VALUES ('otel-demo', %s, %s, %s, 'unknown', %s, '{}')",
                         (T0 + timedelta(minutes=minutes), type_, service, desc))

    everything = list_recent_changes(ctx(pool, incident_id), WINDOW)
    assert [c.description for c in everything.changes] == [
        "flag paymentFailure: off -> 50%", "cart version 1 -> 2", "email version 1 -> 2"]
    assert [c.evidence for c in everything.changes] == ["E1", "E2", "E3"]

    only_cart = list_recent_changes(ctx(pool, incident_id), WINDOW, services=["cart"])
    assert [c.description for c in only_cart.changes] == ["flag paymentFailure: off -> 50%", "cart version 1 -> 2"]

    quiet = TimeWindow(start=T0 + timedelta(hours=2), end=T0 + timedelta(hours=3))
    assert list_recent_changes(ctx(pool, incident_id), quiet).status is Status.NO_DATA
