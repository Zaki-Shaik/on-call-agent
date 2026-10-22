from datetime import datetime, timedelta, timezone

import pytest

from oncall.telemetry.models import LogRecord
from oncall.tools.logs import group_patterns, pattern_of

T0 = datetime(2026, 10, 22, 3, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(("body", "expected"), [
    ("Successful to write message. offset: 0, duration: 34.425778ms",
     "Successful to write message. offset: <*>, duration: <*>"),
    ("Order 9f8b2c1e-4d3a-4b5c-8e7f-1a2b3c4d5e6f placed by user 42",
     "Order <*> placed by user <*>"),
    ("connect to 10.0.3.17:50051 failed", "connect to <*> failed"),
    ('Payment request failed. Invalid token. app.loyalty.level=gold',
     "Payment request failed. Invalid token. app.loyalty.level=gold"),
    ("product OLJCESPC7Z not found", "product <*> not found"),
    ("span 4bf92f3577b34da6a3ce929d0e0e4736 done", "span <*> done"),
])
def test_pattern_of(body: str, expected: str) -> None:
    assert pattern_of(body) == expected


def record(body: str, minutes: int, severity: str = "INFO", number: int = 9, trace: str | None = None):
    return LogRecord(T0 + timedelta(minutes=minutes), severity, number, body, trace)


def test_group_patterns_counts_orders_and_marks_new() -> None:
    records = [
        record("charged order 1", 1), record("charged order 2", 2), record("charged order 3", 3),
        record("Payment request failed: token 77", 2, "ERROR", 17, trace="abc"),
        record("Payment request failed: token 78", 4, "ERROR", 17),
    ]
    known = {pattern_of("charged order 0")}

    patterns = group_patterns(records, known)

    assert [p.pattern for p in patterns] == ["Payment request failed: token <*>", "charged order <*>"]
    error, info = patterns
    assert (error.count, error.new, error.example_trace_id) == (2, True, "abc")
    assert (error.first_seen, error.last_seen) == (T0 + timedelta(minutes=2), T0 + timedelta(minutes=4))
    assert (info.count, info.new) == (3, False)


def test_new_is_unknown_without_a_baseline() -> None:
    [p] = group_patterns([record("charged order 1", 1)], known=None)
    assert p.new is None
