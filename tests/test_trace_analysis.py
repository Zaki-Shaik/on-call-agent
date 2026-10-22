from oncall.telemetry.models import Span, Trace
from oncall.tools.traces import critical_path


def span(span_id, parent, service, start, duration, kind="server"):
    return Span(trace_id="t1", span_id=span_id, parent_id=parent, service=service, operation=f"op-{span_id}",
                kind=kind, start_us=start, duration_us=duration, error=False)


def test_sequential_children_split_the_time() -> None:
    # frontend 0..100 calls checkout 10..40, then payment 50..90.
    trace = Trace("t1", (
        span("a", None, "frontend", 0, 100),
        span("b", "a", "checkout", 10, 30),
        span("c", "a", "payment", 50, 40),
    ))
    assert critical_path(trace) == {"frontend": 30, "checkout": 30, "payment": 40}


def test_parallel_children_only_the_last_to_finish_counts() -> None:
    # Two parallel calls; the one ending at 90 decides when the parent can finish.
    trace = Trace("t1", (
        span("a", None, "frontend", 0, 100),
        span("b", "a", "cart", 10, 50),       # 10..60, hidden behind the slower call
        span("c", "a", "currency", 10, 80),   # 10..90
    ))
    assert critical_path(trace) == {"frontend": 20, "currency": 80}


def test_nested_calls() -> None:
    trace = Trace("t1", (
        span("a", None, "frontend", 0, 100),
        span("b", "a", "checkout", 10, 80),
        span("c", "b", "payment", 20, 60),
    ))
    assert critical_path(trace) == {"frontend": 20, "checkout": 20, "payment": 60}


def test_child_outliving_its_parent_is_clipped() -> None:
    # Async work that ends after the parent (e.g. fire-and-forget) only counts up to the parent's end.
    trace = Trace("t1", (
        span("a", None, "frontend", 0, 50),
        span("b", "a", "email", 30, 100),
    ))
    assert critical_path(trace) == {"frontend": 30, "email": 20}
