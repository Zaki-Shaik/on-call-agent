"""Backend-neutral types shared by the telemetry adapters and the tools."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, model_validator


class BackendError(Exception):
    """A backend was unreachable, timed out, or returned something unusable.

    Tools turn this into an explicit error status, never into empty or zero data.
    """


class TimeWindow(BaseModel):
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def _check(self) -> "TimeWindow":
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("time window bounds must be timezone-aware")
        if self.end <= self.start:
            raise ValueError("time window end must be after start")
        return self


class Signal(StrEnum):
    LATENCY_P50 = "latency_p50"
    LATENCY_P95 = "latency_p95"
    LATENCY_P99 = "latency_p99"
    TRAFFIC = "traffic"
    ERROR_RATE = "error_rate"
    CPU = "cpu"
    MEMORY = "memory"


SIGNAL_UNITS = {
    Signal.LATENCY_P50: "ms",
    Signal.LATENCY_P95: "ms",
    Signal.LATENCY_P99: "ms",
    Signal.TRAFFIC: "req/s",
    Signal.ERROR_RATE: "ratio",
    Signal.CPU: "cores",
    Signal.MEMORY: "bytes",
}

# A time series: (timestamp, value) pairs in time order. Gaps are simply missing points.
Series = list[tuple[datetime, float]]


@dataclass(frozen=True)
class Span:
    trace_id: str
    span_id: str
    parent_id: str | None
    service: str
    operation: str
    kind: str  # server, client, producer, consumer, internal
    start_us: int
    duration_us: int
    error: bool
    status_message: str = ""
    # (trace_id, span_id) of linked spans, e.g. the producer of a consumed message.
    links: tuple[tuple[str, str], ...] = ()
    attributes: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Trace:
    trace_id: str
    spans: tuple[Span, ...]


@dataclass(frozen=True)
class LogRecord:
    timestamp: datetime
    severity_text: str
    severity_number: int
    body: str
    trace_id: str | None = None
