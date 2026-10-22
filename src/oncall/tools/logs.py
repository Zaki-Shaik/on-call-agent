"""search_logs: grouped log patterns with counts and first-seen times, not raw lines.

Log text is untrusted (anyone who can make a service log can write it), so it is shortened
here and labeled as untrusted data before the model reads it (Phase 4).
"""

import re
from collections import defaultdict
from datetime import datetime, timedelta

from pydantic import BaseModel

from oncall.evidence import EvidenceDraft
from oncall.telemetry.logs import SEVERITY_MIN
from oncall.telemetry.models import BackendError, LogRecord, TimeWindow
from oncall.tools.base import Status, ToolContext

SAMPLE_SIZE = 500
TOP = 10
MAX_PATTERN_CHARS = 200
BASELINE = timedelta(hours=1)

# Order matters: specific shapes before the generic number rule.
_VARIABLE_PARTS = [
    re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I),  # uuid
    re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b"),  # ipv4[:port]
    re.compile(r"\b(?=[0-9a-f]*\d)[0-9a-f]{12,}\b", re.I),  # long hex ids
    re.compile(r"\"[^\"]{0,200}\"|'[^']{0,200}'"),  # quoted values
    re.compile(r"\b[A-Z0-9]{8,}\b"),  # opaque upper-case ids
    re.compile(r"-?\d+(?:\.\d+)?(?:[a-zµ%]{1,3})?\b"),  # numbers, durations, sizes
]
_SPACES = re.compile(r"\s+")


def pattern_of(body: str) -> str:
    text = body
    for regex in _VARIABLE_PARTS:
        text = regex.sub("<*>", text)
    return _SPACES.sub(" ", text).strip()[:MAX_PATTERN_CHARS]


class LogPattern(BaseModel):
    pattern: str
    severity: str
    count: int
    first_seen: datetime
    last_seen: datetime
    # Not seen in the hour before the window. None when that hour has no logs to compare with.
    new: bool | None
    example_trace_id: str | None = None
    evidence: str | None = None


class LogsResult(BaseModel):
    tool: str = "search_logs"
    service: str
    window: TimeWindow
    status: Status
    severity_counts: dict[str, int] = {}
    baseline_severity_counts: dict[str, int] = {}
    matching: int = 0
    sampled: int = 0
    patterns: list[LogPattern] = []
    note: str | None = None


def group_patterns(records: list[LogRecord], known: set[str] | None) -> list[LogPattern]:
    """Group records into patterns. `known` = patterns seen before the window (None: unknown)."""
    groups: dict[tuple[str, str], list[LogRecord]] = defaultdict(list)
    for r in records:
        groups[(pattern_of(r.body), r.severity_text or str(r.severity_number))].append(r)
    patterns = [
        LogPattern(
            pattern=p, severity=sev, count=len(rs),
            first_seen=min(r.timestamp for r in rs), last_seen=max(r.timestamp for r in rs),
            new=None if known is None else p not in known,
            example_trace_id=next((r.trace_id for r in rs if r.trace_id), None),
        )
        for (p, sev), rs in groups.items()
    ]
    # Most severe first, then most frequent: one ERROR line matters more than 400 INFO lines.
    severity_rank = {r.severity_text: r.severity_number for r in records}
    return sorted(patterns, key=lambda p: (severity_rank.get(p.severity, 0), p.count), reverse=True)


def search_logs(ctx: ToolContext, service: str, window: TimeWindow,
                min_severity: str | None = None, text: str | None = None) -> LogsResult:
    if min_severity is not None and min_severity not in SEVERITY_MIN:
        raise ValueError(f"min_severity must be one of {', '.join(SEVERITY_MIN)}")
    logs = ctx.backends.logs
    if logs is None:
        return LogsResult(service=service, window=window, status=Status.ERROR,
                          note="no logs backend configured for this system")
    inputs = {"service": service, "window": window.model_dump(mode="json"),
              "min_severity": min_severity, "text": text}
    baseline = TimeWindow(start=window.start - BASELINE, end=window.start)
    try:
        counts = logs.counts_by_severity(service, window.start, window.end)
        base_counts = logs.counts_by_severity(service, baseline.start, baseline.end)
        matching, records = logs.sample(service, window.start, window.end, SAMPLE_SIZE, min_severity, text)
        _, base_records = logs.sample(service, baseline.start, baseline.end, SAMPLE_SIZE, min_severity, text)
    except BackendError as e:
        return LogsResult(service=service, window=window, status=Status.ERROR, note=str(e))
    if not records:
        return LogsResult(service=service, window=window, status=Status.NO_DATA,
                          severity_counts=counts, baseline_severity_counts=base_counts,
                          note="no matching logs; logs may be incomplete, so absence is not evidence")

    known = {pattern_of(r.body) for r in base_records} if base_records else None
    patterns = group_patterns(records, known)[:TOP]
    link = ctx.links.logs(logs.query_text(service, min_severity, text), baseline.start, window.end)
    refs = ctx.save([
        EvidenceDraft("search_logs", inputs,
                      f"{service} log pattern ({p.severity}, {p.count}x{', new' if p.new else ''}): {p.pattern[:120]}",
                      link, p.model_dump(mode="json"))
        for p in patterns
    ])
    for p, ref in zip(patterns, refs):
        p.evidence = ref
    notes = []
    if matching > len(records):
        notes.append(f"patterns from the newest {len(records)} of {matching} matching logs")
    if known is None:
        notes.append("no logs in the hour before the window, so new/old patterns are unknown")
    return LogsResult(
        service=service, window=window, status=Status.OK, severity_counts=counts,
        baseline_severity_counts=base_counts, matching=matching, sampled=len(records), patterns=patterns,
        note="; ".join(notes) or None,
    )
