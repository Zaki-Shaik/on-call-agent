"""Building, storing and sampling service maps.

The healthy map is rebuilt about hourly by one worker, and on demand when an investigation
finds it stale. It samples traces spread over the window, leaving out periods when an incident
was open. The incident view samples the same way, over the incident window, for the services
an investigation asks about.
"""

import logging
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from oncall.servicemap.model import EdgeType, Observations, ServiceMap, SpanRef, extract
from oncall.telemetry.models import BackendError, Span, TimeWindow, Trace
from oncall.telemetry.traces import TraceBackend

log = logging.getLogger(__name__)

HEALTHY_SLICES = 8  # sample points spread over the healthy window
HEALTHY_TRACES_PER_QUERY = 10
INCIDENT_SLICES = 3
INCIDENT_TRACES_PER_QUERY = 20
INCIDENT_ERROR_TRACES = 20
BEFORE = timedelta(hours=1)  # latency reference for the incident view
MIN_SLICE = timedelta(minutes=5)
MAX_LINKED_TRACES = 20  # producer traces fetched to measure consumer lag
PARALLEL_QUERIES = 4
KEEP_SNAPSHOTS = 48
# Excluded around each incident: failures start before the alert fires, and recovery lags.
INCIDENT_LEAD = timedelta(minutes=30)
INCIDENT_TAIL = timedelta(minutes=10)
# An incident whose resolve notification was lost would otherwise exclude everything after it.
MAX_OPEN_EXCLUSION = timedelta(hours=6)


class Snapshot(BaseModel):
    id: int
    built_at: datetime
    window: TimeWindow
    excluded: list[TimeWindow]
    failed_queries: int
    map: ServiceMap
    # Set when this is older than requested because rebuilding failed.
    stale: bool = False


@dataclass
class IncidentView:
    observations: Observations
    queried: set[str]  # services whose traces were sampled successfully
    before: Observations | None = None  # the hour before the window
    failed: set[str] = field(default_factory=set)
    error_examples: dict[tuple[str, str, EdgeType], str] = field(default_factory=dict)


def _subtract(part: tuple[datetime, datetime], cut: TimeWindow) -> list[tuple[datetime, datetime]]:
    a, b = part
    if cut.end <= a or cut.start >= b:
        return [part]
    return [p for p in ((a, cut.start), (cut.end, b)) if p[1] > p[0]]


def sample_windows(window: TimeWindow, excluded: list[TimeWindow], slices: int,
                   min_length: timedelta = MIN_SLICE) -> list[TimeWindow]:
    """Split `window` into equal slices; in each, the longest stretch outside `excluded`."""
    step = (window.end - window.start) / slices
    out = []
    for i in range(slices):
        parts = [(window.start + step * i, window.start + step * (i + 1))]
        for cut in excluded:
            parts = [p for part in parts for p in _subtract(part, cut)]
        parts = [p for p in parts if p[1] - p[0] >= min(min_length, step)]
        if parts:
            start, end = max(parts, key=lambda p: p[1] - p[0])
            out.append(TimeWindow(start=start, end=end))
    return out


class ServiceMaps:
    def __init__(self, pool: ConnectionPool, profile_name: str, traces: TraceBackend,
                 window: timedelta = timedelta(hours=24), refresh: timedelta = timedelta(hours=1),
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        self._pool = pool
        self._profile = profile_name
        self._traces = traces
        self._window = window
        self._refresh = refresh
        self._clock = clock

    # Healthy map

    def latest(self) -> Snapshot | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT * FROM service_map_snapshots WHERE system_profile = %s ORDER BY built_at DESC LIMIT 1",
                (self._profile,),
            ).fetchone()
        if row is None:
            return None
        return Snapshot(id=row["id"], built_at=row["built_at"],
                        window=TimeWindow(start=row["window_start"], end=row["window_end"]),
                        excluded=row["excluded"], failed_queries=row["failed_queries"], map=row["map"])

    def healthy(self) -> Snapshot:
        """The healthy map for an investigation: rebuilt first if older than two refresh periods.

        If rebuilding fails, an older map is still better than none; it comes back marked stale.
        Raises BackendError only when there is no map at all.
        """
        return self.refresh_if_older_than(2 * self._refresh, fall_back_to_stale=True)

    def refresh_if_older_than(self, age: timedelta, fall_back_to_stale: bool = False) -> Snapshot:
        latest = self.latest()
        if latest and self._clock() - latest.built_at < age:
            return latest
        with self._pool.connection() as conn:
            conn.autocommit = True  # the lock is held for the whole build; no long open transaction
            try:
                # One build at a time per system; whoever waited reuses the fresh result.
                conn.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (f"service-map:{self._profile}",))
                latest = self.latest()
                if latest and self._clock() - latest.built_at < age:
                    return latest
                try:
                    return self.build_healthy()
                except BackendError:
                    if fall_back_to_stale and latest:
                        log.exception("service map rebuild failed; using the map from %s", latest.built_at)
                        return latest.model_copy(update={"stale": True})
                    raise
            finally:
                conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (f"service-map:{self._profile}",))
                conn.autocommit = False

    def build_healthy(self) -> Snapshot:
        now = self._clock()
        window = TimeWindow(start=now - self._window, end=now)
        excluded = self.incident_periods(window)
        services = self._traces.services()
        queries = [(svc, w) for w in sample_windows(window, excluded, HEALTHY_SLICES) for svc in services]
        traces, failed = self._sample(queries, HEALTHY_TRACES_PER_QUERY)
        if queries and len(failed) == len(queries):
            raise BackendError("traces: every query for the service map failed")
        observations = extract(traces.values(), set(services), linked=self._linked(traces))
        service_map = observations.to_map()
        with self._pool.connection() as conn:
            row = conn.execute(
                "INSERT INTO service_map_snapshots"
                " (system_profile, built_at, window_start, window_end, excluded, traces_sampled, failed_queries, map)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (self._profile, now, window.start, window.end,
                 Jsonb([w.model_dump(mode="json") for w in excluded]), service_map.traces_sampled,
                 len(failed), Jsonb(service_map.model_dump(mode="json"))),
            ).fetchone()
            conn.execute(
                "DELETE FROM service_map_snapshots WHERE system_profile = %s AND id NOT IN"
                " (SELECT id FROM service_map_snapshots WHERE system_profile = %s ORDER BY built_at DESC LIMIT %s)",
                (self._profile, self._profile, KEEP_SNAPSHOTS),
            )
        log.info("service map built: %d services, %d edges from %d traces (%d failed queries, %d periods excluded)",
                 len(service_map.services()), len(service_map.edges), service_map.traces_sampled,
                 len(failed), len(excluded))
        return Snapshot(id=row["id"], built_at=now, window=window, excluded=excluded,
                        failed_queries=len(failed), map=service_map)

    def incident_periods(self, window: TimeWindow) -> list[TimeWindow]:
        """Periods with an open incident, padded, clipped to `window`. Never part of a healthy map."""
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT i.opened_at, i.status, i.updated_at,"
                "       bool_and(a.status = 'resolved') AS all_resolved, max(a.ends_at) AS last_end"
                " FROM incidents i LEFT JOIN alerts a ON a.incident_id = i.id"
                " WHERE i.system_profile = %s AND i.opened_at < %s"
                " GROUP BY i.id",
                (self._profile, window.end + INCIDENT_LEAD),
            ).fetchall()
        periods = []
        for r in rows:
            if r["all_resolved"] and r["last_end"]:
                end = r["last_end"]
            elif r["status"] in ("resolved", "closed"):
                end = r["updated_at"]
            else:
                end = min(window.end, r["opened_at"] + MAX_OPEN_EXCLUSION)  # still open
            start, end = max(r["opened_at"] - INCIDENT_LEAD, window.start), min(end + INCIDENT_TAIL, window.end)
            if end > start:
                periods.append(TimeWindow(start=start, end=end))
        return sorted(periods, key=lambda w: w.start)

    # Incident view

    def incident_view(self, services: Iterable[str], window: TimeWindow, healthy: ServiceMap) -> IncidentView:
        services = sorted(set(services))
        slices = sample_windows(window, [], INCIDENT_SLICES)
        traces, failed = self._sample([(s, w) for w in slices for s in services], INCIDENT_TRACES_PER_QUERY)
        # Error traces only provide example links: they are a biased sample, so they never count.
        error_traces, _ = self._sample([(s, window) for s in services], INCIDENT_ERROR_TRACES, errors_only=True)
        if services and set(failed) == set(services):
            raise BackendError("traces: every query for the incident view failed")
        known, operations = healthy.services() | set(services), healthy.known_operations()
        observations = extract(traces.values(), known, operations, self._linked(traces))
        before_window = TimeWindow(start=window.start - BEFORE, end=window.start)
        before_traces, _ = self._sample([(s, w) for w in sample_windows(before_window, [], INCIDENT_SLICES)
                                         for s in services], INCIDENT_TRACES_PER_QUERY)
        before = extract(before_traces.values(), known, operations, self._linked(before_traces))
        examples = {k: s.example_error_trace for k, s in extract(error_traces.values(), known, operations).edges.items()
                    if s.example_error_trace}
        for k, s in observations.edges.items():
            if s.example_error_trace:
                examples.setdefault(k, s.example_error_trace)
        return IncidentView(observations, set(services) - set(failed), before, set(failed), examples)

    # Sampling

    def _sample(self, queries: list[tuple[str, TimeWindow]], limit: int,
                errors_only: bool = False) -> tuple[dict[str, Trace], list[str]]:
        """Run trace searches in parallel. Returns traces by id and the service of each failed query."""
        traces: dict[str, Trace] = {}
        failed: list[str] = []

        def run(query: tuple[str, TimeWindow]) -> list[Trace]:
            service, w = query
            return self._traces.find_traces(service, w.start, w.end, limit, errors_only=errors_only)

        with ThreadPoolExecutor(PARALLEL_QUERIES) as executor:
            futures = [(q, executor.submit(run, q)) for q in queries]
            for (service, _), future in futures:
                try:
                    for t in future.result():
                        traces.setdefault(t.trace_id, t)
                except BackendError as e:
                    log.warning("trace query for %s failed: %s", service, e)
                    failed.append(service)
        return traces, failed

    def _linked(self, traces: dict[str, Trace]) -> dict[SpanRef, Span]:
        """Producer spans that sampled consumer spans link to, from traces outside the sample."""
        refs = {ref for t in traces.values() for s in t.spans if s.kind == "consumer" for ref in s.links}
        missing = sorted({trace_id for trace_id, _ in refs if trace_id not in traces})[:MAX_LINKED_TRACES]

        def fetch(trace_id: str) -> Trace | None:
            try:
                return self._traces.get_trace(trace_id)
            except BackendError:
                return None  # lag is a bonus; the edge itself comes from the topic

        with ThreadPoolExecutor(PARALLEL_QUERIES) as executor:
            fetched = [t for t in executor.map(fetch, missing) if t]
        return {(s.trace_id, s.span_id): s for t in fetched for s in t.spans if (s.trace_id, s.span_id) in refs}
