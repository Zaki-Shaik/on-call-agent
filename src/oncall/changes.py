"""Change recorder: turns observed state into change events (flag changes, deploys).

Neither source keeps history, so we poll, diff against the last observed state (stored in
observed_state), and record each difference with the time we observed it. Timestamps are
accurate to the poll interval. The first poll only records a baseline.
"""

import logging
from datetime import datetime, timedelta
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from oncall.flags.provider import FlagProvider
from oncall.telemetry.metrics import MetricsBackend

log = logging.getLogger(__name__)

VERSION_LOOKBACK = timedelta(minutes=5)


class ChangeRecorder:
    def __init__(self, pool: ConnectionPool, profile_name: str, metrics: MetricsBackend,
                 flags: FlagProvider | None, flags_link: str | None) -> None:
        self._pool = pool
        self._profile = profile_name
        self._metrics = metrics
        self._flags = flags
        self._flags_link = flags_link

    def poll_flags(self, now: datetime) -> int:
        """Record flag changes since the last poll. Returns the number of events."""
        if self._flags is None:
            return 0
        states = self._flags.read_flags()
        current = {name: s.as_json() for name, s in states.items()}
        with self._pool.connection() as conn, conn.transaction():
            previous = self._observed(conn, "flag")
            events = 0
            if previous:  # otherwise this is the baseline poll
                for name in sorted(set(previous) | set(current)):
                    old, new = previous.get(name), current.get(name)
                    if old == new:
                        continue
                    description = f"flag {name}: {_flag_text(old)} -> {_flag_text(new)}"
                    if name in states and states[name].description:
                        description += f" ({states[name].description})"
                    self._record(conn, now, "flag_change", None, description, self._flags_link,
                                 {"flag": name, "old": old, "new": new})
                    events += 1
            self._replace_observed(conn, "flag", current, now)
        return events

    def poll_versions(self, now: datetime) -> int:
        """Record a deploy when a service reports a version it did not report before."""
        seen = self._metrics.service_versions(now, int(VERSION_LOOKBACK.total_seconds()))
        with self._pool.connection() as conn, conn.transaction():
            previous = self._observed(conn, "version")
            events = 0
            for service, versions in seen.items():
                old = set(previous.get(service, []))
                # New services are stored silently: at startup services appear one by one.
                if old and versions - old:
                    for version in sorted(versions - old):
                        self._record(conn, now, "deploy", service,
                                     f"{service} version {', '.join(sorted(old))} -> {version}",
                                     None, {"service": service, "old": sorted(old), "new": version})
                        events += 1
            # Keep services that went quiet: a silent service has not been undeployed.
            merged = {**previous, **{s: sorted(v) for s, v in seen.items()}}
            self._replace_observed(conn, "version", merged, now)
        return events

    def _observed(self, conn, kind: str) -> dict[str, Any]:
        rows = conn.execute(
            "SELECT key, value FROM observed_state WHERE system_profile = %s AND kind = %s",
            (self._profile, kind),
        ).fetchall()
        return {r["key"]: r["value"] for r in rows}

    def _replace_observed(self, conn, kind: str, values: dict[str, Any], now: datetime) -> None:
        conn.execute("DELETE FROM observed_state WHERE system_profile = %s AND kind = %s",
                     (self._profile, kind))
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO observed_state (system_profile, kind, key, value, observed_at)"
                " VALUES (%s, %s, %s, %s, %s)",
                [(self._profile, kind, k, Jsonb(v), now) for k, v in values.items()],
            )

    def _record(self, conn, now: datetime, type_: str, service: str | None, description: str,
                link: str | None, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO change_events (system_profile, occurred_at, type, service, actor, description, link, details)"
            " VALUES (%s, %s, %s, %s, 'unknown', %s, %s, %s)",
            (self._profile, now, type_, service, description, link, Jsonb(details)),
        )
        log.info("recorded %s: %s", type_, description)


def _flag_text(state: dict[str, Any] | None) -> str:
    if state is None:
        return "(absent)"
    targeted = state.get("targeted_variants") or []
    return state["default_variant"] + (f" (targeting: {', '.join(targeted)})" if targeted else "")
