"""Alert ingestion: dedupe, create or attach to an incident, enqueue an investigation.

Each alert is handled in its own transaction. The job is enqueued only after the incident
is committed; if Redis fails at that point the incident stays 'queued' and the sweeper
(Phase 2) re-enqueues it.
"""

import logging
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from uuid import UUID

import psycopg
import redis
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from oncall.alerts.models import Alert, AlertStatus
from oncall.jobs import JobQueue

log = logging.getLogger(__name__)

OPEN_STATUSES = ["queued", "investigating", "awaiting_approval"]


class Outcome(StrEnum):
    CREATED = "created"  # new incident, job enqueued
    ATTACHED = "attached"  # joined an existing open incident
    DUPLICATE = "duplicate"  # already seen; nothing changed
    RESOLVED = "resolved"  # alert marked resolved; never starts an investigation
    IGNORED = "ignored"  # stored, but no service label to route on


@dataclass(frozen=True)
class IngestResult:
    fingerprint: str
    outcome: Outcome
    incident_id: UUID | None


class Ingestor:
    def __init__(
        self,
        pool: ConnectionPool,
        queue: JobQueue,
        profile_name: str,
        group_window: timedelta = timedelta(minutes=10),
    ) -> None:
        self._pool = pool
        self._queue = queue
        self._profile = profile_name
        self._window = group_window

    def ingest(self, alerts: list[Alert]) -> list[IngestResult]:
        results = []
        for alert in alerts:
            if alert.status is AlertStatus.FIRING:
                result = self._ingest_firing(alert)
            else:
                result = self._ingest_resolved(alert)
            if result.outcome is Outcome.CREATED:
                self._enqueue(result.incident_id)
            results.append(result)
        return results

    def _ingest_firing(self, alert: Alert) -> IngestResult:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute(
                """
                INSERT INTO alerts (source, fingerprint, starts_at, status, service, severity,
                                    summary, raw_payload)
                VALUES (%s, %s, %s, 'firing', %s, %s, %s, %s)
                ON CONFLICT (source, fingerprint, starts_at) DO NOTHING
                RETURNING id
                """,
                (alert.source, alert.fingerprint, alert.starts_at, alert.service,
                 alert.severity, alert.summary, Jsonb(alert.raw_payload)),
            ).fetchone()
            if row is None:
                return IngestResult(alert.fingerprint, Outcome.DUPLICATE, self._incident_of(conn, alert))
            if alert.service is None:
                log.warning("alert %s from %s has no service label; stored, not routed",
                            alert.fingerprint, alert.source)
                return IngestResult(alert.fingerprint, Outcome.IGNORED, None)

            # Grouping rule (interim, until the service map exists): join any open incident of
            # this system opened within the window, whatever its service, so one failure that
            # cascades across services is one incident. Serialized per system so two concurrent
            # alerts can't both open an incident.
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                         (f"incident:{self._profile}",))
            incident = conn.execute(
                """
                SELECT id FROM incidents
                WHERE system_profile = %s AND status = ANY(%s)
                  AND opened_at BETWEEN %s AND %s
                ORDER BY opened_at DESC LIMIT 1
                """,
                (self._profile, OPEN_STATUSES,
                 alert.starts_at - self._window, alert.starts_at + self._window),
            ).fetchone()
            if incident is not None:
                outcome, incident_id = Outcome.ATTACHED, incident["id"]
            else:
                incident_id = conn.execute(
                    """
                    INSERT INTO incidents (status, service, system_profile, opened_at)
                    VALUES ('queued', %s, %s, %s) RETURNING id
                    """,
                    (alert.service, self._profile, alert.starts_at),
                ).fetchone()["id"]
                outcome = Outcome.CREATED
            conn.execute("UPDATE alerts SET incident_id = %s WHERE id = %s", (incident_id, row["id"]))
            return IngestResult(alert.fingerprint, outcome, incident_id)

    def _ingest_resolved(self, alert: Alert) -> IngestResult:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute(
                """
                UPDATE alerts
                SET status = 'resolved', ends_at = %s, resolved_payload = %s, updated_at = now()
                WHERE source = %s AND fingerprint = %s AND starts_at = %s AND status = 'firing'
                RETURNING incident_id
                """,
                (alert.ends_at, Jsonb(alert.raw_payload), alert.source, alert.fingerprint,
                 alert.starts_at),
            ).fetchone()
            if row is not None:
                return IngestResult(alert.fingerprint, Outcome.RESOLVED, row["incident_id"])

            # Resolved without a firing record (e.g. we were down when it fired): store it for
            # the record, attached to nothing. Re-sends of an already resolved alert are duplicates.
            inserted = conn.execute(
                """
                INSERT INTO alerts (source, fingerprint, starts_at, ends_at, status, service,
                                    severity, summary, raw_payload, resolved_payload)
                VALUES (%s, %s, %s, %s, 'resolved', %s, %s, %s, %s, %s)
                ON CONFLICT (source, fingerprint, starts_at) DO NOTHING
                RETURNING id
                """,
                (alert.source, alert.fingerprint, alert.starts_at, alert.ends_at, alert.service,
                 alert.severity, alert.summary, Jsonb(alert.raw_payload), Jsonb(alert.raw_payload)),
            ).fetchone()
            if inserted is None:
                return IngestResult(alert.fingerprint, Outcome.DUPLICATE, self._incident_of(conn, alert))
            return IngestResult(alert.fingerprint, Outcome.RESOLVED, None)

    @staticmethod
    def _incident_of(conn: psycopg.Connection, alert: Alert) -> UUID | None:
        row = conn.execute(
            "SELECT incident_id FROM alerts WHERE source = %s AND fingerprint = %s AND starts_at = %s",
            (alert.source, alert.fingerprint, alert.starts_at),
        ).fetchone()
        return row["incident_id"] if row else None

    def _enqueue(self, incident_id: UUID | None) -> None:
        try:
            self._queue.enqueue(incident_id)
        except redis.RedisError:
            # The incident is committed as 'queued'; the sweeper will re-enqueue it.
            log.exception("failed to enqueue incident %s; left for the sweeper", incident_id)
