"""Investigation job consumer: Redis Stream consumer group plus a lease on the incident row.

Delivery guarantees:
- A job is acked only after the handler finishes. If the worker dies first, the entry stays
  pending and is reclaimed (XAUTOCLAIM) once it has been idle longer than the lease.
- The lease on the incident makes the handler run at most once at a time per incident, even
  when the same incident is enqueued twice (the sweeper re-enqueues stuck incidents).
"""

import logging
import threading
from collections.abc import Callable
from datetime import timedelta
from uuid import UUID

import redis
from psycopg_pool import ConnectionPool

from oncall.jobs import INVESTIGATIONS_STREAM

log = logging.getLogger(__name__)

GROUP = "oncall-workers"


class Consumer:
    def __init__(
        self,
        client: redis.Redis,
        pool: ConnectionPool,
        handler: Callable[[UUID], None],
        name: str,
        lease: timedelta = timedelta(minutes=5),
        max_deliveries: int = 3,
        stream: str = INVESTIGATIONS_STREAM,
    ) -> None:
        self._redis = client
        self._pool = pool
        self._handler = handler
        self.name = name
        self._lease = lease
        self._max_deliveries = max_deliveries
        self._stream = stream

    def ensure_group(self) -> None:
        try:
            # From the start of the stream, so jobs enqueued before the first worker are not lost.
            self._redis.xgroup_create(self._stream, GROUP, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def run(self, stop: threading.Event, block_ms: int = 5000) -> None:
        self.ensure_group()
        while not stop.is_set():
            self.poll_once(block_ms)

    def poll_once(self, block_ms: int = 5000) -> bool:
        """Process at most one job. Returns whether there was one."""
        entry = self._reclaim() or self._read(block_ms)
        if entry is None:
            return False
        self._process(*entry)
        return True

    def _reclaim(self) -> tuple[str, dict] | None:
        _, entries, _ = self._redis.xautoclaim(
            self._stream, GROUP, self.name, min_idle_time=int(self._lease.total_seconds() * 1000),
            start_id="0-0", count=1,
        )
        if entries:
            log.warning("reclaimed job %s from a worker that did not finish it", entries[0][0])
            return entries[0]
        return None

    def _read(self, block_ms: int) -> tuple[str, dict] | None:
        response = self._redis.xreadgroup(GROUP, self.name, {self._stream: ">"}, count=1, block=block_ms)
        if not response:
            return None
        _, entries = response[0]
        return entries[0] if entries else None

    def _process(self, entry_id: bytes | str, fields: dict) -> None:
        fields = {_text(k): _text(v) for k, v in fields.items()}
        incident_id = UUID(fields["incident_id"])
        if self._deliveries(entry_id) > self._max_deliveries:
            log.error("job %s for incident %s failed %d times; dropping it", _text(entry_id),
                      incident_id, self._max_deliveries)
            self._redis.xack(self._stream, GROUP, entry_id)
            return
        if not self._claim(incident_id):
            log.info("incident %s already handled or leased elsewhere; skipping duplicate job", incident_id)
            self._redis.xack(self._stream, GROUP, entry_id)
            return
        try:
            self._handler(incident_id)
        except Exception:
            # Leave the job unacked: it is retried once the lease expires.
            log.exception("investigation of incident %s failed", incident_id)
            return
        self._release(incident_id)
        self._redis.xack(self._stream, GROUP, entry_id)

    def _deliveries(self, entry_id: bytes | str) -> int:
        pending = self._redis.xpending_range(self._stream, GROUP, min=entry_id, max=entry_id, count=1)
        return pending[0]["times_delivered"] if pending else 1

    def _claim(self, incident_id: UUID) -> bool:
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE incidents
                SET status = 'investigating', lease_owner = %s,
                    lease_expires_at = now() + %s, updated_at = now()
                WHERE id = %s
                  AND (status = 'queued'
                       -- an expired lease means the worker that held it died; a released
                       -- (NULL) lease means the investigation finished and must not rerun
                       OR (status = 'investigating' AND lease_expires_at < now()))
                RETURNING id
                """,
                (self.name, self._lease, incident_id),
            ).fetchone()
        return row is not None

    def _release(self, incident_id: UUID) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                "UPDATE incidents SET lease_owner = NULL, lease_expires_at = NULL, updated_at = now()"
                " WHERE id = %s AND lease_owner = %s",
                (incident_id, self.name),
            )


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value
