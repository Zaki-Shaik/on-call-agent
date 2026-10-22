"""Background tasks that must run in exactly one worker: the sweeper, the change recorder and
the service map rebuild.

Leadership is a Postgres session advisory lock held on a dedicated connection. If the leader
dies, its connection closes, the lock is released, and another worker takes over. Each group of
tasks has its own lock and thread, so a slow map build never delays the 5-second flag poll.
"""

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

import psycopg
from psycopg_pool import ConnectionPool

from oncall.jobs import JobQueue

log = logging.getLogger(__name__)

LEADER_LOCK_ID = 7_301_002
MAP_LEADER_LOCK_ID = 7_301_003


def sweep_stuck_incidents(pool: ConnectionPool, queue: JobQueue, older_than: timedelta) -> list[UUID]:
    """Re-enqueue incidents still 'queued' after `older_than` (e.g. Redis failed right after commit)."""
    with pool.connection() as conn:
        rows = conn.execute(
            "UPDATE incidents SET updated_at = now()"
            " WHERE status = 'queued' AND updated_at < now() - %s RETURNING id",
            (older_than,),
        ).fetchall()
    ids = [r["id"] for r in rows]
    for incident_id in ids:
        queue.enqueue(incident_id)
        log.warning("re-enqueued stuck incident %s", incident_id)
    return ids


@dataclass
class Task:
    name: str
    interval_s: float
    run: Callable[[], object]
    next_due: float = 0.0


class LeaderTasks(threading.Thread):
    def __init__(self, database_url: str, tasks: list[Task], stop: threading.Event,
                 lock_id: int = LEADER_LOCK_ID) -> None:
        super().__init__(name=f"leader-tasks-{lock_id}", daemon=True)
        self._lock_id = lock_id
        self._url = database_url
        self._tasks = tasks
        self._stop = stop

    def run(self) -> None:
        conn = None
        while not self._stop.is_set():
            try:
                if conn is None or conn.closed:
                    conn = psycopg.connect(self._url, autocommit=True)
                    if not conn.execute("SELECT pg_try_advisory_lock(%s)", (self._lock_id,)).fetchone()[0]:
                        conn.close()
                        conn = None
                        self._stop.wait(15)  # another worker leads; check again later
                        continue
                    log.info("this worker now runs: %s", ", ".join(t.name for t in self._tasks))
                self._run_due()
            except psycopg.Error:
                log.exception("leader connection lost; retrying")
                conn = None
            self._stop.wait(1)
        if conn is not None:
            conn.close()

    def _run_due(self) -> None:
        now = time.monotonic()
        for task in self._tasks:
            if now < task.next_due:
                continue
            task.next_due = now + task.interval_s
            try:
                task.run()
            except Exception:
                # A failing backend must not stop the other tasks or kill the thread.
                log.exception("background task %s failed", task.name)
