import time
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from oncall.jobs import JobQueue
from oncall.worker.background import sweep_stuck_incidents
from oncall.worker.consumer import GROUP, Consumer

STREAM = "test:investigations"


def new_incident(pool, status: str = "queued") -> UUID:
    with pool.connection() as conn:
        return conn.execute(
            "INSERT INTO incidents (status, service, system_profile, opened_at)"
            " VALUES (%s, 'svc-a', 'test-profile', %s) RETURNING id",
            (status, datetime.now(timezone.utc)),
        ).fetchone()["id"]


def incident(pool, incident_id: UUID) -> dict:
    with pool.connection() as conn:
        return conn.execute("SELECT * FROM incidents WHERE id = %s", (incident_id,)).fetchone()


class Handler:
    def __init__(self, fail_times: int = 0) -> None:
        self.calls: list[UUID] = []
        self.fail_times = fail_times

    def __call__(self, incident_id: UUID) -> None:
        self.calls.append(incident_id)
        if len(self.calls) <= self.fail_times:
            raise RuntimeError("boom")


def make(redis_client, pool, handler, name="w1", lease=timedelta(minutes=5), **kw) -> Consumer:
    c = Consumer(redis_client, pool, handler, name=name, lease=lease, stream=STREAM, **kw)
    c.ensure_group()
    return c


def pending(redis_client) -> int:
    return redis_client.xpending(STREAM, GROUP)["pending"]


def test_job_runs_handler_then_acks_and_releases_lease(redis_client, pool) -> None:
    handler = Handler()
    consumer = make(redis_client, pool, handler)
    incident_id = new_incident(pool)
    JobQueue(redis_client, STREAM).enqueue(incident_id)

    assert consumer.poll_once(block_ms=100)

    assert handler.calls == [incident_id]
    assert pending(redis_client) == 0
    row = incident(pool, incident_id)
    assert row["status"] == "investigating"
    assert row["lease_owner"] is None and row["lease_expires_at"] is None


def test_duplicate_job_is_skipped(redis_client, pool) -> None:
    handler = Handler()
    consumer = make(redis_client, pool, handler)
    incident_id = new_incident(pool)
    queue = JobQueue(redis_client, STREAM)
    queue.enqueue(incident_id)
    queue.enqueue(incident_id)  # e.g. the sweeper re-enqueued it

    consumer.poll_once(block_ms=100)
    consumer.poll_once(block_ms=100)

    assert handler.calls == [incident_id]
    assert pending(redis_client) == 0


def test_job_of_crashed_worker_is_reclaimed_after_lease(redis_client, pool) -> None:
    incident_id = new_incident(pool)
    JobQueue(redis_client, STREAM).enqueue(incident_id)
    lease = timedelta(milliseconds=300)
    crashing = make(redis_client, pool, Handler(fail_times=1), name="w1", lease=lease)
    crashing.poll_once(block_ms=100)  # handler raises: job stays pending, lease held
    assert pending(redis_client) == 1

    survivor_handler = Handler()
    survivor = make(redis_client, pool, survivor_handler, name="w2", lease=lease)
    assert not survivor.poll_once(block_ms=50)  # lease still valid: nothing to take over
    time.sleep(0.4)
    assert survivor.poll_once(block_ms=50)

    assert survivor_handler.calls == [incident_id]
    assert pending(redis_client) == 0


def test_heartbeat_keeps_a_long_job_from_being_taken_over(redis_client, pool) -> None:
    lease = timedelta(milliseconds=300)
    incident_id = new_incident(pool)
    JobQueue(redis_client, STREAM).enqueue(incident_id)
    other = make(redis_client, pool, Handler(), name="w2", lease=lease)
    took_over = []

    def slow_investigation(_: UUID) -> None:
        time.sleep(0.6)  # twice the lease
        took_over.append(other.poll_once(block_ms=50))
        assert incident(pool, incident_id)["lease_expires_at"] > datetime.now(timezone.utc)

    make(redis_client, pool, slow_investigation, name="w1", lease=lease).poll_once(block_ms=100)

    assert took_over == [False]
    assert pending(redis_client) == 0


def test_job_failing_repeatedly_is_dropped(redis_client, pool) -> None:
    incident_id = new_incident(pool)
    JobQueue(redis_client, STREAM).enqueue(incident_id)
    handler = Handler(fail_times=99)
    consumer = make(redis_client, pool, handler, lease=timedelta(milliseconds=50), max_deliveries=2)

    for _ in range(4):
        time.sleep(0.07)
        consumer.poll_once(block_ms=50)

    assert len(handler.calls) == 2
    assert pending(redis_client) == 0


@pytest.mark.parametrize("status", ["awaiting_approval", "closed"])
def test_finished_incident_is_not_rerun(redis_client, pool, status) -> None:
    handler = Handler()
    consumer = make(redis_client, pool, handler)
    JobQueue(redis_client, STREAM).enqueue(new_incident(pool, status=status))

    consumer.poll_once(block_ms=100)

    assert handler.calls == []
    assert pending(redis_client) == 0


def test_sweeper_requeues_only_stuck_queued_incidents(redis_client, pool) -> None:
    stuck, fresh, running = new_incident(pool), new_incident(pool), new_incident(pool, "investigating")
    with pool.connection() as conn:
        conn.execute("UPDATE incidents SET updated_at = now() - interval '10 minutes' WHERE id = ANY(%s)",
                     ([stuck, running],))

    requeued = sweep_stuck_incidents(pool, JobQueue(redis_client, STREAM), timedelta(minutes=2))

    assert requeued == [stuck]
    [(_, fields)] = redis_client.xrange(STREAM)
    assert fields[b"incident_id"].decode() == str(stuck)
    assert fresh not in requeued
