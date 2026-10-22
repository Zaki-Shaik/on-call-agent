from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
import redis
from psycopg_pool import ConnectionPool

from oncall.alerts.models import Alert, AlertStatus
from oncall.ingest import Ingestor, Outcome

T0 = datetime(2026, 10, 22, 1, 0, tzinfo=timezone.utc)


class FakeQueue:
    def __init__(self, fail: bool = False) -> None:
        self.jobs: list[UUID] = []
        self.fail = fail

    def enqueue(self, incident_id: UUID) -> str:
        if self.fail:
            raise redis.ConnectionError("redis down")
        self.jobs.append(incident_id)
        return f"{len(self.jobs)}-0"


def make_alert(
    fingerprint: str = "fp-1",
    service: str | None = "svc-a",
    starts_at: datetime = T0,
    status: AlertStatus = AlertStatus.FIRING,
    ends_at: datetime | None = None,
) -> Alert:
    return Alert(
        fingerprint=fingerprint,
        source="alertmanager",
        status=status,
        service=service,
        starts_at=starts_at,
        ends_at=ends_at,
        severity="critical",
        summary="something is wrong",
        raw_payload={"fingerprint": fingerprint, "status": status.value},
    )


def resolved(alert: Alert, after: timedelta = timedelta(minutes=5)) -> Alert:
    return alert.model_copy(update={"status": AlertStatus.RESOLVED, "ends_at": alert.starts_at + after,
                                    "raw_payload": {"status": "resolved"}})


@pytest.fixture
def queue() -> FakeQueue:
    return FakeQueue()


@pytest.fixture
def ingestor(pool: ConnectionPool, queue: FakeQueue) -> Ingestor:
    return Ingestor(pool, queue, profile_name="test-profile")


def incidents(pool: ConnectionPool) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute("SELECT * FROM incidents ORDER BY opened_at").fetchall()


def alert_rows(pool: ConnectionPool) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute("SELECT * FROM alerts ORDER BY id").fetchall()


def test_first_firing_alert_opens_incident_and_enqueues_one_job(ingestor, pool, queue) -> None:
    [result] = ingestor.ingest([make_alert()])

    assert result.outcome is Outcome.CREATED
    [incident] = incidents(pool)
    assert incident["id"] == result.incident_id
    assert incident["status"] == "queued"
    assert incident["service"] == "svc-a"
    assert incident["system_profile"] == "test-profile"
    assert incident["opened_at"] == T0
    assert queue.jobs == [result.incident_id]
    [row] = alert_rows(pool)
    assert row["incident_id"] == result.incident_id
    assert row["raw_payload"] == {"fingerprint": "fp-1", "status": "firing"}


def test_resend_is_deduplicated(ingestor, pool, queue) -> None:
    [first] = ingestor.ingest([make_alert()])
    [again] = ingestor.ingest([make_alert()])

    assert again.outcome is Outcome.DUPLICATE
    assert again.incident_id == first.incident_id
    assert len(incidents(pool)) == 1
    assert len(alert_rows(pool)) == 1
    assert len(queue.jobs) == 1


def test_related_alert_attaches_to_open_incident(ingestor, pool, queue) -> None:
    [first] = ingestor.ingest([make_alert("fp-errors")])
    [second] = ingestor.ingest([make_alert("fp-latency", starts_at=T0 + timedelta(minutes=4))])

    assert second.outcome is Outcome.ATTACHED
    assert second.incident_id == first.incident_id
    assert len(incidents(pool)) == 1
    assert len(queue.jobs) == 1


def test_grouping_window_boundary(ingestor, pool) -> None:
    [first] = ingestor.ingest([make_alert("fp-1")])
    [inside] = ingestor.ingest([make_alert("fp-2", starts_at=T0 + timedelta(minutes=10))])
    [outside] = ingestor.ingest([make_alert("fp-3", starts_at=T0 + timedelta(minutes=10, seconds=1))])

    assert inside.outcome is Outcome.ATTACHED
    assert outside.outcome is Outcome.CREATED
    assert outside.incident_id != first.incident_id


def test_cascading_alert_on_another_service_joins_the_incident(ingestor, pool, queue) -> None:
    [first] = ingestor.ingest([make_alert("fp-1", service="svc-a")])
    [other] = ingestor.ingest([make_alert("fp-2", service="svc-b", starts_at=T0 + timedelta(minutes=1))])

    assert other.outcome is Outcome.ATTACHED
    assert other.incident_id == first.incident_id
    [incident] = incidents(pool)
    assert incident["service"] == "svc-a"  # first alerting service
    assert {r["service"] for r in alert_rows(pool)} == {"svc-a", "svc-b"}
    assert len(queue.jobs) == 1


def test_another_system_is_never_grouped(ingestor, pool, queue) -> None:
    other_system = Ingestor(pool, queue, profile_name="other-profile")
    [first] = ingestor.ingest([make_alert("fp-1")])
    [second] = other_system.ingest([make_alert("fp-2")])

    assert second.outcome is Outcome.CREATED
    assert second.incident_id != first.incident_id


def test_closed_incident_is_not_reused(ingestor, pool) -> None:
    [first] = ingestor.ingest([make_alert("fp-1")])
    with pool.connection() as conn:
        conn.execute("UPDATE incidents SET status = 'closed' WHERE id = %s", (first.incident_id,))

    [second] = ingestor.ingest([make_alert("fp-2", starts_at=T0 + timedelta(minutes=1))])
    assert second.outcome is Outcome.CREATED


def test_resolved_alert_updates_status_only(ingestor, pool, queue) -> None:
    alert = make_alert()
    [created] = ingestor.ingest([alert])
    [result] = ingestor.ingest([resolved(alert)])

    assert result.outcome is Outcome.RESOLVED
    assert result.incident_id == created.incident_id
    [row] = alert_rows(pool)
    assert row["status"] == "resolved"
    assert row["ends_at"] == T0 + timedelta(minutes=5)
    assert row["resolved_payload"] == {"status": "resolved"}
    assert row["raw_payload"]["status"] == "firing"  # first payload kept unchanged
    [incident] = incidents(pool)
    assert incident["status"] == "queued"  # incident lifecycle is not driven by alert resolution
    assert len(queue.jobs) == 1


def test_resolved_resend_is_duplicate(ingestor, pool, queue) -> None:
    alert = make_alert()
    ingestor.ingest([alert])
    ingestor.ingest([resolved(alert)])
    [again] = ingestor.ingest([resolved(alert)])
    assert again.outcome is Outcome.DUPLICATE
    assert len(alert_rows(pool)) == 1


def test_resolved_without_firing_never_starts_an_investigation(ingestor, pool, queue) -> None:
    [result] = ingestor.ingest([resolved(make_alert())])

    assert result.outcome is Outcome.RESOLVED
    assert result.incident_id is None
    assert incidents(pool) == []
    assert queue.jobs == []
    [row] = alert_rows(pool)
    assert row["status"] == "resolved"


def test_refire_after_resolve_is_a_new_alert_on_the_same_incident(ingestor, pool, queue) -> None:
    first = make_alert()
    [created] = ingestor.ingest([first])
    ingestor.ingest([resolved(first, after=timedelta(minutes=2))])
    # Same fingerprint (same label set), new episode.
    [refire] = ingestor.ingest([make_alert(starts_at=T0 + timedelta(minutes=6))])

    assert refire.outcome is Outcome.ATTACHED
    assert refire.incident_id == created.incident_id
    assert len(alert_rows(pool)) == 2
    assert len(queue.jobs) == 1


def test_alert_without_service_is_stored_but_not_routed(ingestor, pool, queue) -> None:
    [result] = ingestor.ingest([make_alert(service=None)])

    assert result.outcome is Outcome.IGNORED
    assert incidents(pool) == []
    assert queue.jobs == []
    assert len(alert_rows(pool)) == 1


def test_enqueue_failure_leaves_incident_queued(pool) -> None:
    ingestor = Ingestor(pool, FakeQueue(fail=True), profile_name="test-profile")

    [result] = ingestor.ingest([make_alert()])

    assert result.outcome is Outcome.CREATED
    [incident] = incidents(pool)
    assert incident["status"] == "queued"  # the sweeper re-enqueues these


def test_concurrent_alerts_open_exactly_one_incident(ingestor, pool, queue) -> None:
    alerts = [make_alert(f"fp-{i}", service=f"svc-{i % 3}", starts_at=T0 + timedelta(seconds=i))
              for i in range(8)]

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = [r for batch in executor.map(lambda a: ingestor.ingest([a]), alerts) for r in batch]

    assert sorted(r.outcome for r in results).count(Outcome.CREATED) == 1
    assert len({r.incident_id for r in results}) == 1
    assert len(incidents(pool)) == 1
    assert len(queue.jobs) == 1
