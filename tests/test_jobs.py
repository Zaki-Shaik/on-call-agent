from uuid import uuid4

from oncall.jobs import JobQueue


def test_enqueue_appends_incident_id_to_stream(redis_client) -> None:
    queue = JobQueue(redis_client, stream="test:jobs")
    incident_id = uuid4()

    entry_id = queue.enqueue(incident_id)

    [(stored_id, fields)] = redis_client.xrange("test:jobs")
    assert stored_id.decode() == entry_id
    assert fields == {b"incident_id": str(incident_id).encode()}
