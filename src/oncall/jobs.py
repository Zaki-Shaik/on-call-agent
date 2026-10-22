"""Investigation job queue on a Redis Stream (consumed by the worker with a consumer group)."""

from uuid import UUID

import redis

INVESTIGATIONS_STREAM = "oncall:investigations"


class JobQueue:
    def __init__(self, client: redis.Redis, stream: str = INVESTIGATIONS_STREAM) -> None:
        self._redis = client
        self.stream = stream

    def enqueue(self, incident_id: UUID) -> str:
        """Add an investigation job. Returns the stream entry id."""
        entry_id = self._redis.xadd(
            self.stream, {"incident_id": str(incident_id)}, maxlen=10_000, approximate=True
        )
        return entry_id.decode() if isinstance(entry_id, bytes) else entry_id
