"""Worker process: consumes investigation jobs; one worker also runs the sweeper and change recorder.

Phase 2 runs a placeholder investigation; the LangGraph investigation replaces it in Phase 4.
"""

import logging
import os
import signal
import socket
import threading
from datetime import datetime, timedelta, timezone
from uuid import UUID

import redis

from oncall.changes import ChangeRecorder
from oncall.config import Settings
from oncall.db import create_pool, migrate
from oncall.jobs import JobQueue
from oncall.profile import load_profile
from oncall.telemetry.factory import build_backends
from oncall.worker.background import LeaderTasks, Task, sweep_stuck_incidents
from oncall.worker.consumer import Consumer

log = logging.getLogger("oncall.worker")


def placeholder_investigation(incident_id: UUID) -> None:
    log.info("incident %s: no investigation yet (the agent graph arrives in Phase 4)", incident_id)


def main() -> None:
    settings = Settings()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per backend request is noise
    profile = load_profile(settings.profile_path)
    migrate(settings.database_url)
    pool = create_pool(settings.database_url)
    client = redis.Redis.from_url(settings.redis_url, socket_timeout=10)
    backends = build_backends(profile)
    recorder = ChangeRecorder(pool, profile.name, backends.metrics, backends.flags, profile.links.flags)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    now = lambda: datetime.now(timezone.utc)  # noqa: E731
    LeaderTasks(settings.database_url, [
        Task("sweeper", 30, lambda: sweep_stuck_incidents(pool, JobQueue(client), timedelta(minutes=2))),
        Task("flag changes", 5, lambda: recorder.poll_flags(now())),
        Task("deploys", 60, lambda: recorder.poll_versions(now())),
    ], stop).start()

    consumer = Consumer(client, pool, placeholder_investigation, name=f"{socket.gethostname()}-{os.getpid()}")
    log.info("worker %s started for profile %s", consumer.name, profile.name)
    consumer.run(stop)
    pool.close()
    log.info("worker stopped")


if __name__ == "__main__":
    main()
