"""Worker process: runs investigations (the LangGraph agent) from the job stream; one worker also
runs the sweeper, the change recorder and the service map rebuild.
"""

import logging
import os
import signal
import socket
import threading
from datetime import datetime, timedelta, timezone

import redis

from oncall.agent.factory import create_checkpointer, create_investigator
from oncall.changes import ChangeRecorder
from oncall.config import Settings
from oncall.db import create_pool, migrate
from oncall.jobs import JobQueue
from oncall.profile import load_profile
from oncall.servicemap.store import ServiceMaps
from oncall.telemetry.factory import build_backends
from oncall.worker.background import MAP_LEADER_LOCK_ID, LeaderTasks, Task, sweep_stuck_incidents
from oncall.worker.consumer import Consumer

log = logging.getLogger("oncall.worker")


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
    refresh = timedelta(minutes=settings.map_refresh_minutes)
    maps = ServiceMaps(pool, profile.name, backends.traces, timedelta(hours=settings.map_window_hours), refresh)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    now = lambda: datetime.now(timezone.utc)  # noqa: E731
    LeaderTasks(settings.database_url, [
        Task("sweeper", 30, lambda: sweep_stuck_incidents(pool, JobQueue(client), timedelta(minutes=2))),
        Task("flag changes", 5, lambda: recorder.poll_flags(now())),
        Task("deploys", 60, lambda: recorder.poll_versions(now())),
    ], stop).start()
    # Checked every 5 minutes, rebuilt when an hour old: a restarted worker doesn't rebuild a fresh map.
    LeaderTasks(settings.database_url, [
        Task("service map", 300, lambda: maps.refresh_if_older_than(refresh)),
    ], stop, MAP_LEADER_LOCK_ID).start()

    investigator = create_investigator(settings, profile, pool, client, backends, maps,
                                       create_checkpointer(settings.database_url))
    consumer = Consumer(client, pool, investigator.run, name=f"{socket.gethostname()}-{os.getpid()}")
    log.info("worker %s started for profile %s", consumer.name, profile.name)
    consumer.run(stop)
    pool.close()
    log.info("worker stopped")


if __name__ == "__main__":
    main()
