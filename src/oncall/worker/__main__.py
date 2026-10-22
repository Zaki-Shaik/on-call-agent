"""Worker process. Phase 0: loads the profile, checks Redis, and idles until stopped.

Phase 2 replaces the idle loop with the Redis Stream consumer.
"""

import logging
import signal
import threading

import redis

from oncall.config import Settings
from oncall.profile import load_profile

log = logging.getLogger("oncall.worker")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings()
    profile = load_profile(settings.profile_path)
    redis.Redis.from_url(settings.redis_url, socket_timeout=5).ping()
    log.info("worker started for profile %s", profile.name)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    while not stop.wait(timeout=60):
        log.info("idle (no consumer yet)")
    log.info("worker stopped")


if __name__ == "__main__":
    main()
