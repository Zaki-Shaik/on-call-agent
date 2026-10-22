"""FastAPI app. Phase 0: health endpoints only."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
import redis
from fastapi import FastAPI, Response

from oncall.config import Settings
from oncall.profile import load_profile


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings()
    app.state.settings = settings
    # Fail at startup, not on the first alert, if the profile is broken.
    app.state.profile = load_profile(settings.profile_path)
    yield


app = FastAPI(title="oncall", lifespan=lifespan)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness: the process is up and the profile loaded."""
    return {"status": "ok", "profile": app.state.profile.name}


@app.get("/readyz")
def readyz(response: Response) -> dict[str, str]:
    """Readiness: our Postgres and Redis are reachable."""
    settings: Settings = app.state.settings
    checks: dict[str, str] = {}
    try:
        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except psycopg.Error as e:
        checks["postgres"] = f"error: {e}"
    try:
        redis.Redis.from_url(settings.redis_url, socket_timeout=2).ping()
        checks["redis"] = "ok"
    except redis.RedisError as e:
        checks["redis"] = f"error: {e}"
    if any(v != "ok" for v in checks.values()):
        response.status_code = 503
    return checks
