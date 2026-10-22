"""FastAPI app: health endpoints and alert webhooks."""

import hmac
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import psycopg
import redis
from fastapi import Body, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from oncall.alerts.sources import SOURCES
from oncall.config import Settings
from oncall.db import create_pool, migrate
from oncall.ingest import Ingestor
from oncall.jobs import JobQueue
from oncall.profile import load_profile

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = app.state.settings = Settings()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # Fail at startup, not on the first alert, if the profile is broken.
    profile = app.state.profile = load_profile(settings.profile_path)
    migrate(settings.database_url)
    pool = app.state.pool = create_pool(settings.database_url)
    app.state.redis = redis.Redis.from_url(settings.redis_url, socket_timeout=2)
    app.state.ingestor = Ingestor(
        pool,
        JobQueue(app.state.redis),
        profile.name,
        group_window=timedelta(minutes=settings.group_window_minutes),
    )
    yield
    pool.close()
    app.state.redis.close()


app = FastAPI(title="oncall", lifespan=lifespan)


@app.exception_handler(psycopg.OperationalError)
def database_unavailable(request: Request, exc: psycopg.OperationalError) -> JSONResponse:
    # 503 makes Alertmanager retry the delivery; re-sends are deduplicated.
    log.error("database unavailable: %s", exc)
    return JSONResponse({"detail": "database unavailable"}, status_code=503)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness: the process is up and the profile loaded."""
    return {"status": "ok", "profile": app.state.profile.name}


@app.get("/readyz")
def readyz(response: Response) -> dict[str, str]:
    """Readiness: our Postgres and Redis are reachable."""
    checks: dict[str, str] = {}
    try:
        with app.state.pool.connection(timeout=2) as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except psycopg.Error as e:  # includes the pool's PoolTimeout
        checks["postgres"] = f"error: {e}"
    try:
        app.state.redis.ping()
        checks["redis"] = "ok"
    except redis.RedisError as e:
        checks["redis"] = f"error: {e}"
    if any(v != "ok" for v in checks.values()):
        response.status_code = 503
    return checks


def _check_token(authorization: str | None) -> None:
    expected = app.state.settings.webhook_token
    if not expected:
        return
    supplied = (authorization or "").removeprefix("Bearer ").strip()
    if not hmac.compare_digest(supplied.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="invalid webhook token")


@app.post("/webhooks/{source_name}")
def receive_webhook(
    source_name: str,
    body: dict[str, Any] = Body(...),
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Accept an alert webhook, store it, and return immediately.

    The investigation runs in the worker; this only dedupes, groups and enqueues.
    """
    source = SOURCES.get(source_name)
    if source is None:
        raise HTTPException(status_code=404, detail=f"unknown alert source {source_name!r}")
    _check_token(authorization)
    log.debug("%s webhook body: %s", source_name, json.dumps(body))
    try:
        alerts = source.parse(body, app.state.profile.alerts)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors(include_url=False)) from e

    results = app.state.ingestor.ingest(alerts)
    for r in results:
        log.info("%s alert %s: %s (incident %s)", source_name, r.fingerprint, r.outcome, r.incident_id)
    return {
        "received": len(results),
        "results": [
            {"fingerprint": r.fingerprint, "outcome": r.outcome,
             "incident_id": str(r.incident_id) if r.incident_id else None}
            for r in results
        ],
    }
