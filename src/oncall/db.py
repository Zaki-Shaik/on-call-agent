"""Postgres connection pool and schema migrations.

Migrations are plain numbered SQL files in oncall/migrations, applied in order at startup.
Each runs in its own transaction and is recorded in schema_migrations.
"""

import logging
from importlib import resources

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)

# Arbitrary constant: serializes migration runs when the API and worker start together.
_MIGRATION_LOCK_ID = 7_301_001


def create_pool(url: str) -> ConnectionPool:
    return ConnectionPool(url, min_size=1, max_size=10, kwargs={"row_factory": dict_row}, open=True)


def migrate(url: str) -> list[str]:
    """Apply pending migrations. Returns the versions applied."""
    files = sorted(
        (f for f in resources.files("oncall.migrations").iterdir() if f.name.endswith(".sql")),
        key=lambda f: f.name,
    )
    applied = []
    # Autocommit, so each `transaction()` block below is a real transaction of its own.
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (_MIGRATION_LOCK_ID,))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        done = {r["version"] for r in conn.execute("SELECT version FROM schema_migrations")}
        for f in files:
            version = f.name.removesuffix(".sql")
            if version in done:
                continue
            with conn.transaction():
                conn.execute(f.read_text(encoding="utf-8"))
                conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
            log.info("applied migration %s", version)
            applied.append(version)
        # The advisory lock is released when the connection closes.
    return applied
