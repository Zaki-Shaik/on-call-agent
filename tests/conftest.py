"""Shared fixtures.

Tests that need Postgres or Redis read ONCALL_TEST_DATABASE_URL / ONCALL_TEST_REDIS_URL and
are skipped (with the reason shown in the summary) when those are not set. The test database
is dropped and recreated once per session; tables are truncated between tests.
"""

import os
from collections.abc import Iterator
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest
import redis
from psycopg import sql
from psycopg_pool import ConnectionPool

from oncall.db import create_pool, migrate


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("ONCALL_TEST_DATABASE_URL")
    if not url:
        pytest.skip("ONCALL_TEST_DATABASE_URL not set")
    parts = urlsplit(url)
    name = parts.path.lstrip("/")
    admin_url = urlunsplit(parts._replace(path="/postgres"))
    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    migrate(url)
    return url


@pytest.fixture
def pool(database_url: str) -> Iterator[ConnectionPool]:
    p = create_pool(database_url)
    with p.connection() as conn:
        conn.execute("TRUNCATE alerts, evidence, incidents, change_events, observed_state, service_map_snapshots RESTART IDENTITY CASCADE")
    yield p
    p.close()


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    url = os.environ.get("ONCALL_TEST_REDIS_URL")
    if not url:
        pytest.skip("ONCALL_TEST_REDIS_URL not set")
    client = redis.Redis.from_url(url)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()
