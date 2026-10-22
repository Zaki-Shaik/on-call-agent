import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from oncall.api.main import app
from oncall.jobs import INVESTIGATIONS_STREAM

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures" / "alerts"


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def client(pool, redis_client, database_url, monkeypatch):
    monkeypatch.setenv("ONCALL_DATABASE_URL", database_url)
    monkeypatch.setenv("ONCALL_REDIS_URL", os.environ["ONCALL_TEST_REDIS_URL"])
    monkeypatch.setenv("ONCALL_PROFILE_PATH", str(ROOT / "profiles" / "otel-demo.yaml"))
    monkeypatch.delenv("ONCALL_WEBHOOK_TOKEN", raising=False)
    with TestClient(app) as c:
        yield c


def test_alertmanager_firing_then_resend_then_resolved(client, redis_client) -> None:
    first = client.post("/webhooks/alertmanager", json=load("alertmanager_firing.json"))
    assert first.status_code == 200
    [result] = first.json()["results"]
    assert result["outcome"] == "created"
    assert redis_client.xlen(INVESTIGATIONS_STREAM) == 1
    [(_, fields)] = redis_client.xrange(INVESTIGATIONS_STREAM)
    assert fields[b"incident_id"].decode() == result["incident_id"]

    resend = client.post("/webhooks/alertmanager", json=load("alertmanager_firing.json"))
    assert resend.json()["results"][0]["outcome"] == "duplicate"

    done = client.post("/webhooks/alertmanager", json=load("alertmanager_resolved.json"))
    assert done.json()["results"][0]["outcome"] == "resolved"
    assert redis_client.xlen(INVESTIGATIONS_STREAM) == 1


def test_grafana_source(client) -> None:
    response = client.post("/webhooks/grafana", json=load("grafana_firing.json"))
    assert response.status_code == 200
    assert response.json()["results"][0]["outcome"] == "created"


def test_unknown_source_is_404(client) -> None:
    assert client.post("/webhooks/nagios", json={}).status_code == 404


def test_malformed_body_is_422(client) -> None:
    assert client.post("/webhooks/alertmanager", json={"alerts": [{"status": "firing"}]}).status_code == 422


def test_token_is_enforced_when_configured(client) -> None:
    client.app.state.settings.webhook_token = "s3cret"
    body = load("alertmanager_firing.json")
    assert client.post("/webhooks/alertmanager", json=body).status_code == 401
    bad = client.post("/webhooks/alertmanager", json=body, headers={"Authorization": "Bearer nope"})
    assert bad.status_code == 401
    ok = client.post("/webhooks/alertmanager", json=body, headers={"Authorization": "Bearer s3cret"})
    assert ok.status_code == 200
