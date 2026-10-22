import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from oncall.alerts.models import AlertStatus
from oncall.alerts.sources import SOURCES
from oncall.profile import AlertMapping

FIXTURES = Path(__file__).parent / "fixtures" / "alerts"
MAPPING = AlertMapping(service_label="service_name")


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("source", "fixture"),
    [("alertmanager", "alertmanager_firing.json"), ("grafana", "grafana_firing.json")],
)
def test_firing_payload_converts_to_internal_format(source: str, fixture: str) -> None:
    body = load(fixture)
    raw = body["alerts"][0]

    [alert] = SOURCES[source].parse(body, MAPPING)

    assert alert.source == source
    assert alert.status is AlertStatus.FIRING
    assert alert.fingerprint == raw["fingerprint"]
    assert alert.service == raw["labels"]["service_name"]
    assert alert.starts_at == datetime.fromisoformat(raw["startsAt"])
    assert alert.starts_at.tzinfo is not None
    assert alert.ends_at is None  # zero-value endsAt on a firing alert is not an end time
    assert alert.summary
    assert alert.raw_payload == raw  # unchanged, including source-specific extra fields


def test_resolved_payload_carries_end_time() -> None:
    body = load("alertmanager_resolved.json")
    [alert] = SOURCES["alertmanager"].parse(body, MAPPING)
    assert alert.status is AlertStatus.RESOLVED
    assert alert.ends_at == datetime.fromisoformat(body["alerts"][0]["endsAt"])


def test_service_and_severity_labels_come_from_the_profile() -> None:
    body = load("alertmanager_firing.json")
    body["alerts"][0]["labels"] |= {"app": "svc-a", "level": "page"}
    [alert] = SOURCES["alertmanager"].parse(body, AlertMapping(service_label="app", severity_label="level"))
    assert alert.service == "svc-a"
    assert alert.severity == "page"


def test_missing_labels() -> None:
    body = load("alertmanager_firing.json")
    body["alerts"][0]["labels"] = {"alertname": "NoLabels"}
    body["alerts"][0]["annotations"] = {}
    [alert] = SOURCES["alertmanager"].parse(body, MAPPING)
    assert alert.service is None
    assert alert.severity == "unknown"
    assert alert.summary == "NoLabels"


def test_multiple_alerts_in_one_delivery() -> None:
    body = load("alertmanager_firing.json")
    second = json.loads(json.dumps(body["alerts"][0]))
    second["fingerprint"] = "ffff000011112222"
    second["startsAt"] = "2026-10-22T00:46:00Z"
    body["alerts"].append(second)
    alerts = SOURCES["alertmanager"].parse(body, MAPPING)
    assert [a.fingerprint for a in alerts] == [body["alerts"][0]["fingerprint"], "ffff000011112222"]
    assert alerts[1].starts_at == datetime(2026, 10, 22, 0, 46, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda b: b.pop("alerts"),
        lambda b: b["alerts"][0].pop("fingerprint"),
        lambda b: b["alerts"][0].update(status="pending"),
        lambda b: b["alerts"][0].update(startsAt="yesterday"),
    ],
    ids=["no-alerts", "no-fingerprint", "bad-status", "bad-timestamp"],
)
def test_malformed_payload_is_rejected(mutate) -> None:
    body = load("alertmanager_firing.json")
    mutate(body)
    with pytest.raises(ValidationError):
        SOURCES["alertmanager"].parse(body, MAPPING)
