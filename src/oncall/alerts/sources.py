"""Alert sources: each one turns a webhook body into internal Alerts.

Adding a source means adding a class here and registering it in SOURCES; ingestion,
storage and the webhook endpoint stay the same.
"""

from datetime import datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict

from oncall.alerts.models import Alert, AlertStatus
from oncall.profile import AlertMapping


class AlertSource(Protocol):
    name: str

    def parse(self, body: dict[str, Any], mapping: AlertMapping) -> list[Alert]:
        """Validate a webhook body and convert it. Raises pydantic.ValidationError if malformed."""
        ...


class _WebhookAlert(BaseModel):
    """One alert in the Alertmanager webhook format (also used by Grafana)."""

    model_config = ConfigDict(extra="allow")

    status: Literal["firing", "resolved"]
    labels: dict[str, str]
    annotations: dict[str, str] = {}
    startsAt: datetime
    endsAt: datetime | None = None
    fingerprint: str


class _WebhookBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    alerts: list[_WebhookAlert]


def _convert(source: str, raw: dict[str, Any], alert: _WebhookAlert, mapping: AlertMapping) -> Alert:
    status = AlertStatus(alert.status)
    # Firing alerts carry a zero-value endsAt (0001-01-01) or a future expiry; neither is an end.
    ends_at = alert.endsAt if status is AlertStatus.RESOLVED else None
    summary = (
        alert.annotations.get("summary")
        or alert.annotations.get("description")
        or alert.labels.get("alertname", "")
    )
    return Alert(
        fingerprint=alert.fingerprint,
        source=source,
        status=status,
        service=alert.labels.get(mapping.service_label) or None,
        starts_at=alert.startsAt,
        ends_at=ends_at,
        severity=alert.labels.get(mapping.severity_label, "unknown"),
        summary=summary,
        raw_payload=raw,
    )


class _WebhookFormatSource:
    """Shared parser for sources that send the Alertmanager webhook format."""

    name: str

    def parse(self, body: dict[str, Any], mapping: AlertMapping) -> list[Alert]:
        parsed = _WebhookBody.model_validate(body)
        # Keep each alert's original dict (not a re-serialization) as the raw payload.
        return [_convert(self.name, raw, a, mapping) for raw, a in zip(body["alerts"], parsed.alerts)]


class AlertmanagerSource(_WebhookFormatSource):
    name = "alertmanager"


class GrafanaSource(_WebhookFormatSource):
    # Grafana-managed alerting sends the Alertmanager format plus extra fields
    # (values, valueString, dashboardURL, ...), which are kept in raw_payload.
    name = "grafana"


SOURCES: dict[str, AlertSource] = {s.name: s for s in (AlertmanagerSource(), GrafanaSource())}
