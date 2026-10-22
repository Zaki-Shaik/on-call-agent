"""Internal alert format. Every alert source is converted to this before anything else sees it."""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict


class AlertStatus(StrEnum):
    FIRING = "firing"
    RESOLVED = "resolved"


class Alert(BaseModel):
    model_config = ConfigDict(frozen=True)

    # Set by the source (Alertmanager hashes the label set). Stable across re-sends and
    # across firing episodes, so dedupe uses (source, fingerprint, starts_at).
    fingerprint: str
    source: str
    status: AlertStatus
    # Read from the label named in the profile. None when the label is missing.
    service: str | None
    starts_at: datetime
    ends_at: datetime | None = None
    severity: str
    summary: str
    # The alert exactly as the source sent it, for replay.
    raw_payload: dict[str, Any]
