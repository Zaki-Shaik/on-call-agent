"""What every tool shares: its context, result statuses, and number formatting."""

from dataclasses import dataclass
from enum import StrEnum
from math import floor, log10
from typing import TYPE_CHECKING
from uuid import UUID

from psycopg_pool import ConnectionPool

from oncall.evidence import EvidenceDraft, EvidenceStore
from oncall.links import Linker
from oncall.profile import SystemProfile
from oncall.telemetry.factory import Backends

if TYPE_CHECKING:  # the service map imports this module
    from oncall.servicemap.store import ServiceMaps


class Status(StrEnum):
    OK = "ok"
    # The backend answered and has nothing for this query. Distinct from a zero value.
    NO_DATA = "no_data"
    # The backend failed or timed out. Distinct from "nothing there".
    ERROR = "error"


@dataclass(frozen=True)
class ToolContext:
    profile: SystemProfile
    backends: Backends
    links: Linker
    evidence: EvidenceStore
    pool: ConnectionPool
    incident_id: UUID
    maps: "ServiceMaps | None" = None

    def save(self, drafts: list[EvidenceDraft]) -> list[str]:
        return self.evidence.save(self.incident_id, drafts)


def sig(value: float | None, digits: int = 3) -> float | None:
    """Round to significant digits, so outputs stay short."""
    if value is None or value == 0:
        return value
    return round(value, -int(floor(log10(abs(value)))) + digits - 1)
