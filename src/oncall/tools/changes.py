"""list_recent_changes: flag changes and deploys in a window, in one shape."""

from datetime import datetime

from pydantic import BaseModel

from oncall.evidence import EvidenceDraft
from oncall.telemetry.models import TimeWindow
from oncall.tools.base import Status, ToolContext


class Change(BaseModel):
    time: datetime
    type: str  # flag_change, deploy
    service: str | None
    actor: str
    description: str
    evidence: str | None = None


class ChangesResult(BaseModel):
    tool: str = "list_recent_changes"
    window: TimeWindow
    status: Status
    changes: list[Change] = []
    note: str | None = None


def list_recent_changes(ctx: ToolContext, window: TimeWindow,
                        services: list[str] | None = None) -> ChangesResult:
    inputs = {"window": window.model_dump(mode="json"), "services": services}
    with ctx.pool.connection() as conn:
        rows = conn.execute(
            """
            SELECT occurred_at, type, service, actor, description, link
            FROM change_events
            WHERE system_profile = %s AND occurred_at >= %s AND occurred_at < %s
              -- changes not tied to a service (flags) are always relevant
              AND (%s::text[] IS NULL OR service IS NULL OR service = ANY(%s::text[]))
            ORDER BY occurred_at
            """,
            (ctx.profile.name, window.start, window.end, services, services),
        ).fetchall()
    if not rows:
        return ChangesResult(window=window, status=Status.NO_DATA,
                             note="no flag changes or deploys recorded in the window")
    changes = [Change(time=r["occurred_at"], type=r["type"], service=r["service"],
                      actor=r["actor"], description=r["description"]) for r in rows]
    refs = ctx.save([
        EvidenceDraft("list_recent_changes", inputs,
                      f"{c.time:%H:%M:%SZ} {c.type}{f' {c.service}' if c.service else ''}: {c.description}",
                      r["link"], c.model_dump(mode="json"))
        for c, r in zip(changes, rows)
    ])
    for c, ref in zip(changes, refs):
        c.evidence = ref
    return ChangesResult(window=window, status=Status.OK, changes=changes)
