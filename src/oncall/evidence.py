"""Evidence: every finding a tool returns, stored so the report can cite it by short id (E1, E2, ...)."""

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool


@dataclass(frozen=True)
class EvidenceDraft:
    tool: str
    inputs: dict[str, Any]
    summary: str
    link: str | None
    data: dict[str, Any] = field(default_factory=dict)


class EvidenceStore:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def save(self, incident_id: UUID, drafts: list[EvidenceDraft]) -> list[str]:
        """Store drafts and return their refs, in order."""
        if not drafts:
            return []
        with self._pool.connection() as conn, conn.transaction():
            # Atomic per-incident counter: concurrent saves never hand out the same ref.
            row = conn.execute(
                "UPDATE incidents SET evidence_seq = evidence_seq + %s WHERE id = %s RETURNING evidence_seq",
                (len(drafts), incident_id),
            ).fetchone()
            if row is None:
                raise ValueError(f"incident {incident_id} does not exist")
            first = row["evidence_seq"] - len(drafts) + 1
            refs = [f"E{first + i}" for i in range(len(drafts))]
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO evidence (incident_id, ref, tool, inputs, summary, link, data)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    [(incident_id, ref, d.tool, Jsonb(d.inputs), d.summary, d.link, Jsonb(d.data))
                     for ref, d in zip(refs, drafts)],
                )
            return refs

    def get(self, incident_id: UUID, ref: str) -> dict[str, Any] | None:
        with self._pool.connection() as conn:
            return conn.execute(
                "SELECT * FROM evidence WHERE incident_id = %s AND ref = %s", (incident_id, ref)
            ).fetchone()
