CREATE TABLE incidents (
    -- Also the LangGraph thread_id, so no separate column.
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    status          text NOT NULL
        CHECK (status IN ('queued', 'investigating', 'awaiting_approval', 'resolved', 'closed')),
    -- Service of the first alert; later alerts may name other services (see alerts.service).
    service         text NOT NULL,
    system_profile  text NOT NULL,
    -- starts_at of the first alert, not the wall clock, so grouping is replay-safe.
    opened_at       timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Grouping lookup: open incidents of a system by time.
CREATE INDEX incidents_open ON incidents (system_profile, opened_at)
    WHERE status IN ('queued', 'investigating', 'awaiting_approval');

CREATE TABLE alerts (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source            text NOT NULL,
    fingerprint       text NOT NULL,
    starts_at         timestamptz NOT NULL,
    ends_at           timestamptz,
    status            text NOT NULL CHECK (status IN ('firing', 'resolved')),
    service           text,
    severity          text NOT NULL,
    summary           text NOT NULL,
    incident_id       uuid REFERENCES incidents (id),
    -- First payload seen for this alert, unchanged; the resolve notification kept separately.
    raw_payload       jsonb NOT NULL,
    resolved_payload  jsonb,
    received_at       timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    -- The fingerprint repeats when the same alert fires again later; starts_at tells episodes apart.
    UNIQUE (source, fingerprint, starts_at)
);

CREATE INDEX alerts_incident ON alerts (incident_id);
