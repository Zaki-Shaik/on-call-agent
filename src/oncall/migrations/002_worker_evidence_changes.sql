-- Worker lease: the worker that claimed an incident and until when. A crashed worker's lease
-- expires, so another worker can take the job over without two running it at once.
ALTER TABLE incidents
    ADD COLUMN lease_owner       text,
    ADD COLUMN lease_expires_at  timestamptz,
    -- Per-incident counter for short evidence ids (E1, E2, ...).
    ADD COLUMN evidence_seq      integer NOT NULL DEFAULT 0;

CREATE INDEX incidents_queued ON incidents (updated_at) WHERE status = 'queued';

-- Every finding a tool returns, citable by the report as `ref` (E1, E2, ...).
CREATE TABLE evidence (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_id  uuid NOT NULL REFERENCES incidents (id),
    ref          text NOT NULL,
    tool         text NOT NULL,
    inputs       jsonb NOT NULL,
    summary      text NOT NULL,
    link         text,
    data         jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (incident_id, ref)
);

-- Changes to the monitored system (flag changes, deploys), in one shape.
CREATE TABLE change_events (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    system_profile  text NOT NULL,
    occurred_at     timestamptz NOT NULL,
    type            text NOT NULL CHECK (type IN ('flag_change', 'deploy')),
    -- Null when the change is not tied to one service (e.g. a feature flag).
    service         text,
    actor           text NOT NULL,
    description     text NOT NULL,
    link            text,
    details         jsonb NOT NULL,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX change_events_by_time ON change_events (system_profile, occurred_at);

-- Last observed state the change recorder diffs against (flag values, service versions).
CREATE TABLE observed_state (
    system_profile  text NOT NULL,
    kind            text NOT NULL CHECK (kind IN ('flag', 'version')),
    key             text NOT NULL,
    value           jsonb NOT NULL,
    observed_at     timestamptz NOT NULL,
    PRIMARY KEY (system_profile, kind, key)
);
