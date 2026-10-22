-- Healthy service maps built from traces. Each build is one row; the newest is the one in use.
CREATE TABLE service_map_snapshots (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    system_profile  text NOT NULL,
    built_at        timestamptz NOT NULL DEFAULT now(),
    window_start    timestamptz NOT NULL,
    window_end      timestamptz NOT NULL,
    -- Periods left out of the window because an incident was open: [{start, end}, ...].
    excluded        jsonb NOT NULL,
    traces_sampled  integer NOT NULL,
    failed_queries  integer NOT NULL,
    -- {nodes: [...], edges: [...]}, read and written whole.
    map             jsonb NOT NULL
);

CREATE INDEX service_map_latest ON service_map_snapshots (system_profile, built_at DESC);
