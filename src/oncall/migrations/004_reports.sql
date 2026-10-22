-- 'reported': the investigation finished and its report is published, with no action pending.
-- The incident is still open (later alerts still join it) until someone resolves it.
ALTER TABLE incidents DROP CONSTRAINT incidents_status_check;
ALTER TABLE incidents ADD CONSTRAINT incidents_status_check
    CHECK (status IN ('queued', 'investigating', 'reported', 'awaiting_approval', 'resolved', 'closed'));

DROP INDEX incidents_open;
CREATE INDEX incidents_open ON incidents (system_profile, opened_at)
    WHERE status IN ('queued', 'investigating', 'reported', 'awaiting_approval');

-- One report per incident: the agent's structured report plus what the run cost.
CREATE TABLE reports (
    incident_id      uuid PRIMARY KEY REFERENCES incidents (id),
    report           jsonb NOT NULL,
    confidence       text NOT NULL CHECK (confidence IN ('high', 'medium', 'low', 'inconclusive')),
    -- Why the investigation stopped early (a budget, model failure, ...); null when it finished.
    forced_stop      text,
    -- The flag change the report proposes, if it passed the allowed-flag check: {flag, value}.
    proposed_action  jsonb,
    tool_calls       integer NOT NULL,
    llm_calls        integer NOT NULL,
    input_tokens     integer NOT NULL,
    output_tokens    integer NOT NULL,
    cost_usd         numeric(10, 6) NOT NULL,
    -- Time spent investigating (excludes time the worker was down before resuming).
    active_seconds   double precision NOT NULL,
    -- From the incident opening (first alert) to the report: what the engineer waits.
    time_to_report_seconds double precision NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);
