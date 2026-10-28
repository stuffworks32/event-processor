-- main table
CREATE TABLE IF NOT EXISTS received_events(
   tenant_id TEXT NOT NULL,
   event_id UUID NOT NULL,
   event_type TEXT,
   schema_version TEXT,
   payload JSONB,
   status TEXT NOT NULL CHECK (status in ('VALID', 'INVALID')),
   errors  JSONB,
   occurred_at  TIMESTAMPTZ,
   loaded_at  TIMESTAMPTZ NOT NULL default now(),
   PRIMARY KEY (tenant_id, event_id)
);
--this index helps with figuring the new records 
CREATE INDEX IF NOT EXISTS ix_received_events_valid_loaded_at
    ON received_events (loaded_at)
    WHERE status = 'VALID';

--not parsable rows go here
CREATE TABLE IF NOT EXISTS bad_messages(
   body_hash BYTEA NOT NULL PRIMARY KEY,
   raw_body BYTEA NOT NULL,
   error TEXT NOT NULL,
   loaded_at  TIMESTAMPTZ NOT NULL default now()
);