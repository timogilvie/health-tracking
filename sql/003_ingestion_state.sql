CREATE TABLE source_sync_state (
    source_id UUID PRIMARY KEY REFERENCES sources(source_id),
    last_successful_end TIMESTAMPTZ,
    cursor JSON,
    last_ingestion_run_id UUID REFERENCES ingestion_runs(ingestion_run_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);
