CREATE INDEX observations_metric_time_idx ON observations(metric, observed_at);
CREATE INDEX observations_source_time_idx ON observations(source_id, observed_at);
CREATE INDEX blood_pressure_date_idx ON blood_pressure(local_date, measured_at);
CREATE INDEX sleep_sessions_date_idx ON sleep_sessions(sleep_date);
CREATE INDEX workouts_date_type_idx ON workouts(local_date, workout_type);
CREATE INDEX lab_results_name_time_idx ON lab_results(canonical_name, collected_at);
CREATE INDEX events_type_time_idx ON events(event_type, started_at);
CREATE INDEX ingestion_runs_source_time_idx ON ingestion_runs(source_id, started_at);
CREATE INDEX duplicate_links_canonical_idx ON duplicate_links(record_type, canonical_record_id);
