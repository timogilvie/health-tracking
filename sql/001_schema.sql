CREATE TABLE sources (
    source_id UUID PRIMARY KEY DEFAULT uuid(),
    name VARCHAR NOT NULL UNIQUE,
    source_type VARCHAR NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    metadata JSON NOT NULL DEFAULT '{}'
);

CREATE TABLE devices (
    device_id UUID PRIMARY KEY DEFAULT uuid(),
    source_id UUID NOT NULL REFERENCES sources(source_id),
    manufacturer VARCHAR,
    model VARCHAR,
    name VARCHAR,
    metadata JSON NOT NULL DEFAULT '{}',
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    UNIQUE (source_id, manufacturer, model, name)
);

CREATE TABLE ingestion_runs (
    ingestion_run_id UUID PRIMARY KEY DEFAULT uuid(),
    source_id UUID REFERENCES sources(source_id),
    requested_start TIMESTAMPTZ,
    requested_end TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    finished_at TIMESTAMPTZ,
    raw_count BIGINT NOT NULL DEFAULT 0 CHECK (raw_count >= 0),
    normalized_count BIGINT NOT NULL DEFAULT 0 CHECK (normalized_count >= 0),
    inserted_count BIGINT NOT NULL DEFAULT 0 CHECK (inserted_count >= 0),
    updated_count BIGINT NOT NULL DEFAULT 0 CHECK (updated_count >= 0),
    duplicate_count BIGINT NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
    status VARCHAR NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    error VARCHAR,
    metadata JSON NOT NULL DEFAULT '{}'
);

CREATE TABLE observations (
    observation_id UUID PRIMARY KEY DEFAULT uuid(),
    metric VARCHAR NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    observed_until TIMESTAMPTZ,
    value DOUBLE NOT NULL,
    unit VARCHAR NOT NULL,
    source_id UUID NOT NULL REFERENCES sources(source_id),
    source_record_id VARCHAR NOT NULL,
    device_id UUID REFERENCES devices(device_id),
    original_metric VARCHAR,
    original_value DOUBLE,
    original_unit VARCHAR,
    quality VARCHAR NOT NULL DEFAULT 'valid' CHECK (quality IN ('valid', 'suspect', 'invalid')),
    timezone VARCHAR,
    local_date DATE,
    raw_file VARCHAR NOT NULL,
    transform_version VARCHAR NOT NULL,
    metadata JSON NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (source_id, source_record_id, metric)
);

CREATE TABLE blood_pressure (
    blood_pressure_id UUID PRIMARY KEY DEFAULT uuid(),
    measured_at TIMESTAMPTZ NOT NULL,
    local_date DATE NOT NULL,
    systolic_mmhg DOUBLE NOT NULL CHECK (systolic_mmhg > 0),
    diastolic_mmhg DOUBLE NOT NULL CHECK (diastolic_mmhg > 0),
    pulse_bpm DOUBLE CHECK (pulse_bpm > 0),
    measurement_number INTEGER,
    source_id UUID NOT NULL REFERENCES sources(source_id),
    source_record_id VARCHAR NOT NULL,
    source_group_id VARCHAR,
    device_id UUID REFERENCES devices(device_id),
    context VARCHAR,
    notes VARCHAR,
    quality VARCHAR NOT NULL DEFAULT 'valid' CHECK (quality IN ('valid', 'suspect', 'invalid')),
    raw_file VARCHAR NOT NULL,
    transform_version VARCHAR NOT NULL,
    metadata JSON NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (source_id, source_record_id)
);

CREATE TABLE sleep_sessions (
    sleep_session_id UUID PRIMARY KEY DEFAULT uuid(),
    sleep_date DATE NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    time_in_bed_seconds BIGINT,
    total_sleep_seconds BIGINT,
    awake_seconds BIGINT,
    light_seconds BIGINT,
    deep_seconds BIGINT,
    rem_seconds BIGINT,
    latency_seconds BIGINT,
    efficiency_pct DOUBLE,
    resting_hr_bpm DOUBLE,
    lowest_hr_bpm DOUBLE,
    average_hrv_rmssd_ms DOUBLE,
    respiratory_rate DOUBLE,
    sleep_score DOUBLE,
    source_id UUID NOT NULL REFERENCES sources(source_id),
    source_record_id VARCHAR NOT NULL,
    device_id UUID REFERENCES devices(device_id),
    raw_file VARCHAR NOT NULL,
    transform_version VARCHAR NOT NULL,
    metadata JSON NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    CHECK (ended_at >= started_at),
    UNIQUE (source_id, source_record_id)
);

CREATE TABLE workouts (
    workout_id UUID PRIMARY KEY DEFAULT uuid(),
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    local_date DATE NOT NULL,
    workout_type VARCHAR NOT NULL CHECK (
        workout_type IN (
            'resistance', 'walking', 'running', 'cycling', 'rowing', 'swimming',
            'rucking', 'elliptical', 'mobility', 'sports', 'other'
        )
    ),
    duration_seconds BIGINT NOT NULL CHECK (duration_seconds >= 0),
    intensity VARCHAR,
    rpe DOUBLE CHECK (rpe IS NULL OR (rpe >= 0 AND rpe <= 10)),
    distance_m DOUBLE CHECK (distance_m IS NULL OR distance_m >= 0),
    energy_kcal DOUBLE CHECK (energy_kcal IS NULL OR energy_kcal >= 0),
    average_hr_bpm DOUBLE,
    max_hr_bpm DOUBLE,
    source_id UUID NOT NULL REFERENCES sources(source_id),
    source_record_id VARCHAR NOT NULL,
    device_id UUID REFERENCES devices(device_id),
    notes VARCHAR,
    raw_file VARCHAR NOT NULL,
    transform_version VARCHAR NOT NULL,
    metadata JSON NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    CHECK (ended_at >= started_at),
    UNIQUE (source_id, source_record_id)
);

CREATE TABLE lab_results (
    lab_result_id UUID PRIMARY KEY DEFAULT uuid(),
    collected_at TIMESTAMPTZ,
    resulted_at TIMESTAMPTZ,
    canonical_name VARCHAR,
    original_name VARCHAR NOT NULL,
    numeric_value DOUBLE,
    text_value VARCHAR,
    unit VARCHAR,
    reference_low DOUBLE,
    reference_high DOUBLE,
    reference_text VARCHAR,
    abnormal_flag VARCHAR,
    provider VARCHAR,
    fasting BOOLEAN,
    source_id UUID NOT NULL REFERENCES sources(source_id),
    source_record_id VARCHAR NOT NULL,
    raw_file VARCHAR NOT NULL,
    transform_version VARCHAR NOT NULL,
    metadata JSON NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    CHECK (numeric_value IS NOT NULL OR text_value IS NOT NULL),
    UNIQUE (source_id, source_record_id)
);

CREATE TABLE events (
    event_id UUID PRIMARY KEY DEFAULT uuid(),
    event_type VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    local_date DATE NOT NULL,
    value DOUBLE,
    unit VARCHAR,
    notes VARCHAR,
    source_id UUID NOT NULL REFERENCES sources(source_id),
    source_record_id VARCHAR NOT NULL,
    raw_file VARCHAR NOT NULL,
    transform_version VARCHAR NOT NULL,
    metadata JSON NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    CHECK (ended_at IS NULL OR ended_at >= started_at),
    UNIQUE (source_id, source_record_id)
);

CREATE TABLE duplicate_links (
    duplicate_link_id UUID PRIMARY KEY DEFAULT uuid(),
    record_type VARCHAR NOT NULL,
    canonical_record_id UUID NOT NULL,
    duplicate_record_id UUID NOT NULL,
    match_method VARCHAR NOT NULL,
    confidence DOUBLE NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    resolution VARCHAR NOT NULL DEFAULT 'candidate' CHECK (
        resolution IN ('candidate', 'confirmed', 'rejected')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    metadata JSON NOT NULL DEFAULT '{}',
    CHECK (canonical_record_id <> duplicate_record_id),
    UNIQUE (record_type, canonical_record_id, duplicate_record_id, match_method)
);
