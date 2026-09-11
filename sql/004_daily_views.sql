CREATE TABLE source_priorities (
    data_type VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    priority INTEGER NOT NULL CHECK (priority > 0),
    PRIMARY KEY (data_type, source),
    UNIQUE (data_type, priority)
);

CREATE VIEW canonical_observations AS
WITH candidates AS (
    SELECT
        o.*,
        s.name AS source_name,
        COALESCE(
            o.local_date,
            CAST(timezone(COALESCE(o.timezone, 'UTC'), o.observed_at) AS DATE)
        ) AS canonical_date,
        COALESCE(sp.priority, 10000) AS source_priority
    FROM observations o
    JOIN sources s ON s.source_id = o.source_id
    LEFT JOIN source_priorities sp
        ON sp.data_type = o.metric
       AND sp.source = s.name
    WHERE s.enabled
      AND o.quality <> 'invalid'
), ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY metric, canonical_date
            ORDER BY
                CASE quality WHEN 'valid' THEN 0 ELSE 1 END,
                source_priority,
                source_name
        ) AS selection_rank
    FROM candidates
)
SELECT * EXCLUDE (selection_rank)
FROM ranked
WHERE selection_rank = 1;

CREATE VIEW canonical_blood_pressure AS
WITH candidates AS (
    SELECT
        bp.*,
        s.name AS source_name,
        COALESCE(sp.priority, 10000) AS source_priority
    FROM blood_pressure bp
    JOIN sources s ON s.source_id = bp.source_id
    LEFT JOIN source_priorities sp
        ON sp.data_type = 'blood_pressure'
       AND sp.source = s.name
    WHERE s.enabled
      AND bp.quality <> 'invalid'
), ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY local_date
            ORDER BY
                CASE quality WHEN 'valid' THEN 0 ELSE 1 END,
                source_priority,
                source_name
        ) AS selection_rank
    FROM candidates
)
SELECT * EXCLUDE (selection_rank)
FROM ranked
WHERE selection_rank = 1;

CREATE VIEW blood_pressure_session_readings AS
WITH ordered AS (
    SELECT
        *,
        LAG(measured_at) OVER (
            PARTITION BY local_date, source_id
            ORDER BY measured_at, blood_pressure_id
        ) AS previous_measured_at
    FROM canonical_blood_pressure
), boundaries AS (
    SELECT
        *,
        CASE
            WHEN previous_measured_at IS NULL
              OR measured_at > previous_measured_at + INTERVAL '10 minutes'
            THEN 1 ELSE 0
        END AS starts_session
    FROM ordered
), sessionized AS (
    SELECT
        *,
        SUM(starts_session) OVER (
            PARTITION BY local_date, source_id
            ORDER BY measured_at, blood_pressure_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_number
    FROM boundaries
)
SELECT
    * EXCLUDE (previous_measured_at, starts_session),
    ROW_NUMBER() OVER (
        PARTITION BY local_date, source_id, session_number
        ORDER BY measured_at, blood_pressure_id
    ) AS session_reading_number
FROM sessionized;

CREATE VIEW blood_pressure_sessions AS
WITH aggregated AS (
    SELECT
        local_date,
        source_id,
        source_name,
        session_number,
        MIN(measured_at) AS session_started_at,
        MAX(measured_at) AS session_ended_at,
        COUNT(*) AS reading_count,
        FIRST(systolic_mmhg ORDER BY measured_at, blood_pressure_id) AS first_systolic_mmhg,
        FIRST(diastolic_mmhg ORDER BY measured_at, blood_pressure_id) AS first_diastolic_mmhg,
        FIRST(pulse_bpm ORDER BY measured_at, blood_pressure_id) AS first_pulse_bpm,
        AVG(systolic_mmhg) FILTER (
            WHERE session_reading_number > 1
        ) AS subsequent_systolic_mmhg,
        AVG(diastolic_mmhg) FILTER (
            WHERE session_reading_number > 1
        ) AS subsequent_diastolic_mmhg,
        AVG(pulse_bpm) FILTER (
            WHERE session_reading_number > 1
        ) AS subsequent_pulse_bpm,
        AVG(systolic_mmhg) AS session_systolic_mmhg,
        AVG(diastolic_mmhg) AS session_diastolic_mmhg,
        AVG(pulse_bpm) AS session_pulse_bpm
    FROM blood_pressure_session_readings
    GROUP BY local_date, source_id, source_name, session_number
)
SELECT
    CONCAT(source_id, ':', local_date, ':', session_number) AS session_id,
    *,
    COALESCE(subsequent_systolic_mmhg, first_systolic_mmhg) AS preferred_systolic_mmhg,
    COALESCE(subsequent_diastolic_mmhg, first_diastolic_mmhg) AS preferred_diastolic_mmhg,
    COALESCE(subsequent_pulse_bpm, first_pulse_bpm) AS preferred_pulse_bpm
FROM aggregated;

CREATE VIEW canonical_sleep_sessions AS
WITH candidates AS (
    SELECT
        ss.*,
        ss.sleep_date AS wake_date,
        s.name AS source_name,
        COALESCE(sp.priority, 10000) AS source_priority
    FROM sleep_sessions ss
    JOIN sources s ON s.source_id = ss.source_id
    LEFT JOIN source_priorities sp
        ON sp.data_type = 'sleep'
       AND sp.source = s.name
    WHERE s.enabled
), ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY wake_date
            ORDER BY source_priority, source_name
        ) AS selection_rank
    FROM candidates
)
SELECT * EXCLUDE (selection_rank)
FROM ranked
WHERE selection_rank = 1;

CREATE VIEW canonical_workouts AS
WITH candidates AS (
    SELECT
        w.*,
        s.name AS source_name,
        COALESCE(sp.priority, 10000) AS source_priority
    FROM workouts w
    JOIN sources s ON s.source_id = w.source_id
    LEFT JOIN source_priorities sp
        ON sp.data_type = 'workouts'
       AND sp.source = s.name
    WHERE s.enabled
), ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY local_date, workout_type
            ORDER BY source_priority, source_name
        ) AS selection_rank
    FROM candidates
)
SELECT * EXCLUDE (selection_rank)
FROM ranked
WHERE selection_rank = 1;

CREATE VIEW daily_health AS
WITH observation_daily AS (
    SELECT
        canonical_date AS local_date,
        ARG_MAX(value, observed_at) FILTER (WHERE metric = 'weight_kg') AS weight_kg,
        ARG_MAX(value, observed_at) FILTER (WHERE metric = 'body_fat_pct') AS body_fat_pct,
        ARG_MAX(value, observed_at) FILTER (WHERE metric = 'lean_mass_kg') AS lean_mass_kg,
        ARG_MAX(value, observed_at) FILTER (
            WHERE metric = 'body_fat_mass_kg'
        ) AS body_fat_mass_kg,
        ARG_MAX(value, observed_at) FILTER (
            WHERE metric = 'skeletal_muscle_mass_kg'
        ) AS skeletal_muscle_mass_kg,
        ARG_MAX(value, observed_at) FILTER (
            WHERE metric = 'body_water_mass_kg'
        ) AS body_water_mass_kg,
        ARG_MAX(value, observed_at) FILTER (WHERE metric = 'bone_mass_kg') AS bone_mass_kg,
        AVG(value) FILTER (WHERE metric = 'resting_hr_bpm') AS resting_hr_bpm,
        AVG(value) FILTER (WHERE metric = 'hrv_rmssd_ms') AS hrv_rmssd_ms,
        SUM(value) FILTER (WHERE metric = 'steps') AS steps,
        SUM(value) FILTER (WHERE metric = 'active_energy_kcal') AS active_energy_kcal
    FROM canonical_observations
    GROUP BY canonical_date
), blood_pressure_daily AS (
    SELECT
        local_date,
        AVG(preferred_systolic_mmhg) AS systolic_mmhg,
        AVG(preferred_diastolic_mmhg) AS diastolic_mmhg,
        AVG(preferred_pulse_bpm) AS blood_pressure_pulse_bpm,
        COUNT(*) AS blood_pressure_session_count,
        SUM(reading_count) AS blood_pressure_reading_count
    FROM blood_pressure_sessions
    GROUP BY local_date
), sleep_daily AS (
    SELECT
        wake_date AS local_date,
        SUM(total_sleep_seconds) / 60.0 AS total_sleep_minutes,
        SUM(time_in_bed_seconds) / 60.0 AS time_in_bed_minutes,
        SUM(awake_seconds) / 60.0 AS awake_minutes,
        SUM(light_seconds) / 60.0 AS light_sleep_minutes,
        SUM(deep_seconds) / 60.0 AS deep_sleep_minutes,
        SUM(rem_seconds) / 60.0 AS rem_sleep_minutes,
        AVG(efficiency_pct) AS sleep_efficiency_pct,
        ARG_MAX(resting_hr_bpm, COALESCE(total_sleep_seconds, 0)) AS sleep_resting_hr_bpm,
        ARG_MAX(average_hrv_rmssd_ms, COALESCE(total_sleep_seconds, 0)) AS sleep_hrv_rmssd_ms,
        ARG_MAX(respiratory_rate, COALESCE(total_sleep_seconds, 0)) AS respiratory_rate,
        ARG_MAX(sleep_score, COALESCE(total_sleep_seconds, 0)) AS sleep_score
    FROM canonical_sleep_sessions
    GROUP BY wake_date
), workout_daily AS (
    SELECT
        local_date,
        SUM(duration_seconds) FILTER (
            WHERE workout_type = 'resistance'
        ) / 60.0 AS resistance_minutes,
        SUM(duration_seconds) FILTER (
            WHERE workout_type IN (
                'walking', 'running', 'cycling', 'rowing', 'swimming',
                'rucking', 'elliptical', 'sports'
            )
        ) / 60.0 AS cardio_minutes,
        COUNT(*) AS workout_count
    FROM canonical_workouts
    GROUP BY local_date
), event_daily AS (
    SELECT
        e.local_date,
        SUM(e.value) FILTER (WHERE e.event_type = 'alcohol') AS alcohol_units,
        COUNT(*) FILTER (WHERE e.event_type = 'alcohol') AS alcohol_event_count,
        STRING_AGG(DISTINCT e.event_type, ',' ORDER BY e.event_type) AS event_types
    FROM events e
    JOIN sources s ON s.source_id = e.source_id
    WHERE s.enabled
    GROUP BY e.local_date
), dates AS (
    SELECT local_date FROM observation_daily
    UNION
    SELECT local_date FROM blood_pressure_daily
    UNION
    SELECT local_date FROM sleep_daily
    UNION
    SELECT local_date FROM workout_daily
    UNION
    SELECT local_date FROM event_daily
)
SELECT
    dates.local_date,
    observations.weight_kg,
    observations.body_fat_pct,
    observations.lean_mass_kg,
    observations.body_fat_mass_kg,
    observations.skeletal_muscle_mass_kg,
    observations.body_water_mass_kg,
    observations.bone_mass_kg,
    pressure.systolic_mmhg,
    pressure.diastolic_mmhg,
    pressure.blood_pressure_pulse_bpm,
    pressure.blood_pressure_session_count,
    pressure.blood_pressure_reading_count,
    sleep.total_sleep_minutes,
    sleep.time_in_bed_minutes,
    sleep.awake_minutes,
    sleep.light_sleep_minutes,
    sleep.deep_sleep_minutes,
    sleep.rem_sleep_minutes,
    sleep.sleep_efficiency_pct,
    COALESCE(observations.resting_hr_bpm, sleep.sleep_resting_hr_bpm) AS resting_hr_bpm,
    COALESCE(observations.hrv_rmssd_ms, sleep.sleep_hrv_rmssd_ms) AS hrv_rmssd_ms,
    sleep.respiratory_rate,
    sleep.sleep_score,
    observations.steps,
    observations.active_energy_kcal,
    workouts.resistance_minutes,
    workouts.cardio_minutes,
    workouts.workout_count,
    events.alcohol_units,
    events.alcohol_event_count,
    events.event_types
FROM dates
LEFT JOIN observation_daily observations USING (local_date)
LEFT JOIN blood_pressure_daily pressure USING (local_date)
LEFT JOIN sleep_daily sleep USING (local_date)
LEFT JOIN workout_daily workouts USING (local_date)
LEFT JOIN event_daily events USING (local_date);
