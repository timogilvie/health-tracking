CREATE VIEW canonical_sleep_daily AS
WITH apple_ordered AS (
    SELECT
        ss.*,
        COALESCE(sp.priority, 10000) AS source_priority,
        MAX(ss.ended_at) OVER (
            ORDER BY ss.started_at, ss.ended_at, ss.sleep_session_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS previous_max_ended_at
    FROM sleep_sessions ss
    JOIN sources s ON s.source_id = ss.source_id
    LEFT JOIN source_priorities sp
        ON sp.data_type = 'sleep'
       AND sp.source = s.name
    WHERE s.enabled
      AND s.name = 'apple_health'
), apple_boundaries AS (
    SELECT
        *,
        CASE
            WHEN previous_max_ended_at IS NULL
              OR started_at > previous_max_ended_at + INTERVAL '4 hours'
            THEN 1 ELSE 0
        END AS starts_episode
    FROM apple_ordered
), apple_episode_numbered AS (
    SELECT
        *,
        SUM(starts_episode) OVER (
            ORDER BY started_at, ended_at, sleep_session_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS episode_number
    FROM apple_boundaries
), apple_episode_rows AS (
    SELECT
        *,
        MAX(sleep_date) OVER (PARTITION BY episode_number) AS wake_date
    FROM apple_episode_numbered
), apple_dimensions AS (
    SELECT episode_number, wake_date, source_priority, 'total_sleep' AS dimension,
           started_at, ended_at, sleep_session_id
    FROM apple_episode_rows WHERE total_sleep_seconds IS NOT NULL
    UNION ALL
    SELECT episode_number, wake_date, source_priority, 'time_in_bed',
           started_at, ended_at, sleep_session_id
    FROM apple_episode_rows WHERE time_in_bed_seconds IS NOT NULL
    UNION ALL
    SELECT episode_number, wake_date, source_priority, 'awake',
           started_at, ended_at, sleep_session_id
    FROM apple_episode_rows WHERE awake_seconds IS NOT NULL
    UNION ALL
    SELECT episode_number, wake_date, source_priority, 'light',
           started_at, ended_at, sleep_session_id
    FROM apple_episode_rows WHERE light_seconds IS NOT NULL
    UNION ALL
    SELECT episode_number, wake_date, source_priority, 'deep',
           started_at, ended_at, sleep_session_id
    FROM apple_episode_rows WHERE deep_seconds IS NOT NULL
    UNION ALL
    SELECT episode_number, wake_date, source_priority, 'rem',
           started_at, ended_at, sleep_session_id
    FROM apple_episode_rows WHERE rem_seconds IS NOT NULL
), apple_dimension_ordered AS (
    SELECT
        *,
        MAX(ended_at) OVER (
            PARTITION BY episode_number, dimension
            ORDER BY started_at, ended_at, sleep_session_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS previous_max_ended_at
    FROM apple_dimensions
), apple_island_boundaries AS (
    SELECT
        *,
        CASE
            WHEN previous_max_ended_at IS NULL OR started_at > previous_max_ended_at
            THEN 1 ELSE 0
        END AS starts_island
    FROM apple_dimension_ordered
), apple_island_numbered AS (
    SELECT
        *,
        SUM(starts_island) OVER (
            PARTITION BY episode_number, dimension
            ORDER BY started_at, ended_at, sleep_session_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS island_number
    FROM apple_island_boundaries
), apple_islands AS (
    SELECT
        wake_date,
        source_priority,
        episode_number,
        dimension,
        island_number,
        MIN(started_at) AS island_started_at,
        MAX(ended_at) AS island_ended_at
    FROM apple_island_numbered
    GROUP BY wake_date, source_priority, episode_number, dimension, island_number
), apple_dimension_daily AS (
    SELECT
        wake_date AS local_date,
        source_priority,
        dimension,
        SUM(date_diff('second', island_started_at, island_ended_at)) AS seconds
    FROM apple_islands
    GROUP BY wake_date, source_priority, dimension
), apple_daily AS (
    SELECT
        local_date,
        MAX(seconds) FILTER (WHERE dimension = 'total_sleep') / 60.0
            AS total_sleep_minutes,
        MAX(seconds) FILTER (WHERE dimension = 'time_in_bed') / 60.0
            AS time_in_bed_minutes,
        MAX(seconds) FILTER (WHERE dimension = 'awake') / 60.0 AS awake_minutes,
        MAX(seconds) FILTER (WHERE dimension = 'light') / 60.0 AS light_sleep_minutes,
        MAX(seconds) FILTER (WHERE dimension = 'deep') / 60.0 AS deep_sleep_minutes,
        MAX(seconds) FILTER (WHERE dimension = 'rem') / 60.0 AS rem_sleep_minutes,
        NULL::DOUBLE AS sleep_efficiency_pct,
        NULL::DOUBLE AS sleep_resting_hr_bpm,
        NULL::DOUBLE AS sleep_hrv_rmssd_ms,
        NULL::DOUBLE AS respiratory_rate,
        NULL::DOUBLE AS sleep_score,
        'apple_health' AS source_name,
        source_priority
    FROM apple_dimension_daily
    GROUP BY local_date, source_priority
), non_apple_daily AS (
    SELECT
        ss.sleep_date AS local_date,
        SUM(ss.total_sleep_seconds) / 60.0 AS total_sleep_minutes,
        SUM(ss.time_in_bed_seconds) / 60.0 AS time_in_bed_minutes,
        SUM(ss.awake_seconds) / 60.0 AS awake_minutes,
        SUM(ss.light_seconds) / 60.0 AS light_sleep_minutes,
        SUM(ss.deep_seconds) / 60.0 AS deep_sleep_minutes,
        SUM(ss.rem_seconds) / 60.0 AS rem_sleep_minutes,
        AVG(ss.efficiency_pct) AS sleep_efficiency_pct,
        ARG_MAX(ss.resting_hr_bpm, COALESCE(ss.total_sleep_seconds, 0))
            AS sleep_resting_hr_bpm,
        ARG_MAX(ss.average_hrv_rmssd_ms, COALESCE(ss.total_sleep_seconds, 0))
            AS sleep_hrv_rmssd_ms,
        ARG_MAX(ss.respiratory_rate, COALESCE(ss.total_sleep_seconds, 0))
            AS respiratory_rate,
        ARG_MAX(ss.sleep_score, COALESCE(ss.total_sleep_seconds, 0)) AS sleep_score,
        s.name AS source_name,
        COALESCE(sp.priority, 10000) AS source_priority
    FROM sleep_sessions ss
    JOIN sources s ON s.source_id = ss.source_id
    LEFT JOIN source_priorities sp
        ON sp.data_type = 'sleep'
       AND sp.source = s.name
    WHERE s.enabled
      AND s.name <> 'apple_health'
    GROUP BY ss.sleep_date, s.name, COALESCE(sp.priority, 10000)
), candidates AS (
    SELECT * FROM apple_daily
    UNION ALL
    SELECT * FROM non_apple_daily
), ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY local_date
            ORDER BY source_priority, source_name
        ) AS selection_rank
    FROM candidates
)
SELECT * EXCLUDE (selection_rank)
FROM ranked
WHERE selection_rank = 1;

CREATE OR REPLACE VIEW daily_health AS
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
    SELECT local_date FROM canonical_sleep_daily
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
LEFT JOIN canonical_sleep_daily sleep USING (local_date)
LEFT JOIN workout_daily workouts USING (local_date)
LEFT JOIN event_daily events USING (local_date);
