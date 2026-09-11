CREATE VIEW health_calendar AS
WITH bounds AS (
    SELECT MIN(local_date) AS first_date, MAX(local_date) AS last_date
    FROM daily_health
), calendar AS (
    SELECT CAST(day AS DATE) AS local_date
    FROM bounds,
         LATERAL generate_series(first_date, last_date, INTERVAL '1 day') AS days(day)
)
SELECT
    calendar.local_date,
    daily.* EXCLUDE (local_date)
FROM calendar
LEFT JOIN daily_health daily USING (local_date);

CREATE VIEW weekly_health AS
SELECT
    CAST(date_trunc('week', local_date) AS DATE) AS week_start,
    CAST(date_trunc('week', local_date) + INTERVAL '6 days' AS DATE) AS week_end,
    COUNT(*) AS calendar_days,
    AVG(weight_kg) AS weight_kg_avg,
    COUNT(weight_kg) AS weight_days,
    AVG(systolic_mmhg) AS systolic_mmhg_avg,
    AVG(diastolic_mmhg) AS diastolic_mmhg_avg,
    COUNT(systolic_mmhg) AS blood_pressure_days,
    AVG(hrv_rmssd_ms) AS hrv_rmssd_ms_avg,
    COUNT(hrv_rmssd_ms) AS hrv_days,
    AVG(resting_hr_bpm) AS resting_hr_bpm_avg,
    COUNT(resting_hr_bpm) AS resting_hr_days,
    AVG(total_sleep_minutes) AS total_sleep_minutes_avg,
    COUNT(total_sleep_minutes) AS sleep_days,
    SUM(steps) AS steps_total,
    AVG(steps) AS steps_daily_avg,
    COUNT(steps) AS steps_days,
    SUM(resistance_minutes) AS resistance_minutes_total,
    SUM(cardio_minutes) AS cardio_minutes_total,
    SUM(
        COALESCE(resistance_minutes, 0) + COALESCE(cardio_minutes, 0)
    ) FILTER (
        WHERE resistance_minutes IS NOT NULL OR cardio_minutes IS NOT NULL
    ) AS exercise_minutes_total,
    SUM(workout_count) AS workout_count,
    COUNT(*) FILTER (
        WHERE resistance_minutes IS NOT NULL OR cardio_minutes IS NOT NULL
    ) AS exercise_days
FROM health_calendar
GROUP BY date_trunc('week', local_date);

CREATE VIEW rolling_health AS
WITH windows(window_days) AS (
    VALUES (7), (30), (90), (365)
), anchors AS (
    SELECT local_date AS anchor_date FROM health_calendar
)
SELECT
    anchors.anchor_date,
    windows.window_days,
    anchors.anchor_date - (windows.window_days - 1) AS window_start,
    anchors.anchor_date AS window_end,
    COUNT(*) AS calendar_days,
    AVG(metrics.weight_kg) AS weight_kg_avg,
    COUNT(metrics.weight_kg) AS weight_days,
    AVG(metrics.systolic_mmhg) AS systolic_mmhg_avg,
    AVG(metrics.diastolic_mmhg) AS diastolic_mmhg_avg,
    COUNT(metrics.systolic_mmhg) AS blood_pressure_days,
    AVG(metrics.hrv_rmssd_ms) AS hrv_rmssd_ms_avg,
    COUNT(metrics.hrv_rmssd_ms) AS hrv_days,
    AVG(metrics.resting_hr_bpm) AS resting_hr_bpm_avg,
    COUNT(metrics.resting_hr_bpm) AS resting_hr_days,
    AVG(metrics.total_sleep_minutes) AS total_sleep_minutes_avg,
    COUNT(metrics.total_sleep_minutes) AS sleep_days,
    SUM(metrics.steps) AS steps_total,
    AVG(metrics.steps) AS steps_daily_avg,
    COUNT(metrics.steps) AS steps_days,
    SUM(metrics.resistance_minutes) AS resistance_minutes_total,
    SUM(metrics.cardio_minutes) AS cardio_minutes_total,
    SUM(
        COALESCE(metrics.resistance_minutes, 0) + COALESCE(metrics.cardio_minutes, 0)
    ) FILTER (
        WHERE metrics.resistance_minutes IS NOT NULL
           OR metrics.cardio_minutes IS NOT NULL
    ) AS exercise_minutes_total,
    SUM(metrics.workout_count) AS workout_count,
    COUNT(*) FILTER (
        WHERE metrics.resistance_minutes IS NOT NULL
           OR metrics.cardio_minutes IS NOT NULL
    ) AS exercise_days
FROM anchors
CROSS JOIN windows
JOIN health_calendar metrics
  ON metrics.local_date BETWEEN
     anchors.anchor_date - (windows.window_days - 1)
     AND anchors.anchor_date
GROUP BY anchors.anchor_date, windows.window_days;

CREATE VIEW rolling_health_latest AS
SELECT *
FROM rolling_health
WHERE anchor_date = (SELECT MAX(anchor_date) FROM rolling_health);
