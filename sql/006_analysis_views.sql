CREATE VIEW analysis_daily_lagged AS
SELECT
    current_day.local_date AS predictor_date,
    COALESCE(current_day.alcohol_units, 0) AS alcohol_units,
    COALESCE(current_day.resistance_minutes, 0)
        + COALESCE(current_day.cardio_minutes, 0) AS exercise_minutes,
    current_day.weight_kg * 2.2046226218487757 AS weight_lb,
    current_day.total_sleep_minutes AS wake_date_sleep_minutes,
    current_day.hrv_rmssd_ms AS wake_date_hrv_rmssd_ms,
    current_day.resting_hr_bpm AS wake_date_resting_hr_bpm,
    current_day.systolic_mmhg AS same_day_systolic_mmhg,
    current_day.diastolic_mmhg AS same_day_diastolic_mmhg,
    next_day.local_date AS outcome_date,
    next_day.total_sleep_minutes AS next_day_total_sleep_minutes,
    next_day.hrv_rmssd_ms AS next_day_hrv_rmssd_ms,
    next_day.resting_hr_bpm AS next_day_resting_hr_bpm,
    next_day.systolic_mmhg AS next_day_systolic_mmhg,
    next_day.diastolic_mmhg AS next_day_diastolic_mmhg
FROM health_calendar current_day
LEFT JOIN health_calendar next_day
  ON next_day.local_date = current_day.local_date + 1;
