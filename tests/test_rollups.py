from datetime import date
from pathlib import Path

from health.config import HealthSettings, load_project_config
from health.db import connect, migrate
from health.transforms import sync_source_priorities


def test_weekly_and_calendar_day_rolling_rollups(tmp_path: Path, project_root: Path) -> None:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    config = load_project_config(HealthSettings(project_root=project_root, _env_file=None))
    sync_source_priorities(database, config)

    with connect(database) as connection:
        withings = connection.execute(
            "INSERT INTO sources (name, source_type) VALUES ('withings', 'api') RETURNING source_id"
        ).fetchone()[0]
        oura = connection.execute(
            "INSERT INTO sources (name, source_type) VALUES ('oura', 'api') RETURNING source_id"
        ).fetchone()[0]
        manual = connection.execute(
            "INSERT INTO sources (name, source_type) "
            "VALUES ('manual', 'manual') RETURNING source_id"
        ).fetchone()[0]

        observations = [
            ("weight_kg", "2026-08-31", 80, "kg", withings, "weight-1"),
            ("steps", "2026-08-31", 5000, "count", oura, "steps-1"),
            ("hrv_rmssd_ms", "2026-09-01", 42, "ms", oura, "hrv-1"),
            ("resting_hr_bpm", "2026-09-01", 54, "bpm", oura, "rhr-1"),
            ("steps", "2026-09-01", 7000, "count", oura, "steps-2"),
            ("weight_kg", "2026-09-06", 79, "kg", withings, "weight-2"),
            ("steps", "2026-09-06", 9000, "count", oura, "steps-3"),
            ("hrv_rmssd_ms", "2026-09-07", 48, "ms", oura, "hrv-2"),
            ("resting_hr_bpm", "2026-09-07", 51, "bpm", oura, "rhr-2"),
            ("steps", "2026-09-07", 8000, "count", oura, "steps-4"),
            ("weight_kg", "2026-09-10", 78, "kg", withings, "weight-3"),
            ("hrv_rmssd_ms", "2026-09-10", 50, "ms", oura, "hrv-3"),
            ("resting_hr_bpm", "2026-09-10", 50, "bpm", oura, "rhr-3"),
            ("steps", "2026-09-10", 10000, "count", oura, "steps-5"),
        ]
        connection.executemany(
            """
            INSERT INTO observations (
                metric, observed_at, value, unit, source_id, source_record_id,
                quality, timezone, local_date, raw_file, transform_version
            ) VALUES (?, CAST(? AS DATE)::TIMESTAMP AT TIME ZONE 'America/New_York',
                      ?, ?, ?, ?, 'valid', 'America/New_York', ?, ?, 'fixture-v1')
            """,
            [
                (*row, row[1], f"raw/{row[5]}.json")
                for row in observations
            ],
        )
        connection.executemany(
            """
            INSERT INTO blood_pressure (
                measured_at, local_date, systolic_mmhg, diastolic_mmhg,
                source_id, source_record_id, raw_file, transform_version
            ) VALUES (?::TIMESTAMPTZ, ?, ?, ?, ?, ?, ?, 'fixture-v1')
            """,
            [
                (
                    "2026-09-01T12:00:00Z",
                    "2026-09-01",
                    120,
                    75,
                    withings,
                    "bp-1",
                    "raw/bp-1.json",
                ),
                (
                    "2026-09-10T12:00:00Z",
                    "2026-09-10",
                    116,
                    72,
                    withings,
                    "bp-2",
                    "raw/bp-2.json",
                ),
            ],
        )
        connection.executemany(
            """
            INSERT INTO sleep_sessions (
                sleep_date, started_at, ended_at, total_sleep_seconds,
                source_id, source_record_id, raw_file, transform_version
            ) VALUES (?, ?::TIMESTAMPTZ, ?::TIMESTAMPTZ, ?, ?, ?, ?, 'fixture-v1')
            """,
            [
                (
                    "2026-09-01",
                    "2026-09-01T03:00:00Z",
                    "2026-09-01T10:00:00Z",
                    25200,
                    oura,
                    "sleep-1",
                    "raw/sleep-1.json",
                ),
                (
                    "2026-09-07",
                    "2026-09-07T03:00:00Z",
                    "2026-09-07T10:30:00Z",
                    27000,
                    oura,
                    "sleep-2",
                    "raw/sleep-2.json",
                ),
                (
                    "2026-09-10",
                    "2026-09-10T03:00:00Z",
                    "2026-09-10T11:00:00Z",
                    28800,
                    oura,
                    "sleep-3",
                    "raw/sleep-3.json",
                ),
            ],
        )
        connection.executemany(
            """
            INSERT INTO workouts (
                started_at, ended_at, local_date, workout_type,
                duration_seconds, source_id, source_record_id,
                raw_file, transform_version
            ) VALUES (?::TIMESTAMPTZ, ?::TIMESTAMPTZ, ?, ?, ?, ?, ?, ?, 'fixture-v1')
            """,
            [
                (
                    "2026-08-31T20:00:00Z",
                    "2026-08-31T21:00:00Z",
                    "2026-08-31",
                    "resistance",
                    3600,
                    manual,
                    "lift-1",
                    "raw/lift-1.json",
                ),
                (
                    "2026-09-08T20:00:00Z",
                    "2026-09-08T20:55:00Z",
                    "2026-09-08",
                    "resistance",
                    3300,
                    manual,
                    "lift-2",
                    "raw/lift-2.json",
                ),
                (
                    "2026-09-10T12:00:00Z",
                    "2026-09-10T12:30:00Z",
                    "2026-09-10",
                    "running",
                    1800,
                    manual,
                    "run-1",
                    "raw/run-1.json",
                ),
            ],
        )

    with connect(database, read_only=True) as connection:
        calendar = connection.execute(
            "SELECT count(*), count(weight_kg) FROM health_calendar"
        ).fetchone()
        gap = connection.execute(
            "SELECT weight_kg, steps FROM health_calendar WHERE local_date = '2026-09-04'"
        ).fetchone()
        weeks = connection.execute(
            """
            SELECT week_start, week_end, calendar_days, weight_kg_avg,
                   steps_total, exercise_minutes_total, exercise_days
            FROM weekly_health ORDER BY week_start
            """
        ).fetchall()
        latest = connection.execute(
            """
            SELECT window_days, window_start, window_end, calendar_days,
                   weight_kg_avg, systolic_mmhg_avg, hrv_rmssd_ms_avg,
                   resting_hr_bpm_avg, total_sleep_minutes_avg,
                   steps_total, steps_daily_avg, exercise_minutes_total
            FROM rolling_health_latest ORDER BY window_days
            """
        ).fetchall()

    assert calendar == (11, 3)
    assert gap == (None, None)
    assert weeks == [
        (date(2026, 8, 31), date(2026, 9, 6), 7, 79.5, 21000.0, 60.0, 1),
        (date(2026, 9, 7), date(2026, 9, 13), 4, 78.0, 18000.0, 85.0, 2),
    ]
    assert [row[0] for row in latest] == [7, 30, 90, 365]
    seven = latest[0]
    assert seven == (
        7,
        date(2026, 9, 4),
        date(2026, 9, 10),
        7,
        78.5,
        116.0,
        49.0,
        50.5,
        465.0,
        27000.0,
        9000.0,
        85.0,
    )
    assert latest[1][1] == date(2026, 8, 12)
    assert latest[1][3] == 11
    assert latest[2][3] == 11
    assert latest[3][3] == 11
