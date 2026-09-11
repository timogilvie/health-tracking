from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from health.config import HealthSettings, load_project_config
from health.db import connect, migrate
from health.transforms import source_priority_rows, sync_source_priorities


def _source(connection, name: str, *, enabled: bool = True):
    return connection.execute(
        """
        INSERT INTO sources (name, source_type, enabled)
        VALUES (?, 'fixture', ?)
        RETURNING source_id
        """,
        [name, enabled],
    ).fetchone()[0]


def _observation(
    connection,
    source_id,
    *,
    record_id: str,
    metric: str,
    observed_at: str,
    value: float,
    unit: str,
    quality: str = "valid",
) -> None:
    connection.execute(
        """
        INSERT INTO observations (
            metric, observed_at, value, unit, source_id, source_record_id,
            quality, timezone, local_date, raw_file, transform_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'America/New_York',
                  CAST(timezone('America/New_York', ?::TIMESTAMPTZ) AS DATE), ?, 'fixture-v1')
        """,
        [
            metric,
            observed_at,
            value,
            unit,
            source_id,
            record_id,
            quality,
            observed_at,
            f"raw/{record_id}.json",
        ],
    )


def _blood_pressure(
    connection,
    source_id,
    *,
    record_id: str,
    measured_at: str,
    systolic: float,
    diastolic: float,
    pulse: float,
) -> None:
    connection.execute(
        """
        INSERT INTO blood_pressure (
            measured_at, local_date, systolic_mmhg, diastolic_mmhg, pulse_bpm,
            source_id, source_record_id, quality, raw_file, transform_version
        ) VALUES (
            ?, CAST(timezone('America/New_York', ?::TIMESTAMPTZ) AS DATE),
            ?, ?, ?, ?, ?, 'valid', ?, 'fixture-v1'
        )
        """,
        [
            measured_at,
            measured_at,
            systolic,
            diastolic,
            pulse,
            source_id,
            record_id,
            f"raw/{record_id}.json",
        ],
    )


@pytest.fixture
def database(tmp_path: Path, project_root: Path) -> Path:
    path = tmp_path / "health.duckdb"
    migrate(path, project_root / "sql")
    config = load_project_config(HealthSettings(project_root=project_root, _env_file=None))
    sync_source_priorities(path, config)
    return path


def test_priority_configuration_is_validated_and_synced(
    database: Path,
    project_root: Path,
) -> None:
    config = load_project_config(HealthSettings(project_root=project_root, _env_file=None))
    rows = source_priority_rows(config)

    assert ("weight_kg", "withings", 1) in rows
    assert ("sleep", "oura", 1) in rows
    assert ("workouts", "manual", 1) in rows
    assert sync_source_priorities(database, config) == len(rows)
    with connect(database, read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM source_priorities").fetchone()[0] == len(
            rows
        )

    invalid = {"source_priority": {"metrics": {"weight_kg": ["withings", "withings"]}}}
    with pytest.raises(ValueError, match="contains duplicates"):
        source_priority_rows(invalid)


def test_canonical_views_and_daily_rollup_select_without_deleting(database: Path) -> None:
    with connect(database) as connection:
        withings = _source(connection, "withings")
        apple = _source(connection, "apple_health")
        oura = _source(connection, "oura")
        eight_sleep = _source(connection, "eight_sleep")
        manual = _source(connection, "manual")
        disabled = _source(connection, "disabled", enabled=False)

        _observation(
            connection,
            withings,
            record_id="withings-weight-1",
            metric="weight_kg",
            observed_at="2026-09-10T12:00:00Z",
            value=80,
            unit="kg",
        )
        _observation(
            connection,
            withings,
            record_id="withings-weight-2",
            metric="weight_kg",
            observed_at="2026-09-10T13:00:00Z",
            value=79,
            unit="kg",
        )
        _observation(
            connection,
            apple,
            record_id="apple-weight",
            metric="weight_kg",
            observed_at="2026-09-10T14:00:00Z",
            value=81,
            unit="kg",
        )
        _observation(
            connection,
            disabled,
            record_id="disabled-weight",
            metric="weight_kg",
            observed_at="2026-09-10T15:00:00Z",
            value=70,
            unit="kg",
        )
        _observation(
            connection,
            withings,
            record_id="suspect-weight",
            metric="weight_kg",
            observed_at="2026-09-11T12:00:00Z",
            value=78,
            unit="kg",
            quality="suspect",
        )
        _observation(
            connection,
            apple,
            record_id="valid-weight",
            metric="weight_kg",
            observed_at="2026-09-11T13:00:00Z",
            value=77,
            unit="kg",
        )
        for record_id, value in (("oura-steps-1", 1000), ("oura-steps-2", 2000)):
            _observation(
                connection,
                oura,
                record_id=record_id,
                metric="steps",
                observed_at="2026-09-10T16:00:00Z",
                value=value,
                unit="count",
            )
        _observation(
            connection,
            apple,
            record_id="apple-steps",
            metric="steps",
            observed_at="2026-09-10T16:00:00Z",
            value=9000,
            unit="count",
        )

        for index, (measured_at, systolic, diastolic, pulse) in enumerate(
            (
                ("2026-09-10T12:00:00Z", 140, 90, 65),
                ("2026-09-10T12:05:00Z", 130, 80, 60),
                ("2026-09-10T12:20:00Z", 120, 75, 58),
            ),
            start=1,
        ):
            _blood_pressure(
                connection,
                withings,
                record_id=f"withings-bp-{index}",
                measured_at=measured_at,
                systolic=systolic,
                diastolic=diastolic,
                pulse=pulse,
            )
        _blood_pressure(
            connection,
            apple,
            record_id="apple-bp",
            measured_at="2026-09-10T12:03:00Z",
            systolic=110,
            diastolic=70,
            pulse=55,
        )

        connection.execute(
            """
            INSERT INTO sleep_sessions (
                sleep_date, started_at, ended_at, time_in_bed_seconds,
                total_sleep_seconds, awake_seconds, light_seconds, deep_seconds,
                rem_seconds, efficiency_pct, resting_hr_bpm, average_hrv_rmssd_ms,
                respiratory_rate, sleep_score, source_id, source_record_id,
                raw_file, transform_version
            ) VALUES
                ('2026-09-10', '2026-09-10T03:00:00Z', '2026-09-10T11:00:00Z',
                 28800, 25200, 3600, 12600, 7200, 5400, 87.5, 52, 45, 14, 82,
                 ?, 'oura-sleep', 'raw/oura-sleep.json', 'fixture-v1'),
                ('2026-09-10', '2026-09-10T03:00:00Z', '2026-09-10T11:00:00Z',
                 28800, 27000, 1800, 14400, 7200, 5400, 93.75, 50, 48, 13, 90,
                 ?, 'eight-sleep', 'raw/eight-sleep.json', 'fixture-v1')
            """,
            [oura, eight_sleep],
        )

        connection.execute(
            """
            INSERT INTO workouts (
                started_at, ended_at, local_date, workout_type, duration_seconds,
                source_id, source_record_id, raw_file, transform_version
            ) VALUES
                ('2026-09-10T20:00:00Z', '2026-09-10T20:55:00Z', '2026-09-10',
                 'resistance', 3300, ?, 'manual-lift', 'raw/manual-lift.json', 'fixture-v1'),
                ('2026-09-10T20:00:00Z', '2026-09-10T20:45:00Z', '2026-09-10',
                 'resistance', 2700, ?, 'oura-lift', 'raw/oura-lift.json', 'fixture-v1'),
                ('2026-09-10T15:00:00Z', '2026-09-10T15:30:00Z', '2026-09-10',
                 'running', 1800, ?, 'oura-run', 'raw/oura-run.json', 'fixture-v1'),
                ('2026-09-10T15:00:00Z', '2026-09-10T15:35:00Z', '2026-09-10',
                 'running', 2100, ?, 'apple-run', 'raw/apple-run.json', 'fixture-v1')
            """,
            [manual, oura, oura, apple],
        )
        connection.execute(
            """
            INSERT INTO events (
                event_type, started_at, local_date, value, unit, source_id,
                source_record_id, raw_file, transform_version
            ) VALUES ('alcohol', '2026-09-10T23:00:00Z', '2026-09-10', 2, 'drink', ?,
                      'alcohol-1', 'raw/alcohol.json', 'fixture-v1')
            """,
            [manual],
        )

    with connect(database, read_only=True) as connection:
        canonical_weight = connection.execute(
            """
            SELECT canonical_date, source_name, value
            FROM canonical_observations
            WHERE metric = 'weight_kg'
            ORDER BY canonical_date, observed_at
            """
        ).fetchall()
        sessions = connection.execute(
            """
            SELECT reading_count, first_systolic_mmhg, subsequent_systolic_mmhg,
                   preferred_systolic_mmhg
            FROM blood_pressure_sessions
            ORDER BY session_started_at
            """
        ).fetchall()
        selected_workouts = connection.execute(
            "SELECT source_name, workout_type, duration_seconds FROM canonical_workouts "
            "ORDER BY workout_type"
        ).fetchall()
        daily = connection.execute(
            "SELECT * FROM daily_health WHERE local_date = '2026-09-10'"
        ).fetchone()
        columns = [column[0] for column in connection.description]
        base_counts = (
            connection.execute("SELECT count(*) FROM observations").fetchone()[0],
            connection.execute("SELECT count(*) FROM blood_pressure").fetchone()[0],
            connection.execute("SELECT count(*) FROM workouts").fetchone()[0],
        )

    assert canonical_weight == [
        (date(2026, 9, 10), "withings", 80.0),
        (date(2026, 9, 10), "withings", 79.0),
        (date(2026, 9, 11), "apple_health", 77.0),
    ]
    assert [str(row[0]) for row in canonical_weight] == [
        "2026-09-10",
        "2026-09-10",
        "2026-09-11",
    ]
    assert sessions == [
        (2, 140.0, 130.0, 130.0),
        (1, 120.0, None, 120.0),
    ]
    assert selected_workouts == [
        ("manual", "resistance", 3300),
        ("apple_health", "running", 2100),
    ]
    row = dict(zip(columns, daily, strict=True))
    assert row["weight_kg"] == 79.0
    assert row["steps"] == 3000.0
    assert row["systolic_mmhg"] == 125.0
    assert row["diastolic_mmhg"] == 77.5
    assert row["blood_pressure_session_count"] == 2
    assert row["blood_pressure_reading_count"] == 3
    assert row["total_sleep_minutes"] == 420.0
    assert row["resting_hr_bpm"] == 52.0
    assert row["hrv_rmssd_ms"] == 45.0
    assert row["resistance_minutes"] == 55.0
    assert row["cardio_minutes"] == 35.0
    assert row["alcohol_units"] == 2.0
    assert row["event_types"] == "alcohol"
    assert base_counts == (9, 4, 4)
