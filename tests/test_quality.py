from datetime import UTC, datetime
from pathlib import Path
from shutil import copytree

import pytest
from typer.testing import CliRunner

import health.cli as health_cli
from health.cli import app
from health.db import connect, migrate
from health.quality import quality_report


def seed_quality(database: Path) -> None:
    with connect(database) as connection:
        apple = connection.execute(
            "INSERT INTO sources (name, source_type) "
            "VALUES ('apple_health', 'file') RETURNING source_id"
        ).fetchone()[0]
        withings = connection.execute(
            "INSERT INTO sources (name, source_type) "
            "VALUES ('withings', 'file') RETURNING source_id"
        ).fetchone()[0]
        observations = connection.execute(
            """
            INSERT INTO observations (
                metric, observed_at, value, unit, source_id, source_record_id,
                local_date, quality, raw_file, transform_version
            ) VALUES
                ('steps', '2026-09-10T12:00:00Z', 1, 'count', ?, 'steps-1',
                 '2026-09-10', 'valid', 'raw/a', 'fixture'),
                ('steps', '2026-09-12T12:00:00Z', 1, 'count', ?, 'steps-2',
                 '2026-09-12', 'suspect', 'raw/b', 'fixture'),
                ('resting_hr_bpm', '2026-09-13T12:00:00Z', 1, 'bpm', ?, 'hr-1',
                 NULL, 'invalid', 'raw/c', 'fixture'),
                ('weight_kg', '2026-08-01T12:00:00Z', 1, 'kg', ?, 'weight-1',
                 '2026-08-01', 'valid', 'raw/d', 'fixture')
            RETURNING observation_id
            """,
            [apple, apple, apple, withings],
        ).fetchall()
        connection.execute(
            """
            INSERT INTO blood_pressure (
                measured_at, local_date, systolic_mmhg, diastolic_mmhg,
                source_id, source_record_id, quality, raw_file, transform_version
            ) VALUES ('2026-09-01T12:00:00Z', '2026-09-01', 1, 1,
                      ?, 'bp-1', 'invalid', 'raw/e', 'fixture')
            """,
            [withings],
        )
        connection.execute(
            """
            INSERT INTO sleep_sessions (
                sleep_date, started_at, ended_at, total_sleep_seconds,
                source_id, source_record_id, raw_file, transform_version
            ) VALUES ('2026-09-13', '2026-09-12T00:00:00Z',
                      '2026-09-13T01:00:00Z', 90000, ?, 'sleep-1', 'raw/f', 'fixture')
            """,
            [withings],
        )
        connection.execute(
            """
            INSERT INTO events (
                event_type, started_at, local_date, value, unit, source_id,
                source_record_id, raw_file, transform_version
            ) VALUES ('alcohol', '2026-09-10T23:00:00Z', '2026-09-10', 1, 'drink',
                      ?, 'event-1', 'raw/g', 'fixture')
            """,
            [apple],
        )
        connection.execute(
            """
            INSERT INTO duplicate_links (
                record_type, canonical_record_id, duplicate_record_id,
                match_method, confidence
            ) VALUES ('observation', ?, ?, 'fixture', 0.9)
            """,
            [observations[0][0], observations[1][0]],
        )
        connection.execute(
            """
            INSERT INTO ingestion_runs (
                source_id, started_at, finished_at, normalized_count,
                inserted_count, duplicate_count, status
            ) VALUES
                (?, '2026-09-12T10:00:00Z', '2026-09-12T10:00:20Z',
                 50, 50, 0, 'succeeded'),
                (?, '2026-09-13T10:00:00Z', '2026-09-13T10:00:10Z',
                 100, 80, 20, 'succeeded'),
                (?, '2026-09-14T09:00:00Z', '2026-09-14T09:00:01Z',
                 0, 0, 0, 'failed'),
                (?, '2026-09-13T08:00:00Z', '2026-09-13T08:00:00Z',
                 10, 10, 0, 'succeeded'),
                (?, '2026-09-14T08:00:00Z', NULL, 0, 0, 0, 'running')
            """,
            [apple, apple, apple, withings, withings],
        )


def test_quality_report_calculates_coverage_freshness_and_diagnostics(
    tmp_path: Path,
    project_root: Path,
) -> None:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    seed_quality(database)

    with connect(database, read_only=True) as connection:
        report = quality_report(connection, now=datetime(2026, 9, 14, 12, tzinfo=UTC))

    steps = next(row for row in report["coverage"] if row["metric"] == "steps")
    assert steps == {
        "metric": "steps",
        "source": "apple_health",
        "first_date": datetime(2026, 9, 10).date(),
        "last_date": datetime(2026, 9, 12).date(),
        "record_count": 2,
        "observed_days": 2,
        "span_days": 3,
        "cadence": "daily",
        "missing_days": 1,
        "coverage_pct": pytest.approx(66.7),
        "freshness_days": 2,
        "freshness_status": "current",
    }
    resting_hr = next(row for row in report["coverage"] if row["metric"] == "resting_hr_bpm")
    assert resting_hr["last_date"].isoformat() == "2026-09-13"
    weight = next(row for row in report["coverage"] if row["metric"] == "weight_kg")
    assert weight["cadence"] == "periodic"
    assert weight["freshness_status"] == "stale"
    event = next(row for row in report["coverage"] if row["metric"] == "event:alcohol")
    assert event["cadence"] == "episodic"
    assert event["span_days"] == 1
    assert event["missing_days"] == 0
    assert event["coverage_pct"] == 100
    assert event["freshness_status"] == "episodic"
    assert report["source_runs"][0]["source"] == "apple_health"
    assert report["source_runs"][0]["duration_seconds"] == 10
    assert report["source_runs"][0]["throughput_per_second"] == 10
    assert report["source_runs"][1]["duration_seconds"] == 0
    assert report["source_runs"][1]["throughput_per_second"] is None
    assert report["diagnostics"] == {
        "suspect_records": 1,
        "invalid_records": 2,
        "duplicate_candidates": 1,
        "duplicate_confirmed": 0,
        "duplicate_rejected": 0,
        "sleep_over_24h": 1,
        "sleep_over_16h": 1,
        "latest_failed_imports": 1,
        "latest_running_imports": 1,
    }
    assert report["status"] == "attention"


def test_quality_report_handles_empty_database(tmp_path: Path, project_root: Path) -> None:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")

    with connect(database, read_only=True) as connection:
        report = quality_report(connection, now=datetime(2026, 9, 14, tzinfo=UTC))

    assert report["status"] == "empty"
    assert report["coverage"] == []
    assert report["source_runs"] == []
    assert set(report["diagnostics"].values()) == {0}


def test_coverage_cli_defaults_to_private_counts_and_details_are_opt_in(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    runner = CliRunner()
    assert runner.invoke(app, ["init", "--root", str(tmp_path)]).exit_code == 0
    seed_quality(tmp_path / "data/health.duckdb")
    monkeypatch.setattr(
        health_cli,
        "current_time",
        lambda: datetime(2026, 9, 14, 12, tzinfo=UTC),
    )

    summary = runner.invoke(app, ["coverage", "--root", str(tmp_path)])
    details = runner.invoke(app, ["coverage", "--details", "--root", str(tmp_path)])

    assert summary.exit_code == 0, summary.output
    assert "PASS Coverage: status=attention metric_sources=6" in summary.output
    assert "2026-09-10" not in summary.output
    assert "measurement values" not in summary.output
    assert details.exit_code == 0, details.output
    assert "metric=steps source=apple_health" in details.output
    assert "first=2026-09-10 last=2026-09-12" in details.output
    assert "source=apple_health finished=2026-09-13T10:00:10+00:00" in details.output
