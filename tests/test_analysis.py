from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from shutil import copytree

from typer.testing import CliRunner

import health.cli
from health.analysis import analyze, render_html, write_report
from health.cli import app
from health.db import connect, migrate

NOW = datetime(2026, 9, 11, 12, tzinfo=UTC)
runner = CliRunner()


def _seed(database: Path) -> None:
    with connect(database) as connection:
        withings = connection.execute(
            """
            INSERT INTO sources (name, source_type)
            VALUES ('withings', 'import') RETURNING source_id
            """
        ).fetchone()[0]
        oura = connection.execute(
            "INSERT INTO sources (name, source_type) VALUES ('oura', 'api') RETURNING source_id"
        ).fetchone()[0]
        manual = connection.execute(
            """
            INSERT INTO sources (name, source_type)
            VALUES ('manual', 'manual') RETURNING source_id
            """
        ).fetchone()[0]
        alcohol = [0, 1, 3, 0, 2, 0]
        for index in range(6):
            day = date(2026, 9, 1) + timedelta(days=index)
            observed_at = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(
                hours=8
            )
            for metric, value, unit in (
                ("weight_kg", 80 + index, "kg"),
                ("resting_hr_bpm", 50 + alcohol[index - 1] if index else 50, "bpm"),
                ("hrv_rmssd_ms", 55 - (alcohol[index - 1] * 2 if index else 0), "ms"),
            ):
                source = withings if metric == "weight_kg" else oura
                connection.execute(
                    """
                    INSERT INTO observations (
                        metric, observed_at, value, unit, source_id, source_record_id,
                        local_date, raw_file, transform_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'fixture', 'test-v1')
                    """,
                    [metric, observed_at, value, unit, source, f"{metric}-{day}", day],
                )
            previous_alcohol = alcohol[index - 1] if index else 0
            sleep_end = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=7)
            sleep_seconds = (480 - previous_alcohol * 20) * 60
            connection.execute(
                """
                INSERT INTO sleep_sessions (
                    sleep_date, started_at, ended_at, total_sleep_seconds,
                    source_id, source_record_id, raw_file, transform_version
                ) VALUES (?, ?, ?, ?, ?, ?, 'fixture', 'test-v1')
                """,
                [
                    day,
                    sleep_end - timedelta(seconds=sleep_seconds),
                    sleep_end,
                    sleep_seconds,
                    oura,
                    f"sleep-{day}",
                ],
            )
            connection.execute(
                """
                INSERT INTO blood_pressure (
                    measured_at, local_date, systolic_mmhg, diastolic_mmhg,
                    source_id, source_record_id, raw_file, transform_version
                ) VALUES (?, ?, ?, ?, ?, ?, 'fixture', 'test-v1')
                """,
                [observed_at, day, 118 + index, 72 + index, withings, f"bp-{day}"],
            )
            workout_end = observed_at + timedelta(minutes=index * 10)
            connection.execute(
                """
                INSERT INTO workouts (
                    started_at, ended_at, local_date, workout_type, duration_seconds,
                    source_id, source_record_id, raw_file, transform_version
                ) VALUES (?, ?, ?, 'resistance', ?, ?, ?, 'fixture', 'test-v1')
                """,
                [
                    observed_at,
                    workout_end,
                    day,
                    index * 600,
                    manual,
                    f"workout-{day}",
                ],
            )
            if alcohol[index]:
                connection.execute(
                    """
                    INSERT INTO events (
                        event_type, started_at, local_date, value, unit,
                        source_id, source_record_id, raw_file, transform_version
                    ) VALUES ('alcohol', ?, ?, ?, 'drinks', ?, ?, 'fixture', 'test-v1')
                    """,
                    [observed_at, day, alcohol[index], manual, f"alcohol-{day}"],
                )


def _database(tmp_path: Path, project_root: Path) -> Path:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    _seed(database)
    return database


def test_lagged_analysis_reports_samples_correlations_and_uncertainty(
    tmp_path: Path, project_root: Path
) -> None:
    report = analyze(_database(tmp_path, project_root), now=NOW)
    by_slug = {relationship.slug: relationship for relationship in report.relationships}

    assert len(report.relationships) == 11
    assert by_slug["alcohol-sleep"].sample_size == 5
    assert by_slug["alcohol-sleep"].correlation == -1.0
    assert by_slug["alcohol-sleep"].confidence_low is not None
    assert by_slug["weight-systolic"].sample_size == 6
    assert by_slug["weight-systolic"].x_unit == "lb"
    assert by_slug["exercise-hrv"].sample_size == 5


def test_html_has_inline_plots_sample_sizes_and_guardrails(
    tmp_path: Path, project_root: Path
) -> None:
    report = analyze(_database(tmp_path, project_root), now=NOW)
    rendered = render_html(report)
    target = write_report(report, tmp_path / "analysis.html")

    assert "Descriptive—not causal or diagnostic" in rendered
    assert "Unlogged alcohol is treated as zero" in rendered
    assert "n=5" in rendered
    assert rendered.count("<svg") == 11
    assert "http://" not in rendered and "https://" not in rendered
    assert target.stat().st_mode & 0o777 == 0o600


def test_cli_writes_private_report_to_requested_path(
    tmp_path: Path, project_root: Path, monkeypatch
) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    assert runner.invoke(app, ["init", "--root", str(tmp_path)]).exit_code == 0
    _seed(tmp_path / "data" / "health.duckdb")
    monkeypatch.setattr(health.cli, "current_time", lambda: NOW)
    output = tmp_path / "data" / "exports" / "report.html"

    result = runner.invoke(
        app,
        ["analyze", "--output", str(output), "--root", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    assert "relationships=11 estimable=11" in result.output
    assert output.is_file()
