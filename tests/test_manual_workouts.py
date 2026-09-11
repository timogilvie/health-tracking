import json
from datetime import UTC, datetime
from pathlib import Path
from shutil import copytree

import pytest
from typer.testing import CliRunner

import health.cli as health_cli
from health.cli import app
from health.db import connect
from health.ingestion import RawStore

runner = CliRunner()
NOW = datetime(2026, 9, 10, 22, tzinfo=UTC)


def initialize_project(tmp_path: Path, project_root: Path) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    result = runner.invoke(app, ["init", "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output


def test_lift_shorthand_is_raw_first_canonical_and_retry_safe(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_project(tmp_path, project_root)
    monkeypatch.setattr(health_cli, "current_time", lambda: NOW)
    command = [
        "lift",
        "55",
        "--focus",
        "upper body",
        "--rpe",
        "8",
        "--notes",
        "synthetic private note",
        "--root",
        str(tmp_path),
    ]

    first = runner.invoke(app, command)
    repeated = runner.invoke(app, command)

    assert first.exit_code == 0, first.output
    assert "PASS Manual workout:" in first.output
    assert "inserted=1 updated=0 duplicate=0" in first.output
    assert repeated.exit_code == 0, repeated.output
    assert "inserted=0 updated=0 duplicate=1" in repeated.output
    assert "synthetic private note" not in first.output + repeated.output
    assert "upper body" not in first.output + repeated.output

    with connect(tmp_path / "data/health.duckdb", read_only=True) as connection:
        workout = connection.execute(
            """
            SELECT s.name, s.source_type, w.workout_type, w.duration_seconds,
                   w.local_date, w.rpe, w.notes,
                   json_extract_string(w.metadata, '$.manual.focus')
            FROM workouts w JOIN sources s USING (source_id)
            """
        ).fetchone()
        daily = connection.execute(
            """
            SELECT resistance_minutes, workout_count
            FROM daily_health WHERE local_date = '2026-09-10'
            """
        ).fetchone()
    assert workout == (
        "manual",
        "manual",
        "resistance",
        3300,
        datetime(2026, 9, 10).date(),
        8.0,
        "synthetic private note",
        "upper body",
    )
    assert daily == (55.0, 1)

    raw_store = RawStore(tmp_path / "data/raw")
    refs = list(raw_store.iter_refs("manual"))
    assert len(refs) == 2
    manifests = [raw_store.manifest(ref) for ref in refs]
    assert "synthetic private note" not in json.dumps(manifests)
    assert all(raw_store.read(ref).startswith(b'{"created_at"') for ref in refs)


def test_general_workout_add_supports_explicit_local_date_time_and_notes(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_project(tmp_path, project_root)
    monkeypatch.setattr(health_cli, "current_time", lambda: NOW)

    result = runner.invoke(
        app,
        [
            "workout",
            "add",
            "--type",
            "running",
            "--minutes",
            "30",
            "--date",
            "2026-09-09",
            "--time",
            "07:15",
            "--focus",
            "easy aerobic",
            "--rpe",
            "4.5",
            "--notes",
            "synthetic morning run",
            "--root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    with connect(tmp_path / "data/health.duckdb", read_only=True) as connection:
        workout = connection.execute(
            """
            SELECT workout_type, duration_seconds, local_date, started_at,
                   ended_at, rpe, notes,
                   json_extract_string(metadata, '$.manual.focus')
            FROM workouts
            """
        ).fetchone()
    assert workout == (
        "running",
        1800,
        datetime(2026, 9, 9).date(),
        datetime(2026, 9, 9, 11, 15, tzinfo=UTC),
        datetime(2026, 9, 9, 11, 45, tzinfo=UTC),
        4.5,
        "synthetic morning run",
        "easy aerobic",
    )


def test_manual_workout_rejects_invalid_type_and_local_date(
    tmp_path: Path,
    project_root: Path,
) -> None:
    initialize_project(tmp_path, project_root)

    invalid_type = runner.invoke(
        app,
        [
            "workout",
            "add",
            "--type",
            "teleporting",
            "--minutes",
            "30",
            "--root",
            str(tmp_path),
        ],
    )
    invalid_date = runner.invoke(
        app,
        ["lift", "55", "--date", "09/10/2026", "--root", str(tmp_path)],
    )

    assert invalid_type.exit_code == 1
    assert "FAIL Manual workout: workout type must be one of:" in invalid_type.output
    assert invalid_date.exit_code == 2
    assert "must be YYYY-MM-DD" in invalid_date.output
