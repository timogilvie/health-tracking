from __future__ import annotations

from datetime import UTC, date, datetime, time
from pathlib import Path
from shutil import copytree

import pytest
from typer.testing import CliRunner

import health.cli
from health.cli import app
from health.config import HealthSettings, load_project_config
from health.db import connect, migrate
from health.ingestion import DuckDBCanonicalSink, IngestionRunner, RawStore
from health.manual import ManualEventError, build_manual_event, record_manual_event
from health.transforms import source_priority_rows, sync_source_priorities

NOW = datetime(2026, 9, 11, 2, 15, tzinfo=UTC)
runner = CliRunner()


def _initialized(tmp_path: Path, project_root: Path):
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    config = load_project_config(HealthSettings(project_root=project_root))
    source_priority_rows(config)
    sync_source_priorities(database, config)
    raw_store = RawStore(tmp_path / "raw")
    ingestion = IngestionRunner(
        database=database,
        raw_store=raw_store,
        sink=DuckDBCanonicalSink(database),
    )
    return database, raw_store, ingestion


def test_builds_local_dated_alcohol_event() -> None:
    document = build_manual_event(
        event_type="alcohol",
        timezone_name="America/New_York",
        event_date=date(2026, 9, 10),
        event_time=time(20, 30),
        value=4,
        now=NOW,
    )

    assert document.local_date == date(2026, 9, 10)
    assert document.started_at.isoformat() == "2026-09-10T20:30:00-04:00"
    assert document.value == 4
    assert document.unit == "drinks"


def test_rejects_unknown_types_and_nonpositive_alcohol() -> None:
    with pytest.raises(ManualEventError, match="event type"):
        build_manual_event(event_type="mystery", timezone_name="UTC", now=NOW)
    with pytest.raises(ManualEventError, match="positive drink count"):
        build_manual_event(
            event_type="alcohol", timezone_name="UTC", value=0, now=NOW
        )


def test_event_is_raw_first_idempotent_and_participates_in_daily_health(
    tmp_path: Path, project_root: Path
) -> None:
    database, raw_store, ingestion = _initialized(tmp_path, project_root)
    document = build_manual_event(
        event_type="alcohol",
        timezone_name="America/New_York",
        event_date=date(2026, 9, 10),
        event_time=time(20),
        value=4,
        notes="synthetic",
        now=NOW,
    )

    first = record_manual_event(document, raw_store=raw_store, runner=ingestion)
    second = record_manual_event(document, raw_store=raw_store, runner=ingestion)

    assert first.inserted_count == 1
    assert second.duplicate_count == 1
    assert len(list(raw_store.iter_refs("manual"))) == 2
    with connect(database, read_only=True) as connection:
        event = connection.execute(
            "SELECT event_type, value, unit, notes FROM events"
        ).fetchone()
        daily = connection.execute(
            "SELECT alcohol_units, alcohol_event_count FROM daily_health"
        ).fetchone()
    assert event == ("alcohol", 4.0, "drinks", "synthetic")
    assert daily == (4.0, 1)


def test_cli_supports_alcohol_shorthand_and_general_event(
    tmp_path: Path, project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    assert runner.invoke(app, ["init", "--root", str(tmp_path)]).exit_code == 0
    monkeypatch.setattr(health.cli, "current_time", lambda: NOW)

    alcohol = runner.invoke(
        app,
        ["alcohol", "4", "--date", "2026-09-10", "--root", str(tmp_path)],
    )
    illness = runner.invoke(
        app,
        [
            "event",
            "add",
            "--type",
            "illness",
            "--date",
            "2026-09-09",
            "--duration-hours",
            "48",
            "--notes",
            "synthetic",
            "--root",
            str(tmp_path),
        ],
    )

    assert alcohol.exit_code == 0, alcohol.output
    assert "PASS Manual event: type=alcohol" in alcohol.output
    assert illness.exit_code == 0, illness.output
    assert "PASS Manual event: type=illness" in illness.output
    with connect(tmp_path / "data" / "health.duckdb", read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 2
