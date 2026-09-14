from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from shutil import copytree
from zipfile import ZipFile

import pytest
from typer.testing import CliRunner

import health.ingestion.canonical_sink as canonical_sink_module
from health.cli import app
from health.connectors.apple_health import (
    AppleHealthExportConnector,
    AppleHealthExportError,
    import_apple_health_export,
)
from health.db import connect, migrate
from health.ingestion import DuckDBCanonicalSink, IngestionRunner, RawStore

NOW = datetime(2026, 9, 10, 17, tzinfo=UTC)
runner = CliRunner()


def _fixture(project_root: Path) -> Path:
    return project_root / "tests" / "fixtures" / "providers" / "apple_health" / "export.xml"


def _ingestion(tmp_path: Path, project_root: Path):
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    store = RawStore(tmp_path / "raw")
    return database, store, IngestionRunner(
        database=database,
        raw_store=store,
        sink=DuckDBCanonicalSink(database),
    )


@pytest.mark.parametrize("archived", [False, True])
def test_streams_xml_or_zip_into_canonical_records(
    archived: bool, tmp_path: Path, project_root: Path
) -> None:
    database, store, ingestion = _ingestion(tmp_path, project_root)
    source = _fixture(project_root)
    if archived:
        export = tmp_path / "export.zip"
        with ZipFile(export, "w") as archive:
            archive.write(source, "apple_health_export/export.xml")
    else:
        export = source

    result = import_apple_health_export(
        export,
        raw_store=store,
        runner=ingestion,
        now=NOW,
    )

    assert (result.raw_count, result.normalized_count, result.inserted_count) == (1, 5, 5)
    with connect(database, read_only=True) as connection:
        observations = connection.execute(
            "SELECT metric, value, unit, original_unit, metadata FROM observations ORDER BY metric"
        ).fetchall()
        blood_pressure = connection.execute(
            "SELECT systolic_mmhg, diastolic_mmhg FROM blood_pressure"
        ).fetchone()
        sleep_seconds = connection.execute(
            "SELECT total_sleep_seconds, light_seconds FROM sleep_sessions"
        ).fetchone()
        workout = connection.execute(
            "SELECT workout_type, duration_seconds, energy_kcal FROM workouts"
        ).fetchone()
    assert [(row[0], row[2]) for row in observations] == [
        ("resting_hr_bpm", "bpm"),
        ("weight_kg", "kg"),
    ]
    assert observations[1][1] == pytest.approx(80.0, abs=0.01)
    assert observations[1][3] == "lb"
    assert '"HKWasUserEntered":"0"' in observations[1][4].replace(" ", "")
    assert blood_pressure == (118.0, 74.0)
    assert sleep_seconds == (25_200, 25_200)
    assert workout == ("resistance", 2_700, 300.0)


def test_reimport_is_record_idempotent_and_raw_files_remain_immutable(
    tmp_path: Path, project_root: Path
) -> None:
    _, store, ingestion = _ingestion(tmp_path, project_root)
    source = _fixture(project_root)

    first = import_apple_health_export(source, raw_store=store, runner=ingestion, now=NOW)
    second = import_apple_health_export(source, raw_store=store, runner=ingestion, now=NOW)

    assert first.inserted_count == 5
    assert second.duplicate_count == 5
    refs = list(store.iter_refs("apple_health"))
    assert len(refs) == 2
    assert all(store.manifest(ref)["sha256"] for ref in refs)
    assert all(
        store.manifest(ref)["request_metadata"]["parser_version"]
        == AppleHealthExportConnector.transform_version
        for ref in refs
    )


def test_import_commits_records_through_one_bounded_sink_batch(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, store, ingestion = _ingestion(tmp_path, project_root)
    connect_calls = 0
    real_connect = canonical_sink_module.connect

    def counted_connect(*args, **kwargs):
        nonlocal connect_calls
        connect_calls += 1
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(canonical_sink_module, "connect", counted_connect)

    result = import_apple_health_export(
        _fixture(project_root),
        raw_store=store,
        runner=ingestion,
        now=NOW,
    )

    assert result.normalized_count == 5
    assert connect_calls == 1


def test_cli_imports_apple_health_without_credentials(
    tmp_path: Path, project_root: Path
) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    assert runner.invoke(app, ["init", "--root", str(tmp_path)]).exit_code == 0

    result = runner.invoke(
        app,
        ["import", "apple-health", str(_fixture(project_root)), "--root", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    assert "PASS Apple Health export import:" in result.output
    assert "files=1 normalized=5 inserted=5 updated=0 duplicate=0" in result.output


def test_rejects_archives_without_a_single_export_xml(
    tmp_path: Path, project_root: Path
) -> None:
    _, store, ingestion = _ingestion(tmp_path, project_root)
    archive_path = tmp_path / "bad.zip"
    with ZipFile(archive_path, "w") as archive:
        archive.writestr("notes.txt", "not health data")

    with pytest.raises(AppleHealthExportError, match="exactly one export.xml"):
        import_apple_health_export(
            archive_path,
            raw_store=store,
            runner=ingestion,
            now=NOW,
        )
