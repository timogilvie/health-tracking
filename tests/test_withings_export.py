from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from shutil import copytree
from zipfile import ZipFile

import pytest
from typer.testing import CliRunner

from health.cli import app
from health.connectors import RawPage
from health.connectors.withings import (
    WithingsExportConnector,
    WithingsExportError,
    import_withings_export,
)
from health.db import connect, migrate
from health.ingestion import DuckDBCanonicalSink, IngestionRunner, RawStore

NOW = datetime(2026, 9, 10, 15, tzinfo=UTC)
runner = CliRunner()


def _fixture(project_root: Path, name: str) -> Path:
    return project_root / "tests" / "fixtures" / "providers" / "withings" / name


def _save_csv(store: RawStore, path: Path):
    return store.save(
        RawPage(
            source="withings",
            endpoint=f"export/{path.name}",
            retrieved_at=NOW,
            content=path.read_bytes(),
            content_type="text/csv",
            request_metadata={"export_entry": path.name},
        ),
        ingestion_run_id=None,
        transform_version="withings-export-v1",
    )


def _zip_export(path: Path, project_root: Path) -> None:
    with ZipFile(path, "w") as archive:
        archive.write(
            _fixture(project_root, "export-weight.csv"),
            "Withings export/weight.csv",
        )
        archive.write(
            _fixture(project_root, "export-blood-pressure.csv"),
            "Withings export/blood_pressure.csv",
        )
        archive.writestr("Withings export/activities.csv", "Date,Steps\n2026-09-10,1000\n")


def test_normalizes_current_weight_export_and_converts_pounds(
    tmp_path: Path,
    project_root: Path,
) -> None:
    store = RawStore(tmp_path / "raw")
    raw_ref = _save_csv(store, _fixture(project_root, "export-weight.csv"))
    connector = WithingsExportConnector(timezone_name="America/New_York")

    records = list(connector.normalize(raw_ref, store))

    assert [record.values["metric"] for record in records] == [
        "weight_kg",
        "body_fat_mass_kg",
        "bone_mass_kg",
        "skeletal_muscle_mass_kg",
        "body_water_mass_kg",
    ]
    assert [record.values["value"] for record in records] == pytest.approx(
        [100.0, 20.0, 3.0, 75.0, 50.0]
    )
    assert {record.values["source_record_id"] for record in records} == {
        "export:weight:2026-09-10T11:30:00+00:00"
    }
    assert all(record.values["unit"] == "kg" for record in records)
    assert all(record.values["original_unit"] == "lb" for record in records)
    assert all(record.values["timezone"] == "America/New_York" for record in records)
    assert all(record.values["quality"] == "valid" for record in records)
    assert all(
        record.values["metadata"]["withings_export"]["comments"] == "Synthetic scale fixture"
        for record in records
    )


def test_normalizes_blood_pressure_and_skips_standalone_pulse(
    tmp_path: Path,
    project_root: Path,
) -> None:
    store = RawStore(tmp_path / "raw")
    raw_ref = _save_csv(store, _fixture(project_root, "export-blood-pressure.csv"))
    connector = WithingsExportConnector(timezone_name="America/New_York")

    records = list(connector.normalize(raw_ref, store))

    assert len(records) == 1
    values = records[0].values
    assert values["measured_at"] == datetime(2026, 9, 10, 8, tzinfo=connector.timezone)
    assert values["local_date"].isoformat() == "2026-09-10"
    assert (values["systolic_mmhg"], values["diastolic_mmhg"], values["pulse_bpm"]) == (
        122.0,
        78.0,
        61.0,
    )
    assert values["notes"] == "Seated synthetic fixture"
    assert values["quality"] == "valid"


def test_zip_import_is_raw_first_idempotent_and_does_not_advance_api_watermark(
    tmp_path: Path,
    project_root: Path,
) -> None:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    store = RawStore(tmp_path / "raw")
    ingestion = IngestionRunner(
        database=database,
        raw_store=store,
        sink=DuckDBCanonicalSink(database),
    )
    export = tmp_path / "withings.zip"
    _zip_export(export, project_root)

    first = import_withings_export(
        export,
        timezone_name="America/New_York",
        raw_store=store,
        runner=ingestion,
        now=NOW,
    )
    second = import_withings_export(
        export,
        timezone_name="America/New_York",
        raw_store=store,
        runner=ingestion,
        now=NOW,
    )

    assert (first.raw_count, first.normalized_count, first.inserted_count) == (2, 6, 6)
    assert (second.raw_count, second.normalized_count, second.duplicate_count) == (2, 6, 6)
    refs = list(store.iter_refs("withings"))
    assert len(refs) == 6
    manifests = [store.manifest(raw_ref) for raw_ref in refs]
    archives = [manifest for manifest in manifests if manifest["content_type"] == "application/zip"]
    children = [manifest for manifest in manifests if manifest["content_type"] == "text/csv"]
    assert len(archives) == 2
    assert len(children) == 4
    assert all(manifest["parent_archive"] for manifest in children)
    assert all(manifest["request_metadata"]["archive_sha256"] for manifest in children)

    with connect(database, read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM observations").fetchone()[0] == 5
        assert connection.execute("SELECT count(*) FROM blood_pressure").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM source_sync_state").fetchone()[0] == 0
        runs = connection.execute(
            "SELECT metadata->>'replay' FROM ingestion_runs ORDER BY started_at"
        ).fetchall()
    assert runs == [("true",), ("true",)]


def test_cli_imports_zip_without_credentials_or_network(
    tmp_path: Path,
    project_root: Path,
) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    initialized = runner.invoke(app, ["init", "--root", str(tmp_path)])
    assert initialized.exit_code == 0, initialized.output
    export = tmp_path / "withings.zip"
    _zip_export(export, project_root)

    result = runner.invoke(
        app,
        ["import", "withings", str(export), "--root", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    assert "PASS Withings export import:" in result.output
    assert "files=2 normalized=6 inserted=6 updated=0 duplicate=0" in result.output
    assert "Synthetic scale fixture" not in result.output


def test_rejects_unrecognized_csv_and_invalid_project_timezone(tmp_path: Path) -> None:
    unsupported = tmp_path / "activities.csv"
    unsupported.write_text("Date,Steps\n2026-09-10,1000\n", encoding="utf-8")
    database = tmp_path / "health.duckdb"
    project_root = Path(__file__).parents[1]
    migrate(database, project_root / "sql")
    store = RawStore(tmp_path / "raw")
    ingestion = IngestionRunner(
        database=database,
        raw_store=store,
        sink=DuckDBCanonicalSink(database),
    )

    with pytest.raises(WithingsExportError, match="no supported"):
        import_withings_export(
            unsupported,
            timezone_name="America/New_York",
            raw_store=store,
            runner=ingestion,
            now=NOW,
        )
    with pytest.raises(WithingsExportError, match="invalid project timezone"):
        WithingsExportConnector(timezone_name="Mars/Olympus")
