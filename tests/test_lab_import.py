from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from shutil import copytree

import pytest
from typer.testing import CliRunner

from health.cli import app
from health.config import load_yaml
from health.connectors.labs import (
    LabImportError,
    import_lab_csv,
    load_biomarker_vocabulary,
    preview_lab_csv,
)
from health.db import connect, migrate
from health.ingestion import DuckDBCanonicalSink, IngestionRunner, RawStore

NOW = datetime(2026, 9, 10, 17, tzinfo=UTC)
runner = CliRunner()


def _fixture(project_root: Path) -> Path:
    return project_root / "tests" / "fixtures" / "labs" / "results.csv"


def _vocabulary(project_root: Path) -> dict[str, str]:
    return load_biomarker_vocabulary(load_yaml(project_root / "config" / "biomarkers.yaml"))


def _ingestion(tmp_path: Path, project_root: Path):
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    store = RawStore(tmp_path / "raw")
    return database, store, IngestionRunner(
        database=database,
        raw_store=store,
        sink=DuckDBCanonicalSink(database),
    )


def test_preview_maps_aliases_and_reports_unknown_without_writing(
    tmp_path: Path, project_root: Path
) -> None:
    database, _, _ = _ingestion(tmp_path, project_root)

    preview = preview_lab_csv(
        _fixture(project_root),
        vocabulary=_vocabulary(project_root),
        timezone_name="America/New_York",
    )

    assert preview.rows == 4
    assert preview.recognized == 2
    assert preview.unknown == ("Apolipoprotein B", "Appearance")
    with connect(database, read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM lab_results").fetchone()[0] == 0


def test_commit_preserves_ranges_provider_unknowns_and_is_idempotent(
    tmp_path: Path, project_root: Path
) -> None:
    database, store, ingestion = _ingestion(tmp_path, project_root)
    kwargs = {
        "vocabulary": _vocabulary(project_root),
        "timezone_name": "America/New_York",
        "raw_store": store,
        "runner": ingestion,
        "now": NOW,
    }

    first = import_lab_csv(_fixture(project_root), **kwargs)
    second = import_lab_csv(_fixture(project_root), **kwargs)

    assert (first.normalized_count, first.inserted_count) == (4, 4)
    assert second.duplicate_count == 4
    with connect(database, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT canonical_name, original_name, numeric_value, text_value, unit,
                   reference_low, reference_high, reference_text, provider, fasting
            FROM lab_results ORDER BY original_name
            """
        ).fetchall()
    assert rows[0][:4] == (None, "Apolipoprotein B", 72.0, None)
    assert rows[1][:4] == (None, "Appearance", None, "Clear")
    assert rows[2] == (
        "A1C",
        "HbA1c",
        5.1,
        None,
        "%",
        4.0,
        5.6,
        "4.0-5.6",
        "Synthetic Lab",
        True,
    )
    assert rows[3][0] == "LDL cholesterol"
    assert rows[3][6] == 100.0


def test_cli_previews_by_default_and_requires_commit_to_write(
    tmp_path: Path, project_root: Path
) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    assert runner.invoke(app, ["init", "--root", str(tmp_path)]).exit_code == 0

    preview = runner.invoke(
        app, ["import", "labs", str(_fixture(project_root)), "--root", str(tmp_path)]
    )
    committed = runner.invoke(
        app,
        [
            "import",
            "labs",
            str(_fixture(project_root)),
            "--commit",
            "--root",
            str(tmp_path),
        ],
    )

    assert preview.exit_code == 0, preview.output
    assert "No data written" in preview.output
    assert committed.exit_code == 0, committed.output
    assert "rows=4 recognized=2 unknown=2 inserted=4" in committed.output


def test_invalid_csv_is_rejected_before_raw_storage(
    tmp_path: Path, project_root: Path
) -> None:
    _, store, ingestion = _ingestion(tmp_path, project_root)
    invalid = tmp_path / "invalid.csv"
    invalid.write_text("Date,Test Name\n2026-09-08,A1C\n", encoding="utf-8")

    with pytest.raises(LabImportError, match="missing required column"):
        import_lab_csv(
            invalid,
            vocabulary=_vocabulary(project_root),
            timezone_name="America/New_York",
            raw_store=store,
            runner=ingestion,
            now=NOW,
        )
    assert list(store.iter_refs("labs")) == []
