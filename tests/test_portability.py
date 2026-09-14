from pathlib import Path
from shutil import copytree

import duckdb
from typer.testing import CliRunner

from health.cli import app


def _initialized_project(tmp_path: Path, project_root: Path) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    result = CliRunner().invoke(app, ["init", "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output


def test_fixture_raw_store_rebuilds_offline_and_exports_portable_datasets(
    tmp_path: Path,
    project_root: Path,
) -> None:
    _initialized_project(tmp_path, project_root)
    cli = CliRunner()
    fixture = project_root / "tests/fixtures/providers/apple_health/export.xml"
    imported = cli.invoke(
        app,
        ["import", "apple-health", str(fixture), "--root", str(tmp_path)],
    )
    assert imported.exit_code == 0, imported.output

    rebuilt = tmp_path / "data/rebuilt.duckdb"
    result = cli.invoke(
        app,
        ["rebuild", "--target", str(rebuilt), "--root", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output
    assert "raw=1 normalized=5 inserted=5" in result.output

    original = duckdb.connect(str(tmp_path / "data/health.duckdb"), read_only=True)
    restored = duckdb.connect(str(rebuilt), read_only=True)
    try:
        observation_query = """
            SELECT metric, observed_at, observed_until, value, unit, source_record_id
            FROM observations ORDER BY metric, observed_at, source_record_id
        """
        sleep_query = """
            SELECT sleep_date, started_at, ended_at, total_sleep_seconds,
                   light_seconds, source_record_id
            FROM sleep_sessions ORDER BY started_at, source_record_id
        """
        assert original.execute(observation_query).fetchall() == restored.execute(
            observation_query
        ).fetchall()
        assert original.execute(sleep_query).fetchall() == restored.execute(sleep_query).fetchall()
    finally:
        original.close()
        restored.close()

    for format_name, suffix in (("parquet", ".parquet"), ("csv", ".csv")):
        output = tmp_path / f"portable-{format_name}"
        exported = cli.invoke(
            app,
            [
                "export",
                "datasets",
                "--format",
                format_name,
                "--output",
                str(output),
                "--root",
                str(tmp_path),
            ],
            env={"HEALTH_DATABASE_PATH": str(rebuilt)},
        )
        assert exported.exit_code == 0, exported.output
        assert "files=9" in exported.output
        assert len(list(output.glob(f"*{suffix}"))) == 9

    exported_rows = duckdb.connect().execute(
        "SELECT count(*) FROM read_parquet(?)",
        [str(tmp_path / "portable-parquet/daily.parquet")],
    ).fetchone()[0]
    assert exported_rows > 0


def test_rebuild_and_export_refuse_to_overwrite_existing_targets(
    tmp_path: Path,
    project_root: Path,
) -> None:
    _initialized_project(tmp_path, project_root)
    cli = CliRunner()
    target = tmp_path / "already.duckdb"
    target.write_text("keep", encoding="utf-8")
    export_target = tmp_path / "already-exported"
    export_target.mkdir()

    rebuild = cli.invoke(
        app,
        ["rebuild", "--target", str(target), "--root", str(tmp_path)],
    )
    exported = cli.invoke(
        app,
        ["export", "datasets", "--output", str(export_target), "--root", str(tmp_path)],
    )

    assert rebuild.exit_code == 1
    assert "target already exists" in rebuild.output
    assert target.read_text(encoding="utf-8") == "keep"
    assert exported.exit_code == 1
    assert "target already exists" in exported.output
