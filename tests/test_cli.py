from pathlib import Path
from shutil import copytree

from typer.testing import CliRunner

from health.cli import app

runner = CliRunner()


def test_init_then_doctor_smoke(tmp_path: Path, project_root: Path) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")

    initialized = runner.invoke(app, ["init", "--root", str(tmp_path)])
    diagnosed = runner.invoke(app, ["doctor", "--root", str(tmp_path)])

    assert initialized.exit_code == 0, initialized.output
    assert "applied 3 migration(s)" in initialized.output
    assert diagnosed.exit_code == 0, diagnosed.output
    assert "PASS python" in diagnosed.output
    assert "PASS database" in diagnosed.output


def test_doctor_fails_before_initialization(tmp_path: Path, project_root: Path) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")

    result = runner.invoke(app, ["doctor", "--root", str(tmp_path)])

    assert result.exit_code == 1
    assert "FAIL data directories" in result.output
    assert "FAIL database" in result.output
