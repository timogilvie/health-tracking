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


def test_withings_status_does_not_print_configuration_secret(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["withings", "status", "--root", str(tmp_path)],
        env={
            "HEALTH_WITHINGS_CLIENT_ID": "synthetic-client",
            "HEALTH_WITHINGS_CLIENT_SECRET": "never-print-this",
            "HEALTH_WITHINGS_REDIRECT_URI": "http://localhost:8765/callback",
        },
    )

    assert result.exit_code == 1
    assert "PASS Withings configuration: complete" in result.output
    assert "state=missing" in result.output
    assert "never-print-this" not in result.output


def test_withings_exchange_prompts_hide_callback_values(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["withings", "exchange", "--root", str(tmp_path)],
        input="synthetic-code\nsynthetic-state\n",
        env={
            "HEALTH_WITHINGS_CLIENT_ID": "synthetic-client",
            "HEALTH_WITHINGS_CLIENT_SECRET": "never-print-this",
            "HEALTH_WITHINGS_REDIRECT_URI": "http://localhost:8765/callback",
        },
    )

    assert result.exit_code == 1
    assert "Authorization code" in result.output
    assert "Returned state" in result.output
    assert "synthetic-code" not in result.output
    assert "synthetic-state" not in result.output
    assert "never-print-this" not in result.output
