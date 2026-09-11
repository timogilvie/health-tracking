from datetime import UTC, datetime, timedelta
from pathlib import Path
from shutil import copytree
from urllib.parse import parse_qs

import httpx
import pytest
from typer.testing import CliRunner

import health.cli as health_cli
from health.cli import app
from health.db import connect

runner = CliRunner()


class FakeWithingsOAuth:
    def authenticate(self) -> None:
        pass

    def access_token(self) -> str:
        return "synthetic-access"


class EmptyOuraConnector:
    name = "oura"
    transform_version = "oura-test-v1"

    def authenticate(self) -> None:
        pass

    def fetch(self, start: datetime, end: datetime):
        assert start.tzinfo is not None
        assert end.tzinfo is not None
        return iter(())

    def normalize(self, raw_ref, raw_store):
        return ()


def initialize_project(tmp_path: Path, project_root: Path) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    result = runner.invoke(app, ["init", "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output


def test_init_then_doctor_smoke(tmp_path: Path, project_root: Path) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")

    initialized = runner.invoke(app, ["init", "--root", str(tmp_path)])
    diagnosed = runner.invoke(app, ["doctor", "--root", str(tmp_path)])

    assert initialized.exit_code == 0, initialized.output
    assert "applied 6 migration(s)" in initialized.output
    assert diagnosed.exit_code == 0, diagnosed.output
    assert "PASS python" in diagnosed.output
    assert "PASS database" in diagnosed.output
    assert "PASS source priorities: current" in diagnosed.output


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


def test_oura_status_does_not_print_configuration_secret(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["oura", "status", "--root", str(tmp_path)],
        env={
            "HEALTH_OURA_CLIENT_ID": "synthetic-client",
            "HEALTH_OURA_CLIENT_SECRET": "never-print-this",
            "HEALTH_OURA_REDIRECT_URI": "http://localhost:8765/callback",
        },
    )

    assert result.exit_code == 1
    assert "PASS Oura configuration: complete" in result.output
    assert "state=missing" in result.output
    assert "never-print-this" not in result.output


def test_oura_exchange_prompts_hide_callback_values(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["oura", "exchange", "--root", str(tmp_path)],
        input="synthetic-code\nsynthetic-state\n",
        env={
            "HEALTH_OURA_CLIENT_ID": "synthetic-client",
            "HEALTH_OURA_CLIENT_SECRET": "never-print-this",
            "HEALTH_OURA_REDIRECT_URI": "http://localhost:8765/callback",
        },
    )

    assert result.exit_code == 1
    assert "Authorization code" in result.output
    assert "Returned state" in result.output
    assert "synthetic-code" not in result.output
    assert "synthetic-state" not in result.output
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


def test_sync_status_reports_never_synced_without_credentials_or_network(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_project(tmp_path, project_root)
    monkeypatch.setattr(
        health_cli,
        "http_client",
        lambda: (_ for _ in ()).throw(AssertionError("status must not use network")),
    )

    result = runner.invoke(
        app,
        ["sync", "status", "--source", "withings", "--root", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    assert result.output.strip() == "PASS Sync status: source=withings state=never-synced"


def test_sync_withings_wires_runtime_windows_replay_and_status(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_project(tmp_path, project_root)
    first_page = project_root / "tests/fixtures/providers/withings/measure-page-1.json"
    second_page = project_root / "tests/fixtures/providers/withings/measure-page-2.json"
    forms: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        forms.append(form)
        fixture = second_page if form.get("offset") == ["2"] else first_page
        return httpx.Response(200, content=fixture.read_bytes())

    monkeypatch.setattr(
        health_cli,
        "http_client",
        lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )
    monkeypatch.setattr(
        health_cli,
        "withings_oauth",
        lambda _settings, _client: FakeWithingsOAuth(),
    )
    end = datetime(2026, 9, 10, 15, tzinfo=UTC)
    arguments = [
        "sync",
        "withings",
        "--end",
        end.isoformat(),
        "--root",
        str(tmp_path),
    ]

    first = runner.invoke(app, arguments)
    overlap = runner.invoke(app, arguments)
    explicit_start = end - timedelta(days=14)
    explicit = runner.invoke(
        app,
        [
            "sync",
            "withings",
            "--start",
            explicit_start.isoformat(),
            "--end",
            end.isoformat(),
            "--root",
            str(tmp_path),
        ],
    )

    assert first.exit_code == 0, first.output
    assert "PASS Withings sync:" in first.output
    assert "raw=2 normalized=8 inserted=8 updated=0 duplicate=0" in first.output
    assert overlap.exit_code == 0, overlap.output
    assert "raw=2 normalized=8 inserted=0 updated=0 duplicate=8" in overlap.output
    assert explicit.exit_code == 0, explicit.output
    assert "raw=2 normalized=8 inserted=0 updated=0 duplicate=8" in explicit.output
    assert all(value not in first.output for value in ("82.1", "122.0", "synthetic-access"))

    assert forms[0]["startdate"] == [str(int((end - timedelta(days=7)).timestamp()))]
    assert forms[2]["startdate"] == [str(int((end - timedelta(hours=72)).timestamp()))]
    assert forms[4]["startdate"] == [str(int(explicit_start.timestamp()))]
    assert all(form["enddate"] == [str(int(end.timestamp()))] for form in forms)

    monkeypatch.setattr(
        health_cli,
        "http_client",
        lambda: (_ for _ in ()).throw(AssertionError("status must not use network")),
    )
    status = runner.invoke(
        app,
        ["sync", "status", "--source", "withings", "--root", str(tmp_path)],
    )

    assert status.exit_code == 0, status.output
    assert "state=succeeded" in status.output
    assert "raw=2 normalized=8 inserted=0 updated=0 duplicate=8" in status.output
    assert f"last_successful_end={end.isoformat()}" in status.output
    with connect(tmp_path / "data/health.duckdb", read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM observations").fetchone()[0] == 7
        assert connection.execute("SELECT count(*) FROM blood_pressure").fetchone()[0] == 1


def test_sync_oura_wires_shared_watermark_and_status(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_project(tmp_path, project_root)
    monkeypatch.setattr(
        health_cli,
        "oura_connector",
        lambda _settings, timezone_name: (
            EmptyOuraConnector()
            if timezone_name == "America/New_York"
            else pytest.fail("unexpected timezone")
        ),
    )
    end = datetime(2026, 9, 10, 15, tzinfo=UTC)

    synced = runner.invoke(
        app,
        ["sync", "oura", "--end", end.isoformat(), "--root", str(tmp_path)],
    )
    status = runner.invoke(
        app,
        ["sync", "status", "--source", "oura", "--root", str(tmp_path)],
    )

    assert synced.exit_code == 0, synced.output
    assert "PASS Oura sync:" in synced.output
    assert "raw=0 normalized=0 inserted=0 updated=0 duplicate=0" in synced.output
    assert status.exit_code == 0, status.output
    assert "source=oura state=succeeded" in status.output
    assert f"last_successful_end={end.isoformat()}" in status.output


def test_sync_rejects_invalid_windows_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        health_cli,
        "http_client",
        lambda: (_ for _ in ()).throw(AssertionError("invalid window must not use network")),
    )

    naive = runner.invoke(
        app,
        ["sync", "withings", "--start", "2026-09-01T12:00:00", "--root", str(tmp_path)],
        terminal_width=160,
    )
    reversed_window = runner.invoke(
        app,
        [
            "sync",
            "withings",
            "--start",
            "2026-09-10T12:00:00Z",
            "--end",
            "2026-09-09T12:00:00Z",
            "--root",
            str(tmp_path),
        ],
        terminal_width=160,
    )

    assert naive.exit_code == 2
    assert "must include a timezone offset" in naive.output
    assert reversed_window.exit_code == 2
    assert "must not be before --start" in reversed_window.output


def test_sync_failure_output_does_not_expose_unexpected_error_details(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_project(tmp_path, project_root)
    monkeypatch.setattr(
        health_cli,
        "http_client",
        lambda: (_ for _ in ()).throw(RuntimeError("never-print-this")),
    )

    result = runner.invoke(
        app,
        ["sync", "withings", "--root", str(tmp_path)],
    )

    assert result.exit_code == 1
    assert "FAIL Withings sync: unexpected RuntimeError" in result.output
    assert "never-print-this" not in result.output


@pytest.mark.parametrize(("state", "exit_code"), [("running", 0), ("failed", 1)])
def test_sync_status_distinguishes_active_and_failed_runs_without_error_detail(
    tmp_path: Path,
    project_root: Path,
    state: str,
    exit_code: int,
) -> None:
    initialize_project(tmp_path, project_root)
    database = tmp_path / "data/health.duckdb"
    with connect(database) as connection:
        source_id = connection.execute(
            """
            INSERT INTO sources (name, source_type)
            VALUES ('withings', 'api')
            RETURNING source_id
            """
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO ingestion_runs (source_id, status, error)
            VALUES (?, ?, ?)
            """,
            [source_id, state, "RuntimeError: never-print-this" if state == "failed" else None],
        )

    result = runner.invoke(
        app,
        ["sync", "status", "--source", "withings", "--root", str(tmp_path)],
    )

    assert result.exit_code == exit_code, result.output
    assert f"state={state}" in result.output
    expected_error = "RuntimeError" if state == "failed" else "none"
    assert f"error={expected_error}" in result.output
    assert "never-print-this" not in result.output
