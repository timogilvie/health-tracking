import json
import threading
from pathlib import Path
from shutil import copytree
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest
from typer.testing import CliRunner

import health.cli as health_cli
from health.cli import app
from health.config import HealthSettings, load_project_config
from health.dashboard import ASSET_ROOT, create_dashboard_server, dashboard_payload
from health.db import connect, migrate
from health.transforms import sync_source_priorities


def initialized_database(tmp_path: Path, project_root: Path) -> Path:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    config = load_project_config(HealthSettings(project_root=project_root, _env_file=None))
    sync_source_priorities(database, config)
    return database


def seed_dashboard(database: Path) -> None:
    with connect(database) as connection:
        withings = connection.execute(
            "INSERT INTO sources (name, source_type) VALUES ('withings', 'api') RETURNING source_id"
        ).fetchone()[0]
        oura = connection.execute(
            "INSERT INTO sources (name, source_type) VALUES ('oura', 'api') RETURNING source_id"
        ).fetchone()[0]
        manual = connection.execute(
            "INSERT INTO sources (name, source_type) "
            "VALUES ('manual', 'manual') RETURNING source_id"
        ).fetchone()[0]
        connection.executemany(
            """
            INSERT INTO observations (
                metric, observed_at, value, unit, source_id, source_record_id,
                quality, timezone, local_date, raw_file, transform_version
            ) VALUES (?, ?::TIMESTAMPTZ, ?, ?, ?, ?, 'valid',
                      'America/New_York', ?, ?, 'fixture-v1')
            """,
            [
                (
                    "weight_kg", "2026-09-09T12:00:00Z", 80, "kg",
                    withings, "w1", "2026-09-09", "raw/w1.json",
                ),
                (
                    "weight_kg", "2026-09-10T12:00:00Z", 79, "kg",
                    withings, "w2", "2026-09-10", "raw/w2.json",
                ),
                (
                    "resting_hr_bpm", "2026-09-10T11:00:00Z", 52, "bpm",
                    oura, "r1", "2026-09-10", "raw/r1.json",
                ),
                (
                    "hrv_rmssd_ms", "2026-09-10T11:00:00Z", 48, "ms",
                    oura, "h1", "2026-09-10", "raw/h1.json",
                ),
                (
                    "steps", "2026-09-10T23:00:00Z", 9000, "count",
                    oura, "s1", "2026-09-10", "raw/s1.json",
                ),
            ],
        )
        connection.execute(
            """
            INSERT INTO blood_pressure (
                measured_at, local_date, systolic_mmhg, diastolic_mmhg,
                source_id, source_record_id, raw_file, transform_version
            ) VALUES ('2026-09-10T12:00:00Z', '2026-09-10', 118, 74,
                      ?, 'bp1', 'raw/bp1.json', 'fixture-v1')
            """,
            [withings],
        )
        connection.execute(
            """
            INSERT INTO sleep_sessions (
                sleep_date, started_at, ended_at, total_sleep_seconds,
                source_id, source_record_id, raw_file, transform_version
            ) VALUES ('2026-09-10', '2026-09-10T03:00:00Z',
                      '2026-09-10T10:30:00Z', 27000, ?, 'sleep1',
                      'raw/sleep1.json', 'fixture-v1')
            """,
            [oura],
        )
        connection.execute(
            """
            INSERT INTO workouts (
                started_at, ended_at, local_date, workout_type,
                duration_seconds, source_id, source_record_id,
                raw_file, transform_version
            ) VALUES ('2026-09-10T20:00:00Z', '2026-09-10T20:55:00Z',
                      '2026-09-10', 'resistance', 3300, ?, 'lift1',
                      'raw/lift1.json', 'fixture-v1')
            """,
            [manual],
        )
        connection.execute(
            """
            INSERT INTO lab_results (
                collected_at, canonical_name, original_name, numeric_value,
                unit, reference_low, reference_high, provider, source_id,
                source_record_id, raw_file, transform_version
            ) VALUES ('2026-09-08T13:00:00Z', 'A1C', 'Hemoglobin A1C',
                      5.1, '%', 4, 5.6, 'Synthetic Lab', ?, 'lab1',
                      'raw/lab1.json', 'fixture-v1')
            """,
            [manual],
        )


def test_dashboard_payload_exposes_current_trends_rollups_and_labs(
    tmp_path: Path,
    project_root: Path,
) -> None:
    database = initialized_database(tmp_path, project_root)
    seed_dashboard(database)

    payload = dashboard_payload(database, days=7)

    assert payload["latest_date"] == "2026-09-10"
    assert payload["summary"]["weight"]["value"] == pytest.approx(174.165, abs=0.001)
    assert payload["summary"]["weight"]["unit"] == "lb"
    assert payload["summary"]["weight"]["date"] == "2026-09-10"
    assert payload["summary"]["sleep"]["value"] == 450.0
    assert len(payload["daily"]) == 2
    assert payload["daily"][0]["weight_lb"] == pytest.approx(176.37, abs=0.001)
    assert all("weight_kg" not in row for row in payload["daily"])
    assert {row["window_days"] for row in payload["rolling"]} == {7, 30, 90, 365}
    assert all("weight_lb_avg" in row and "weight_kg_avg" not in row for row in payload["rolling"])
    assert payload["weekly"][0]["resistance_minutes_total"] == 55.0
    assert payload["labs"][0]["name"] == "A1C"
    assert payload["labs"][0]["numeric_value"] == 5.1
    assert payload["quality"]["status"] == "clear"
    assert payload["quality"]["metric_source_count"] == 8
    assert payload["quality"]["coverage"][0]["first_date"]

    with pytest.raises(ValueError, match="range must be"):
        dashboard_payload(database, days=14)


def test_dashboard_payload_handles_an_empty_database(
    tmp_path: Path,
    project_root: Path,
) -> None:
    payload = dashboard_payload(initialized_database(tmp_path, project_root))

    assert payload["latest_date"] is None
    assert payload["daily"] == []
    assert payload["rolling"] == []
    assert payload["labs"] == []
    assert all(value is None for value in payload["summary"].values())
    assert payload["quality"]["status"] == "empty"
    assert payload["quality"]["coverage"] == []


def test_dashboard_server_is_loopback_only_and_sets_private_security_headers(
    tmp_path: Path,
    project_root: Path,
) -> None:
    database = initialized_database(tmp_path, project_root)
    server = create_dashboard_server(database, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    assert host == "127.0.0.1"
    try:
        with urlopen(f"http://127.0.0.1:{port}/", timeout=3) as response:
            html = response.read().decode()
            assert response.headers["Cache-Control"] == "no-store"
            assert response.headers["X-Frame-Options"] == "DENY"
            assert "default-src 'none'" in response.headers["Content-Security-Policy"]
            assert "Private health ledger" in html
        request = Request(
            f"http://127.0.0.1:{port}/api/dashboard",
            headers={"Accept": "application/json"},
        )
        with urlopen(request, timeout=3) as response:
            assert response.headers["Content-Type"] == "application/json"
            assert json.loads(response.read())["daily"] == []
        with pytest.raises(HTTPError) as error:
            urlopen(f"http://127.0.0.1:{port}/private-file", timeout=3)
        assert error.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_dashboard_assets_have_accessible_sections_and_no_remote_dependencies() -> None:
    html = (ASSET_ROOT / "index.html").read_text()
    javascript = (ASSET_ROOT / "app.js").read_text()
    stylesheet = (ASSET_ROOT / "styles.css").read_text()

    for section in ("weight", "bp", "sleep", "exercise", "labs", "quality"):
        assert f'id="{section}"' in html
    assert "aria-live" in html
    assert "prefers-reduced-motion" in stylesheet
    assert "https://" not in html + javascript + stylesheet
    assert "weight_lb" in javascript
    assert "weight_kg" not in javascript
    assert ">lb<" in html
    assert 'id="quality-coverage-body"' in html
    assert 'id="quality-import-body"' in html
    assert "renderQuality" in javascript


def test_dashboard_cli_validates_project_and_passes_private_server_options(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    copytree(project_root / "config", tmp_path / "config")
    copytree(project_root / "sql", tmp_path / "sql")
    command_runner = CliRunner()
    initialized = command_runner.invoke(app, ["init", "--root", str(tmp_path)])
    assert initialized.exit_code == 0, initialized.output
    called = {}

    def fake_server(database: Path, *, port: int, open_browser: bool) -> None:
        called.update(database=database, port=port, open_browser=open_browser)

    monkeypatch.setattr(health_cli, "serve_dashboard", fake_server)

    result = command_runner.invoke(
        app,
        ["dashboard", "--port", "9876", "--no-open", "--root", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    assert called == {
        "database": tmp_path / "data/health.duckdb",
        "port": 9876,
        "open_browser": False,
    }
