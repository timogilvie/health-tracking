import json
from collections.abc import Callable
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from oura_ring import OuraClient

from health.config import HealthSettings, load_project_config
from health.connectors import RawPage
from health.connectors.oura import OuraConnector, OuraPayloadError, OuraSleepConnector
from health.db import connect, migrate
from health.ingestion import DuckDBCanonicalSink, IngestionRunner, RawStore
from health.transforms import sync_source_priorities

START = datetime(2026, 9, 10, 4, tzinfo=UTC)
END = datetime(2026, 9, 11, 3, tzinfo=UTC)


class FakeOAuth:
    def __init__(self) -> None:
        self.authenticated = 0

    def authenticate(self) -> None:
        self.authenticated += 1

    def access_token(self) -> str:
        return "synthetic-oura-token"


class FixtureSession:
    """Small requests.Session substitute that still exercises OuraClient pagination."""

    def __init__(self, fixtures: Path) -> None:
        self.fixtures = fixtures
        self.headers: dict[str, str] = {}
        self.hooks: dict[str, list[Callable[..., Any]]] = {"response": []}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    def request(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any],
        timeout: int,
        **_kwargs: Any,
    ) -> requests.Response:
        assert timeout == 60
        path = urlparse(url).path
        copied_params = dict(params)
        self.calls.append((path, copied_params))
        filename = {
            "/v2/usercollection/sleep": (
                "sleep-page-2.json" if copied_params.get("next_token") else "sleep-page-1.json"
            ),
            "/v2/usercollection/daily_sleep": "daily-sleep.json",
            "/v2/usercollection/daily_readiness": "daily-readiness.json",
            "/v2/usercollection/heartrate": "heart-rate.json",
            "/v2/usercollection/daily_activity": "daily-activity.json",
            "/v2/usercollection/workout": (
                "workout-page-2.json"
                if copied_params.get("next_token")
                else "workout-page-1.json"
            ),
            "/v2/usercollection/session": "session.json",
        }[path]
        prepared = requests.Request(method=method, url=url, params=params).prepare()
        response = requests.Response()
        response.status_code = 200
        response._content = (self.fixtures / filename).read_bytes()
        response.headers["Content-Type"] = "application/json"
        response.headers["X-Oura-Request-Id"] = f"synthetic-{len(self.calls)}"
        response.request = prepared
        for hook in self.hooks["response"]:
            hook(response)
        return response

    def close(self) -> None:
        self.closed = True


def make_connector(
    project_root: Path,
) -> tuple[OuraSleepConnector, FakeOAuth, FixtureSession]:
    oauth = FakeOAuth()
    session = FixtureSession(project_root / "tests/fixtures/providers/oura")

    def factory(token: str) -> OuraClient:
        assert token == "synthetic-oura-token"
        client = OuraClient(access_token=token)
        client.session = session
        return client

    connector = OuraSleepConnector(
        oauth=oauth,  # type: ignore[arg-type]
        timezone_name="America/New_York",
        client_factory=factory,
        now=lambda: END,
    )
    return connector, oauth, session


def make_complete_connector(
    project_root: Path,
) -> tuple[OuraConnector, FakeOAuth, FixtureSession]:
    oauth = FakeOAuth()
    session = FixtureSession(project_root / "tests/fixtures/providers/oura")

    def factory(token: str) -> OuraClient:
        assert token == "synthetic-oura-token"
        client = OuraClient(access_token=token)
        client.session = session
        return client

    connector = OuraConnector(
        oauth=oauth,  # type: ignore[arg-type]
        timezone_name="America/New_York",
        client_factory=factory,
        now=lambda: END,
    )
    return connector, oauth, session


def initialized_database(tmp_path: Path, project_root: Path) -> Path:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    config = load_project_config(HealthSettings(project_root=project_root, _env_file=None))
    sync_source_priorities(database, config)
    return database


def test_oura_sleep_sync_paginates_stores_raw_and_populates_canonical_tables(
    tmp_path: Path,
    project_root: Path,
) -> None:
    database = initialized_database(tmp_path, project_root)
    raw_store = RawStore(tmp_path / "raw")
    connector, oauth, session = make_connector(project_root)
    runner = IngestionRunner(
        database=database,
        raw_store=raw_store,
        sink=DuckDBCanonicalSink(database),
        now=lambda: END,
    )

    result = runner.sync(connector, start=START, end=END)

    assert oauth.authenticated == 1
    assert session.closed is True
    assert result.raw_count == 5
    assert result.normalized_count == 8
    assert result.inserted_count == 8
    assert [path for path, _params in session.calls].count("/v2/usercollection/sleep") == 2
    assert session.calls[0][1] == {
        "start_date": "2026-09-10",
        "end_date": "2026-09-10",
    }
    assert session.calls[1][1]["next_token"] == "page-2"
    assert session.calls[-1][1] == {
        "start_datetime": "2026-09-10 04:00:00+00:00",
        "end_datetime": "2026-09-11 03:00:00+00:00",
    }

    refs = list(raw_store.iter_refs("oura"))
    assert len(refs) == 5
    manifests = [raw_store.manifest(ref) for ref in refs]
    serialized_manifests = json.dumps(manifests)
    assert "synthetic-oura-token" not in serialized_manifests
    assert "authorization" not in serialized_manifests.lower()
    assert all(
        manifest["request_metadata"]["window_start"] == START.isoformat()
        for manifest in manifests
    )
    assert all(manifest["response_headers"]["X-Oura-Request-Id"] for manifest in manifests)
    first_page_ref = next(
        ref
        for ref in refs
        if raw_store.manifest(ref)["endpoint"] == "v2/usercollection/sleep"
        and "next_token" not in raw_store.manifest(ref)["request_metadata"]["query"]
    )
    assert raw_store.read(first_page_ref) == (
        project_root / "tests/fixtures/providers/oura/sleep-page-1.json"
    ).read_bytes()

    with connect(database, read_only=True) as connection:
        sessions = connection.execute(
            """
            SELECT source_record_id, sleep_date, total_sleep_seconds,
                   efficiency_pct, resting_hr_bpm, average_hrv_rmssd_ms
            FROM sleep_sessions ORDER BY started_at
            """
        ).fetchall()
        observations = connection.execute(
            """
            SELECT metric, value, quality
            FROM observations ORDER BY metric, observed_at
            """
        ).fetchall()
        daily = connection.execute(
            """
            SELECT total_sleep_minutes, resting_hr_bpm, hrv_rmssd_ms
            FROM daily_health WHERE local_date = '2026-09-10'
            """
        ).fetchone()
        nap_metadata = json.loads(
            connection.execute(
                """
                SELECT metadata FROM sleep_sessions
                WHERE source_record_id = 'nap-2026-09-10'
                """
            ).fetchone()[0]
        )
    assert sessions == [
        ("sleep-2026-09-09", datetime(2026, 9, 10).date(), 26400, 91.0, 52.5, 47.0),
        ("nap-2026-09-10", datetime(2026, 9, 10).date(), 2100, 87.5, None, 39.0),
    ]
    assert "non_positive_average_heart_rate" in nap_metadata["quality_reasons"]
    assert observations == [
        ("heart_rate_bpm", 54.0, "valid"),
        ("heart_rate_bpm", 72.0, "valid"),
        ("readiness_score", 87.0, "valid"),
        ("sleep_score", 85.0, "valid"),
        ("temperature_deviation_c", -0.12, "valid"),
        ("temperature_trend_deviation_c", 0.04, "valid"),
    ]
    assert daily == (475.0, 52.5, 47.0)

    replay = runner.replay(connector, refs)
    assert replay.normalized_count == 8
    assert replay.duplicate_count == 8
    assert replay.inserted_count == 0


def test_oura_sleep_rejects_malformed_collection_page(
    tmp_path: Path,
    project_root: Path,
) -> None:
    connector, _oauth, _session = make_connector(project_root)
    store = RawStore(tmp_path / "raw")

    ref = store.save(
        RawPage(
            source="oura",
            endpoint="v2/usercollection/sleep",
            retrieved_at=END,
            content=b'{"data":"not-a-list","next_token":null}',
            http_status=200,
        ),
        ingestion_run_id=None,
        transform_version=connector.transform_version,
    )

    with pytest.raises(OuraPayloadError, match="schema validation failed"):
        list(connector.normalize(ref, store))


def test_oura_sleep_requires_aware_sync_window(project_root: Path) -> None:
    connector, _oauth, _session = make_connector(project_root)

    with pytest.raises(ValueError, match="start must be timezone-aware"):
        list(connector.fetch(datetime(2026, 9, 10), END))


def test_oura_heart_rate_requests_are_split_into_thirty_day_windows(
    project_root: Path,
) -> None:
    connector, _oauth, session = make_connector(project_root)
    start = datetime(2026, 6, 1, tzinfo=UTC)
    end = datetime(2026, 8, 1, tzinfo=UTC)

    list(connector.fetch(start, end))

    calls = [
        params
        for path, params in session.calls
        if path == "/v2/usercollection/heartrate"
    ]
    assert len(calls) == 3
    windows = [
        (
            datetime.fromisoformat(params["start_datetime"]),
            datetime.fromisoformat(params["end_datetime"]),
        )
        for params in calls
    ]
    assert windows[0][0] == start
    assert windows[-1][1] == end
    assert all(
        window_end - window_start <= connector.heart_rate_max_window
        for window_start, window_end in windows
    )
    assert all(
        previous_end == next_start
        for (_previous_start, previous_end), (next_start, _next_end) in pairwise(windows)
    )


def test_captured_query_metadata_has_no_access_token(project_root: Path) -> None:
    connector, _oauth, _session = make_connector(project_root)

    pages = list(connector.fetch(START, END))

    assert all("access_token" not in parse_qs(urlparse(page.endpoint).query) for page in pages)
    assert all("synthetic-oura-token" not in json.dumps(page.request_metadata) for page in pages)


def test_complete_oura_sync_preserves_activity_coverage_workouts_and_sessions(
    tmp_path: Path,
    project_root: Path,
) -> None:
    database = initialized_database(tmp_path, project_root)
    raw_store = RawStore(tmp_path / "raw")
    connector, oauth, session = make_complete_connector(project_root)
    runner = IngestionRunner(
        database=database,
        raw_store=raw_store,
        sink=DuckDBCanonicalSink(database),
        now=lambda: END,
    )

    result = runner.sync(connector, start=START, end=END)

    assert oauth.authenticated == 1
    assert result.raw_count == 9
    assert result.normalized_count == 21
    assert result.inserted_count == 21
    called_paths = [path for path, _params in session.calls]
    assert called_paths.index("/v2/usercollection/daily_activity") < called_paths.index(
        "/v2/usercollection/heartrate"
    )
    workout_calls = [
        params
        for path, params in session.calls
        if path == "/v2/usercollection/workout"
    ]
    assert len(workout_calls) == 2
    assert workout_calls[1]["next_token"] == "workout-page-2"

    refs = list(raw_store.iter_refs("oura"))
    workout_contents = {
        raw_store.read(ref)
        for ref in refs
        if raw_store.manifest(ref)["endpoint"] == "v2/usercollection/workout"
    }
    assert workout_contents == {
        (project_root / "tests/fixtures/providers/oura/workout-page-1.json").read_bytes(),
        (project_root / "tests/fixtures/providers/oura/workout-page-2.json").read_bytes(),
    }

    with connect(database, read_only=True) as connection:
        activity = connection.execute(
            """
            SELECT metric, value, original_value, original_unit,
                   json_extract_string(metadata, '$.coverage.missing_values_are_unknown')
            FROM observations
            WHERE metric IN ('steps', 'active_energy_kcal', 'non_wear_minutes')
            ORDER BY metric
            """
        ).fetchall()
        workouts = connection.execute(
            """
            SELECT source_record_id, workout_type, duration_seconds, distance_m,
                   energy_kcal,
                   json_extract_string(metadata, '$.oura.record.source')
            FROM workouts ORDER BY started_at
            """
        ).fetchall()
        event = connection.execute(
            """
            SELECT event_type,
                   json_extract_string(metadata, '$.oura.record.type'),
                   json_extract_string(metadata, '$.oura.record.mood')
            FROM events
            """
        ).fetchone()
        daily = connection.execute(
            """
            SELECT steps, active_energy_kcal, resistance_minutes,
                   cardio_minutes, workout_count
            FROM daily_health WHERE local_date = '2026-09-10'
            """
        ).fetchone()
    assert activity == [
        ("active_energy_kcal", 640.0, 640.0, "kcal", "true"),
        ("non_wear_minutes", 60.0, 3600.0, "s", "true"),
        ("steps", 11234.0, 11234.0, "count", "true"),
    ]
    assert workouts == [
        ("workout-strength-2026-09-10", "resistance", 3000, None, 280.0, "confirmed"),
        ("workout-run-2026-09-10", "running", 1800, 5200.0, 360.0, "auto"),
    ]
    assert event == ("oura_session", "meditation", "relaxed")
    assert daily == (11234.0, 640.0, 50.0, 30.0, 2)

    replay = runner.replay(connector, refs)
    assert replay.normalized_count == 21
    assert replay.duplicate_count == 21


def test_delayed_activity_revision_updates_overlap_without_zero_filling(
    tmp_path: Path,
    project_root: Path,
) -> None:
    database = initialized_database(tmp_path, project_root)
    raw_store = RawStore(tmp_path / "raw")
    connector, _oauth, _session = make_complete_connector(project_root)
    runner = IngestionRunner(
        database=database,
        raw_store=raw_store,
        sink=DuckDBCanonicalSink(database),
        now=lambda: END,
    )
    runner.sync(connector, start=START, end=END)

    fixture = json.loads(
        (project_root / "tests/fixtures/providers/oura/daily-activity.json").read_text()
    )
    fixture["data"][0]["active_calories"] = 700
    revised_ref = raw_store.save(
        RawPage(
            source="oura",
            endpoint="v2/usercollection/daily_activity",
            retrieved_at=datetime(2026, 9, 11, 4, tzinfo=UTC),
            content=json.dumps(fixture, sort_keys=True).encode(),
            http_status=200,
        ),
        ingestion_run_id=None,
        transform_version=connector.transform_version,
    )

    replay = runner.replay(connector, [revised_ref])

    assert replay.normalized_count == 10
    assert replay.updated_count == 10
    with connect(database, read_only=True) as connection:
        values = dict(
            connection.execute(
                """
                SELECT metric, value FROM observations
                WHERE metric IN ('active_energy_kcal', 'steps')
                """
            ).fetchall()
        )
    assert values == {"active_energy_kcal": 700.0, "steps": 11234.0}
