import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs

import httpx

from health.auth import FileSecretStore, OAuthTokenPair
from health.connectors import RawPage
from health.connectors.withings import (
    WithingsMeasurementConnector,
    WithingsOAuth,
    WithingsOAuthConfig,
)
from health.db import connect, migrate
from health.ingestion import DuckDBCanonicalSink, IngestionRunner, RawStore

NOW = datetime(2026, 9, 10, 15, tzinfo=UTC)
START = NOW - timedelta(days=7)


def make_connector(
    tmp_path: Path,
    handler,
) -> WithingsMeasurementConnector:
    secrets = FileSecretStore(tmp_path / "secrets")
    secrets.save_tokens(
        "withings",
        OAuthTokenPair(
            access_token="synthetic-access",
            refresh_token="synthetic-refresh",
            expires_at=NOW + timedelta(days=1),
        ),
    )
    oauth = WithingsOAuth(
        config=WithingsOAuthConfig(
            client_id="synthetic-client",
            client_secret="synthetic-secret",
            redirect_uri="http://localhost/callback",
        ),
        secret_store=secrets,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: (_ for _ in ()).throw(
                    AssertionError("valid token must not refresh")
                )
            )
        ),
        now=lambda: NOW,
    )
    return WithingsMeasurementConnector(
        oauth=oauth,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        api_base_url="https://withings.test",
    )


def save_fixture(store: RawStore, fixture: Path):
    return store.save(
        RawPage(
            source="withings",
            endpoint="measure/getmeas",
            retrieved_at=NOW,
            content=fixture.read_bytes(),
            http_status=200,
        ),
        ingestion_run_id=None,
        transform_version="withings-measurements-v1",
    )


def test_normalize_weight_and_body_composition_with_provenance(
    tmp_path: Path,
    project_root: Path,
) -> None:
    store = RawStore(tmp_path / "raw")
    fixture = project_root / "tests/fixtures/providers/withings/measure-page-1.json"
    raw_ref = save_fixture(store, fixture)
    connector = make_connector(
        tmp_path,
        lambda _request: (_ for _ in ()).throw(AssertionError("replay must be offline")),
    )

    records = list(connector.normalize(raw_ref, store))

    canonical_values = [
        (record.values["metric"], record.values["value"], record.values["unit"])
        for record in records
    ]
    assert canonical_values == [
        ("weight_kg", 82.1, "kg"),
        ("lean_mass_kg", 65.4, "kg"),
        ("body_fat_pct", 20.3, "%"),
        ("body_fat_mass_kg", 16.7, "kg"),
    ]
    assert {record.values["source_record_id"] for record in records} == {"42001"}
    assert all(record.values["quality"] == "valid" for record in records)
    assert all(record.values["timezone"] == "America/New_York" for record in records)
    assert all(
        record.values["device"]["vendor_device_id"] == "synthetic-scale-device"
        for record in records
    )
    weight = records[0].values
    assert weight["original_metric"] == "withings_meastype_1"
    assert weight["original_value"] == 82100.0
    assert weight["original_unit"] == "10^-3 kg"
    assert weight["metadata"]["withings"]["grpid"] == 42001
    assert weight["metadata"]["withings"]["group"]["created"] == 1789041660
    assert weight["metadata"]["withings"]["measure"]["unit"] == -3
    assert weight["metadata"]["withings"]["response"]["updatetime"] == 1789041900


def test_missing_group_id_invalid_timezone_and_implausible_value_are_suspect(
    tmp_path: Path,
) -> None:
    content = json.dumps(
        {
            "status": 0,
            "body": {
                "timezone": "Mars/Olympus",
                "measuregrps": [
                    {
                        "date": int(NOW.timestamp()),
                        "deviceid": "synthetic-scale",
                        "measures": [{"type": 1, "value": 1000000, "unit": -3}],
                    }
                ],
                "more": 0,
                "offset": 0,
            },
        }
    ).encode()
    store = RawStore(tmp_path / "raw")
    connector = make_connector(tmp_path, lambda _request: httpx.Response(500))
    identities: list[str] = []
    for minute in (0, 1):
        raw_ref = store.save(
            RawPage(
                source="withings",
                endpoint="measure/getmeas",
                retrieved_at=NOW + timedelta(minutes=minute),
                content=content,
                http_status=200,
            ),
            ingestion_run_id=None,
            transform_version="withings-measurements-v1",
        )
        record = next(iter(connector.normalize(raw_ref, store)))
        identities.append(record.identity)
        assert record.values["quality"] == "suspect"
        assert record.values["timezone"] is None
        assert record.values["local_date"] == NOW.date()
        assert record.values["value"] == 1000.0
        assert set(record.values["metadata"]["quality_reasons"]) == {
            "missing_grpid",
            "invalid_timezone",
            "implausible_value",
        }

    assert identities[0] == identities[1]
    assert identities[0].startswith("missing-grpid:")


def test_runner_writes_observations_devices_and_replays_as_duplicates(
    tmp_path: Path,
    project_root: Path,
) -> None:
    first = project_root / "tests/fixtures/providers/withings/measure-page-1.json"
    second = project_root / "tests/fixtures/providers/withings/measure-page-2.json"

    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        fixture = second if form.get("offset") == ["2"] else first
        return httpx.Response(200, content=fixture.read_bytes())

    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    store = RawStore(tmp_path / "raw")
    connector = make_connector(tmp_path, handler)
    runner = IngestionRunner(
        database=database,
        raw_store=store,
        sink=DuckDBCanonicalSink(database),
        now=lambda: NOW,
    )

    sync = runner.sync(connector, start=START, end=NOW)

    assert (sync.raw_count, sync.normalized_count, sync.inserted_count) == (2, 7, 7)
    with connect(database, read_only=True) as connection:
        observations = connection.execute(
            """
            SELECT metric, value, unit, source_record_id, quality, timezone,
                   raw_file, transform_version,
                   json_extract_string(metadata, '$.ingestion_run_id')
            FROM observations ORDER BY metric
            """
        ).fetchall()
        devices = connection.execute(
            """
            SELECT manufacturer, model, name,
                   json_extract_string(metadata, '$.vendor_device_id')
            FROM devices
            """
        ).fetchall()
        device_links = connection.execute(
            """
            SELECT count(DISTINCT device_id),
                   count(*) FILTER (WHERE device_id IS NULL)
            FROM observations
            """
        ).fetchone()
    assert len(observations) == 7
    assert {row[0]: row[1] for row in observations} == {
        "body_fat_mass_kg": 16.7,
        "body_fat_pct": 20.3,
        "body_water_mass_kg": 45.1,
        "bone_mass_kg": 3.2,
        "lean_mass_kg": 65.4,
        "skeletal_muscle_mass_kg": 34.2,
        "weight_kg": 82.1,
    }
    assert {row[0]: row[3] for row in observations} == {
        "body_fat_mass_kg": "42001",
        "body_fat_pct": "42001",
        "body_water_mass_kg": "42003",
        "bone_mass_kg": "42003",
        "lean_mass_kg": "42001",
        "skeletal_muscle_mass_kg": "42003",
        "weight_kg": "42001",
    }
    assert all(row[4] == "valid" for row in observations)
    assert all(row[5] == "America/New_York" for row in observations)
    assert all(row[6].startswith("withings/") for row in observations)
    assert all(row[7] == "withings-measurements-v2" for row in observations)
    assert all(row[8] == str(sync.ingestion_run_id) for row in observations)
    assert devices == [
        ("Withings", "Body Comp", "synthetic-scale-device", "synthetic-scale-device")
    ]
    assert device_links == (1, 0)

    replay = runner.replay(connector, list(store.iter_refs("withings")))

    assert replay.normalized_count == 7
    assert replay.duplicate_count == 7
    assert replay.inserted_count == 0
    with connect(database, read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM observations").fetchone()[0] == 7


def test_replay_updates_changed_vendor_record_without_duplication(
    tmp_path: Path,
    project_root: Path,
) -> None:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    store = RawStore(tmp_path / "raw")
    connector = make_connector(tmp_path, lambda _request: httpx.Response(500))
    runner = IngestionRunner(
        database=database,
        raw_store=store,
        sink=DuckDBCanonicalSink(database),
    )
    initial = {
        "status": 0,
        "body": {
            "timezone": "America/New_York",
            "measuregrps": [
                {
                    "grpid": 99,
                    "date": int(NOW.timestamp()),
                    "measures": [{"type": 1, "value": 82100, "unit": -3}],
                }
            ],
        },
    }

    def persist(document: dict, minute: int):
        return store.save(
            RawPage(
                source="withings",
                endpoint="measure/getmeas",
                retrieved_at=NOW + timedelta(minutes=minute),
                content=json.dumps(document).encode(),
                http_status=200,
            ),
            ingestion_run_id=None,
            transform_version="withings-measurements-v2",
        )

    first_ref = persist(initial, 0)
    first = runner.replay(connector, [first_ref])
    changed = json.loads(json.dumps(initial))
    changed["body"]["measuregrps"][0]["measures"][0]["value"] = 83000
    second_ref = persist(changed, 1)
    second = runner.replay(connector, [second_ref])

    assert first.inserted_count == 1
    assert second.updated_count == 1
    with connect(database, read_only=True) as connection:
        rows = connection.execute(
            "SELECT value, source_record_id, raw_file FROM observations"
        ).fetchall()
    assert rows == [(83.0, "99", second_ref.value)]
