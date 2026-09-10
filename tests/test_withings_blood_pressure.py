import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

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


def make_connector(tmp_path: Path) -> WithingsMeasurementConnector:
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
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: (_ for _ in ()).throw(
                    AssertionError("replay must not use the network")
                )
            )
        ),
        now=lambda: NOW,
        api_base_url="https://withings.test",
    )


def save_content(store: RawStore, content: bytes, *, minute: int = 0):
    return store.save(
        RawPage(
            source="withings",
            endpoint="measure/getmeas",
            retrieved_at=NOW + timedelta(minutes=minute),
            content=content,
            http_status=200,
        ),
        ingestion_run_id=None,
        transform_version="withings-measurements-v3",
    )


def measurement_document(groups: list[dict], *, timezone: str = "America/New_York") -> bytes:
    return json.dumps(
        {
            "status": 0,
            "body": {
                "updatetime": int(NOW.timestamp()),
                "timezone": timezone,
                "measuregrps": groups,
                "more": 0,
                "offset": 0,
            },
        }
    ).encode()


def test_normalizes_one_blood_pressure_event_with_provenance(
    tmp_path: Path,
    project_root: Path,
) -> None:
    store = RawStore(tmp_path / "raw")
    fixture = project_root / "tests/fixtures/providers/withings/measure-page-1.json"
    raw_ref = save_content(store, fixture.read_bytes())

    records = [
        record
        for record in make_connector(tmp_path).normalize(raw_ref, store)
        if record.record_type == "blood_pressure"
    ]

    assert len(records) == 1
    record = records[0]
    assert record.identity == "42002:blood_pressure"
    assert record.values["systolic_mmhg"] == 122.0
    assert record.values["diastolic_mmhg"] == 78.0
    assert record.values["pulse_bpm"] == 61.0
    assert record.values["measurement_number"] is None
    assert record.values["source_record_id"] == "42002"
    assert record.values["source_group_id"] == "42002"
    assert record.values["quality"] == "valid"
    assert record.values["local_date"].isoformat() == "2026-09-10"
    assert record.values["device"] == {
        "manufacturer": "Withings",
        "model": "BPM Connect",
        "vendor_device_id": "synthetic-bp-device",
    }
    metadata = record.values["metadata"]
    assert metadata["timezone"] == "America/New_York"
    assert metadata["quality_reasons"] == []
    assert metadata["withings"]["grpid"] == 42002
    assert metadata["withings"]["group"]["measures"] == [
        {"value": 78, "type": 9, "unit": 0, "position": 2},
        {"value": 122, "type": 10, "unit": 0, "position": 1},
        {"value": 61, "type": 11, "unit": 0, "position": 3},
    ]
    assert metadata["withings"]["response"]["updatetime"] == 1789041900


def test_flags_suspect_group_and_skips_incomplete_pressure_pair(tmp_path: Path) -> None:
    groups = [
        {
            "date": int(NOW.timestamp()),
            "measures": [
                {"type": 9, "value": 220, "unit": 0},
                {"type": 10, "value": 20, "unit": 0},
                {"type": 10, "value": 21, "unit": 0},
            ],
        },
        {
            "grpid": 9002,
            "date": int(NOW.timestamp()),
            "measures": [
                {"type": 10, "value": 120, "unit": 0},
                {"type": 11, "value": 60, "unit": 0},
            ],
        },
    ]
    content = measurement_document(groups, timezone="Mars/Olympus")
    store = RawStore(tmp_path / "raw")
    connector = make_connector(tmp_path)
    identities: list[str] = []

    for minute in (0, 1):
        raw_ref = save_content(store, content, minute=minute)
        records = [
            record
            for record in connector.normalize(raw_ref, store)
            if record.record_type == "blood_pressure"
        ]
        assert len(records) == 1
        record = records[0]
        identities.append(record.identity)
        assert record.values["quality"] == "suspect"
        assert record.values["pulse_bpm"] is None
        assert record.values["local_date"] == NOW.date()
        assert set(record.values["metadata"]["quality_reasons"]) == {
            "missing_grpid",
            "invalid_timezone",
            "duplicate_systolic_mmhg",
            "missing_pulse_bpm",
            "implausible_systolic_mmhg",
            "implausible_diastolic_mmhg",
            "systolic_not_above_diastolic",
        }

    assert identities[0] == identities[1]
    assert identities[0].startswith("missing-grpid:")


def test_sink_preserves_repeated_readings_and_replays_deterministically(
    tmp_path: Path,
    project_root: Path,
) -> None:
    groups = [
        {
            "grpid": 1001,
            "date": int(NOW.timestamp()),
            "deviceid": "synthetic-bp-device",
            "model": "BPM Connect",
            "measures": [
                {"type": 9, "value": 78, "unit": 0},
                {"type": 10, "value": 122, "unit": 0},
                {"type": 11, "value": 61, "unit": 0},
            ],
        },
        {
            "grpid": 1002,
            "date": int((NOW + timedelta(minutes=5)).timestamp()),
            "deviceid": "synthetic-bp-device",
            "model": "BPM Connect",
            "measures": [
                {"type": 9, "value": 76, "unit": 0},
                {"type": 10, "value": 118, "unit": 0},
                {"type": 11, "value": 58, "unit": 0},
            ],
        },
    ]
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    store = RawStore(tmp_path / "raw")
    connector = make_connector(tmp_path)
    runner = IngestionRunner(
        database=database,
        raw_store=store,
        sink=DuckDBCanonicalSink(database),
        now=lambda: NOW,
    )
    first_ref = save_content(store, measurement_document(groups))

    first = runner.replay(connector, [first_ref])
    replay = runner.replay(connector, [first_ref])

    assert (first.normalized_count, first.inserted_count) == (2, 2)
    assert (replay.normalized_count, replay.duplicate_count) == (2, 2)
    with connect(database, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT systolic_mmhg, diastolic_mmhg, pulse_bpm,
                   source_record_id, source_group_id, quality, raw_file,
                   transform_version, json_extract_string(metadata, '$.timezone')
            FROM blood_pressure ORDER BY measured_at
            """
        ).fetchall()
        devices = connection.execute(
            """
            SELECT manufacturer, model,
                   json_extract_string(metadata, '$.vendor_device_id')
            FROM devices
            """
        ).fetchall()
    assert rows == [
        (
            122.0,
            78.0,
            61.0,
            "1001",
            "1001",
            "valid",
            first_ref.value,
            "withings-measurements-v3",
            "America/New_York",
        ),
        (
            118.0,
            76.0,
            58.0,
            "1002",
            "1002",
            "valid",
            first_ref.value,
            "withings-measurements-v3",
            "America/New_York",
        ),
    ]
    assert devices == [("Withings", "BPM Connect", "synthetic-bp-device")]

    changed = json.loads(measurement_document(groups))
    changed["body"]["measuregrps"][1]["measures"][1]["value"] = 119
    changed_ref = save_content(store, json.dumps(changed).encode(), minute=2)
    update = runner.replay(connector, [changed_ref])

    assert (update.updated_count, update.duplicate_count) == (1, 1)
    with connect(database, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT source_record_id, systolic_mmhg, raw_file
            FROM blood_pressure ORDER BY source_record_id
            """
        ).fetchall()
    assert rows == [
        ("1001", 122.0, first_ref.value),
        ("1002", 119.0, changed_ref.value),
    ]
