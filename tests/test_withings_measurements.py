import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs
from uuid import UUID

import httpx
import pytest

from health.auth import FileSecretStore, OAuthTokenPair
from health.connectors import Connector
from health.connectors.withings import (
    MEASURE_TYPE_CODES,
    WithingsMeasurementConnector,
    WithingsOAuth,
    WithingsOAuthConfig,
    WithingsPaginationError,
    WithingsPayloadError,
    parse_measurement_envelope,
)
from health.db import connect, migrate
from health.ingestion import (
    IngestionRunner,
    NormalizedRecord,
    RawRef,
    RawStore,
    RetryPolicy,
    WriteDisposition,
)

START = datetime(2026, 9, 1, tzinfo=UTC)
END = datetime(2026, 9, 10, 15, tzinfo=UTC)


class NoopSink:
    def __init__(self) -> None:
        self.refreshes = 0

    def process(
        self,
        record: NormalizedRecord,
        *,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        raise AssertionError("HOK-2978 must not emit canonical records")

    def refresh(self) -> None:
        self.refreshes += 1


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def make_connector(
    tmp_path: Path,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    now: Callable[[], datetime] = lambda: END,
    max_pages: int = 200,
) -> WithingsMeasurementConnector:
    secret_store = FileSecretStore(tmp_path / "secrets")
    secret_store.save_tokens(
        "withings",
        OAuthTokenPair(
            access_token="synthetic-access-token",
            refresh_token="synthetic-refresh-token",
            expires_at=END + timedelta(days=30),
        ),
    )
    oauth = WithingsOAuth(
        config=WithingsOAuthConfig(
            client_id="synthetic-client",
            client_secret="synthetic-secret",
            redirect_uri="http://localhost:8765/callback",
        ),
        secret_store=secret_store,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: pytest.fail("valid token must not refresh")
            )
        ),
        now=now,
    )
    return WithingsMeasurementConnector(
        oauth=oauth,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=now,
        api_base_url="https://withings.test",
        max_pages=max_pages,
    )


def initialized_database(tmp_path: Path, project_root: Path) -> Path:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    return database


def test_bounded_paginated_sync_persists_exact_envelopes_and_audit(
    tmp_path: Path,
    project_root: Path,
) -> None:
    first = project_root / "tests/fixtures/providers/withings/measure-page-1.json"
    second = project_root / "tests/fixtures/providers/withings/measure-page-2.json"
    forms: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer synthetic-access-token"
        form = parse_qs(request.content.decode())
        forms.append(form)
        fixture = second if form.get("offset") == ["2"] else first
        return httpx.Response(
            200,
            content=fixture.read_bytes(),
            headers={
                "Content-Type": "application/json",
                "X-Withings-Request-Id": f"request-{len(forms)}",
            },
        )

    database = initialized_database(tmp_path, project_root)
    store = RawStore(tmp_path / "raw")
    sink = NoopSink()
    runner = IngestionRunner(
        database=database,
        raw_store=store,
        sink=sink,
        now=lambda: END,
    )

    connector = make_connector(tmp_path, handler)
    assert isinstance(connector, Connector)

    result = runner.sync(connector, start=START, end=END)

    assert result.requested_start == START
    assert result.requested_end == END
    assert result.raw_count == 2
    assert result.normalized_count == 0
    assert sink.refreshes == 1
    assert forms[0] == {
        "action": ["getmeas"],
        "meastypes": [",".join(str(code) for code in MEASURE_TYPE_CODES)],
        "category": ["1"],
        "startdate": [str(int(START.timestamp()))],
        "enddate": [str(int(END.timestamp()))],
    }
    assert forms[1]["offset"] == ["2"]

    pages: dict[int, tuple[bytes, dict]] = {}
    for raw_ref in store.iter_refs("withings"):
        manifest = store.manifest(raw_ref)
        page_index = manifest["request_metadata"]["page_index"]
        pages[page_index] = (store.read(raw_ref), manifest)
    assert pages[0][0] == first.read_bytes()
    assert pages[1][0] == second.read_bytes()
    assert pages[0][1]["request_metadata"]["requested_offset"] is None
    assert pages[1][1]["request_metadata"]["requested_offset"] == 2
    assert pages[0][1]["request_metadata"]["window_start"] == START.isoformat()
    assert pages[0][1]["response_headers"]["x-withings-request-id"] == "request-1"
    manifests = [manifest for _content, manifest in pages.values()]
    assert "authorization" not in json.dumps(manifests)
    assert "synthetic-access-token" not in json.dumps(manifests)

    first_page = parse_measurement_envelope(pages[0][0])
    assert first_page.body is not None
    assert first_page.body.timezone == "America/New_York"
    assert first_page.body.more == 1
    assert first_page.body.offset == 2
    assert first_page.body.measuregrps[0].grpid == 42001
    assert first_page.body.measuregrps[0].timezone == "America/New_York"
    bp_measures = first_page.body.measuregrps[1].measures
    assert [(item.type, item.value, item.unit) for item in bp_measures] == [
        (9, 78, 0),
        (10, 122, 0),
        (11, 61, 0),
    ]

    with connect(database, read_only=True) as connection:
        audit = connection.execute(
            """
            SELECT status, requested_start, requested_end, raw_count, normalized_count
            FROM ingestion_runs WHERE ingestion_run_id = ?
            """,
            [result.ingestion_run_id],
        ).fetchone()
        watermark = connection.execute(
            "SELECT last_successful_end FROM source_sync_state"
        ).fetchone()[0]
    assert audit == ("succeeded", START, END, 2, 0)
    assert watermark == END


def test_incremental_sync_uses_project_owned_72_hour_overlap(
    tmp_path: Path,
    project_root: Path,
) -> None:
    fixture = project_root / "tests/fixtures/providers/withings/measure-page-2.json"
    forms: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        forms.append(parse_qs(request.content.decode()))
        return httpx.Response(200, content=fixture.read_bytes())

    database = initialized_database(tmp_path, project_root)
    clock = MutableClock(END)
    runner = IngestionRunner(
        database=database,
        raw_store=RawStore(tmp_path / "raw"),
        sink=NoopSink(),
        now=clock,
    )
    connector = make_connector(tmp_path, handler, now=clock)

    runner.sync(connector)
    clock.value = END + timedelta(days=1)
    runner.sync(connector)

    assert forms[0]["startdate"] == [str(int((END - timedelta(days=7)).timestamp()))]
    assert forms[1]["startdate"] == [str(int((END - timedelta(hours=72)).timestamp()))]
    assert forms[1]["enddate"] == [str(int(clock.value.timestamp()))]


def test_rate_limit_envelope_is_saved_before_retry_and_success(
    tmp_path: Path,
    project_root: Path,
) -> None:
    limited = b'{"status":601,"body":{"error":"synthetic provider detail"}}'
    success = (project_root / "tests/fixtures/providers/withings/measure-page-2.json").read_bytes()
    responses = iter([limited, success])
    sleeps: list[float] = []
    database = initialized_database(tmp_path, project_root)
    store = RawStore(tmp_path / "raw")
    runner = IngestionRunner(
        database=database,
        raw_store=store,
        sink=NoopSink(),
        retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0.25),
        now=lambda: END,
        sleeper=sleeps.append,
    )
    connector = make_connector(
        tmp_path,
        lambda _request: httpx.Response(200, content=next(responses)),
    )

    result = runner.sync(connector, start=START, end=END)

    assert result.raw_count == 2
    assert sleeps == [0.25]
    persisted = [store.read(raw_ref) for raw_ref in store.iter_refs("withings")]
    assert limited in persisted
    assert success in persisted


def test_failed_pagination_is_audited_after_raw_page_is_saved(
    tmp_path: Path,
    project_root: Path,
) -> None:
    nonadvancing = b'{"status":0,"body":{"measuregrps":[],"more":1,"offset":0}}'
    database = initialized_database(tmp_path, project_root)
    store = RawStore(tmp_path / "raw")
    runner = IngestionRunner(
        database=database,
        raw_store=store,
        sink=NoopSink(),
        now=lambda: END,
    )

    with pytest.raises(WithingsPaginationError, match="did not advance"):
        runner.sync(
            make_connector(
                tmp_path,
                lambda _request: httpx.Response(200, content=nonadvancing),
            ),
            start=START,
            end=END,
        )

    raw_ref = next(store.iter_refs("withings"))
    assert store.read(raw_ref) == nonadvancing
    with connect(database, read_only=True) as connection:
        audit = connection.execute(
            "SELECT status, raw_count, error FROM ingestion_runs"
        ).fetchone()
        watermarks = connection.execute(
            "SELECT count(*) FROM source_sync_state"
        ).fetchone()[0]
    assert audit[0:2] == ("failed", 1)
    assert "WithingsPaginationError" in audit[2]
    assert watermarks == 0


def test_invalid_success_schema_is_saved_before_validation_failure(
    tmp_path: Path,
    project_root: Path,
) -> None:
    malformed = b'{"status":0,"body":{"measuregrps":"not-a-list","more":0}}'
    database = initialized_database(tmp_path, project_root)
    store = RawStore(tmp_path / "raw")
    runner = IngestionRunner(
        database=database,
        raw_store=store,
        sink=NoopSink(),
        now=lambda: END,
    )

    with pytest.raises(WithingsPayloadError, match="schema validation"):
        runner.sync(
            make_connector(
                tmp_path,
                lambda _request: httpx.Response(200, content=malformed),
            ),
            start=START,
            end=END,
        )

    raw_ref = next(store.iter_refs("withings"))
    assert store.read(raw_ref) == malformed
    with connect(database, read_only=True) as connection:
        audit = connection.execute(
            "SELECT status, raw_count, normalized_count FROM ingestion_runs"
        ).fetchone()
    assert audit == ("failed", 1, 0)


def test_connector_rejects_unbounded_or_reversed_datetime_inputs(tmp_path: Path) -> None:
    connector = make_connector(
        tmp_path,
        lambda _request: pytest.fail("invalid bounds must not make a request"),
    )

    with pytest.raises(ValueError, match="start must be timezone-aware"):
        connector.fetch(START.replace(tzinfo=None), END)
    with pytest.raises(ValueError, match="end must be timezone-aware"):
        connector.fetch(START, END.replace(tzinfo=None))
    with pytest.raises(ValueError, match="must not be after"):
        connector.fetch(END, START)
