import json
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from health.connectors import RawPage
from health.db import connect, migrate
from health.ingestion import (
    IngestionProgress,
    IngestionRunner,
    NormalizedRecord,
    RawRef,
    RawStore,
    RetryableIngestionError,
    RetryPolicy,
    WriteDisposition,
)


class FakeSink:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def process(
        self,
        record: NormalizedRecord,
        *,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        self.events.append(f"process:{record.identity}")
        assert raw_ref.value
        assert ingestion_run_id
        return {
            "one": WriteDisposition.INSERTED,
            "two": WriteDisposition.UPDATED,
            "three": WriteDisposition.DUPLICATE,
        }[record.identity]

    def refresh(self) -> None:
        self.events.append("refresh")


class BatchSink(FakeSink):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.batch_sizes: list[int] = []

    def process_many(
        self,
        records: Iterable[NormalizedRecord],
        *,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> tuple[WriteDisposition, ...]:
        batch = tuple(records)
        self.batch_sizes.append(len(batch))
        self.events.append("batch:" + ",".join(record.identity for record in batch))
        assert raw_ref.value
        assert ingestion_run_id
        dispositions = {
            "one": WriteDisposition.INSERTED,
            "two": WriteDisposition.UPDATED,
            "three": WriteDisposition.DUPLICATE,
        }
        return tuple(dispositions[record.identity] for record in batch)


class InterruptingBatchSink(FakeSink):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.calls = 0

    def process_many(
        self,
        records: Iterable[NormalizedRecord],
        *,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> tuple[WriteDisposition, ...]:
        batch = tuple(records)
        assert batch
        assert raw_ref.value
        assert ingestion_run_id
        self.calls += 1
        if self.calls == 1:
            return (WriteDisposition.INSERTED, WriteDisposition.UPDATED)
        raise KeyboardInterrupt


class FakeConnector:
    name = "oura"
    transform_version = "oura-v1"

    def __init__(self, events: list[str], *, transient_failures: int = 0) -> None:
        self.events = events
        self.transient_failures = transient_failures
        self.windows: list[tuple[datetime, datetime]] = []

    def authenticate(self) -> None:
        self.events.append("authenticate")

    def fetch(self, start: datetime, end: datetime):
        self.events.append("fetch")
        self.windows.append((start, end))
        if self.transient_failures:
            self.transient_failures -= 1
            raise RetryableIngestionError("rate limited", retry_after=0.25)
        yield RawPage(
            source=self.name,
            endpoint="usercollection/sleep",
            retrieved_at=end,
            content=b'{"data":[{"id":"one"},{"id":"two"},{"id":"three"}]}',
            http_status=200,
            response_headers={"x-request-id": "request-1"},
        )

    def normalize(self, raw_ref: RawRef, raw_store: RawStore):
        self.events.append("normalize")
        payload = json.loads(raw_store.read(raw_ref))
        return [
            NormalizedRecord("sleep", item["id"], item)
            for item in payload["data"]
        ]


class FailingConnector(FakeConnector):
    def fetch(self, start: datetime, end: datetime):
        self.windows.append((start, end))
        raise RuntimeError("provider unavailable")
        yield  # pragma: no cover


class WrongSourceConnector(FakeConnector):
    def fetch(self, start: datetime, end: datetime):
        yield RawPage(
            source="withings",
            endpoint="measure/getmeas",
            retrieved_at=end,
            content=b"{}",
        )


class ReplayOnlyConnector(FakeConnector):
    def authenticate(self) -> None:
        raise AssertionError("replay must not authenticate")

    def fetch(self, start: datetime, end: datetime):
        raise AssertionError("replay must not fetch")
        yield  # pragma: no cover


@pytest.fixture
def initialized_database(tmp_path: Path, project_root: Path) -> Path:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    return database


def test_sync_is_raw_first_audited_and_advances_state(
    initialized_database: Path, tmp_path: Path
) -> None:
    events: list[str] = []
    end = datetime(2026, 9, 10, 15, tzinfo=UTC)
    store = RawStore(tmp_path / "raw")
    connector = FakeConnector(events)
    runner = IngestionRunner(
        database=initialized_database,
        raw_store=store,
        sink=FakeSink(events),
        now=lambda: end,
    )

    result = runner.sync(connector)

    assert result.requested_start == end - timedelta(days=7)
    assert (result.raw_count, result.normalized_count) == (1, 3)
    assert (result.inserted_count, result.updated_count, result.duplicate_count) == (1, 1, 1)
    assert events == [
        "authenticate",
        "fetch",
        "normalize",
        "process:one",
        "process:two",
        "process:three",
        "refresh",
    ]
    raw_ref = next(store.iter_refs("oura"))
    manifest = store.manifest(raw_ref)
    assert manifest["ingestion_run_id"] == str(result.ingestion_run_id)
    assert manifest["request_metadata"]["window_start"] == result.requested_start.isoformat()

    with connect(initialized_database, read_only=True) as connection:
        audit = connection.execute(
            """
            SELECT status, raw_count, normalized_count, inserted_count,
                   updated_count, duplicate_count
            FROM ingestion_runs WHERE ingestion_run_id = ?
            """,
            [result.ingestion_run_id],
        ).fetchone()
        state = connection.execute(
            "SELECT last_successful_end FROM source_sync_state"
        ).fetchone()[0]
    assert audit == ("succeeded", 1, 3, 1, 1, 1)
    assert state == end


def test_second_sync_uses_overlap_window(
    initialized_database: Path, tmp_path: Path
) -> None:
    events: list[str] = []
    first_end = datetime(2026, 9, 10, 15, tzinfo=UTC)
    second_end = first_end + timedelta(days=1)
    times = iter([first_end, second_end])
    connector = FakeConnector(events)
    runner = IngestionRunner(
        database=initialized_database,
        raw_store=RawStore(tmp_path / "raw"),
        sink=FakeSink(events),
        now=lambda: next(times),
    )

    runner.sync(connector)
    runner.sync(connector)

    assert connector.windows[1] == (first_end - timedelta(hours=72), second_end)


def test_runner_uses_bounded_batches_and_reports_committed_progress(
    initialized_database: Path, tmp_path: Path
) -> None:
    events: list[str] = []
    updates: list[IngestionProgress] = []
    times = iter([100.0, 101.0, 102.0])
    sink = BatchSink(events)
    runner = IngestionRunner(
        database=initialized_database,
        raw_store=RawStore(tmp_path / "raw"),
        sink=sink,
        now=lambda: datetime(2026, 9, 10, 15, tzinfo=UTC),
        batch_size=2,
        progress=updates.append,
        clock=lambda: next(times),
    )

    result = runner.sync(FakeConnector(events))

    assert sink.batch_sizes == [2, 1]
    assert [update.normalized_count for update in updates] == [2, 3]
    assert [update.elapsed_seconds for update in updates] == [1.0, 2.0]
    assert (
        updates[-1].inserted_count,
        updates[-1].updated_count,
        updates[-1].duplicate_count,
    ) == (1, 1, 1)
    assert result.normalized_count == 3
    assert events == [
        "authenticate",
        "fetch",
        "normalize",
        "batch:one,two",
        "batch:three",
        "refresh",
    ]


def test_runner_rejects_invalid_batch_size(
    initialized_database: Path, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="batch_size must be at least one"):
        IngestionRunner(
            database=initialized_database,
            raw_store=RawStore(tmp_path / "raw"),
            sink=FakeSink([]),
            batch_size=0,
        )


def test_interrupted_batch_marks_run_failed(
    initialized_database: Path, tmp_path: Path
) -> None:
    events: list[str] = []
    runner = IngestionRunner(
        database=initialized_database,
        raw_store=RawStore(tmp_path / "raw"),
        sink=InterruptingBatchSink(events),
        now=lambda: datetime(2026, 9, 10, 15, tzinfo=UTC),
        batch_size=2,
    )

    with pytest.raises(KeyboardInterrupt):
        runner.sync(FakeConnector(events))

    with connect(initialized_database, read_only=True) as connection:
        audit = connection.execute(
            """
            SELECT status, normalized_count, inserted_count, error
            FROM ingestion_runs ORDER BY started_at DESC LIMIT 1
            """
        ).fetchone()
    assert audit == ("failed", 2, 1, "KeyboardInterrupt: ")


def test_retryable_fetch_uses_policy(
    initialized_database: Path, tmp_path: Path
) -> None:
    events: list[str] = []
    sleeps: list[float] = []
    connector = FakeConnector(events, transient_failures=1)
    runner = IngestionRunner(
        database=initialized_database,
        raw_store=RawStore(tmp_path / "raw"),
        sink=FakeSink(events),
        retry_policy=RetryPolicy(max_attempts=2),
        now=lambda: datetime(2026, 9, 10, 15, tzinfo=UTC),
        sleeper=sleeps.append,
    )

    result = runner.sync(connector)

    assert result.raw_count == 1
    assert sleeps == [0.25]
    assert events.count("fetch") == 2


def test_failed_sync_is_audited_without_advancing_state(
    initialized_database: Path, tmp_path: Path
) -> None:
    events: list[str] = []
    end = datetime(2026, 9, 10, 15, tzinfo=UTC)
    runner = IngestionRunner(
        database=initialized_database,
        raw_store=RawStore(tmp_path / "raw"),
        sink=FakeSink(events),
        retry_policy=RetryPolicy(max_attempts=1),
        now=lambda: end,
    )

    with pytest.raises(RuntimeError, match="provider unavailable"):
        runner.sync(FailingConnector(events))

    with connect(initialized_database, read_only=True) as connection:
        audit = connection.execute(
            "SELECT status, error FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        states = connection.execute("SELECT count(*) FROM source_sync_state").fetchone()[0]
    assert audit[0] == "failed"
    assert audit[1] == "RuntimeError: provider unavailable"
    assert states == 0


def test_connector_cannot_write_another_sources_raw_namespace(
    initialized_database: Path, tmp_path: Path
) -> None:
    events: list[str] = []
    runner = IngestionRunner(
        database=initialized_database,
        raw_store=RawStore(tmp_path / "raw"),
        sink=FakeSink(events),
        now=lambda: datetime(2026, 9, 10, 15, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="yielded source"):
        runner.sync(WrongSourceConnector(events))

    assert list((tmp_path / "raw").rglob("*.manifest.json")) == []


def test_replay_uses_existing_raw_without_auth_or_network(
    initialized_database: Path, tmp_path: Path
) -> None:
    events: list[str] = []
    store = RawStore(tmp_path / "raw")
    raw_ref = store.save(
        RawPage(
            source="oura",
            endpoint="usercollection/sleep",
            retrieved_at=datetime(2026, 9, 10, 15, tzinfo=UTC),
            content=b'{"data":[{"id":"one"},{"id":"two"},{"id":"three"}]}',
        ),
        ingestion_run_id=None,
        transform_version="fixture-v1",
    )
    runner = IngestionRunner(
        database=initialized_database,
        raw_store=store,
        sink=FakeSink(events),
    )

    result = runner.replay(ReplayOnlyConnector(events), [raw_ref])

    assert result.replay is True
    assert result.requested_start is None
    assert (result.raw_count, result.normalized_count) == (1, 3)
    assert "authenticate" not in events
    assert "fetch" not in events
    with connect(initialized_database, read_only=True) as connection:
        replay = connection.execute(
            "SELECT metadata->>'replay' FROM ingestion_runs WHERE ingestion_run_id = ?",
            [result.ingestion_run_id],
        ).fetchone()[0]
        states = connection.execute("SELECT count(*) FROM source_sync_state").fetchone()[0]
    assert replay == "true"
    assert states == 0
