"""Raw-first ingestion orchestration and offline replay."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import islice
from pathlib import Path
from typing import Protocol
from uuid import UUID

import duckdb

from health.connectors import Connector, RawPage
from health.db import connect
from health.ingestion.models import NormalizedRecord, WriteDisposition
from health.ingestion.raw_store import RawRef, RawStore


class RetryableIngestionError(RuntimeError):
    """A transient provider error that may include a server-requested delay."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class CanonicalSink(Protocol):
    """Validation, deduplication, and upsert boundary for canonical storage."""

    def process(
        self,
        record: NormalizedRecord,
        *,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition: ...

    def process_many(
        self,
        records: Iterable[NormalizedRecord],
        *,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> Iterable[WriteDisposition]: ...

    def refresh(self) -> None: ...


@dataclass(frozen=True, slots=True)
class SyncWindowPolicy:
    initial_lookback: timedelta = timedelta(days=7)
    overlap: timedelta = timedelta(hours=72)

    def determine(
        self,
        *,
        end: datetime,
        last_successful_end: datetime | None,
        requested_start: datetime | None = None,
    ) -> tuple[datetime, datetime]:
        if end.tzinfo is None or end.utcoffset() is None:
            raise ValueError("sync window end must be timezone-aware")
        if requested_start is not None:
            if requested_start.tzinfo is None or requested_start.utcoffset() is None:
                raise ValueError("sync window start must be timezone-aware")
            start = requested_start
        elif last_successful_end is not None:
            start = last_successful_end - self.overlap
        else:
            start = end - self.initial_lookback
        if start > end:
            raise ValueError("sync window start must not be after end")
        return start, end


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one")

    def delay(self, attempt: int, error: RetryableIngestionError) -> float:
        if error.retry_after is not None:
            return min(max(error.retry_after, 0), self.max_delay_seconds)
        return min(self.base_delay_seconds * (2 ** (attempt - 1)), self.max_delay_seconds)


@dataclass(frozen=True, slots=True)
class RunResult:
    ingestion_run_id: UUID
    source: str
    requested_start: datetime | None
    requested_end: datetime | None
    raw_count: int
    normalized_count: int
    inserted_count: int
    updated_count: int
    duplicate_count: int
    replay: bool


@dataclass(frozen=True, slots=True)
class IngestionProgress:
    source: str
    raw_count: int
    normalized_count: int
    inserted_count: int
    updated_count: int
    duplicate_count: int
    elapsed_seconds: float


@dataclass(slots=True)
class _Counts:
    raw: int = 0
    normalized: int = 0
    inserted: int = 0
    updated: int = 0
    duplicate: int = 0

    def record(self, disposition: WriteDisposition) -> None:
        if disposition is WriteDisposition.INSERTED:
            self.inserted += 1
        elif disposition is WriteDisposition.UPDATED:
            self.updated += 1
        elif disposition is WriteDisposition.DUPLICATE:
            self.duplicate += 1
        else:
            raise ValueError(f"unknown write disposition: {disposition}")


class IngestionRunner:
    def __init__(
        self,
        *,
        database: Path,
        raw_store: RawStore,
        sink: CanonicalSink,
        window_policy: SyncWindowPolicy | None = None,
        retry_policy: RetryPolicy | None = None,
        now: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        batch_size: int = 5_000,
        progress: Callable[[IngestionProgress], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least one")
        self.database = database
        self.raw_store = raw_store
        self.sink = sink
        self.window_policy = window_policy or SyncWindowPolicy()
        self.retry_policy = retry_policy or RetryPolicy()
        self.now = now or (lambda: datetime.now(UTC))
        self.sleeper = sleeper
        self.batch_size = batch_size
        self.progress = progress
        self.clock = clock

    @staticmethod
    def _source_id(
        connection: duckdb.DuckDBPyConnection,
        source: str,
        source_type: str = "api",
    ) -> UUID:
        if source_type not in {"api", "import", "manual"}:
            raise ValueError(f"invalid source type: {source_type}")
        connection.execute(
            """
            INSERT INTO sources (name, source_type)
            SELECT ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM sources WHERE name = ?)
            """,
            [source, source_type, source],
        )
        row = connection.execute(
            "SELECT source_id FROM sources WHERE name = ?", [source]
        ).fetchone()
        if row is None:
            raise RuntimeError(f"could not resolve source: {source}")
        return row[0]

    @staticmethod
    def _last_successful_end(
        connection: duckdb.DuckDBPyConnection, source_id: UUID
    ) -> datetime | None:
        row = connection.execute(
            "SELECT last_successful_end FROM source_sync_state WHERE source_id = ?",
            [source_id],
        ).fetchone()
        return row[0] if row else None

    @staticmethod
    def _start_run(
        connection: duckdb.DuckDBPyConnection,
        *,
        source_id: UUID,
        start: datetime | None,
        end: datetime | None,
        replay: bool,
    ) -> UUID:
        return connection.execute(
            """
            INSERT INTO ingestion_runs (
                source_id, requested_start, requested_end, status, metadata
            ) VALUES (?, ?, ?, 'running', ?)
            RETURNING ingestion_run_id
            """,
            [source_id, start, end, json.dumps({"replay": replay})],
        ).fetchone()[0]

    @staticmethod
    def _finish_run(
        connection: duckdb.DuckDBPyConnection,
        *,
        run_id: UUID,
        counts: _Counts,
        status: str,
        error: str | None = None,
    ) -> None:
        connection.execute(
            """
            UPDATE ingestion_runs
            SET finished_at = current_timestamp,
                raw_count = ?,
                normalized_count = ?,
                inserted_count = ?,
                updated_count = ?,
                duplicate_count = ?,
                status = ?,
                error = ?
            WHERE ingestion_run_id = ?
            """,
            [
                counts.raw,
                counts.normalized,
                counts.inserted,
                counts.updated,
                counts.duplicate,
                status,
                error[:4000] if error else None,
                run_id,
            ],
        )

    @staticmethod
    def _checkpoint_run(
        connection: duckdb.DuckDBPyConnection,
        *,
        run_id: UUID,
        counts: _Counts,
    ) -> None:
        connection.execute(
            """
            UPDATE ingestion_runs
            SET raw_count = ?, normalized_count = ?, inserted_count = ?,
                updated_count = ?, duplicate_count = ?
            WHERE ingestion_run_id = ?
            """,
            [
                counts.raw,
                counts.normalized,
                counts.inserted,
                counts.updated,
                counts.duplicate,
                run_id,
            ],
        )

    @staticmethod
    def _advance_state(
        connection: duckdb.DuckDBPyConnection,
        *,
        source_id: UUID,
        end: datetime,
        run_id: UUID,
    ) -> None:
        connection.execute(
            """
            INSERT INTO source_sync_state (
                source_id, last_successful_end, last_ingestion_run_id
            ) VALUES (?, ?, ?)
            ON CONFLICT (source_id) DO UPDATE SET
                last_successful_end = excluded.last_successful_end,
                last_ingestion_run_id = excluded.last_ingestion_run_id,
                updated_at = excluded.updated_at
            """,
            [source_id, end, run_id],
        )

    def _fetch_with_retries(
        self, connector: Connector, start: datetime, end: datetime
    ) -> Iterator[RawPage]:
        for attempt in range(1, self.retry_policy.max_attempts + 1):
            try:
                yield from connector.fetch(start, end)
                return
            except RetryableIngestionError as exc:
                if attempt == self.retry_policy.max_attempts:
                    raise
                self.sleeper(self.retry_policy.delay(attempt, exc))

    def _process_ref(
        self,
        connector: Connector,
        *,
        raw_ref: RawRef,
        run_id: UUID,
        counts: _Counts,
        source: str,
        audit_connection: duckdb.DuckDBPyConnection,
        started_at: float,
    ) -> None:
        self.raw_store.read(raw_ref)
        records = iter(connector.normalize(raw_ref, self.raw_store))
        process_many = getattr(self.sink, "process_many", None)
        while batch := tuple(islice(records, self.batch_size)):
            if callable(process_many):
                dispositions = tuple(
                    process_many(
                        batch,
                        raw_ref=raw_ref,
                        ingestion_run_id=run_id,
                    )
                )
                if len(dispositions) != len(batch):
                    raise RuntimeError("canonical sink returned the wrong batch result count")
            else:
                dispositions = tuple(
                    self.sink.process(
                        record,
                        raw_ref=raw_ref,
                        ingestion_run_id=run_id,
                    )
                    for record in batch
                )
            counts.normalized += len(batch)
            for disposition in dispositions:
                counts.record(disposition)
            self._checkpoint_run(audit_connection, run_id=run_id, counts=counts)
            if self.progress is not None:
                self.progress(
                    IngestionProgress(
                        source=source,
                        raw_count=counts.raw,
                        normalized_count=counts.normalized,
                        inserted_count=counts.inserted,
                        updated_count=counts.updated,
                        duplicate_count=counts.duplicate,
                        elapsed_seconds=self.clock() - started_at,
                    )
                )

    def sync(
        self,
        connector: Connector,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> RunResult:
        requested_end = end or self.now()
        counts = _Counts()
        started_at = self.clock()
        with connect(self.database) as connection:
            source_id = self._source_id(
                connection,
                connector.name,
                getattr(connector, "source_type", "api"),
            )
            requested_start, requested_end = self.window_policy.determine(
                end=requested_end,
                last_successful_end=self._last_successful_end(connection, source_id),
                requested_start=start,
            )
            run_id = self._start_run(
                connection,
                source_id=source_id,
                start=requested_start,
                end=requested_end,
                replay=False,
            )
            try:
                connector.authenticate()
                for page in self._fetch_with_retries(
                    connector, requested_start, requested_end
                ):
                    if page.source != connector.name:
                        raise ValueError(
                            f"connector {connector.name!r} yielded source {page.source!r}"
                        )
                    metadata = {
                        **page.request_metadata,
                        "window_start": requested_start.isoformat(),
                        "window_end": requested_end.isoformat(),
                    }
                    page = RawPage(
                        source=page.source,
                        endpoint=page.endpoint,
                        retrieved_at=page.retrieved_at,
                        content=page.content,
                        content_type=page.content_type,
                        request_metadata=metadata,
                        http_status=page.http_status,
                        response_headers=page.response_headers,
                    )
                    raw_ref = self.raw_store.save(
                        page,
                        ingestion_run_id=str(run_id),
                        transform_version=connector.transform_version,
                    )
                    counts.raw += 1
                    self._process_ref(
                        connector,
                        raw_ref=raw_ref,
                        run_id=run_id,
                        counts=counts,
                        source=connector.name,
                        audit_connection=connection,
                        started_at=started_at,
                    )
                self.sink.refresh()
                connection.execute("BEGIN TRANSACTION")
                try:
                    self._finish_run(
                        connection, run_id=run_id, counts=counts, status="succeeded"
                    )
                    self._advance_state(
                        connection,
                        source_id=source_id,
                        end=requested_end,
                        run_id=run_id,
                    )
                    connection.execute("COMMIT")
                except Exception:
                    connection.execute("ROLLBACK")
                    raise
            except (Exception, KeyboardInterrupt) as exc:
                self._finish_run(
                    connection,
                    run_id=run_id,
                    counts=counts,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                )
                raise

        return RunResult(
            ingestion_run_id=run_id,
            source=connector.name,
            requested_start=requested_start,
            requested_end=requested_end,
            raw_count=counts.raw,
            normalized_count=counts.normalized,
            inserted_count=counts.inserted,
            updated_count=counts.updated,
            duplicate_count=counts.duplicate,
            replay=False,
        )

    def replay(
        self, connector: Connector, raw_refs: Iterable[RawRef]
    ) -> RunResult:
        counts = _Counts()
        started_at = self.clock()
        with connect(self.database) as connection:
            source_id = self._source_id(
                connection,
                connector.name,
                getattr(connector, "source_type", "api"),
            )
            run_id = self._start_run(
                connection,
                source_id=source_id,
                start=None,
                end=None,
                replay=True,
            )
            try:
                for raw_ref in raw_refs:
                    counts.raw += 1
                    self._process_ref(
                        connector,
                        raw_ref=raw_ref,
                        run_id=run_id,
                        counts=counts,
                        source=connector.name,
                        audit_connection=connection,
                        started_at=started_at,
                    )
                self.sink.refresh()
                self._finish_run(
                    connection, run_id=run_id, counts=counts, status="succeeded"
                )
            except (Exception, KeyboardInterrupt) as exc:
                self._finish_run(
                    connection,
                    run_id=run_id,
                    counts=counts,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                )
                raise

        return RunResult(
            ingestion_run_id=run_id,
            source=connector.name,
            requested_start=None,
            requested_end=None,
            raw_count=counts.raw,
            normalized_count=counts.normalized,
            inserted_count=counts.inserted,
            updated_count=counts.updated,
            duplicate_count=counts.duplicate,
            replay=True,
        )
