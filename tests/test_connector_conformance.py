import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from health.connectors import RawPage
from health.db import migrate
from health.ingestion import (
    IngestionRunner,
    NormalizedRecord,
    RawRef,
    RawStore,
    WriteDisposition,
)


class FixtureConnector:
    transform_version = "fixture-v1"

    def __init__(self, source: str, fixture: Path) -> None:
        self.name = source
        self.fixture = fixture
        self.credentials_used = False

    def authenticate(self) -> None:
        # Conformance fixtures must never need credentials.
        self.credentials_used = False

    def fetch(self, start: datetime, end: datetime):
        yield RawPage(
            source=self.name,
            endpoint="fixture",
            retrieved_at=end,
            content=self.fixture.read_bytes(),
        )

    def normalize(self, raw_ref: RawRef, raw_store: RawStore):
        payload = json.loads(raw_store.read(raw_ref))
        if self.name == "oura":
            values = payload["data"]
            identities = [item["id"] for item in values]
        else:
            values = payload["body"]["measuregrps"]
            identities = [str(item["grpid"]) for item in values]
        return [
            NormalizedRecord(self.name, identity, value)
            for identity, value in zip(identities, values, strict=True)
        ]


class CollectingSink:
    def __init__(self) -> None:
        self.identities: list[str] = []

    def process(
        self,
        record: NormalizedRecord,
        *,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        assert raw_ref.value
        assert ingestion_run_id
        self.identities.append(record.identity)
        return WriteDisposition.INSERTED

    def refresh(self) -> None:
        return None


@pytest.mark.parametrize(
    ("source", "expected_identity"),
    [("oura", "sleep-2026-09-09"), ("withings", "42001")],
)
def test_provider_fixture_conforms_without_credentials(
    source: str,
    expected_identity: str,
    tmp_path: Path,
    project_root: Path,
) -> None:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    connector = FixtureConnector(
        source,
        project_root / "tests" / "fixtures" / "providers" / source / "page.json",
    )
    sink = CollectingSink()
    runner = IngestionRunner(
        database=database,
        raw_store=RawStore(tmp_path / "raw"),
        sink=sink,
        now=lambda: datetime(2026, 9, 10, 15, tzinfo=UTC),
    )

    result = runner.sync(connector)

    assert connector.credentials_used is False
    assert result.raw_count == 1
    assert result.inserted_count == 1
    assert sink.identities == [expected_identity]
