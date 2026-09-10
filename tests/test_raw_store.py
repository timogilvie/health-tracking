import hashlib
import json
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest

from health.connectors import RawPage
from health.ingestion import RawIntegrityError, RawRef, RawStorageError, RawStore


def page(content: bytes = b'{"weight":82.1}') -> RawPage:
    return RawPage(
        source="withings",
        endpoint="measure/getmeas",
        retrieved_at=datetime(2026, 9, 10, 14, 30, tzinfo=UTC),
        content=content,
        request_metadata={
            "start": "2026-09-01T00:00:00Z",
            "end": "2026-09-10T14:30:00Z",
            "page_token": "offset-12",
            "access_token": "must-not-leak",
        },
        http_status=200,
        response_headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer must-not-leak",
        },
    )


def test_save_persists_exact_bytes_and_complete_manifest(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "raw")
    content = b'{"weight":82.1}\n'

    raw_ref = store.save(
        page(content),
        ingestion_run_id="run-1",
        transform_version="withings-v1",
    )
    manifest = store.manifest(raw_ref)
    payload_path = store.root / raw_ref.value

    assert store.read(raw_ref) == content
    assert raw_ref.value.startswith("withings/2026/09/10/measure-getmeas__")
    assert manifest["sha256"] == hashlib.sha256(content).hexdigest()
    assert manifest["byte_length"] == len(content)
    assert manifest["endpoint"] == "measure/getmeas"
    assert manifest["request_metadata"]["page_token"] == "offset-12"
    assert manifest["request_metadata"]["access_token"] == "[REDACTED]"
    assert manifest["response_headers"]["Authorization"] == "[REDACTED]"
    assert manifest["ingestion_run_id"] == "run-1"
    assert manifest["transform_version"] == "withings-v1"
    assert stat.S_IMODE(payload_path.stat().st_mode) == 0o600


def test_repeated_save_never_overwrites_an_artifact(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "raw")

    first = store.save(page(), ingestion_run_id=None, transform_version="v1")
    second = store.save(page(), ingestion_run_id=None, transform_version="v1")

    assert first != second
    assert list(store.iter_refs("withings")) == sorted([first, second], key=lambda ref: ref.value)
    assert store.read(first) == store.read(second)


def test_corrupted_payload_is_rejected(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "raw")
    raw_ref = store.save(page(), ingestion_run_id=None, transform_version="v1")
    (store.root / raw_ref.value).write_bytes(b"changed")

    with pytest.raises(RawIntegrityError, match="integrity check failed"):
        store.read(raw_ref)


def test_corrupted_or_mismatched_manifest_is_rejected(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "raw")
    raw_ref = store.save(page(), ingestion_run_id=None, transform_version="v1")
    manifest_path = (store.root / raw_ref.value).with_name(
        f"{Path(raw_ref.value).name}.manifest.json"
    )
    manifest = json.loads(manifest_path.read_text())
    manifest["raw_ref"] = "withings/other.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RawIntegrityError, match="identity mismatch"):
        store.read(raw_ref)


def test_path_traversal_and_invalid_source_are_rejected(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "raw")

    with pytest.raises(RawStorageError, match="escapes store"):
        store.read(RawRef("../secret"))
    with pytest.raises(RawStorageError, match="invalid source"):
        store.save(
            RawPage(
                source="../oura",
                endpoint="sleep",
                retrieved_at=datetime.now(UTC),
                content=b"{}",
            ),
            ingestion_run_id=None,
            transform_version="v1",
        )


def test_raw_page_rejects_non_bytes_and_naive_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        page = RawPage(
            source="oura",
            endpoint="sleep",
            retrieved_at=datetime(2026, 9, 10),
            content=b"{}",
        )
        del page

    with pytest.raises(TypeError, match="exact bytes"):
        RawPage(
            source="oura",
            endpoint="sleep",
            retrieved_at=datetime.now(UTC),
            content="{}",  # type: ignore[arg-type]
        )
