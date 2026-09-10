"""Raw-first ingestion pipeline."""

from health.ingestion.canonical_sink import DuckDBCanonicalSink, ObservationPayload
from health.ingestion.models import NormalizedRecord, WriteDisposition
from health.ingestion.raw_store import RawIntegrityError, RawRef, RawStorageError, RawStore
from health.ingestion.runner import (
    CanonicalSink,
    IngestionRunner,
    RetryableIngestionError,
    RetryPolicy,
    RunResult,
    SyncWindowPolicy,
)

__all__ = [
    "CanonicalSink",
    "DuckDBCanonicalSink",
    "IngestionRunner",
    "NormalizedRecord",
    "ObservationPayload",
    "RawIntegrityError",
    "RawRef",
    "RawStorageError",
    "RawStore",
    "RetryableIngestionError",
    "RetryPolicy",
    "RunResult",
    "SyncWindowPolicy",
    "WriteDisposition",
]
