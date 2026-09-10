"""Storage-neutral boundary for provider integrations."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from health.ingestion.models import NormalizedRecord
    from health.ingestion.raw_store import RawRef, RawStore


@dataclass(frozen=True, slots=True)
class RawPage:
    """Exact response bytes plus request metadata, before normalization."""

    source: str
    endpoint: str
    retrieved_at: datetime
    content: bytes
    content_type: str = "application/json"
    request_metadata: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None
    response_headers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.retrieved_at.tzinfo is None or self.retrieved_at.utcoffset() is None:
            raise ValueError("retrieved_at must be timezone-aware")
        if not isinstance(self.content, bytes):
            raise TypeError("content must be exact bytes")


@runtime_checkable
class Connector(Protocol):
    """Providers yield raw pages and never write canonical storage directly."""

    name: str
    transform_version: str

    def authenticate(self) -> None: ...

    def fetch(self, start: datetime, end: datetime) -> Iterable[RawPage]: ...

    def normalize(
        self, raw_ref: RawRef, raw_store: RawStore
    ) -> Iterable[NormalizedRecord]: ...
