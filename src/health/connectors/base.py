"""Storage-neutral boundary for provider integrations."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class RawPage:
    """Exact response bytes plus request metadata, before normalization."""

    source: str
    endpoint: str
    retrieved_at: datetime
    content: bytes
    content_type: str = "application/json"
    request_metadata: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Connector(Protocol):
    """Providers yield raw pages and never write canonical storage directly."""

    name: str

    def authenticate(self) -> None: ...

    def fetch(self, start: datetime, end: datetime) -> Iterable[RawPage]: ...
