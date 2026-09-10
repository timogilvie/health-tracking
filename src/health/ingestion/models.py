"""Storage-neutral normalized records and write outcomes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


@dataclass(frozen=True, slots=True)
class NormalizedRecord:
    """A connector's source record, not yet written to canonical storage."""

    record_type: str
    identity: str
    values: dict[str, Any] = field(default_factory=dict)


class WriteDisposition(StrEnum):
    INSERTED = "inserted"
    UPDATED = "updated"
    DUPLICATE = "duplicate"
