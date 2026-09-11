"""Raw-first contextual event entry for longitudinal health analysis."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from health.connectors import RawPage
from health.ingestion import IngestionRunner, NormalizedRecord, RawRef, RawStore, RunResult

EventType = Literal[
    "alcohol",
    "illness",
    "travel",
    "injury",
    "supplement_change",
    "diet_change",
    "training_change",
    "medication_change",
    "other",
]
EVENT_TYPES = {
    "alcohol",
    "illness",
    "travel",
    "injury",
    "supplement_change",
    "diet_change",
    "training_change",
    "medication_change",
    "other",
}


class ManualEventError(ValueError):
    """A contextual event cannot be parsed or validated."""


class ManualEventDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    source_record_id: str = Field(min_length=1)
    created_at: datetime
    event_type: EventType
    started_at: datetime
    ended_at: datetime | None = None
    local_date: date
    value: float | None = Field(default=None, allow_inf_nan=False)
    unit: str | None = Field(default=None, max_length=100)
    notes: str | None = Field(default=None, max_length=4000)

    @field_validator("created_at", "started_at", "ended_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("manual event timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def valid_bounds_and_alcohol(self) -> ManualEventDocument:
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("manual event end must not precede start")
        if self.event_type == "alcohol" and (self.value is None or self.value <= 0):
            raise ValueError("alcohol events require a positive drink count")
        return self


def _clean(value: str | None) -> str | None:
    cleaned = value.strip() if value else ""
    return cleaned or None


def build_manual_event(
    *,
    event_type: str,
    timezone_name: str,
    event_date: date | None = None,
    event_time: time | None = None,
    duration_hours: float | None = None,
    value: float | None = None,
    unit: str | None = None,
    notes: str | None = None,
    now: datetime | None = None,
) -> ManualEventDocument:
    """Build a deterministic event document in the configured local timezone."""

    normalized_type = event_type.strip().casefold().replace("-", "_").replace(" ", "_")
    if normalized_type not in EVENT_TYPES:
        raise ManualEventError("event type must be one of: " + ", ".join(sorted(EVENT_TYPES)))
    if duration_hours is not None and not 0 < duration_hours <= 24 * 365:
        raise ManualEventError("duration hours must be greater than 0 and at most 8760")
    try:
        timezone = ZoneInfo(timezone_name)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise ManualEventError("settings.timezone must be an IANA timezone name") from exc
    created_at = now or datetime.now(UTC)
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ManualEventError("entry time must be timezone-aware")
    created_at = created_at.astimezone(UTC)
    local_now = created_at.astimezone(timezone)
    local_date = event_date or local_now.date()
    local_time = event_time or local_now.time().replace(second=0, microsecond=0)
    started_at = datetime.combine(local_date, local_time, tzinfo=timezone)
    ended_at = (
        started_at + timedelta(hours=duration_hours) if duration_hours is not None else None
    )
    normalized_unit = _clean(unit)
    if normalized_type == "alcohol":
        normalized_unit = normalized_unit or "drinks"
    semantic: dict[str, Any] = {
        "event_type": normalized_type,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat() if ended_at else None,
        "value": value,
        "unit": normalized_unit,
        "notes": _clean(notes),
    }
    digest = hashlib.sha256(
        json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:32]
    try:
        return ManualEventDocument(
            source_record_id=f"manual-event:{digest}",
            created_at=created_at,
            event_type=normalized_type,
            started_at=started_at,
            ended_at=ended_at,
            local_date=started_at.date(),
            value=value,
            unit=normalized_unit,
            notes=semantic["notes"],
        )
    except ValueError as exc:
        raise ManualEventError(str(exc)) from exc


class ManualEventConnector:
    name = "manual"
    source_type = "manual"
    transform_version = "manual-event-v1"

    def authenticate(self) -> None:
        return None

    def fetch(self, start: datetime, end: datetime):
        return ()

    def normalize(self, raw_ref: RawRef, raw_store: RawStore) -> list[NormalizedRecord]:
        manifest = raw_store.manifest(raw_ref)
        if manifest.get("source") != self.name or manifest.get("endpoint") != "manual/event":
            raise ManualEventError("raw artifact is not a manual event")
        try:
            document = ManualEventDocument.model_validate_json(raw_store.read(raw_ref))
        except ValueError as exc:
            raise ManualEventError("manual event document is invalid") from exc
        return [
            NormalizedRecord(
                "event",
                document.source_record_id,
                {
                    "event_type": document.event_type,
                    "started_at": document.started_at,
                    "ended_at": document.ended_at,
                    "local_date": document.local_date,
                    "value": document.value,
                    "unit": document.unit,
                    "notes": document.notes,
                    "source": self.name,
                    "source_record_id": document.source_record_id,
                    "transform_version": self.transform_version,
                    "metadata": {
                        "manual": {"entry_schema_version": document.schema_version}
                    },
                },
            )
        ]


def record_manual_event(
    document: ManualEventDocument,
    *,
    raw_store: RawStore,
    runner: IngestionRunner,
) -> RunResult:
    """Persist the event document immutably before canonical replay."""

    raw_ref = raw_store.save(
        RawPage(
            source="manual",
            endpoint="manual/event",
            retrieved_at=document.created_at,
            content=(
                json.dumps(document.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
                + "\n"
            ).encode(),
            request_metadata={
                "entry_schema_version": document.schema_version,
                "event_type": document.event_type,
            },
        ),
        ingestion_run_id=None,
        transform_version=ManualEventConnector.transform_version,
    )
    return runner.replay(ManualEventConnector(), [raw_ref])
