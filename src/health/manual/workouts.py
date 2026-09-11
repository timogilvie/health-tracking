"""Raw-first manual workout entry and deterministic replay."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from health.connectors import RawPage
from health.ingestion import IngestionRunner, NormalizedRecord, RawRef, RawStore, RunResult

WorkoutType = Literal[
    "resistance",
    "walking",
    "running",
    "cycling",
    "rowing",
    "swimming",
    "rucking",
    "elliptical",
    "mobility",
    "sports",
    "other",
]
WORKOUT_TYPES = {
    "resistance",
    "walking",
    "running",
    "cycling",
    "rowing",
    "swimming",
    "rucking",
    "elliptical",
    "mobility",
    "sports",
    "other",
}


class ManualWorkoutError(ValueError):
    """A manual workout cannot be parsed or validated."""


class ManualWorkoutDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    source_record_id: str = Field(min_length=1)
    created_at: datetime
    started_at: datetime
    ended_at: datetime
    local_date: date
    workout_type: WorkoutType
    duration_seconds: int = Field(gt=0, le=86_400)
    focus: str | None = Field(default=None, max_length=200)
    rpe: float | None = Field(default=None, ge=0, le=10, allow_inf_nan=False)
    notes: str | None = Field(default=None, max_length=4000)

    @field_validator("created_at", "started_at", "ended_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("manual workout timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def bounds_match_duration(self) -> ManualWorkoutDocument:
        elapsed = int((self.ended_at - self.started_at).total_seconds())
        if elapsed != self.duration_seconds:
            raise ValueError("manual workout duration must match its timestamps")
        return self


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def build_manual_workout(
    *,
    workout_type: str,
    minutes: int,
    timezone_name: str,
    workout_date: date | None = None,
    workout_time: time | None = None,
    focus: str | None = None,
    rpe: float | None = None,
    notes: str | None = None,
    now: datetime | None = None,
) -> ManualWorkoutDocument:
    """Build a validated document; absent date/time means a workout ending now."""

    normalized_type = workout_type.strip().lower().replace("-", "_")
    if normalized_type not in WORKOUT_TYPES:
        raise ManualWorkoutError(
            "workout type must be one of: " + ", ".join(sorted(WORKOUT_TYPES))
        )
    if isinstance(minutes, bool) or not 1 <= minutes <= 1_440:
        raise ManualWorkoutError("workout minutes must be between 1 and 1440")
    try:
        timezone = ZoneInfo(timezone_name)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise ManualWorkoutError("settings.timezone must be an IANA timezone name") from exc
    created_at = now or datetime.now(UTC)
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ManualWorkoutError("entry time must be timezone-aware")
    created_at = created_at.astimezone(UTC)
    local_now = created_at.astimezone(timezone)
    duration = timedelta(minutes=minutes)
    if workout_date is None and workout_time is None:
        ended_at = local_now.replace(second=0, microsecond=0)
        started_at = ended_at - duration
    else:
        start_date = workout_date or local_now.date()
        start_time = workout_time or local_now.time().replace(second=0, microsecond=0)
        started_at = datetime.combine(start_date, start_time, tzinfo=timezone)
        ended_at = started_at + duration

    semantic: dict[str, Any] = {
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "workout_type": normalized_type,
        "duration_seconds": minutes * 60,
        "focus": _clean_optional(focus),
        "rpe": rpe,
        "notes": _clean_optional(notes),
    }
    digest = hashlib.sha256(
        json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:32]
    return ManualWorkoutDocument(
        source_record_id=f"manual-workout:{digest}",
        created_at=created_at,
        started_at=started_at,
        ended_at=ended_at,
        local_date=started_at.astimezone(timezone).date(),
        workout_type=normalized_type,
        duration_seconds=minutes * 60,
        focus=semantic["focus"],
        rpe=rpe,
        notes=semantic["notes"],
    )


class ManualWorkoutConnector:
    name = "manual"
    source_type = "manual"
    transform_version = "manual-workout-v1"

    def authenticate(self) -> None:
        return None

    def fetch(self, start: datetime, end: datetime):
        return ()

    def normalize(self, raw_ref: RawRef, raw_store: RawStore) -> list[NormalizedRecord]:
        manifest = raw_store.manifest(raw_ref)
        if manifest.get("source") != self.name or manifest.get("endpoint") != "manual/workout":
            raise ManualWorkoutError("raw artifact is not a manual workout")
        try:
            document = ManualWorkoutDocument.model_validate_json(raw_store.read(raw_ref))
        except ValueError as exc:
            raise ManualWorkoutError("manual workout document is invalid") from exc
        return [
            NormalizedRecord(
                record_type="workout",
                identity=document.source_record_id,
                values={
                    "started_at": document.started_at,
                    "ended_at": document.ended_at,
                    "local_date": document.local_date,
                    "workout_type": document.workout_type,
                    "duration_seconds": document.duration_seconds,
                    "intensity": None,
                    "rpe": document.rpe,
                    "distance_m": None,
                    "energy_kcal": None,
                    "average_hr_bpm": None,
                    "max_hr_bpm": None,
                    "source": self.name,
                    "source_record_id": document.source_record_id,
                    "device": None,
                    "notes": document.notes,
                    "transform_version": self.transform_version,
                    "metadata": {
                        "manual": {
                            "entry_schema_version": document.schema_version,
                            "focus": document.focus,
                        }
                    },
                },
            )
        ]


def record_manual_workout(
    document: ManualWorkoutDocument,
    *,
    raw_store: RawStore,
    runner: IngestionRunner,
) -> RunResult:
    """Persist the entry before replaying it into the canonical workout table."""

    raw_ref = raw_store.save(
        RawPage(
            source="manual",
            endpoint="manual/workout",
            retrieved_at=document.created_at,
            content=(
                json.dumps(document.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
                + "\n"
            ).encode(),
            request_metadata={"entry_schema_version": document.schema_version},
        ),
        ingestion_run_id=None,
        transform_version=ManualWorkoutConnector.transform_version,
    )
    return runner.replay(ManualWorkoutConnector(), [raw_ref])
