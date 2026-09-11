"""DuckDB persistence for normalized canonical records."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import duckdb
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from health.db import connect
from health.ingestion.models import NormalizedRecord, WriteDisposition
from health.ingestion.raw_store import RawRef


class DeviceDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    manufacturer: str = Field(min_length=1)
    model: str | None = None
    vendor_device_id: str = Field(min_length=1)


class ObservationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric: str = Field(min_length=1)
    observed_at: datetime
    observed_until: datetime | None = None
    value: float = Field(allow_inf_nan=False)
    unit: str = Field(min_length=1)
    source: str = Field(min_length=1)
    source_record_id: str = Field(min_length=1)
    device: DeviceDescriptor | None = None
    original_metric: str | None = None
    original_value: float | None = Field(default=None, allow_inf_nan=False)
    original_unit: str | None = None
    quality: Literal["valid", "suspect", "invalid"] = "valid"
    timezone: str | None = None
    local_date: date | None = None
    transform_version: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("observed_at", "observed_until")
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("observation timestamps must be timezone-aware")
        return value


class BloodPressurePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    measured_at: datetime
    local_date: date
    systolic_mmhg: float = Field(gt=0, allow_inf_nan=False)
    diastolic_mmhg: float = Field(gt=0, allow_inf_nan=False)
    pulse_bpm: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    measurement_number: int | None = Field(default=None, ge=1)
    source: str = Field(min_length=1)
    source_record_id: str = Field(min_length=1)
    source_group_id: str | None = Field(default=None, min_length=1)
    device: DeviceDescriptor | None = None
    context: str | None = None
    notes: str | None = None
    quality: Literal["valid", "suspect", "invalid"] = "valid"
    transform_version: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("measured_at")
    @classmethod
    def measured_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("blood-pressure timestamp must be timezone-aware")
        return value


class SleepSessionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sleep_date: date
    started_at: datetime
    ended_at: datetime
    time_in_bed_seconds: int | None = Field(default=None, ge=0)
    total_sleep_seconds: int | None = Field(default=None, ge=0)
    awake_seconds: int | None = Field(default=None, ge=0)
    light_seconds: int | None = Field(default=None, ge=0)
    deep_seconds: int | None = Field(default=None, ge=0)
    rem_seconds: int | None = Field(default=None, ge=0)
    latency_seconds: int | None = Field(default=None, ge=0)
    efficiency_pct: float | None = Field(default=None, ge=0, le=100, allow_inf_nan=False)
    resting_hr_bpm: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    lowest_hr_bpm: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    average_hrv_rmssd_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    respiratory_rate: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    sleep_score: float | None = Field(default=None, ge=0, le=100, allow_inf_nan=False)
    source: str = Field(min_length=1)
    source_record_id: str = Field(min_length=1)
    device: DeviceDescriptor | None = None
    transform_version: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("started_at", "ended_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("sleep timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def end_is_not_before_start(self) -> SleepSessionPayload:
        if self.ended_at < self.started_at:
            raise ValueError("sleep end must not be before start")
        return self


class WorkoutPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    started_at: datetime
    ended_at: datetime
    local_date: date
    workout_type: Literal[
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
    duration_seconds: int = Field(ge=0)
    intensity: str | None = None
    rpe: float | None = Field(default=None, ge=0, le=10, allow_inf_nan=False)
    distance_m: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    energy_kcal: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    average_hr_bpm: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    max_hr_bpm: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    source: str = Field(min_length=1)
    source_record_id: str = Field(min_length=1)
    device: DeviceDescriptor | None = None
    notes: str | None = None
    transform_version: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("started_at", "ended_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("workout timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def duration_matches_bounds(self) -> WorkoutPayload:
        if self.ended_at < self.started_at:
            raise ValueError("workout end must not be before start")
        elapsed = int((self.ended_at - self.started_at).total_seconds())
        if abs(elapsed - self.duration_seconds) > 1:
            raise ValueError("workout duration must match its timestamps")
        return self


class LabResultPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    collected_at: datetime | None = None
    resulted_at: datetime | None = None
    canonical_name: str | None = None
    original_name: str = Field(min_length=1)
    numeric_value: float | None = Field(default=None, allow_inf_nan=False)
    text_value: str | None = None
    unit: str | None = None
    reference_low: float | None = Field(default=None, allow_inf_nan=False)
    reference_high: float | None = Field(default=None, allow_inf_nan=False)
    reference_text: str | None = None
    abnormal_flag: str | None = None
    provider: str | None = None
    fasting: bool | None = None
    source: str = Field(min_length=1)
    source_record_id: str = Field(min_length=1)
    transform_version: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("collected_at", "resulted_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("lab timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def has_one_result_value(self) -> LabResultPayload:
        if self.numeric_value is None and not self.text_value:
            raise ValueError("lab result must have a numeric or text value")
        return self


class EventPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: str = Field(min_length=1)
    started_at: datetime
    ended_at: datetime | None = None
    local_date: date
    value: float | None = Field(default=None, allow_inf_nan=False)
    unit: str | None = None
    notes: str | None = None
    source: str = Field(min_length=1)
    source_record_id: str = Field(min_length=1)
    transform_version: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("started_at", "ended_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("event timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def end_is_not_before_start(self) -> EventPayload:
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("event end must not be before start")
        return self


def _source_id(connection: duckdb.DuckDBPyConnection, source: str) -> UUID:
    connection.execute(
        """
        INSERT INTO sources (name, source_type)
        SELECT ?, 'api'
        WHERE NOT EXISTS (SELECT 1 FROM sources WHERE name = ?)
        """,
        [source, source],
    )
    row = connection.execute(
        "SELECT source_id FROM sources WHERE name = ?", [source]
    ).fetchone()
    if row is None:
        raise RuntimeError(f"could not resolve source: {source}")
    return row[0]


def _device_id(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_id: UUID,
    device: DeviceDescriptor | None,
) -> UUID | None:
    if device is None:
        return None
    row = connection.execute(
        """
        SELECT device_id
        FROM devices
        WHERE source_id = ?
          AND json_extract_string(metadata, '$.vendor_device_id') = ?
        """,
        [source_id, device.vendor_device_id],
    ).fetchone()
    metadata = json.dumps({"vendor_device_id": device.vendor_device_id})
    if row is not None:
        return row[0]
    return connection.execute(
        """
        INSERT INTO devices (source_id, manufacturer, model, name, metadata)
        VALUES (?, ?, ?, ?, ?)
        RETURNING device_id
        """,
        [
            source_id,
            device.manufacturer,
            device.model,
            device.vendor_device_id,
            metadata,
        ],
    ).fetchone()[0]


def _metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    return value if isinstance(value, dict) else {}


class DuckDBCanonicalSink:
    """Persist canonical records and classify overlap writes deterministically."""

    def __init__(self, database: Path) -> None:
        self.database = database

    def process(
        self,
        record: NormalizedRecord,
        *,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        if record.record_type == "observation":
            payload: (
                ObservationPayload
                | BloodPressurePayload
                | SleepSessionPayload
                | WorkoutPayload
                | LabResultPayload
                | EventPayload
            ) = (
                ObservationPayload.model_validate(record.values)
            )
        elif record.record_type == "blood_pressure":
            payload = BloodPressurePayload.model_validate(record.values)
        elif record.record_type == "sleep_session":
            payload = SleepSessionPayload.model_validate(record.values)
        elif record.record_type == "workout":
            payload = WorkoutPayload.model_validate(record.values)
        elif record.record_type == "lab_result":
            payload = LabResultPayload.model_validate(record.values)
        elif record.record_type == "event":
            payload = EventPayload.model_validate(record.values)
        else:
            raise ValueError(f"unsupported canonical record type: {record.record_type}")
        with connect(self.database) as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                if isinstance(payload, ObservationPayload):
                    disposition = self._write_observation(
                        connection,
                        observation=payload,
                        raw_ref=raw_ref,
                        ingestion_run_id=ingestion_run_id,
                    )
                elif isinstance(payload, BloodPressurePayload):
                    disposition = self._write_blood_pressure(
                        connection,
                        blood_pressure=payload,
                        raw_ref=raw_ref,
                        ingestion_run_id=ingestion_run_id,
                    )
                elif isinstance(payload, SleepSessionPayload):
                    disposition = self._write_sleep_session(
                        connection,
                        sleep_session=payload,
                        raw_ref=raw_ref,
                        ingestion_run_id=ingestion_run_id,
                    )
                elif isinstance(payload, WorkoutPayload):
                    disposition = self._write_workout(
                        connection,
                        workout=payload,
                        raw_ref=raw_ref,
                        ingestion_run_id=ingestion_run_id,
                    )
                elif isinstance(payload, LabResultPayload):
                    disposition = self._write_lab_result(
                        connection,
                        lab_result=payload,
                        raw_ref=raw_ref,
                        ingestion_run_id=ingestion_run_id,
                    )
                else:
                    disposition = self._write_event(
                        connection,
                        event=payload,
                        raw_ref=raw_ref,
                        ingestion_run_id=ingestion_run_id,
                    )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return disposition

    def _write_observation(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        observation: ObservationPayload,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        source_id = _source_id(connection, observation.source)
        device_id = _device_id(
            connection,
            source_id=source_id,
            device=observation.device,
        )
        existing = connection.execute(
            """
            SELECT observation_id, observed_at, observed_until, value, unit, device_id,
                   original_metric, original_value, original_unit, quality, timezone,
                   local_date, transform_version, metadata
            FROM observations
            WHERE source_id = ? AND source_record_id = ? AND metric = ?
            """,
            [source_id, observation.source_record_id, observation.metric],
        ).fetchone()
        metadata = {
            **observation.metadata,
            "ingestion_run_id": str(ingestion_run_id),
        }
        semantic = (
            observation.observed_at,
            observation.observed_until,
            observation.value,
            observation.unit,
            device_id,
            observation.original_metric,
            observation.original_value,
            observation.original_unit,
            observation.quality,
            observation.timezone,
            observation.local_date,
            observation.transform_version,
            observation.metadata,
        )
        if existing is not None:
            existing_metadata = _metadata(existing[13])
            existing_metadata.pop("ingestion_run_id", None)
            if (*existing[1:13], existing_metadata) == semantic:
                return WriteDisposition.DUPLICATE
            connection.execute(
                """
                UPDATE observations
                SET observed_at = ?, observed_until = ?, value = ?, unit = ?,
                    device_id = ?, original_metric = ?, original_value = ?,
                    original_unit = ?, quality = ?, timezone = ?, local_date = ?,
                    raw_file = ?, transform_version = ?, metadata = ?,
                    ingested_at = current_timestamp
                WHERE observation_id = ?
                """,
                [
                    observation.observed_at,
                    observation.observed_until,
                    observation.value,
                    observation.unit,
                    device_id,
                    observation.original_metric,
                    observation.original_value,
                    observation.original_unit,
                    observation.quality,
                    observation.timezone,
                    observation.local_date,
                    raw_ref.value,
                    observation.transform_version,
                    json.dumps(metadata, sort_keys=True),
                    existing[0],
                ],
            )
            return WriteDisposition.UPDATED

        connection.execute(
            """
            INSERT INTO observations (
                metric, observed_at, observed_until, value, unit, source_id,
                source_record_id, device_id, original_metric, original_value,
                original_unit, quality, timezone, local_date, raw_file,
                transform_version, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                observation.metric,
                observation.observed_at,
                observation.observed_until,
                observation.value,
                observation.unit,
                source_id,
                observation.source_record_id,
                device_id,
                observation.original_metric,
                observation.original_value,
                observation.original_unit,
                observation.quality,
                observation.timezone,
                observation.local_date,
                raw_ref.value,
                observation.transform_version,
                json.dumps(metadata, sort_keys=True),
            ],
        )
        return WriteDisposition.INSERTED

    def _write_blood_pressure(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        blood_pressure: BloodPressurePayload,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        source_id = _source_id(connection, blood_pressure.source)
        device_id = _device_id(
            connection,
            source_id=source_id,
            device=blood_pressure.device,
        )
        existing = connection.execute(
            """
            SELECT blood_pressure_id, measured_at, local_date, systolic_mmhg,
                   diastolic_mmhg, pulse_bpm, measurement_number, device_id,
                   source_group_id, context, notes, quality, transform_version, metadata
            FROM blood_pressure
            WHERE source_id = ? AND source_record_id = ?
            """,
            [source_id, blood_pressure.source_record_id],
        ).fetchone()
        metadata = {
            **blood_pressure.metadata,
            "ingestion_run_id": str(ingestion_run_id),
        }
        semantic = (
            blood_pressure.measured_at,
            blood_pressure.local_date,
            blood_pressure.systolic_mmhg,
            blood_pressure.diastolic_mmhg,
            blood_pressure.pulse_bpm,
            blood_pressure.measurement_number,
            device_id,
            blood_pressure.source_group_id,
            blood_pressure.context,
            blood_pressure.notes,
            blood_pressure.quality,
            blood_pressure.transform_version,
            blood_pressure.metadata,
        )
        if existing is not None:
            existing_metadata = _metadata(existing[13])
            existing_metadata.pop("ingestion_run_id", None)
            if (*existing[1:13], existing_metadata) == semantic:
                return WriteDisposition.DUPLICATE
            connection.execute(
                """
                UPDATE blood_pressure
                SET measured_at = ?, local_date = ?, systolic_mmhg = ?,
                    diastolic_mmhg = ?, pulse_bpm = ?, measurement_number = ?,
                    device_id = ?, source_group_id = ?, context = ?, notes = ?,
                    quality = ?, raw_file = ?, transform_version = ?, metadata = ?,
                    ingested_at = current_timestamp
                WHERE blood_pressure_id = ?
                """,
                [
                    blood_pressure.measured_at,
                    blood_pressure.local_date,
                    blood_pressure.systolic_mmhg,
                    blood_pressure.diastolic_mmhg,
                    blood_pressure.pulse_bpm,
                    blood_pressure.measurement_number,
                    device_id,
                    blood_pressure.source_group_id,
                    blood_pressure.context,
                    blood_pressure.notes,
                    blood_pressure.quality,
                    raw_ref.value,
                    blood_pressure.transform_version,
                    json.dumps(metadata, sort_keys=True),
                    existing[0],
                ],
            )
            return WriteDisposition.UPDATED

        connection.execute(
            """
            INSERT INTO blood_pressure (
                measured_at, local_date, systolic_mmhg, diastolic_mmhg,
                pulse_bpm, measurement_number, source_id, source_record_id,
                source_group_id, device_id, context, notes, quality, raw_file,
                transform_version, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                blood_pressure.measured_at,
                blood_pressure.local_date,
                blood_pressure.systolic_mmhg,
                blood_pressure.diastolic_mmhg,
                blood_pressure.pulse_bpm,
                blood_pressure.measurement_number,
                source_id,
                blood_pressure.source_record_id,
                blood_pressure.source_group_id,
                device_id,
                blood_pressure.context,
                blood_pressure.notes,
                blood_pressure.quality,
                raw_ref.value,
                blood_pressure.transform_version,
                json.dumps(metadata, sort_keys=True),
            ],
        )
        return WriteDisposition.INSERTED

    def _write_sleep_session(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        sleep_session: SleepSessionPayload,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        source_id = _source_id(connection, sleep_session.source)
        device_id = _device_id(
            connection,
            source_id=source_id,
            device=sleep_session.device,
        )
        existing = connection.execute(
            """
            SELECT sleep_session_id, sleep_date, started_at, ended_at,
                   time_in_bed_seconds, total_sleep_seconds, awake_seconds,
                   light_seconds, deep_seconds, rem_seconds, latency_seconds,
                   efficiency_pct, resting_hr_bpm, lowest_hr_bpm,
                   average_hrv_rmssd_ms, respiratory_rate, sleep_score,
                   device_id, transform_version, metadata
            FROM sleep_sessions
            WHERE source_id = ? AND source_record_id = ?
            """,
            [source_id, sleep_session.source_record_id],
        ).fetchone()
        metadata = {
            **sleep_session.metadata,
            "ingestion_run_id": str(ingestion_run_id),
        }
        semantic = (
            sleep_session.sleep_date,
            sleep_session.started_at,
            sleep_session.ended_at,
            sleep_session.time_in_bed_seconds,
            sleep_session.total_sleep_seconds,
            sleep_session.awake_seconds,
            sleep_session.light_seconds,
            sleep_session.deep_seconds,
            sleep_session.rem_seconds,
            sleep_session.latency_seconds,
            sleep_session.efficiency_pct,
            sleep_session.resting_hr_bpm,
            sleep_session.lowest_hr_bpm,
            sleep_session.average_hrv_rmssd_ms,
            sleep_session.respiratory_rate,
            sleep_session.sleep_score,
            device_id,
            sleep_session.transform_version,
            sleep_session.metadata,
        )
        if existing is not None:
            existing_metadata = _metadata(existing[19])
            existing_metadata.pop("ingestion_run_id", None)
            if (*existing[1:19], existing_metadata) == semantic:
                return WriteDisposition.DUPLICATE
            connection.execute(
                """
                UPDATE sleep_sessions
                SET sleep_date = ?, started_at = ?, ended_at = ?,
                    time_in_bed_seconds = ?, total_sleep_seconds = ?,
                    awake_seconds = ?, light_seconds = ?, deep_seconds = ?,
                    rem_seconds = ?, latency_seconds = ?, efficiency_pct = ?,
                    resting_hr_bpm = ?, lowest_hr_bpm = ?,
                    average_hrv_rmssd_ms = ?, respiratory_rate = ?,
                    sleep_score = ?, device_id = ?, raw_file = ?,
                    transform_version = ?, metadata = ?,
                    ingested_at = current_timestamp
                WHERE sleep_session_id = ?
                """,
                [
                    sleep_session.sleep_date,
                    sleep_session.started_at,
                    sleep_session.ended_at,
                    sleep_session.time_in_bed_seconds,
                    sleep_session.total_sleep_seconds,
                    sleep_session.awake_seconds,
                    sleep_session.light_seconds,
                    sleep_session.deep_seconds,
                    sleep_session.rem_seconds,
                    sleep_session.latency_seconds,
                    sleep_session.efficiency_pct,
                    sleep_session.resting_hr_bpm,
                    sleep_session.lowest_hr_bpm,
                    sleep_session.average_hrv_rmssd_ms,
                    sleep_session.respiratory_rate,
                    sleep_session.sleep_score,
                    device_id,
                    raw_ref.value,
                    sleep_session.transform_version,
                    json.dumps(metadata, sort_keys=True),
                    existing[0],
                ],
            )
            return WriteDisposition.UPDATED

        connection.execute(
            """
            INSERT INTO sleep_sessions (
                sleep_date, started_at, ended_at, time_in_bed_seconds,
                total_sleep_seconds, awake_seconds, light_seconds,
                deep_seconds, rem_seconds, latency_seconds, efficiency_pct,
                resting_hr_bpm, lowest_hr_bpm, average_hrv_rmssd_ms,
                respiratory_rate, sleep_score, source_id, source_record_id,
                device_id, raw_file, transform_version, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                sleep_session.sleep_date,
                sleep_session.started_at,
                sleep_session.ended_at,
                sleep_session.time_in_bed_seconds,
                sleep_session.total_sleep_seconds,
                sleep_session.awake_seconds,
                sleep_session.light_seconds,
                sleep_session.deep_seconds,
                sleep_session.rem_seconds,
                sleep_session.latency_seconds,
                sleep_session.efficiency_pct,
                sleep_session.resting_hr_bpm,
                sleep_session.lowest_hr_bpm,
                sleep_session.average_hrv_rmssd_ms,
                sleep_session.respiratory_rate,
                sleep_session.sleep_score,
                source_id,
                sleep_session.source_record_id,
                device_id,
                raw_ref.value,
                sleep_session.transform_version,
                json.dumps(metadata, sort_keys=True),
            ],
        )
        return WriteDisposition.INSERTED

    def _write_workout(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        workout: WorkoutPayload,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        source_id = _source_id(connection, workout.source)
        device_id = _device_id(
            connection,
            source_id=source_id,
            device=workout.device,
        )
        existing = connection.execute(
            """
            SELECT workout_id, started_at, ended_at, local_date, workout_type,
                   duration_seconds, intensity, rpe, distance_m, energy_kcal,
                   average_hr_bpm, max_hr_bpm, device_id, notes,
                   transform_version, metadata
            FROM workouts
            WHERE source_id = ? AND source_record_id = ?
            """,
            [source_id, workout.source_record_id],
        ).fetchone()
        metadata = {**workout.metadata, "ingestion_run_id": str(ingestion_run_id)}
        semantic = (
            workout.started_at,
            workout.ended_at,
            workout.local_date,
            workout.workout_type,
            workout.duration_seconds,
            workout.intensity,
            workout.rpe,
            workout.distance_m,
            workout.energy_kcal,
            workout.average_hr_bpm,
            workout.max_hr_bpm,
            device_id,
            workout.notes,
            workout.transform_version,
            workout.metadata,
        )
        if existing is not None:
            existing_metadata = _metadata(existing[15])
            existing_metadata.pop("ingestion_run_id", None)
            if (*existing[1:15], existing_metadata) == semantic:
                return WriteDisposition.DUPLICATE
            connection.execute(
                """
                UPDATE workouts
                SET started_at = ?, ended_at = ?, local_date = ?,
                    workout_type = ?, duration_seconds = ?, intensity = ?,
                    rpe = ?, distance_m = ?, energy_kcal = ?,
                    average_hr_bpm = ?, max_hr_bpm = ?, device_id = ?,
                    notes = ?, raw_file = ?, transform_version = ?, metadata = ?,
                    ingested_at = current_timestamp
                WHERE workout_id = ?
                """,
                [
                    workout.started_at,
                    workout.ended_at,
                    workout.local_date,
                    workout.workout_type,
                    workout.duration_seconds,
                    workout.intensity,
                    workout.rpe,
                    workout.distance_m,
                    workout.energy_kcal,
                    workout.average_hr_bpm,
                    workout.max_hr_bpm,
                    device_id,
                    workout.notes,
                    raw_ref.value,
                    workout.transform_version,
                    json.dumps(metadata, sort_keys=True),
                    existing[0],
                ],
            )
            return WriteDisposition.UPDATED

        connection.execute(
            """
            INSERT INTO workouts (
                started_at, ended_at, local_date, workout_type,
                duration_seconds, intensity, rpe, distance_m, energy_kcal,
                average_hr_bpm, max_hr_bpm, source_id, source_record_id,
                device_id, notes, raw_file, transform_version, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                workout.started_at,
                workout.ended_at,
                workout.local_date,
                workout.workout_type,
                workout.duration_seconds,
                workout.intensity,
                workout.rpe,
                workout.distance_m,
                workout.energy_kcal,
                workout.average_hr_bpm,
                workout.max_hr_bpm,
                source_id,
                workout.source_record_id,
                device_id,
                workout.notes,
                raw_ref.value,
                workout.transform_version,
                json.dumps(metadata, sort_keys=True),
            ],
        )
        return WriteDisposition.INSERTED

    def _write_lab_result(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        lab_result: LabResultPayload,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        source_id = _source_id(connection, lab_result.source)
        existing = connection.execute(
            """
            SELECT lab_result_id, collected_at, resulted_at, canonical_name,
                   original_name, numeric_value, text_value, unit, reference_low,
                   reference_high, reference_text, abnormal_flag, provider, fasting,
                   transform_version, metadata
            FROM lab_results
            WHERE source_id = ? AND source_record_id = ?
            """,
            [source_id, lab_result.source_record_id],
        ).fetchone()
        metadata = {**lab_result.metadata, "ingestion_run_id": str(ingestion_run_id)}
        semantic = (
            lab_result.collected_at,
            lab_result.resulted_at,
            lab_result.canonical_name,
            lab_result.original_name,
            lab_result.numeric_value,
            lab_result.text_value,
            lab_result.unit,
            lab_result.reference_low,
            lab_result.reference_high,
            lab_result.reference_text,
            lab_result.abnormal_flag,
            lab_result.provider,
            lab_result.fasting,
            lab_result.transform_version,
            lab_result.metadata,
        )
        if existing is not None:
            existing_metadata = _metadata(existing[15])
            existing_metadata.pop("ingestion_run_id", None)
            if (*existing[1:15], existing_metadata) == semantic:
                return WriteDisposition.DUPLICATE
            connection.execute(
                """
                UPDATE lab_results
                SET collected_at = ?, resulted_at = ?, canonical_name = ?,
                    original_name = ?, numeric_value = ?, text_value = ?, unit = ?,
                    reference_low = ?, reference_high = ?, reference_text = ?,
                    abnormal_flag = ?, provider = ?, fasting = ?, raw_file = ?,
                    transform_version = ?, metadata = ?, ingested_at = current_timestamp
                WHERE lab_result_id = ?
                """,
                [
                    lab_result.collected_at,
                    lab_result.resulted_at,
                    lab_result.canonical_name,
                    lab_result.original_name,
                    lab_result.numeric_value,
                    lab_result.text_value,
                    lab_result.unit,
                    lab_result.reference_low,
                    lab_result.reference_high,
                    lab_result.reference_text,
                    lab_result.abnormal_flag,
                    lab_result.provider,
                    lab_result.fasting,
                    raw_ref.value,
                    lab_result.transform_version,
                    json.dumps(metadata, sort_keys=True),
                    existing[0],
                ],
            )
            return WriteDisposition.UPDATED
        connection.execute(
            """
            INSERT INTO lab_results (
                collected_at, resulted_at, canonical_name, original_name,
                numeric_value, text_value, unit, reference_low, reference_high,
                reference_text, abnormal_flag, provider, fasting, source_id,
                source_record_id, raw_file, transform_version, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                lab_result.collected_at,
                lab_result.resulted_at,
                lab_result.canonical_name,
                lab_result.original_name,
                lab_result.numeric_value,
                lab_result.text_value,
                lab_result.unit,
                lab_result.reference_low,
                lab_result.reference_high,
                lab_result.reference_text,
                lab_result.abnormal_flag,
                lab_result.provider,
                lab_result.fasting,
                source_id,
                lab_result.source_record_id,
                raw_ref.value,
                lab_result.transform_version,
                json.dumps(metadata, sort_keys=True),
            ],
        )
        return WriteDisposition.INSERTED

    def _write_event(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        event: EventPayload,
        raw_ref: RawRef,
        ingestion_run_id: UUID,
    ) -> WriteDisposition:
        source_id = _source_id(connection, event.source)
        existing = connection.execute(
            """
            SELECT event_id, event_type, started_at, ended_at, local_date,
                   value, unit, notes, transform_version, metadata
            FROM events
            WHERE source_id = ? AND source_record_id = ?
            """,
            [source_id, event.source_record_id],
        ).fetchone()
        metadata = {**event.metadata, "ingestion_run_id": str(ingestion_run_id)}
        semantic = (
            event.event_type,
            event.started_at,
            event.ended_at,
            event.local_date,
            event.value,
            event.unit,
            event.notes,
            event.transform_version,
            event.metadata,
        )
        if existing is not None:
            existing_metadata = _metadata(existing[9])
            existing_metadata.pop("ingestion_run_id", None)
            if (*existing[1:9], existing_metadata) == semantic:
                return WriteDisposition.DUPLICATE
            connection.execute(
                """
                UPDATE events
                SET event_type = ?, started_at = ?, ended_at = ?,
                    local_date = ?, value = ?, unit = ?, notes = ?,
                    raw_file = ?, transform_version = ?, metadata = ?,
                    ingested_at = current_timestamp
                WHERE event_id = ?
                """,
                [
                    event.event_type,
                    event.started_at,
                    event.ended_at,
                    event.local_date,
                    event.value,
                    event.unit,
                    event.notes,
                    raw_ref.value,
                    event.transform_version,
                    json.dumps(metadata, sort_keys=True),
                    existing[0],
                ],
            )
            return WriteDisposition.UPDATED

        connection.execute(
            """
            INSERT INTO events (
                event_type, started_at, ended_at, local_date, value, unit,
                notes, source_id, source_record_id, raw_file,
                transform_version, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event.event_type,
                event.started_at,
                event.ended_at,
                event.local_date,
                event.value,
                event.unit,
                event.notes,
                source_id,
                event.source_record_id,
                raw_ref.value,
                event.transform_version,
                json.dumps(metadata, sort_keys=True),
            ],
        )
        return WriteDisposition.INSERTED

    def refresh(self) -> None:
        """SQL views are live and require no materialized refresh."""
