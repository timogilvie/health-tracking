"""DuckDB persistence for normalized canonical records."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import duckdb
from pydantic import BaseModel, ConfigDict, Field, field_validator

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
            payload: ObservationPayload | BloodPressurePayload = (
                ObservationPayload.model_validate(record.values)
            )
        elif record.record_type == "blood_pressure":
            payload = BloodPressurePayload.model_validate(record.values)
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
                else:
                    disposition = self._write_blood_pressure(
                        connection,
                        blood_pressure=payload,
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

    def refresh(self) -> None:
        """Derived views are introduced by later work packages."""
