"""Streaming importer for Apple Health ``export.xml`` archives."""

from __future__ import annotations

import hashlib
import math
import zipfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO
from xml.etree import ElementTree

from health.ingestion import IngestionRunner, NormalizedRecord, RawRef, RawStore, RunResult

MAX_ARCHIVE_ENTRIES = 10_000
MAX_XML_BYTES = 8 * 1024 * 1024 * 1024
TRANSFORM_VERSION = "apple-health-export-v1"
POUNDS_TO_KILOGRAMS = 0.45359237


class AppleHealthExportError(ValueError):
    """The supplied Apple Health export is invalid or unsupported."""


def _timestamp(value: str, *, field: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise AppleHealthExportError(f"invalid Apple Health {field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AppleHealthExportError(f"Apple Health {field} must include a timezone offset")
    return parsed


def _number(value: str, *, field: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise AppleHealthExportError(f"invalid numeric Apple Health {field}") from exc
    if not math.isfinite(parsed):
        raise AppleHealthExportError(f"non-finite Apple Health {field}")
    return parsed


def _identity(kind: str, attributes: dict[str, str]) -> str:
    material = "\x1f".join(f"{key}={attributes.get(key, '')}" for key in sorted(attributes))
    return f"apple:{kind}:{hashlib.sha256(material.encode()).hexdigest()}"


def _device(attributes: dict[str, str]) -> dict[str, str] | None:
    raw = attributes.get("device", "").strip()
    if not raw:
        return None
    source_name = attributes.get("sourceName", "Apple Health")
    return {
        "manufacturer": "Apple" if "Apple" in raw or "Apple" in source_name else source_name,
        "model": None,
        "vendor_device_id": hashlib.sha256(raw.encode()).hexdigest(),
    }


def _provenance(attributes: dict[str, str], metadata: dict[str, str]) -> dict[str, Any]:
    return {
        "apple_health": {
            "source_name": attributes.get("sourceName"),
            "source_version": attributes.get("sourceVersion"),
            "device": attributes.get("device"),
            "creation_date": attributes.get("creationDate"),
            "start_date": attributes.get("startDate"),
            "end_date": attributes.get("endDate"),
            "metadata": metadata,
        }
    }


def _mass(value: float, unit: str) -> float:
    normalized = unit.casefold()
    if normalized == "kg":
        return value
    if normalized in {"lb", "lbs"}:
        return value * POUNDS_TO_KILOGRAMS
    if normalized == "g":
        return value / 1000
    raise AppleHealthExportError(f"unsupported Apple Health mass unit: {unit}")


def _temperature(value: float, unit: str) -> float:
    normalized = unit.casefold().replace("°", "deg")
    if normalized in {"degc", "c"}:
        return value
    if normalized in {"degf", "f"}:
        return (value - 32) * 5 / 9
    raise AppleHealthExportError(f"unsupported Apple Health temperature unit: {unit}")


def _observation(attributes: dict[str, str], metadata: dict[str, str]) -> NormalizedRecord | None:
    definition = {
        "HKQuantityTypeIdentifierBodyMass": ("weight_kg", "kg", _mass),
        "HKQuantityTypeIdentifierBodyFatPercentage": ("body_fat_pct", "%", None),
        "HKQuantityTypeIdentifierLeanBodyMass": ("lean_mass_kg", "kg", _mass),
        "HKQuantityTypeIdentifierHeartRate": ("heart_rate_bpm", "bpm", None),
        "HKQuantityTypeIdentifierRestingHeartRate": ("resting_hr_bpm", "bpm", None),
        "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": ("hrv_sdnn_ms", "ms", None),
        "HKQuantityTypeIdentifierStepCount": ("steps", "count", None),
        "HKQuantityTypeIdentifierActiveEnergyBurned": ("active_energy_kcal", "kcal", None),
        "HKQuantityTypeIdentifierDistanceWalkingRunning": ("walking_running_distance_m", "m", None),
        "HKQuantityTypeIdentifierAppleExerciseTime": ("exercise_minutes", "min", None),
        "HKQuantityTypeIdentifierBodyTemperature": ("body_temperature_c", "degC", _temperature),
        "HKQuantityTypeIdentifierRespiratoryRate": ("respiratory_rate", "breaths/min", None),
        "HKQuantityTypeIdentifierOxygenSaturation": ("oxygen_saturation_pct", "%", None),
    }.get(attributes.get("type", ""))
    if definition is None:
        return None
    metric, canonical_unit, converter = definition
    original_value = _number(attributes.get("value", ""), field="record value")
    original_unit = attributes.get("unit", canonical_unit)
    value = converter(original_value, original_unit) if converter else original_value
    if metric == "walking_running_distance_m" and original_unit.casefold() == "mi":
        value *= 1609.344
    observed_at = _timestamp(attributes["startDate"], field="startDate")
    observed_until = _timestamp(attributes["endDate"], field="endDate")
    source_record_id = _identity("record", attributes)
    return NormalizedRecord(
        record_type="observation",
        identity=f"{source_record_id}:{metric}",
        values={
            "metric": metric,
            "observed_at": observed_at,
            "observed_until": observed_until,
            "value": value,
            "unit": canonical_unit,
            "source": "apple_health",
            "source_record_id": source_record_id,
            "device": _device(attributes),
            "original_metric": attributes.get("type"),
            "original_value": original_value,
            "original_unit": original_unit,
            "quality": "valid",
            "timezone": str(observed_at.tzinfo),
            "local_date": observed_at.date(),
            "transform_version": TRANSFORM_VERSION,
            "metadata": _provenance(attributes, metadata),
        },
    )


def _sleep(attributes: dict[str, str], metadata: dict[str, str]) -> NormalizedRecord | None:
    if attributes.get("type") != "HKCategoryTypeIdentifierSleepAnalysis":
        return None
    stage = attributes.get("value", "").removeprefix("HKCategoryValueSleepAnalysis")
    supported_stages = {
        "InBed",
        "Awake",
        "Asleep",
        "AsleepUnspecified",
        "AsleepCore",
        "AsleepDeep",
        "AsleepREM",
    }
    if stage not in supported_stages:
        return None
    started_at = _timestamp(attributes["startDate"], field="sleep startDate")
    ended_at = _timestamp(attributes["endDate"], field="sleep endDate")
    seconds = int((ended_at - started_at).total_seconds())
    if seconds < 0:
        raise AppleHealthExportError("Apple Health sleep end precedes start")
    values: dict[str, Any] = {
        "sleep_date": ended_at.date(),
        "started_at": started_at,
        "ended_at": ended_at,
        "time_in_bed_seconds": seconds if stage == "InBed" else None,
        "total_sleep_seconds": seconds if stage.startswith("Asleep") else None,
        "awake_seconds": seconds if stage == "Awake" else None,
        "light_seconds": seconds if stage == "AsleepCore" else None,
        "deep_seconds": seconds if stage == "AsleepDeep" else None,
        "rem_seconds": seconds if stage == "AsleepREM" else None,
        "source": "apple_health",
        "source_record_id": _identity("sleep", attributes),
        "device": _device(attributes),
        "transform_version": TRANSFORM_VERSION,
        "metadata": {**_provenance(attributes, metadata), "apple_health_sleep_stage": stage},
    }
    return NormalizedRecord("sleep_session", values["source_record_id"], values)


def _workout_type(value: str) -> str:
    normalized = value.removeprefix("HKWorkoutActivityType").casefold()
    if "traditionalstrengthtraining" in normalized or "functionalstrengthtraining" in normalized:
        return "resistance"
    for needle, canonical in (
        ("walk", "walking"),
        ("run", "running"),
        ("cycling", "cycling"),
        ("row", "rowing"),
        ("swim", "swimming"),
        ("elliptical", "elliptical"),
        ("yoga", "mobility"),
        ("cooldown", "mobility"),
    ):
        if needle in normalized:
            return canonical
    return "other"


def _workout(attributes: dict[str, str], metadata: dict[str, str]) -> NormalizedRecord:
    started_at = _timestamp(attributes["startDate"], field="workout startDate")
    ended_at = _timestamp(attributes["endDate"], field="workout endDate")
    duration = int((ended_at - started_at).total_seconds())
    if duration < 0:
        raise AppleHealthExportError("Apple Health workout end precedes start")
    energy = attributes.get("totalEnergyBurned")
    distance = attributes.get("totalDistance")
    distance_value = _number(distance, field="workout distance") if distance else None
    if distance_value is not None and attributes.get("totalDistanceUnit", "").casefold() == "mi":
        distance_value *= 1609.344
    source_record_id = _identity("workout", attributes)
    return NormalizedRecord(
        "workout",
        source_record_id,
        {
            "started_at": started_at,
            "ended_at": ended_at,
            "local_date": started_at.date(),
            "workout_type": _workout_type(attributes.get("workoutActivityType", "")),
            "duration_seconds": duration,
            "distance_m": distance_value,
            "energy_kcal": _number(energy, field="workout energy") if energy else None,
            "source": "apple_health",
            "source_record_id": source_record_id,
            "device": _device(attributes),
            "transform_version": TRANSFORM_VERSION,
            "metadata": {
                **_provenance(attributes, metadata),
                "apple_health_workout": attributes,
            },
        },
    )


def _blood_pressure(
    attributes: dict[str, str],
    metadata: dict[str, str],
    pending: dict[tuple[str, str, str], dict[str, tuple[float, dict[str, str], dict[str, str]]]],
) -> NormalizedRecord | None:
    kind = attributes.get("type", "")
    if kind not in {
        "HKQuantityTypeIdentifierBloodPressureSystolic",
        "HKQuantityTypeIdentifierBloodPressureDiastolic",
    }:
        return None
    part = "systolic" if kind.endswith("Systolic") else "diastolic"
    key = (
        attributes.get("sourceName", ""),
        attributes.get("startDate", ""),
        attributes.get("endDate", ""),
    )
    pair = pending.setdefault(key, {})
    pair[part] = (_number(attributes.get("value", ""), field=part), attributes, metadata)
    if set(pair) != {"systolic", "diastolic"}:
        return None
    systolic, systolic_attributes, systolic_metadata = pair["systolic"]
    diastolic, _, diastolic_metadata = pair["diastolic"]
    del pending[key]
    measured_at = _timestamp(key[1], field="blood pressure startDate")
    correlation = _identity("blood-pressure-correlation", systolic_attributes)
    return NormalizedRecord(
        "blood_pressure",
        correlation,
        {
            "measured_at": measured_at,
            "local_date": measured_at.date(),
            "systolic_mmhg": systolic,
            "diastolic_mmhg": diastolic,
            "source": "apple_health",
            "source_record_id": correlation,
            "source_group_id": correlation,
            "device": _device(systolic_attributes),
            "quality": "valid" if systolic > diastolic else "suspect",
            "transform_version": TRANSFORM_VERSION,
            "metadata": {
                **_provenance(systolic_attributes, systolic_metadata),
                "diastolic_metadata": diastolic_metadata,
                "apple_health_correlation_id": correlation,
            },
        },
    )


@contextmanager
def _xml_stream(raw_ref: RawRef, raw_store: RawStore) -> Iterator[BinaryIO]:
    manifest = raw_store.manifest(raw_ref)
    content_type = str(manifest.get("content_type", ""))
    with raw_store.open_verified(raw_ref) as handle:
        if content_type == "application/zip":
            try:
                with zipfile.ZipFile(handle) as archive:
                    members = [member for member in archive.infolist() if not member.is_dir()]
                    if len(members) > MAX_ARCHIVE_ENTRIES:
                        raise AppleHealthExportError("Apple Health archive contains too many files")
                    matches = [
                        member
                        for member in members
                        if PurePosixPath(member.filename).name.casefold() == "export.xml"
                    ]
                    if len(matches) != 1:
                        raise AppleHealthExportError(
                            "Apple Health archive must contain exactly one export.xml"
                        )
                    member = matches[0]
                    if member.flag_bits & 0x1:
                        raise AppleHealthExportError(
                            "encrypted Apple Health archives are unsupported"
                        )
                    if member.file_size > MAX_XML_BYTES:
                        raise AppleHealthExportError("Apple Health export.xml is too large")
                    with archive.open(member) as xml_handle:
                        yield xml_handle
            except zipfile.BadZipFile as exc:
                raise AppleHealthExportError("Apple Health ZIP is invalid") from exc
        elif content_type in {"application/xml", "text/xml"}:
            yield handle
        else:
            raise AppleHealthExportError("Apple Health export must be XML or ZIP")


class AppleHealthExportConnector:
    """Parse selected Apple Health records with bounded XML element memory."""

    name = "apple_health"
    source_type = "import"
    transform_version = TRANSFORM_VERSION

    def authenticate(self) -> None:
        return None

    def fetch(self, start: datetime, end: datetime) -> Iterable[object]:
        del start, end
        raise RuntimeError("Apple Health exports are imported from local files")

    def normalize(self, raw_ref: RawRef, raw_store: RawStore) -> Iterable[NormalizedRecord]:
        def records() -> Iterator[NormalizedRecord]:
            pending_bp: dict[
                tuple[str, str, str],
                dict[str, tuple[float, dict[str, str], dict[str, str]]],
            ] = {}
            metadata_by_parent: dict[int, dict[str, str]] = {}
            stack: list[ElementTree.Element] = []
            with _xml_stream(raw_ref, raw_store) as stream:
                try:
                    for event, element in ElementTree.iterparse(stream, events=("start", "end")):
                        if event == "start":
                            stack.append(element)
                            continue
                        tag = element.tag.rsplit("}", 1)[-1]
                        if tag == "MetadataEntry" and len(stack) > 1:
                            metadata_by_parent.setdefault(id(stack[-2]), {})[
                                element.attrib.get("key", "")
                            ] = element.attrib.get("value", "")
                        elif tag == "Record":
                            attributes = dict(element.attrib)
                            metadata = metadata_by_parent.pop(id(element), {})
                            blood_pressure = _blood_pressure(attributes, metadata, pending_bp)
                            if blood_pressure is not None:
                                yield blood_pressure
                            else:
                                record = _observation(attributes, metadata) or _sleep(
                                    attributes, metadata
                                )
                                if record is not None:
                                    yield record
                        elif tag == "Workout":
                            yield _workout(
                                dict(element.attrib), metadata_by_parent.pop(id(element), {})
                            )
                        stack.pop()
                        element.clear()
                except ElementTree.ParseError as exc:
                    raise AppleHealthExportError("Apple Health export.xml is malformed") from exc
            if pending_bp:
                raise AppleHealthExportError("Apple Health export has unpaired blood-pressure data")

        return records()


def import_apple_health_export(
    path: Path,
    *,
    raw_store: RawStore,
    runner: IngestionRunner,
    now: datetime | None = None,
) -> RunResult:
    """Retain an Apple Health archive immutably and replay its XML incrementally."""

    source = path.expanduser().resolve()
    if not source.is_file():
        raise AppleHealthExportError(f"Apple Health export does not exist: {source}")
    retrieved_at = now or datetime.now(UTC)
    suffix = source.suffix.casefold()
    if suffix == ".zip":
        content_type = "application/zip"
    elif suffix == ".xml":
        content_type = "application/xml"
    else:
        raise AppleHealthExportError("Apple Health export must be a ZIP or export.xml")
    raw_ref = raw_store.save_file(
        source,
        source="apple_health",
        endpoint="export/archive" if suffix == ".zip" else "export/export.xml",
        retrieved_at=retrieved_at,
        content_type=content_type,
        request_metadata={"export_file": source.name, "parser_version": TRANSFORM_VERSION},
        ingestion_run_id=None,
        transform_version=TRANSFORM_VERSION,
    )
    return runner.replay(AppleHealthExportConnector(), [raw_ref])
