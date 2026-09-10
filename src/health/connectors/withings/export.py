"""Offline ingestion for Withings account-export CSV files."""

from __future__ import annotations

import csv
import hashlib
import io
import math
import re
import zipfile
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from health.connectors import RawPage
from health.connectors.withings.measurements import (
    BLOOD_PRESSURE_MEASURES,
    BODY_COMPOSITION_MEASURES,
)
from health.ingestion import IngestionRunner, NormalizedRecord, RawRef, RawStore, RunResult

MAX_ARCHIVE_ENTRIES = 10_000
MAX_CSV_BYTES = 256 * 1024 * 1024
MAX_TOTAL_CSV_BYTES = 512 * 1024 * 1024
POUNDS_TO_KILOGRAMS = 0.45359237


class WithingsExportError(ValueError):
    """The supplied account export cannot be imported safely or unambiguously."""


@dataclass(frozen=True, slots=True)
class _ExportFile:
    name: str
    content: bytes


@dataclass(frozen=True, slots=True)
class _MetricColumn:
    metric: str
    canonical_unit: str
    original_unit: str
    factor: float
    plausible_min: float
    plausible_max: float


def _normalized_header(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lstrip("\ufeff")).casefold()


def _decode_csv(content: bytes, *, name: str) -> str:
    if len(content) > MAX_CSV_BYTES:
        raise WithingsExportError(f"CSV is too large: {name}")
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise WithingsExportError(f"CSV is not UTF-8 encoded: {name}") from exc


def _csv_rows(content: bytes, *, name: str) -> tuple[list[str], list[dict[str, str]]]:
    text = _decode_csv(content, name=name)
    try:
        dialect = csv.Sniffer().sniff(text[:8192], delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text, newline=""), dialect=dialect)
    if reader.fieldnames is None:
        raise WithingsExportError(f"CSV has no header: {name}")
    headers = [header.strip().lstrip("\ufeff") for header in reader.fieldnames]
    rows: list[dict[str, str]] = []
    try:
        for row in reader:
            if None in row:
                raise WithingsExportError(f"CSV row has more values than headers: {name}")
            normalized = {
                header: (row.get(original) or "").strip()
                for header, original in zip(headers, reader.fieldnames, strict=True)
            }
            if any(normalized.values()):
                rows.append(normalized)
    except csv.Error as exc:
        raise WithingsExportError(f"CSV cannot be parsed: {name}") from exc
    return headers, rows


def _csv_headers(content: bytes, *, name: str) -> list[str]:
    text = _decode_csv(content, name=name)
    try:
        dialect = csv.Sniffer().sniff(text[:8192], delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    try:
        headers = next(csv.reader(io.StringIO(text, newline=""), dialect=dialect))
    except (StopIteration, csv.Error) as exc:
        raise WithingsExportError(f"CSV has no readable header: {name}") from exc
    return [header.strip().lstrip("\ufeff") for header in headers]


def _date_header(headers: list[str]) -> str | None:
    return next((header for header in headers if _normalized_header(header) == "date"), None)


def _mass_column(header: str) -> _MetricColumn | None:
    normalized = _normalized_header(header)
    unit_match = re.search(r"\(([^)]*)\)$", normalized)
    if unit_match is None or unit_match.group(1) == "kg":
        unit = "kg"
    elif unit_match.group(1) in {"lb", "lbs"}:
        unit = "lb"
    else:
        return None
    factor = POUNDS_TO_KILOGRAMS if unit == "lb" else 1.0
    stem = re.sub(r"\s*\([^)]*\)$", "", normalized)
    definitions = {
        "weight": ("weight_kg", 20.0, 500.0),
        "fat mass": ("body_fat_mass_kg", 0.0, 300.0),
        "bone mass": ("bone_mass_kg", 0.0, 30.0),
        "muscle mass": ("skeletal_muscle_mass_kg", 0.0, 250.0),
        "hydration": ("body_water_mass_kg", 0.0, 250.0),
        "water mass": ("body_water_mass_kg", 0.0, 250.0),
    }
    definition = definitions.get(stem)
    if definition is None:
        return None
    metric, plausible_min, plausible_max = definition
    return _MetricColumn(
        metric=metric,
        canonical_unit="kg",
        original_unit=unit,
        factor=factor,
        plausible_min=plausible_min,
        plausible_max=plausible_max,
    )


def _percentage_column(header: str) -> _MetricColumn | None:
    normalized = _normalized_header(header)
    stem = re.sub(r"\s*\([^)]*\)$", "", normalized)
    if stem not in {"body fat", "fat ratio", "fat percentage"}:
        return None
    definition = BODY_COMPOSITION_MEASURES[6]
    return _MetricColumn(
        metric=definition.metric,
        canonical_unit=definition.unit,
        original_unit="%",
        factor=1.0,
        plausible_min=definition.plausible_min,
        plausible_max=definition.plausible_max,
    )


def _metric_column(header: str) -> _MetricColumn | None:
    return _mass_column(header) or _percentage_column(header)


def _is_systolic(header: str) -> bool:
    stem = re.sub(r"\s*\([^)]*\)$", "", _normalized_header(header))
    return stem in {"systolic", "systolic blood pressure"}


def _is_diastolic(header: str) -> bool:
    stem = re.sub(r"\s*\([^)]*\)$", "", _normalized_header(header))
    return stem in {"diastolic", "diastolic blood pressure"}


def _is_pulse(header: str) -> bool:
    stem = re.sub(r"\s*\([^)]*\)$", "", _normalized_header(header))
    return stem in {"heart rate", "heart pulse", "pulse"}


def _kind(headers: list[str]) -> str | None:
    if _date_header(headers) is None:
        return None
    if any(_metric_column(header) is not None for header in headers):
        return "weight"
    if any(_is_systolic(header) for header in headers) and any(
        _is_diastolic(header) for header in headers
    ):
        return "blood_pressure"
    return None


def _number(value: str, *, name: str, row_number: int, column: str) -> float:
    normalized = value.strip().replace("\u00a0", "")
    if "," in normalized and "." not in normalized:
        normalized = normalized.replace(",", ".")
    try:
        number = float(normalized)
    except ValueError as exc:
        raise WithingsExportError(
            f"invalid number in {name} row {row_number}, column {column!r}"
        ) from exc
    if not math.isfinite(number):
        raise WithingsExportError(
            f"non-finite number in {name} row {row_number}, column {column!r}"
        )
    return number


def _timestamp(
    value: str,
    *,
    timezone: ZoneInfo,
    name: str,
    row_number: int,
) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise WithingsExportError(f"invalid date in {name} row {row_number}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone)
    return parsed


def _identity(kind: str, observed_at: datetime, occurrence: int) -> str:
    timestamp = observed_at.astimezone(UTC).isoformat()
    suffix = f":{occurrence}" if occurrence > 1 else ""
    return f"export:{kind}:{timestamp}{suffix}"


def _weight_records(
    *,
    headers: list[str],
    rows: list[dict[str, str]],
    timezone: ZoneInfo,
    timezone_name: str,
    name: str,
    transform_version: str,
) -> list[NormalizedRecord]:
    date_header = _date_header(headers)
    assert date_header is not None
    metric_columns = [
        (header, definition)
        for header in headers
        if (definition := _metric_column(header)) is not None
    ]
    timestamps = [
        _timestamp(
            row[date_header],
            timezone=timezone,
            name=name,
            row_number=row_number,
        )
        for row_number, row in enumerate(rows, start=2)
    ]
    occurrences: Counter[datetime] = Counter()
    records: list[NormalizedRecord] = []
    comments_header = next(
        (header for header in headers if _normalized_header(header) in {"comment", "comments"}),
        None,
    )
    for row_number, (row, observed_at) in enumerate(zip(rows, timestamps, strict=True), start=2):
        occurrences[observed_at] += 1
        source_record_id = _identity("weight", observed_at, occurrences[observed_at])
        for header, definition in metric_columns:
            raw_value = row[header]
            if not raw_value:
                continue
            original_value = _number(
                raw_value,
                name=name,
                row_number=row_number,
                column=header,
            )
            value = original_value * definition.factor
            quality_reasons = []
            if not definition.plausible_min <= value <= definition.plausible_max:
                quality_reasons.append("implausible_value")
            export_metadata: dict[str, object] = {
                "file": PurePosixPath(name).name,
                "row": row_number,
                "column": header,
            }
            if comments_header and row[comments_header]:
                export_metadata["comments"] = row[comments_header]
            records.append(
                NormalizedRecord(
                    record_type="observation",
                    identity=f"{source_record_id}:{definition.metric}",
                    values={
                        "metric": definition.metric,
                        "observed_at": observed_at,
                        "value": value,
                        "unit": definition.canonical_unit,
                        "source": "withings",
                        "source_record_id": source_record_id,
                        "device": None,
                        "original_metric": header,
                        "original_value": original_value,
                        "original_unit": definition.original_unit,
                        "quality": "suspect" if quality_reasons else "valid",
                        "timezone": timezone_name,
                        "local_date": observed_at.astimezone(timezone).date(),
                        "transform_version": transform_version,
                        "metadata": {
                            "withings_export": export_metadata,
                            "quality_reasons": quality_reasons,
                        },
                    },
                )
            )
    return records


def _blood_pressure_records(
    *,
    headers: list[str],
    rows: list[dict[str, str]],
    timezone: ZoneInfo,
    timezone_name: str,
    name: str,
    transform_version: str,
) -> list[NormalizedRecord]:
    date_header = _date_header(headers)
    assert date_header is not None
    systolic_header = next(header for header in headers if _is_systolic(header))
    diastolic_header = next(header for header in headers if _is_diastolic(header))
    pulse_header = next((header for header in headers if _is_pulse(header)), None)
    comments_header = next(
        (header for header in headers if _normalized_header(header) in {"comment", "comments"}),
        None,
    )
    parsed: list[tuple[dict[str, str], int, datetime]] = []
    for row_number, row in enumerate(rows, start=2):
        if not row[systolic_header] and not row[diastolic_header]:
            continue
        if not row[systolic_header] or not row[diastolic_header]:
            raise WithingsExportError(f"incomplete blood-pressure pair in {name} row {row_number}")
        parsed.append(
            (
                row,
                row_number,
                _timestamp(
                    row[date_header],
                    timezone=timezone,
                    name=name,
                    row_number=row_number,
                ),
            )
        )
    occurrences: Counter[datetime] = Counter()
    records: list[NormalizedRecord] = []
    for row, row_number, measured_at in parsed:
        occurrences[measured_at] += 1
        source_record_id = _identity("blood-pressure", measured_at, occurrences[measured_at])
        systolic = _number(
            row[systolic_header],
            name=name,
            row_number=row_number,
            column=systolic_header,
        )
        diastolic = _number(
            row[diastolic_header],
            name=name,
            row_number=row_number,
            column=diastolic_header,
        )
        pulse = (
            _number(
                row[pulse_header],
                name=name,
                row_number=row_number,
                column=pulse_header,
            )
            if pulse_header and row[pulse_header]
            else None
        )
        quality_reasons: list[str] = []
        values = {9: diastolic, 10: systolic, 11: pulse}
        for measure_type, value in values.items():
            definition = BLOOD_PRESSURE_MEASURES[measure_type]
            if value is None:
                quality_reasons.append(f"missing_{definition.metric}")
            elif not definition.plausible_min <= value <= definition.plausible_max:
                quality_reasons.append(f"implausible_{definition.metric}")
        if systolic <= diastolic:
            quality_reasons.append("systolic_not_above_diastolic")
        records.append(
            NormalizedRecord(
                record_type="blood_pressure",
                identity=f"{source_record_id}:blood_pressure",
                values={
                    "measured_at": measured_at,
                    "local_date": measured_at.astimezone(timezone).date(),
                    "systolic_mmhg": systolic,
                    "diastolic_mmhg": diastolic,
                    "pulse_bpm": pulse,
                    "measurement_number": None,
                    "source": "withings",
                    "source_record_id": source_record_id,
                    "source_group_id": source_record_id,
                    "device": None,
                    "context": None,
                    "notes": (
                        row[comments_header] if comments_header and row[comments_header] else None
                    ),
                    "quality": "suspect" if quality_reasons else "valid",
                    "transform_version": transform_version,
                    "metadata": {
                        "withings_export": {
                            "file": PurePosixPath(name).name,
                            "row": row_number,
                        },
                        "timezone": timezone_name,
                        "quality_reasons": quality_reasons,
                    },
                },
            )
        )
    return records


class WithingsExportConnector:
    """Normalize Withings account-export CSVs without credentials or network access."""

    name = "withings"
    transform_version = "withings-export-v1"

    def __init__(self, *, timezone_name: str) -> None:
        try:
            self.timezone = ZoneInfo(timezone_name)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise WithingsExportError(f"invalid project timezone: {timezone_name}") from exc
        self.timezone_name = timezone_name

    def authenticate(self) -> None:
        return None

    def fetch(self, start: datetime, end: datetime) -> Iterable[RawPage]:
        del start, end
        raise RuntimeError("Withings exports are imported from local files")

    def normalize(self, raw_ref: RawRef, raw_store: RawStore) -> Iterable[NormalizedRecord]:
        manifest = raw_store.manifest(raw_ref)
        name = str(manifest.get("request_metadata", {}).get("export_entry") or raw_ref.value)
        headers, rows = _csv_rows(raw_store.read(raw_ref), name=name)
        kind = _kind(headers)
        if kind == "weight":
            return _weight_records(
                headers=headers,
                rows=rows,
                timezone=self.timezone,
                timezone_name=self.timezone_name,
                name=name,
                transform_version=self.transform_version,
            )
        if kind == "blood_pressure":
            return _blood_pressure_records(
                headers=headers,
                rows=rows,
                timezone=self.timezone,
                timezone_name=self.timezone_name,
                name=name,
                transform_version=self.transform_version,
            )
        raise WithingsExportError(f"unsupported Withings CSV headers: {name}")


def _recognized_csvs(files: Iterable[_ExportFile]) -> list[_ExportFile]:
    recognized: list[_ExportFile] = []
    total_size = 0
    for file in files:
        if PurePosixPath(file.name).suffix.casefold() != ".csv":
            continue
        total_size += len(file.content)
        if total_size > MAX_TOTAL_CSV_BYTES:
            raise WithingsExportError("export contains too much CSV data")
        headers = _csv_headers(file.content, name=file.name)
        if _kind(headers) is not None:
            recognized.append(file)
    if not recognized:
        raise WithingsExportError("no supported weight or blood-pressure CSV was found")
    return recognized


def _files_from_path(path: Path) -> tuple[bytes | None, list[_ExportFile]]:
    if path.is_dir():
        csv_paths = [file for file in sorted(path.rglob("*.csv")) if file.is_file()]
        if any(file.stat().st_size > MAX_CSV_BYTES for file in csv_paths):
            raise WithingsExportError("export contains a CSV that is too large")
        if sum(file.stat().st_size for file in csv_paths) > MAX_TOTAL_CSV_BYTES:
            raise WithingsExportError("export contains too much CSV data")
        files = [
            _ExportFile(file.relative_to(path).as_posix(), file.read_bytes()) for file in csv_paths
        ]
        return None, _recognized_csvs(files)
    if not path.is_file():
        raise WithingsExportError(f"export path does not exist: {path}")
    if zipfile.is_zipfile(path):
        archive = path.read_bytes()
        try:
            with zipfile.ZipFile(io.BytesIO(archive)) as handle:
                members = [member for member in handle.infolist() if not member.is_dir()]
                if len(members) > MAX_ARCHIVE_ENTRIES:
                    raise WithingsExportError("export archive contains too many files")
                if any(member.flag_bits & 0x1 for member in members):
                    raise WithingsExportError("encrypted export archives are not supported")
                csv_members = [
                    member
                    for member in members
                    if PurePosixPath(member.filename).suffix.casefold() == ".csv"
                ]
                if any(member.file_size > MAX_CSV_BYTES for member in csv_members):
                    raise WithingsExportError("export contains a CSV that is too large")
                if sum(member.file_size for member in csv_members) > MAX_TOTAL_CSV_BYTES:
                    raise WithingsExportError("export contains too much CSV data")
                candidates = [
                    _ExportFile(member.filename, handle.read(member)) for member in csv_members
                ]
        except zipfile.BadZipFile as exc:
            raise WithingsExportError("export archive is invalid") from exc
        return archive, _recognized_csvs(candidates)
    content = path.read_bytes()
    return None, _recognized_csvs([_ExportFile(path.name, content)])


def import_withings_export(
    path: Path,
    *,
    timezone_name: str,
    raw_store: RawStore,
    runner: IngestionRunner,
    now: datetime | None = None,
) -> RunResult:
    """Preserve a ZIP/CSV export and replay supported CSVs into canonical storage."""

    retrieved_at = now or datetime.now(UTC)
    if retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
        raise ValueError("import timestamp must be timezone-aware")
    archive, files = _files_from_path(path.expanduser().resolve())
    archive_ref: RawRef | None = None
    if archive is not None:
        archive_ref = raw_store.save(
            RawPage(
                source="withings",
                endpoint="export/archive",
                retrieved_at=retrieved_at,
                content=archive,
                content_type="application/zip",
                request_metadata={"export_file": path.name},
            ),
            ingestion_run_id=None,
            transform_version=WithingsExportConnector.transform_version,
        )
    raw_refs: list[RawRef] = []
    archive_digest = hashlib.sha256(archive).hexdigest() if archive is not None else None
    for file in files:
        raw_refs.append(
            raw_store.save(
                RawPage(
                    source="withings",
                    endpoint=f"export/{PurePosixPath(file.name).name}",
                    retrieved_at=retrieved_at,
                    content=file.content,
                    content_type="text/csv",
                    request_metadata={
                        "export_entry": file.name,
                        "archive_sha256": archive_digest,
                    },
                ),
                ingestion_run_id=None,
                transform_version=WithingsExportConnector.transform_version,
                parent_archive=archive_ref,
            )
        )
    connector = WithingsExportConnector(timezone_name=timezone_name)
    return runner.replay(connector, raw_refs)
