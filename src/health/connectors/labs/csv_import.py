"""Validated long-format CSV ingestion for laboratory results."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, time
from io import TextIOWrapper
from pathlib import Path
from typing import Any, BinaryIO
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from health.ingestion import IngestionRunner, NormalizedRecord, RawRef, RawStore, RunResult

TRANSFORM_VERSION = "lab-csv-v1"
MAX_CSV_BYTES = 64 * 1024 * 1024


class LabImportError(ValueError):
    """A lab CSV or biomarker vocabulary is invalid."""


@dataclass(frozen=True, slots=True)
class LabPreview:
    rows: int
    recognized: int
    unknown: tuple[str, ...]


def _normalized(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def load_biomarker_vocabulary(config: Mapping[str, Any]) -> dict[str, str]:
    """Build a normalized alias-to-canonical mapping and reject collisions."""

    biomarkers = config.get("biomarkers")
    if not isinstance(biomarkers, dict) or not biomarkers:
        raise LabImportError("biomarkers.yaml must define a non-empty biomarkers mapping")
    vocabulary: dict[str, str] = {}
    for canonical, aliases in biomarkers.items():
        if not isinstance(canonical, str) or not canonical.strip():
            raise LabImportError("biomarker canonical names must be non-empty strings")
        if not isinstance(aliases, list) or not all(isinstance(alias, str) for alias in aliases):
            raise LabImportError(f"biomarker aliases must be a list of strings: {canonical}")
        for alias in [canonical, *aliases]:
            key = _normalized(alias)
            previous = vocabulary.get(key)
            if previous is not None and previous != canonical:
                raise LabImportError(f"biomarker alias is ambiguous: {alias}")
            vocabulary[key] = canonical
    return vocabulary


HEADER_ALIASES = {
    "name": ("name", "test", "test name", "marker", "analyte", "component"),
    "value": ("value", "result", "result value"),
    "unit": ("unit", "units"),
    "collected_at": ("collected at", "collected", "collection date", "date"),
    "resulted_at": ("resulted at", "resulted", "result date", "reported date"),
    "reference_low": ("reference low", "low", "range low"),
    "reference_high": ("reference high", "high", "range high"),
    "reference_text": ("reference range", "reference", "normal range", "range"),
    "abnormal_flag": ("abnormal flag", "flag", "status"),
    "provider": ("provider", "lab", "laboratory", "facility"),
    "fasting": ("fasting", "fasted"),
}


def _columns(fieldnames: list[str] | None) -> dict[str, str]:
    if not fieldnames:
        raise LabImportError("lab CSV has no header")
    normalized = {_normalized(header): header for header in fieldnames}
    columns = {
        field: original
        for field, aliases in HEADER_ALIASES.items()
        if (original := next((normalized[alias] for alias in aliases if alias in normalized), None))
    }
    missing = [field for field in ("name", "value") if field not in columns]
    if missing:
        raise LabImportError(f"lab CSV is missing required column(s): {', '.join(missing)}")
    return columns


def _optional(row: dict[str, str], columns: dict[str, str], field: str) -> str | None:
    column = columns.get(field)
    value = row.get(column, "").strip() if column else ""
    return value or None


def _number(value: str | None, *, row_number: int, field: str) -> float | None:
    if value is None:
        return None
    normalized = value.replace(",", "").strip()
    try:
        number = float(normalized)
    except ValueError as exc:
        raise LabImportError(f"invalid {field} in lab CSV row {row_number}") from exc
    if not math.isfinite(number):
        raise LabImportError(f"non-finite {field} in lab CSV row {row_number}")
    return number


def _timestamp(
    value: str | None, *, timezone: ZoneInfo, row_number: int, field: str
) -> datetime | None:
    if value is None:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise LabImportError(f"invalid {field} in lab CSV row {row_number}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        if parsed.time() == time.min:
            parsed = parsed.replace(hour=12)
        parsed = parsed.replace(tzinfo=timezone)
    return parsed


def _fasting(value: str | None, *, row_number: int) -> bool | None:
    if value is None:
        return None
    normalized = value.casefold()
    if normalized in {"yes", "y", "true", "1", "fasting", "fasted"}:
        return True
    if normalized in {"no", "n", "false", "0", "non-fasting", "not fasting"}:
        return False
    raise LabImportError(f"invalid fasting value in lab CSV row {row_number}")


def _reference_range(value: str | None) -> tuple[float | None, float | None]:
    if value is None:
        return None, None
    match = re.fullmatch(
        r"\s*(-?\d+(?:\.\d+)?)\s*(?:-|–|to)\s*(-?\d+(?:\.\d+)?)\s*", value
    )
    if match:
        return float(match.group(1)), float(match.group(2))
    lower = re.fullmatch(r"\s*>?=?\s*(-?\d+(?:\.\d+)?)\s*", value)
    upper = re.fullmatch(r"\s*<?=?\s*(-?\d+(?:\.\d+)?)\s*", value)
    if value.lstrip().startswith(">") and lower:
        return float(lower.group(1)), None
    if value.lstrip().startswith("<") and upper:
        return None, float(upper.group(1))
    return None, None


def _records(
    stream: BinaryIO,
    *,
    vocabulary: Mapping[str, str],
    timezone: ZoneInfo,
) -> Iterator[NormalizedRecord]:
    text = TextIOWrapper(stream, encoding="utf-8-sig", newline="")
    reader = csv.DictReader(text)
    columns = _columns(reader.fieldnames)
    occurrences: Counter[str] = Counter()
    try:
        for row_number, row in enumerate(reader, start=2):
            if None in row:
                raise LabImportError(f"lab CSV row {row_number} has more values than headers")
            if not any((value or "").strip() for value in row.values()):
                continue
            original_name = _optional(row, columns, "name")
            raw_value = _optional(row, columns, "value")
            if original_name is None or raw_value is None:
                raise LabImportError(f"lab CSV row {row_number} requires name and value")
            try:
                numeric_value = float(raw_value.replace(",", ""))
                if not math.isfinite(numeric_value):
                    raise ValueError
                text_value = None
            except ValueError:
                numeric_value = None
                text_value = raw_value
            reference_text = _optional(row, columns, "reference_text")
            parsed_low, parsed_high = _reference_range(reference_text)
            reference_low = _number(
                _optional(row, columns, "reference_low"),
                row_number=row_number,
                field="reference low",
            )
            reference_high = _number(
                _optional(row, columns, "reference_high"),
                row_number=row_number,
                field="reference high",
            )
            reference_low = reference_low if reference_low is not None else parsed_low
            reference_high = reference_high if reference_high is not None else parsed_high
            semantic = {
                key: (value or "").strip()
                for key, value in sorted(row.items())
                if key is not None
            }
            digest = hashlib.sha256(
                json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            occurrences[digest] += 1
            source_record_id = f"lab:{digest}:{occurrences[digest]}"
            canonical_name = vocabulary.get(_normalized(original_name))
            yield NormalizedRecord(
                "lab_result",
                source_record_id,
                {
                    "collected_at": _timestamp(
                        _optional(row, columns, "collected_at"),
                        timezone=timezone,
                        row_number=row_number,
                        field="collection date",
                    ),
                    "resulted_at": _timestamp(
                        _optional(row, columns, "resulted_at"),
                        timezone=timezone,
                        row_number=row_number,
                        field="result date",
                    ),
                    "canonical_name": canonical_name,
                    "original_name": original_name,
                    "numeric_value": numeric_value,
                    "text_value": text_value,
                    "unit": _optional(row, columns, "unit"),
                    "reference_low": reference_low,
                    "reference_high": reference_high,
                    "reference_text": reference_text,
                    "abnormal_flag": _optional(row, columns, "abnormal_flag"),
                    "provider": _optional(row, columns, "provider"),
                    "fasting": _fasting(
                        _optional(row, columns, "fasting"), row_number=row_number
                    ),
                    "source": "labs",
                    "source_record_id": source_record_id,
                    "transform_version": TRANSFORM_VERSION,
                    "metadata": {
                        "lab_csv": {"row": row_number, "columns": semantic},
                        "unknown_biomarker": canonical_name is None,
                    },
                },
            )
    except UnicodeDecodeError as exc:
        raise LabImportError("lab CSV must be UTF-8 encoded") from exc


class LabCsvConnector:
    name = "labs"
    source_type = "import"
    transform_version = TRANSFORM_VERSION

    def __init__(self, *, vocabulary: Mapping[str, str], timezone_name: str) -> None:
        self.vocabulary = vocabulary
        try:
            self.timezone = ZoneInfo(timezone_name)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise LabImportError(f"invalid project timezone: {timezone_name}") from exc

    def authenticate(self) -> None:
        return None

    def fetch(self, start: datetime, end: datetime) -> Iterable[object]:
        del start, end
        raise RuntimeError("lab CSVs are imported from local files")

    def normalize(self, raw_ref: RawRef, raw_store: RawStore) -> Iterable[NormalizedRecord]:
        def normalized() -> Iterator[NormalizedRecord]:
            with raw_store.open_verified(raw_ref) as stream:
                yield from _records(
                    stream, vocabulary=self.vocabulary, timezone=self.timezone
                )

        return normalized()


def _validated_records(
    path: Path, *, vocabulary: Mapping[str, str], timezone_name: str
) -> list[NormalizedRecord]:
    source = path.expanduser().resolve()
    if not source.is_file() or source.suffix.casefold() != ".csv":
        raise LabImportError("lab import requires a readable CSV file")
    if source.stat().st_size > MAX_CSV_BYTES:
        raise LabImportError("lab CSV is too large")
    try:
        timezone = ZoneInfo(timezone_name)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise LabImportError(f"invalid project timezone: {timezone_name}") from exc
    with source.open("rb") as stream:
        records = list(_records(stream, vocabulary=vocabulary, timezone=timezone))
    if not records:
        raise LabImportError("lab CSV contains no result rows")
    return records


def preview_lab_csv(
    path: Path, *, vocabulary: Mapping[str, str], timezone_name: str
) -> LabPreview:
    records = _validated_records(path, vocabulary=vocabulary, timezone_name=timezone_name)
    unknown = tuple(
        sorted(
            {
                str(record.values["original_name"])
                for record in records
                if record.values["canonical_name"] is None
            },
            key=str.casefold,
        )
    )
    recognized = sum(record.values["canonical_name"] is not None for record in records)
    return LabPreview(len(records), recognized, unknown)


def import_lab_csv(
    path: Path,
    *,
    vocabulary: Mapping[str, str],
    timezone_name: str,
    raw_store: RawStore,
    runner: IngestionRunner,
    now: datetime | None = None,
) -> RunResult:
    """Validate fully, retain the CSV immutably, and write long-format lab rows."""

    source = path.expanduser().resolve()
    _validated_records(source, vocabulary=vocabulary, timezone_name=timezone_name)
    raw_ref = raw_store.save_file(
        source,
        source="labs",
        endpoint="import/results.csv",
        retrieved_at=now or datetime.now(UTC),
        content_type="text/csv",
        request_metadata={"import_file": source.name},
        ingestion_run_id=None,
        transform_version=TRANSFORM_VERSION,
    )
    return runner.replay(
        LabCsvConnector(vocabulary=vocabulary, timezone_name=timezone_name), [raw_ref]
    )
