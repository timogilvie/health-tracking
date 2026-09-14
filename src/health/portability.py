"""Offline database rebuild and portable analytical dataset exports."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

from health.config import HealthSettings, load_project_config
from health.connectors.apple_health import AppleHealthExportConnector
from health.connectors.labs import LabCsvConnector, load_biomarker_vocabulary
from health.connectors.oura import OuraConnector
from health.connectors.withings import WithingsExportConnector, WithingsMeasurementConnector
from health.db import connect, migrate
from health.ingestion import DuckDBCanonicalSink, IngestionRunner, RawRef, RawStore
from health.manual import ManualEventConnector, ManualWorkoutConnector
from health.transforms import source_priority_rows, sync_source_priorities

EXPORT_DATASETS = {
    "observations": "SELECT * FROM canonical_observations",
    "blood_pressure": "SELECT * FROM blood_pressure_sessions",
    "sleep": "SELECT * FROM canonical_sleep_daily",
    "workouts": "SELECT * FROM canonical_workouts",
    "labs": "SELECT * FROM lab_results",
    "events": "SELECT * FROM events",
    "daily": "SELECT * FROM daily_health",
    "weekly": "SELECT * FROM weekly_health",
    "rolling": "SELECT * FROM rolling_health",
}


class PortabilityError(RuntimeError):
    """A rebuild or export cannot be completed safely."""


@dataclass(frozen=True, slots=True)
class ExportReport:
    output: Path
    format: str
    files: int


@dataclass(frozen=True, slots=True)
class RebuildReport:
    database: Path
    raw_artifacts: int
    normalized: int
    inserted: int
    updated: int
    duplicate: int
    skipped_parent_archives: int


class _OfflineOAuth:
    """Constructor-compatible guard that fails if replay attempts authentication."""

    def authenticate(self) -> None:  # pragma: no cover - only called by a regression
        raise PortabilityError("offline rebuild attempted provider authentication")

    def access_token(self) -> str:  # pragma: no cover - only called by a regression
        raise PortabilityError("offline rebuild attempted provider token access")


def export_datasets(database: Path, output: Path, *, format_name: str) -> ExportReport:
    """Atomically export canonical analytical datasets as Parquet or CSV files."""

    normalized_format = format_name.casefold()
    if normalized_format not in {"parquet", "csv"}:
        raise PortabilityError("export format must be parquet or csv")
    database = database.expanduser().resolve()
    target = output.expanduser().resolve()
    if not database.is_file():
        raise PortabilityError(f"database does not exist: {database}")
    if target.exists():
        raise PortabilityError(f"export target already exists: {target}")

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.building")
    temporary.mkdir(mode=0o700)
    suffix = ".parquet" if normalized_format == "parquet" else ".csv"
    try:
        with connect(database, read_only=True) as connection:
            for name, query in EXPORT_DATASETS.items():
                path = temporary / f"{name}{suffix}"
                if normalized_format == "parquet":
                    connection.execute(f"COPY ({query}) TO ? (FORMAT PARQUET)", [str(path)])
                else:
                    connection.execute(
                        f"COPY ({query}) TO ? (FORMAT CSV, HEADER true)", [str(path)]
                    )
                os.chmod(path, 0o600)
        temporary.rename(target)
    except Exception:
        for child in temporary.iterdir() if temporary.exists() else ():
            child.unlink(missing_ok=True)
        temporary.rmdir()
        raise
    return ExportReport(target, normalized_format, len(EXPORT_DATASETS))


def _route_raw_refs(
    raw_store: RawStore,
) -> tuple[dict[str, list[RawRef]], int]:
    routes: dict[str, list[RawRef]] = {}
    skipped = 0
    for raw_ref in raw_store.iter_refs():
        manifest = raw_store.manifest(raw_ref)
        source = manifest.get("source")
        endpoint = manifest.get("endpoint")
        content_type = str(manifest.get("content_type") or "").partition(";")[0].casefold()
        route: str | None = None
        if source == "apple_health":
            route = "apple_health"
        elif source == "withings" and content_type == "text/csv":
            route = "withings_export"
        elif source == "withings" and content_type == "application/json":
            route = "withings_api"
        elif source == "withings" and endpoint == "export/archive":
            skipped += 1
            continue
        elif source == "oura":
            route = "oura"
        elif source == "labs":
            route = "labs"
        elif source == "manual" and endpoint == "manual/workout":
            route = "manual_workout"
        elif source == "manual" and endpoint == "manual/event":
            route = "manual_event"
        if route is None:
            raise PortabilityError(
                f"no offline replay adapter for source={source!r}, endpoint={endpoint!r}"
            )
        routes.setdefault(route, []).append(raw_ref)
    return routes, skipped


def rebuild_database(settings: HealthSettings, target: Path) -> RebuildReport:
    """Rebuild a new DuckDB solely from verified immutable raw artifacts."""

    target = target.expanduser().resolve()
    if target == settings.database.resolve():
        raise PortabilityError("rebuild target must differ from the active database")
    if target.exists():
        raise PortabilityError(f"rebuild target already exists: {target}")
    if not settings.raw.is_dir():
        raise PortabilityError(f"raw store does not exist: {settings.raw}")

    config = load_project_config(settings)
    source_priority_rows(config)
    timezone_name = str(config["settings"].get("timezone") or "")
    vocabulary = load_biomarker_vocabulary(config["biomarkers"])
    raw_store = RawStore(settings.raw)
    routes, skipped = _route_raw_refs(raw_store)
    if not routes:
        raise PortabilityError("raw store contains no replayable artifacts")

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.building")
    totals = {"normalized": 0, "inserted": 0, "updated": 0, "duplicate": 0}
    oauth: Any = _OfflineOAuth()
    try:
        migrate(temporary, settings.project_root / "sql")
        sync_source_priorities(temporary, config)
        runner = IngestionRunner(
            database=temporary,
            raw_store=raw_store,
            sink=DuckDBCanonicalSink(temporary),
        )
        with httpx.Client() as client:
            connectors = {
                "apple_health": AppleHealthExportConnector(),
                "withings_export": WithingsExportConnector(timezone_name=timezone_name),
                "withings_api": WithingsMeasurementConnector(oauth=oauth, client=client),
                "oura": OuraConnector(oauth=oauth, timezone_name=timezone_name),
                "labs": LabCsvConnector(
                    vocabulary=vocabulary,
                    timezone_name=timezone_name,
                ),
                "manual_workout": ManualWorkoutConnector(),
                "manual_event": ManualEventConnector(),
            }
            for route in (
                "apple_health",
                "withings_export",
                "withings_api",
                "oura",
                "labs",
                "manual_workout",
                "manual_event",
            ):
                refs = routes.get(route)
                if not refs:
                    continue
                result = runner.replay(connectors[route], refs)
                totals["normalized"] += result.normalized_count
                totals["inserted"] += result.inserted_count
                totals["updated"] += result.updated_count
                totals["duplicate"] += result.duplicate_count
        temporary.rename(target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return RebuildReport(
        database=target,
        raw_artifacts=sum(len(refs) for refs in routes.values()),
        normalized=totals["normalized"],
        inserted=totals["inserted"],
        updated=totals["updated"],
        duplicate=totals["duplicate"],
        skipped_parent_archives=skipped,
    )
