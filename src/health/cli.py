"""Command-line interface for local health data operations."""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

import duckdb
import httpx
import typer

from health.auth import FileSecretStore, OAuthStateError, SecretStoreError
from health.config import HealthSettings, load_project_config
from health.connectors.apple_health import AppleHealthExportError, import_apple_health_export
from health.connectors.oura import (
    OuraAPIError,
    OuraConnector,
    OuraOAuth,
    OuraOAuthConfig,
    OuraOAuthError,
    OuraPayloadError,
)
from health.connectors.withings import (
    WithingsAPIError,
    WithingsExportError,
    WithingsMeasurementConnector,
    WithingsOAuth,
    WithingsOAuthConfig,
    WithingsOAuthError,
    WithingsPaginationError,
    WithingsPayloadError,
    import_withings_export,
)
from health.dashboard import serve_dashboard
from health.db import MigrationError, connect, migrate, migration_status
from health.ingestion import (
    DuckDBCanonicalSink,
    IngestionRunner,
    RawStorageError,
    RawStore,
    RetryableIngestionError,
    SyncWindowPolicy,
)
from health.layout import initialize_layout
from health.manual import ManualWorkoutError, build_manual_workout, record_manual_workout
from health.oss_policy import PolicyError, validate_repository_policy
from health.transforms import (
    DuplicateResolutionError,
    list_duplicate_candidates,
    reconcile_duplicates,
    resolve_duplicate,
    source_priority_rows,
    sync_source_priorities,
)

app = typer.Typer(no_args_is_help=True, help="Local-first personal health data platform.")
auth_app = typer.Typer(no_args_is_help=True, help="Authorize provider accounts.")
withings_app = typer.Typer(no_args_is_help=True, help="Manage Withings OAuth credentials.")
oura_app = typer.Typer(no_args_is_help=True, help="Manage Oura OAuth credentials.")
sync_app = typer.Typer(no_args_is_help=True, help="Synchronize and inspect provider data.")
import_app = typer.Typer(no_args_is_help=True, help="Import provider export files.")
workout_app = typer.Typer(no_args_is_help=True, help="Record manual workouts.")
duplicates_app = typer.Typer(no_args_is_help=True, help="Reconcile and review duplicate links.")
app.add_typer(auth_app, name="auth")
app.add_typer(withings_app, name="withings")
app.add_typer(oura_app, name="oura")
app.add_typer(sync_app, name="sync")
app.add_typer(import_app, name="import")
app.add_typer(workout_app, name="workout")
app.add_typer(duplicates_app, name="duplicates")
RootOption = Annotated[
    Path,
    typer.Option(
        "--root",
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        help="Project root containing config/ and sql/.",
    ),
]
TimestampOption = Annotated[
    str | None,
    typer.Option(help="Timezone-aware ISO-8601 timestamp."),
]
SourceOption = Annotated[
    str,
    typer.Option(help="Provider source to inspect."),
]
ExportPathArgument = Annotated[
    Path,
    typer.Argument(
        exists=True,
        readable=True,
        resolve_path=True,
        help="Provider export file or extracted directory.",
    ),
]


def settings_for(root: Path) -> HealthSettings:
    return HealthSettings(project_root=root, _env_file=root / ".env")


def withings_oauth(settings: HealthSettings, client: httpx.Client) -> WithingsOAuth:
    return WithingsOAuth(
        config=WithingsOAuthConfig(
            client_id=settings.withings_client_id,
            client_secret=settings.withings_client_secret.get_secret_value(),
            redirect_uri=settings.withings_redirect_uri,
            scope=settings.withings_scope,
        ),
        secret_store=FileSecretStore(settings.secrets),
        client=client,
    )


def oura_oauth(settings: HealthSettings) -> OuraOAuth:
    return OuraOAuth(
        config=OuraOAuthConfig(
            client_id=settings.oura_client_id,
            client_secret=settings.oura_client_secret.get_secret_value(),
            redirect_uri=settings.oura_redirect_uri,
            scope=settings.oura_scope,
        ),
        secret_store=FileSecretStore(settings.secrets),
    )


def oura_connector(settings: HealthSettings, timezone_name: str) -> OuraConnector:
    return OuraConnector(
        oauth=oura_oauth(settings),
        timezone_name=timezone_name,
    )


def http_client() -> httpx.Client:
    """Create the shared provider client; isolated for deterministic CLI tests."""

    return httpx.Client()


def _parse_timestamp(value: str | None, *, option: str) -> datetime | None:
    if value is None:
        return None
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(
            "must be an ISO-8601 timestamp",
            param_hint=f"--{option}",
        ) from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise typer.BadParameter(
            "must include a timezone offset",
            param_hint=f"--{option}",
        )
    return timestamp


def _runtime_config(
    settings: HealthSettings,
    *,
    require_directories: bool,
    refresh_priorities: bool = False,
) -> dict[str, dict[str, Any]]:
    project_config = load_project_config(settings)
    source_priority_rows(project_config)
    if require_directories:
        required = (settings.raw, settings.exports, settings.snapshots, settings.secrets)
        missing = [str(path) for path in required if not path.is_dir()]
        if missing:
            raise MigrationError("project is not initialized; run `health init`")
    if not settings.database.is_file():
        raise MigrationError("database is not initialized; run `health init`")
    with connect(settings.database, read_only=True) as connection:
        pending, drift = migration_status(connection, settings.project_root / "sql")
    if pending or drift:
        raise MigrationError(f"database schema is not current: pending={pending}, drift={drift}")
    if refresh_priorities:
        sync_source_priorities(settings.database, project_config)
    return project_config


def _sync_window_policy(project_config: dict[str, dict[str, Any]]) -> SyncWindowPolicy:
    lookback = project_config["settings"].get("default_sync_lookback_days", 7)
    if isinstance(lookback, bool) or not isinstance(lookback, int) or lookback < 1:
        raise ValueError("default_sync_lookback_days must be a positive integer")
    return SyncWindowPolicy(initial_lookback=timedelta(days=lookback))


def _safe_sync_error(error: Exception) -> str:
    safe_errors = (
        AppleHealthExportError,
        DuplicateResolutionError,
        MigrationError,
        ManualWorkoutError,
        OuraAPIError,
        OuraOAuthError,
        OuraPayloadError,
        RawStorageError,
        RetryableIngestionError,
        SecretStoreError,
        WithingsAPIError,
        WithingsExportError,
        WithingsOAuthError,
        WithingsPaginationError,
        WithingsPayloadError,
    )
    if isinstance(error, safe_errors):
        return str(error)
    if isinstance(error, httpx.TransportError):
        return "network unavailable"
    if isinstance(error, duckdb.Error):
        return "database error"
    if isinstance(error, OSError):
        return "filesystem error"
    if isinstance(error, ValueError):
        return "configuration error"
    return f"unexpected {type(error).__name__}"


def _iso(value: datetime | None) -> str:
    return value.astimezone(UTC).isoformat() if value is not None else "none"


def current_time() -> datetime:
    """Return the current instant; isolated for deterministic command tests."""

    return datetime.now(UTC)


def _parse_local_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("must be YYYY-MM-DD", param_hint="--date") from exc


def _parse_local_time(value: str | None) -> time | None:
    if value is None:
        return None
    try:
        parsed = time.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("must be HH:MM", param_hint="--time") from exc
    if parsed.tzinfo is not None:
        raise typer.BadParameter("must be a local time without an offset", param_hint="--time")
    return parsed


def _record_manual_workout(
    *,
    workout_type: str,
    minutes: int,
    workout_date: str | None,
    workout_time: str | None,
    focus: str | None,
    rpe: float | None,
    notes: str | None,
    root: Path,
) -> None:
    settings = settings_for(root)
    try:
        project_config = _runtime_config(
            settings,
            require_directories=True,
            refresh_priorities=True,
        )
        timezone_name = project_config["settings"].get("timezone")
        if not isinstance(timezone_name, str) or not timezone_name.strip():
            raise ManualWorkoutError("settings.timezone must be an IANA timezone name")
        document = build_manual_workout(
            workout_type=workout_type,
            minutes=minutes,
            timezone_name=timezone_name,
            workout_date=_parse_local_date(workout_date),
            workout_time=_parse_local_time(workout_time),
            focus=focus,
            rpe=rpe,
            notes=notes,
            now=current_time(),
        )
        raw_store = RawStore(settings.raw)
        result = record_manual_workout(
            document,
            raw_store=raw_store,
            runner=IngestionRunner(
                database=settings.database,
                raw_store=raw_store,
                sink=DuckDBCanonicalSink(settings.database),
            ),
        )
    except typer.BadParameter:
        raise
    except Exception as exc:
        typer.echo(f"FAIL Manual workout: {_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc

    typer.echo(
        "PASS Manual workout: "
        f"run_id={result.ingestion_run_id} "
        f"inserted={result.inserted_count} "
        f"updated={result.updated_count} "
        f"duplicate={result.duplicate_count}"
    )


def exchange_withings_from_prompt(service: WithingsOAuth) -> None:
    code = typer.prompt("Authorization code", hide_input=True)
    state = typer.prompt("Returned state", hide_input=True)
    tokens = service.exchange_code(code, state)
    typer.echo(f"PASS Withings authorized; token expires at {tokens.expires_at.isoformat()}")


def exchange_oura_from_prompt(service: OuraOAuth) -> None:
    code = typer.prompt("Authorization code", hide_input=True)
    state = typer.prompt("Returned state", hide_input=True)
    tokens = service.exchange_code(code, state)
    typer.echo(f"PASS Oura authorized; token expires at {tokens.expires_at.isoformat()}")


@app.command("init")
def init_command(root: RootOption = Path(".")) -> None:
    """Create private data directories and initialize the database."""

    settings = settings_for(root)
    project_config = load_project_config(settings)
    source_priority_rows(project_config)
    initialize_layout(settings)
    applied = migrate(settings.database, settings.project_root / "sql")
    sync_source_priorities(settings.database, project_config)
    suffix = f"; applied {len(applied)} migration(s)" if applied else "; schema current"
    typer.echo(f"Initialized {settings.project_root}{suffix}")


@app.command()
def doctor(root: RootOption = Path(".")) -> None:
    """Check configuration, paths, and database schema health."""

    settings = settings_for(root)
    checks: list[tuple[str, bool, str]] = []
    project_config: dict[str, dict[str, Any]] | None = None

    checks.append(("python", sys.version_info >= (3, 12), sys.version.split()[0]))
    try:
        project_config = load_project_config(settings)
        source_priority_rows(project_config)
        checks.append(("config", True, str(settings.configs)))
    except ValueError as exc:
        checks.append(("config", False, str(exc)))

    required_dirs = (settings.raw, settings.exports, settings.snapshots)
    missing_dirs = [str(path) for path in required_dirs if not path.is_dir()]
    checks.append(("data directories", not missing_dirs, ", ".join(missing_dirs) or "present"))

    try:
        if not settings.database.is_file():
            raise MigrationError(f"database does not exist: {settings.database}")
        with connect(settings.database, read_only=True) as connection:
            pending, drift = migration_status(connection, settings.project_root / "sql")
            if not pending and not drift and project_config is not None:
                actual_priorities = connection.execute(
                    "SELECT data_type, source, priority FROM source_priorities "
                    "ORDER BY data_type, priority"
                ).fetchall()
                expected_priorities = sorted(
                    source_priority_rows(project_config),
                    key=lambda row: (row[0], row[2]),
                )
                priorities_current = actual_priorities == expected_priorities
                checks.append(
                    (
                        "source priorities",
                        priorities_current,
                        "current" if priorities_current else "run `health init`",
                    )
                )
        checks.append(("database", not pending and not drift, f"pending={pending}, drift={drift}"))
    except (MigrationError, OSError) as exc:
        checks.append(("database", False, str(exc)))

    for name, passed, detail in checks:
        typer.echo(f"{'PASS' if passed else 'FAIL'} {name}: {detail}")
    if not all(passed for _, passed, _ in checks):
        raise typer.Exit(code=1)


@app.command("dashboard")
def dashboard_command(
    port: Annotated[
        int,
        typer.Option(min=1, max=65_535, help="Private localhost port."),
    ] = 8766,
    open_browser: Annotated[
        bool,
        typer.Option("--open/--no-open", help="Open the dashboard in the default browser."),
    ] = True,
    root: RootOption = Path("."),
) -> None:
    """Serve the private health dashboard on the loopback interface."""

    settings = settings_for(root)
    try:
        _runtime_config(
            settings,
            require_directories=True,
            refresh_priorities=True,
        )
        serve_dashboard(
            settings.database,
            port=port,
            open_browser=open_browser,
        )
    except Exception as exc:
        typer.echo(f"FAIL Dashboard: {_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc


@app.command("policy-check")
def policy_check(root: RootOption = Path(".")) -> None:
    """Validate OSS licenses, provenance, attribution, and lockfile pins."""

    try:
        report = validate_repository_policy(root)
    except (OSError, PolicyError) as exc:
        typer.echo(f"FAIL policy: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(
        "PASS policy: "
        f"{report.entries} entries, "
        f"{report.packages_verified} locked package(s), "
        f"{report.source_references_verified} source reference(s)"
    )


@duplicates_app.command("refresh")
def duplicates_refresh(root: RootOption = Path(".")) -> None:
    """Reconcile cross-source duplicates while retaining every original row."""

    settings = settings_for(root)
    try:
        _runtime_config(settings, require_directories=False, refresh_priorities=True)
        report = reconcile_duplicates(settings.database)
    except Exception as exc:
        typer.echo(f"FAIL Duplicate reconciliation: {_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc
    typer.echo(
        "PASS Duplicate reconciliation: "
        f"matched={report.matched} confirmed={report.confirmed} "
        f"candidates={report.candidates} removed_stale={report.removed_stale}"
    )


@duplicates_app.command("list")
def duplicates_list(root: RootOption = Path(".")) -> None:
    """List ambiguous duplicate candidates without printing health values."""

    settings = settings_for(root)
    try:
        _runtime_config(settings, require_directories=False)
        candidates = list_duplicate_candidates(settings.database)
    except Exception as exc:
        typer.echo(f"FAIL Duplicate candidates: {_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc
    if not candidates:
        typer.echo("PASS Duplicate candidates: none")
        return
    for candidate in candidates:
        typer.echo(
            f"{candidate['id']} type={candidate['record_type']} "
            f"method={candidate['method']} confidence={candidate['confidence']:.3f} "
            f"canonical={candidate['canonical_record_id']} "
            f"duplicate={candidate['duplicate_record_id']}"
        )


@duplicates_app.command("resolve")
def duplicates_resolve(
    link_id: Annotated[UUID, typer.Argument(help="Duplicate link UUID from `duplicates list`.")],
    resolution: Annotated[
        str,
        typer.Option(help="Review decision: confirmed or rejected."),
    ],
    root: RootOption = Path("."),
) -> None:
    """Confirm or reject an ambiguous duplicate candidate."""

    if resolution not in {"confirmed", "rejected"}:
        raise typer.BadParameter(
            "must be confirmed or rejected", param_hint="--resolution"
        )
    settings = settings_for(root)
    try:
        _runtime_config(settings, require_directories=False)
        resolve_duplicate(settings.database, link_id, resolution)
    except Exception as exc:
        typer.echo(f"FAIL Duplicate resolution: {_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc
    typer.echo(f"PASS Duplicate resolution: id={link_id} resolution={resolution}")


@app.command("lift")
def lift_command(
    minutes: Annotated[
        int,
        typer.Argument(min=1, max=1_440, help="Workout duration in minutes."),
    ],
    workout_date: Annotated[
        str | None,
        typer.Option("--date", help="Local workout date (YYYY-MM-DD)."),
    ] = None,
    workout_time: Annotated[
        str | None,
        typer.Option("--time", help="Local workout start time (HH:MM)."),
    ] = None,
    focus: Annotated[
        str | None,
        typer.Option(help="Resistance focus, such as upper, lower, or full body."),
    ] = None,
    rpe: Annotated[
        float | None,
        typer.Option(min=0, max=10, help="Session effort from 0 to 10."),
    ] = None,
    notes: Annotated[str | None, typer.Option(help="Optional private notes.")] = None,
    root: RootOption = Path("."),
) -> None:
    """Record resistance training; for example, `health lift 55`."""

    _record_manual_workout(
        workout_type="resistance",
        minutes=minutes,
        workout_date=workout_date,
        workout_time=workout_time,
        focus=focus,
        rpe=rpe,
        notes=notes,
        root=root,
    )


@workout_app.command("add")
def workout_add(
    workout_type: Annotated[
        str,
        typer.Option("--type", help="Canonical workout type."),
    ],
    minutes: Annotated[
        int,
        typer.Option("--minutes", min=1, max=1_440, help="Duration in minutes."),
    ],
    workout_date: Annotated[
        str | None,
        typer.Option("--date", help="Local workout date (YYYY-MM-DD)."),
    ] = None,
    workout_time: Annotated[
        str | None,
        typer.Option("--time", help="Local workout start time (HH:MM)."),
    ] = None,
    focus: Annotated[str | None, typer.Option(help="Optional workout focus.")] = None,
    rpe: Annotated[
        float | None,
        typer.Option(min=0, max=10, help="Session effort from 0 to 10."),
    ] = None,
    notes: Annotated[str | None, typer.Option(help="Optional private notes.")] = None,
    root: RootOption = Path("."),
) -> None:
    """Record a dated manual workout of any canonical type."""

    _record_manual_workout(
        workout_type=workout_type,
        minutes=minutes,
        workout_date=workout_date,
        workout_time=workout_time,
        focus=focus,
        rpe=rpe,
        notes=notes,
        root=root,
    )


@sync_app.command("withings")
def sync_withings(
    start: TimestampOption = None,
    end: TimestampOption = None,
    root: RootOption = Path("."),
) -> None:
    """Synchronize Withings measurements through the raw-first ingestion pipeline."""

    requested_start = _parse_timestamp(start, option="start")
    requested_end = _parse_timestamp(end, option="end")
    if (
        requested_start is not None
        and requested_end is not None
        and requested_start > requested_end
    ):
        raise typer.BadParameter(
            "must not be before --start",
            param_hint="--end",
        )

    settings = settings_for(root)
    try:
        project_config = _runtime_config(
            settings,
            require_directories=True,
            refresh_priorities=True,
        )
        window_policy = _sync_window_policy(project_config)
        with http_client() as client:
            oauth = withings_oauth(settings, client)
            connector = WithingsMeasurementConnector(oauth=oauth, client=client)
            runner = IngestionRunner(
                database=settings.database,
                raw_store=RawStore(settings.raw),
                sink=DuckDBCanonicalSink(settings.database),
                window_policy=window_policy,
            )
            result = runner.sync(
                connector,
                start=requested_start,
                end=requested_end,
            )
    except ValueError:
        typer.echo("FAIL Withings sync: configuration error")
        raise typer.Exit(code=1) from None
    except Exception as exc:
        typer.echo(f"FAIL Withings sync: {_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc

    typer.echo(
        "PASS Withings sync: "
        f"source={result.source} "
        f"run_id={result.ingestion_run_id} "
        f"start={_iso(result.requested_start)} "
        f"end={_iso(result.requested_end)} "
        f"raw={result.raw_count} "
        f"normalized={result.normalized_count} "
        f"inserted={result.inserted_count} "
        f"updated={result.updated_count} "
        f"duplicate={result.duplicate_count}"
    )


@sync_app.command("oura")
def sync_oura(
    start: TimestampOption = None,
    end: TimestampOption = None,
    root: RootOption = Path("."),
) -> None:
    """Synchronize Oura recovery and activity data through one source watermark."""

    requested_start = _parse_timestamp(start, option="start")
    requested_end = _parse_timestamp(end, option="end")
    if (
        requested_start is not None
        and requested_end is not None
        and requested_start > requested_end
    ):
        raise typer.BadParameter("must not be before --start", param_hint="--end")

    settings = settings_for(root)
    try:
        project_config = _runtime_config(
            settings,
            require_directories=True,
            refresh_priorities=True,
        )
        timezone_name = project_config["settings"].get("timezone")
        if not isinstance(timezone_name, str) or not timezone_name.strip():
            raise ValueError("settings.timezone must be an IANA timezone name")
        runner = IngestionRunner(
            database=settings.database,
            raw_store=RawStore(settings.raw),
            sink=DuckDBCanonicalSink(settings.database),
            window_policy=_sync_window_policy(project_config),
        )
        result = runner.sync(
            oura_connector(settings, timezone_name),
            start=requested_start,
            end=requested_end,
        )
    except Exception as exc:
        typer.echo(f"FAIL Oura sync: {_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc

    typer.echo(
        "PASS Oura sync: "
        f"source={result.source} "
        f"run_id={result.ingestion_run_id} "
        f"start={_iso(result.requested_start)} "
        f"end={_iso(result.requested_end)} "
        f"raw={result.raw_count} "
        f"normalized={result.normalized_count} "
        f"inserted={result.inserted_count} "
        f"updated={result.updated_count} "
        f"duplicate={result.duplicate_count}"
    )


@import_app.command("withings")
def import_withings_command(
    export_path: ExportPathArgument,
    root: RootOption = Path("."),
) -> None:
    """Import a downloaded Withings ZIP or supported CSV without API access."""

    settings = settings_for(root)
    try:
        project_config = _runtime_config(
            settings,
            require_directories=True,
            refresh_priorities=True,
        )
        timezone_name = project_config["settings"].get("timezone")
        if not isinstance(timezone_name, str) or not timezone_name.strip():
            raise WithingsExportError("settings.timezone must be an IANA timezone name")
        raw_store = RawStore(settings.raw)
        result = import_withings_export(
            export_path,
            timezone_name=timezone_name,
            raw_store=raw_store,
            runner=IngestionRunner(
                database=settings.database,
                raw_store=raw_store,
                sink=DuckDBCanonicalSink(settings.database),
            ),
        )
    except Exception as exc:
        typer.echo(f"FAIL Withings export import: {_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc

    typer.echo(
        "PASS Withings export import: "
        f"source={result.source} "
        f"run_id={result.ingestion_run_id} "
        f"files={result.raw_count} "
        f"normalized={result.normalized_count} "
        f"inserted={result.inserted_count} "
        f"updated={result.updated_count} "
        f"duplicate={result.duplicate_count}"
    )


@import_app.command("apple-health")
def import_apple_health_command(
    export_path: ExportPathArgument,
    root: RootOption = Path("."),
) -> None:
    """Import an Apple Health ZIP or export.xml with bounded parser memory."""

    settings = settings_for(root)
    try:
        _runtime_config(
            settings,
            require_directories=True,
            refresh_priorities=True,
        )
        raw_store = RawStore(settings.raw)
        result = import_apple_health_export(
            export_path,
            raw_store=raw_store,
            runner=IngestionRunner(
                database=settings.database,
                raw_store=raw_store,
                sink=DuckDBCanonicalSink(settings.database),
            ),
        )
    except Exception as exc:
        typer.echo(f"FAIL Apple Health export import: {_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc

    typer.echo(
        "PASS Apple Health export import: "
        f"source={result.source} "
        f"run_id={result.ingestion_run_id} "
        f"files={result.raw_count} "
        f"normalized={result.normalized_count} "
        f"inserted={result.inserted_count} "
        f"updated={result.updated_count} "
        f"duplicate={result.duplicate_count}"
    )


@sync_app.command("status")
def sync_status(
    source: SourceOption = "withings",
    root: RootOption = Path("."),
) -> None:
    """Report local ingestion state without credentials or provider requests."""

    if source not in {"withings", "oura"}:
        raise typer.BadParameter(
            "must be 'withings' or 'oura'",
            param_hint="--source",
        )
    settings = settings_for(root)
    try:
        _runtime_config(settings, require_directories=False)
        with connect(settings.database, read_only=True) as connection:
            source_row = connection.execute(
                "SELECT source_id FROM sources WHERE name = ?",
                [source],
            ).fetchone()
            if source_row is None:
                latest = None
                last_successful_end = None
            else:
                latest = connection.execute(
                    """
                    SELECT ingestion_run_id, status, requested_start, requested_end,
                           started_at, finished_at, raw_count, normalized_count,
                           inserted_count, updated_count, duplicate_count, error
                    FROM ingestion_runs
                    WHERE source_id = ?
                    ORDER BY started_at DESC, ingestion_run_id DESC
                    LIMIT 1
                    """,
                    [source_row[0]],
                ).fetchone()
                watermark = connection.execute(
                    """
                    SELECT last_successful_end
                    FROM source_sync_state
                    WHERE source_id = ?
                    """,
                    [source_row[0]],
                ).fetchone()
                last_successful_end = watermark[0] if watermark else None
    except Exception as exc:
        typer.echo(f"FAIL Sync status: source={source} error={_safe_sync_error(exc)}")
        raise typer.Exit(code=1) from exc

    if latest is None:
        typer.echo(f"PASS Sync status: source={source} state=never-synced")
        return

    error_category = str(latest[11]).partition(":")[0] if latest[11] else "none"
    passed = latest[1] != "failed"
    typer.echo(
        f"{'PASS' if passed else 'FAIL'} Sync status: "
        f"source={source} "
        f"state={latest[1]} "
        f"run_id={latest[0]} "
        f"start={_iso(latest[2])} "
        f"end={_iso(latest[3])} "
        f"started_at={_iso(latest[4])} "
        f"finished_at={_iso(latest[5])} "
        f"raw={latest[6]} "
        f"normalized={latest[7]} "
        f"inserted={latest[8]} "
        f"updated={latest[9]} "
        f"duplicate={latest[10]} "
        f"last_successful_end={_iso(last_successful_end)} "
        f"error={error_category}"
    )
    if not passed:
        raise typer.Exit(code=1)


@withings_app.command("authorize-url")
def withings_authorize_url(root: RootOption = Path(".")) -> None:
    """Start OAuth and print the URL to open in a browser."""

    settings = settings_for(root)
    with httpx.Client() as client:
        service = withings_oauth(settings, client)
        try:
            request = service.begin_authorization()
        except (OSError, SecretStoreError, WithingsOAuthError) as exc:
            typer.echo(f"FAIL Withings authorization: {exc}")
            raise typer.Exit(code=1) from exc
    typer.echo(request.url)
    typer.echo("OAuth state saved locally for 15 minutes.")


@withings_app.command("exchange")
def withings_exchange(root: RootOption = Path(".")) -> None:
    """Exchange a callback code and persist the returned token pair."""

    settings = settings_for(root)
    with httpx.Client() as client:
        service = withings_oauth(settings, client)
        try:
            exchange_withings_from_prompt(service)
        except (OSError, OAuthStateError, SecretStoreError, WithingsOAuthError) as exc:
            typer.echo(f"FAIL Withings token exchange: {exc}")
            raise typer.Exit(code=1) from exc


@auth_app.command("withings")
def auth_withings(root: RootOption = Path(".")) -> None:
    """Run the complete Withings OAuth flow from the terminal."""

    settings = settings_for(root)
    with httpx.Client() as client:
        service = withings_oauth(settings, client)
        try:
            request = service.begin_authorization()
            typer.echo(request.url)
            typer.echo("Complete authorization in the browser, then enter the callback values.")
            exchange_withings_from_prompt(service)
        except (OSError, OAuthStateError, SecretStoreError, WithingsOAuthError) as exc:
            typer.echo(f"FAIL Withings authorization: {exc}")
            raise typer.Exit(code=1) from exc


@withings_app.command("status")
def withings_status(root: RootOption = Path(".")) -> None:
    """Report configuration and stored-token health without revealing secrets."""

    settings = settings_for(root)
    try:
        with httpx.Client() as client:
            status = withings_oauth(settings, client).status()
    except (OSError, SecretStoreError) as exc:
        typer.echo(f"FAIL Withings credentials: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(
        f"{'PASS' if status.configured else 'FAIL'} Withings configuration: "
        f"{'complete' if status.configured else status.detail}"
    )
    expiry = status.expires_at.isoformat() if status.expires_at else "none"
    typer.echo(
        f"{'PASS' if status.token_state == 'valid' else 'FAIL'} Withings tokens: "
        f"state={status.token_state}, expires_at={expiry}"
    )
    if not status.configured or status.token_state != "valid":
        raise typer.Exit(code=1)


@oura_app.command("authorize-url")
def oura_authorize_url(root: RootOption = Path(".")) -> None:
    """Start Oura OAuth and print the URL to open in a browser."""

    settings = settings_for(root)
    try:
        request = oura_oauth(settings).begin_authorization()
    except (OSError, SecretStoreError, OuraOAuthError) as exc:
        typer.echo(f"FAIL Oura authorization: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(request.url)
    typer.echo("OAuth state saved locally for 15 minutes.")


@oura_app.command("exchange")
def oura_exchange(root: RootOption = Path(".")) -> None:
    """Exchange an Oura callback code and persist the rotated token pair."""

    settings = settings_for(root)
    try:
        exchange_oura_from_prompt(oura_oauth(settings))
    except (OSError, OAuthStateError, SecretStoreError, OuraOAuthError) as exc:
        typer.echo(f"FAIL Oura token exchange: {exc}")
        raise typer.Exit(code=1) from exc


@auth_app.command("oura")
def auth_oura(root: RootOption = Path(".")) -> None:
    """Run the complete Oura OAuth flow from the terminal."""

    settings = settings_for(root)
    service = oura_oauth(settings)
    try:
        request = service.begin_authorization()
        typer.echo(request.url)
        typer.echo("Complete authorization in the browser, then enter the callback values.")
        exchange_oura_from_prompt(service)
    except (OSError, OAuthStateError, SecretStoreError, OuraOAuthError) as exc:
        typer.echo(f"FAIL Oura authorization: {exc}")
        raise typer.Exit(code=1) from exc


@oura_app.command("status")
def oura_status(root: RootOption = Path(".")) -> None:
    """Report Oura configuration and token health without revealing secrets."""

    settings = settings_for(root)
    try:
        status = oura_oauth(settings).status()
    except (OSError, SecretStoreError) as exc:
        typer.echo(f"FAIL Oura credentials: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(
        f"{'PASS' if status.configured else 'FAIL'} Oura configuration: "
        f"{'complete' if status.configured else status.detail}"
    )
    expiry = status.expires_at.isoformat() if status.expires_at else "none"
    typer.echo(
        f"{'PASS' if status.token_state == 'valid' else 'FAIL'} Oura tokens: "
        f"state={status.token_state}, expires_at={expiry}"
    )
    if not status.configured or status.token_state != "valid":
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
