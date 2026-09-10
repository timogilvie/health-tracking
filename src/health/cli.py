"""Command-line interface for local health data operations."""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any

import duckdb
import httpx
import typer

from health.auth import FileSecretStore, OAuthStateError, SecretStoreError
from health.config import HealthSettings, load_project_config
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
from health.oss_policy import PolicyError, validate_repository_policy

app = typer.Typer(no_args_is_help=True, help="Local-first personal health data platform.")
auth_app = typer.Typer(no_args_is_help=True, help="Authorize provider accounts.")
withings_app = typer.Typer(no_args_is_help=True, help="Manage Withings OAuth credentials.")
sync_app = typer.Typer(no_args_is_help=True, help="Synchronize and inspect provider data.")
import_app = typer.Typer(no_args_is_help=True, help="Import provider export files.")
app.add_typer(auth_app, name="auth")
app.add_typer(withings_app, name="withings")
app.add_typer(sync_app, name="sync")
app.add_typer(import_app, name="import")
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
        help="Withings export ZIP, extracted directory, or CSV file.",
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
) -> dict[str, dict[str, Any]]:
    project_config = load_project_config(settings)
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
    return project_config


def _sync_window_policy(project_config: dict[str, dict[str, Any]]) -> SyncWindowPolicy:
    lookback = project_config["settings"].get("default_sync_lookback_days", 7)
    if isinstance(lookback, bool) or not isinstance(lookback, int) or lookback < 1:
        raise ValueError("default_sync_lookback_days must be a positive integer")
    return SyncWindowPolicy(initial_lookback=timedelta(days=lookback))


def _safe_sync_error(error: Exception) -> str:
    safe_errors = (
        MigrationError,
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


def exchange_withings_from_prompt(service: WithingsOAuth) -> None:
    code = typer.prompt("Authorization code", hide_input=True)
    state = typer.prompt("Returned state", hide_input=True)
    tokens = service.exchange_code(code, state)
    typer.echo(f"PASS Withings authorized; token expires at {tokens.expires_at.isoformat()}")


@app.command("init")
def init_command(root: RootOption = Path(".")) -> None:
    """Create private data directories and initialize the database."""

    settings = settings_for(root)
    load_project_config(settings)
    initialize_layout(settings)
    applied = migrate(settings.database, settings.project_root / "sql")
    suffix = f"; applied {len(applied)} migration(s)" if applied else "; schema current"
    typer.echo(f"Initialized {settings.project_root}{suffix}")


@app.command()
def doctor(root: RootOption = Path(".")) -> None:
    """Check configuration, paths, and database schema health."""

    settings = settings_for(root)
    checks: list[tuple[str, bool, str]] = []

    checks.append(("python", sys.version_info >= (3, 12), sys.version.split()[0]))
    try:
        load_project_config(settings)
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
        checks.append(("database", not pending and not drift, f"pending={pending}, drift={drift}"))
    except (MigrationError, OSError) as exc:
        checks.append(("database", False, str(exc)))

    for name, passed, detail in checks:
        typer.echo(f"{'PASS' if passed else 'FAIL'} {name}: {detail}")
    if not all(passed for _, passed, _ in checks):
        raise typer.Exit(code=1)


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
        project_config = _runtime_config(settings, require_directories=True)
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


@import_app.command("withings")
def import_withings_command(
    export_path: ExportPathArgument,
    root: RootOption = Path("."),
) -> None:
    """Import a downloaded Withings ZIP or supported CSV without API access."""

    settings = settings_for(root)
    try:
        project_config = _runtime_config(settings, require_directories=True)
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


@sync_app.command("status")
def sync_status(
    source: SourceOption = "withings",
    root: RootOption = Path("."),
) -> None:
    """Report local ingestion state without credentials or provider requests."""

    if source != "withings":
        raise typer.BadParameter(
            "only 'withings' is currently supported",
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


if __name__ == "__main__":
    app()
