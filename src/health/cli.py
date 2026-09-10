"""Command-line interface for local health data operations."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import httpx
import typer

from health.auth import FileSecretStore, OAuthStateError, SecretStoreError
from health.config import HealthSettings, load_project_config
from health.connectors.withings import WithingsOAuth, WithingsOAuthConfig, WithingsOAuthError
from health.db import MigrationError, connect, migrate, migration_status
from health.layout import initialize_layout
from health.oss_policy import PolicyError, validate_repository_policy

app = typer.Typer(no_args_is_help=True, help="Local-first personal health data platform.")
auth_app = typer.Typer(no_args_is_help=True, help="Authorize provider accounts.")
withings_app = typer.Typer(no_args_is_help=True, help="Manage Withings OAuth credentials.")
app.add_typer(auth_app, name="auth")
app.add_typer(withings_app, name="withings")
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
