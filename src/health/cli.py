"""Command-line interface for local health data operations."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer

from health.config import HealthSettings, load_project_config
from health.db import MigrationError, connect, migrate, migration_status
from health.layout import initialize_layout
from health.oss_policy import PolicyError, validate_repository_policy

app = typer.Typer(no_args_is_help=True, help="Local-first personal health data platform.")
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


if __name__ == "__main__":
    app()
