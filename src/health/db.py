"""DuckDB connection and forward-only SQL migration support."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb

MIGRATION_PATTERN = re.compile(r"^(?P<version>\d{3})_(?P<name>[a-z0-9_]+)\.sql$")


class MigrationError(RuntimeError):
    """Raised when migration history is invalid or cannot be applied safely."""


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    path: Path
    checksum: str

    @property
    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")


def discover_migrations(directory: Path) -> list[Migration]:
    """Return validated migrations in version order."""

    if not directory.is_dir():
        raise MigrationError(f"migration directory does not exist: {directory}")

    migrations: list[Migration] = []
    for path in sorted(directory.glob("*.sql")):
        match = MIGRATION_PATTERN.fullmatch(path.name)
        if not match:
            raise MigrationError(f"invalid migration filename: {path.name}")
        raw = path.read_bytes()
        migrations.append(
            Migration(
                version=int(match.group("version")),
                name=match.group("name"),
                path=path,
                checksum=hashlib.sha256(raw).hexdigest(),
            )
        )

    versions = [migration.version for migration in migrations]
    if len(versions) != len(set(versions)):
        raise MigrationError("migration versions must be unique")
    return migrations


@contextmanager
def connect(path: Path, *, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open and always close a DuckDB connection."""

    connection = duckdb.connect(str(path), read_only=read_only)
    try:
        yield connection
    finally:
        connection.close()


def _ensure_version_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            checksum VARCHAR NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
        )
        """
    )


def _applied(connection: duckdb.DuckDBPyConnection) -> dict[int, tuple[str, str]]:
    rows = connection.execute(
        "SELECT version, name, checksum FROM schema_version ORDER BY version"
    ).fetchall()
    return {int(version): (str(name), str(checksum)) for version, name, checksum in rows}


def migration_status(
    connection: duckdb.DuckDBPyConnection, directory: Path
) -> tuple[list[int], list[int]]:
    """Return pending versions and versions whose checked-in SQL has drifted."""

    migrations = discover_migrations(directory)
    try:
        applied = _applied(connection)
    except duckdb.CatalogException:
        return [migration.version for migration in migrations], []

    by_version = {migration.version: migration for migration in migrations}
    pending = [migration.version for migration in migrations if migration.version not in applied]
    drift = [
        version
        for version, (name, checksum) in applied.items()
        if version not in by_version
        or by_version[version].name != name
        or by_version[version].checksum != checksum
    ]
    return pending, drift


def migrate(database: Path, directory: Path) -> list[int]:
    """Apply pending migrations atomically, refusing changed migration history."""

    database.parent.mkdir(parents=True, exist_ok=True)
    migrations = discover_migrations(directory)
    applied_now: list[int] = []

    with connect(database) as connection:
        _ensure_version_table(connection)
        applied = _applied(connection)
        available = {migration.version for migration in migrations}
        unknown = sorted(set(applied) - available)
        if unknown:
            raise MigrationError(f"database contains unknown migration version(s): {unknown}")

        for migration in migrations:
            previous = applied.get(migration.version)
            if previous is not None:
                if previous != (migration.name, migration.checksum):
                    raise MigrationError(
                        f"migration {migration.version:03d}_{migration.name} has changed"
                    )
                continue

            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute(migration.sql)
                connection.execute(
                    "INSERT INTO schema_version (version, name, checksum) VALUES (?, ?, ?)",
                    [migration.version, migration.name, migration.checksum],
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            applied_now.append(migration.version)

    return applied_now
