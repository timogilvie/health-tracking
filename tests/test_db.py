from pathlib import Path

import duckdb
import pytest

from health.db import MigrationError, connect, migrate, migration_status

EXPECTED_TABLES = {
    "blood_pressure",
    "devices",
    "duplicate_links",
    "events",
    "ingestion_runs",
    "lab_results",
    "observations",
    "schema_version",
    "sleep_sessions",
    "sources",
    "workouts",
}
EXPECTED_INDEXES = {
    "blood_pressure_date_idx",
    "duplicate_links_canonical_idx",
    "events_type_time_idx",
    "ingestion_runs_source_time_idx",
    "lab_results_name_time_idx",
    "observations_metric_time_idx",
    "observations_source_time_idx",
    "sleep_sessions_date_idx",
    "workouts_date_type_idx",
}


def test_database_initializes_and_migrations_are_idempotent(
    tmp_path: Path, project_root: Path
) -> None:
    database = tmp_path / "health.duckdb"
    migrations = project_root / "sql"

    assert migrate(database, migrations) == [1, 2]
    assert migrate(database, migrations) == []

    with connect(database, read_only=True) as connection:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        indexes = {
            row[0]
            for row in connection.execute("SELECT index_name FROM duckdb_indexes()").fetchall()
        }
        pending, drift = migration_status(connection, migrations)
        versions = connection.execute(
            "SELECT version, name FROM schema_version ORDER BY version"
        ).fetchall()

    assert tables == EXPECTED_TABLES
    assert indexes == EXPECTED_INDEXES
    assert versions == [(1, "schema"), (2, "indexes")]
    assert pending == []
    assert drift == []


def test_migration_checksum_drift_is_rejected(tmp_path: Path) -> None:
    migrations = tmp_path / "sql"
    migrations.mkdir()
    migration = migrations / "001_initial.sql"
    migration.write_text("CREATE TABLE example (id INTEGER);\n", encoding="utf-8")
    database = tmp_path / "health.duckdb"
    migrate(database, migrations)
    migration.write_text("CREATE TABLE example (id BIGINT);\n", encoding="utf-8")

    with pytest.raises(MigrationError, match="has changed"):
        migrate(database, migrations)


def test_core_constraints_protect_identity_and_confidence(
    tmp_path: Path, project_root: Path
) -> None:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")

    with connect(database) as connection:
        source_id = connection.execute(
            "INSERT INTO sources (name, source_type) VALUES ('oura', 'api') RETURNING source_id"
        ).fetchone()[0]
        values = [
            "weight_kg",
            "2026-09-10T08:00:00Z",
            80.0,
            "kg",
            source_id,
            "record-1",
            "raw/oura/one.json",
            "v1",
        ]
        connection.execute(
            """
            INSERT INTO observations (
                metric, observed_at, value, unit, source_id, source_record_id,
                raw_file, transform_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        with pytest.raises(duckdb.ConstraintException):
            connection.execute(
                """
                INSERT INTO observations (
                    metric, observed_at, value, unit, source_id, source_record_id,
                    raw_file, transform_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
        with pytest.raises(duckdb.ConstraintException):
            connection.execute(
                """
                INSERT INTO duplicate_links (
                    record_type, canonical_record_id, duplicate_record_id,
                    match_method, confidence
                ) VALUES ('observation', uuid(), uuid(), 'heuristic', 1.2)
                """
            )
