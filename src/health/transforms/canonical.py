"""Canonical source-priority configuration for derived database views."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from health.db import connect


def source_priority_rows(project_config: dict[str, dict[str, Any]]) -> list[tuple[str, str, int]]:
    """Validate YAML source priorities and return database rows."""

    configured = project_config.get("source_priority", {}).get("metrics")
    if not isinstance(configured, dict):
        raise ValueError("source_priority.metrics must be a mapping")
    rows: list[tuple[str, str, int]] = []
    for data_type, sources in configured.items():
        if not isinstance(data_type, str) or not data_type.strip():
            raise ValueError("source-priority keys must be non-empty strings")
        if not isinstance(sources, list) or not sources:
            raise ValueError(f"source priority for {data_type!r} must be a non-empty list")
        if any(not isinstance(source, str) or not source.strip() for source in sources):
            raise ValueError(f"source priority for {data_type!r} contains an invalid source")
        normalized = [source.strip() for source in sources]
        if len(normalized) != len(set(normalized)):
            raise ValueError(f"source priority for {data_type!r} contains duplicates")
        rows.extend(
            (data_type.strip(), source, priority)
            for priority, source in enumerate(normalized, start=1)
        )
    return rows


def sync_source_priorities(
    database: Path,
    project_config: dict[str, dict[str, Any]],
) -> int:
    """Atomically replace database priorities with the checked YAML configuration."""

    rows = source_priority_rows(project_config)
    with connect(database) as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute("DELETE FROM source_priorities")
            if rows:
                connection.executemany(
                    "INSERT INTO source_priorities (data_type, source, priority) VALUES (?, ?, ?)",
                    rows,
                )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    return len(rows)
