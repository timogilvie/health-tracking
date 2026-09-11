from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from health.db import connect, migrate
from health.transforms import (
    list_duplicate_candidates,
    reconcile_duplicates,
    resolve_duplicate,
)

NOW = datetime(2026, 9, 10, 12, tzinfo=UTC)


def _source(connection, name: str, source_type: str = "api"):
    return connection.execute(
        "INSERT INTO sources (name, source_type) VALUES (?, ?) RETURNING source_id",
        [name, source_type],
    ).fetchone()[0]


def _observation(
    connection,
    source_id,
    record_id: str,
    *,
    value: float,
    observed_at: datetime = NOW,
    metadata: dict | None = None,
):
    return connection.execute(
        """
        INSERT INTO observations (
            metric, observed_at, value, unit, source_id, source_record_id,
            raw_file, transform_version, metadata
        ) VALUES ('weight_kg', ?, ?, 'kg', ?, ?, 'fixture', 'test-v1', ?)
        RETURNING observation_id
        """,
        [observed_at, value, source_id, record_id, json.dumps(metadata or {})],
    ).fetchone()[0]


def _database(tmp_path: Path, project_root: Path) -> Path:
    database = tmp_path / "health.duckdb"
    migrate(database, project_root / "sql")
    return database


def test_apple_provenance_confirms_direct_vendor_as_canonical(
    tmp_path: Path, project_root: Path
) -> None:
    database = _database(tmp_path, project_root)
    with connect(database) as connection:
        withings = _source(connection, "withings")
        apple = _source(connection, "apple_health", "import")
        connection.execute(
            "INSERT INTO source_priorities VALUES ('weight_kg', 'withings', 1), "
            "('weight_kg', 'apple_health', 2)"
        )
        direct_id = _observation(connection, withings, "direct", value=80.0)
        apple_id = _observation(
            connection,
            apple,
            "apple-copy",
            value=80.0,
            metadata={"apple_health": {"source_name": "Withings"}},
        )

    report = reconcile_duplicates(database)

    assert (report.matched, report.confirmed, report.candidates) == (1, 1, 0)
    with connect(database, read_only=True) as connection:
        link = connection.execute(
            """
            SELECT canonical_record_id, duplicate_record_id, match_method, resolution
            FROM duplicate_links
            """
        ).fetchone()
        assert connection.execute("SELECT count(*) FROM observations").fetchone()[0] == 2
    assert link == (direct_id, apple_id, "apple_provenance", "confirmed")


def test_heuristic_candidate_can_be_rejected_and_replay_preserves_decision(
    tmp_path: Path, project_root: Path
) -> None:
    database = _database(tmp_path, project_root)
    with connect(database) as connection:
        left = _source(connection, "sensor_a")
        right = _source(connection, "sensor_b")
        _observation(connection, left, "left", value=80.0)
        _observation(
            connection,
            right,
            "right",
            value=81.6,
            observed_at=NOW + timedelta(minutes=2),
        )

    first = reconcile_duplicates(database)
    candidate = list_duplicate_candidates(database)[0]
    resolve_duplicate(database, candidate["id"], "rejected")
    second = reconcile_duplicates(database)

    assert first.candidates == 1
    assert second.matched == 1
    assert list_duplicate_candidates(database) == []
    with connect(database, read_only=True) as connection:
        assert connection.execute("SELECT resolution FROM duplicate_links").fetchone()[0] == (
            "rejected"
        )


def test_manual_workout_wins_over_overlapping_apple_copy(
    tmp_path: Path, project_root: Path
) -> None:
    database = _database(tmp_path, project_root)
    with connect(database) as connection:
        manual = _source(connection, "manual", "manual")
        apple = _source(connection, "apple_health", "import")
        connection.execute(
            "INSERT INTO source_priorities VALUES ('workouts', 'manual', 1), "
            "('workouts', 'apple_health', 2)"
        )
        ids = []
        for source, record_id, start, end in (
            (manual, "manual-workout", NOW, NOW + timedelta(minutes=55)),
            (
                apple,
                "apple-workout",
                NOW + timedelta(minutes=2),
                NOW + timedelta(minutes=54),
            ),
        ):
            ids.append(
                connection.execute(
                    """
                    INSERT INTO workouts (
                        started_at, ended_at, local_date, workout_type, duration_seconds,
                        source_id, source_record_id, raw_file, transform_version
                    ) VALUES (?, ?, '2026-09-10', 'resistance', ?, ?, ?, 'fixture', 'test-v1')
                    RETURNING workout_id
                    """,
                    [start, end, int((end - start).total_seconds()), source, record_id],
                ).fetchone()[0]
            )

    report = reconcile_duplicates(database)

    assert report.confirmed == 1
    with connect(database, read_only=True) as connection:
        link = connection.execute(
            "SELECT canonical_record_id, duplicate_record_id, match_method FROM duplicate_links"
        ).fetchone()
    assert link == (ids[0], ids[1], "workout_overlap")
