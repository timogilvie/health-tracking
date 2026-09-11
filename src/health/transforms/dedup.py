"""Deterministic cross-source duplicate linking without deleting source records."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import duckdb

from health.db import connect

DEDUPE_VERSION = "health.dedup.v1"
AUTO_CONFIRM_THRESHOLD = 0.9
Resolution = Literal["candidate", "confirmed", "rejected"]


class DuplicateResolutionError(ValueError):
    """A duplicate link or requested resolution is invalid."""


@dataclass(frozen=True, slots=True)
class DuplicateReport:
    matched: int
    confirmed: int
    candidates: int
    removed_stale: int


@dataclass(frozen=True, slots=True)
class _Match:
    record_type: str
    data_type: str
    left_id: UUID
    left_source: str
    right_id: UUID
    right_source: str
    method: str
    confidence: float
    facts: dict[str, Any]


def _metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return value if isinstance(value, dict) else {}


def _normalized_source(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.casefold()).replace("healthmate", "withings")


def _apple_provenance_matches(
    left_source: str,
    left_metadata: Any,
    right_source: str,
    right_metadata: Any,
) -> bool:
    for apple_source, apple_metadata, direct_source in (
        (left_source, left_metadata, right_source),
        (right_source, right_metadata, left_source),
    ):
        if apple_source != "apple_health":
            continue
        original = (
            _metadata(apple_metadata).get("apple_health", {}).get("source_name") or ""
        )
        direct = _normalized_source(direct_source)
        normalized_original = _normalized_source(str(original))
        if direct and normalized_original and (
            direct in normalized_original or normalized_original in direct
        ):
            return True
    return False


def _seconds(left: datetime, right: datetime) -> float:
    return abs((left - right).total_seconds())


def _observation_matches(connection: duckdb.DuckDBPyConnection) -> list[_Match]:
    rows = connection.execute(
        """
        SELECT a.observation_id, sa.name, a.metric, a.observed_at, a.value, a.metadata,
               b.observation_id, sb.name, b.observed_at, b.value, b.metadata
        FROM observations a
        JOIN sources sa ON sa.source_id = a.source_id
        JOIN observations b
          ON a.metric = b.metric
         AND a.source_id <> b.source_id
         AND CAST(a.observation_id AS VARCHAR) < CAST(b.observation_id AS VARCHAR)
         AND abs(epoch(a.observed_at) - epoch(b.observed_at)) <= 600
        JOIN sources sb ON sb.source_id = b.source_id
        WHERE a.quality <> 'invalid' AND b.quality <> 'invalid'
        """
    ).fetchall()
    matches: list[_Match] = []
    for row in rows:
        left_id, left_source, metric, left_at, left_value, left_meta = row[:6]
        right_id, right_source, right_at, right_value, right_meta = row[6:]
        difference = abs(float(left_value) - float(right_value))
        scale = max(abs(float(left_value)), abs(float(right_value)), 1.0)
        relative_difference = difference / scale
        time_difference = _seconds(left_at, right_at)
        provenance = _apple_provenance_matches(
            left_source, left_meta, right_source, right_meta
        )
        if provenance and time_difference <= 60 and relative_difference <= 0.01:
            method, confidence = "apple_provenance", 0.99
        elif time_difference <= 1 and difference <= 1e-6:
            method, confidence = "exact", 0.98
        elif relative_difference <= 0.05:
            method = "heuristic"
            confidence = max(0.7, 0.9 - time_difference / 6000 - relative_difference * 2)
        else:
            continue
        matches.append(
            _Match(
                "observation",
                metric,
                left_id,
                left_source,
                right_id,
                right_source,
                method,
                confidence,
                {
                    "seconds_apart": time_difference,
                    "absolute_value_difference": difference,
                    "relative_value_difference": relative_difference,
                },
            )
        )
    return matches


def _blood_pressure_matches(connection: duckdb.DuckDBPyConnection) -> list[_Match]:
    rows = connection.execute(
        """
        SELECT a.blood_pressure_id, sa.name, a.measured_at, a.systolic_mmhg,
               a.diastolic_mmhg, a.metadata, b.blood_pressure_id, sb.name,
               b.measured_at, b.systolic_mmhg, b.diastolic_mmhg, b.metadata
        FROM blood_pressure a
        JOIN sources sa ON sa.source_id = a.source_id
        JOIN blood_pressure b
          ON a.source_id <> b.source_id
         AND CAST(a.blood_pressure_id AS VARCHAR) < CAST(b.blood_pressure_id AS VARCHAR)
         AND abs(epoch(a.measured_at) - epoch(b.measured_at)) <= 300
        JOIN sources sb ON sb.source_id = b.source_id
        WHERE a.quality <> 'invalid' AND b.quality <> 'invalid'
        """
    ).fetchall()
    matches: list[_Match] = []
    for row in rows:
        left_id, left_source, left_at, left_sys, left_dia, left_meta = row[:6]
        right_id, right_source, right_at, right_sys, right_dia, right_meta = row[6:]
        time_difference = _seconds(left_at, right_at)
        systolic_difference = abs(float(left_sys) - float(right_sys))
        diastolic_difference = abs(float(left_dia) - float(right_dia))
        provenance = _apple_provenance_matches(
            left_source, left_meta, right_source, right_meta
        )
        if provenance and systolic_difference <= 1 and diastolic_difference <= 1:
            method, confidence = "apple_provenance", 0.99
        elif time_difference <= 1 and systolic_difference == 0 and diastolic_difference == 0:
            method, confidence = "exact", 0.98
        elif systolic_difference <= 5 and diastolic_difference <= 5:
            method, confidence = "heuristic", 0.82
        else:
            continue
        matches.append(
            _Match(
                "blood_pressure",
                "blood_pressure",
                left_id,
                left_source,
                right_id,
                right_source,
                method,
                confidence,
                {
                    "seconds_apart": time_difference,
                    "systolic_difference": systolic_difference,
                    "diastolic_difference": diastolic_difference,
                },
            )
        )
    return matches


def _interval_matches(
    connection: duckdb.DuckDBPyConnection,
    *,
    table: str,
    id_column: str,
    record_type: str,
    data_type: str,
    type_column: str | None = None,
) -> list[_Match]:
    type_select = f", a.{type_column}, b.{type_column}" if type_column else ""
    type_join = f"AND a.{type_column} = b.{type_column}" if type_column else ""
    rows = connection.execute(
        f"""
        SELECT a.{id_column}, sa.name, a.started_at, a.ended_at, a.metadata,
               b.{id_column}, sb.name, b.started_at, b.ended_at, b.metadata
               {type_select}
        FROM {table} a
        JOIN sources sa ON sa.source_id = a.source_id
        JOIN {table} b
          ON a.source_id <> b.source_id
         AND CAST(a.{id_column} AS VARCHAR) < CAST(b.{id_column} AS VARCHAR)
         AND a.started_at < b.ended_at
         AND b.started_at < a.ended_at
         {type_join}
        JOIN sources sb ON sb.source_id = b.source_id
        """
    ).fetchall()
    matches: list[_Match] = []
    for row in rows:
        left_id, left_source, left_start, left_end, left_meta = row[:5]
        right_id, right_source, right_start, right_end, right_meta = row[5:10]
        subtype = row[10] if type_column else None
        left_seconds = max((left_end - left_start).total_seconds(), 1)
        right_seconds = max((right_end - right_start).total_seconds(), 1)
        overlap = max(
            0.0,
            (min(left_end, right_end) - max(left_start, right_start)).total_seconds(),
        )
        overlap_ratio = overlap / min(left_seconds, right_seconds)
        duration_ratio = min(left_seconds, right_seconds) / max(left_seconds, right_seconds)
        provenance = _apple_provenance_matches(
            left_source, left_meta, right_source, right_meta
        )
        exact_bounds = _seconds(left_start, right_start) <= 1 and _seconds(
            left_end, right_end
        ) <= 1
        if provenance and overlap_ratio >= 0.9:
            method, confidence = "apple_provenance", 0.99
        elif exact_bounds:
            method, confidence = "exact", 0.98
        elif overlap_ratio >= 0.5:
            method = "workout_overlap" if record_type == "workout" else "interval_overlap"
            confidence = min(0.94, 0.65 + 0.2 * overlap_ratio + 0.1 * duration_ratio)
        else:
            continue
        facts: dict[str, Any] = {
            "overlap_ratio": overlap_ratio,
            "duration_ratio": duration_ratio,
        }
        if subtype is not None:
            facts["subtype"] = subtype
        matches.append(
            _Match(
                record_type,
                data_type,
                left_id,
                left_source,
                right_id,
                right_source,
                method,
                confidence,
                facts,
            )
        )
    return matches


def _priorities(connection: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], int]:
    return {
        (str(data_type), str(source)): int(priority)
        for data_type, source, priority in connection.execute(
            "SELECT data_type, source, priority FROM source_priorities"
        ).fetchall()
    }


def _canonical_pair(
    match: _Match, priorities: dict[tuple[str, str], int]
) -> tuple[UUID, UUID]:
    left_rank = (priorities.get((match.data_type, match.left_source), 10_000), match.left_source)
    right_rank = (
        priorities.get((match.data_type, match.right_source), 10_000),
        match.right_source,
    )
    if left_rank < right_rank or (
        left_rank == right_rank and str(match.left_id) < str(match.right_id)
    ):
        return match.left_id, match.right_id
    return match.right_id, match.left_id


def reconcile_duplicates(database: Path) -> DuplicateReport:
    """Rebuild generated links while preserving explicit rejected review decisions."""

    with connect(database) as connection:
        matches = [
            *_observation_matches(connection),
            *_blood_pressure_matches(connection),
            *_interval_matches(
                connection,
                table="sleep_sessions",
                id_column="sleep_session_id",
                record_type="sleep_session",
                data_type="sleep",
            ),
            *_interval_matches(
                connection,
                table="workouts",
                id_column="workout_id",
                record_type="workout",
                data_type="workouts",
                type_column="workout_type",
            ),
        ]
        priorities = _priorities(connection)
        live_keys: set[tuple[str, UUID, UUID, str]] = set()
        confirmed = 0
        candidates = 0
        connection.execute("BEGIN TRANSACTION")
        try:
            for match in matches:
                canonical_id, duplicate_id = _canonical_pair(match, priorities)
                resolution = (
                    "confirmed"
                    if match.confidence >= AUTO_CONFIRM_THRESHOLD
                    else "candidate"
                )
                confirmed += resolution == "confirmed"
                candidates += resolution == "candidate"
                key = (match.record_type, canonical_id, duplicate_id, match.method)
                live_keys.add(key)
                metadata = json.dumps(
                    {"generated_by": DEDUPE_VERSION, "facts": match.facts}, sort_keys=True
                )
                connection.execute(
                    """
                    INSERT INTO duplicate_links (
                        record_type, canonical_record_id, duplicate_record_id,
                        match_method, confidence, resolution, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (
                        record_type, canonical_record_id, duplicate_record_id, match_method
                    ) DO UPDATE SET
                        confidence = excluded.confidence,
                        resolution = CASE
                            WHEN duplicate_links.resolution = 'rejected' THEN 'rejected'
                            ELSE excluded.resolution
                        END,
                        metadata = excluded.metadata
                    """,
                    [*key, match.confidence, resolution, metadata],
                )
            generated = connection.execute(
                """
                SELECT record_type, canonical_record_id, duplicate_record_id, match_method
                FROM duplicate_links
                WHERE json_extract_string(metadata, '$.generated_by') = ?
                  AND resolution <> 'rejected'
                """,
                [DEDUPE_VERSION],
            ).fetchall()
            stale = [tuple(row) for row in generated if tuple(row) not in live_keys]
            for key in stale:
                connection.execute(
                    """
                    DELETE FROM duplicate_links
                    WHERE record_type = ? AND canonical_record_id = ?
                      AND duplicate_record_id = ? AND match_method = ?
                    """,
                    list(key),
                )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    return DuplicateReport(len(matches), confirmed, candidates, len(stale))


def list_duplicate_candidates(database: Path) -> list[dict[str, Any]]:
    """Return unresolved candidate links without exposing measurement values."""

    with connect(database, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT duplicate_link_id, record_type, canonical_record_id,
                   duplicate_record_id, match_method, confidence, created_at
            FROM duplicate_links
            WHERE resolution = 'candidate'
            ORDER BY confidence DESC, created_at, duplicate_link_id
            """
        ).fetchall()
    return [
        {
            "id": str(row[0]),
            "record_type": row[1],
            "canonical_record_id": str(row[2]),
            "duplicate_record_id": str(row[3]),
            "method": row[4],
            "confidence": float(row[5]),
            "created_at": row[6],
        }
        for row in rows
    ]


def resolve_duplicate(database: Path, link_id: UUID | str, resolution: Resolution) -> None:
    """Record an explicit review decision for a candidate duplicate link."""

    if resolution not in {"confirmed", "rejected"}:
        raise DuplicateResolutionError("resolution must be confirmed or rejected")
    with connect(database) as connection:
        result = connection.execute(
            """
            UPDATE duplicate_links SET resolution = ? WHERE duplicate_link_id = ?
            RETURNING duplicate_link_id
            """,
            [resolution, link_id],
        ).fetchone()
    if result is None:
        raise DuplicateResolutionError(f"duplicate link does not exist: {link_id}")
