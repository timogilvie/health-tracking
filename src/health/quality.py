"""Read-only dataset coverage, freshness, and quality reporting."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import duckdb

PERIODIC_METRICS = {
    "blood_pressure",
    "body_fat_mass_kg",
    "body_fat_pct",
    "body_water_mass_kg",
    "bone_mass_kg",
    "lean_mass_kg",
    "skeletal_muscle_mass_kg",
    "weight_kg",
}


def _rows(connection: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = connection.execute(query)
    columns = [column[0] for column in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def _cadence(metric: str) -> str:
    if metric.startswith(("lab:", "event:", "workout:")):
        return "episodic"
    if metric in PERIODIC_METRICS:
        return "periodic"
    return "daily"


def _freshness_status(cadence: str, freshness_days: int) -> str:
    if cadence == "episodic":
        return "episodic"
    limit = 30 if cadence == "periodic" else 3
    return "current" if freshness_days <= limit else "stale"


def quality_report(
    connection: duckdb.DuckDBPyConnection,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a measurement-free audit of coverage, imports, and quality signals."""

    generated_at = now or datetime.now(UTC)
    today = generated_at.astimezone(UTC).date()
    coverage = _rows(
        connection,
        """
        WITH dated_records AS (
            SELECT o.metric, s.name AS source,
                   COALESCE(o.local_date, CAST(o.observed_at AS DATE)) AS observed_date
            FROM observations o
            JOIN sources s USING (source_id)
            WHERE s.enabled
            UNION ALL
            SELECT 'blood_pressure', s.name, b.local_date
            FROM blood_pressure b
            JOIN sources s USING (source_id)
            WHERE s.enabled
            UNION ALL
            SELECT 'sleep', s.name, sl.sleep_date
            FROM sleep_sessions sl
            JOIN sources s USING (source_id)
            WHERE s.enabled
            UNION ALL
            SELECT 'workout:' || w.workout_type, s.name, w.local_date
            FROM workouts w
            JOIN sources s USING (source_id)
            WHERE s.enabled
            UNION ALL
            SELECT 'lab:' || COALESCE(l.canonical_name, l.original_name), s.name,
                   CAST(COALESCE(l.collected_at, l.resulted_at, l.ingested_at) AS DATE)
            FROM lab_results l
            JOIN sources s USING (source_id)
            WHERE s.enabled
            UNION ALL
            SELECT 'event:' || e.event_type, s.name, e.local_date
            FROM events e
            JOIN sources s USING (source_id)
            WHERE s.enabled
        )
        SELECT metric, source, MIN(observed_date) AS first_date,
               MAX(observed_date) AS last_date,
               COUNT(*) AS record_count,
               COUNT(DISTINCT observed_date) AS observed_days,
               date_diff('day', MIN(observed_date), MAX(observed_date)) + 1 AS span_days
        FROM dated_records
        WHERE observed_date IS NOT NULL
        GROUP BY metric, source
        ORDER BY metric, source
        """,
    )
    for row in coverage:
        span_days = int(row["span_days"])
        observed_days = int(row["observed_days"])
        last_date = row["last_date"]
        freshness_days = max((today - last_date).days, 0)
        cadence = _cadence(str(row["metric"]))
        row.update(
            cadence=cadence,
            missing_days=max(span_days - observed_days, 0),
            coverage_pct=round(observed_days / span_days * 100, 1),
            freshness_days=freshness_days,
            freshness_status=_freshness_status(cadence, freshness_days),
        )

    source_runs = _rows(
        connection,
        """
        SELECT source, started_at, finished_at, raw_count, normalized_count,
               inserted_count, updated_count, duplicate_count,
               CASE
                   WHEN finished_at IS NULL THEN NULL
                   ELSE date_diff('millisecond', started_at, finished_at) / 1000.0
               END AS duration_seconds
        FROM (
            SELECT s.name AS source, r.started_at, r.finished_at, r.raw_count,
                   r.normalized_count, r.inserted_count, r.updated_count,
                   r.duplicate_count,
                   row_number() OVER (
                       PARTITION BY s.source_id
                       ORDER BY r.started_at DESC, r.ingestion_run_id DESC
                   ) AS rank
            FROM ingestion_runs r
            JOIN sources s USING (source_id)
            WHERE s.enabled AND r.status = 'succeeded'
        )
        WHERE rank = 1
        ORDER BY source
        """,
    )
    for row in source_runs:
        duration_seconds = row["duration_seconds"]
        row["throughput_per_second"] = (
            round(int(row["normalized_count"]) / float(duration_seconds), 1)
            if duration_seconds is not None and float(duration_seconds) > 0
            else None
        )
        finished_at = row["finished_at"]
        row["age_days"] = (
            max((generated_at.astimezone(UTC) - finished_at.astimezone(UTC)).days, 0)
            if finished_at is not None
            else None
        )

    quality_counts = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM observations WHERE quality = 'suspect')
                + (SELECT COUNT(*) FROM blood_pressure WHERE quality = 'suspect') AS suspect,
            (SELECT COUNT(*) FROM observations WHERE quality = 'invalid')
                + (SELECT COUNT(*) FROM blood_pressure WHERE quality = 'invalid') AS invalid,
            (SELECT COUNT(*) FROM duplicate_links WHERE resolution = 'candidate') AS candidates,
            (SELECT COUNT(*) FROM duplicate_links WHERE resolution = 'confirmed') AS confirmed,
            (SELECT COUNT(*) FROM duplicate_links WHERE resolution = 'rejected') AS rejected,
            (SELECT COUNT(*) FROM canonical_sleep_daily WHERE total_sleep_minutes > 24 * 60)
                AS sleep_over_24h,
            (SELECT COUNT(*) FROM canonical_sleep_daily WHERE total_sleep_minutes > 16 * 60)
                AS sleep_over_16h
        """
    ).fetchone()
    latest_run_counts = connection.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE status = 'failed') AS failed,
            COUNT(*) FILTER (WHERE status = 'running') AS running
        FROM (
            SELECT status,
                   row_number() OVER (
                       PARTITION BY source_id
                       ORDER BY started_at DESC, ingestion_run_id DESC
                   ) AS rank
            FROM ingestion_runs
            WHERE source_id IS NOT NULL
        )
        WHERE rank = 1
        """
    ).fetchone()
    diagnostics = {
        "suspect_records": int(quality_counts[0]),
        "invalid_records": int(quality_counts[1]),
        "duplicate_candidates": int(quality_counts[2]),
        "duplicate_confirmed": int(quality_counts[3]),
        "duplicate_rejected": int(quality_counts[4]),
        "sleep_over_24h": int(quality_counts[5]),
        "sleep_over_16h": int(quality_counts[6]),
        "latest_failed_imports": int(latest_run_counts[0]),
        "latest_running_imports": int(latest_run_counts[1]),
    }

    attention = (
        diagnostics["invalid_records"]
        + diagnostics["duplicate_candidates"]
        + diagnostics["sleep_over_24h"]
        + diagnostics["latest_failed_imports"]
    )
    review = (
        diagnostics["suspect_records"]
        + diagnostics["sleep_over_16h"]
        + diagnostics["latest_running_imports"]
    )
    if not coverage and not source_runs:
        status = "empty"
    elif attention:
        status = "attention"
    elif review:
        status = "review"
    else:
        status = "clear"

    return {
        "generated_at": generated_at,
        "status": status,
        "metric_source_count": len(coverage),
        "source_import_count": len(source_runs),
        "stale_metric_sources": sum(row["freshness_status"] == "stale" for row in coverage),
        "coverage": coverage,
        "source_runs": source_runs,
        "diagnostics": diagnostics,
    }
