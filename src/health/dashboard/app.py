"""Private localhost dashboard server and DuckDB read model."""

from __future__ import annotations

import json
import threading
import webbrowser
from datetime import date, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from health.db import connect
from health.quality import quality_report

ASSET_ROOT = Path(__file__).parent.parent / "dashboard_assets"
ALLOWED_RANGE_DAYS = {7, 30, 90, 365}
KG_TO_LB = 2.2046226218487757
DAILY_COLUMNS = (
    "local_date",
    "weight_kg",
    "body_fat_pct",
    "systolic_mmhg",
    "diastolic_mmhg",
    "resting_hr_bpm",
    "hrv_rmssd_ms",
    "total_sleep_minutes",
    "sleep_efficiency_pct",
    "steps",
    "active_energy_kcal",
    "resistance_minutes",
    "cardio_minutes",
    "workout_count",
)


def _rows(connection, query: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
    result = connection.execute(query, parameters or [])
    names = [column[0] for column in result.description]
    return [dict(zip(names, row, strict=True)) for row in result.fetchall()]


def _json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    return value


def _convert_weight_units(
    daily: list[dict[str, Any]],
    weekly: list[dict[str, Any]],
    rolling: list[dict[str, Any]],
) -> None:
    """Convert canonical kilograms to display-only pounds in the browser read model."""

    for rows, source, target in (
        (daily, "weight_kg", "weight_lb"),
        (weekly, "weight_kg_avg", "weight_lb_avg"),
        (rolling, "weight_kg_avg", "weight_lb_avg"),
    ):
        for row in rows:
            value = row.pop(source, None)
            row[target] = float(value) * KG_TO_LB if value is not None else None


def _latest_date(connection) -> date | None:
    row = connection.execute(
        """
        SELECT MAX(last_date)
        FROM (
            SELECT MAX(COALESCE(o.local_date, CAST(o.observed_at AS DATE))) AS last_date
            FROM observations o JOIN sources s USING (source_id) WHERE s.enabled
            UNION ALL
            SELECT MAX(b.local_date) FROM blood_pressure b
            JOIN sources s USING (source_id) WHERE s.enabled
            UNION ALL
            SELECT MAX(sl.sleep_date) FROM sleep_sessions sl
            JOIN sources s USING (source_id) WHERE s.enabled
            UNION ALL
            SELECT MAX(w.local_date) FROM workouts w
            JOIN sources s USING (source_id) WHERE s.enabled
            UNION ALL
            SELECT MAX(e.local_date) FROM events e
            JOIN sources s USING (source_id) WHERE s.enabled
        )
        """
    ).fetchone()
    return row[0] if row else None


def _average(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return sum(values) / len(values) if values else None


def _sum(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return sum(values) if values else None


def _aggregate_period(rows: list[dict[str, Any]]) -> dict[str, Any]:
    exercise = [
        float(row.get("resistance_minutes") or 0) + float(row.get("cardio_minutes") or 0)
        for row in rows
        if row.get("resistance_minutes") is not None or row.get("cardio_minutes") is not None
    ]
    return {
        "calendar_days": len(rows),
        "weight_kg_avg": _average(rows, "weight_kg"),
        "weight_days": sum(row.get("weight_kg") is not None for row in rows),
        "systolic_mmhg_avg": _average(rows, "systolic_mmhg"),
        "diastolic_mmhg_avg": _average(rows, "diastolic_mmhg"),
        "blood_pressure_days": sum(row.get("systolic_mmhg") is not None for row in rows),
        "hrv_rmssd_ms_avg": _average(rows, "hrv_rmssd_ms"),
        "hrv_days": sum(row.get("hrv_rmssd_ms") is not None for row in rows),
        "resting_hr_bpm_avg": _average(rows, "resting_hr_bpm"),
        "resting_hr_days": sum(row.get("resting_hr_bpm") is not None for row in rows),
        "total_sleep_minutes_avg": _average(rows, "total_sleep_minutes"),
        "sleep_days": sum(row.get("total_sleep_minutes") is not None for row in rows),
        "steps_total": _sum(rows, "steps"),
        "steps_daily_avg": _average(rows, "steps"),
        "steps_days": sum(row.get("steps") is not None for row in rows),
        "resistance_minutes_total": _sum(rows, "resistance_minutes"),
        "cardio_minutes_total": _sum(rows, "cardio_minutes"),
        "exercise_minutes_total": sum(exercise) if exercise else None,
        "workout_count": _sum(rows, "workout_count"),
        "exercise_days": len(exercise),
    }


def _weekly_rows(rows: list[dict[str, Any]], latest_date: date) -> list[dict[str, Any]]:
    groups: dict[date, list[dict[str, Any]]] = {}
    for row in rows:
        local_date = row["local_date"]
        week_start = local_date - timedelta(days=local_date.weekday())
        groups.setdefault(week_start, []).append(row)
    threshold = latest_date - timedelta(days=371)
    return [
        {
            "week_start": week_start,
            "week_end": week_start + timedelta(days=6),
            **_aggregate_period(group),
        }
        for week_start, group in sorted(groups.items())
        if week_start >= threshold
    ]


def _rolling_rows(rows: list[dict[str, Any]], latest_date: date) -> list[dict[str, Any]]:
    rolling = []
    for window_days in sorted(ALLOWED_RANGE_DAYS):
        window_start = latest_date - timedelta(days=window_days - 1)
        window = [row for row in rows if row["local_date"] >= window_start]
        rolling.append(
            {
                "anchor_date": latest_date,
                "window_days": window_days,
                "window_start": window_start,
                "window_end": latest_date,
                **_aggregate_period(window),
            }
        )
    return rolling


def dashboard_summary_payload(database: Path) -> dict[str, Any]:
    """Build the small, first-paint dashboard payload."""

    summary = {
        "weight": None,
        "systolic": None,
        "diastolic": None,
        "resting_hr": None,
        "hrv": None,
    }
    with connect(database, read_only=True) as connection:
        latest_date = _latest_date(connection)
        observations = _rows(
            connection,
            """
            SELECT metric, value, canonical_date
            FROM (
                SELECT metric, value, canonical_date, observed_at,
                       row_number() OVER (
                           PARTITION BY metric
                           ORDER BY canonical_date DESC, observed_at DESC, observation_id DESC
                       ) AS rank
                FROM canonical_observations
                WHERE metric IN ('weight_kg', 'resting_hr_bpm', 'hrv_rmssd_ms')
            )
            WHERE rank = 1
            """,
        )
        for row in observations:
            metric = row["metric"]
            name, unit, multiplier = {
                "weight_kg": ("weight", "lb", KG_TO_LB),
                "resting_hr_bpm": ("resting_hr", "bpm", 1),
                "hrv_rmssd_ms": ("hrv", "ms", 1),
            }[metric]
            summary[name] = {
                "value": float(row["value"]) * multiplier,
                "unit": unit,
                "date": row["canonical_date"],
            }
        pressure = connection.execute(
            """
            SELECT local_date, preferred_systolic_mmhg, preferred_diastolic_mmhg
            FROM blood_pressure_sessions
            ORDER BY local_date DESC, session_ended_at DESC, session_id DESC
            LIMIT 1
            """
        ).fetchone()
        if pressure:
            summary["systolic"] = {"value": pressure[1], "unit": "mmHg", "date": pressure[0]}
            summary["diastolic"] = {"value": pressure[2], "unit": "mmHg", "date": pressure[0]}
        sleep = connection.execute(
            """
            SELECT local_date, total_sleep_minutes
            FROM canonical_sleep_daily
            WHERE total_sleep_minutes IS NOT NULL
            ORDER BY local_date DESC
            LIMIT 1
            """
        ).fetchone()
        if sleep:
            summary["sleep"] = {"value": sleep[1], "unit": "min", "date": sleep[0]}
        else:
            summary["sleep"] = None
    return _json_value(
        {
            "generated_at": datetime.now().astimezone(),
            "latest_date": latest_date,
            "summary": summary,
        }
    )


def dashboard_trends_payload(database: Path, *, days: int = 365) -> dict[str, Any]:
    """Build deferred chart and lab data from one canonical daily scan."""

    if days not in ALLOWED_RANGE_DAYS:
        raise ValueError("dashboard range must be 7, 30, 90, or 365 days")
    columns = ", ".join(DAILY_COLUMNS)
    with connect(database, read_only=True) as connection:
        latest_date = _latest_date(connection)
        if latest_date is None:
            daily: list[dict[str, Any]] = []
            weekly: list[dict[str, Any]] = []
            rolling: list[dict[str, Any]] = []
        else:
            analysis_rows = _rows(
                connection,
                f"""
                SELECT {columns}
                FROM health_calendar
                WHERE local_date BETWEEN ? - 377 AND ?
                ORDER BY local_date
                """,
                [latest_date, latest_date],
            )
            cutoff = latest_date - timedelta(days=days - 1)
            daily = [row for row in analysis_rows if row["local_date"] >= cutoff]
            weekly = _weekly_rows(analysis_rows, latest_date)
            rolling = _rolling_rows(analysis_rows, latest_date)
        labs = _rows(
            connection,
            """
            SELECT COALESCE(canonical_name, original_name) AS name,
                   numeric_value, text_value, unit, reference_low, reference_high,
                   reference_text, abnormal_flag,
                   COALESCE(collected_at, resulted_at) AS observed_at, provider,
                   s.name AS source
            FROM lab_results l
            JOIN sources s USING (source_id)
            WHERE s.enabled
            ORDER BY COALESCE(collected_at, resulted_at) DESC NULLS LAST,
                     l.ingested_at DESC
            LIMIT 50
            """,
        )
    _convert_weight_units(daily, weekly, rolling)
    return _json_value(
        {
            "generated_at": datetime.now().astimezone(),
            "latest_date": latest_date,
            "range_days": days,
            "daily": daily,
            "weekly": weekly,
            "rolling": rolling,
            "labs": labs,
        }
    )


def dashboard_quality_payload(database: Path) -> dict[str, Any]:
    """Build the deferred full-ledger audit payload."""

    with connect(database, read_only=True) as connection:
        quality = quality_report(connection)
    return _json_value({"generated_at": datetime.now().astimezone(), "quality": quality})


def dashboard_payload(database: Path, *, days: int = 365) -> dict[str, Any]:
    """Build the combined read model retained for local API compatibility."""

    summary = dashboard_summary_payload(database)
    trends = dashboard_trends_payload(database, days=days)
    quality = dashboard_quality_payload(database)
    return {**trends, **summary, **quality}


class _DashboardHandler(BaseHTTPRequestHandler):
    database: Path

    def log_message(self, _format: str, *_args: object) -> None:
        return None

    def _send(
        self,
        content: bytes,
        *,
        content_type: str,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; script-src 'self'; style-src 'self'; "
            "connect-src 'self'; img-src 'self' data:; base-uri 'none'; "
            "form-action 'none'; frame-ancestors 'none'",
        )
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/":
            self._send(
                (ASSET_ROOT / "index.html").read_bytes(),
                content_type="text/html; charset=utf-8",
            )
            return
        assets = {
            "/assets/styles.css": ("styles.css", "text/css; charset=utf-8"),
            "/assets/app.js": ("app.js", "text/javascript; charset=utf-8"),
        }
        if path in assets:
            filename, content_type = assets[path]
            self._send((ASSET_ROOT / filename).read_bytes(), content_type=content_type)
            return
        payloads = {
            "/api/dashboard": lambda: dashboard_payload(self.database),
            "/api/dashboard/summary": lambda: dashboard_summary_payload(self.database),
            "/api/dashboard/trends": lambda: dashboard_trends_payload(self.database),
            "/api/dashboard/quality": lambda: dashboard_quality_payload(self.database),
        }
        if path in payloads:
            try:
                content = json.dumps(
                    payloads[path](),
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode()
            except Exception:
                self._send(
                    b'{"error":"dashboard query failed"}',
                    content_type="application/json",
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
                return
            self._send(content, content_type="application/json")
            return
        if path == "/favicon.ico":
            self._send(b"", content_type="image/x-icon", status=HTTPStatus.NO_CONTENT)
            return
        self._send(
            b"Not found\n",
            content_type="text/plain; charset=utf-8",
            status=HTTPStatus.NOT_FOUND,
        )


def create_dashboard_server(database: Path, *, port: int = 8766) -> ThreadingHTTPServer:
    """Create a loopback-only server; a separate constructor keeps tests non-blocking."""

    if not 0 <= port <= 65_535:
        raise ValueError("dashboard port must be between 0 and 65535")

    class Handler(_DashboardHandler):
        pass

    Handler.database = database
    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def serve_dashboard(database: Path, *, port: int = 8766, open_browser: bool = True) -> None:
    """Serve until interrupted, optionally opening the private loopback URL."""

    server = create_dashboard_server(database, port=port)
    actual_port = server.server_address[1]
    url = f"http://127.0.0.1:{actual_port}"
    if open_browser:
        threading.Timer(0.2, lambda: webbrowser.open(url)).start()
    print(f"Dashboard available at {url} (press Ctrl-C to stop)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
