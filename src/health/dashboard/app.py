"""Private localhost dashboard server and DuckDB read model."""

from __future__ import annotations

import json
import threading
import webbrowser
from datetime import date, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from health.db import connect

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


def _latest_summary(daily: list[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
    definitions = {
        "weight": ("weight_lb", "lb"),
        "systolic": ("systolic_mmhg", "mmHg"),
        "diastolic": ("diastolic_mmhg", "mmHg"),
        "resting_hr": ("resting_hr_bpm", "bpm"),
        "hrv": ("hrv_rmssd_ms", "ms"),
        "sleep": ("total_sleep_minutes", "min"),
        "steps": ("steps", "steps"),
        "resistance": ("resistance_minutes", "min"),
        "cardio": ("cardio_minutes", "min"),
    }
    summary: dict[str, dict[str, Any] | None] = {}
    for name, (column, unit) in definitions.items():
        match = next((row for row in reversed(daily) if row.get(column) is not None), None)
        summary[name] = (
            {"value": match[column], "unit": unit, "date": match["local_date"]}
            if match is not None
            else None
        )
    return summary


def dashboard_payload(database: Path, *, days: int = 365) -> dict[str, Any]:
    """Build the local dashboard read model without mutating health data."""

    if days not in ALLOWED_RANGE_DAYS:
        raise ValueError("dashboard range must be 7, 30, 90, or 365 days")
    columns = ", ".join(DAILY_COLUMNS)
    with connect(database, read_only=True) as connection:
        latest_row = connection.execute("SELECT MAX(local_date) FROM daily_health").fetchone()
        latest_date = latest_row[0] if latest_row else None
        if latest_date is None:
            daily: list[dict[str, Any]] = []
            weekly: list[dict[str, Any]] = []
            rolling: list[dict[str, Any]] = []
        else:
            daily = _rows(
                connection,
                f"""
                SELECT {columns}
                FROM health_calendar
                WHERE local_date BETWEEN ? - (? - 1) AND ?
                ORDER BY local_date
                """,
                [latest_date, days, latest_date],
            )
            weekly = _rows(
                connection,
                """
                SELECT week_start, week_end, weight_kg_avg, systolic_mmhg_avg,
                       diastolic_mmhg_avg, hrv_rmssd_ms_avg, resting_hr_bpm_avg,
                       total_sleep_minutes_avg, steps_total, steps_daily_avg,
                       resistance_minutes_total, cardio_minutes_total,
                       exercise_minutes_total, workout_count, exercise_days
                FROM weekly_health
                WHERE week_start >= ? - 371
                ORDER BY week_start
                """,
                [latest_date],
            )
            rolling = _rows(
                connection,
                """
                SELECT * FROM rolling_health_latest ORDER BY window_days
                """,
            )
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
    payload = {
        "generated_at": datetime.now().astimezone(),
        "latest_date": latest_date,
        "range_days": days,
        "summary": _latest_summary(daily),
        "daily": daily,
        "weekly": weekly,
        "rolling": rolling,
        "labs": labs,
    }
    return _json_value(payload)


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
        if path == "/api/dashboard":
            try:
                content = json.dumps(
                    dashboard_payload(self.database),
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
