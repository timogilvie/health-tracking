"""Descriptive, local-only cross-dataset analysis and HTML plotting."""

from __future__ import annotations

import html
import math
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import fmean

from health.db import connect


@dataclass(frozen=True, slots=True)
class Relationship:
    slug: str
    title: str
    x_label: str
    y_label: str
    x_unit: str
    y_unit: str
    points: tuple[tuple[float, float], ...]
    correlation: float | None
    confidence_low: float | None
    confidence_high: float | None

    @property
    def sample_size(self) -> int:
        return len(self.points)


@dataclass(frozen=True, slots=True)
class AnalysisReport:
    generated_at: datetime
    first_date: date | None
    last_date: date | None
    relationships: tuple[Relationship, ...]


@dataclass(frozen=True, slots=True)
class _Spec:
    slug: str
    title: str
    x_column: str
    y_column: str
    x_label: str
    y_label: str
    x_unit: str
    y_unit: str


SPECS = (
    _Spec(
        "alcohol-sleep",
        "Alcohol vs next wake-date sleep",
        "alcohol_units",
        "next_day_total_sleep_minutes",
        "Standard drinks",
        "Next-day sleep",
        "drinks",
        "minutes",
    ),
    _Spec(
        "alcohol-hrv",
        "Alcohol vs next-day HRV",
        "alcohol_units",
        "next_day_hrv_rmssd_ms",
        "Standard drinks",
        "Next-day HRV",
        "drinks",
        "ms",
    ),
    _Spec(
        "alcohol-rhr",
        "Alcohol vs next-day resting heart rate",
        "alcohol_units",
        "next_day_resting_hr_bpm",
        "Standard drinks",
        "Next-day resting HR",
        "drinks",
        "bpm",
    ),
    _Spec(
        "alcohol-bp",
        "Alcohol vs next-day systolic blood pressure",
        "alcohol_units",
        "next_day_systolic_mmhg",
        "Standard drinks",
        "Next-day systolic BP",
        "drinks",
        "mmHg",
    ),
    _Spec(
        "sleep-systolic",
        "Wake-date sleep vs same-day systolic blood pressure",
        "wake_date_sleep_minutes",
        "same_day_systolic_mmhg",
        "Sleep",
        "Systolic BP",
        "minutes",
        "mmHg",
    ),
    _Spec(
        "sleep-diastolic",
        "Wake-date sleep vs same-day diastolic blood pressure",
        "wake_date_sleep_minutes",
        "same_day_diastolic_mmhg",
        "Sleep",
        "Diastolic BP",
        "minutes",
        "mmHg",
    ),
    _Spec(
        "weight-systolic",
        "Weight vs same-day systolic blood pressure",
        "weight_lb",
        "same_day_systolic_mmhg",
        "Weight",
        "Systolic BP",
        "lb",
        "mmHg",
    ),
    _Spec(
        "weight-diastolic",
        "Weight vs same-day diastolic blood pressure",
        "weight_lb",
        "same_day_diastolic_mmhg",
        "Weight",
        "Diastolic BP",
        "lb",
        "mmHg",
    ),
    _Spec(
        "exercise-sleep",
        "Exercise vs next wake-date sleep",
        "exercise_minutes",
        "next_day_total_sleep_minutes",
        "Exercise",
        "Next-day sleep",
        "minutes",
        "minutes",
    ),
    _Spec(
        "exercise-hrv",
        "Exercise vs next-day HRV",
        "exercise_minutes",
        "next_day_hrv_rmssd_ms",
        "Exercise",
        "Next-day HRV",
        "minutes",
        "ms",
    ),
    _Spec(
        "exercise-rhr",
        "Exercise vs next-day resting heart rate",
        "exercise_minutes",
        "next_day_resting_hr_bpm",
        "Exercise",
        "Next-day resting HR",
        "minutes",
        "bpm",
    ),
)


def _correlation(points: tuple[tuple[float, float], ...]) -> float | None:
    if len(points) < 3:
        return None
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    mean_x = fmean(xs)
    mean_y = fmean(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in points)
    denominator = math.sqrt(sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys))
    if denominator == 0:
        return None
    return max(-1.0, min(1.0, numerator / denominator))


def _confidence_interval(
    correlation: float | None, sample_size: int
) -> tuple[float | None, float | None]:
    if correlation is None or sample_size < 4:
        return None, None
    bounded = max(-0.999999, min(0.999999, correlation))
    fisher = math.atanh(bounded)
    margin = 1.96 / math.sqrt(sample_size - 3)
    return math.tanh(fisher - margin), math.tanh(fisher + margin)


def analyze(database: Path, *, now: datetime | None = None) -> AnalysisReport:
    """Calculate source-selected descriptive relationships from lagged daily fields."""

    metric_columns = {spec.x_column for spec in SPECS} | {spec.y_column for spec in SPECS}
    columns = ["predictor_date", *sorted(metric_columns)]
    with connect(database, read_only=True) as connection:
        cursor = connection.execute(
            f"SELECT {', '.join(columns)} FROM analysis_daily_lagged ORDER BY predictor_date"
        )
        rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    relationships: list[Relationship] = []
    for spec in SPECS:
        points = tuple(
            (float(row[spec.x_column]), float(row[spec.y_column]))
            for row in rows
            if row[spec.x_column] is not None and row[spec.y_column] is not None
        )
        correlation = _correlation(points)
        low, high = _confidence_interval(correlation, len(points))
        relationships.append(
            Relationship(
                spec.slug,
                spec.title,
                spec.x_label,
                spec.y_label,
                spec.x_unit,
                spec.y_unit,
                points,
                correlation,
                low,
                high,
            )
        )
    dates = [row["predictor_date"] for row in rows]
    return AnalysisReport(
        generated_at=now or datetime.now(UTC),
        first_date=min(dates) if dates else None,
        last_date=max(dates) if dates else None,
        relationships=tuple(relationships),
    )


def _format_stat(value: float | None) -> str:
    return "insufficient data" if value is None else f"{value:+.2f}"


def _scatter(relationship: Relationship) -> str:
    width, height, pad = 560, 300, 42
    if not relationship.points:
        return '<p class="empty">No paired observations yet.</p>'
    points = relationship.points
    displayed = points if len(points) <= 500 else points[:: math.ceil(len(points) / 500)]
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    span_x = max(max_x - min_x, 1)
    span_y = max(max_y - min_y, 1)
    circles = "".join(
        (
            f'<circle cx="{pad + (x - min_x) / span_x * (width - pad * 2):.1f}" '
            f'cy="{height - pad - (y - min_y) / span_y * (height - pad * 2):.1f}" r="3"/>'
        )
        for x, y in displayed
    )
    return (
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="Scatter plot: {html.escape(relationship.title)}">'
        f'<line x1="{pad}" y1="{height - pad}" x2="{width - pad}" y2="{height - pad}"/>'
        f'<line x1="{pad}" y1="{pad}" x2="{pad}" y2="{height - pad}"/>{circles}'
        f'<text x="{width / 2}" y="{height - 8}" text-anchor="middle">'
        f"{html.escape(relationship.x_label)} ({html.escape(relationship.x_unit)})</text>"
        f'<text x="14" y="{height / 2}" transform="rotate(-90 14 {height / 2})" '
        f'text-anchor="middle">{html.escape(relationship.y_label)} '
        f"({html.escape(relationship.y_unit)})</text></svg>"
    )


def render_html(report: AnalysisReport) -> str:
    """Render a private, dependency-free analysis report with inline SVG plots."""

    date_range = (
        f"{report.first_date.isoformat()} through {report.last_date.isoformat()}"
        if report.first_date and report.last_date
        else "No daily data available"
    )
    cards = []
    for relationship in report.relationships:
        confidence = (
            f"95% CI {relationship.confidence_low:+.2f} to {relationship.confidence_high:+.2f}"
            if relationship.confidence_low is not None and relationship.confidence_high is not None
            else "95% CI unavailable (requires at least 4 variable pairs)"
        )
        cards.append(
            f"""<section><h2>{html.escape(relationship.title)}</h2>
<p class="stat">n={relationship.sample_size} ·
Pearson r={_format_stat(relationship.correlation)} · {confidence}</p>
{_scatter(relationship)}</section>"""
        )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<meta name="robots" content="noindex,nofollow"><title>Baseline descriptive analysis</title>
<style>
body{{margin:0;background:#f5f0e7;color:#15211d;font:16px/1.5 "Avenir Next",Avenir,sans-serif}}
main{{max-width:1200px;margin:auto;padding:48px 24px}}
h1{{font:clamp(2.4rem,6vw,5rem)/.95 Georgia,serif}}
.guardrail{{border:2px solid #d7472f;padding:18px;background:#fff8ef}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:18px}}
section{{background:#fffdf8;border:1px solid #a9a69d;padding:20px;margin-top:24px}}
h2{{font:1.45rem Georgia,serif;margin-top:0}}.stat{{font-size:.88rem}}
svg{{width:100%;height:auto;background:#faf7f0}}svg line{{stroke:#777;stroke-width:1}}
svg circle{{fill:#d7472f;opacity:.58}}svg text{{font-size:12px;fill:#333}}
.empty{{padding:80px 20px;text-align:center;color:#666}}
</style></head><body><main><p>BASELINE · PRIVATE LOCAL REPORT</p>
<h1>Associations in your health record</h1>
<p>{html.escape(date_range)} · Generated {html.escape(report.generated_at.isoformat())}</p>
<div class="guardrail"><strong>Descriptive—not causal or diagnostic.</strong>
These correlations do not establish that one behavior caused an outcome. Small samples,
missing data, measurement timing, confounding, and repeated observations can materially
distort results. Unlogged alcohol is treated as zero, so alcohol comparisons are meaningful
only when logging is consistent. Discuss clinical decisions with a qualified clinician.</div>
<div class="grid">{"".join(cards)}</div></main></body></html>"""


def write_report(report: AnalysisReport, path: Path) -> Path:
    """Write a private report without overwriting an existing artifact."""

    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("x", encoding="utf-8") as handle:
            os.chmod(target, 0o600)
            handle.write(render_html(report))
    except FileExistsError as exc:
        raise ValueError(f"analysis report already exists: {target}") from exc
    return target
