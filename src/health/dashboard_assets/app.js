"use strict";

const state = {
  payload: { latest_date: null, summary: {}, daily: [], weekly: [], rolling: [], labs: [], quality: null },
  days: 365,
  trendsLoaded: false,
};
const SVG_NS = "http://www.w3.org/2000/svg";

const byId = (id) => document.getElementById(id);

function parseDay(value) {
  return value ? new Date(`${value}T12:00:00Z`) : null;
}

function shortDate(value) {
  const parsed = parseDay(value);
  return parsed
    ? new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(parsed)
    : "No reading";
}

function compactDate(value) {
  const parsed = parseDay(value);
  return parsed
    ? new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", timeZone: "UTC" }).format(parsed)
    : "—";
}

function number(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits }).format(Number(value));
}

function duration(value) {
  if (value === null || value === undefined) return "—";
  const minutes = Math.round(Number(value));
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  return hours ? `${hours}h ${String(remainder).padStart(2, "0")}m` : `${remainder}m`;
}

function elapsed(value) {
  if (value === null || value === undefined) return "—";
  const seconds = Number(value);
  if (seconds < 60) return `${number(seconds, 1)}s`;
  if (seconds < 3600) return `${number(seconds / 60, 1)}m`;
  return `${number(seconds / 3600, 1)}h`;
}

function metricName(value) {
  const [kind, detail] = value.split(":", 2);
  const label = detail ? `${kind} · ${detail}` : kind;
  return label.replaceAll("_", " ");
}

function cutoffDate(latest, days) {
  const value = parseDay(latest);
  value.setUTCDate(value.getUTCDate() - days + 1);
  return value;
}

function rowsInWindow(rows) {
  if (!state.payload.latest_date) return [];
  const cutoff = cutoffDate(state.payload.latest_date, state.days);
  return rows.filter((row) => parseDay(row.local_date) >= cutoff);
}

function svgElement(name, attributes = {}) {
  const element = document.createElementNS(SVG_NS, name);
  Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, String(value)));
  return element;
}

function emptyChart(container, message = "No readings in this window") {
  container.replaceChildren();
  const empty = document.createElement("p");
  empty.className = "chart-empty";
  empty.textContent = message;
  container.append(empty);
}

function renderLineChart(id, rows, series, options = {}) {
  const container = byId(id);
  const allValues = rows.flatMap((row) => series.map((item) => row[item.key])).filter((value) => value !== null && value !== undefined).map(Number);
  if (!allValues.length) {
    emptyChart(container);
    return;
  }

  const width = 1000;
  const height = options.height || 300;
  const margin = { top: 22, right: 22, bottom: 35, left: 54 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  let minimum = Math.min(...allValues);
  let maximum = Math.max(...allValues);
  const spread = maximum - minimum || Math.max(Math.abs(maximum) * 0.1, 1);
  minimum -= spread * 0.14;
  maximum += spread * 0.14;
  const x = (index) => margin.left + (rows.length === 1 ? innerWidth / 2 : index / (rows.length - 1) * innerWidth);
  const y = (value) => margin.top + (maximum - value) / (maximum - minimum) * innerHeight;

  const svg = svgElement("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": options.label || "Health trend chart" });
  for (let index = 0; index < 4; index += 1) {
    const ratio = index / 3;
    const gridY = margin.top + ratio * innerHeight;
    svg.append(svgElement("line", { x1: margin.left, x2: width - margin.right, y1: gridY, y2: gridY, class: "chart-grid" }));
    const label = svgElement("text", { x: 0, y: gridY + 4, class: "chart-axis-label" });
    label.textContent = number(maximum - ratio * (maximum - minimum), options.axisDigits || 0);
    svg.append(label);
  }

  series.forEach((item, seriesIndex) => {
    let path = "";
    rows.forEach((row, index) => {
      const value = row[item.key];
      if (value === null || value === undefined) return;
      const previous = index > 0 ? rows[index - 1][item.key] : null;
      const command = previous === null || previous === undefined ? "M" : "L";
      path += `${command}${x(index).toFixed(2)},${y(Number(value)).toFixed(2)} `;
    });
    svg.append(svgElement("path", { d: path.trim(), class: `chart-line${seriesIndex ? " secondary" : ""}` }));
    if (allValues.length <= 100) {
      rows.forEach((row, index) => {
        const value = row[item.key];
        if (value === null || value === undefined) return;
        const point = svgElement("circle", { cx: x(index), cy: y(Number(value)), r: 3.4, class: `chart-point${seriesIndex ? " secondary" : ""}` });
        const title = svgElement("title");
        title.textContent = `${item.label}: ${item.format ? item.format(value) : number(value, 1)} · ${shortDate(row.local_date)}`;
        point.append(title);
        svg.append(point);
      });
    }
  });

  [rows[0], rows[rows.length - 1]].forEach((row, index) => {
    const label = svgElement("text", { x: index ? width - margin.right : margin.left, y: height - 5, "text-anchor": index ? "end" : "start", class: "chart-axis-label" });
    label.textContent = compactDate(row.local_date);
    svg.append(label);
  });
  container.replaceChildren(svg);
}

function setSummary(id, record, formatter, dateId) {
  byId(id).textContent = record ? formatter(record.value) : "—";
  byId(dateId).textContent = record ? `As of ${shortDate(record.date)}` : "No reading";
}

function renderSummary() {
  const summary = state.payload.summary;
  setSummary("summary-weight", summary.weight, (value) => `${number(value, 1)} lb`, "summary-weight-date");
  const pressure = summary.systolic && summary.diastolic
    ? `${number(summary.systolic.value)}/${number(summary.diastolic.value)}`
    : "—";
  byId("summary-bp").textContent = pressure;
  byId("summary-bp-date").textContent = summary.systolic ? `As of ${shortDate(summary.systolic.date)}` : "No reading";
  setSummary("summary-rhr", summary.resting_hr, (value) => `${number(value)} bpm`, "summary-rhr-date");
  setSummary("summary-hrv", summary.hrv, (value) => `${number(value)} ms`, "summary-hrv-date");
  setSummary("summary-sleep", summary.sleep, duration, "summary-sleep-date");
}

function renderWindows(containerId, valueKeys, formatter) {
  const container = byId(containerId);
  const labels = { 7: "7 day", 30: "30 day", 90: "90 day", 365: "1 year" };
  const items = state.payload.rolling.map((row) => {
    const item = document.createElement("div");
    item.className = "window-item";
    const label = document.createElement("span");
    label.textContent = labels[row.window_days];
    const strong = document.createElement("strong");
    strong.textContent = formatter(valueKeys.map((key) => row[key]));
    const coverage = document.createElement("small");
    coverage.textContent = `${row.calendar_days} calendar day${row.calendar_days === 1 ? "" : "s"} available`;
    item.append(label, strong, coverage);
    return item;
  });
  container.replaceChildren(...items);
}

function average(rows, key) {
  const values = rows.map((row) => row[key]).filter((value) => value !== null && value !== undefined).map(Number);
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
}

function sum(rows, key) {
  const values = rows.map((row) => row[key]).filter((value) => value !== null && value !== undefined).map(Number);
  return values.length ? values.reduce((total, value) => total + value, 0) : null;
}

function renderWeight(rows) {
  renderLineChart("weight-chart", rows, [{ key: "weight_lb", label: "Weight", format: (value) => `${number(value, 1)} lb` }], { label: `Weight over ${state.days} days`, axisDigits: 1 });
  const observed = rows.filter((row) => row.weight_lb !== null);
  if (observed.length >= 2) {
    const change = Number(observed.at(-1).weight_lb) - Number(observed[0].weight_lb);
    byId("weight-change").textContent = `${change > 0 ? "+" : ""}${number(change, 1)} lb across selected readings`;
  } else {
    byId("weight-change").textContent = "Not enough data for change";
  }
  renderWindows("weight-windows", ["weight_lb_avg"], ([value]) => value === null ? "—" : `${number(value, 1)} lb`);
}

function renderPressure(rows) {
  renderLineChart("bp-chart", rows, [
    { key: "systolic_mmhg", label: "Systolic", format: (value) => `${number(value)} mmHg` },
    { key: "diastolic_mmhg", label: "Diastolic", format: (value) => `${number(value)} mmHg` },
  ], { label: `Blood pressure over ${state.days} days` });
  renderWindows("bp-windows", ["systolic_mmhg_avg", "diastolic_mmhg_avg"], ([systolic, diastolic]) => systolic === null || diastolic === null ? "—" : `${number(systolic)}/${number(diastolic)}`);
}

function renderRecovery(rows) {
  const sleepAverage = average(rows, "total_sleep_minutes");
  const rhrAverage = average(rows, "resting_hr_bpm");
  const hrvAverage = average(rows, "hrv_rmssd_ms");
  byId("sleep-window-average").textContent = duration(sleepAverage);
  byId("rhr-average").textContent = rhrAverage === null ? "—" : `${number(rhrAverage)} bpm`;
  byId("hrv-average").textContent = hrvAverage === null ? "—" : `${number(hrvAverage)} ms`;
  renderLineChart("sleep-chart", rows, [{ key: "total_sleep_minutes", label: "Sleep", format: duration }], { height: 180, label: `Sleep duration over ${state.days} days` });
  renderLineChart("rhr-chart", rows, [{ key: "resting_hr_bpm", label: "Resting HR" }], { height: 170, label: `Resting heart rate over ${state.days} days` });
  renderLineChart("hrv-chart", rows, [{ key: "hrv_rmssd_ms", label: "HRV" }], { height: 170, label: `HRV over ${state.days} days` });
}

function renderExercise(rows) {
  const resistance = sum(rows, "resistance_minutes");
  const cardio = sum(rows, "cardio_minutes");
  const steps = average(rows, "steps");
  byId("resistance-total").textContent = resistance === null ? "—" : `${number(resistance)} min`;
  byId("cardio-total").textContent = cardio === null ? "—" : `${number(cardio)} min`;
  byId("steps-average").textContent = steps === null ? "—" : number(steps);

  if (!state.payload.latest_date) {
    emptyChart(byId("exercise-chart"), "No workouts in this window");
    return;
  }

  const cutoff = cutoffDate(state.payload.latest_date, state.days);
  const weeks = state.payload.weekly.filter((row) => parseDay(row.week_end) >= cutoff);
  const chart = byId("exercise-chart");
  const values = weeks.flatMap((row) => [row.resistance_minutes_total || 0, row.cardio_minutes_total || 0]);
  const maximum = Math.max(...values, 1);
  const bars = document.createElement("div");
  bars.className = "bars";
  weeks.forEach((row) => {
    const group = document.createElement("div");
    group.className = "bar-week";
    [["resistance_minutes_total", "Resistance", ""], ["cardio_minutes_total", "Cardio", " cardio"]].forEach(([key, label, className]) => {
      const bar = document.createElement("div");
      const value = Number(row[key] || 0);
      bar.className = `bar${className}`;
      bar.style.height = `${value / maximum * 100}%`;
      bar.title = `${label}: ${number(value)} min · week of ${shortDate(row.week_start)}`;
      group.append(bar);
    });
    bars.append(group);
  });
  if (weeks.length) chart.replaceChildren(bars); else emptyChart(chart, "No workouts in this window");
}

function renderLabs() {
  const body = byId("labs-body");
  const empty = byId("labs-empty");
  if (!state.payload.labs.length) {
    body.replaceChildren();
    empty.hidden = false;
    return;
  }
  empty.hidden = true;
  const rows = state.payload.labs.map((lab) => {
    const row = document.createElement("tr");
    const marker = document.createElement("td");
    marker.textContent = lab.name;
    if (lab.abnormal_flag) marker.className = "lab-flag";
    const result = document.createElement("td");
    const resultValue = lab.numeric_value !== null ? number(lab.numeric_value, 2) : (lab.text_value || "—");
    result.textContent = `${resultValue}${lab.unit ? ` ${lab.unit}` : ""}`;
    const reference = document.createElement("td");
    reference.textContent = lab.reference_text || (lab.reference_low !== null || lab.reference_high !== null ? `${lab.reference_low ?? "—"} – ${lab.reference_high ?? "—"}` : "—");
    const observed = document.createElement("td");
    observed.textContent = lab.observed_at ? shortDate(lab.observed_at.slice(0, 10)) : "—";
    row.append(marker, result, reference, observed);
    return row;
  });
  body.replaceChildren(...rows);
}

function renderQuality() {
  const quality = state.payload.quality;
  const statusLabels = {
    clear: "Clear",
    review: "Review",
    attention: "Attention",
    empty: "Awaiting data",
  };
  const status = byId("quality-state");
  status.textContent = statusLabels[quality.status] || quality.status;
  status.dataset.state = quality.status;
  byId("quality-metrics").textContent = number(quality.metric_source_count);
  byId("quality-imports").textContent = number(quality.source_import_count);
  const diagnostic = quality.diagnostics;
  const reviewCount = diagnostic.suspect_records + diagnostic.invalid_records
    + diagnostic.duplicate_candidates + diagnostic.sleep_over_24h
    + diagnostic.latest_failed_imports + diagnostic.latest_running_imports
    + quality.stale_metric_sources;
  byId("quality-review").textContent = number(reviewCount);

  const coverageBody = byId("quality-coverage-body");
  const coverageEmpty = byId("quality-coverage-empty");
  coverageEmpty.hidden = quality.coverage.length > 0;
  coverageBody.replaceChildren(...quality.coverage.map((item) => {
    const row = document.createElement("tr");
    const metric = document.createElement("th");
    metric.scope = "row";
    const metricLabel = document.createElement("strong");
    metricLabel.textContent = metricName(item.metric);
    const source = document.createElement("small");
    source.textContent = `${item.source} · ${item.cadence}`;
    metric.append(metricLabel, source);
    const range = document.createElement("td");
    range.textContent = `${compactDate(item.first_date)} — ${compactDate(item.last_date)}`;
    const days = document.createElement("td");
    days.textContent = `${number(item.observed_days)} / ${number(item.span_days)}`;
    const missing = document.createElement("small");
    missing.textContent = `${number(item.missing_days)} missing`;
    days.append(missing);
    const density = document.createElement("td");
    density.textContent = `${number(item.coverage_pct, 1)}%`;
    const freshness = document.createElement("td");
    const badge = document.createElement("span");
    badge.className = `freshness-badge ${item.freshness_status}`;
    badge.textContent = item.freshness_status;
    const age = document.createElement("small");
    age.textContent = `${number(item.freshness_days)}d since last record`;
    freshness.append(badge, age);
    row.append(metric, range, days, density, freshness);
    return row;
  }));

  const importBody = byId("quality-import-body");
  const importEmpty = byId("quality-import-empty");
  importEmpty.hidden = quality.source_runs.length > 0;
  importBody.replaceChildren(...quality.source_runs.map((item) => {
    const row = document.createElement("tr");
    const source = document.createElement("th");
    source.scope = "row";
    source.textContent = item.source;
    const completed = document.createElement("td");
    completed.textContent = item.finished_at ? shortDate(item.finished_at.slice(0, 10)) : "—";
    const durationCell = document.createElement("td");
    durationCell.textContent = elapsed(item.duration_seconds);
    const rate = document.createElement("td");
    rate.textContent = item.throughput_per_second === null ? "—" : `${number(item.throughput_per_second, 1)}/s`;
    const result = document.createElement("td");
    result.textContent = `${number(item.normalized_count)} normalized`;
    const detail = document.createElement("small");
    detail.textContent = `${number(item.inserted_count)} new · ${number(item.duplicate_count)} duplicate`;
    result.append(detail);
    row.append(source, completed, durationCell, rate, result);
    return row;
  }));

  const diagnostics = [
    ["Suspect records", diagnostic.suspect_records, "review"],
    ["Invalid records", diagnostic.invalid_records, "attention"],
    ["Duplicate candidates", diagnostic.duplicate_candidates, "attention"],
    ["Confirmed duplicates", diagnostic.duplicate_confirmed, "neutral"],
    ["Rejected duplicates", diagnostic.duplicate_rejected, "neutral"],
    ["Sleep days over 16h", diagnostic.sleep_over_16h, "review"],
    ["Sleep days over 24h", diagnostic.sleep_over_24h, "attention"],
    ["Latest failed imports", diagnostic.latest_failed_imports, "attention"],
    ["Latest running imports", diagnostic.latest_running_imports, "review"],
  ];
  byId("quality-diagnostics").replaceChildren(...diagnostics.map(([label, value, severity]) => {
    const item = document.createElement("div");
    item.className = `diagnostic-item ${Number(value) ? severity : "clear"}`;
    const term = document.createElement("dt");
    term.textContent = label;
    const count = document.createElement("dd");
    count.textContent = number(value);
    item.append(term, count);
    return item;
  }));
}

function renderTrends() {
  const rows = rowsInWindow(state.payload.daily);
  renderWeight(rows);
  renderPressure(rows);
  renderRecovery(rows);
  renderExercise(rows);
  renderLabs();
  byId("data-status").textContent = state.payload.latest_date
    ? `${rows.length} calendar days shown · source priorities applied locally`
    : "The dashboard is ready; import data to begin the ledger.";
}

function setRange(days) {
  if (!state.trendsLoaded) return;
  state.days = days;
  document.querySelectorAll("[data-days]").forEach((button) => {
    button.setAttribute("aria-pressed", Number(button.dataset.days) === days ? "true" : "false");
  });
  renderTrends();
}

async function fetchPayload(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) throw new Error(`Dashboard request failed: ${path}`);
  return response.json();
}

async function loadSummary() {
  try {
    Object.assign(state.payload, await fetchPayload("/api/dashboard/summary"));
    renderSummary();
    byId("latest-date").textContent = state.payload.latest_date ? `through ${shortDate(state.payload.latest_date)}` : "no data yet";
    byId("summary-status").textContent = state.payload.latest_date
      ? "Latest available readings loaded. Trend history is loading below."
      : "No readings yet. Import data to begin the ledger.";
  } catch (_error) {
    byId("summary-status").textContent = "Could not load the latest signals. Run health doctor and retry.";
  } finally {
    byId("overview").setAttribute("aria-busy", "false");
  }
}

async function loadTrends() {
  byId("data-status").textContent = "Loading trend history…";
  try {
    Object.assign(state.payload, await fetchPayload("/api/dashboard/trends"));
    state.trendsLoaded = true;
    document.querySelectorAll("[data-days]").forEach((button) => { button.disabled = false; });
    renderTrends();
  } catch (_error) {
    byId("data-status").textContent = "Could not read the local health database. Run health doctor and retry.";
    document.querySelectorAll(".line-chart, .bar-chart").forEach((chart) => emptyChart(chart, "Dashboard data unavailable"));
  } finally {
    byId("trend-controls").setAttribute("aria-busy", "false");
  }
}

async function loadQuality() {
  try {
    Object.assign(state.payload, await fetchPayload("/api/dashboard/quality"));
    renderQuality();
  } catch (_error) {
    byId("quality-state").textContent = "Unavailable";
    byId("quality-state").dataset.state = "attention";
  } finally {
    byId("quality").setAttribute("aria-busy", "false");
  }
}

async function load() {
  await loadSummary();
  await loadTrends();
  await loadQuality();
}

document.querySelectorAll("[data-days]").forEach((button) => {
  button.addEventListener("click", () => setRange(Number(button.dataset.days)));
});

load();
