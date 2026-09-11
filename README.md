# Health tracking

A local-first longitudinal personal health data platform. It preserves immutable vendor
responses, normalizes them into DuckDB with complete provenance, links duplicates without
deleting originals, and exports portable analytical datasets.

## Development

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run health init
uv run health doctor
uv run pytest
```

By default, commands operate on the current directory. Use `--root PATH` to initialize or
inspect another project directory.

Real health data, credentials, local databases, exports, and snapshots are ignored by Git.
Tests must use synthetic fixtures only.

## Oura authorization

Create an OAuth application in the [Oura developer portal](https://developer.ouraring.com),
register its exact callback URI, and set the corresponding `HEALTH_OURA_*` values from
`.env.example`. The default scopes are the minimum needed by the MVP: `daily`, `heartrate`,
`workout`, and `session`. New integrations use the server-side authorization-code flow rather
than deprecated personal access tokens; see Oura's
[OAuth documentation](https://cloud.ouraring.com/docs/authentication).

```bash
uv run health auth oura
# Or run the two steps separately:
uv run health oura authorize-url
uv run health oura exchange
uv run health oura status
```

Open the authorization URL, approve the requested scopes, then copy the callback's `code` and
`state` values into the hidden prompts. Oura refresh tokens are single-use; refresh is serialized
under a provider lock and the new access/refresh pair is persisted atomically.

## Oura synchronization

After authorization, one command imports sleep periods, daily sleep/readiness, sampled heart
rate, daily activity, workouts, and Oura sessions. Keeping these resources in one connector
gives the Oura account one overlap window and one authoritative watermark.

```bash
uv run health sync oura \
  --start '2026-09-01T00:00:00-04:00' \
  --end '2026-09-10T23:59:59-04:00'
uv run health sync status --source oura
```

Each paginated response is stored before normalization. Repeating a bounded sync is safe and
should be duplicate-heavy unless Oura revised a record. Daily non-wear time is stored explicitly;
missing Oura samples remain unknown and never imply inactivity. Oura workout records remain in
the database even where a manual resistance workout is preferred by the canonical view.

## Withings authorization

Copy `.env.example` to `.env` and set the Withings client ID, client secret, redirect URI,
and optional scope for your own Withings application. The secret is loaded only at runtime;
`.env` and `data/secrets/` are ignored by Git.

```bash
uv run health auth withings
# Or run the two steps separately:
uv run health withings authorize-url
uv run health withings exchange
uv run health withings status
```

Open the first command's URL, then pass the callback's code and state to the hidden prompts
in `exchange`. Each rotated access/refresh pair is stored together using a private,
cross-process lock and atomic file replacement.

## Withings synchronization

### Manual account export

Withings API access is optional. The offline importer accepts the ZIP delivered by Withings,
an extracted export directory, or an individual supported CSV. It currently imports weight,
fat mass, bone mass, muscle mass, hydration, and complete blood-pressure readings. Pound-based
mass columns are converted to kilograms.

In the Withings mobile app, open **Profile**, select **Settings**, choose **Export All Health
Data**, select the user profile, and start the archive. Withings emails a download link when the
archive is ready. See Withings' current instructions for
[iOS](https://support.withings.com/hc/en-us/articles/360001399167-Withings-App-iOS-Exporting-your-data)
or
[Android](https://support.withings.com/hc/en-us/articles/31647944317201-Withings-App-Android-Exporting-your-data).

Import the downloaded file directly; it does not need to be extracted or moved into the
repository:

```bash
uv run health import withings ~/Downloads/withings-export.zip
uv run health sync status --source withings
```

Naive CSV timestamps use `timezone` from `config/settings.yaml`. The original ZIP and each
recognized CSV are retained in immutable raw storage, while unsupported export files are
ignored. Re-importing the same archive is safe and reports canonical duplicates. File imports
do not advance the API synchronization watermark, so API access can be enabled later without
changing its normal lookback; use an explicit API `--start` if a historical API backfill is
needed.

### API synchronization

After initialization and authorization, run the raw-first Withings pipeline with an optional
bounded window. Timestamps must be ISO-8601 values with a timezone offset.

```bash
uv run health sync withings \
  --start '2026-08-28T00:00:00-04:00' \
  --end '2026-09-10T13:00:00-04:00'
uv run health sync status --source withings
```

Without `--start`, the first sync uses `default_sync_lookback_days` from
`config/settings.yaml`. Later syncs deliberately overlap the prior successful window by 72
hours so delayed vendor changes can be updated. A successful command reports only the run ID,
window, and raw/normalized/inserted/updated/duplicate counts; it does not print measurements.

For a live smoke test, record the canonical row counts, run the same bounded command again,
and confirm the second run is duplicate-heavy without unexplained row growth. A vendor record
changed between requests may correctly appear as an update. `health sync status` reads only the
local database, requires no credentials, and makes no provider request.

Do not paste `.env`, token files, raw payloads, or measurement values into bug reports. Share
only the run ID, requested window, counts, status, and sanitized error category. Confirm private
artifacts remain ignored after a live run:

```bash
git status --short
git check-ignore -v .env data/health.duckdb data/secrets/withings.tokens.json
```

## Apple Health import

Export your health data from the Health app on iPhone, then import the downloaded ZIP directly:

```bash
uv run health import apple-health ~/Downloads/export.zip
```

The original ZIP or `export.xml` is copied into immutable raw storage with a SHA-256 manifest.
The XML parser processes one element at a time and imports selected body measurements,
cardiovascular readings, activity, sleep stages, and workouts. Stable source-record identities
make repeat imports safe. Apple provenance—including source app, version, device, timestamps,
metadata, and workout/correlation identity—is retained for cross-source reconciliation.

## Manual workouts

Record resistance training with a shorthand designed for times when the Oura ring is removed.
Without `--date` or `--time`, the workout is treated as ending now:

```bash
uv run health lift 55 --focus "upper body" --rpe 8 --notes "optional private note"
```

Use the general command for other canonical workout types. When a date or time is supplied, the
given local time is the workout start. Supported types are `resistance`, `walking`, `running`,
`cycling`, `rowing`, `swimming`, `rucking`, `elliptical`, `mobility`, `sports`, and `other`.

```bash
uv run health workout add --type running --minutes 30 \
  --date 2026-09-10 --time 07:15 --focus "easy aerobic" --rpe 4
```

Manual entries are stored as private immutable raw documents before canonical insertion. Repeating
the exact same entry is duplicate-safe, and manual workouts have presentation priority without
deleting overlapping Apple Health or Oura records.

## Local dashboard

Launch the private dashboard after initialization. It binds only to `127.0.0.1`, sends no-store
and restrictive content-security headers, and loads no remote scripts, fonts, or analytics.

```bash
uv run health dashboard
# Keep the browser closed or choose another private port:
uv run health dashboard --no-open --port 9876
```

The overview and dedicated Weight, BP, Sleep, Exercise, and Labs sections use the canonical daily,
weekly, and rolling views. Switch among 7-day, 30-day, 90-day, and one-year charts in the header.
The latest values may have different observation dates; each card states its own date. Stop the
server with Ctrl-C. Weight remains normalized in kilograms in DuckDB and is converted to pounds at
the dashboard presentation boundary.

## Architecture invariants

- Raw source bytes are durable before normalization or cursor advancement.
- Canonical rows retain source, device, vendor identity, raw reference, and transform version.
- Source priority selects presentation; it never deletes overlapping source records.
- The database can be rebuilt without contacting vendors.

## Canonical and daily views

`health init` applies the derived-view migration and synchronizes
`config/source_priority.yaml` into DuckDB. Provider sync and file-import commands refresh those
priorities before ingestion; `health doctor` reports if the database copy is stale.

The live views preserve every canonical source row while selecting the preferred enabled source
for presentation:

- `canonical_observations`, `canonical_blood_pressure`, `canonical_sleep_sessions`, and
  `canonical_workouts` apply source priority without deleting lower-priority data.
- `blood_pressure_session_readings` groups readings no more than ten minutes apart, and
  `blood_pressure_sessions` retains first, subsequent-reading, session-mean, and preferred
  values.
- `daily_health` provides one row per local date for weight/body composition, BP, sleep and
  recovery, steps/activity, resistance/cardio minutes, and contextual events. Sleep is assigned
  to its stored wake date.
- `health_calendar` fills calendar dates between the first and last daily record without turning
  missing measurements into zeroes.
- `weekly_health` starts weeks on Monday and exposes averages, totals, and metric-specific coverage
  counts. `rolling_health` calculates the same core trends over calendar-day 7/30/90/365 windows;
  `rolling_health_latest` is the dashboard-ready four-row snapshot at the newest local date.

After pulling a migration or changing source priorities, run:

```bash
uv run health init
uv run health doctor
```

The open-source dependency decision is documented in
[`docs/adr/0001-open-source-ingestion-foundation.md`](docs/adr/0001-open-source-ingestion-foundation.md).
Dependency provenance, attribution, upgrades, rollback, and replacement criteria are enforced by
[`docs/dependency-policy.md`](docs/dependency-policy.md) and `health policy-check`.
# health-tracking
