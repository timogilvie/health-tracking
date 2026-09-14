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

Before committing, verify that the Git index contains no credentials or likely personal-health
artifacts. This scans tracked and staged files only; it never reads ignored private data:

```bash
uv run health privacy-check
```

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

### Manual account export and import

Withings API access is optional. The offline importer accepts the ZIP delivered by Withings,
an extracted export directory, or an individual supported CSV. It currently imports weight,
fat mass, bone mass, muscle mass, hydration, and complete blood-pressure readings. Pound-based
mass columns are converted to kilograms.

To create and import a manual export:

1. In the Withings mobile app, open **Profile** and select **Settings**.
2. Choose **Export All Health Data**, select the user profile, and start the archive.
3. When Withings emails the download link, download the ZIP to the computer where this project
   is installed. Leave the archive zipped.
4. Import the ZIP, refresh cross-source duplicate links, and check the local source status:

```bash
uv run health import withings ~/Downloads/data_PROFILE_1234567890.zip
uv run health duplicates refresh
uv run health sync status --source withings
```

Replace the example filename with the downloaded archive's name. To avoid path mistakes, type
`uv run health import withings ` (including the trailing space), drag the ZIP from Finder into
Terminal, and press Return. See Withings' current export instructions for
[iOS](https://support.withings.com/hc/en-us/articles/360001399167-Withings-App-iOS-Exporting-your-data)
or
[Android](https://support.withings.com/hc/en-us/articles/31647944317201-Withings-App-Android-Exporting-your-data).

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

Apple Health provides a manual export from the Health app on iPhone. It contains the health and
fitness data available on the device in XML format. To create and import it:

1. Open **Health** on the iPhone and select **Summary**.
2. Tap your picture or initials in the upper-right corner.
3. Scroll down and tap **Export All Health Data**, then tap **Export**. Building a large archive
   may take several minutes.
4. AirDrop the archive to the Mac or choose **Save to Files** and download it on the Mac. Leave
   the resulting `export.zip` archive zipped.
5. Import the ZIP and refresh cross-source duplicate links:

```bash
uv run health import apple-health ~/Downloads/export.zip
uv run health duplicates refresh
```

Large Apple archives are streamed and committed in bounded 5,000-record batches. Imports that
run longer than five seconds print periodic `PROGRESS` lines with normalized, inserted, updated,
and duplicate counts, elapsed time, and throughput. The total record count is not known until the
XML stream reaches the end, so progress reports show completed records rather than a potentially
misleading percentage. Batch counts are checkpointed in `ingestion_runs`, and stable record IDs
make a retry safe if an import is interrupted.

If the archive has a different name or location, type `uv run health import apple-health `
(including the trailing space), drag the ZIP from Finder into Terminal, and press Return. See
[Apple's current export instructions](https://support.apple.com/guide/iphone/share-your-health-data-iph5ede58c3d/ios).

The original ZIP or `export.xml` is copied into immutable raw storage with a SHA-256 manifest.
The XML parser processes one element at a time and imports selected body measurements,
cardiovascular readings, activity, sleep stages, and workouts. Stable source-record identities
make repeat imports safe. Apple provenance—including source app, version, device, timestamps,
metadata, and workout/correlation identity—is retained for cross-source reconciliation.

Reconcile direct-vendor records with Apple Health copies after importing:

```bash
uv run health duplicates list
uv run health duplicates resolve LINK_UUID --resolution confirmed
```

High-confidence provenance, exact, and overlap matches are confirmed automatically. Ambiguous
heuristic matches remain review candidates. Resolution changes presentation selection only:
every original source row remains in DuckDB, and rejected review decisions survive refreshes.

Both provider archives contain sensitive health information. Do not extract them into a tracked
directory, commit them to Git, or attach them to public issues. The importers retain private raw
copies under the project's ignored `data/` directory.

## Laboratory results

Use a long-format CSV with at least `Test Name` and `Result` columns. Optional columns include
collection/result dates, unit, reference bounds or text range, flag, provider, and fasting status.
Preview is the default and never writes data:

```bash
uv run health import labs ~/Downloads/lab-results.csv
uv run health import labs ~/Downloads/lab-results.csv --commit
```

Aliases in `config/biomarkers.yaml` map common test names to the dashboard vocabulary. Unknown
tests are retained with their original name and reported in the preview; they are never silently
dropped. A committed CSV and all source columns remain in private immutable raw storage.

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

## Contextual events

Record standard drinks with the shortest path, or add a dated contextual event:

```bash
uv run health alcohol 4 --notes "optional private note"
uv run health event add --type illness --date 2026-09-10 --duration-hours 48
```

Supported contexts include alcohol, illness, travel, injury, supplement, diet, training, and
medication changes, plus `other`. Optional magnitude, unit, duration, and notes are stored as an
immutable manual document before canonical insertion. Alcohol totals participate in daily
rollups and all event types are available to longitudinal analysis.

## Descriptive analysis

Generate a private, self-contained HTML report after building enough consistently logged history:

```bash
uv run health analyze
```

The report plots alcohol against next-day sleep/recovery/BP, wake-date sleep against same-day
BP, weight against same-day BP, and exercise against next-day recovery. It shows the paired
sample size, Pearson correlation, and an approximate 95% confidence interval when the sample is
large enough. Lagging is explicit in the `analysis_daily_lagged` view. Results are descriptive
associations—not causal claims, diagnoses, or treatment advice—and the report calls out missing
data, timing, confounding, repeated observations, and alcohol-logging completeness.

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
