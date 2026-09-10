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

## Architecture invariants

- Raw source bytes are durable before normalization or cursor advancement.
- Canonical rows retain source, device, vendor identity, raw reference, and transform version.
- Source priority selects presentation; it never deletes overlapping source records.
- The database can be rebuilt without contacting vendors.

The open-source dependency decision is documented in
[`docs/adr/0001-open-source-ingestion-foundation.md`](docs/adr/0001-open-source-ingestion-foundation.md).
Dependency provenance, attribution, upgrades, rollback, and replacement criteria are enforced by
[`docs/dependency-policy.md`](docs/dependency-policy.md) and `health policy-check`.
# health-tracking
