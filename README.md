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
