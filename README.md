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

## Architecture invariants

- Raw source bytes are durable before normalization or cursor advancement.
- Canonical rows retain source, device, vendor identity, raw reference, and transform version.
- Source priority selects presentation; it never deletes overlapping source records.
- The database can be rebuilt without contacting vendors.

The open-source dependency decision is documented in
[`docs/adr/0001-open-source-ingestion-foundation.md`](docs/adr/0001-open-source-ingestion-foundation.md).
# health-tracking
