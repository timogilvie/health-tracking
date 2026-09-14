# Releases, migrations, and compatibility

## Versioning and release decision

The package follows semantic versioning. Before 1.0, minor releases may intentionally change CLI
or analytical-view behavior; patch releases are limited to compatible fixes where practical.
Completing release-readiness checks does not publish the repository or a package. A maintainer must
separately approve repository visibility, the release tag, and distribution artifacts after
reviewing the privacy and license gates.

Only the latest release is supported before 1.0. There is no hosted service, uptime commitment,
provider-API SLA, or guarantee that every historical export format remains supported.

## Database migrations

SQL migrations in `sql/` are append-only and checksummed. Applied files must never be edited or
renumbered. `health init` applies pending migrations transactionally; `health doctor` reports
pending migrations and checksum drift.

Before upgrading, stop writers and back up the ignored DuckDB file. A release may add tables,
columns, views, indexes, or stricter derived semantics. Destructive canonical-schema changes require
a documented migration and release note. Immutable raw manifests remain the recovery boundary.

There is no automatic downgrade. To roll back, restore a compatible private database backup or run
the prior release against a new database rebuilt from immutable raw artifacts. Never point an older
release at a database after newer migrations unless that release explicitly documents compatibility.

## Compatibility promises

- Raw manifest schema changes require a version field and backward-compatible reader or migration.
- Connector contracts retain raw-before-normalize and offline-replay behavior.
- Stable source-record identities remain duplicate-safe within a transform version.
- CSV/Parquet exports are portable snapshots, not a stable public API before 1.0.
- Provider behavior may require a connector update even when the core remains compatible.

Release notes must identify migrations, transform-version changes, replay recommendations, provider
changes, security fixes, and export-schema changes.
