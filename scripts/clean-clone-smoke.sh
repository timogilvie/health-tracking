#!/usr/bin/env bash
set -euo pipefail

smoke_root="$(mktemp -d)"
trap 'rm -rf -- "$smoke_root"' EXIT

cp -R config sql "$smoke_root/"
export UV_OFFLINE=1
unset HEALTH_OURA_CLIENT_ID HEALTH_OURA_CLIENT_SECRET HEALTH_WITHINGS_CLIENT_ID
unset HEALTH_WITHINGS_CLIENT_SECRET

uv run health init --root "$smoke_root"
uv run health import apple-health \
  tests/fixtures/providers/apple_health/export.xml --root "$smoke_root"
uv run health rebuild --target "$smoke_root/data/rebuilt.duckdb" --root "$smoke_root"

HEALTH_DATABASE_PATH=data/rebuilt.duckdb uv run health doctor --root "$smoke_root"
HEALTH_DATABASE_PATH=data/rebuilt.duckdb uv run health export datasets \
  --format parquet --output "$smoke_root/data/exports/parquet" --root "$smoke_root"
HEALTH_DATABASE_PATH=data/rebuilt.duckdb uv run health export datasets \
  --format csv --output "$smoke_root/data/exports/csv" --root "$smoke_root"

test "$(find "$smoke_root/data/exports/parquet" -name '*.parquet' | wc -l)" -eq 9
test "$(find "$smoke_root/data/exports/csv" -name '*.csv' | wc -l)" -eq 9
