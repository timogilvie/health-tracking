# ADR-0001: Selective open-source reuse for health ingestion

- Status: Accepted
- Date: 2026-09-10
- Decision owner: Health tracking project
- Linear issue: [HOK-2994](https://linear.app/hokusai/issue/HOK-2994/health-platform-evaluate-open-source-foundation-before-implementation)
- Review basis: repository snapshots pinned below; all factual statements are as of 2026-09-10

## Decision

Choose **Strategy C: keep the planned Python/DuckDB architecture and reuse selected clients and mapping assets**.

Use [`oura-ring` 1.0.1](https://github.com/hedgertronic/oura-ring/tree/691dc2e75e97b976772c6bed80ad6936e3dee438) as the Oura HTTP/OAuth2 client, with project-owned encrypted token persistence, refresh-token rotation, overlap windows, immutable page capture, normalization, and replay. Its Python 3.12 floor matches this project, it is MIT-licensed, it covers the required Oura v2 resources and pagination, and its 61 mocked tests passed locally.[1]

For Withings, selectively port or adapt the MIT-licensed OAuth, RPC/pagination, schemas, endpoint coverage, and fixture patterns from [Open Wearables at `802862fa`](https://github.com/the-momentum/open-wearables/tree/802862fa1ac08f165a897cb9adc4f4b312b08e5c).[2] Keep these behind the project connector contract; do not import Open Wearables' database, workers, raw store, normalized schema, or deployment topology.

For Apple Health, retain a project-owned streaming `export.xml` parser. Reuse MIT-licensed type maps or small parser routines from Open Wearables only when they preserve `sourceName`, `sourceVersion`, `device`, timestamps, metadata, deterministic external identity, and the original archive. Do not adopt its upload/S3 workflow.

Do not copy HealthLog's current code. Its current license is PolyForm Noncommercial 1.0.0, commercial use requires a separate agreement, and it is therefore unsuitable for a permissively licensed offering.[3] HealthLog remains a valuable behavioral reference for rotating tokens, incremental watermarks, Apple import idempotency, source priority, labs, and tests.

Do not reuse Allos code unless this project deliberately adopts AGPL-3.0-compatible distribution. Use it as a behavioral reference for row-level sync provenance, manual correction locks, export completeness, and ambiguous-merge review.[4]

## Why not the alternatives?

| Strategy | Result | Rationale |
|---|---|---|
| A. Adopt or fork a full project | Reject | All three impose a TypeScript/Postgres or service-heavy product architecture that displaces the local-first DuckDB plan. HealthLog and Allos also create licensing constraints. |
| B. Run an OSS ingestion sidecar and keep DuckDB canonical | Reject for MVP | Open Wearables is the only plausible sidecar, but it adds Postgres, Redis, Celery, S3-compatible storage, Svix, and operational reconciliation. Its raw capture is optional/best-effort and its Withings API pulls are not captured, so the sidecar still cannot satisfy rebuild-from-raw.[5] |
| C. Keep the architecture and reuse selected clients | Accept | Preserves local-first operation and canonical invariants while avoiding provider-protocol plumbing. MIT components can be attributed and maintained behind a small adapter boundary. |

## Candidate findings

### Open Wearables

- **Maturity and activity:** active Python project with frequent releases; the reviewed head contains production-style tests and native Oura, Withings, Apple Health, Garmin, Fitbit, Polar, WHOOP, Strava, and other providers.[2]
- **Compatibility:** Python is favorable, but the backend currently targets Python 3.13 and assumes FastAPI, PostgreSQL, Redis/Celery, and S3-like storage. Extraction is practical; adoption is not.
- **Oura:** OAuth2, rotating refresh tokens, pagination, webhooks, workouts, daily scores, heart rate, SpO2, VO2 max and cardiovascular data. API responses are passed to an optional raw-payload hook.[6]
- **Withings:** OAuth2 and rotating refresh, `getmeas`, activity, sleep, workouts, webhooks, broad measurement mappings, and pagination. The reviewed API pull path does not call the raw-payload store; only webhook envelopes are captured. Blood-pressure components are emitted as time series rather than preserved as a vendor measurement group.[7]
- **Raw/replay:** raw storage is disabled by default, skips oversized payloads, does not hash content, and is described as debugging storage. The replay utility targets the SDK-sync path, not a general replay of all cloud-provider pulls.[5]
- **Provenance/dedup:** source/device metadata and vendor IDs are represented, with same-source uniqueness and provider preference. It does not model cross-source duplicate links or confidence, and preference is not equivalent to retaining an explicit duplicate graph.
- **Gaps for this plan:** no direct Eight Sleep connector, no first-class manual lifting/labs workflow matching the plan, no DuckDB/Parquet export and no database rebuild guarantee.

### HealthLog

- **Maturity and activity:** large, actively released TypeScript/Next.js/Postgres product with extensive tests and unusually complete Withings, Apple Health, labs, manual health records and FHIR workflows.[3]
- **Oura/Withings:** strong OAuth token rotation and guarded watermarks; Withings coverage is broad. The reviewed Oura code does not cover all required workout/heart-rate paths.
- **Apple Health:** supports large archive streaming, file SHA-256 idempotency, parser revisions and deterministic record reconciliation.[8] However, the staged upload is deleted after processing, and some raw source/device labels are deliberately collapsed, so the canonical store cannot be rebuilt from the original archive.[9]
- **Provenance/dedup:** strong source priority and display/write dedup, but no project-equivalent `duplicate_links` graph with confidence and resolution history.
- **License:** current releases are PolyForm Noncommercial 1.0.0; only releases through 1.15.18 remain AGPL-3.0. Neither line is suitable source material for the recommended Apache-2.0 core without separate permission or a deliberate copyleft decision.[3]

### Allos

- **Maturity and activity:** very active but pre-1.0, with no tagged releases at review time. It is a large TypeScript/Next.js/SQLite application with strong tests, broad manual health tracking, labs, FHIR, exports, Oura, Withings, and Android Health Connect.[4]
- **Oura:** the reviewed implementation asks for a personal access token rather than OAuth2. That cannot be the new-user path after Oura's December 2025 PAT deprecation.[10]
- **Apple Health/Eight Sleep:** no implemented Apple HealthKit ingestion endpoint; the mobile companion document is a draft. No direct Eight Sleep connector.
- **Raw/replay:** provider pulls write linked raw debug payloads, but each payload is capped at 512 KiB, only the newest 50 per source are retained, and backups exclude raw provider payloads. This is useful diagnostics, not immutable replay.[11]
- **Provenance/dedup:** detailed per-row sync provenance, edit locks, tombstones, natural-key upserts and user review for ambiguous merges are strong reference designs.
- **License:** AGPL-3.0 requires source availability for modified network-served versions; reusing its code would make a permissive core or proprietary hosted layer much harder.[4][12]

### Focused libraries

- **`oura-ring` 1.0.1:** adopt as a normal dependency, pinned by lockfile. It is current, small, MIT, Python 3.12+, typed, and has mocked OAuth/pagination/endpoint tests. It intentionally leaves token persistence and automatic refresh to the caller, which is the correct boundary for our secret store and ingestion runner.[1]
- **`python_withings_api` 2.4.0:** do not adopt. It is MIT and has a substantial test suite, but its last push was in April 2022, targets Python 3.6-3.8 tooling, and its tests do not run in the current environment without old dependencies.[13]
- **Withings' official Python OAuth sample:** reference only; its own README says it has no error handling or tests and is not for production.[14]
- **Apple Health parser samples:** no focused, current Python library met the coverage/provenance bar. Implement the narrow streaming parser in the plan, optionally using attributed MIT maps from Open Wearables.

## Exact reuse boundary

### Oura

Depend on `oura-ring==1.0.1` (resolved and hash-pinned by `uv.lock`):

- `oura_ring.auth.OuraAuth` for authorization URL, code exchange and refresh calls;
- `oura_ring.client.OuraClient` for pagination and Oura v2 resources;
- upstream mocked cases as behavioral guidance, supplemented by project contract tests.

The project owns: encrypted credentials, atomic persistence of rotated refresh tokens, retry/rate-limit policy, overlap windows, immutable capture of each response page before parsing, raw manifests/SHA-256, normalization, external IDs, dedup, and replay.

### Withings

Port the smallest useful seams from Open Wearables, pinned to `802862fa1ac08f165a897cb9adc4f4b312b08e5c` until the first upstream-sync review:

- `backend/app/services/providers/withings/oauth.py`;
- `backend/app/services/providers/withings/handlers/rpc_client.py` and relevant response schemas;
- endpoint coverage/mapping definitions in `coverage.py`, `data_247.py`, and `workouts.py`;
- the corresponding tests and fixtures, rewritten against the local connector contract.

The project owns: storage, retries, cursors, 72-hour overlap, raw envelopes, page manifests, `grpid` preservation for blood pressure, canonical normalization, duplicate links, and replay.

### Apple Health

Own the streaming importer. If code is borrowed, limit it to type/unit maps and stateless parsing routines from Open Wearables' `apple/apple_xml/xml_service.py`; retain the full archive plus a content hash and generate deterministic identities from HealthKit fields.

Every copied or substantially adapted MIT file must retain its upstream copyright/license notice, exact source path and commit in `NOTICE`, and an upstream-sync owner/date.

## Expected engineering impact

These are planning estimates, not observed delivery data:

| Area | Greenfield estimate | Reuse/adaptation estimate | Avoided |
|---|---:|---:|---:|
| Oura transport, OAuth and endpoint pagination | 3-4 engineer-weeks | 1-2 | 2-3 |
| Withings OAuth, RPC envelopes, pagination and endpoint schemas | 4-6 | 2-3 | 2-3 |
| Apple Health parsing/type mapping | 3-5 | 2-4 | 1 |
| **Total** | **10-15** | **5-9** | **5-6 engineer-weeks** |

The estimate intentionally excludes canonical modeling, raw manifests, provenance, cross-source dedup and replay; those are differentiating work that no candidate eliminates.

## Backlog consequences

- **Retain and strengthen:** HOK-2973 through HOK-2976, HOK-2981, HOK-2985 through HOK-2993. The DuckDB schema, immutable raw store, ingestion runner, manual workouts, rollups, dashboard, dedup, labs, Eight Sleep adapter, events and analysis remain project-owned.
- **Rewrite from greenfield to adaptation:** HOK-2977, HOK-2978, HOK-2982, HOK-2983 and HOK-2984.
- **Retain but narrow:** HOK-2988 remains a project-owned streaming importer with optional attributed reuse of stateless Open Wearables parsing maps.
- **Remove:** no existing issue. None is made redundant by reusable code.
- **Add:** upstream dependency/provenance policy and an open-source release-readiness gate.

## Open-source offering recommendation

Make the codebase **open-source-ready now**, but do not add a public launch to the MVP critical path.

The recommended offering is an Apache-2.0 local-first core containing the schema/migrations, immutable raw manifest, connector contracts, normalizers, replay engine, dedup rules, CLI and fixture-only examples. Apache-2.0 is permissive and includes an explicit patent grant; distributions should carry `LICENSE`, `NOTICE`, third-party attribution and retained MIT notices.[15]

Use a provider/plugin boundary so credentials and real health data never ship. Require user-supplied API applications and acceptance of provider terms. Before a public release, add `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, DCO sign-off, support/non-medical disclaimers, secret scanning, dependency/SBOM generation, reproducible fixture-based tests, documented migration compatibility, and a data-export/rebuild smoke test. Keep branding/trademark and hosted-service decisions outside the code license.

Revisit the license only if the business objective changes from broad adoption/integration to preventing unshared hosted forks. In that case evaluate AGPL-3.0 or an open-core/dual-license model with counsel before accepting external contributions; do not mix that decision into connector implementation.

## Guardrails and exit criteria

1. Every connector returns raw pages/envelopes before normalization; ingestion refuses to advance the cursor until raw bytes and their manifest are durable.
2. A fixture replay into an empty DuckDB produces the same canonical rows and hashes.
3. Vendor IDs, source/device fields, import/run IDs and transform versions survive normalization.
4. Source priority controls presentation, never destructive deletion; ambiguous matches create duplicate-link candidates.
5. A dependency inventory records package/repository, version/commit, license, copied paths, local changes, owner and review date.
6. If adapting an upstream component costs more than roughly half of a clean implementation or requires its persistence/runtime stack, replace it with a small local implementation behind the same contract.

## Sources

[1] [`oura-ring` repository, MIT license, Python requirements and tests](https://github.com/hedgertronic/oura-ring/tree/691dc2e75e97b976772c6bed80ad6936e3dee438)

[2] [Open Wearables repository at reviewed commit](https://github.com/the-momentum/open-wearables/tree/802862fa1ac08f165a897cb9adc4f4b312b08e5c) and [MIT license](https://github.com/the-momentum/open-wearables/blob/802862fa1ac08f165a897cb9adc4f4b312b08e5c/LICENSE)

[3] [HealthLog current license and licensing note](https://github.com/MBombeck/HealthLog/blob/036720bcfb08907a2138c4e6b9b4cbd87134ddb0/LICENSE)

[4] [Allos repository at reviewed commit](https://github.com/FloorLamp/allos/tree/7bd1753bfb6cb14882761faae7c4a8db0904f74e) and [AGPL-3.0 license](https://github.com/FloorLamp/allos/blob/7bd1753bfb6cb14882761faae7c4a8db0904f74e/LICENSE)

[5] [Open Wearables raw-payload storage](https://github.com/the-momentum/open-wearables/blob/802862fa1ac08f165a897cb9adc4f4b312b08e5c/backend/app/services/raw_payload_storage.py) and [deployment topology](https://github.com/the-momentum/open-wearables/blob/802862fa1ac08f165a897cb9adc4f4b312b08e5c/docker-compose.yml)

[6] [Open Wearables Oura provider](https://github.com/the-momentum/open-wearables/tree/802862fa1ac08f165a897cb9adc4f4b312b08e5c/backend/app/services/providers/oura)

[7] [Open Wearables Withings provider](https://github.com/the-momentum/open-wearables/tree/802862fa1ac08f165a897cb9adc4f4b312b08e5c/backend/app/services/providers/withings)

[8] [HealthLog Apple Health import documentation](https://github.com/MBombeck/HealthLog/blob/036720bcfb08907a2138c4e6b9b4cbd87134ddb0/docs/integrations/apple-health.md)

[9] [HealthLog Apple Health worker lifecycle](https://github.com/MBombeck/HealthLog/blob/036720bcfb08907a2138c4e6b9b4cbd87134ddb0/src/lib/jobs/apple-health-import-worker.ts)

[10] [`oura-ring` OAuth2/PAT migration guidance](https://github.com/hedgertronic/oura-ring/blob/691dc2e75e97b976772c6bed80ad6936e3dee438/README.md)

[11] [Allos raw retention/cap implementation](https://github.com/FloorLamp/allos/blob/7bd1753bfb6cb14882761faae7c4a8db0904f74e/lib/integrations/raw-log-format.ts) and [backup exclusions](https://github.com/FloorLamp/allos/blob/7bd1753bfb6cb14882761faae7c4a8db0904f74e/docs/backups.md)

[12] [GNU AGPL-3.0, section 13](https://www.gnu.org/licenses/agpl-3.0.en.html#section13)

[13] [`python_withings_api` at reviewed commit](https://github.com/vangorra/python_withings_api/tree/69c21c32449b0900b4837c1299dde0611a0b8c87)

[14] [Withings official Python OAuth sample](https://github.com/withings-sas/api-oauth2-python)

[15] [Apache License 2.0 terms and patent grant](https://www.apache.org/licenses/LICENSE-2.0.html) and [application/NOTICE guidance](https://www.apache.org/legal/apply-license.html)
