# Open-source dependency and upstream-sync policy

This policy implements ADR-0001 and applies to runtime packages, source adaptations, test
fixtures, and connector reference code.

## Inventory

`THIRD_PARTY.yml` is the machine-readable source of truth for deliberately reused components.
The lockfile remains the complete resolved Python dependency inventory. Every entry must record:

- package or repository name and URL;
- exact package version and/or full 40-character reviewed commit;
- SPDX license identifier;
- purpose, copied paths, local changes, owner, and next review date;
- upstream upgrade strategy and a concrete rollback path.

`health policy-check` validates these fields, verifies package versions against `uv.lock`,
rejects prohibited licenses, and requires a NOTICE entry before copied paths can be listed.
CI runs that check without provider credentials.

## License boundary

The core is Apache-2.0. MIT, BSD, ISC, and Apache-2.0 dependencies may be used with their
required notices. Current PolyForm HealthLog and AGPL Allos code must not be copied into this
permissive core. They may inform requirements and black-box behavioral tests only.

When code is copied or substantially adapted:

1. Add exact upstream paths and commit to `THIRD_PARTY.yml`.
2. Preserve the upstream copyright and license header in the affected files.
3. Add an attribution paragraph naming the component to `NOTICE`.
4. Describe local changes; do not use `none` or `none-yet`.
5. Add synthetic conformance fixtures before merging.

This is an engineering policy, not legal advice. Escalate ambiguous licensing to counsel.

## Upgrade workflow

The `health-platform` owner reviews active components quarterly and sooner for relevant security
advisories or provider API changes.

1. Record the proposed upstream version/commit and review its license.
2. Inspect the diff across the exact old/new revisions.
3. Update one provider component at a time.
4. Run `uv lock --check`, `health policy-check`, Ruff, unit tests, connector conformance tests,
   raw replay tests, and the clean database rebuild smoke test.
5. Update the inventory, local-change notes, fixtures, and next review date.
6. Keep the previous lockfile/commit usable until the connector has completed a successful sync
   and replay against non-committed local data.

Rollback restores the prior pinned package/commit and its compatible local adapter. Raw data is
never rolled back or deleted.

## Local patches and replacement threshold

Local patches must remain narrowly scoped behind the connector contract, have an explicit owner,
and include a comment linking the upstream path/commit when code is adapted. Do not import an
upstream persistence model, worker runtime, secret store, or canonical schema.

Replace a dependency with a small local implementation when adaptation is expected to exceed
roughly half the cost of clean implementation, when upstream requires its own persistence/runtime
stack, when its license changes incompatibly, or when it cannot preserve raw-first replay and
provenance. A replacement keeps the same conformance fixtures and connector contract.
