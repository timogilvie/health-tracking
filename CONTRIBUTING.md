# Contributing

Thank you for helping improve Health Tracking. This project accepts focused changes to the
local-first core: schema and migrations, raw manifests, connector contracts, normalizers,
replay, deduplication, CLI tools, portable exports, documentation, and synthetic fixtures.

## Before opening a change

1. Open an issue describing the user problem, scope, privacy impact, and validation plan.
2. Keep provider credentials, application registrations, real health data, exports, databases,
   screenshots, logs containing measurements, and identifying metadata outside the repository.
3. Use only synthetic data under `tests/fixtures/`. Never derive a fixture by redacting a real
   export; construct it from scratch.
4. Record copied or substantially adapted upstream work in `THIRD_PARTY.yml` and `NOTICE`.

## Development checks

```bash
uv sync --locked --python 3.12
uv run ruff check .
uv run health policy-check
uv run health privacy-check
uv run pytest --cov=health --cov-report=term-missing
bash scripts/clean-clone-smoke.sh
```

Provider-network tests must use injected clients and synthetic responses. The clean-clone smoke
test runs without credentials or network access after dependencies are installed.

## Developer Certificate of Origin

Every commit must include a `Signed-off-by` trailer certifying the
[Developer Certificate of Origin 1.1](https://developercertificate.org/):

```text
Signed-off-by: Your Name <your.email@example.com>
```

Use `git commit -s` to add the trailer. By signing off, you certify that you have the right to
submit the contribution under the project's Apache-2.0 license. Do not include the Codex or other
tool identity as the DCO signer; the human or legal entity submitting the contribution signs it.

## Pull requests

Keep changes reviewable, include regression tests, document migrations and compatibility impact,
and complete the privacy/security checklist in the pull-request template. Maintainers may close
requests containing personal data and ask the author to rotate any exposed credential.
