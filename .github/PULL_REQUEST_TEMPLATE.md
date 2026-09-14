## Summary

- 

## Validation

- [ ] `uv run ruff check .`
- [ ] `uv run health policy-check`
- [ ] `uv run health privacy-check`
- [ ] `uv run pytest --cov=health --cov-report=term-missing`
- [ ] `bash scripts/clean-clone-smoke.sh` when storage, replay, migrations, or exports change

## Privacy, security, and compatibility

- [ ] Fixtures are synthetic from inception and contain no identifying metadata.
- [ ] No credentials, provider exports, databases, measurements, or private logs are included.
- [ ] New dependencies or adapted code are recorded in `THIRD_PARTY.yml` and `NOTICE`.
- [ ] Migration, raw-manifest, CLI, and export compatibility impacts are documented.
- [ ] The commits include a human `Signed-off-by` trailer for the DCO.
