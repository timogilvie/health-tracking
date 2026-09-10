from pathlib import Path
from shutil import copy

import pytest
from typer.testing import CliRunner

from health.cli import app
from health.oss_policy import PolicyError, validate_repository_policy


def policy_root(tmp_path: Path, project_root: Path) -> Path:
    for filename in ("THIRD_PARTY.yml", "NOTICE", "uv.lock", "pyproject.toml"):
        copy(project_root / filename, tmp_path / filename)
    return tmp_path


def test_repository_policy_matches_lockfile(project_root: Path) -> None:
    report = validate_repository_policy(project_root)

    assert report.entries == 2
    assert report.packages_verified == 1
    assert report.source_references_verified == 1


def test_policy_check_cli(project_root: Path) -> None:
    result = CliRunner().invoke(app, ["policy-check", "--root", str(project_root)])

    assert result.exit_code == 0, result.output
    assert "PASS policy: 2 entries" in result.output


def test_policy_rejects_prohibited_license(tmp_path: Path, project_root: Path) -> None:
    root = policy_root(tmp_path, project_root)
    inventory = root / "THIRD_PARTY.yml"
    inventory.write_text(
        inventory.read_text().replace("license: MIT", "license: AGPL-3.0", 1),
        encoding="utf-8",
    )

    with pytest.raises(PolicyError, match="prohibited license"):
        validate_repository_policy(root)


def test_policy_requires_full_commit(tmp_path: Path, project_root: Path) -> None:
    root = policy_root(tmp_path, project_root)
    inventory = root / "THIRD_PARTY.yml"
    inventory.write_text(
        inventory.read_text().replace(
            "691dc2e75e97b976772c6bed80ad6936e3dee438", "691dc2e"
        ),
        encoding="utf-8",
    )

    with pytest.raises(PolicyError, match="full lowercase SHA-1"):
        validate_repository_policy(root)


def test_copied_code_requires_notice_attribution(
    tmp_path: Path, project_root: Path
) -> None:
    root = policy_root(tmp_path, project_root)
    inventory = root / "THIRD_PARTY.yml"
    inventory.write_text(
        inventory.read_text().replace(
            "copied_paths: []",
            "copied_paths: [src/health/connectors/oura.py]",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(PolicyError, match="requires an attribution in NOTICE"):
        validate_repository_policy(root)
