from pathlib import Path, PurePosixPath

import pytest
from typer.testing import CliRunner

from health.cli import app
from health.privacy_policy import PrivacyPolicyError, validate_repository_privacy


def _validate(tmp_path: Path, relative_path: str, content: bytes = b"synthetic") -> None:
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    validate_repository_privacy(
        tmp_path,
        tracked_paths=[PurePosixPath(relative_path)],
    )


def test_repository_privacy_check_passes(project_root: Path) -> None:
    report = validate_repository_privacy(project_root)

    assert report.tracked_files > 0
    assert report.text_files_scanned > 0


def test_privacy_check_cli(project_root: Path) -> None:
    result = CliRunner().invoke(app, ["privacy-check", "--root", str(project_root)])

    assert result.exit_code == 0, result.output
    assert "PASS privacy:" in result.output


def test_private_data_directory_content_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(PrivacyPolicyError, match="private data/ content"):
        _validate(tmp_path, "data/Bloodwork.pdf")


def test_health_artifact_outside_synthetic_fixtures_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(PrivacyPolicyError, match="health-data artifact"):
        _validate(tmp_path, "downloads/export.zip")


def test_synthetic_health_fixture_is_allowed(tmp_path: Path) -> None:
    synthetic_export = b"".join(
        [
            b"<Health",
            b'Data><Rec',
            b'ord type="HKQua',
            b'ntityTypeIdentifierBodyMass"/></HealthData>',
        ]
    )
    _validate(
        tmp_path,
        "tests/fixtures/providers/apple_health/export.xml",
        synthetic_export,
    )


def test_high_confidence_secret_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(PrivacyPolicyError, match="possible private key"):
        _validate(
            tmp_path,
            "config/credential.txt",
            b"-----BEGIN "
            b"PRIVATE KEY-----\nsynthetic\n-----END PRIVATE KEY-----\n",
        )
