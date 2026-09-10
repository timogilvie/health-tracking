from pathlib import Path

import pytest

from health.config import HealthSettings, load_project_config, load_yaml


def test_settings_resolve_paths_against_root(tmp_path: Path) -> None:
    settings = HealthSettings(project_root=tmp_path)

    assert settings.database == tmp_path / "data" / "health.duckdb"
    assert settings.raw == tmp_path / "data" / "raw"


def test_load_project_config_reads_required_maps(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "settings.yaml").write_text("timezone: UTC\n", encoding="utf-8")
    (config / "metrics.yaml").write_text("metrics: {}\n", encoding="utf-8")
    (config / "source_priority.yaml").write_text("metrics: {}\n", encoding="utf-8")

    loaded = load_project_config(HealthSettings(project_root=tmp_path))

    assert loaded["settings"]["timezone"] == "UTC"


def test_load_yaml_rejects_non_mapping(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("- not\n- a\n- mapping\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a mapping"):
        load_yaml(path)
