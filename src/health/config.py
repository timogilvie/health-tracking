"""Typed settings and checked YAML configuration loading."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class HealthSettings(BaseSettings):
    """Runtime paths, overridable through `HEALTH_*` environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="HEALTH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    project_root: Path = Field(default_factory=Path.cwd)
    database_path: Path = Path("data/health.duckdb")
    config_dir: Path = Path("config")
    raw_dir: Path = Path("data/raw")
    export_dir: Path = Path("data/exports")
    snapshot_dir: Path = Path("data/snapshots")
    secret_dir: Path = Path("data/secrets")
    withings_client_id: str = ""
    withings_client_secret: SecretStr = SecretStr("")
    withings_redirect_uri: str = ""
    withings_scope: str = "user.metrics"
    oura_client_id: str = ""
    oura_client_secret: SecretStr = SecretStr("")
    oura_redirect_uri: str = ""
    oura_scope: str = "daily heartrate workout session"

    @field_validator("project_root", mode="before")
    @classmethod
    def expand_project_root(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()

    def resolve(self, value: Path) -> Path:
        """Resolve a configured path against the project root."""

        return value.expanduser().resolve() if value.is_absolute() else self.project_root / value

    @property
    def database(self) -> Path:
        return self.resolve(self.database_path)

    @property
    def configs(self) -> Path:
        return self.resolve(self.config_dir)

    @property
    def raw(self) -> Path:
        return self.resolve(self.raw_dir)

    @property
    def exports(self) -> Path:
        return self.resolve(self.export_dir)

    @property
    def snapshots(self) -> Path:
        return self.resolve(self.snapshot_dir)

    @property
    def secrets(self) -> Path:
        return self.resolve(self.secret_dir)


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML mapping and reject ambiguous top-level values."""

    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Cannot read configuration {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in {path}: {exc}") from exc

    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Configuration must be a mapping: {path}")
    return value


def load_project_config(settings: HealthSettings) -> dict[str, dict[str, Any]]:
    """Load every required project configuration file."""

    return {
        name: load_yaml(settings.configs / f"{name}.yaml")
        for name in ("settings", "metrics", "source_priority")
    }
