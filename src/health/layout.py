"""Safe project-directory initialization."""

from __future__ import annotations

import os
from pathlib import Path

from health.config import HealthSettings


def initialize_layout(settings: HealthSettings) -> list[Path]:
    """Create runtime directories without touching existing data."""

    created: list[Path] = []
    for path in (
        settings.database.parent,
        settings.raw,
        settings.exports,
        settings.snapshots,
        settings.secrets,
    ):
        if not path.exists():
            path.mkdir(parents=True, mode=0o700)
            created.append(path)
        if path.is_dir():
            os.chmod(path, 0o700)
    return created
