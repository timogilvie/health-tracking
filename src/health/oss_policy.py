"""Validation for deliberate open-source reuse and attribution."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

APPROVED_LICENSES = {
    "Apache-2.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "MIT",
}
PROHIBITED_LICENSE_PREFIXES = ("AGPL", "PolyForm")
COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
REQUIRED_FIELDS = {
    "name",
    "kind",
    "repository",
    "license",
    "usage",
    "copied_paths",
    "local_changes",
    "owner",
    "next_review",
    "upstream_sync",
}


class PolicyError(RuntimeError):
    """Raised when the repository's OSS policy is incomplete or unsafe."""


@dataclass(frozen=True, slots=True)
class PolicyReport:
    entries: int
    packages_verified: int
    source_references_verified: int


def _load_mapping(path: Path) -> dict[str, Any]:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise PolicyError(f"cannot load {path.name}: {exc}") from exc
    if not isinstance(value, dict):
        raise PolicyError(f"{path.name} must contain a mapping")
    return value


def _locked_packages(path: Path) -> dict[str, list[dict[str, Any]]]:
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise PolicyError(f"cannot load uv.lock: {exc}") from exc
    packages: dict[str, list[dict[str, Any]]] = {}
    for package in document.get("package", []):
        if isinstance(package, dict) and isinstance(package.get("name"), str):
            packages.setdefault(package["name"], []).append(package)
    return packages


def _exact_direct_dependencies(path: Path) -> set[tuple[str, str]]:
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise PolicyError(f"cannot load pyproject.toml: {exc}") from exc
    dependencies = document.get("project", {}).get("dependencies", [])
    exact: set[tuple[str, str]] = set()
    for requirement in dependencies:
        if isinstance(requirement, str) and "==" in requirement:
            name, version = requirement.split("==", maxsplit=1)
            exact.add((name.strip().lower().replace("_", "-"), version.strip()))
    return exact


def _artifacts_are_hashed(package: dict[str, Any]) -> bool:
    artifacts: list[dict[str, Any]] = []
    sdist = package.get("sdist")
    if isinstance(sdist, dict):
        artifacts.append(sdist)
    wheels = package.get("wheels", [])
    if isinstance(wheels, list):
        artifacts.extend(item for item in wheels if isinstance(item, dict))
    return bool(artifacts) and all(
        isinstance(artifact.get("hash"), str) and artifact["hash"].startswith("sha256:")
        for artifact in artifacts
    )


def _validate_entry(entry: dict[str, Any], notice: str) -> tuple[bool, bool]:
    missing = sorted(REQUIRED_FIELDS - entry.keys())
    if missing:
        raise PolicyError(f"entry {entry.get('name', '<unnamed>')} missing fields: {missing}")
    name = entry["name"]
    if not isinstance(name, str) or not name:
        raise PolicyError("inventory entry name must be non-empty")

    license_id = entry["license"]
    if not isinstance(license_id, str):
        raise PolicyError(f"{name}: license must be an SPDX string")
    if license_id.startswith(PROHIBITED_LICENSE_PREFIXES):
        raise PolicyError(f"{name}: prohibited license for permissive core: {license_id}")
    if license_id not in APPROVED_LICENSES:
        raise PolicyError(f"{name}: unreviewed license: {license_id}")
    repository = entry["repository"]
    if not isinstance(repository, str) or not repository.startswith("https://"):
        raise PolicyError(f"{name}: repository must be an HTTPS URL")

    reviewed_commit = entry.get("reviewed_commit")
    if reviewed_commit is not None and (
        not isinstance(reviewed_commit, str) or not COMMIT_PATTERN.fullmatch(reviewed_commit)
    ):
        raise PolicyError(f"{name}: reviewed_commit must be a full lowercase SHA-1")

    next_review = entry["next_review"]
    if not isinstance(next_review, date | str):
        raise PolicyError(f"{name}: next_review must be an ISO date")
    if isinstance(next_review, str):
        try:
            date.fromisoformat(next_review)
        except ValueError as exc:
            raise PolicyError(f"{name}: next_review must be an ISO date") from exc

    copied_paths = entry["copied_paths"]
    if not isinstance(copied_paths, list) or not all(
        isinstance(path, str)
        and path
        and not Path(path).is_absolute()
        and ".." not in Path(path).parts
        for path in copied_paths
    ):
        raise PolicyError(f"{name}: copied_paths must contain safe relative paths")
    if copied_paths and name.lower() not in notice.lower():
        raise PolicyError(f"{name}: copied code requires an attribution in NOTICE")

    upstream = entry["upstream_sync"]
    if not isinstance(upstream, dict) or not all(
        isinstance(upstream.get(field), str) and upstream[field].strip()
        for field in ("strategy", "rollback")
    ):
        raise PolicyError(f"{name}: upstream_sync requires strategy and rollback")

    kind = entry["kind"]
    if kind == "package":
        if not isinstance(entry.get("version"), str) or not entry["version"]:
            raise PolicyError(f"{name}: package requires an exact version")
        return True, False
    if kind == "source-reference":
        if reviewed_commit is None:
            raise PolicyError(f"{name}: source reference requires reviewed_commit")
        return False, True
    raise PolicyError(f"{name}: unknown inventory kind: {kind!r}")


def validate_repository_policy(root: Path) -> PolicyReport:
    """Validate provenance, licensing, attribution, and lockfile alignment."""

    inventory = _load_mapping(root / "THIRD_PARTY.yml")
    entries = inventory.get("dependencies")
    if inventory.get("schema_version") != 1 or not isinstance(entries, list) or not entries:
        raise PolicyError("THIRD_PARTY.yml requires schema_version 1 and dependency entries")
    notice = (root / "NOTICE").read_text(encoding="utf-8")
    locked = _locked_packages(root / "uv.lock")
    exact_direct = _exact_direct_dependencies(root / "pyproject.toml")

    packages_verified = 0
    source_references_verified = 0
    names: set[str] = set()
    for value in entries:
        if not isinstance(value, dict):
            raise PolicyError("every dependency entry must be a mapping")
        package, source_reference = _validate_entry(value, notice)
        name = value["name"]
        if name in names:
            raise PolicyError(f"duplicate dependency entry: {name}")
        names.add(name)
        if package:
            version = value["version"]
            matches = [
                candidate
                for candidate in locked.get(name, [])
                if candidate.get("version") == version
            ]
            if not matches:
                raise PolicyError(f"{name}=={version} is not present in uv.lock")
            if not any(_artifacts_are_hashed(candidate) for candidate in matches):
                raise PolicyError(f"{name}=={version} has no complete SHA-256 artifact pins")
            normalized = name.lower().replace("_", "-")
            if (normalized, version) not in exact_direct:
                raise PolicyError(f"{name}=={version} is not an exact direct dependency")
            packages_verified += 1
        if source_reference:
            source_references_verified += 1

    return PolicyReport(
        entries=len(entries),
        packages_verified=packages_verified,
        source_references_verified=source_references_verified,
    )
