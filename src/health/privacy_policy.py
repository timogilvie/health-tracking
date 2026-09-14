"""Repository guardrails against committing credentials or personal health data."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

ALLOWED_DATA_FILES = {
    "data/exports/.gitkeep",
    "data/raw/.gitkeep",
    "data/secrets/.gitkeep",
    "data/snapshots/.gitkeep",
}
PRIVATE_ARTIFACT_SUFFIXES = {
    ".csv",
    ".db",
    ".dcm",
    ".duckdb",
    ".fit",
    ".gpx",
    ".gz",
    ".key",
    ".p12",
    ".parquet",
    ".pdf",
    ".pem",
    ".pfx",
    ".sqlite",
    ".sqlite3",
    ".tar",
    ".tsv",
    ".xls",
    ".xlsx",
    ".xml",
    ".zip",
}
PRIVATE_FILENAMES = {
    ".env",
    "credentials.json",
    "export.xml",
    "export.zip",
}
HIGH_CONFIDENCE_SECRET_PATTERNS = {
    "private key": re.compile(rb"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"),
    "GitHub token": re.compile(rb"\b(?:ghp|github_pat)_[A-Za-z0-9_]{20,}\b"),
    "OpenAI-style token": re.compile(rb"\bsk-[A-Za-z0-9_-]{20,}\b"),
}
_HEALTHKIT_PREFIX = rb"HK"
HEALTH_EXPORT_PATTERNS = {
    "Apple Health export": re.compile(
        rb"<(?:HealthData|Record)\b[^>]*\b"
        + _HEALTHKIT_PREFIX
        + rb"(?:Quantity|Category|Workout)"
    ),
    "Apple Health personal profile": re.compile(
        _HEALTHKIT_PREFIX + rb"CharacteristicTypeIdentifierDateOfBirth"
    ),
}


class PrivacyPolicyError(RuntimeError):
    """Raised when tracked repository content may contain secrets or health data."""


@dataclass(frozen=True, slots=True)
class PrivacyPolicyReport:
    tracked_files: int
    text_files_scanned: int
    history_blobs_scanned: int


def _tracked_files(root: Path) -> tuple[PurePosixPath, ...]:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"],
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise PrivacyPolicyError(f"cannot inspect tracked files: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise PrivacyPolicyError(f"cannot inspect tracked files: {detail or 'git failed'}")
    return tuple(
        PurePosixPath(value.decode("utf-8", errors="surrogateescape"))
        for value in result.stdout.split(b"\0")
        if value
    )


def _is_fixture(path: PurePosixPath) -> bool:
    return path.parts[:2] == ("tests", "fixtures")


def _path_violation(path: PurePosixPath) -> str | None:
    value = path.as_posix()
    if path.parts and path.parts[0] == "data" and value not in ALLOWED_DATA_FILES:
        return "private data/ content is tracked"
    if path.parts[:3] == ("tests", "fixtures", "private"):
        return "private test fixture is tracked"

    name = path.name.lower()
    if name.startswith(".env") and name != ".env.example":
        return "environment file is tracked"
    if name.endswith(".tokens.json") or name.startswith("client_secret"):
        return "credential file is tracked"
    if not _is_fixture(path) and (
        name in PRIVATE_FILENAMES
        or any(name.endswith(suffix) for suffix in PRIVATE_ARTIFACT_SUFFIXES)
    ):
        return "possible provider export or health-data artifact is tracked outside tests/fixtures"
    return None


def _git(root: Path, *args: str, input_bytes: bytes | None = None) -> bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *args],
            input=input_bytes,
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise PrivacyPolicyError(f"cannot inspect Git history: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise PrivacyPolicyError(f"cannot inspect Git history: {detail or 'git failed'}")
    return result.stdout


def _history_blobs(root: Path) -> tuple[tuple[PurePosixPath, bytes], ...]:
    listed = _git(root, "rev-list", "--objects", "--all")
    paths_by_oid: dict[bytes, set[PurePosixPath]] = {}
    for line in listed.splitlines():
        oid, separator, encoded_path = line.partition(b" ")
        if not separator or not encoded_path:
            continue
        path = PurePosixPath(encoded_path.decode("utf-8", errors="surrogateescape"))
        paths_by_oid.setdefault(oid, set()).add(path)
    if not paths_by_oid:
        return ()

    oids = tuple(paths_by_oid)
    checked = _git(
        root,
        "cat-file",
        "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        input_bytes=b"\n".join(oids) + b"\n",
    )
    blob_oids: list[bytes] = []
    for line in checked.splitlines():
        oid, object_type, _size = line.split(b" ", 2)
        if object_type == b"blob":
            blob_oids.append(oid)
    if not blob_oids:
        return ()

    batch = _git(
        root,
        "cat-file",
        "--batch",
        input_bytes=b"\n".join(blob_oids) + b"\n",
    )
    offset = 0
    blobs: list[tuple[PurePosixPath, bytes]] = []
    for expected_oid in blob_oids:
        header_end = batch.find(b"\n", offset)
        if header_end < 0:
            raise PrivacyPolicyError("cannot parse Git history blob header")
        oid, object_type, encoded_size = batch[offset:header_end].split(b" ", 2)
        if oid != expected_oid or object_type != b"blob":
            raise PrivacyPolicyError("Git history blob identity mismatch")
        size = int(encoded_size)
        content_start = header_end + 1
        content_end = content_start + size
        content = batch[content_start:content_end]
        if len(content) != size:
            raise PrivacyPolicyError("Git history blob is truncated")
        offset = content_end + 1
        for path in paths_by_oid[oid]:
            blobs.append((path, content))
    return tuple(blobs)


def _content_violations(path: PurePosixPath, content: bytes) -> list[str]:
    if b"\0" in content:
        return []
    violations = [
        f"possible {label}"
        for label, pattern in HIGH_CONFIDENCE_SECRET_PATTERNS.items()
        if pattern.search(content)
    ]
    if not _is_fixture(path):
        violations.extend(
            f"possible {label}"
            for label, pattern in HEALTH_EXPORT_PATTERNS.items()
            if pattern.search(content)
        )
    return violations


def validate_repository_privacy(
    root: Path,
    *,
    tracked_paths: Iterable[PurePosixPath] | None = None,
    include_history: bool | None = None,
) -> PrivacyPolicyReport:
    """Validate tracked files and history without reading ignored personal files."""

    root = root.resolve()
    scan_history = tracked_paths is None if include_history is None else include_history
    paths = tuple(tracked_paths) if tracked_paths is not None else _tracked_files(root)
    violations: list[str] = []
    text_files_scanned = 0

    for path in paths:
        reason = _path_violation(path)
        if reason:
            violations.append(f"{path.as_posix()}: {reason}")
            continue

        absolute = root.joinpath(*path.parts)
        try:
            content = absolute.read_bytes()
        except OSError as exc:
            violations.append(f"{path.as_posix()}: cannot read tracked file: {exc}")
            continue
        if b"\0" in content:
            continue
        text_files_scanned += 1

        violations.extend(
            f"{path.as_posix()}: {reason}"
            for reason in _content_violations(path, content)
        )

    history_blobs = _history_blobs(root) if scan_history else ()
    for path, content in history_blobs:
        reason = _path_violation(path)
        if reason:
            violations.append(f"history:{path.as_posix()}: {reason}")
            continue
        violations.extend(
            f"history:{path.as_posix()}: {detail}"
            for detail in _content_violations(path, content)
        )

    if violations:
        joined = "\n- ".join(violations)
        raise PrivacyPolicyError(f"privacy check failed:\n- {joined}")
    return PrivacyPolicyReport(
        tracked_files=len(paths),
        text_files_scanned=text_files_scanned,
        history_blobs_scanned=len(history_blobs),
    )
