"""Immutable, content-verified storage for source payloads."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path
from typing import Any
from uuid import uuid4

from health.connectors import RawPage

SOURCE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
SECRET_KEYS = {
    "authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "api-key",
    "access_token",
    "refresh_token",
    "client_secret",
}


class RawStorageError(RuntimeError):
    """Base error for raw artifact storage."""


class RawIntegrityError(RawStorageError):
    """Raised when a raw artifact no longer matches its manifest."""


@dataclass(frozen=True, slots=True)
class RawRef:
    """Portable path to a payload relative to the raw-store root."""

    value: str

    def __str__(self) -> str:
        return self.value


def _redact(value: Any, *, key: str | None = None) -> Any:
    if key is not None and key.lower() in SECRET_KEYS:
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(child): _redact(item, key=str(child)) for child, item in value.items()}
    if isinstance(value, list | tuple):
        return [_redact(item) for item in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _endpoint_slug(endpoint: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", endpoint).strip("-").lower()
    return (slug or "payload")[:80]


def _extension(content_type: str) -> str:
    normalized = content_type.partition(";")[0].strip().lower()
    return {
        "application/json": ".json",
        "application/xml": ".xml",
        "text/xml": ".xml",
        "application/zip": ".zip",
    }.get(normalized, ".bin")


def _write_exclusive_atomic(path: Path, content: bytes) -> None:
    """Publish a fully flushed file without ever replacing an existing path."""

    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            os.chmod(temporary, 0o600)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    except FileExistsError as exc:
        raise RawStorageError(f"raw artifact already exists: {path}") from exc
    finally:
        temporary.unlink(missing_ok=True)


class RawStore:
    """Append-only payload store with sidecar integrity manifests."""

    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)

    def save(
        self,
        page: RawPage,
        *,
        ingestion_run_id: str | None,
        transform_version: str,
        parent_archive: RawRef | None = None,
    ) -> RawRef:
        """Durably publish payload and manifest, returning only after both exist."""

        if not SOURCE_PATTERN.fullmatch(page.source):
            raise RawStorageError(f"invalid source name: {page.source!r}")
        retrieved = page.retrieved_at.astimezone(UTC)
        directory = (
            self.root
            / page.source
            / f"{retrieved:%Y}"
            / f"{retrieved:%m}"
            / f"{retrieved:%d}"
        )
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(directory, 0o700)

        identity = uuid4().hex
        timestamp = retrieved.strftime("%Y%m%dT%H%M%S.%fZ")
        filename = (
            f"{_endpoint_slug(page.endpoint)}__{timestamp}__{identity}"
            f"{_extension(page.content_type)}"
        )
        payload_path = directory / filename
        manifest_path = directory / f"{filename}.manifest.json"
        raw_ref = RawRef(payload_path.relative_to(self.root).as_posix())
        digest = hashlib.sha256(page.content).hexdigest()
        manifest = {
            "schema_version": 1,
            "raw_ref": raw_ref.value,
            "sha256": digest,
            "byte_length": len(page.content),
            "source": page.source,
            "endpoint": page.endpoint,
            "retrieved_at": retrieved.isoformat().replace("+00:00", "Z"),
            "content_type": page.content_type,
            "http_status": page.http_status,
            "response_headers": _redact(page.response_headers),
            "request_metadata": _redact(page.request_metadata),
            "ingestion_run_id": ingestion_run_id,
            "transform_version": transform_version,
            "parent_archive": parent_archive.value if parent_archive else None,
        }
        manifest_bytes = (
            json.dumps(manifest, indent=2, sort_keys=True, separators=(",", ": "))
            + "\n"
        ).encode()

        _write_exclusive_atomic(payload_path, page.content)
        try:
            _write_exclusive_atomic(manifest_path, manifest_bytes)
        except Exception:
            payload_path.unlink(missing_ok=True)
            raise
        return raw_ref

    def _path(self, raw_ref: RawRef) -> Path:
        relative = Path(raw_ref.value)
        if relative.is_absolute() or ".." in relative.parts:
            raise RawStorageError(f"raw reference escapes store: {raw_ref}")
        path = (self.root / relative).resolve()
        if not path.is_relative_to(self.root):
            raise RawStorageError(f"raw reference escapes store: {raw_ref}")
        return path

    def manifest(self, raw_ref: RawRef) -> dict[str, Any]:
        path = self._path(raw_ref)
        manifest_path = path.with_name(f"{path.name}.manifest.json")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RawIntegrityError(f"cannot read manifest for {raw_ref}: {exc}") from exc
        if not isinstance(manifest, dict) or manifest.get("raw_ref") != raw_ref.value:
            raise RawIntegrityError(f"manifest identity mismatch for {raw_ref}")
        return manifest

    def read(self, raw_ref: RawRef) -> bytes:
        """Read exact bytes after verifying length and SHA-256."""

        path = self._path(raw_ref)
        manifest = self.manifest(raw_ref)
        try:
            content = path.read_bytes()
        except OSError as exc:
            raise RawIntegrityError(f"cannot read payload {raw_ref}: {exc}") from exc
        digest = hashlib.sha256(content).hexdigest()
        if len(content) != manifest.get("byte_length") or digest != manifest.get("sha256"):
            raise RawIntegrityError(f"payload integrity check failed for {raw_ref}")
        return content

    def iter_refs(self, source: str | None = None) -> Iterator[RawRef]:
        """Yield committed artifacts in lexical path order."""

        if source is not None and not SOURCE_PATTERN.fullmatch(source):
            raise RawStorageError(f"invalid source name: {source!r}")
        base = self.root / source if source else self.root
        if not base.exists():
            return
        suffix = ".manifest.json"
        for manifest_path in sorted(base.rglob(f"*{suffix}")):
            payload_path = manifest_path.with_name(manifest_path.name.removesuffix(suffix))
            yield RawRef(payload_path.relative_to(self.root).as_posix())
