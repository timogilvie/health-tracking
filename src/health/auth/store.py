"""Private, atomic filesystem storage for rotating OAuth credentials."""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import re
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

PROVIDER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


class SecretStoreError(RuntimeError):
    """A credential file is unavailable, corrupt, or insecurely permissioned."""


class OAuthStateError(SecretStoreError):
    """An OAuth state value is missing, expired, or does not match."""


@dataclass(frozen=True, slots=True)
class OAuthTokenPair:
    """An access/refresh pair whose representation never reveals credentials."""

    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expires_at: datetime
    token_type: str = "Bearer"
    scope: str | None = None
    provider_user_id: str | None = None

    def __post_init__(self) -> None:
        if not self.access_token or not self.refresh_token:
            raise ValueError("OAuth access and refresh tokens must be non-empty")
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("OAuth token expiry must be timezone-aware")

    def to_document(self) -> dict[str, Any]:
        document = asdict(self)
        document["expires_at"] = self.expires_at.astimezone(UTC).isoformat()
        return document

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> OAuthTokenPair:
        try:
            expires_at = datetime.fromisoformat(str(document["expires_at"]))
            return cls(
                access_token=str(document["access_token"]),
                refresh_token=str(document["refresh_token"]),
                expires_at=expires_at,
                token_type=str(document.get("token_type", "Bearer")),
                scope=_optional_string(document.get("scope")),
                provider_user_id=_optional_string(document.get("provider_user_id")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SecretStoreError("credential file contains an invalid token document") from exc


def _optional_string(value: Any) -> str | None:
    return str(value) if value is not None else None


class LockedProviderSecrets:
    """Provider-scoped operations while the cross-process lock is held."""

    def __init__(self, store: FileSecretStore, provider: str) -> None:
        self._store = store
        self._provider = provider

    def load_tokens(self) -> OAuthTokenPair | None:
        return self._store._load_tokens(self._provider)

    def save_tokens(self, tokens: OAuthTokenPair) -> None:
        self._store._write_json(self._store._token_path(self._provider), tokens.to_document())


class FileSecretStore:
    """A local secret store with private permissions and atomic pair replacement."""

    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chmod(self.root, 0o700)

    @staticmethod
    def _validate_provider(provider: str) -> str:
        if not PROVIDER_PATTERN.fullmatch(provider):
            raise ValueError(f"invalid provider name: {provider!r}")
        return provider

    def _token_path(self, provider: str) -> Path:
        return self.root / f"{self._validate_provider(provider)}.tokens.json"

    def _state_path(self, provider: str) -> Path:
        return self.root / f"{self._validate_provider(provider)}.state.json"

    def _lock_path(self, provider: str) -> Path:
        return self.root / f"{self._validate_provider(provider)}.lock"

    @contextmanager
    def locked(self, provider: str) -> Iterator[LockedProviderSecrets]:
        """Serialize a provider operation across threads and processes."""

        path = self._lock_path(provider)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield LockedProviderSecrets(self, provider)
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def load_tokens(self, provider: str) -> OAuthTokenPair | None:
        with self.locked(provider) as locked:
            return locked.load_tokens()

    def save_tokens(self, provider: str, tokens: OAuthTokenPair) -> None:
        with self.locked(provider) as locked:
            locked.save_tokens(tokens)

    def _load_tokens(self, provider: str) -> OAuthTokenPair | None:
        path = self._token_path(provider)
        if not path.exists():
            return None
        self._require_private_file(path)
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SecretStoreError("credential file cannot be read or decoded") from exc
        if not isinstance(document, dict):
            raise SecretStoreError("credential file must contain a JSON object")
        return OAuthTokenPair.from_document(document)

    def save_pending_state(
        self,
        provider: str,
        state_value: str,
        *,
        issued_at: datetime,
    ) -> None:
        if not state_value:
            raise ValueError("OAuth state must be non-empty")
        if issued_at.tzinfo is None or issued_at.utcoffset() is None:
            raise ValueError("OAuth state timestamp must be timezone-aware")
        document = {
            "state_sha256": hashlib.sha256(state_value.encode()).hexdigest(),
            "issued_at": issued_at.astimezone(UTC).isoformat(),
        }
        with self.locked(provider):
            self._write_json(self._state_path(provider), document)

    def consume_pending_state(
        self,
        provider: str,
        state_value: str,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("OAuth state validation time must be timezone-aware")
        with self.locked(provider):
            path = self._state_path(provider)
            if not path.exists():
                raise OAuthStateError("OAuth state is missing; start authorization again")
            self._require_private_file(path)
            try:
                document = json.loads(path.read_text(encoding="utf-8"))
                expected = str(document["state_sha256"])
                issued_at = datetime.fromisoformat(str(document["issued_at"]))
                if issued_at.tzinfo is None or issued_at.utcoffset() is None:
                    raise ValueError("OAuth state timestamp is naive")
            except (OSError, json.JSONDecodeError, KeyError, ValueError) as exc:
                raise OAuthStateError(
                    "OAuth state file is invalid; start authorization again"
                ) from exc
            supplied = hashlib.sha256(state_value.encode()).hexdigest()
            if not hmac.compare_digest(expected, supplied):
                raise OAuthStateError("OAuth state does not match")
            if issued_at.tzinfo is None or now - issued_at > ttl or now < issued_at:
                raise OAuthStateError("OAuth state expired; start authorization again")
            path.unlink()
            self._fsync_directory()

    @staticmethod
    def _require_private_file(path: Path) -> None:
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
        except OSError as exc:
            raise SecretStoreError("credential file metadata cannot be read") from exc
        if mode & 0o077:
            raise SecretStoreError("credential file permissions must be 0600")

    def _write_json(self, destination: Path, document: dict[str, Any]) -> None:
        payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        descriptor, temporary_name = tempfile.mkstemp(
            dir=self.root,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
            self._fsync_directory()
        finally:
            temporary.unlink(missing_ok=True)

    def _fsync_directory(self) -> None:
        descriptor = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
