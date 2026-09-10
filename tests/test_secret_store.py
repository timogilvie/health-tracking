import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import health.auth.store as store_module
from health.auth import FileSecretStore, OAuthStateError, OAuthTokenPair, SecretStoreError


def token_pair(*, access: str = "access-one", refresh: str = "refresh-one") -> OAuthTokenPair:
    return OAuthTokenPair(
        access_token=access,
        refresh_token=refresh,
        expires_at=datetime(2026, 9, 10, 16, tzinfo=UTC),
    )


def test_token_pair_round_trip_is_private_and_redacted(tmp_path: Path) -> None:
    store = FileSecretStore(tmp_path / "secrets")
    tokens = token_pair()

    store.save_tokens("withings", tokens)

    assert store.load_tokens("withings") == tokens
    assert "access-one" not in repr(tokens)
    assert "refresh-one" not in repr(tokens)
    assert stat.S_IMODE(store.root.stat().st_mode) == 0o700
    assert stat.S_IMODE((store.root / "withings.tokens.json").stat().st_mode) == 0o600


def test_interrupted_pair_replacement_preserves_previous_tokens(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FileSecretStore(tmp_path / "secrets")
    original = token_pair()
    store.save_tokens("withings", original)

    def interrupt(_source: Path, _destination: Path) -> None:
        raise OSError("simulated interruption")

    with monkeypatch.context() as patch:
        patch.setattr(store_module.os, "replace", interrupt)
        with pytest.raises(OSError, match="simulated interruption"):
            store.save_tokens(
                "withings",
                token_pair(access="access-two", refresh="refresh-two"),
            )

    assert store.load_tokens("withings") == original
    assert list(store.root.glob("*.tmp")) == []


def test_store_rejects_credentials_with_broad_permissions(tmp_path: Path) -> None:
    store = FileSecretStore(tmp_path / "secrets")
    store.save_tokens("withings", token_pair())
    path = store.root / "withings.tokens.json"
    os.chmod(path, 0o644)

    with pytest.raises(SecretStoreError, match="permissions"):
        store.load_tokens("withings")


def test_oauth_state_is_hashed_expiring_and_one_time(tmp_path: Path) -> None:
    store = FileSecretStore(tmp_path / "secrets")
    issued_at = datetime(2026, 9, 10, 15, tzinfo=UTC)
    state = "synthetic-state"
    store.save_pending_state("withings", state, issued_at=issued_at)

    state_document = (store.root / "withings.state.json").read_text(encoding="utf-8")
    assert state not in state_document
    with pytest.raises(OAuthStateError, match="does not match"):
        store.consume_pending_state(
            "withings",
            "wrong-state",
            now=issued_at,
            ttl=timedelta(minutes=15),
        )

    store.consume_pending_state(
        "withings",
        state,
        now=issued_at + timedelta(minutes=14),
        ttl=timedelta(minutes=15),
    )
    with pytest.raises(OAuthStateError, match="missing"):
        store.consume_pending_state(
            "withings",
            state,
            now=issued_at + timedelta(minutes=14),
            ttl=timedelta(minutes=15),
        )


def test_oauth_state_expires(tmp_path: Path) -> None:
    store = FileSecretStore(tmp_path / "secrets")
    issued_at = datetime(2026, 9, 10, 15, tzinfo=UTC)
    store.save_pending_state("withings", "state", issued_at=issued_at)

    with pytest.raises(OAuthStateError, match="expired"):
        store.consume_pending_state(
            "withings",
            "state",
            now=issued_at + timedelta(minutes=16),
            ttl=timedelta(minutes=15),
        )
