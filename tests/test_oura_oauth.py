from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from oura_ring import OuraAuth
from oura_ring import auth as package_auth

import health.auth.store as store_module
from health.auth import FileSecretStore, OAuthTokenPair
from health.connectors.oura import OuraOAuth, OuraOAuthConfig, OuraOAuthError

NOW = datetime(2026, 9, 11, 15, tzinfo=UTC)


class FakeOuraAuth:
    def __init__(self) -> None:
        self.authorizations: list[tuple[str | None, tuple[str, ...] | None, str | None]] = []
        self.exchanges: list[tuple[str, str | None]] = []
        self.refreshes: list[str] = []
        self.response = token_response()

    def authorize_url(
        self,
        redirect_uri: str | None = None,
        scope: list[str] | tuple[str, ...] | None = None,
        state: str | None = None,
    ) -> str:
        scopes = tuple(scope) if scope is not None else None
        self.authorizations.append((redirect_uri, scopes, state))
        return f"https://cloud.ouraring.test/authorize?state={state}"

    def exchange_code(
        self,
        code: str,
        redirect_uri: str | None = None,
    ) -> dict[str, object]:
        self.exchanges.append((code, redirect_uri))
        return self.response

    def refresh_token(self, refresh_token: str) -> dict[str, object]:
        self.refreshes.append(refresh_token)
        return self.response


def token_response(
    *,
    access: str = "new-access",
    refresh: str = "new-refresh",
) -> dict[str, object]:
    return {
        "access_token": access,
        "refresh_token": refresh,
        "expires_in": 3600,
        "token_type": "bearer",
        "scope": "daily heartrate workout session",
    }


def config() -> OuraOAuthConfig:
    return OuraOAuthConfig(
        client_id="synthetic-client",
        client_secret="synthetic-secret",
        redirect_uri="http://localhost:8765/callback",
    )


def service(
    tmp_path: Path,
    auth_client: FakeOuraAuth | None = None,
    **kwargs,
) -> OuraOAuth:
    return OuraOAuth(
        config=config(),
        secret_store=FileSecretStore(tmp_path / "secrets"),
        auth_client=auth_client or FakeOuraAuth(),
        now=lambda: NOW,
        **kwargs,
    )


def test_pinned_client_authorization_contract_uses_server_side_flow(tmp_path: Path) -> None:
    oauth = OuraOAuth(
        config=config(),
        secret_store=FileSecretStore(tmp_path / "secrets"),
        now=lambda: NOW,
    )

    assert isinstance(oauth.auth_client, OuraAuth)
    authorization = oauth.begin_authorization()
    query = parse_qs(urlparse(authorization.url).query)

    assert query == {
        "response_type": ["code"],
        "client_id": ["synthetic-client"],
        "redirect_uri": ["http://localhost:8765/callback"],
        "scope": ["daily heartrate workout session"],
        "state": [authorization.state],
    }
    assert "synthetic-secret" not in authorization.url


def test_code_exchange_consumes_state_and_persists_pair(tmp_path: Path) -> None:
    client = FakeOuraAuth()
    oauth = service(tmp_path, client)
    authorization = oauth.begin_authorization()

    tokens = oauth.exchange_code("synthetic-code", authorization.state)

    assert client.authorizations == [
        (
            "http://localhost:8765/callback",
            ("daily", "heartrate", "workout", "session"),
            authorization.state,
        )
    ]
    assert client.exchanges == [("synthetic-code", "http://localhost:8765/callback")]
    assert oauth.secret_store.load_tokens("oura") == tokens
    assert tokens.expires_at == NOW + timedelta(hours=1)


def test_pinned_client_exchange_and_refresh_request_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests_made: list[dict[str, str]] = []
    responses = iter(
        [
            token_response(access="exchange-access", refresh="exchange-refresh"),
            token_response(access="refresh-access", refresh="refresh-refresh"),
        ]
    )

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return next(responses)

    def post(_url: str, *, data: dict[str, str], timeout: int) -> Response:
        assert timeout == 60
        requests_made.append(data)
        return Response()

    monkeypatch.setattr(package_auth.requests, "post", post)
    oauth = OuraOAuth(
        config=config(),
        secret_store=FileSecretStore(tmp_path / "secrets"),
        now=lambda: NOW,
    )
    authorization = oauth.begin_authorization()
    exchanged = oauth.exchange_code("synthetic-code", authorization.state)
    oauth.secret_store.save_tokens(
        "oura",
        OAuthTokenPair(
            access_token=exchanged.access_token,
            refresh_token=exchanged.refresh_token,
            expires_at=NOW,
        ),
    )

    refreshed = oauth.ensure_valid_tokens()

    assert requests_made == [
        {
            "grant_type": "authorization_code",
            "code": "synthetic-code",
            "client_id": "synthetic-client",
            "client_secret": "synthetic-secret",
            "redirect_uri": "http://localhost:8765/callback",
        },
        {
            "grant_type": "refresh_token",
            "refresh_token": "exchange-refresh",
            "client_id": "synthetic-client",
            "client_secret": "synthetic-secret",
        },
    ]
    assert refreshed.access_token == "refresh-access"
    assert refreshed.refresh_token == "refresh-refresh"


def test_refresh_rotates_single_use_pair_once_across_concurrent_callers(
    tmp_path: Path,
) -> None:
    client = FakeOuraAuth()
    oauth = service(tmp_path, client)
    oauth.secret_store.save_tokens(
        "oura",
        OAuthTokenPair(
            access_token="old-access",
            refresh_token="old-refresh",
            expires_at=NOW,
        ),
    )
    original_refresh = client.refresh_token
    lock = threading.Lock()

    def delayed_refresh(refresh_token: str) -> dict[str, object]:
        with lock:
            time.sleep(0.05)
            return original_refresh(refresh_token)

    client.refresh_token = delayed_refresh  # type: ignore[method-assign]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _index: oauth.ensure_valid_tokens(), range(2)))

    assert client.refreshes == ["old-refresh"]
    assert {tokens.access_token for tokens in results} == {"new-access"}
    assert {tokens.refresh_token for tokens in results} == {"new-refresh"}
    assert oauth.secret_store.load_tokens("oura") == results[0]


def test_refresh_requires_new_token_and_atomic_write_preserves_old_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeOuraAuth()
    oauth = service(tmp_path, client)
    original = OAuthTokenPair(
        access_token="old-access",
        refresh_token="old-refresh",
        expires_at=NOW,
    )
    oauth.secret_store.save_tokens("oura", original)
    client.response = {"access_token": "new-access", "expires_in": 3600}

    with pytest.raises(OuraOAuthError, match="token_response"):
        oauth.ensure_valid_tokens()
    assert oauth.secret_store.load_tokens("oura") == original

    client.response = token_response()
    with monkeypatch.context() as patch:
        patch.setattr(
            store_module.os,
            "replace",
            lambda _source, _destination: (_ for _ in ()).throw(OSError("interrupted")),
        )
        with pytest.raises(OSError, match="interrupted"):
            oauth.ensure_valid_tokens()
    assert oauth.secret_store.load_tokens("oura") == original


def test_transient_errors_retry_and_messages_never_leak_provider_content(
    tmp_path: Path,
) -> None:
    client = FakeOuraAuth()
    sleeps: list[float] = []
    oauth = service(tmp_path, client, sleeper=sleeps.append, max_attempts=2)
    oauth.secret_store.save_tokens(
        "oura",
        OAuthTokenPair(
            access_token="old-access",
            refresh_token="old-refresh",
            expires_at=NOW,
        ),
    )
    calls = 0

    def transient(_refresh_token: str) -> dict[str, object]:
        nonlocal calls
        calls += 1
        if calls == 1:
            response = requests.Response()
            response.status_code = 503
            response._content = b"synthetic-secret old-refresh"
            raise requests.HTTPError(response=response)
        return token_response()

    client.refresh_token = transient  # type: ignore[method-assign]

    assert oauth.ensure_valid_tokens().access_token == "new-access"
    assert calls == 2
    assert sleeps == [0.25]

    client.refresh_token = lambda _token: (_ for _ in ()).throw(
        requests.HTTPError(response=_response(401, b"synthetic-secret old-refresh"))
    )
    oauth.secret_store.save_tokens(
        "oura",
        OAuthTokenPair(
            access_token="expired-access",
            refresh_token="old-refresh",
            expires_at=NOW,
        ),
    )
    with pytest.raises(OuraOAuthError) as caught:
        oauth.ensure_valid_tokens()
    assert caught.value.invalid_grant is True
    assert "authentication_failed" in str(caught.value)
    assert "synthetic-secret" not in str(caught.value)
    assert "old-refresh" not in str(caught.value)


def _response(status: int, content: bytes) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = content
    return response


def test_status_and_representations_do_not_expose_credentials(tmp_path: Path) -> None:
    oauth = service(tmp_path)
    assert oauth.status().token_state == "missing"
    oauth.secret_store.save_tokens(
        "oura",
        OAuthTokenPair(
            access_token="access-never-print",
            refresh_token="refresh-never-print",
            expires_at=NOW + timedelta(hours=1),
        ),
    )

    status = oauth.status()

    assert status.token_state == "valid"
    assert "synthetic-secret" not in repr(oauth.config)
    assert "access-never-print" not in repr(status)
    assert "refresh-never-print" not in repr(status)
    oauth.authenticate()
