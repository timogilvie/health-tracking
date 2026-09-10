import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

import health.auth.store as store_module
from health.auth import FileSecretStore, OAuthTokenPair
from health.connectors.withings import WithingsOAuth, WithingsOAuthConfig, WithingsOAuthError

NOW = datetime(2026, 9, 10, 15, tzinfo=UTC)


def config() -> WithingsOAuthConfig:
    return WithingsOAuthConfig(
        client_id="synthetic-client",
        client_secret="synthetic-secret",
        redirect_uri="http://localhost:8765/callback",
        scope="user.metrics,user.activity",
    )


def service(
    tmp_path: Path,
    handler,
) -> WithingsOAuth:
    return WithingsOAuth(
        config=config(),
        secret_store=FileSecretStore(tmp_path / "secrets"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
    )


def token_response(
    access: str = "new-access",
    refresh: str | None = "new-refresh",
) -> dict[str, object]:
    body: dict[str, object] = {
        "access_token": access,
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "user.metrics",
        "userid": 12345,
    }
    if refresh is not None:
        body["refresh_token"] = refresh
    return {"status": 0, "body": body}


def test_authorization_url_and_code_exchange_persist_pair(tmp_path: Path) -> None:
    requests: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(parse_qs(request.content.decode()))
        return httpx.Response(200, json=token_response())

    oauth = service(tmp_path, handler)
    authorization = oauth.begin_authorization()
    query = parse_qs(urlparse(authorization.url).query)

    assert query == {
        "response_type": ["code"],
        "client_id": ["synthetic-client"],
        "redirect_uri": ["http://localhost:8765/callback"],
        "state": [authorization.state],
        "scope": ["user.metrics,user.activity"],
    }

    tokens = oauth.exchange_code("synthetic-code", authorization.state)

    assert requests == [
        {
            "action": ["requesttoken"],
            "grant_type": ["authorization_code"],
            "client_id": ["synthetic-client"],
            "client_secret": ["synthetic-secret"],
            "code": ["synthetic-code"],
            "redirect_uri": ["http://localhost:8765/callback"],
        }
    ]
    assert tokens.provider_user_id == "12345"
    assert oauth.secret_store.load_tokens("withings") == tokens


def test_refresh_rotates_both_tokens_and_uses_withings_envelope(tmp_path: Path) -> None:
    request_forms: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        request_forms.append(parse_qs(request.content.decode()))
        return httpx.Response(200, json=token_response())

    oauth = service(tmp_path, handler)
    oauth.secret_store.save_tokens(
        "withings",
        OAuthTokenPair(
            access_token="old-access",
            refresh_token="old-refresh",
            expires_at=NOW - timedelta(seconds=1),
        ),
    )

    refreshed = oauth.ensure_valid_tokens()

    assert refreshed.access_token == "new-access"
    assert refreshed.refresh_token == "new-refresh"
    assert oauth.secret_store.load_tokens("withings") == refreshed
    assert request_forms[0]["refresh_token"] == ["old-refresh"]
    assert request_forms[0]["action"] == ["requesttoken"]


def test_refresh_keeps_previous_refresh_token_when_provider_omits_rotation(
    tmp_path: Path,
) -> None:
    oauth = service(
        tmp_path,
        lambda _request: httpx.Response(200, json=token_response(refresh=None)),
    )
    oauth.secret_store.save_tokens(
        "withings",
        OAuthTokenPair(
            access_token="old-access",
            refresh_token="old-refresh",
            expires_at=NOW,
        ),
    )

    assert oauth.ensure_valid_tokens().refresh_token == "old-refresh"


def test_concurrent_refresh_uses_rotating_grant_once(tmp_path: Path) -> None:
    calls = 0
    calls_lock = threading.Lock()

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.05)
        return httpx.Response(200, json=token_response())

    oauth = service(tmp_path, handler)
    oauth.secret_store.save_tokens(
        "withings",
        OAuthTokenPair(
            access_token="old-access",
            refresh_token="old-refresh",
            expires_at=NOW,
        ),
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _index: oauth.ensure_valid_tokens(), range(2)))

    assert calls == 1
    assert {result.access_token for result in results} == {"new-access"}
    assert {result.refresh_token for result in results} == {"new-refresh"}


def test_interrupted_refresh_does_not_split_token_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oauth = service(
        tmp_path,
        lambda _request: httpx.Response(200, json=token_response()),
    )
    original = OAuthTokenPair(
        access_token="old-access",
        refresh_token="old-refresh",
        expires_at=NOW,
    )
    oauth.secret_store.save_tokens("withings", original)

    with monkeypatch.context() as patch:
        patch.setattr(
            store_module.os,
            "replace",
            lambda _source, _destination: (_ for _ in ()).throw(OSError("interrupted")),
        )
        with pytest.raises(OSError, match="interrupted"):
            oauth.ensure_valid_tokens()

    assert oauth.secret_store.load_tokens("withings") == original


@pytest.mark.parametrize(
    ("response", "expected_category", "invalid_grant"),
    [
        (httpx.Response(401, text="synthetic-secret leaked"), "authentication_failed", True),
        (httpx.Response(429, text="synthetic-secret leaked"), "rate_limited", False),
        (
            httpx.Response(200, json={"status": 601, "body": {"error": "synthetic-secret"}}),
            "rate_limited",
            False,
        ),
    ],
)
def test_refresh_errors_are_classified_without_secret_leakage(
    tmp_path: Path,
    response: httpx.Response,
    expected_category: str,
    invalid_grant: bool,
) -> None:
    oauth = service(tmp_path, lambda _request: response)
    oauth.secret_store.save_tokens(
        "withings",
        OAuthTokenPair(
            access_token="old-access",
            refresh_token="old-refresh",
            expires_at=NOW,
        ),
    )

    with pytest.raises(WithingsOAuthError) as caught:
        oauth.ensure_valid_tokens()

    message = str(caught.value)
    assert expected_category in message
    assert caught.value.invalid_grant is invalid_grant
    assert "synthetic-secret" not in message
    assert "old-refresh" not in message


def test_status_and_representations_do_not_expose_credentials(tmp_path: Path) -> None:
    oauth = service(tmp_path, lambda _request: pytest.fail("network must not be used"))

    missing = oauth.status()
    assert missing.token_state == "missing"

    oauth.secret_store.save_tokens(
        "withings",
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
