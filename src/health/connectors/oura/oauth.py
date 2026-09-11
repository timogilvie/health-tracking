"""Project-owned safety boundary around the pinned Oura OAuth client."""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import requests
from oura_ring import OuraAuth

from health.auth import FileSecretStore, OAuthTokenPair

PROVIDER = "oura"
STATE_TTL = timedelta(minutes=15)
REFRESH_SKEW = timedelta(minutes=5)
DEFAULT_SCOPE = "daily heartrate workout session"


class OuraAuthClient(Protocol):
    def authorize_url(
        self,
        redirect_uri: str | None = None,
        scope: list[str] | tuple[str, ...] | None = None,
        state: str | None = None,
    ) -> str: ...

    def exchange_code(
        self,
        code: str,
        redirect_uri: str | None = None,
    ) -> dict[str, Any]: ...

    def refresh_token(self, refresh_token: str) -> dict[str, Any]: ...


class OuraOAuthError(RuntimeError):
    """A categorized Oura error that never includes provider response content."""

    def __init__(self, task: str, *, http_status: int | None = None) -> None:
        self.task = task
        self.http_status = http_status
        self.invalid_grant = task == "refresh" and http_status in {400, 401}
        self.retryable = http_status is None or http_status == 429 or http_status >= 500
        if task in {"authorization_required", "configuration", "token_response"}:
            category = task
        elif http_status == 429:
            category = "rate_limited"
        elif http_status in {400, 401, 403}:
            category = "authentication_failed"
        elif http_status is not None and http_status < 500:
            category = "invalid_request"
        else:
            category = "provider_unavailable"
        super().__init__(f"Oura OAuth {task} failed ({category})")


@dataclass(frozen=True, slots=True)
class OuraOAuthConfig:
    client_id: str
    client_secret: str = field(repr=False)
    redirect_uri: str
    scope: str = DEFAULT_SCOPE

    @property
    def missing_fields(self) -> tuple[str, ...]:
        values = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
        }
        return tuple(name for name, value in values.items() if not value.strip())

    @property
    def scopes(self) -> tuple[str, ...] | None:
        values = tuple(part for part in self.scope.split() if part)
        return values or None


@dataclass(frozen=True, slots=True)
class AuthorizationRequest:
    url: str
    state: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class AuthenticationStatus:
    provider: str
    configured: bool
    tokens_present: bool
    token_state: str
    expires_at: datetime | None
    detail: str


class OuraOAuth:
    """Secure state, rotation, retry, and persistence around ``oura-ring``."""

    def __init__(
        self,
        *,
        config: OuraOAuthConfig,
        secret_store: FileSecretStore,
        auth_client: OuraAuthClient | None = None,
        now: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        max_attempts: int = 3,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        self.config = config
        self.secret_store = secret_store
        self.auth_client = auth_client or OuraAuth(config.client_id, config.client_secret)
        self.now = now or (lambda: datetime.now(UTC))
        self.sleeper = sleeper
        self.max_attempts = max_attempts

    def _require_config(self) -> None:
        if self.config.missing_fields:
            raise OuraOAuthError("configuration")

    def begin_authorization(self) -> AuthorizationRequest:
        self._require_config()
        state = secrets.token_urlsafe(32)
        self.secret_store.save_pending_state(PROVIDER, state, issued_at=self.now())
        url = self.auth_client.authorize_url(
            redirect_uri=self.config.redirect_uri,
            scope=self.config.scopes,
            state=state,
        )
        return AuthorizationRequest(url=url, state=state)

    def exchange_code(self, code: str, state: str) -> OAuthTokenPair:
        self._require_config()
        if not code:
            raise ValueError("authorization code must be non-empty")
        self.secret_store.consume_pending_state(
            PROVIDER,
            state,
            now=self.now(),
            ttl=STATE_TTL,
        )
        body = self._provider_call(
            lambda: self.auth_client.exchange_code(
                code,
                redirect_uri=self.config.redirect_uri,
            ),
            task="exchange",
        )
        tokens = self._parse_tokens(body)
        self.secret_store.save_tokens(PROVIDER, tokens)
        return tokens

    def authenticate(self) -> None:
        self.ensure_valid_tokens()

    def ensure_valid_tokens(self) -> OAuthTokenPair:
        self._require_config()
        with self.secret_store.locked(PROVIDER) as locked:
            current = locked.load_tokens()
            if current is None:
                raise OuraOAuthError("authorization_required")
            if current.expires_at > self.now() + REFRESH_SKEW:
                return current
            body = self._provider_call(
                lambda: self.auth_client.refresh_token(current.refresh_token),
                task="refresh",
            )
            replacement = self._parse_tokens(body)
            locked.save_tokens(replacement)
            return replacement

    def access_token(self) -> str:
        return self.ensure_valid_tokens().access_token

    def status(self) -> AuthenticationStatus:
        missing = self.config.missing_fields
        if missing:
            return AuthenticationStatus(
                provider=PROVIDER,
                configured=False,
                tokens_present=False,
                token_state="unconfigured",
                expires_at=None,
                detail=f"missing configuration: {', '.join(missing)}",
            )
        tokens = self.secret_store.load_tokens(PROVIDER)
        if tokens is None:
            return AuthenticationStatus(
                provider=PROVIDER,
                configured=True,
                tokens_present=False,
                token_state="missing",
                expires_at=None,
                detail="authorization required",
            )
        now = self.now()
        if tokens.expires_at <= now:
            token_state = "expired"
        elif tokens.expires_at <= now + REFRESH_SKEW:
            token_state = "expiring"
        else:
            token_state = "valid"
        return AuthenticationStatus(
            provider=PROVIDER,
            configured=True,
            tokens_present=True,
            token_state=token_state,
            expires_at=tokens.expires_at,
            detail="stored credentials are private and readable",
        )

    def _provider_call(
        self,
        call: Callable[[], dict[str, Any]],
        *,
        task: str,
    ) -> dict[str, Any]:
        for attempt in range(1, self.max_attempts + 1):
            try:
                body = call()
            except requests.RequestException as exc:
                status = exc.response.status_code if exc.response is not None else None
                error = OuraOAuthError(task, http_status=status)
                if not error.retryable or attempt == self.max_attempts:
                    raise error from exc
                self.sleeper(min(0.25 * (2 ** (attempt - 1)), 5.0))
                continue
            if not isinstance(body, dict):
                raise OuraOAuthError("token_response")
            return body
        raise RuntimeError("unreachable")

    def _parse_tokens(self, body: dict[str, Any]) -> OAuthTokenPair:
        try:
            access_token = body["access_token"]
            refresh_token = body["refresh_token"]
            expires_in = int(body["expires_in"])
            if not isinstance(access_token, str) or not isinstance(refresh_token, str):
                raise TypeError
            if expires_in <= 0:
                raise ValueError
            return OAuthTokenPair(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=self.now() + timedelta(seconds=expires_in),
                token_type=str(body.get("token_type", "Bearer")),
                scope=str(body["scope"]) if body.get("scope") is not None else None,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise OuraOAuthError("token_response") from exc
