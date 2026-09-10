"""Withings OAuth 2.0 flow with atomic rotating-token persistence.

Substantially adapted from Open Wearables' Withings OAuth implementation:
https://github.com/the-momentum/open-wearables/blob/802862fa1ac08f165a897cb9adc4f4b312b08e5c/backend/app/services/providers/withings/oauth.py
Copyright (c) 2025 Momentum, used under the MIT License. See NOTICE.
"""

from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

import httpx

from health.auth import FileSecretStore, OAuthTokenPair

AUTHORIZE_URL = "https://account.withings.com/oauth2_user/authorize2"
TOKEN_URL = "https://wbsapi.withings.net/v2/oauth2"
PROVIDER = "withings"
STATE_TTL = timedelta(minutes=15)
REFRESH_SKEW = timedelta(minutes=5)

_TOKEN_CLIENT_ERROR_STATUSES = {247, 250, 283, 286, 293, 303, 304, 342}
_AUTHENTICATION_FAILED_STATUSES = {100, 101, 102, 200, 401}
_RATE_LIMIT_STATUS = 601


class WithingsOAuthError(RuntimeError):
    """A sanitized Withings OAuth error that never includes response bodies."""

    def __init__(
        self,
        task: str,
        *,
        provider_status: int | None = None,
        http_status: int | None = None,
    ) -> None:
        self.task = task
        self.provider_status = provider_status
        self.http_status = http_status
        authentication_failed = (
            provider_status in _AUTHENTICATION_FAILED_STATUSES
            or http_status in {400, 401}
        )
        self.invalid_grant = task == "refresh" and authentication_failed
        self.retryable = provider_status == _RATE_LIMIT_STATUS or http_status == 429 or (
            http_status is not None and http_status >= 500
        )
        if task in {"authorization_required", "configuration", "token_response"}:
            category = task
        elif provider_status == _RATE_LIMIT_STATUS or http_status == 429:
            category = "rate_limited"
        elif authentication_failed:
            category = "authentication_failed"
        elif provider_status in _TOKEN_CLIENT_ERROR_STATUSES or (
            http_status is not None and http_status < 500
        ):
            category = "invalid_request"
        else:
            category = "provider_unavailable"
        super().__init__(f"Withings OAuth {task} failed ({category})")


@dataclass(frozen=True, slots=True)
class WithingsOAuthConfig:
    client_id: str
    client_secret: str = field(repr=False)
    redirect_uri: str
    scope: str = "user.metrics"

    @property
    def missing_fields(self) -> tuple[str, ...]:
        values = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
        }
        return tuple(name for name, value in values.items() if not value.strip())


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


class WithingsOAuth:
    """Provider auth boundary used by the future Withings data connector."""

    def __init__(
        self,
        *,
        config: WithingsOAuthConfig,
        secret_store: FileSecretStore,
        client: httpx.Client,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self.secret_store = secret_store
        self.client = client
        self.now = now or (lambda: datetime.now(UTC))

    def _require_config(self) -> None:
        missing = self.config.missing_fields
        if missing:
            raise WithingsOAuthError("configuration")

    def begin_authorization(self) -> AuthorizationRequest:
        self._require_config()
        state = secrets.token_urlsafe(32)
        self.secret_store.save_pending_state(PROVIDER, state, issued_at=self.now())
        parameters = {
            "response_type": "code",
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "state": state,
        }
        if self.config.scope:
            parameters["scope"] = self.config.scope
        return AuthorizationRequest(f"{AUTHORIZE_URL}?{urlencode(parameters)}", state)

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
        body = self._request_token(
            {
                "action": "requesttoken",
                "grant_type": "authorization_code",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "code": code,
                "redirect_uri": self.config.redirect_uri,
            },
            task="exchange",
        )
        tokens = self._parse_tokens(body)
        self.secret_store.save_tokens(PROVIDER, tokens)
        return tokens

    def authenticate(self) -> None:
        """Satisfy the connector authentication contract."""

        self.ensure_valid_tokens()

    def ensure_valid_tokens(self) -> OAuthTokenPair:
        self._require_config()
        with self.secret_store.locked(PROVIDER) as locked:
            current = locked.load_tokens()
            if current is None:
                raise WithingsOAuthError("authorization_required")
            if current.expires_at > self.now() + REFRESH_SKEW:
                return current
            body = self._request_token(
                {
                    "action": "requesttoken",
                    "grant_type": "refresh_token",
                    "client_id": self.config.client_id,
                    "client_secret": self.config.client_secret,
                    "refresh_token": current.refresh_token,
                },
                task="refresh",
            )
            replacement = self._parse_tokens(body, fallback_refresh=current.refresh_token)
            locked.save_tokens(replacement)
            return replacement

    def access_token(self) -> str:
        """Return a valid bearer token for an in-process provider request."""

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

    def _request_token(self, payload: dict[str, str], *, task: str) -> dict[str, Any]:
        try:
            response = self.client.post(
                TOKEN_URL,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30.0,
            )
        except httpx.TransportError as exc:
            raise WithingsOAuthError(task) from exc
        if response.status_code >= 400:
            raise WithingsOAuthError(task, http_status=response.status_code)
        try:
            envelope = response.json()
        except ValueError as exc:
            raise WithingsOAuthError(task, http_status=response.status_code) from exc
        if not isinstance(envelope, dict):
            raise WithingsOAuthError(task, http_status=response.status_code)
        provider_status = envelope.get("status")
        if provider_status != 0:
            status_value = provider_status if isinstance(provider_status, int) else None
            raise WithingsOAuthError(task, provider_status=status_value)
        body = envelope.get("body")
        if not isinstance(body, dict):
            raise WithingsOAuthError(task)
        return body

    def _parse_tokens(
        self,
        body: dict[str, Any],
        *,
        fallback_refresh: str | None = None,
    ) -> OAuthTokenPair:
        try:
            access_token = body["access_token"]
            refresh_token = body.get("refresh_token") or fallback_refresh
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
                provider_user_id=(
                    str(body["userid"]) if body.get("userid") is not None else None
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WithingsOAuthError("token_response") from exc
