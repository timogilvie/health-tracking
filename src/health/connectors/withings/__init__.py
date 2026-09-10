"""Withings authentication and connector components."""

from health.connectors.withings.oauth import (
    AuthenticationStatus,
    AuthorizationRequest,
    WithingsOAuth,
    WithingsOAuthConfig,
    WithingsOAuthError,
)

__all__ = [
    "AuthenticationStatus",
    "AuthorizationRequest",
    "WithingsOAuth",
    "WithingsOAuthConfig",
    "WithingsOAuthError",
]
