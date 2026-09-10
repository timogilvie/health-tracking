"""Raw-first Withings RPC-over-POST transport and pagination.

Substantially adapted from Open Wearables' Withings RPC implementation:
https://github.com/the-momentum/open-wearables/blob/802862fa1ac08f165a897cb9adc4f4b312b08e5c/backend/app/services/providers/withings/handlers/rpc_client.py
Copyright (c) 2025 Momentum, used under the MIT License. See NOTICE.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from typing import Any

import httpx

from health.connectors import RawPage
from health.ingestion import RetryableIngestionError

API_BASE_URL = "https://wbsapi.withings.net"
RATE_LIMIT_STATUS = 601
AUTHENTICATION_STATUSES = {100, 101, 102, 200, 401}
DEFAULT_MAX_PAGES = 200


class WithingsAPIError(RuntimeError):
    """A sanitized, non-retryable HTTP or provider-envelope failure."""

    def __init__(
        self,
        *,
        action: str,
        provider_status: int | None = None,
        http_status: int | None = None,
    ) -> None:
        self.action = action
        self.provider_status = provider_status
        self.http_status = http_status
        self.authentication_failed = (
            provider_status in AUTHENTICATION_STATUSES or http_status in {401, 403}
        )
        category = "authentication_failed" if self.authentication_failed else "provider_error"
        super().__init__(f"Withings {action} failed ({category})")


class WithingsPayloadError(RuntimeError):
    """A successful Withings response has an unusable envelope or body."""

    def __init__(self, action: str, reason: str) -> None:
        self.action = action
        self.reason = reason
        super().__init__(f"Invalid Withings {action} response: {reason}")


class WithingsPaginationError(RuntimeError):
    """Withings pagination failed to advance or exceeded its safety cap."""


def decode_envelope(content: bytes, *, action: str) -> dict[str, Any]:
    try:
        envelope = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WithingsPayloadError(action, "response is not a JSON object") from exc
    if not isinstance(envelope, dict):
        raise WithingsPayloadError(action, "response is not a JSON object")
    return envelope


def _retry_after(headers: Mapping[str, str]) -> float | None:
    value = headers.get("retry-after")
    if value is None:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        return None


def inspect_page(
    *,
    content: bytes,
    http_status: int,
    headers: Mapping[str, str],
    action: str,
    list_key: str,
) -> dict[str, Any]:
    """Classify a persisted response and return its successful body."""

    if http_status == 429 or http_status >= 500:
        raise RetryableIngestionError(
            f"Withings {action} temporarily unavailable",
            retry_after=_retry_after(headers),
        )
    if http_status >= 400:
        raise WithingsAPIError(action=action, http_status=http_status)

    envelope = decode_envelope(content, action=action)
    provider_status = envelope.get("status")
    if provider_status != 0:
        status = provider_status if isinstance(provider_status, int) else None
        if status == RATE_LIMIT_STATUS:
            raise RetryableIngestionError(f"Withings {action} rate limited")
        raise WithingsAPIError(action=action, provider_status=status)
    body = envelope.get("body")
    if not isinstance(body, dict):
        raise WithingsPayloadError(action, "body is not an object")
    rows = body.get(list_key, [])
    if not isinstance(rows, list):
        raise WithingsPayloadError(action, f"body.{list_key} is not a list")
    return body


class WithingsRPCClient:
    """Yield exact envelopes before inspecting them for pagination."""

    def __init__(
        self,
        *,
        client: httpx.Client,
        access_token: Callable[[], str],
        now: Callable[[], datetime] | None = None,
        api_base_url: str = API_BASE_URL,
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> None:
        if max_pages < 1:
            raise ValueError("max_pages must be at least one")
        self.client = client
        self.access_token = access_token
        self.now = now or (lambda: datetime.now(UTC))
        self.api_base_url = api_base_url.rstrip("/")
        self.max_pages = max_pages

    def paginate(
        self,
        *,
        service_path: str,
        action: str,
        params: Mapping[str, str | int],
        list_key: str,
    ) -> Iterator[RawPage]:
        """Fetch pages, with parsing deferred until after each yielded page is saved."""

        requested_offset: int | None = None
        for page_index in range(self.max_pages):
            form: dict[str, str | int] = {"action": action, **params}
            if requested_offset is not None:
                form["offset"] = requested_offset
            try:
                response = self.client.post(
                    f"{self.api_base_url}/{service_path.lstrip('/')}",
                    data=form,
                    headers={"Authorization": f"Bearer {self.access_token()}"},
                    timeout=30.0,
                )
            except httpx.TransportError as exc:
                raise RetryableIngestionError(
                    f"Withings {action} transport unavailable"
                ) from exc

            response_headers = dict(response.headers)
            page = RawPage(
                source="withings",
                endpoint=f"{service_path.strip('/')}/{action}",
                retrieved_at=self.now(),
                content=response.content,
                content_type=response.headers.get("content-type", "application/json"),
                request_metadata={
                    "service_path": service_path,
                    "action": action,
                    "list_key": list_key,
                    "page_index": page_index,
                    "requested_offset": requested_offset,
                    "parameters": dict(params),
                },
                http_status=response.status_code,
                response_headers=response_headers,
            )
            yield page

            body = inspect_page(
                content=page.content,
                http_status=response.status_code,
                headers=response.headers,
                action=action,
                list_key=list_key,
            )
            if not body.get("more"):
                return
            try:
                next_offset = int(body["offset"])
            except (KeyError, TypeError, ValueError) as exc:
                raise WithingsPaginationError(
                    f"Withings {action} pagination omitted a usable offset"
                ) from exc
            previous_offset = requested_offset if requested_offset is not None else 0
            if next_offset <= previous_offset:
                raise WithingsPaginationError(
                    f"Withings {action} pagination offset did not advance"
                )
            requested_offset = next_offset

        raise WithingsPaginationError(
            f"Withings {action} pagination exceeded {self.max_pages} pages"
        )
