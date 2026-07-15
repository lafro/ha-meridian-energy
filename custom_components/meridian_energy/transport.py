"""Asynchronous HTTP transport for Meridian customer services."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout

from .const import (
    DEFAULT_RETRY_AFTER_SECONDS,
    MAX_RETRY_AFTER_SECONDS,
    MIN_RETRY_AFTER_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
)
from .models import require_mapping

_LOGGER = logging.getLogger(__name__)
_HTTP_BAD_REQUEST = 400


class MeridianTransportError(Exception):
    """The Meridian transport could not complete a request."""


class MeridianHttpError(MeridianTransportError):
    """Meridian returned an unsuccessful HTTP status."""

    def __init__(
        self,
        status: int,
        payload: dict[str, Any],
        retry_after: float = DEFAULT_RETRY_AFTER_SECONDS,
    ) -> None:
        super().__init__(f"Meridian returned HTTP {status}")
        self.status = status
        self.payload = payload
        self.retry_after = retry_after


class MeridianTransport:
    """Perform JSON POST requests without logging sensitive payloads."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session
        self._timeout = ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

    async def async_json_request(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Post a request and return a validated JSON object."""
        try:
            async with self._session.post(
                url,
                json=json,
                data=data,
                headers=headers,
                timeout=self._timeout,
            ) as response:
                response_headers = getattr(response, "headers", {})
                retry_after = parse_retry_after(response_headers.get("Retry-After"))
                try:
                    payload = await response.json(content_type=None)
                except (ValueError, TypeError) as err:
                    if response.status >= _HTTP_BAD_REQUEST:
                        raise MeridianHttpError(
                            response.status, {}, retry_after
                        ) from err
                    raise MeridianTransportError(
                        "Meridian returned an unreadable response"
                    ) from err
                if response.status >= _HTTP_BAD_REQUEST:
                    _LOGGER.debug("Meridian request returned HTTP %d", response.status)
                    raise MeridianHttpError(
                        response.status,
                        payload if isinstance(payload, dict) else {},
                        retry_after,
                    )
                return require_mapping(payload, "HTTP response")
        except MeridianHttpError:
            raise
        except (ClientError, TimeoutError) as err:
            _LOGGER.debug("Meridian request failed: %s", type(err).__name__)
            raise MeridianTransportError("Unable to reach Meridian") from err


def parse_retry_after(value: str | None) -> float:
    """Return a bounded Retry-After delay from seconds or an HTTP date."""
    delay = float(DEFAULT_RETRY_AFTER_SECONDS)
    if value:
        try:
            delay = float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None or retry_at.utcoffset() is None:
                    raise ValueError
                delay = (retry_at.astimezone(UTC) - datetime.now(UTC)).total_seconds()
            except TypeError, ValueError, OverflowError:
                delay = float(DEFAULT_RETRY_AFTER_SECONDS)
    return min(
        float(MAX_RETRY_AFTER_SECONDS),
        max(float(MIN_RETRY_AFTER_SECONDS), delay),
    )
