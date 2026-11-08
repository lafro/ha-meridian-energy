"""Asynchronous HTTP transport for Meridian customer services."""

from __future__ import annotations

import json as json_module
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from time import monotonic
from typing import Any, NamedTuple

from aiohttp import ClientError, ClientSession, ClientTimeout, ConnectionTimeoutError

from .const import (
    CONNECT_TIMEOUT_SECONDS,
    DEFAULT_RETRY_AFTER_SECONDS,
    MAX_RETRY_AFTER_SECONDS,
    MIN_RETRY_AFTER_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
)
from .models import require_mapping

_LOGGER = logging.getLogger(__name__)
_HTTP_BAD_REQUEST = 400


class MeridianTransportError(Exception):
    """
    The Meridian transport could not complete a request.

    ``error_class`` names the network library's error behind it, if any. That
    error is never chained, because its text can hold the request URL.
    """

    def __init__(self, message: str, *, error_class: str | None = None) -> None:
        super().__init__(message)
        self.error_class = error_class


class MeridianTimeoutError(MeridianTransportError):
    """Meridian did not answer within the request's time limit."""


class MeridianUnreadableResponseError(MeridianTransportError):
    """A successful status came with a body that is not JSON, such as a web page."""

    def __init__(self, status: int, content: str) -> None:
        super().__init__("Meridian returned an unreadable response")
        self.status = status
        self.content = content


class MeridianHttpError(MeridianTransportError):
    """
    Meridian returned an unsuccessful HTTP status.

    ``json_body`` says the body was JSON, as Meridian's own errors are; a
    gateway's error page is not. ``error_class`` names the network library's
    error when the body failed after the status had arrived.
    """

    def __init__(
        self,
        status: int,
        payload: dict[str, Any],
        retry_after: float = DEFAULT_RETRY_AFTER_SECONDS,
        *,
        error_class: str | None = None,
        json_body: bool = False,
    ) -> None:
        super().__init__(f"Meridian returned HTTP {status}", error_class=error_class)
        self.status = status
        self.payload = payload
        self.retry_after = retry_after
        self.json_body = json_body


class _Head(NamedTuple):
    """What an answer had sent before its body."""

    status: int
    content: str
    retry_after: float


class MeridianTransport:
    """Perform JSON POST requests without logging sensitive payloads."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session

    async def async_json_request(
        self,
        url: str,
        *,
        operation: str,
        time_limit: float = REQUEST_TIMEOUT_SECONDS,
        json: dict[str, Any] | None = None,
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        context: str = "",
    ) -> dict[str, Any]:
        """
        Post a request and return a validated JSON object.

        Each request logs one DEBUG line: the operation, ``context`` (such as
        a window as day offsets), the status, the kind and size of the body,
        or the error class, and how long it took. URLs, headers and bodies
        are never logged. A network error names its class but not its text,
        which can hold the URL. Making the connection has its own limit, so a
        connection never made is a network error, never a timeout of
        Meridian's answer. An unsuccessful status that arrived before its
        body failed is the answer: it is raised as that HTTP error.
        """
        started = monotonic()
        head: _Head | None = None
        try:
            async with self._session.post(
                url,
                json=json,
                data=data,
                headers=headers,
                timeout=ClientTimeout(
                    total=time_limit,
                    connect=min(CONNECT_TIMEOUT_SECONDS, time_limit),
                ),
            ) as response:
                response_headers = getattr(response, "headers", {})
                status = response.status
                head = _Head(
                    status=status,
                    content=_content_class(_header(response_headers, "content-type")),
                    retry_after=parse_retry_after(
                        _header(response_headers, "retry-after")
                    ),
                )
                body = await response.read()
        except (ClientError, TimeoutError) as err:
            # ConnectionTimeoutError is the connect limit: nothing reached
            # Meridian, a network failure. The total limit running out
            # raises a plain TimeoutError, even while connecting.
            timed_out = isinstance(err, TimeoutError) and not isinstance(
                err, ConnectionTimeoutError
            )
            _log(operation, context, started, head, error=type(err).__name__)
            failure = _status_failure(head, err)
            if failure is not None:
                raise failure from None
            if timed_out:
                raise MeridianTimeoutError(
                    f"Meridian did not answer in time ({type(err).__name__})",
                    error_class=type(err).__name__,
                ) from None
            raise MeridianTransportError(
                f"Unable to reach Meridian ({type(err).__name__})",
                error_class=type(err).__name__,
            ) from None
        _log(operation, context, started, head, size=len(body))
        try:
            payload = json_module.loads(body) if body.strip() else None
        except ValueError as err:
            if status >= _HTTP_BAD_REQUEST:
                # An error page is not expected to be JSON; its status says it all.
                raise MeridianHttpError(status, {}, head.retry_after) from None
            raise MeridianUnreadableResponseError(status, head.content) from err
        if status >= _HTTP_BAD_REQUEST:
            raise MeridianHttpError(
                status,
                payload if isinstance(payload, dict) else {},
                head.retry_after,
                json_body=payload is not None,
            )
        return require_mapping(payload, "HTTP response")


def _log(
    operation: str,
    context: str,
    started: float,
    head: _Head | None,
    *,
    size: int | None = None,
    error: str | None = None,
) -> None:
    """Log one request's outcome at DEBUG, without its URL, headers or body."""
    _LOGGER.debug(
        "Meridian request: operation=%s%s status=%s content=%s %s elapsed=%.1fs",
        operation,
        f" {context}" if context else "",
        "-" if head is None else head.status,
        "-" if head is None else head.content,
        f"bytes={size}" if error is None else f"error={error}",
        monotonic() - started,
    )


def _status_failure(head: _Head | None, err: Exception) -> MeridianHttpError | None:
    """Return the HTTP error of an unsuccessful status whose body failed, or None."""
    if head is None or head.status < _HTTP_BAD_REQUEST:
        return None
    return MeridianHttpError(
        head.status, {}, head.retry_after, error_class=type(err).__name__
    )


def _header(headers: Mapping[str, str], name: str) -> str:
    """Return a response header by its case-insensitive name, or ''."""
    for key, value in headers.items():
        if key.lower() == name:
            return value
    return ""


def _content_class(content_type: str) -> str:
    """Return the kind of a response body from its Content-Type."""
    mime = content_type.split(";", 1)[0].strip().lower()
    if not mime:
        return "none"
    if mime == "application/json" or mime.endswith("+json"):
        return "json"
    if mime == "text/html":
        return "html"
    if mime.startswith("text/"):
        return "text"
    return "other"


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
