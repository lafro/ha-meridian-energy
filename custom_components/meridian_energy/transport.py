"""Asynchronous HTTP transport for Meridian customer services."""

from __future__ import annotations

import json as json_module
import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from statistics import median
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
# Fixed markers of the hop that wrote an error page, checked in this order on
# the lower-cased body. Only the class is logged, never the page: gateway
# error pages show the client's IP address.
_EDGE_MARKERS = (
    ("cloudflare", (b"cloudflare", b"ray id")),
    ("awselb", (b"awselb",)),
    ("nginx", (b"nginx",)),
)


@dataclass(slots=True)
class RequestTally:
    """
    The HTTP requests sent while a tally counts, and how long each took.

    ``sent`` counts every request as it is sent; ``times`` holds the
    operation and time of each one that ended, answered or failed.
    """

    sent: int = 0
    times: list[tuple[str, float]] = field(default_factory=list)

    def median_seconds(self, operation: str | None = None) -> float | None:
        """Return the median time of the requests that ended, or of one kind."""
        seconds = [
            elapsed
            for name, elapsed in self.times
            if operation is None or name == operation
        ]
        return median(seconds) if seconds else None


_TALLY: ContextVar[RequestTally | None] = ContextVar(
    "meridian_energy_request_tally", default=None
)


@contextmanager
def count_requests() -> Iterator[RequestTally]:
    """
    Count the requests the transport sends in this context, while the block runs.

    A client call can send several requests (a session refresh before the
    query, a retry with a new token, one request per account), so only the
    transport knows how many were sent. The tally follows the running task,
    so requests sent elsewhere at the same time are not counted.
    """
    tally = RequestTally()
    token = _TALLY.set(tally)
    try:
        yield tally
    finally:
        _TALLY.reset(token)


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

    ``error_class`` names the network library's error when the body failed
    or timed out after the status had arrived; the payload is then empty.
    """

    def __init__(
        self,
        status: int,
        payload: dict[str, Any],
        retry_after: float = DEFAULT_RETRY_AFTER_SECONDS,
        *,
        error_class: str | None = None,
    ) -> None:
        super().__init__(f"Meridian returned HTTP {status}", error_class=error_class)
        self.status = status
        self.payload = payload
        self.retry_after = retry_after


class _Head(NamedTuple):
    """What an answer had sent before its body, and how long that took."""

    status: int
    content: str
    cf_ray: bool
    seconds: float
    retry_after: float


def _head_fields(head: _Head | None) -> str:
    """Return an answer's status and headers as log fields, or ``-`` for none."""
    if head is None:
        return "status=- content=- cf_ray=- headers_after=-"
    return (
        f"status={head.status} content={head.content} "
        f"cf_ray={'yes' if head.cf_ray else 'no'} headers_after={head.seconds:.1f}s"
    )


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
    ) -> dict[str, Any]:
        """
        Post a request and return a validated JSON object.

        Every answer is logged at debug level once its body has been read and
        before it is parsed: the operation, the status, the kind and size of
        the body, which hop wrote an error page, and how long it took. A
        request that times out or fails logs its error class and how long it
        waited, and, when the status and headers had arrived before the body
        failed, those too. URLs, headers and bodies are never logged.

        A network error is raised as a transport error that names its class
        but not its text, which can hold the URL, so a traceback never shows
        it. Making the connection has its own limit, 20 s at most, so a
        connection never made is a network error, never a timeout of
        Meridian's answer. When an unsuccessful status had arrived before
        the body failed or timed out, the status is the answer: it is raised
        as that HTTP error with an empty payload, as for a complete error
        page, naming the network error's class.
        """
        tally = _TALLY.get()
        if tally is not None:
            tally.sent += 1
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
                    cf_ray=bool(_header(response_headers, "cf-ray")),
                    seconds=monotonic() - started,
                    retry_after=parse_retry_after(
                        _header(response_headers, "retry-after")
                    ),
                )
                body = await response.read()
                elapsed = _ended(tally, operation, started)
                _LOGGER.debug(
                    "Meridian response: operation=%s status=%d content=%s bytes=%d "
                    "edge=%s cf_ray=%s elapsed=%.1fs",
                    operation,
                    status,
                    head.content,
                    len(body),
                    "-"
                    if head.content == "json" and status < _HTTP_BAD_REQUEST
                    else _edge_class(body),
                    "yes" if head.cf_ray else "no",
                    elapsed,
                )
        except ConnectionTimeoutError as err:
            # No connection within the connect limit: nothing reached
            # Meridian, which is a network failure, not a gateway timeout.
            # aiohttp raises this only for that limit; the request's total
            # limit running out raises a plain TimeoutError, even while
            # connecting.
            elapsed = _ended(tally, operation, started)
            raise _network_failure(operation, err, head, elapsed) from None
        except TimeoutError as err:
            _LOGGER.debug(
                "Meridian request timed out: operation=%s error=%s limit=%ds %s "
                "elapsed=%.1fs",
                operation,
                type(err).__name__,
                time_limit,
                _head_fields(head),
                _ended(tally, operation, started),
            )
            raise _status_failure(head, err) or MeridianTimeoutError(
                f"Meridian did not answer in time ({type(err).__name__})",
                error_class=type(err).__name__,
            ) from None
        except ClientError as err:
            elapsed = _ended(tally, operation, started)
            raise _network_failure(operation, err, head, elapsed) from None
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
            )
        return require_mapping(payload, "HTTP response")


def _ended(tally: RequestTally | None, operation: str, started: float) -> float:
    """Return how long a request took, and count its time in the tally."""
    elapsed = monotonic() - started
    if tally is not None:
        tally.times.append((operation, elapsed))
    return elapsed


def _network_failure(
    operation: str, err: Exception, head: _Head | None, elapsed: float
) -> MeridianTransportError:
    """Log a request that failed on the network, and return the error to raise."""
    _LOGGER.debug(
        "Meridian request failed: operation=%s error=%s %s elapsed=%.1fs",
        operation,
        type(err).__name__,
        _head_fields(head),
        elapsed,
    )
    return _status_failure(head, err) or MeridianTransportError(
        f"Unable to reach Meridian ({type(err).__name__})",
        error_class=type(err).__name__,
    )


def _status_failure(head: _Head | None, err: Exception) -> MeridianHttpError | None:
    """
    Return the HTTP error for an unsuccessful status whose body then failed.

    The status and headers are the server's answer, whole: a gateway's 504
    whose page was cut off is still a gateway timeout, and a 429's
    Retry-After still applies. A successful status whose body failed has
    no answer to give, so it stays a network error or a timeout (None).
    """
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


def _edge_class(body: bytes) -> str:
    """Return which hop wrote an error page, from fixed markers in its body."""
    lowered = body.lower()
    for edge, markers in _EDGE_MARKERS:
        if any(marker in lowered for marker in markers):
            return edge
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
