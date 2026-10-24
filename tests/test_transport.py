"""Tests for the Meridian HTTP transport and the requests it sends."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import traceback
from datetime import UTC, date, datetime, timedelta
from typing import Any, Self
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import (
    ClientConnectionError,
    ClientPayloadError,
    ClientTimeout,
    ConnectionTimeoutError,
    ServerTimeoutError,
)

from custom_components.meridian_energy import api as api_module
from custom_components.meridian_energy import transport as transport_module
from custom_components.meridian_energy.api import (
    MeridianApiClient,
    MeridianConnectionError,
    MeridianRateLimitError,
    MeridianRejectedError,
    MeridianUpstreamError,
    http_status,
)
from custom_components.meridian_energy.const import GRAPHQL_URL
from custom_components.meridian_energy.graphql import (
    measurements_operation,
    measurements_query,
)
from custom_components.meridian_energy.models import MeridianTokenSet
from custom_components.meridian_energy.transport import MeridianHttpError

from .meridian_http import stalled_session

_EMPTY_PAGE = {
    "data": {
        "account": {
            "property": {
                "measurements": {
                    "edges": [],
                    "pageInfo": {"hasPreviousPage": False, "startCursor": None},
                }
            }
        }
    }
}


class _Response:
    """One canned HTTP response, readable the way the transport reads it."""

    def __init__(
        self,
        body: bytes,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.status = status
        self.headers = (
            {"Content-Type": "application/json"} if headers is None else headers
        )
        self._body = body
        self._error = error

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def read(self) -> bytes:
        if self._error is not None:
            raise self._error
        return self._body


class _CapturingSession:
    """Record every POST and answer each with the next canned response."""

    def __init__(self, *responses: _Response | dict[str, Any]) -> None:
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self._responses = list(responses)

    def post(self, url: str, **kwargs: Any) -> _Response:
        self.posts.append((url, kwargs))
        response = self._responses[0] if len(self._responses) == 1 else None
        if response is None:
            response = self._responses.pop(0)
        if isinstance(response, dict):
            return _Response(json.dumps(response).encode())
        return response


def _tokens() -> MeridianTokenSet:
    return MeridianTokenSet(
        id_token="synthetic-id-token",
        refresh_token="synthetic-refresh",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        user_id="synthetic-user",
    )


@pytest.mark.asyncio
async def test_measurements_requests_carry_both_bounds_on_every_page() -> None:
    """
    Every measurements request sends its window's dates and page size.

    A continuation page differs from its window's first page only in
    ``before``. The query declares both bounds and ``last`` non-null, so a
    request that left one out would fail validation instead of asking for
    the whole history. It goes out under the web app's name, with the name
    on the URL too (operation-name parity).
    """
    session = _CapturingSession(_EMPTY_PAGE)
    client = MeridianApiClient(session, tokens=_tokens())  # type: ignore[arg-type]
    window = {
        "account_number": "A-TEST-1",
        "property_id": "synthetic-property",
        "direction": "CONSUMPTION",
        "start_on": date(2026, 7, 10),
        "end_on": date(2026, 7, 16),
        "last": 214,
    }

    await client.async_get_measurements(**window)
    await client.async_get_measurements(**window, before="YXJyYXljb25uZWN0aW9uOjc0Mw==")

    (first_url, first), (next_url, continuation) = session.posts
    assert first_url == next_url == f"{GRAPHQL_URL}?opName=GetMeasurements"
    assert first["json"]["operationName"] == "GetMeasurements"
    assert "query GetMeasurements(" in first["json"]["query"]
    for declaration in ("$last: Int!", "$startOn: Date!", "$endOn: Date!"):
        assert declaration in first["json"]["query"]
    arguments = first["json"]["query"].split("measurements(", 1)[1].split(")", 1)[0]
    for argument in ("startAt", "endAt", "first", "after"):
        assert argument not in arguments
    for field in ("totalCount", "edgeCount"):
        assert field not in first["json"]["query"]
    assert json.dumps(first["json"]["variables"]) == (
        '{"accountNumber": "A-TEST-1", "propertyId": "synthetic-property", '
        '"last": 214, "startOn": "2026-07-10", "endOn": "2026-07-16", '
        '"readingFrequencyType": "HOUR_INTERVAL", '
        '"readingDirectionType": "CONSUMPTION", "readingQualityType": "COMBINED"}'
    )
    assert continuation["json"]["query"] == first["json"]["query"]
    assert continuation["json"]["variables"] == {
        **first["json"]["variables"],
        "before": "YXJyYXljb25uZWN0aW9uOjc0Mw==",
    }
    assert first["headers"] == {"Authorization": "synthetic-id-token"}


def test_operation_name_parity_reverts_name_and_url_together() -> None:
    """One switch changes the measurements query's name and URL, never one alone."""
    assert measurements_operation(GRAPHQL_URL) == (
        "GetMeasurements",
        f"{GRAPHQL_URL}?opName=GetMeasurements",
    )
    assert measurements_operation(GRAPHQL_URL, web_app_name=False) == (
        "measurements",
        GRAPHQL_URL,
    )
    assert "query measurements(" in measurements_query("measurements")


@pytest.mark.parametrize(
    ("start_on", "end_on", "last"),
    [
        (date(2026, 7, 17), date(2026, 7, 16), 24),
        (date(2026, 7, 10), date(2026, 7, 16), 0),
    ],
)
@pytest.mark.asyncio
async def test_measurements_reject_a_window_that_is_not_one(
    start_on: date, end_on: date, last: int
) -> None:
    session = _CapturingSession(_EMPTY_PAGE)
    client = MeridianApiClient(session, tokens=_tokens())  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="window"):
        await client.async_get_measurements(
            account_number="A-TEST-1",
            property_id="synthetic-property",
            direction="CONSUMPTION",
            start_on=start_on,
            end_on=end_on,
            last=last,
        )

    assert session.posts == []


# A gateway error page as Cloudflare would serve it, with the client's
# address and an account number in it: none of its text may reach a log.
_HTML_504 = (
    b"<!DOCTYPE html><html><head><title>504 Gateway time-out</title></head>"
    b"<body><h1>Gateway time-out</h1><p>Account A-TEST-1</p>"
    b"<p>Your IP: 203.0.113.7</p><p>Cloudflare Ray ID: 8a1b2c3d4e5f6a7b</p>"
    b"</body></html>"
)
_PRIVATE_TEXT = ("203.0.113.7", "A-TEST-1", "Ray ID", "8a1b2c3d4e5f6a7b", "<html")


class _Clock:
    """A monotonic clock each request moves on by a fixed step."""

    def __init__(self, step: float) -> None:
        self.now = 100.0
        self.step = step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


def _transport_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == "custom_components.meridian_energy.transport"
    ]


async def _measurements(client: MeridianApiClient) -> object:
    return await client.async_get_measurements(
        account_number="A-TEST-1",
        property_id="synthetic-property",
        direction="CONSUMPTION",
        start_on=date(2026, 7, 10),
        end_on=date(2026, 7, 16),
        last=214,
    )


@pytest.mark.asyncio
async def test_html_gateway_error_is_logged_by_class_before_parsing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An HTML 504 logs its status, kind, size and hop, and none of its text."""
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    session = _CapturingSession(
        _Response(
            _HTML_504,
            status=504,
            headers={"Content-Type": "text/html; charset=UTF-8", "CF-RAY": "8a1b"},
        )
    )
    client = MeridianApiClient(session, tokens=_tokens())  # type: ignore[arg-type]

    # Readings at the start, once the headers arrive and once the body has.
    with (
        patch.object(transport_module, "monotonic", side_effect=_Clock(30.62)),
        pytest.raises(MeridianUpstreamError) as raised,
    ):
        await _measurements(client)

    assert raised.value.status == 504
    assert raised.value.gateway is True
    (record,) = _transport_records(caplog)
    assert record.levelno == logging.DEBUG
    assert record.getMessage() == (
        f"Meridian response: operation=measurements status=504 content=html "
        f"bytes={len(_HTML_504)} edge=cloudflare cf_ray=yes elapsed=61.2s"
    )
    for text in _PRIVATE_TEXT:
        assert text not in caplog.text


@pytest.mark.parametrize(
    ("body", "headers", "edge", "cf_ray"),
    [
        (b"<html>cloudflare</html>", {"Content-Type": "text/html"}, "cloudflare", "no"),
        (b"<html>Ray ID: 1</html>", {"cf-ray": "1"}, "cloudflare", "yes"),
        (b"<html><center>awselb/2.0</center></html>", {}, "awselb", "no"),
        (b"<html><center>nginx</center></html>", {}, "nginx", "no"),
        (b"Service Unavailable", {"Content-Type": "text/plain"}, "other", "no"),
    ],
)
@pytest.mark.asyncio
async def test_edge_class_comes_from_fixed_body_markers_only(
    caplog: pytest.LogCaptureFixture,
    body: bytes,
    headers: dict[str, str],
    edge: str,
    cf_ray: str,
) -> None:
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    client = MeridianApiClient(
        _CapturingSession(_Response(body, status=503, headers=headers)),  # type: ignore[arg-type]
        tokens=_tokens(),
    )

    with pytest.raises(MeridianUpstreamError) as raised:
        await _measurements(client)

    assert raised.value.gateway is False
    (record,) = _transport_records(caplog)
    assert f" edge={edge} cf_ray={cf_ray} " in record.getMessage()


@pytest.mark.parametrize(
    ("headers", "content"),
    [
        ({"Content-Type": "application/json"}, "json"),
        ({"Content-Type": "application/problem+json; charset=utf-8"}, "json"),
        ({"Content-Type": "text/html"}, "html"),
        ({"Content-Type": "text/plain"}, "text"),
        ({"Content-Type": "application/octet-stream"}, "other"),
        ({}, "none"),
    ],
)
@pytest.mark.asyncio
async def test_content_class_comes_from_the_content_type(
    caplog: pytest.LogCaptureFixture, headers: dict[str, str], content: str
) -> None:
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    body = json.dumps({"data": {"ok": True}}).encode()
    client = MeridianApiClient(
        _CapturingSession(_Response(body, headers=headers)),  # type: ignore[arg-type]
        tokens=_tokens(),
    )

    assert await client._async_graphql("billingPeriods", "query", {}) == {"ok": True}

    (record,) = _transport_records(caplog)
    assert f" status=200 content={content} bytes={len(body)} " in record.getMessage()
    # A JSON answer comes from Meridian's API itself, so no hop is guessed;
    # any other body is checked for an error page's markers.
    edge = "-" if content == "json" else "other"
    assert f" edge={edge} cf_ray=no " in record.getMessage()


@pytest.mark.asyncio
async def test_non_json_success_is_logged_and_still_unreadable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A maintenance page with HTTP 200 is described, then fails as before."""
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    page = b"<html><body>Down for maintenance</body></html>"
    client = MeridianApiClient(
        _CapturingSession(_Response(page, headers={"Content-Type": "text/html"})),  # type: ignore[arg-type]
        tokens=_tokens(),
    )

    with pytest.raises(MeridianConnectionError, match="unreadable") as raised:
        await _measurements(client)

    assert not isinstance(raised.value, MeridianUpstreamError)
    (record,) = _transport_records(caplog)
    assert record.getMessage().startswith(
        f"Meridian response: operation=measurements status=200 content=html "
        f"bytes={len(page)} edge=other cf_ray=no elapsed="
    )
    assert "maintenance" not in caplog.text


@pytest.mark.asyncio
async def test_timeout_before_an_answer_logs_its_class_and_elapsed_time(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    session = _CapturingSession(_EMPTY_PAGE)
    session.post = MagicMock(side_effect=TimeoutError())  # type: ignore[method-assign]
    client = MeridianApiClient(session, tokens=_tokens())  # type: ignore[arg-type]

    with (
        patch.object(transport_module, "monotonic", side_effect=_Clock(75.04)),
        pytest.raises(MeridianUpstreamError) as raised,
    ):
        await _measurements(client)

    assert raised.value.status is None
    assert raised.value.gateway is True
    assert raised.value.detail == "no answer within the time limit"
    (record,) = _transport_records(caplog)
    assert record.getMessage() == (
        "Meridian request timed out: operation=measurements error=TimeoutError "
        "limit=75s status=- content=- cf_ray=- headers_after=- elapsed=75.0s"
    )


@pytest.mark.parametrize(
    ("error", "line"),
    [
        (
            ServerTimeoutError("Timeout on reading data from socket"),
            (
                "Meridian request timed out: operation=measurements "
                "error=ServerTimeoutError limit=75s"
            ),
        ),
        (
            ClientPayloadError("Response payload is not completed"),
            "Meridian request failed: operation=measurements error=ClientPayloadError",
        ),
    ],
)
@pytest.mark.asyncio
async def test_a_body_that_fails_after_the_headers_logs_what_had_arrived(
    caplog: pytest.LogCaptureFixture, error: Exception, line: str
) -> None:
    """
    An answer that started and then stalled is told apart from no answer.

    The status and headers were in; only the body did not finish. The line
    says what had arrived, and when.
    """
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    response = _Response(
        b"",
        headers={"Content-Type": "application/json", "CF-RAY": "8a1b"},
        error=error,
    )
    client = MeridianApiClient(
        _CapturingSession(response),  # type: ignore[arg-type]
        tokens=_tokens(),
    )

    # Readings at the start, once the headers arrive and when the body fails.
    with (
        patch.object(transport_module, "monotonic", side_effect=[100.0, 112.3, 175.0]),
        pytest.raises(MeridianConnectionError) as raised,
    ):
        await _measurements(client)

    (record,) = _transport_records(caplog)
    assert record.getMessage() == (
        f"{line} status=200 content=json cf_ray=yes headers_after=12.3s elapsed=75.0s"
    )
    assert isinstance(raised.value, MeridianUpstreamError) is isinstance(
        error, TimeoutError
    )


@pytest.mark.parametrize(
    ("status", "gateway"), [(503, False), (504, True)], ids=["503", "504"]
)
@pytest.mark.parametrize(
    "error",
    [
        ClientPayloadError("Response payload is not completed"),
        ServerTimeoutError("Timeout on reading data from socket"),
    ],
    ids=["cut-off", "stalled"],
)
@pytest.mark.asyncio
async def test_an_error_page_that_fails_to_arrive_keeps_its_status(
    caplog: pytest.LogCaptureFixture, status: int, gateway: bool, error: Exception
) -> None:
    """
    An error status that arrived classifies the failure, body or not.

    A gateway's error page cut off or stalled after its status line is still
    that gateway's answer. It used to be a network error, or a request with
    no answer at all, so the form and the warning lost the status, and a 504
    did not read as a gateway timeout.
    """
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    response = _Response(
        b"",
        status=status,
        headers={"Content-Type": "text/html", "CF-RAY": "8a1b"},
        error=error,
    )
    client = MeridianApiClient(
        _CapturingSession(response),  # type: ignore[arg-type]
        tokens=_tokens(),
    )

    with pytest.raises(MeridianUpstreamError) as raised:
        await _measurements(client)

    assert raised.value.status == status
    assert raised.value.gateway is gateway
    assert raised.value.detail == f"HTTP {status}"
    assert http_status(raised.value) == status
    # The cause names what happened to the body, by class only.
    cause = raised.value.__cause__
    assert isinstance(cause, MeridianHttpError)
    assert cause.error_class == type(error).__name__
    logged = "".join(traceback.format_exception(raised.value))
    assert str(error) not in logged
    (record,) = _transport_records(caplog)
    assert f"error={type(error).__name__} " in record.getMessage()
    assert f" status={status} content=html cf_ray=yes " in record.getMessage()


@pytest.mark.parametrize(
    "error",
    [ClientPayloadError("not completed"), ServerTimeoutError("read")],
    ids=["cut-off", "stalled"],
)
@pytest.mark.asyncio
async def test_a_rate_limit_whose_page_fails_to_arrive_is_still_a_rate_limit(
    error: Exception,
) -> None:
    """The status and its Retry-After header arrived, so they are honoured."""
    response = _Response(b"", status=429, headers={"Retry-After": "120"}, error=error)
    client = MeridianApiClient(
        _CapturingSession(response),  # type: ignore[arg-type]
        tokens=_tokens(),
    )

    with pytest.raises(MeridianRateLimitError) as raised:
        await _measurements(client)

    assert raised.value.retry_after == 120


@pytest.mark.asyncio
async def test_a_rejected_token_whose_page_fails_to_arrive_is_still_rejected() -> None:
    """A 401 cut off after its status still gets one forced session refresh."""
    refreshed = {
        "id_token": "synthetic-id-token",
        "refresh_token": "synthetic-refresh",
        "expires_in": "3600",
        "user_id": "synthetic-user",
    }
    session = _CapturingSession(
        _Response(
            b"", status=401, headers={}, error=ClientPayloadError("not completed")
        ),
        refreshed,
        _EMPTY_PAGE,
    )
    client = MeridianApiClient(session, tokens=_tokens())  # type: ignore[arg-type]

    await _measurements(client)

    assert [url == api_module._MEASUREMENTS_URL for url, _kwargs in session.posts] == [
        True,
        False,
        True,
    ]


@pytest.mark.asyncio
async def test_no_connection_in_time_is_a_network_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    A connection not made in time is not Meridian failing to answer.

    No request reached Meridian, which with a Cloudflare front end almost
    always means a local network problem, so it stays a connection error.
    This runs a real aiohttp session whose name lookups never answer: aiohttp
    raises ConnectionTimeoutError only for its connect limit, so with a total
    limit alone the stalled lookup used up the whole limit and read as
    Meridian's service not answering in time.
    """
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    async with stalled_session() as session:
        client = MeridianApiClient(session, tokens=_tokens())
        with (
            patch.object(api_module, "_MEASUREMENTS_URL", "https://meridian.invalid/"),
            patch.object(api_module, "MEASUREMENTS_TIMEOUT_SECONDS", 1.0),
            patch.object(transport_module, "CONNECT_TIMEOUT_SECONDS", 0.05),
            pytest.raises(MeridianConnectionError) as raised,
        ):
            await _measurements(client)

    assert not isinstance(raised.value, MeridianUpstreamError)
    assert raised.value.__cause__.error_class == "ConnectionTimeoutError"
    (record,) = _transport_records(caplog)
    assert re.fullmatch(
        r"Meridian request failed: operation=measurements "
        r"error=ConnectionTimeoutError status=- content=- cf_ray=- "
        r"headers_after=- elapsed=0\.\ds",
        record.getMessage(),
    )


@pytest.mark.asyncio
async def test_an_answer_that_never_comes_is_meridian_not_answering(
    caplog: pytest.LogCaptureFixture, socket_enabled: None
) -> None:
    """
    A connection that was made but never answered is a gateway-class timeout.

    A real aiohttp session connects to a local listener that never answers,
    so only the request's own limit ends it, as when Meridian's service is
    too slow. Sockets are allowed for this test, to 127.0.0.1 only.
    """
    del socket_enabled
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    release = asyncio.Event()

    async def never_answer(
        _reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await release.wait()
        writer.close()

    server = await asyncio.start_server(never_answer, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with stalled_session() as session:
            client = MeridianApiClient(session, tokens=_tokens())
            with (
                patch.object(
                    api_module, "_MEASUREMENTS_URL", f"http://127.0.0.1:{port}/"
                ),
                patch.object(api_module, "MEASUREMENTS_TIMEOUT_SECONDS", 1.0),
                patch.object(transport_module, "CONNECT_TIMEOUT_SECONDS", 0.5),
                pytest.raises(MeridianUpstreamError) as raised,
            ):
                await _measurements(client)
    finally:
        release.set()
        server.close()
        await server.wait_closed()

    assert raised.value.status is None
    assert raised.value.gateway is True
    (record,) = _transport_records(caplog)
    # The connection was made at once; only the answer never came.
    assert record.getMessage().startswith(
        "Meridian request timed out: operation=measurements error=TimeoutError "
        "limit=1s status=- content=- cf_ray=- headers_after=- elapsed="
    )


@pytest.mark.parametrize(
    "error",
    [
        ConnectionTimeoutError(f"Connection timeout to host {GRAPHQL_URL}?key=x"),
        ServerTimeoutError(f"Timeout reading {GRAPHQL_URL}"),
        ClientConnectionError(f"Cannot connect to host {GRAPHQL_URL}"),
    ],
)
@pytest.mark.asyncio
async def test_network_error_text_stays_out_of_the_error_chain(
    error: Exception,
) -> None:
    """
    A traceback names the network error by its class, never by its text.

    Network errors put the URL in their text, query string and all, and a
    failed setup logs its traceback.
    """
    session = _CapturingSession(_EMPTY_PAGE)
    session.post = MagicMock(side_effect=error)  # type: ignore[method-assign]
    client = MeridianApiClient(session, tokens=_tokens())  # type: ignore[arg-type]

    with pytest.raises(MeridianConnectionError) as raised:
        await _measurements(client)

    logged = "".join(traceback.format_exception(raised.value))
    assert "://" not in logged
    assert f"({type(error).__name__})" in logged


@pytest.mark.asyncio
async def test_connection_failure_logs_its_class_and_elapsed_time(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    session = _CapturingSession(_EMPTY_PAGE)
    session.post = MagicMock(side_effect=ClientConnectionError("synthetic reset"))  # type: ignore[method-assign]
    client = MeridianApiClient(session, tokens=_tokens())  # type: ignore[arg-type]

    with (
        patch.object(transport_module, "monotonic", side_effect=_Clock(0.5)),
        pytest.raises(MeridianConnectionError, match="Unable to reach") as raised,
    ):
        await _measurements(client)

    assert not isinstance(raised.value, MeridianUpstreamError)
    (record,) = _transport_records(caplog)
    assert record.getMessage() == (
        "Meridian request failed: operation=measurements "
        "error=ClientConnectionError status=- content=- cf_ray=- headers_after=- "
        "elapsed=0.5s"
    )
    assert "synthetic reset" not in caplog.text


@pytest.mark.parametrize(
    ("status", "gateway"),
    [
        (408, False),
        (500, False),
        (502, False),
        (503, False),
        (520, False),
        (523, False),
        (504, True),
        (524, True),
    ],
)
@pytest.mark.asyncio
async def test_upstream_statuses_are_classified(status: int, gateway: bool) -> None:
    client = MeridianApiClient(
        _CapturingSession(_Response(b"", status=status, headers={})),  # type: ignore[arg-type]
        tokens=_tokens(),
    )

    with pytest.raises(MeridianUpstreamError) as raised:
        await _measurements(client)

    assert raised.value.status == status
    assert raised.value.gateway is gateway
    assert raised.value.detail == f"HTTP {status}"
    assert str(raised.value) == f"Meridian service returned HTTP {status}"


@pytest.mark.parametrize("status", [400, 404, 409])
@pytest.mark.asyncio
async def test_other_client_errors_stay_connection_errors(status: int) -> None:
    """
    Meridian's service refusing a request is a connection error of its own kind.

    So the sync can tell a request Meridian refused from a failure to renew
    the session with Google's service, which carries a status too.
    """
    client = MeridianApiClient(
        _CapturingSession(_Response(b"{}", status=status)),  # type: ignore[arg-type]
        tokens=_tokens(),
    )

    with pytest.raises(MeridianRejectedError) as raised:
        await _measurements(client)

    assert isinstance(raised.value, MeridianConnectionError)
    assert not isinstance(raised.value, MeridianUpstreamError)
    assert raised.value.status == status
    assert str(raised.value) == f"Meridian service returned HTTP {status}"


@pytest.mark.asyncio
async def test_a_client_error_renewing_the_session_is_not_meridians() -> None:
    expired = MeridianTokenSet(
        id_token="synthetic-id-token",
        refresh_token="synthetic-refresh",
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
        user_id="synthetic-user",
    )
    client = MeridianApiClient(
        _CapturingSession(_Response(b"{}", status=404)),  # type: ignore[arg-type]
        tokens=expired,
    )

    with pytest.raises(MeridianConnectionError) as raised:
        await _measurements(client)

    assert not isinstance(raised.value, MeridianRejectedError)
    assert str(raised.value) == "Meridian's session service returned HTTP 404"


@pytest.mark.asyncio
async def test_measurements_wait_75_seconds_and_every_other_call_30() -> None:
    """Only measurements get past the gateway's limit of about 60 seconds."""
    firebase = {
        "idToken": "synthetic-id-token",
        "refreshToken": "synthetic-refresh",
        "expiresIn": "3600",
        "localId": "synthetic-user",
    }
    refreshed = {
        "id_token": "synthetic-id-token",
        "refresh_token": "synthetic-refresh",
        "expires_in": "3600",
        "user_id": "synthetic-user",
    }
    billing = {
        "data": {
            "account": {"billingOptions": {"isFixed": False, "periodLength": "MONTHLY"}}
        }
    }
    session = _CapturingSession(
        {"success": True},
        {"customToken": "synthetic-custom-token"},
        firebase,
        refreshed,
        {"data": {"viewer": {"accounts": []}}},
        _EMPTY_PAGE,
        billing,
    )
    client = MeridianApiClient(session)  # type: ignore[arg-type]

    await client.async_send_otp("person@example.com", "journey")
    await client.async_validate_otp("person@example.com", "123456", "journey")
    await client.async_refresh_tokens(force=True)
    await client.async_get_accounts()
    await _measurements(client)
    await client.async_get_billing_period("A-TEST-1")

    timeouts = [post["timeout"] for _url, post in session.posts]
    assert all(isinstance(timeout, ClientTimeout) for timeout in timeouts)
    assert [timeout.total for timeout in timeouts] == [30, 30, 30, 30, 30, 75, 30]
    # Making the connection has its own, shorter limit, so a connection that
    # is never made is told apart from an answer that never comes.
    assert {timeout.connect for timeout in timeouts} == {20}


@pytest.mark.asyncio
async def test_retry_after_header_is_read_whatever_its_case() -> None:
    client = MeridianApiClient(
        _CapturingSession(
            _Response(b"<html></html>", status=429, headers={"retry-after": "120"})
        ),  # type: ignore[arg-type]
        tokens=_tokens(),
    )

    with pytest.raises(MeridianRateLimitError) as raised:
        await _measurements(client)

    assert raised.value.retry_after == 120


@pytest.mark.parametrize("answer", ["no answer", 503, 504, "cut-off 504"])
@pytest.mark.parametrize("step", ["session_refresh", "session_sign_in"])
@pytest.mark.asyncio
async def test_session_service_failures_are_not_meridians(
    answer: str | int, step: str
) -> None:
    """
    Google's session service failing is not Meridian's service failing.

    The login session is renewed with Firebase, not Meridian, so its timeouts
    and server errors stay connection errors and never read as Meridian's
    service, or a gateway in front of it, not answering.
    """
    if answer == "no answer":
        response = _Response(b"", error=TimeoutError())
    elif answer == "cut-off 504":
        response = _Response(
            b"", status=504, headers={}, error=ClientPayloadError("not completed")
        )
    else:
        response = _Response(b"", status=int(answer), headers={})
    if step == "session_refresh":
        session = _CapturingSession(response)
        expired = MeridianTokenSet(
            id_token="synthetic-id-token",
            refresh_token="synthetic-refresh",
            expires_at=datetime.now(UTC) - timedelta(seconds=1),
            user_id="synthetic-user",
        )
        client = MeridianApiClient(session, tokens=expired)  # type: ignore[arg-type]
        attempt = client.async_refresh_tokens()
    else:
        session = _CapturingSession({"customToken": "synthetic-custom-token"}, response)
        client = MeridianApiClient(session)  # type: ignore[arg-type]
        attempt = client.async_validate_otp("person@example.com", "123456", "journey")

    with pytest.raises(MeridianConnectionError) as raised:
        await attempt

    assert not isinstance(raised.value, MeridianUpstreamError)


@pytest.mark.asyncio
async def test_requests_are_counted_where_they_are_sent() -> None:
    """
    A tally counts every HTTP request sent while it counts, as it was sent.

    One client call can send several requests: a session refresh before the
    query, a retry after a rejected token, one request per account. Each
    counts, a failed one too, with its time; nothing counts outside a tally.
    """
    refreshed = {
        "id_token": "synthetic-id-token",
        "refresh_token": "synthetic-refresh",
        "expires_in": "3600",
        "user_id": "synthetic-user",
    }
    session = _CapturingSession(refreshed, _EMPTY_PAGE, _EMPTY_PAGE)
    expired = MeridianTokenSet(
        id_token="synthetic-id-token",
        refresh_token="synthetic-refresh",
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
        user_id="synthetic-user",
    )
    client = MeridianApiClient(session, tokens=expired)  # type: ignore[arg-type]
    # Readings at the start, the headers and the end of each request: the
    # refresh takes 1 s, the page 40 s, and the request that times out 75 s.
    readings = [0.0, 1.0, 1.0, 10.0, 40.0, 50.0, 100.0, 175.0]

    with (
        patch.object(transport_module, "monotonic", side_effect=readings),
        transport_module.count_requests() as tally,
    ):
        await _measurements(client)
        session.post = MagicMock(side_effect=TimeoutError())  # type: ignore[method-assign]
        with pytest.raises(MeridianUpstreamError):
            await _measurements(client)

    assert tally.sent == 3
    assert tally.median_seconds() == pytest.approx(40.0)
    assert tally.median_seconds("measurements") == pytest.approx(57.5)
    assert tally.median_seconds("billingPeriods") is None

    session.post = MagicMock(side_effect=TimeoutError())  # type: ignore[method-assign]
    with pytest.raises(MeridianUpstreamError):
        await _measurements(client)
    assert tally.sent == 3
