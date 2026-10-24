"""
The integration's logs stay free of identifiers, dates, cursors and bodies.

Each scenario runs setup through Home Assistant's real flow manager and
recorder, with the real client, transport and coordinator in front of a
synthetic Meridian service at the HTTP level, and logs at debug level.
Users paste these logs into public issues, so no record of the integration
may carry a value from a request or an answer.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Iterator
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import ClientPayloadError, SocketTimeoutError
from homeassistant.config_entries import SOURCE_USER
from homeassistant.const import CONF_EMAIL
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType

from custom_components.meridian_energy import config_flow as config_flow_module
from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy import transport as transport_module
from custom_components.meridian_energy.const import DOMAIN, FIREBASE_API_KEY

from .kraken_fake import cursor
from .meridian_http import (
    ACCOUNT_NUMBER,
    CUSTOM_TOKEN,
    EMAIL,
    ID_TOKEN,
    PROPERTY_ID,
    REFRESH_TOKEN,
    FakeMeridianSession,
    Response,
    StalledConnection,
    account_data,
    json_response,
    stalled_session,
)

_NOW = datetime(2026, 7, 16, 12, 30, tzinfo=UTC)
_INTEGRATION = "custom_components.meridian_energy"
# A gateway error page as Cloudflare would serve it, showing the client's
# address and an account number.
_HTML_504 = (
    b"<!DOCTYPE html><html><head><title>504 Gateway time-out</title></head>"
    b"<body><p>A-TEST-1</p><p>Your IP: 203.0.113.7</p>"
    b"<p>Cloudflare Ray ID: 8a1b2c3d4e5f6a7b</p></body></html>"
)
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_BASE64_RUN = re.compile(r"[A-Za-z0-9+/]{16,}={0,2}")
# Text that must never reach any record, its traceback included.
_PRIVATE = (
    "203.0.113.7",
    ACCOUNT_NUMBER,
    "Ray ID",
    "8a1b2c3d4e5f6a7b",
    "arrayconnection",
    # A cursor is only ever seen encoded: base64 of "arrayconnection:".
    "YXJyYXljb25uZWN0aW9u",
    PROPERTY_ID,
    EMAIL,
    "Synthetic Street",
    "0000000001SYN00",
    "synthetic-register",
    "synthetic-meter",
    CUSTOM_TOKEN,
    REFRESH_TOKEN,
    ID_TOKEN,
    *ID_TOKEN.split("."),
    "synthetic-user-0123",
    "<html",
    # Any URL, and the key the session requests carry in theirs.
    "://",
    FIREBASE_API_KEY,
)


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(
    recorder_mock: object, enable_custom_integrations: None
) -> None:
    """Set up recorder before enabling this recorder-dependent integration."""


@pytest.fixture
def meridian() -> Iterator[FakeMeridianSession]:
    session = FakeMeridianSession(_NOW)
    with (
        patch.object(
            config_flow_module, "async_get_clientsession", return_value=session
        ),
        patch(f"{_INTEGRATION}.coordinator._utcnow", return_value=_NOW),
        patch(f"{_INTEGRATION}.async_setup_entry", new=AsyncMock(return_value=True)),
    ):
        yield session


@dataclass
class _Scenario:
    """One way setup can go, and what the user should see at its end."""

    name: str
    answers: dict[str, Response] = field(default_factory=dict)
    continuation: Response | None = None
    # An operation whose connection is never made, through real aiohttp.
    stalled: str | None = None
    outcome: str = "create_entry"
    error: str | None = None
    # What the failure form says failed, and the root of the warning's
    # cause chain, both through the real transport and client.
    detail: str | None = None
    root: str | None = None
    # Text one of the integration's records must contain.
    logged: str | None = None
    # A warning the integration must log with its traceback attached, so
    # the privacy checks cover the traceback too.
    warned: str | None = None


_FIRST_PAGE = (
    "grid import history, window 1 of 13, page 1 (account 1 of 1, property 1 of 1)"
)
_DISCOVERY_WARNING = (
    "Meridian account discovery returned data Home Assistant could not use ({})"
)


def _graphql_error(code: str = "KT-CT-4321") -> Response:
    return json_response(
        {
            "errors": [
                {
                    "message": "No property synthetic-property on account A-TEST-1",
                    "extensions": {"errorCode": code},
                }
            ]
        }
    )


def _malformed_page() -> Response:
    node = {"startAt": "2026-07-16 noon for A-TEST-1", "endAt": "x", "unit": "kWh"}
    return json_response(
        {
            "data": {
                "account": {
                    "id": "1",
                    "property": {
                        "measurements": {
                            "pageInfo": {
                                "hasPreviousPage": True,
                                "startCursor": cursor(0),
                            },
                            "edges": [{"node": node}],
                        }
                    },
                }
            }
        }
    )


def _unusable_account() -> Response:
    """
    Return the account with all its identifiers and a date it cannot read.

    The standard message for that date would quote it, with the account
    number beside it, in the warning's traceback.
    """
    account = account_data()
    register = account["properties"][0]["meterPoints"][0]["registers"][0]
    register.update({"isFeedIn": True, "activeFrom": "2026-13-45 for A-TEST-1"})
    return json_response({"data": {"account": account}})


def _with_identifiers(status: int) -> Response:
    return Response(
        json.dumps(
            {"message": "Account A-TEST-1 from 203.0.113.7 failed", "status": status}
        ).encode(),
        status=status,
        headers={"Content-Type": "application/json", "Retry-After": "120"},
    )


_SCENARIOS = [
    _Scenario("success"),
    _Scenario(
        "html_504_first_page",
        answers={
            "measurements": Response(
                _HTML_504,
                status=504,
                headers={"Content-Type": "text/html", "CF-RAY": "8a1b2c3d4e5f6a7b"},
            )
        },
        outcome="import_failed",
        error="upstream_timeout",
        detail=f"HTTP 504 while requesting {_FIRST_PAGE}",
        root="MeridianHttpError",
    ),
    _Scenario(
        "html_504_later_page",
        continuation=Response(
            _HTML_504, status=504, headers={"Content-Type": "text/html"}
        ),
        outcome="import_failed",
        error="upstream_timeout",
        detail=(
            "HTTP 504 while requesting grid import history, window 1 of 13, page 2 "
            "(account 1 of 1, property 1 of 1)"
        ),
        root="MeridianHttpError",
    ),
    _Scenario(
        "no_answer_in_time",
        answers={"measurements": Response(b"", error=TimeoutError())},
        outcome="import_failed",
        error="upstream_timeout",
        detail=f"no answer within the time limit while requesting {_FIRST_PAGE}",
        root="TimeoutError",
    ),
    _Scenario(
        "no_connection_in_time",
        stalled="measurements",
        outcome="import_failed",
        error="import_cannot_connect",
        detail=(
            f"a network error (ConnectionTimeoutError) while requesting {_FIRST_PAGE}"
        ),
        root="ConnectionTimeoutError",
    ),
    _Scenario(
        "body_stalled",
        answers={
            "measurements": Response(
                b"",
                headers={"Content-Type": "application/json", "CF-RAY": "8a1b2c3d"},
                read_error=SocketTimeoutError("Timeout on reading data from socket"),
            )
        },
        outcome="import_failed",
        error="upstream_timeout",
        detail=f"no answer within the time limit while requesting {_FIRST_PAGE}",
        root="SocketTimeoutError",
        # The answer had started: its status and headers were in.
        logged=(
            "error=SocketTimeoutError limit=75s status=200 content=json cf_ray=yes "
            "headers_after="
        ),
    ),
    _Scenario(
        "body_cut_off",
        answers={
            "measurements": Response(
                b"",
                read_error=ClientPayloadError(
                    "Response payload is not completed: <ContentLengthError: 400, "
                    "message='Not enough data to satisfy content length header.'>"
                ),
            )
        },
        outcome="import_failed",
        error="import_cannot_connect",
        detail=f"a network error (ClientPayloadError) while requesting {_FIRST_PAGE}",
        root="ClientPayloadError",
    ),
    _Scenario(
        "html_504_cut_off",
        answers={
            "measurements": Response(
                b"",
                status=504,
                headers={"Content-Type": "text/html", "CF-RAY": "8a1b2c3d"},
                read_error=ClientPayloadError("Response payload is not completed"),
            )
        },
        outcome="import_failed",
        error="upstream_timeout",
        detail=f"HTTP 504 while requesting {_FIRST_PAGE}",
        root="ClientPayloadError",
        # The warning keeps the status that had arrived.
        logged="root=ClientPayloadError status=504 ",
    ),
    _Scenario(
        "service_error_stalled",
        answers={
            "measurements": Response(
                b"",
                status=503,
                headers={"Content-Type": "text/html"},
                read_error=SocketTimeoutError("Timeout on reading data from socket"),
            )
        },
        outcome="import_failed",
        error="upstream_unavailable",
        detail=f"HTTP 503 while requesting {_FIRST_PAGE}",
        root="SocketTimeoutError",
        logged="root=SocketTimeoutError status=503 ",
    ),
    _Scenario(
        "service_error",
        answers={"measurements": _with_identifiers(503)},
        outcome="import_failed",
        error="upstream_unavailable",
    ),
    _Scenario(
        "rejected_request",
        answers={"measurements": _with_identifiers(400)},
        outcome="import_failed",
        error="import_cannot_connect",
    ),
    _Scenario(
        "rate_limited",
        answers={"measurements": _with_identifiers(429)},
        outcome="import_failed",
        error="import_rate_limited",
    ),
    _Scenario(
        "maintenance_page",
        answers={
            "measurements": Response(
                b"<html>Down for maintenance, A-TEST-1, 203.0.113.7</html>",
                headers={"Content-Type": "text/html"},
            )
        },
        outcome="import_failed",
        error="import_cannot_connect",
        detail=f"an unreadable answer (HTTP 200, html) while requesting {_FIRST_PAGE}",
        root="JSONDecodeError",
    ),
    _Scenario(
        "graphql_error",
        # A query over Kraken's limits, which fails the import.
        answers={"measurements": _graphql_error("KT-CT-1188")},
        outcome="import_failed",
        error="import_invalid_data",
    ),
    _Scenario(
        # Every window refused before any reading, as for a supply to come:
        # setup completes with no history, and says so with the code alone.
        "supply_not_started",
        answers={"measurements": _graphql_error()},
        logged=(
            "Meridian rejected a window before returning any reading (KT-CT-4321); "
            "this is taken as no usage history yet (direction=CONSUMPTION)"
        ),
    ),
    _Scenario(
        "malformed_history",
        answers={"measurements": _malformed_page()},
        outcome="import_failed",
        error="import_invalid_data",
    ),
    _Scenario(
        "discovery_gateway_error",
        answers={
            "accountsList": Response(
                _HTML_504, status=504, headers={"Content-Type": "text/html"}
            )
        },
        outcome="discovery_retry",
        error="upstream_timeout",
    ),
    _Scenario(
        "discovery_graphql_error",
        answers={"accountsList": _graphql_error()},
        outcome="discovery_retry",
        error="invalid_data",
        warned=_DISCOVERY_WARNING.format("MeridianGraphQLError"),
    ),
    _Scenario(
        "discovery_unusable_account",
        answers={"account": _unusable_account()},
        outcome="discovery_retry",
        error="invalid_data",
        warned=_DISCOVERY_WARNING.format("ValueError"),
    ),
]


def _integration_records(
    caplog: pytest.LogCaptureFixture,
) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.name.startswith(_INTEGRATION)]


def _exception_lines(formatted: str, message: str) -> str:
    """
    Return the exception lines of a record's traceback, without its frames.

    Frame lines are indented: they hold file paths and source code, which
    the date and base64 checks would trip on; exception messages are not.
    """
    return "\n".join(
        line
        for line in formatted.removeprefix(message).splitlines()
        if line and not line[0].isspace()
    )


def _assert_private(
    records: list[logging.LogRecord], meridian: FakeMeridianSession
) -> None:
    """Fail on any identifier, date, cursor, token, URL or body text in records."""
    assert records
    formatter = logging.Formatter()
    for record in records:
        message = record.getMessage()
        formatted = formatter.format(record)
        for text in (*_PRIVATE, *meridian.payload_dates(), *meridian.cursors):
            assert text not in formatted, (text, message)
        # The message and every exception message in its traceback.
        checked = f"{message}\n{_exception_lines(formatted, message)}"
        assert not _DATE.search(checked), checked
        runs = [
            run
            for run in _BASE64_RUN.findall(checked)
            if re.search(r"\d", run) and re.search(r"[A-Za-z]", run)
        ]
        assert not runs, checked


def _assert_logged(records: list[logging.LogRecord], scenario: _Scenario) -> None:
    """Fail unless the records hold what the scenario says must be logged."""
    # The transport described every answer it read.
    assert any(
        record.getMessage().startswith("Meridian response: operation=")
        for record in records
    )
    if scenario.logged is not None:
        assert any(scenario.logged in record.getMessage() for record in records)
    if scenario.warned is not None:
        (warning,) = [
            record
            for record in records
            if record.levelno == logging.WARNING
            and record.getMessage() == scenario.warned
        ]
        assert warning.exc_info is not None


async def _start_import(hass: HomeAssistant) -> dict[str, Any]:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    assert result["step_id"] == "otp"
    return dict(
        await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )
    )


@pytest.mark.parametrize("scenario", _SCENARIOS, ids=lambda item: item.name)
@pytest.mark.asyncio
async def test_setup_logs_carry_no_private_values(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    scenario: _Scenario,
) -> None:
    caplog.set_level(logging.DEBUG, logger=_INTEGRATION)
    if scenario.continuation is not None:
        # Two channels: the newest week needs a second page.
        meridian.kraken.channels = 2
    async with AsyncExitStack() as stack:
        stalled = await stack.enter_async_context(stalled_session())
        stack.enter_context(
            patch.object(transport_module, "CONNECT_TIMEOUT_SECONDS", 0.05)
        )

        def answer(
            operation: str, variables: dict[str, Any]
        ) -> Response | StalledConnection | None:
            if operation == scenario.stalled:
                return StalledConnection(stalled)
            if operation == "measurements" and variables.get("before"):
                return scenario.continuation
            return scenario.answers.get(operation)

        meridian.hook = answer
        result = await _start_import(hass)
        if result["type"] is FlowResultType.SHOW_PROGRESS:
            await hass.async_block_till_done()
            result = dict(
                await hass.config_entries.flow.async_configure(result["flow_id"])
            )

    if scenario.outcome == "create_entry":
        assert result["type"] is FlowResultType.CREATE_ENTRY
    else:
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == scenario.outcome
        assert result["errors"] == {"base": scenario.error}
        detail = (result.get("description_placeholders") or {}).get("detail", "")
        for text in _PRIVATE:
            assert text not in detail
        if scenario.detail is not None:
            assert detail == scenario.detail
        if scenario.root is not None:
            (warning,) = [
                record
                for record in _integration_records(caplog)
                if record.getMessage().startswith(
                    "Initial Meridian history import failed"
                )
            ]
            assert f" root={scenario.root} " in warning.getMessage()
    _assert_logged(_integration_records(caplog), scenario)
    _assert_private(_integration_records(caplog), meridian)


@pytest.mark.parametrize("ending", ["time_limit", "dialog_closed"])
@pytest.mark.asyncio
async def test_setup_logs_stay_private_when_an_import_is_stopped(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    ending: str,
) -> None:
    """A request still waiting when the attempt is stopped leaves no trace."""
    caplog.set_level(logging.DEBUG, logger=_INTEGRATION)
    gate = asyncio.Event()
    # Two channels: the newest week needs a second page, which stalls.
    meridian.kraken.channels = 2

    def stall(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation == "measurements" and variables.get("before"):
            return Response(_HTML_504, status=504, gate=gate)
        return None

    meridian.hook = stall
    limit = 0.5 if ending == "time_limit" else 600.0
    with (
        patch.object(config_flow_module, "SETUP_IMPORT_TIMEOUT_SECONDS", limit),
        # The second request must start for the limit to stop it in flight:
        # the run's start reserve and pacing would keep it from starting.
        patch.object(coordinator_module, "REQUEST_START_RESERVE_SECONDS", 0),
        patch.object(coordinator_module, "MEASUREMENTS_PACING_SECONDS", 0),
    ):
        result = await _start_import(hass)
        flow_id = result["flow_id"]
        if ending == "time_limit":
            await hass.async_block_till_done()
            result = dict(await hass.config_entries.flow.async_configure(flow_id))
            assert result["errors"] == {"base": "import_timeout"}
        else:
            for _ in range(1000):
                if meridian.operations.count("measurements") == 2:
                    break
                await asyncio.sleep(0.01)
            task = hass.config_entries.flow._progress[flow_id].async_get_progress_task()
            assert task is not None
            hass.config_entries.flow.async_abort(flow_id)
            await asyncio.wait({task}, timeout=10)
            assert task.cancelled()

    records = _integration_records(caplog)
    warnings = [record for record in records if record.levelno == logging.WARNING]
    assert warnings
    # The continuation page was sent, and was waiting when the attempt stopped.
    assert meridian.operations.count("measurements") == 2
    assert " in_flight=yes " in warnings[0].getMessage()
    _assert_private(records, meridian)
