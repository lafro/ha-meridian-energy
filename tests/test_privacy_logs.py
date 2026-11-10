"""
The integration's logs stay free of identifiers, dates, cursors and bodies.

Each scenario signs in through Home Assistant's real flow manager, then sets
the entry up and runs its first sync, with the real recorder, client,
transport and coordinator in front of a synthetic Meridian service at the
HTTP level, and logs at debug level.
Users paste these logs into public issues, so no record of the integration
may carry a value from a request or an answer.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
from homeassistant.config_entries import SOURCE_USER
from homeassistant.const import CONF_EMAIL
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers.update_coordinator import UpdateFailed

import custom_components.meridian_energy as integration_module
from custom_components.meridian_energy import config_flow as config_flow_module
from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy.api import exception_chain
from custom_components.meridian_energy.const import DOMAIN, FIREBASE_API_KEY

from .meridian_http import (
    ACCOUNT_NUMBER,
    CUSTOM_TOKEN,
    EMAIL,
    ID_TOKEN,
    PROPERTY_ID,
    REFRESH_TOKEN,
    FakeMeridianSession,
    Response,
    json_response,
    prices_data,
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
    "Synthetic Plan",
    # The prices' amounts, and the words of one that can't be read.
    "2.000000",
    "0.300000",
    "Synthetic rate",
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
    # A billing period, so the bill's day totals are asked for too.
    today = (_NOW + timedelta(hours=12)).date()
    session.billing_period = (today - timedelta(days=10), today + timedelta(days=20))
    with (
        patch.object(
            config_flow_module, "async_get_clientsession", return_value=session
        ),
        patch.object(
            integration_module, "async_get_clientsession", return_value=session
        ),
        patch(f"{_INTEGRATION}.coordinator._utcnow", return_value=_NOW),
    ):
        yield session


@dataclass
class _Scenario:
    """One way setup can go, and what the user should see at its end."""

    name: str
    answers: dict[str, Response] = field(default_factory=dict)
    # Whether the first sync succeeds; when not, its failure's key and the
    # root of its cause chain, through the real transport and client.
    synced: bool = True
    error: str | None = None
    root: str | None = None


_SCENARIOS = [
    _Scenario("success"),
    # A gateway's error page carrying the client's address and an account
    # number, as the first answer to every measurements request.
    _Scenario(
        "html_504",
        answers={
            "measurements": Response(
                _HTML_504,
                status=504,
                headers={"Content-Type": "text/html", "CF-RAY": "8a1b2c3d4e5f6a7b"},
            )
        },
        synced=False,
        error="cannot_connect",
        root="MeridianHttpError",
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


def _assert_request_lines(records: list[logging.LogRecord], *, synced: bool) -> None:
    """
    Fail unless each request logged its line, a window only by day offsets.

    A sync that succeeded also asked for the bill's day totals, the balance
    and the prices.
    """
    lines = [
        record.getMessage()
        for record in records
        if record.getMessage().startswith("Meridian request: operation=")
    ]
    measurements = [line for line in lines if "operation=measurements" in line]
    assert measurements
    for line in measurements:
        assert re.fullmatch(
            r"Meridian request: operation=measurements grid_import "
            r"(window=-?\d+\.\.-?\d+d|bill_days) page=\d status=(\d+|-) "
            r"content=\w+ (bytes=\d+|error=\w+) elapsed=\d+\.\ds",
            line,
        ), line
    if synced:
        assert any("bill_days" in line for line in measurements)
        for operation in ("balance", "prices"):
            assert any(
                re.fullmatch(
                    rf"Meridian request: operation={operation} status=200 "
                    r"content=\w+ bytes=\d+ elapsed=\d+\.\ds",
                    line,
                )
                for line in lines
            ), operation


def _root_name(err: BaseException) -> str:
    """
    Return the class of the innermost error in a failure's chain.

    A network error is not chained, so its class is read from the transport
    error that stands for it.
    """
    root = exception_chain(err)[-1]
    return getattr(root, "error_class", None) or type(root).__name__


def _assert_failed_privately(failure: Exception | None, scenario: _Scenario) -> None:
    """Fail unless the first sync failed as the scenario says, naming nothing."""
    assert isinstance(failure, UpdateFailed)
    assert failure.translation_key == scenario.error
    for value in (failure.translation_placeholders or {}).values():
        for text in _PRIVATE:
            assert text not in value
    assert _root_name(failure) == scenario.root


async def _sign_in(hass: HomeAssistant) -> dict[str, Any]:
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
async def test_setup_and_first_sync_logs_carry_no_private_values(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    scenario: _Scenario,
) -> None:
    caplog.set_level(logging.DEBUG, logger=_INTEGRATION)
    meridian.hook = lambda operation, _variables: scenario.answers.get(operation)

    result = await _sign_in(hass)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    entry = result["result"]
    await hass.async_block_till_done(wait_background_tasks=True)
    coordinator = entry.runtime_data.coordinator
    failure = coordinator.last_exception
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert coordinator.last_update_success is scenario.synced
    if not scenario.synced:
        _assert_failed_privately(failure, scenario)
    _assert_request_lines(_integration_records(caplog), synced=scenario.synced)
    _assert_private(_integration_records(caplog), meridian)


@pytest.mark.asyncio
async def test_a_price_that_cannot_be_read_is_logged_by_its_shape_alone(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Its WARNING says how the text looks, never what it says."""
    caplog.set_level(logging.DEBUG, logger=_INTEGRATION)
    meridian.prices = prices_data(anytime=f"$0.300000 Synthetic rate {ACCOUNT_NUMBER}")

    result = await _sign_in(hass)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    await hass.async_block_till_done(wait_background_tasks=True)
    assert await hass.config_entries.async_unload(result["result"].entry_id)
    await hass.async_block_till_done()

    records = _integration_records(caplog)
    assert [
        record.getMessage() for record in records if record.levelno == logging.WARNING
    ] == [
        (
            "Meridian Energy can't read the anytime price in "
            "formattedRateIncludingTax, which looks like '$N.NNNNNN AAAAAAAAA "
            "AAA…' (N a digit, A a letter); Anytime price shows unknown"
        )
    ]
    _assert_private(records, meridian)


@pytest.mark.asyncio
async def test_logs_stay_private_when_a_first_sync_is_stopped(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A request still waiting when the entry is unloaded leaves no trace."""
    caplog.set_level(logging.DEBUG, logger=_INTEGRATION)
    gate = asyncio.Event()
    # Two channels: the newest week needs a second page, which stalls.
    meridian.kraken.channels = 2

    def stall(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation == "measurements" and variables.get("before"):
            return Response(_HTML_504, status=504, gate=gate)
        return None

    meridian.hook = stall
    # The second request must start before the entry is unloaded: pacing
    # would hold it back.
    with patch.object(coordinator_module, "MEASUREMENTS_PACING_SECONDS", 0):
        result = await _sign_in(hass)
        assert result["type"] is FlowResultType.CREATE_ENTRY
        entry = result["result"]
        for _ in range(1000):
            if meridian.operations.count("measurements") == 2:
                break
            await asyncio.sleep(0.01)
        assert meridian.operations.count("measurements") == 2
        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

    records = _integration_records(caplog)
    # The continuation page was sent, and was waiting when the sync stopped.
    assert meridian.operations.count("measurements") == 2
    assert not gate.is_set()
    _assert_private(records, meridian)


@pytest.mark.asyncio
async def test_a_session_renewal_with_an_unreadable_expiry_quotes_nothing(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    An expiry that is no number reaches no record, on an update or a reload.

    The token's renewal comes at an update, when it is due, and at every
    setup; both log the failure's traceback at DEBUG.
    """
    caplog.set_level(logging.DEBUG, logger=_INTEGRATION)
    marker = "synthetic-expiry-marker"
    result = await _sign_in(hass)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    entry = result["result"]
    await hass.async_block_till_done(wait_background_tasks=True)
    coordinator = entry.runtime_data.coordinator
    coordinator.client._tokens = replace(
        coordinator.client._tokens,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    meridian.auth_hook = lambda operation: (
        json_response(
            {
                "id_token": ID_TOKEN,
                "refresh_token": REFRESH_TOKEN,
                "expires_in": marker,
                "user_id": "synthetic-user-0123",
            }
        )
        if operation == "session_refresh"
        else None
    )
    caplog.clear()

    await coordinator.async_refresh()
    assert not coordinator.last_update_success
    assert not await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()

    formatter = logging.Formatter()
    tracebacks = [record for record in caplog.records if record.exc_info]
    assert {record.getMessage() for record in tracebacks} >= {
        "Full error:",
        "Full exception",
    }
    for record in caplog.records:
        assert marker not in formatter.format(record), record.getMessage()
    _assert_private(_integration_records(caplog), meridian)
    assert await hass.config_entries.async_unload(entry.entry_id)
