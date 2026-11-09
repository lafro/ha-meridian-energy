"""Tests for the Meridian Energy config flow."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import probatio
import pytest
from homeassistant.config_entries import (
    SOURCE_IGNORE,
    SOURCE_REAUTH,
    SOURCE_USER,
    ConfigEntryState,
    ConfigFlowResult,
)
from homeassistant.const import CONF_EMAIL
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType, UnknownFlow
from homeassistant.helpers import config_validation as cv
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.meridian_energy import api as api_module
from custom_components.meridian_energy import config_flow as config_flow_module
from custom_components.meridian_energy import transport as transport_module
from custom_components.meridian_energy.api import (
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianOtpError,
    MeridianRateLimitError,
    MeridianRejectedError,
    MeridianSignInServiceUnreadableError,
    MeridianUpstreamError,
)
from custom_components.meridian_energy.config_flow import MeridianEnergyConfigFlow
from custom_components.meridian_energy.const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    DOMAIN,
    NAME,
)
from custom_components.meridian_energy.models import (
    MeridianAccount,
    MeridianMeterPoint,
    MeridianProperty,
    MeridianTokenSet,
)

from .meridian_http import (
    STALLED_URL,
    FakeMeridianSession,
    Response,
    StalledConnection,
    account_data,
    json_response,
    stalled_session,
)

_NOW = datetime(2026, 7, 16, 12, 30, tzinfo=UTC)
_TEST_ACCOUNT = "A-TEST-1"
_TEST_EMAIL = "person@example.com"


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(
    recorder_mock: object, enable_custom_integrations: None
) -> None:
    """Set up recorder before enabling this recorder-dependent integration."""


def _tokens(refresh: str = "synthetic-refresh") -> MeridianTokenSet:
    return MeridianTokenSet(
        id_token="synthetic-id",
        refresh_token=refresh,
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        user_id="synthetic-user",
    )


def _account(number: str = _TEST_ACCOUNT) -> MeridianAccount:
    return MeridianAccount(
        number=number,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id=f"property-{number}",
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint(
                        id=f"meter-{number}",
                        market_identifier=f"icp-{number}",
                        has_feed_in=False,
                    ),
                ),
            ),
        ),
    )


class _FakeMeridian:
    """
    A synthetic Meridian client for sign-in and account discovery.

    It has no measurements or billing methods: the flow must not ask for
    either, as the entry's own first update does. Every call awaits once
    before answering, because a real request never answers at once.
    """

    def __init__(self) -> None:
        self.async_send_otp = AsyncMock(side_effect=self._answer)
        self.async_validate_otp = AsyncMock(return_value=_tokens())
        self.async_get_accounts = AsyncMock(return_value=(_account(),))

    @staticmethod
    async def _answer(*_args: object) -> None:
        await asyncio.sleep(0)


@pytest.fixture
def meridian() -> Iterator[_FakeMeridian]:
    """Route the flow's clients to the fake, and stub the entry's own setup."""
    fake = _FakeMeridian()
    with (
        patch.object(
            MeridianEnergyConfigFlow,
            "_client",
            new_callable=PropertyMock,
            return_value=fake,
        ),
        patch.object(
            MeridianEnergyConfigFlow, "_authenticated_client", return_value=fake
        ),
        patch(
            "custom_components.meridian_energy.async_setup_entry",
            new=AsyncMock(return_value=True),
        ),
    ):
        yield fake


def _flow_ids(hass: HomeAssistant) -> set[str]:
    return {
        flow["flow_id"]
        for flow in hass.config_entries.flow.async_progress(include_uninitialized=True)
    }


async def _start_user_flow(
    hass: HomeAssistant, email: str = _TEST_EMAIL
) -> ConfigFlowResult:
    """Start a setup and send the email: the flow waits at the code form."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    assert result["step_id"] == "user"
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: email}
    )
    assert result["step_id"] == "otp"
    return result


async def _submit_login(
    hass: HomeAssistant, email: str = _TEST_EMAIL
) -> ConfigFlowResult:
    result = await _start_user_flow(hass, email)
    return await hass.config_entries.flow.async_configure(
        result["flow_id"], {"otp": "123456"}
    )


def _configured_entry(hass: HomeAssistant, email: str = _TEST_EMAIL) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id=email,
        version=4,
        data={
            CONF_EMAIL: email,
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
        },
    )
    entry.add_to_hass(hass)
    return entry


@pytest.mark.asyncio
async def test_setup_creates_the_entry_once_the_code_is_accepted(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    The code accepted and an account found, the entry is created at once.

    It holds the login alone: the history is the entry's own first update's
    to fetch, in the background, and every account is followed.
    """
    result = await _submit_login(hass, "  Person@Example.COM ")

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == NAME
    assert result["data"] == {
        CONF_EMAIL: _TEST_EMAIL,
        CONF_REFRESH_TOKEN: "synthetic-refresh",
        CONF_FIREBASE_USER_ID: "synthetic-user",
    }
    # The dialog shows the create_entry text, which says what happens next.
    assert result.get("description") is None
    (entry,) = hass.config_entries.async_entries(DOMAIN)
    assert (entry.version, entry.minor_version) == (4, 1)
    assert entry.unique_id == _TEST_EMAIL
    meridian.async_send_otp.assert_awaited_once()
    assert meridian.async_send_otp.await_args.args[0] == _TEST_EMAIL
    sent_journey = meridian.async_send_otp.await_args.args[1]
    assert meridian.async_validate_otp.await_args.args == (
        _TEST_EMAIL,
        "123456",
        sent_journey,
    )
    meridian.async_get_accounts.assert_awaited_once()
    assert _flow_ids(hass) == set()


@pytest.mark.asyncio
async def test_a_login_with_several_accounts_follows_them_all(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """Two accounts make one entry, with no form to choose between them."""
    meridian.async_get_accounts.return_value = (_account(), _account("A-TEST-2"))

    result = await _submit_login(hass)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert set(result["data"]) == {
        CONF_EMAIL,
        CONF_REFRESH_TOKEN,
        CONF_FIREBASE_USER_ID,
    }
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1


@pytest.mark.asyncio
async def test_a_login_where_only_one_account_has_a_property_is_set_up(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """Only a login with no property on any account is told it has none."""
    meridian.async_get_accounts.return_value = (
        _account(),
        MeridianAccount(number="A-TEST-2", status="ACTIVE", properties=()),
    )

    result = await _submit_login(hass)

    assert result["type"] is FlowResultType.CREATE_ENTRY


@pytest.mark.asyncio
async def test_otp_form_is_serializable_and_validates_locally(hass) -> None:
    """The native config-flow API must be able to serialize the OTP form."""
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._email = _TEST_EMAIL
    flow._journey_id = "journey"
    client = MagicMock()
    client.async_validate_otp = AsyncMock()

    result = await flow.async_step_otp()
    probatio.to_field_list(
        result["data_schema"], custom_serializer=cv.custom_serializer
    )

    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await flow.async_step_otp({"otp": "12ab"})

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "otp_invalid"}
    client.async_validate_otp.assert_not_awaited()


@pytest.mark.asyncio
async def test_user_flow_prevents_duplicate(hass) -> None:
    _configured_entry(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_EMAIL: _TEST_EMAIL},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


@pytest.mark.asyncio
async def test_adding_over_an_entry_refused_as_too_old_says_to_delete_it(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """An entry an earlier version set up must be deleted first; no code is sent."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id=_TEST_EMAIL,
        version=3,
        data={
            CONF_EMAIL: _TEST_EMAIL,
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
        },
    )
    entry.add_to_hass(hass)
    assert not await hass.config_entries.async_setup(entry.entry_id)
    assert entry.state is ConfigEntryState.MIGRATION_ERROR

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_EMAIL: f" {_TEST_EMAIL.upper()} "},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "earlier_entry"
    meridian.async_send_otp.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_entry_a_later_version_set_up_is_already_set_up(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    After a rollback, a later version's entry fails to migrate too.

    It is no earlier version's, so the abort says only that the login is
    set up.
    """
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id=_TEST_EMAIL,
        version=5,
        data={
            CONF_EMAIL: _TEST_EMAIL,
            CONF_REFRESH_TOKEN: "synthetic-refresh",
            CONF_FIREBASE_USER_ID: "synthetic-user",
        },
    )
    entry.add_to_hass(hass)
    entry.mock_state(hass, ConfigEntryState.MIGRATION_ERROR)

    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}, data={CONF_EMAIL: _TEST_EMAIL}
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    meridian.async_send_otp.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_ignored_entry_never_blocks_adding_the_login(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """A login an ignored entry holds can be added, whatever the entry's state."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id=_TEST_EMAIL,
        version=3,
        source=SOURCE_IGNORE,
        data={},
    )
    entry.add_to_hass(hass)
    entry.mock_state(hass, ConfigEntryState.MIGRATION_ERROR)

    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}, data={CONF_EMAIL: _TEST_EMAIL}
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "otp"
    meridian.async_send_otp.assert_awaited_once()


# Errors from a sign-in request that got no usable answer, as each form names
# them: a rate limit names the service that asked to wait, an answer that
# can't be read says so, and everything else is the message about getting an
# answer from Meridian.
_CONNECTION_ERRORS = [
    (MeridianConnectionError(), "cannot_connect"),
    (MeridianUpstreamError(504), "cannot_connect"),
    (MeridianUpstreamError(None), "cannot_connect"),
    (MeridianUpstreamError(503), "cannot_connect"),
    (MeridianRejectedError(400), "cannot_connect"),
    (MeridianRateLimitError(120), "rate_limited"),
    (MeridianRateLimitError(120, sign_in_service=True), "sign_in_rate_limited"),
    # An answer that can't be read, which says nothing of the sign-in.
    (ValueError("unreadable"), "sign_in_unreadable"),
    (MeridianSignInServiceUnreadableError("unexpected data"), "sign_in_unreadable"),
]


@pytest.mark.parametrize(
    ("error", "expected"),
    [*_CONNECTION_ERRORS, (MeridianAuthenticationError(), "email_not_found")],
)
@pytest.mark.asyncio
async def test_user_flow_send_errors(hass, error: Exception, expected: str) -> None:
    client = MagicMock()
    client.async_send_otp = AsyncMock(side_effect=error)
    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_EMAIL: _TEST_EMAIL}
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"
    assert result["errors"] == {"base": expected}


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("OTP_EXPIRED", "otp_expired"),
        ("OTP_INVALID", "otp_invalid"),
        ("OTP_TOO_MANY_ATTEMPTS", "otp_too_many_attempts"),
        ("OTP_NOT_FOUND", "otp_not_found"),
        ("OTHER", "invalid_auth"),
    ],
)
@pytest.mark.asyncio
async def test_otp_errors(
    hass, meridian: _FakeMeridian, code: str, expected: str
) -> None:
    meridian.async_validate_otp.side_effect = MeridianOtpError(code, "hidden")

    result = await _submit_login(hass)

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "otp"
    assert result["errors"] == {"base": expected}


@pytest.mark.parametrize(
    ("error", "expected"),
    [*_CONNECTION_ERRORS, (MeridianAuthenticationError(), "invalid_auth")],
)
@pytest.mark.asyncio
async def test_otp_service_errors(
    hass, meridian: _FakeMeridian, error: Exception, expected: str
) -> None:
    meridian.async_validate_otp.side_effect = error

    result = await _submit_login(hass)

    assert result["step_id"] == "otp"
    assert result["errors"] == {"base": expected}


@pytest.mark.parametrize(
    ("status", "code", "expected"),
    [
        (429, "OTP_TOO_MANY_ATTEMPTS", "otp_too_many_attempts"),
        (429, None, "rate_limited"),
        (400, "OTP_EXPIRED", "otp_expired"),
    ],
)
@pytest.mark.asyncio
async def test_a_refused_code_is_named_whatever_the_status(
    hass: HomeAssistant, status: int, code: str | None, expected: str
) -> None:
    """
    Through the real client: a refused code's own name comes before a 429.

    Too many tries may come with HTTP 429; read as a rate limit, the form
    told the user to wait, when only a new code helps.
    """
    session = FakeMeridianSession(_NOW)
    body = {"code": code} if code is not None else {}

    def refuse(operation: str) -> Response | None:
        if operation != "check_code":
            return None
        return Response(
            json.dumps(body).encode(),
            status=status,
            headers={"Content-Type": "application/json", "Retry-After": "60"},
        )

    session.auth_hook = refuse
    with patch.object(
        config_flow_module, "async_get_clientsession", return_value=session
    ):
        result = await _submit_login(hass)

    assert result["step_id"] == "otp"
    assert result["errors"] == {"base": expected}


@pytest.mark.asyncio
async def test_reauth_updates_existing_entry(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    entry = _configured_entry(hass)
    with patch.object(hass.config_entries, "async_reload", AsyncMock()):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=dict(entry.data),
        )
        assert result["step_id"] == "reauth_confirm"
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
        assert result["step_id"] == "otp"
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    assert entry.data[CONF_REFRESH_TOKEN] == "synthetic-refresh"
    # Reauthentication renews the session alone: no accounts are looked for.
    meridian.async_get_accounts.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_second_reauth_confirmation_sends_no_second_code(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """A confirmation sent again while the code is being sent sends no other."""
    entry = _configured_entry(hass)
    sending = asyncio.Event()
    finish_sending = asyncio.Event()

    async def send(*_args: object) -> None:
        await asyncio.sleep(0)
        if not sending.is_set():  # Only the first send is slow.
            sending.set()
            await finish_sending.wait()

    meridian.async_send_otp.side_effect = send
    with patch.object(hass.config_entries, "async_reload", AsyncMock()):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=dict(entry.data),
        )
        flow_id = result["flow_id"]
        first = hass.async_create_task(
            hass.config_entries.flow.async_configure(flow_id, {})
        )
        await sending.wait()
        second = hass.async_create_task(
            hass.config_entries.flow.async_configure(flow_id, {})
        )
        await asyncio.sleep(0.05)
        assert not second.done()
        finish_sending.set()
        for answer in (await first, await second):
            assert answer["type"] is FlowResultType.FORM
            assert answer["step_id"] == "otp"
        meridian.async_send_otp.assert_awaited_once()
        result = await hass.config_entries.flow.async_configure(
            flow_id, {"otp": "123456"}
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    sent_journey = meridian.async_send_otp.await_args.args[1]
    assert meridian.async_validate_otp.await_args.args[2] == sent_journey
    assert entry.data[CONF_REFRESH_TOKEN] == "synthetic-refresh"


@pytest.mark.parametrize(
    ("error", "expected"),
    [*_CONNECTION_ERRORS, (MeridianAuthenticationError(), "reauth_email_refused")],
)
@pytest.mark.asyncio
async def test_reauth_send_errors(hass, error: Exception, expected: str) -> None:
    """
    Reauthentication names its failures as setup does, but for the email.

    Its form has no email to correct, so a refused email says to set
    Meridian Energy up again with the one now used for MyMeridian.
    """
    entry = _configured_entry(hass)
    client = MagicMock()
    client.async_send_otp = AsyncMock(side_effect=error)

    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await entry.start_reauth_flow(hass)
        assert result["step_id"] == "reauth_confirm"
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})

    assert result["step_id"] == "reauth_confirm"
    assert result["errors"] == {"base": expected}


@pytest.mark.asyncio
async def test_expired_flow_guards(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}

    otp_result = await flow.async_step_otp()
    confirm_result = await flow.async_step_reauth_confirm({})
    discovery_result = await flow.async_step_discovery_retry({})

    assert otp_result["reason"] == "login_expired"
    assert confirm_result["reason"] == "login_expired"
    assert discovery_result["reason"] == "login_expired"


@pytest.mark.asyncio
async def test_a_login_code_that_cannot_be_sent_for_no_connection(
    hass: HomeAssistant,
) -> None:
    """
    A connection that is never made gives the message about reaching Meridian.

    A real aiohttp session whose name lookups never answer stands in for a
    stalled local network.
    """
    async with stalled_session() as stalled:
        with (
            patch.object(
                config_flow_module, "async_get_clientsession", return_value=stalled
            ),
            patch.object(api_module, "AUTH_EMAIL_URL", STALLED_URL),
            patch.object(transport_module, "CONNECT_TIMEOUT_SECONDS", 0.05),
        ):
            result = await hass.config_entries.flow.async_init(
                DOMAIN, context={"source": SOURCE_USER}
            )
            result = await hass.config_entries.flow.async_configure(
                result["flow_id"], {CONF_EMAIL: _TEST_EMAIL}
            )

    assert result["step_id"] == "user"
    assert result["errors"] == {"base": "cannot_connect"}


@pytest.mark.asyncio
async def test_client_property_uses_home_assistant_session(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    assert flow._client is not None


@pytest.mark.asyncio
async def test_account_discovery_retries_without_reusing_otp(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    meridian.async_get_accounts.side_effect = [MeridianConnectionError(), (_account(),)]

    result = await _submit_login(hass)
    assert result["step_id"] == "discovery_retry"
    assert result["errors"] == {"base": "cannot_connect"}
    result = await hass.config_entries.flow.async_configure(result["flow_id"], {})

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert meridian.async_get_accounts.await_count == 2
    meridian.async_validate_otp.assert_awaited_once()


@pytest.mark.asyncio
async def test_account_discovery_keeps_credentials_meridian_rotated(
    hass: HomeAssistant,
) -> None:
    """A session renewed while the accounts were looked for is the one stored."""
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(return_value=_tokens())
    client.async_get_accounts = AsyncMock(
        side_effect=[MeridianConnectionError(), (_account(),)]
    )
    rotated = MeridianTokenSet(
        id_token="rotated-id",
        refresh_token="rotated-refresh",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        user_id="rotated-user",
    )
    with (
        patch.object(
            config_flow_module, "MeridianApiClient", return_value=client
        ) as client_class,
        patch(
            "custom_components.meridian_energy.async_setup_entry",
            new=AsyncMock(return_value=True),
        ),
    ):
        result = await _submit_login(hass)
        assert result["step_id"] == "discovery_retry"
        await client_class.call_args.kwargs["token_update_callback"](rotated)
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_REFRESH_TOKEN] == "rotated-refresh"
    assert result["data"][CONF_FIREBASE_USER_ID] == "rotated-user"


@pytest.mark.asyncio
async def test_rotated_tokens_before_a_login_are_ignored(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    await flow._async_store_discovery_tokens(_tokens("rotated"))
    assert flow._pending_data is None


# A gateway's error page, with nothing in it the logs may repeat.
_GATEWAY_PAGE = b"<html><body>504 Gateway Time-out from 203.0.113.7</body></html>"


def _discovery_failure(kind: str, stalled: Any) -> Response | StalledConnection:
    """Return the answer to the account list for one kind of failure."""
    if kind == "no_connection":
        return StalledConnection(stalled)
    # Meridian's free-text message, which must reach no record.
    message = f"No property synthetic-property on account {_TEST_ACCOUNT}"
    answers = {
        "gateway_timeout": Response(
            _GATEWAY_PAGE, status=504, headers={"Content-Type": "text/html"}
        ),
        "service_error": Response(b"{}", status=503),
        "rate_limited": Response(
            b"{}",
            status=429,
            headers={"Content-Type": "application/json", "Retry-After": "120"},
        ),
        "rejected": Response(
            b'{"message": "Account A-TEST-1 refused"}',
            status=400,
            headers={"Content-Type": "application/json"},
        ),
        "graphql_error": json_response(
            {"errors": [{"message": message, "extensions": {"errorCode": "KT-CT-1"}}]}
        ),
        # Errors without a code, as for a failure inside Meridian's service.
        "uncoded_graphql_error": json_response({"errors": [{"message": message}]}),
        "mixed_graphql_errors": json_response(
            {
                "errors": [
                    {"message": message},
                    {"message": message, "extensions": {"errorCode": "KT-CT-1"}},
                ]
            }
        ),
    }
    return answers.get(
        kind, json_response({"data": {"viewer": {"accounts": "not a list"}}})
    )


_DISCOVERY_FAILURES = [
    ("gateway_timeout", "cannot_connect", "MeridianUpstreamError status=504"),
    ("service_error", "cannot_connect", "MeridianUpstreamError status=503"),
    ("rate_limited", "rate_limited", "MeridianRateLimitError status=429"),
    ("no_connection", "cannot_connect", "MeridianConnectionError status=-"),
    ("rejected", "invalid_data", "MeridianRejectedError"),
    ("graphql_error", "invalid_data", "MeridianGraphQLError"),
    ("uncoded_graphql_error", "cannot_connect", "MeridianGraphQLError status=-"),
    ("mixed_graphql_errors", "invalid_data", "MeridianGraphQLError"),
    ("unusable_data", "invalid_data", "ValueError"),
]
_RESENT = {"gateway_timeout", "service_error", "no_connection"}


@pytest.mark.parametrize(
    "failure", _DISCOVERY_FAILURES, ids=[kind for kind, *_ in _DISCOVERY_FAILURES]
)
@pytest.mark.asyncio
async def test_account_discovery_failures_are_named_and_logged(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
    failure: tuple[str, str, str],
) -> None:
    """
    Finding the accounts says what failed and logs it, then Submit tries again.

    A refusal of the request (a client error, or a GraphQL error with a
    code), or data Home Assistant cannot use, is a defect to report, so it
    leaves one warning with its traceback, and the form says a retry asks
    the same. A failure to get an answer, GraphQL errors without a code
    included, is already on the form, and the transport describes the
    answer, so it gets one debug line. The real client and transport run in
    front of a synthetic service, through Home Assistant's flow manager.
    """
    kind, expected, error = failure
    caplog.set_level(logging.DEBUG, logger=config_flow_module.__name__)
    session = FakeMeridianSession(_NOW)
    async with stalled_session() as stalled:
        # A failure that may pass is sent once more before the form shows it.
        failures = {"remaining": 2 if kind in _RESENT else 1}

        def fail_the_account_list(
            operation: str, _variables: dict[str, Any]
        ) -> Response | StalledConnection | None:
            if operation == "accountsList" and failures["remaining"]:
                failures["remaining"] -= 1
                return _discovery_failure(kind, stalled)
            return None

        session.hook = fail_the_account_list
        with (
            patch.object(
                config_flow_module, "async_get_clientsession", return_value=session
            ),
            patch.object(transport_module, "CONNECT_TIMEOUT_SECONDS", 0.05),
            patch(
                "custom_components.meridian_energy.async_setup_entry",
                new=AsyncMock(return_value=True),
            ),
        ):
            result = await _submit_login(hass)
            assert result["type"] is FlowResultType.FORM
            assert result["step_id"] == "discovery_retry"
            assert result["errors"] == {"base": expected}
            records = [
                record
                for record in caplog.records
                if record.name == config_flow_module.__name__
            ]
            # Submitting the form tries again, and this time the list comes.
            retried = await hass.config_entries.flow.async_configure(
                result["flow_id"], {}
            )

    assert retried["type"] is FlowResultType.CREATE_ENTRY
    if expected == "invalid_data":
        (record,) = records
        assert record.levelno == logging.WARNING
        assert record.exc_info is not None
        assert type(record.exc_info[1]).__name__ == error
        # The error's own words, which name no class.
        assert record.getMessage() == (
            "Meridian refused account discovery or returned data Home Assistant "
            f"could not use: {record.exc_info[1]}"
        )
        assert error not in record.getMessage()
    else:
        assert [(record.levelno, record.getMessage()) for record in records] == [
            (logging.DEBUG, f"Meridian account discovery failed: error={error}")
        ]
    # caplog.text holds each record as formatted, its traceback included.
    for text in ("203.0.113.7", "synthetic-property", _TEST_ACCOUNT):
        assert text not in caplog.text


@pytest.mark.asyncio
async def test_account_discovery_auth_failure_requires_new_login(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    meridian.async_get_accounts.side_effect = MeridianAuthenticationError()

    result = await _submit_login(hass)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "login_expired"
    assert hass.config_entries.async_entries(DOMAIN) == []


@pytest.mark.asyncio
async def test_account_discovery_aborts_when_none_are_available(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    meridian.async_get_accounts.return_value = ()

    result = await _submit_login(hass)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "no_accounts"
    assert hass.config_entries.async_entries(DOMAIN) == []


@pytest.mark.asyncio
async def test_an_account_with_no_property_is_told_apart_from_none(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """The account is listed, with no property on it."""
    meridian.async_get_accounts.return_value = (
        MeridianAccount(number=_TEST_ACCOUNT, status="ACTIVE", properties=()),
    )

    result = await _submit_login(hass)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "no_properties"
    assert hass.config_entries.async_entries(DOMAIN) == []


async def _answer_sign_in_step(
    hass: HomeAssistant, step: str, answer: Response
) -> ConfigFlowResult:
    """Run a setup through the real client, one sign-in step answered so."""
    session = FakeMeridianSession(_NOW)
    session.auth_hook = lambda operation: answer if operation == step else None
    with patch.object(
        config_flow_module, "async_get_clientsession", return_value=session
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_EMAIL: _TEST_EMAIL}
        )
        if step != "send_code":
            result = await hass.config_entries.flow.async_configure(
                result["flow_id"], {"otp": "123456"}
            )
    return result


@pytest.mark.parametrize("step", ["send_code", "check_code", "session_sign_in"])
@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        (
            Response(
                b"<html>Forbidden</html>", status=403, headers={"Content-Type": ""}
            ),
            "cannot_connect",
        ),
        (
            Response(
                b"<html>Not found</html>", status=404, headers={"Content-Type": ""}
            ),
            "cannot_connect",
        ),
        (
            Response(b"<html>Signed in</html>", headers={"Content-Type": "text/html"}),
            "cannot_connect",
        ),
        (json_response(["not", "an", "object"]), "sign_in_unreadable"),
        (
            Response(b"", headers={"Content-Type": "application/json"}),
            "sign_in_unreadable",
        ),
    ],
    ids=["403 page", "404 page", "a web page", "JSON not an object", "empty"],
)
@pytest.mark.asyncio
async def test_an_answer_that_refuses_nothing_is_no_answer_at_either_step(
    hass: HomeAssistant, step: str, answer: Response, expected: str
) -> None:
    """
    Meridian's and Google's refusals are assumed to be JSON.

    A page from something in between, or an answer that can't be read,
    makes no claim about the email, code or session. A page is the service
    not answering; an answer in JSON that can't be read says so. Either
    way the form says what happened, and Submit tries again.
    """
    result = await _answer_sign_in_step(hass, step, answer)

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == ("user" if step == "send_code" else "otp")
    assert result["errors"] == {"base": expected}


@pytest.mark.parametrize(
    "expires",
    [None, "abc", "0", "86401"],
    ids=["missing", "not a number", "zero", "over a day"],
)
@pytest.mark.asyncio
async def test_a_session_whose_expiry_cannot_be_read_is_an_unreadable_answer(
    hass: HomeAssistant, expires: str | None
) -> None:
    """Not a refused sign-in: the code was accepted, so no new code helps."""
    session: dict[str, Any] = {
        "idToken": "synthetic-id",
        "refreshToken": "synthetic-refresh",
        "localId": "synthetic-user",
    }
    if expires is not None:
        session["expiresIn"] = expires

    result = await _answer_sign_in_step(hass, "session_sign_in", json_response(session))

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "otp"
    assert result["errors"] == {"base": "sign_in_unreadable"}


_WAIT_ASKED = "asked Meridian Energy to wait"


@pytest.mark.parametrize(
    ("step", "expected", "said"),
    [
        (
            "send_code",
            "rate_limited",
            (
                "Meridian asked Meridian Energy to wait (HTTP 429, no code, with "
                "Retry-After)"
            ),
        ),
        (
            "check_code",
            "rate_limited",
            (
                "Meridian asked Meridian Energy to wait (HTTP 429, no code, without "
                "Retry-After)"
            ),
        ),
        (
            "session_sign_in",
            "sign_in_rate_limited",
            (
                "The sign-in service asked Meridian Energy to wait (HTTP 429, no "
                "code, with Retry-After)"
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_a_rate_limit_at_a_sign_in_step_is_named_by_its_service(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
    step: str,
    expected: str,
    said: str,
) -> None:
    """The form names who asked to wait; the log notes what its answer said."""
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
    headers = {"Content-Type": "application/json"}
    if "with Retry-After" in said:
        headers["Retry-After"] = "60"

    result = await _answer_sign_in_step(
        hass, step, Response(b"{}", status=429, headers=headers)
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": expected}
    assert [m for m in caplog.messages if _WAIT_ASKED in m] == [
        f"{said}; the form asks to wait"
    ]


@pytest.mark.asyncio
async def test_a_rate_limit_finding_the_accounts_is_noted_with_what_it_said(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Meridian's in-band limit, with no Retry-After: its code, never its text."""
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
    session = FakeMeridianSession(_NOW)
    session.hook = lambda operation, _variables: (
        json_response(
            {
                "errors": [
                    {
                        "message": f"Too many requests for {_TEST_ACCOUNT}",
                        "extensions": {"errorCode": "KT-CT-1199"},
                    }
                ]
            }
        )
        if operation == "accountsList"
        else None
    )
    with patch.object(
        config_flow_module, "async_get_clientsession", return_value=session
    ):
        result = await _submit_login(hass)

    assert result["step_id"] == "discovery_retry"
    assert result["errors"] == {"base": "rate_limited"}
    assert [m for m in caplog.messages if _WAIT_ASKED in m] == [
        (
            "Meridian asked Meridian Energy to wait (HTTP 200, KT-CT-1199, without "
            "Retry-After); the form asks to wait"
        )
    ]
    assert _TEST_ACCOUNT not in caplog.text


@pytest.mark.parametrize(
    "unreadable",
    [{"id": None}, {"meterPoints": "synthetic-meter"}],
    ids=["no id", "meter points not a list"],
)
@pytest.mark.asyncio
async def test_a_login_whose_only_property_cannot_be_read_asks_again(
    hass: HomeAssistant, unreadable: dict[str, object]
) -> None:
    """
    Through the real client: nothing says Meridian lists no property.

    So the form asks to try again, and no entry is created.
    """
    session = FakeMeridianSession(_NOW)
    data = account_data()
    data["properties"][0].update(unreadable)
    session.hook = lambda operation, _variables: (
        json_response({"data": {"account": data}}) if operation == "account" else None
    )
    with patch.object(
        config_flow_module, "async_get_clientsession", return_value=session
    ):
        result = await _submit_login(hass)

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "discovery_retry"
    assert result["errors"] == {"base": "invalid_data"}
    assert hass.config_entries.async_entries(DOMAIN) == []


@pytest.mark.asyncio
async def test_a_session_that_names_no_user_still_signs_in(
    hass: HomeAssistant,
) -> None:
    """The user ID is kept when given; nothing sends it, so none is needed."""
    answer = json_response(
        {
            "idToken": "synthetic-id",
            "refreshToken": "synthetic-refresh",
            "expiresIn": "3600",
        }
    )
    with patch(
        "custom_components.meridian_energy.async_setup_entry",
        new=AsyncMock(return_value=True),
    ):
        result = await _answer_sign_in_step(hass, "session_sign_in", answer)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_FIREBASE_USER_ID] is None
    assert result["data"][CONF_REFRESH_TOKEN] == "synthetic-refresh"


@pytest.mark.asyncio
async def test_a_second_email_submission_sends_no_second_code(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    An email sent again while the first code is being sent sends no second one.

    A second code would start a new login journey, which the code in the
    first email might not match. Both answers are the code form: the
    frontend shows whichever reaches the dialog first, and the email form
    shown again could not be submitted to a flow already at the code step.
    """
    sending = asyncio.Event()
    finish_sending = asyncio.Event()

    async def send(*_args: object) -> None:
        await asyncio.sleep(0)
        if not sending.is_set():  # Only the first send is slow.
            sending.set()
            await finish_sending.wait()

    meridian.async_send_otp.side_effect = send
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    flow_id = result["flow_id"]
    first = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {CONF_EMAIL: _TEST_EMAIL})
    )
    await sending.wait()
    second = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {CONF_EMAIL: _TEST_EMAIL})
    )
    await asyncio.sleep(0.05)
    # The second waits for the first, rather than answer with the email form.
    assert not second.done()
    finish_sending.set()
    for answer in (await first, await second):
        assert answer["type"] is FlowResultType.FORM
        assert answer["step_id"] == "otp"
    meridian.async_send_otp.assert_awaited_once()

    result = await hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    sent_journey = meridian.async_send_otp.await_args.args[1]
    assert meridian.async_validate_otp.await_args.args[2] == sent_journey


@pytest.mark.asyncio
async def test_a_cancelled_second_submission_leaves_the_first_alone(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """A waiting submission whose request is cancelled does not end the first."""
    sending = asyncio.Event()
    finish_sending = asyncio.Event()

    async def send(*_args: object) -> None:
        sending.set()
        await finish_sending.wait()

    meridian.async_send_otp.side_effect = send
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    flow_id = result["flow_id"]
    first = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {CONF_EMAIL: _TEST_EMAIL})
    )
    await sending.wait()
    second = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {CONF_EMAIL: _TEST_EMAIL})
    )
    await asyncio.sleep(0)
    second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second
    finish_sending.set()

    result = await first
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "otp"
    meridian.async_send_otp.assert_awaited_once()


@pytest.mark.parametrize(
    "outcome", ["created", "no_accounts", "rejected", "discovery_failed"]
)
@pytest.mark.asyncio
async def test_a_second_code_submission_shows_the_step_the_first_produced(
    hass: HomeAssistant, meridian: _FakeMeridian, outcome: str
) -> None:
    """
    Both answers to a code pressed twice carry the step the first produced.

    The frontend replaces the dialog's form with the first answer it gets
    and drops a later one, so the second, which waited, must not come back
    with the code form of a flow that moved on. When the first created the
    entry, the second answers once the flow has ended, with an abort the
    dialog drops: a copy of the created entry would make Home Assistant
    create a second one.
    """
    checking = asyncio.Event()
    finish_check = asyncio.Event()

    async def validate(*_args: object) -> MeridianTokenSet:
        checking.set()
        await finish_check.wait()
        if outcome == "rejected":
            raise MeridianOtpError("OTP_INVALID", "synthetic")
        return _tokens()

    meridian.async_validate_otp.side_effect = validate
    if outcome == "no_accounts":
        meridian.async_get_accounts.return_value = ()
    elif outcome == "discovery_failed":
        meridian.async_get_accounts.side_effect = MeridianConnectionError()
    result = await _start_user_flow(hass)
    flow_id = result["flow_id"]
    first = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    )
    await checking.wait()
    second = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    )
    await asyncio.sleep(0.05)
    assert not second.done()
    finish_check.set()
    answers = [await first, await second]

    meridian.async_validate_otp.assert_awaited_once()
    expected = {
        "created": [
            (FlowResultType.CREATE_ENTRY, None, None),
            (FlowResultType.ABORT, None, None),
        ],
        "no_accounts": [(FlowResultType.ABORT, None, None)] * 2,
        "rejected": [(FlowResultType.FORM, "otp", {"base": "otp_invalid"})] * 2,
        "discovery_failed": [
            (FlowResultType.FORM, "discovery_retry", {"base": "cannot_connect"})
        ]
        * 2,
    }[outcome]
    assert [
        (answer["type"], answer.get("step_id"), answer.get("errors"))
        for answer in answers
    ] == expected
    if outcome == "created":
        assert answers[1]["reason"] == "already_configured"
        assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    if outcome == "no_accounts":
        assert [answer["reason"] for answer in answers] == ["no_accounts"] * 2
    if outcome in {"created", "no_accounts"}:
        assert _flow_ids(hass) == set()


@pytest.mark.asyncio
async def test_a_second_discovery_retry_makes_no_second_request(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    Submit pressed twice on the retry form looks for the accounts once.

    The second press waits for the first; the first created the entry, so
    the second answers once the flow has ended, with an abort the dialog
    drops.
    """
    looking = asyncio.Event()
    finish_looking = asyncio.Event()
    calls = 0

    async def accounts() -> tuple[MeridianAccount, ...]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise MeridianConnectionError
        looking.set()
        await finish_looking.wait()
        return (_account(),)

    meridian.async_get_accounts.side_effect = accounts
    result = await _submit_login(hass)
    assert result["step_id"] == "discovery_retry"
    flow_id = result["flow_id"]
    first = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {})
    )
    await looking.wait()
    second = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {})
    )
    await asyncio.sleep(0.05)
    assert not second.done()
    finish_looking.set()
    answers = [await first, await second]

    assert meridian.async_get_accounts.await_count == 2
    assert [(answer["type"], answer.get("reason")) for answer in answers] == [
        (FlowResultType.CREATE_ENTRY, None),
        (FlowResultType.ABORT, "already_configured"),
    ]
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    assert _flow_ids(hass) == set()


@pytest.mark.asyncio
async def test_a_second_code_submission_gets_the_abort_the_first_raised(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    A code pressed twice gets one raised abort, twice.

    The login was set up by another means while this setup looked for its
    accounts: the check that follows raises its abort, and the second
    submission, which waited, ends the same way.
    """
    looking = asyncio.Event()
    finish_looking = asyncio.Event()

    async def accounts() -> tuple[MeridianAccount, ...]:
        looking.set()
        await finish_looking.wait()
        return (_account(),)

    meridian.async_get_accounts.side_effect = accounts
    result = await _start_user_flow(hass)
    flow_id = result["flow_id"]
    first = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    )
    await looking.wait()
    second = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    )
    _configured_entry(hass)
    await asyncio.sleep(0.05)
    assert not second.done()
    finish_looking.set()
    answers = [await first, await second]

    for answer in answers:
        assert answer["type"] is FlowResultType.ABORT
        assert answer["reason"] == "already_configured"
    meridian.async_validate_otp.assert_awaited_once()
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    assert _flow_ids(hass) == set()


@pytest.mark.asyncio
async def test_a_code_sent_again_while_the_new_entry_is_set_up_creates_nothing(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A submission after the entry was created, while it is set up, repeats nothing.

    Home Assistant sets the new entry up before it ends the flow, so the
    flow is still at the code form. A second submission then, as Enter
    pressed again gives, signs in again nowhere and creates no second
    entry: it answers once the flow has ended, with an abort the dialog
    drops.
    """
    result = await _start_user_flow(hass)
    flow_id = result["flow_id"]
    setting_up = asyncio.Event()
    finish_setup = asyncio.Event()
    setups: list[str] = []

    async def slow_setup_entry(hass_: HomeAssistant, entry: Any) -> bool:
        del hass_
        setups.append(entry.entry_id)
        setting_up.set()
        await finish_setup.wait()
        return True

    answers: list[ConfigFlowResult] = []

    async def press() -> None:
        answers.append(
            await hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
        )

    with patch(
        "custom_components.meridian_energy.async_setup_entry", new=slow_setup_entry
    ):
        first = hass.async_create_task(press())
        await setting_up.wait()
        second = hass.async_create_task(press())
        await asyncio.sleep(0.05)
        assert answers == []
        finish_setup.set()
        await asyncio.gather(first, second)
        await hass.async_block_till_done()

    assert [answer["type"] for answer in answers] == [
        FlowResultType.CREATE_ENTRY,
        FlowResultType.ABORT,
    ]
    assert answers[1]["reason"] == "already_configured"
    meridian.async_validate_otp.assert_awaited_once()
    assert [entry.entry_id for entry in hass.config_entries.async_entries(DOMAIN)] == (
        setups
    )
    assert "same unique ID" not in caplog.text
    assert _flow_ids(hass) == set()


@pytest.mark.asyncio
async def test_a_setup_left_at_a_form_does_not_block_a_new_one(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    A setup left open, as in a closed browser tab, is no obstacle.

    Home Assistant does not list user-started flows, so such a flow can be
    neither resumed nor dismissed. A new setup of the same email goes ahead
    beside it, and Home Assistant aborts the one left open once the new one
    creates the entry.
    """
    meridian.async_get_accounts.side_effect = [MeridianConnectionError(), (_account(),)]
    parked_at_code = await _start_user_flow(hass)
    parked_at_retry = await _submit_login(hass)
    assert parked_at_retry["step_id"] == "discovery_retry"

    result = await _submit_login(hass)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    assert parked_at_code["flow_id"] not in _flow_ids(hass)
    assert _flow_ids(hass) == set()


@pytest.mark.asyncio
async def test_new_setup_refuses_a_configured_email_and_leaves_other_flows(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    A configured email is refused, and the flows of others are left alone.

    That leaves a reauthentication alone too, even one waiting at the code
    form: it runs only while its entry exists.
    """
    del meridian
    entry = _configured_entry(hass)
    entry.async_start_reauth(hass)
    await hass.async_block_till_done()
    (reauth,) = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    reauth = await hass.config_entries.flow.async_configure(reauth["flow_id"], {})
    assert reauth["step_id"] == "otp"
    other = await _start_user_flow(hass, "someone@example.com")

    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: _TEST_EMAIL}
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    assert _flow_ids(hass) == {reauth["flow_id"], other["flow_id"]}


@pytest.mark.asyncio
async def test_flow_removed_while_the_code_is_checked_creates_nothing(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """A flow aborted while its code is being checked never creates an entry."""
    checking = asyncio.Event()
    finish_check = asyncio.Event()

    async def validate(*_args: object) -> MeridianTokenSet:
        checking.set()
        await finish_check.wait()
        return _tokens()

    meridian.async_validate_otp.side_effect = validate
    result = await _start_user_flow(hass)
    flow_id = result["flow_id"]
    submitted = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    )
    await checking.wait()
    hass.config_entries.flow.async_abort(flow_id)
    finish_check.set()
    # Home Assistant's own answer for a flow it removed.
    with pytest.raises(UnknownFlow):
        await submitted

    assert hass.config_entries.async_entries(DOMAIN) == []
    assert _flow_ids(hass) == set()
