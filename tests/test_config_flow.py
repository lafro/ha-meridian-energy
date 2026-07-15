"""Tests for the Meridian Energy config flow."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from homeassistant.config_entries import SOURCE_REAUTH, SOURCE_USER
from homeassistant.const import CONF_EMAIL
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import config_validation as cv
from pytest_homeassistant_custom_component.common import MockConfigEntry
from voluptuous_serialize import convert

from custom_components.meridian_energy.api import (
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianOtpError,
)
from custom_components.meridian_energy.config_flow import (
    MeridianEnergyConfigFlow,
    MeridianOptionsFlow,
)
from custom_components.meridian_energy.const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONF_SELECTED_ACCOUNTS,
    DOMAIN,
)
from custom_components.meridian_energy.models import (
    MeridianAccount,
    MeridianMeterPoint,
    MeridianProperty,
    MeridianTokenSet,
)


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(
    recorder_mock: object, enable_custom_integrations: None
) -> None:
    """Set up recorder before enabling this recorder-dependent integration."""


def _tokens() -> MeridianTokenSet:
    return MeridianTokenSet(
        id_token="synthetic-id",
        refresh_token="synthetic-refresh",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        user_id="synthetic-user",
    )


def _account(number: str = "synthetic-account") -> MeridianAccount:
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


@pytest.mark.asyncio
async def test_user_flow_success(hass) -> None:
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(return_value=_tokens())
    client.async_get_accounts = AsyncMock(return_value=(_account(),))

    import_started = asyncio.Event()
    finish_import = asyncio.Event()

    async def initial_import(
        tokens: MeridianTokenSet, selected_accounts: frozenset[str]
    ) -> None:
        assert tokens.refresh_token == "synthetic-refresh"
        assert selected_accounts == {"synthetic-account"}
        import_started.set()
        await finish_import.wait()

    with (
        patch.object(
            MeridianEnergyConfigFlow,
            "_client",
            new_callable=PropertyMock,
            return_value=client,
        ),
        patch.object(
            MeridianEnergyConfigFlow,
            "_async_initial_import",
            side_effect=initial_import,
        ),
        patch.object(
            MeridianEnergyConfigFlow,
            "_authenticated_client",
            return_value=client,
        ),
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "user"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_EMAIL: " PERSON@Example.com "}
        )
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "otp"
        client.async_send_otp.assert_awaited_once()

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        assert result["step_id"] == "initial_import"
        assert result["progress_action"] == "initial_import"
        await import_started.wait()
        finish_import.set()
        await asyncio.sleep(0)
        result = await hass.config_entries.flow.async_configure(result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "person@example.com"
    assert result["data"][CONF_REFRESH_TOKEN] == "synthetic-refresh"
    assert "id_token" not in result["data"]
    assert "otp" not in result["data"]


@pytest.mark.asyncio
async def test_initial_import_failure_aborts(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._email = "person@example.com"
    flow._pending_data = {CONF_EMAIL: flow._email}
    flow._initial_import_task = hass.async_create_task(_raise_initial_import_error())
    await asyncio.sleep(0)

    result = await flow.async_step_initial_import()

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "initial_import_failed"


async def _raise_initial_import_error() -> None:
    raise MeridianConnectionError


@pytest.mark.asyncio
async def test_initial_import_and_finish_expired_guards(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}

    import_result = await flow.async_step_initial_import()
    finish_result = await flow.async_step_finish()

    assert import_result["reason"] == "login_expired"
    assert finish_result["reason"] == "login_expired"


@pytest.mark.asyncio
async def test_initial_import_uses_authenticated_coordinator(hass) -> None:
    tokens = _tokens()
    coordinator = MagicMock()
    coordinator.async_fetch_and_import = AsyncMock()

    with patch(
        "custom_components.meridian_energy.config_flow.MeridianDataCoordinator",
        return_value=coordinator,
    ) as coordinator_class:
        flow = MeridianEnergyConfigFlow()
        flow.hass = hass
        flow.context = {}
        await flow._async_initial_import(tokens, frozenset({"synthetic-account"}))

    client = coordinator_class.call_args.args[1]
    assert client.tokens == tokens
    coordinator.async_fetch_and_import.assert_awaited_once()
    assert coordinator_class.call_args.kwargs["selected_accounts"] == {
        "synthetic-account"
    }


@pytest.mark.asyncio
async def test_otp_form_is_serializable_and_validates_locally(hass) -> None:
    """The native config-flow API must be able to serialize the OTP form."""
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._email = "person@example.com"
    flow._journey_id = "journey"
    client = MagicMock()
    client.async_validate_otp = AsyncMock()

    result = await flow.async_step_otp()
    convert(result["data_schema"], custom_serializer=cv.custom_serializer)

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
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="person@example.com",
        data={CONF_EMAIL: "person@example.com"},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_EMAIL: "person@example.com"},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (MeridianConnectionError(), "cannot_connect"),
        (MeridianAuthenticationError(), "email_not_found"),
    ],
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
            result["flow_id"], {CONF_EMAIL: "person@example.com"}
        )

    assert result["type"] is FlowResultType.FORM
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
async def test_otp_errors(hass, code: str, expected: str) -> None:
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(side_effect=MeridianOtpError(code, "hidden"))
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
            result["flow_id"], {CONF_EMAIL: "person@example.com"}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": expected}


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (MeridianConnectionError(), "cannot_connect"),
        (MeridianAuthenticationError(), "invalid_auth"),
    ],
)
@pytest.mark.asyncio
async def test_otp_service_errors(hass, error: Exception, expected: str) -> None:
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(side_effect=error)
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
            result["flow_id"], {CONF_EMAIL: "person@example.com"}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )

    assert result["errors"] == {"base": expected}


@pytest.mark.asyncio
async def test_reauth_updates_existing_entry(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="person@example.com",
        unique_id="person@example.com",
        data={
            CONF_EMAIL: "person@example.com",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(return_value=_tokens())
    with (
        patch.object(
            MeridianEnergyConfigFlow,
            "_client",
            new_callable=PropertyMock,
            return_value=client,
        ),
        patch.object(hass.config_entries, "async_reload", AsyncMock()),
    ):
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


@pytest.mark.asyncio
async def test_expired_flow_guards(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}

    otp_result = await flow.async_step_otp()
    confirm_result = await flow.async_step_reauth_confirm({})

    assert otp_result["reason"] == "login_expired"
    assert confirm_result["reason"] == "login_expired"


@pytest.mark.asyncio
async def test_reauth_missing_entry_aborts(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {"entry_id": "missing"}

    result = await flow.async_step_reauth({CONF_EMAIL: "person@example.com"})

    assert result["reason"] == "reauth_entry_missing"


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (MeridianConnectionError(), "cannot_connect"),
        (MeridianAuthenticationError(), "email_not_found"),
    ],
)
@pytest.mark.asyncio
async def test_reauth_send_errors(hass, error: Exception, expected: str) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._email = "person@example.com"
    client = MagicMock()
    client.async_send_otp = AsyncMock(side_effect=error)

    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await flow.async_step_reauth_confirm({})

    assert result["errors"] == {"base": expected}


@pytest.mark.asyncio
async def test_client_property_uses_home_assistant_session(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    assert flow._client is not None


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (MeridianConnectionError(), "cannot_connect"),
        (MeridianAuthenticationError(), "invalid_auth"),
        (ValueError(), "invalid_auth"),
    ],
)
@pytest.mark.asyncio
async def test_account_discovery_errors_return_to_otp(
    hass, error: Exception, expected: str
) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._email = "person@example.com"
    flow._journey_id = "journey"
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=error)
    with patch.object(flow, "_authenticated_client", return_value=client):
        result = await flow._async_prepare_accounts(_tokens(), {})
    assert result["step_id"] == "otp"
    assert result["errors"] == {"base": expected}


@pytest.mark.asyncio
async def test_account_discovery_aborts_when_none_are_available(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=())
    with patch.object(flow, "_authenticated_client", return_value=client):
        result = await flow._async_prepare_accounts(_tokens(), {})
    assert result["reason"] == "no_accounts"


@pytest.mark.asyncio
async def test_multiple_accounts_are_selected_before_import(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._tokens = _tokens()
    flow._pending_data = {}
    flow._accounts = (_account("first"), _account("second"))

    form = await flow.async_step_accounts()
    invalid = await flow.async_step_accounts({CONF_SELECTED_ACCOUNTS: []})
    with patch.object(
        flow, "_start_initial_import", new=AsyncMock(return_value={"started": True})
    ) as start:
        result = await flow.async_step_accounts({CONF_SELECTED_ACCOUNTS: ["second"]})

    assert form["step_id"] == "accounts"
    assert invalid["errors"] == {"base": "select_account"}
    assert result == {"started": True}
    assert flow._selected_accounts == {"second"}
    start.assert_awaited_once()


@pytest.mark.asyncio
async def test_options_flow_refreshes_and_saves_account_selection(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_REFRESH_TOKEN: "refresh",
            CONF_FIREBASE_USER_ID: "user",
            CONF_SELECTED_ACCOUNTS: ["first"],
        },
    )
    client = MagicMock()
    client.async_get_accounts = AsyncMock(
        return_value=(_account("first"), _account("second"))
    )
    flow = MeridianOptionsFlow(entry)
    flow.hass = hass
    flow.context = {}
    with patch(
        "custom_components.meridian_energy.config_flow.MeridianApiClient",
        return_value=client,
    ):
        form = await flow.async_step_init()
        invalid = await flow.async_step_init({CONF_SELECTED_ACCOUNTS: []})
        saved = await flow.async_step_init({CONF_SELECTED_ACCOUNTS: ["second"]})

    assert form["step_id"] == "init"
    assert invalid["errors"] == {"base": "select_account"}
    assert saved["type"] is FlowResultType.CREATE_ENTRY
    assert saved["data"] == {CONF_SELECTED_ACCOUNTS: ["second"]}
