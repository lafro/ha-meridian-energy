"""Tests for the Meridian Energy config flow."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from homeassistant.config_entries import SOURCE_REAUTH, SOURCE_USER
from homeassistant.const import CONF_EMAIL
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.meridian_energy.api import (
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianOtpError,
)
from custom_components.meridian_energy.config_flow import MeridianEnergyConfigFlow
from custom_components.meridian_energy.const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    DOMAIN,
)
from custom_components.meridian_energy.models import MeridianTokenSet


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


@pytest.mark.asyncio
async def test_user_flow_success(hass) -> None:
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(return_value=_tokens())

    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
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

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "person@example.com"
    assert result["data"][CONF_REFRESH_TOKEN] == "synthetic-refresh"
    assert "id_token" not in result["data"]
    assert "otp" not in result["data"]


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
