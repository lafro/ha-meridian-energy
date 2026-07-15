"""Tests for config-entry setup and token persistence."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
from homeassistant.core import CoreState
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.meridian_energy import (
    MeridianRuntimeData,
    _async_update_listener,
    async_migrate_entry,
    async_remove_config_entry_device,
    async_setup_entry,
    async_unload_entry,
)
from custom_components.meridian_energy.const import (
    CONF_AUTO_ADD_ACCOUNTS,
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONF_SELECTED_ACCOUNTS,
    DOMAIN,
    NAME,
)
from custom_components.meridian_energy.models import MeridianAccount, MeridianTokenSet
from custom_components.meridian_energy.statistics import account_key


def _entry(*, version: int = 1) -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        data={
            "email": "person@example.com",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
            CONF_SELECTED_ACCOUNTS: ["synthetic-account"],
        },
        version=version,
        minor_version=1,
    )


@pytest.mark.asyncio
async def test_setup_entry_and_rotating_token_persistence(hass) -> None:
    entry = _entry()
    entry.add_to_hass(hass)
    client = MagicMock()
    coordinator = MagicMock()
    coordinator.async_config_entry_first_refresh = AsyncMock()

    with (
        patch(
            "custom_components.meridian_energy.MeridianApiClient",
            return_value=client,
        ) as client_class,
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch.object(
            hass.config_entries, "async_forward_entry_setups", AsyncMock()
        ) as forward,
    ):
        assert await async_setup_entry(hass, entry) is True
        callback = client_class.call_args.kwargs["token_update_callback"]
        await callback(
            MeridianTokenSet(
                id_token="short-lived",
                refresh_token="rotated-refresh",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
                user_id="new-user",
            )
        )

    assert isinstance(entry.runtime_data, MeridianRuntimeData)
    coordinator.async_config_entry_first_refresh.assert_awaited_once()
    forward.assert_awaited_once()
    assert entry.data[CONF_REFRESH_TOKEN] == "rotated-refresh"
    assert "id_token" not in entry.data


@pytest.mark.asyncio
async def test_setup_defers_billing_totals_until_home_assistant_started(
    hass, caplog
) -> None:
    hass.set_state(CoreState.starting)
    entry = _entry()
    entry.add_to_hass(hass)
    coordinator = MagicMock()
    coordinator.async_config_entry_first_refresh = AsyncMock()
    coordinator.async_refresh_billing_totals = AsyncMock()

    with (
        patch("custom_components.meridian_energy.MeridianApiClient"),
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        await async_setup_entry(hass, entry)
        hass.set_state(CoreState.running)
        hass.bus.async_fire(EVENT_HOMEASSISTANT_STARTED)
        await hass.async_block_till_done()

    coordinator.async_refresh_billing_totals.assert_awaited_once()
    await entry._async_process_on_unload(hass)
    assert "Unable to remove unknown job listener" not in caplog.text


@pytest.mark.asyncio
async def test_token_callback_does_not_rewrite_unchanged_data(hass) -> None:
    entry = _entry()
    entry.add_to_hass(hass)
    coordinator = MagicMock()
    coordinator.async_config_entry_first_refresh = AsyncMock()
    with (
        patch("custom_components.meridian_energy.MeridianApiClient") as client_class,
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
        patch.object(hass.config_entries, "async_update_entry") as update_entry,
    ):
        await async_setup_entry(hass, entry)
        callback = client_class.call_args.kwargs["token_update_callback"]
        await callback(
            MeridianTokenSet(
                id_token="different-short-lived",
                refresh_token="old-refresh",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
                user_id="old-user",
            )
        )

    update_entry.assert_not_called()


@pytest.mark.asyncio
async def test_unload_entry(hass) -> None:
    entry = _entry()
    with patch.object(
        hass.config_entries, "async_unload_platforms", AsyncMock(return_value=True)
    ) as unload:
        assert await async_unload_entry(hass, entry) is True
    unload.assert_awaited_once()


@pytest.mark.asyncio
async def test_migrate_entry_accepts_current_version(hass) -> None:
    entry = _entry(version=3)
    assert await async_migrate_entry(hass, entry) is True


@pytest.mark.asyncio
async def test_migrate_entry_updates_legacy_version(hass) -> None:
    entry = _entry()
    entry.add_to_hass(hass)
    assert await async_migrate_entry(hass, entry) is True
    assert entry.version == 3


@pytest.mark.asyncio
async def test_migrate_entry_rejects_unknown_version(hass) -> None:
    entry = _entry(version=99)
    assert await async_migrate_entry(hass, entry) is False


@pytest.mark.asyncio
async def test_setup_populates_missing_account_selection(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            "email": "person@example.com",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
        },
        version=3,
    )
    entry.add_to_hass(hass)
    coordinator = MagicMock()
    coordinator.accounts = (MeridianAccount("account-b", "ACTIVE", ()),)
    coordinator.async_config_entry_first_refresh = AsyncMock()
    with (
        patch("custom_components.meridian_energy.MeridianApiClient"),
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        await async_setup_entry(hass, entry)

    assert entry.data[CONF_SELECTED_ACCOUNTS] == ["account-b"]


@pytest.mark.asyncio
async def test_migrate_options_flow_entry_to_reconfiguration_data(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            "email": "person@example.com",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
            CONF_SELECTED_ACCOUNTS: ["data-account"],
        },
        options={CONF_SELECTED_ACCOUNTS: ["option-account"]},
        version=2,
    )
    entry.add_to_hass(hass)

    assert await async_migrate_entry(hass, entry) is True
    assert entry.version == 3
    assert entry.title == NAME
    assert entry.data[CONF_SELECTED_ACCOUNTS] == ["option-account"]
    assert entry.options == {}
    assert entry.data[CONF_AUTO_ADD_ACCOUNTS] is False


@pytest.mark.asyncio
async def test_manual_device_removal_only_allows_stale_meridian_devices(hass) -> None:
    entry = _entry(version=3)
    coordinator = MagicMock()
    coordinator.accounts = (MeridianAccount("active", "ACTIVE", ()),)
    entry.runtime_data = MeridianRuntimeData(MagicMock(), coordinator)

    active = MagicMock(identifiers={(DOMAIN, account_key("active"))})
    stale = MagicMock(identifiers={(DOMAIN, account_key("stale"))})
    unrelated = MagicMock(identifiers={("other", "value")})

    assert not await async_remove_config_entry_device(hass, entry, active)
    assert await async_remove_config_entry_device(hass, entry, stale)
    assert not await async_remove_config_entry_device(hass, entry, unrelated)


@pytest.mark.asyncio
async def test_update_listener_reloads_entry(hass) -> None:
    entry = _entry(version=3)
    with patch.object(hass.config_entries, "async_reload", AsyncMock()) as reload_entry:
        await _async_update_listener(hass, entry)
    reload_entry.assert_awaited_once_with(entry.entry_id)
