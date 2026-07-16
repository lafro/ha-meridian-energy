"""Meridian Energy integration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import CoreState, HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.device_registry import DeviceEntry
from homeassistant.helpers.start import async_at_started

from .api import MeridianApiClient
from .const import (
    CONF_AUTO_ADD_ACCOUNTS,
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONF_SELECTED_ACCOUNTS,
    DOMAIN,
    NAME,
)
from .coordinator import MeridianDataCoordinator
from .models import MeridianTokenSet
from .statistics import account_key

PLATFORMS = [Platform.SENSOR]
CONFIG_ENTRY_VERSION = 3
_OPTIONS_FLOW_ENTRY_VERSION = 2


@dataclass(slots=True)
class MeridianRuntimeData:
    """Runtime objects for a Meridian config entry."""

    client: MeridianApiClient
    coordinator: MeridianDataCoordinator


type MeridianConfigEntry = ConfigEntry[MeridianRuntimeData]


async def async_setup_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """Set up Meridian Energy from a config entry."""

    async def async_store_tokens(tokens: MeridianTokenSet) -> None:
        if (
            entry.data.get(CONF_REFRESH_TOKEN) == tokens.refresh_token
            and entry.data.get(CONF_FIREBASE_USER_ID) == tokens.user_id
        ):
            return
        hass.config_entries.async_update_entry(
            entry,
            data={
                **entry.data,
                CONF_REFRESH_TOKEN: tokens.refresh_token,
                CONF_FIREBASE_USER_ID: tokens.user_id,
            },
        )

    tokens = MeridianTokenSet(
        id_token="",
        refresh_token=str(entry.data[CONF_REFRESH_TOKEN]),
        expires_at=datetime.fromtimestamp(0, UTC),
        user_id=str(entry.data[CONF_FIREBASE_USER_ID]),
    )
    client = MeridianApiClient(
        async_get_clientsession(hass),
        tokens=tokens,
        token_update_callback=async_store_tokens,
    )
    configured_accounts = entry.data.get(CONF_SELECTED_ACCOUNTS)
    selected_accounts = (
        frozenset(str(value) for value in configured_accounts)
        if configured_accounts is not None
        else None
    )
    coordinator = MeridianDataCoordinator(
        hass,
        client,
        config_entry=entry,
        selected_accounts=selected_accounts,
        auto_add_accounts=bool(entry.data.get(CONF_AUTO_ADD_ACCOUNTS, False)),
    )
    await coordinator.async_config_entry_first_refresh()
    if configured_accounts is None:
        hass.config_entries.async_update_entry(
            entry,
            data={
                **entry.data,
                CONF_SELECTED_ACCOUNTS: sorted(
                    account.number for account in coordinator.accounts
                ),
            },
        )
    entry.runtime_data = MeridianRuntimeData(client, coordinator)
    if hass.state is not CoreState.running:

        async def async_refresh_billing_after_start(_hass: HomeAssistant) -> None:
            """Populate Recorder-derived billing totals once startup is complete."""
            await coordinator.async_refresh_billing_totals()

        entry.async_on_unload(async_at_started(hass, async_refresh_billing_after_start))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """Unload a Meridian config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate legacy account options into reconfigurable entry data."""
    if entry.version == 1:
        hass.config_entries.async_update_entry(entry, version=2, minor_version=0)
    if entry.version == _OPTIONS_FLOW_ENTRY_VERSION:
        selected_accounts = entry.options.get(
            CONF_SELECTED_ACCOUNTS, entry.data.get(CONF_SELECTED_ACCOUNTS)
        )
        data = dict(entry.data)
        if selected_accounts is not None:
            data[CONF_SELECTED_ACCOUNTS] = sorted(
                str(value) for value in selected_accounts
            )
        data.setdefault(CONF_AUTO_ADD_ACCOUNTS, False)
        hass.config_entries.async_update_entry(
            entry,
            data=data,
            options={},
            title=NAME,
            version=CONFIG_ENTRY_VERSION,
            minor_version=0,
        )
        return True
    return entry.version == CONFIG_ENTRY_VERSION


async def async_remove_config_entry_device(
    hass: HomeAssistant,
    entry: MeridianConfigEntry,
    device_entry: DeviceEntry,
) -> bool:
    """Allow manual removal only for devices no longer supplied by Meridian."""
    del hass
    current_keys = {
        account_key(account.number)
        for account in entry.runtime_data.coordinator.accounts
    }
    device_keys = {
        identifier[1]
        for identifier in device_entry.identifiers
        if identifier[0] == DOMAIN
    }
    return bool(device_keys) and device_keys.isdisjoint(current_keys)
