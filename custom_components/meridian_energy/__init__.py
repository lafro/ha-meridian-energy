"""Meridian Energy integration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import MeridianApiClient
from .const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONF_SELECTED_ACCOUNTS,
)
from .coordinator import MeridianDataCoordinator
from .models import MeridianTokenSet

PLATFORMS = [Platform.SENSOR]
CONFIG_ENTRY_VERSION = 2


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
    configured_accounts = entry.options.get(
        CONF_SELECTED_ACCOUNTS, entry.data.get(CONF_SELECTED_ACCOUNTS)
    )
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
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """Unload a Meridian config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate legacy entries; account selection is populated at next setup."""
    if entry.version == 1:
        hass.config_entries.async_update_entry(
            entry, version=CONFIG_ENTRY_VERSION, minor_version=0
        )
        return True
    return entry.version == CONFIG_ENTRY_VERSION


async def _async_update_listener(
    hass: HomeAssistant, entry: MeridianConfigEntry
) -> None:
    """Reload the integration when selected accounts change."""
    await hass.config_entries.async_reload(entry.entry_id)
