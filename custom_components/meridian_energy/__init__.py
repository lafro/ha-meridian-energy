"""Meridian Energy integration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.device_registry import DeviceEntry
from homeassistant.helpers.start import async_at_started

from .api import MeridianApiClient
from .const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
)
from .coordinator import MeridianDataCoordinator
from .models import MeridianTokenSet
from .statistics import account_key

PLATFORMS = [Platform.SENSOR]


@dataclass(slots=True)
class MeridianRuntimeData:
    """Runtime objects for a Meridian config entry."""

    client: MeridianApiClient
    coordinator: MeridianDataCoordinator


type MeridianConfigEntry = ConfigEntry[MeridianRuntimeData]


async def async_setup_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """
    Set up Meridian Energy from a config entry.

    Setup only renews the session and caches the accounts; the first sync
    runs as a background task once Home Assistant has started.
    """

    async def async_store_tokens(tokens: MeridianTokenSet) -> None:
        if (
            # A refresh that outlived the entry's removal, as one update_entity
            # asked for can, has no entry to store them in.
            hass.config_entries.async_get_entry(entry.entry_id) is None
            or (
                entry.data.get(CONF_REFRESH_TOKEN) == tokens.refresh_token
                and entry.data.get(CONF_FIREBASE_USER_ID) == tokens.user_id
            )
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
    coordinator = MeridianDataCoordinator(hass, client, config_entry=entry)
    await coordinator.async_prepare_topology()
    entry.runtime_data = MeridianRuntimeData(client, coordinator)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    @callback
    def _async_start_first_sync(_hass: HomeAssistant) -> None:
        entry.async_create_background_task(
            hass, coordinator.async_refresh(), f"{DOMAIN} first sync"
        )

    entry.async_on_unload(async_at_started(hass, _async_start_first_sync))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """Unload a Meridian config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)


async def async_remove_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> None:
    """Remove a removed entry's repair notice that updates are failing."""
    ir.async_delete_issue(hass, DOMAIN, f"sync_failing_{entry.entry_id}")


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """
    Refuse entries an earlier version set up; accept a later minor version.

    Entries before version 4 chose their accounts and imported in the setup
    flow, so they are set up again rather than converted (``entry_too_old``).
    A later 4.x minor version loads unchanged, so a rollback still loads it.
    """
    del hass
    if entry.version < CONFIG_ENTRY_VERSION:
        raise ConfigEntryError(
            translation_domain=DOMAIN, translation_key="entry_too_old"
        )
    return True


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
