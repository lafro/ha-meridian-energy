"""Meridian Energy integration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.device_registry import DeviceEntry
from homeassistant.helpers.start import async_at_started

from .api import MeridianApiClient
from .const import (
    CONF_AUTO_ADD_ACCOUNTS,
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONF_SELECTED_ACCOUNTS,
    CONF_STATISTICS_STATE_VERSION,
    DOMAIN,
    STATISTICS_STATE_VERSION,
)
from .coordinator import MeridianDataCoordinator
from .models import MeridianTokenSet
from .statistics import account_key

PLATFORMS = [Platform.SENSOR]
CONFIG_ENTRY_VERSION = 3
CONFIG_ENTRY_MINOR_VERSION = 1


@dataclass(slots=True)
class MeridianRuntimeData:
    """Runtime objects for a Meridian config entry."""

    client: MeridianApiClient
    coordinator: MeridianDataCoordinator


type MeridianConfigEntry = ConfigEntry[MeridianRuntimeData]


def _selected_accounts(data: Mapping[str, object]) -> frozenset[str]:
    """Validate and normalize the required selected-account config field."""
    configured_accounts = data[CONF_SELECTED_ACCOUNTS]
    if (
        not isinstance(configured_accounts, list)
        or not configured_accounts
        or any(
            not isinstance(account, str) or not account
            for account in configured_accounts
        )
    ):
        raise ValueError("Invalid Meridian account selection")
    return frozenset(configured_accounts)


async def async_setup_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """
    Set up Meridian Energy from a config entry.

    Setup only renews the stored session and caches the account topology, so
    Home Assistant start-up does not wait for the measurement sync. The first
    sync, which imports statistics, runs as a background task once Home
    Assistant has started, or straight away when it is already running.
    """

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

    selected_accounts = _selected_accounts(entry.data)
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
    coordinator = MeridianDataCoordinator(
        hass,
        client,
        config_entry=entry,
        selected_accounts=selected_accounts,
        auto_add_accounts=bool(entry.data.get(CONF_AUTO_ADD_ACCOUNTS, False)),
    )
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


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """
    Promote completed v0.2.4 entries to the v0.2.5 compatibility boundary.

    Unsupported entries raise a translated ``ConfigEntryError``, which leaves
    the entry in ``migration_error`` with a reason the user can act on.
    """
    if entry.version != CONFIG_ENTRY_VERSION or entry.minor_version not in {
        0,
        CONFIG_ENTRY_MINOR_VERSION,
    }:
        raise ConfigEntryError(
            translation_domain=DOMAIN,
            translation_key="migration_unsupported_version",
            translation_placeholders={
                "version": f"{entry.version}.{entry.minor_version}"
            },
        )
    if entry.minor_version == CONFIG_ENTRY_MINOR_VERSION:
        return True
    marker = entry.data.get(CONF_STATISTICS_STATE_VERSION)
    try:
        _selected_accounts(entry.data)
    except KeyError, ValueError:
        complete = False
    else:
        complete = type(marker) is int and marker == STATISTICS_STATE_VERSION
    if not complete:
        raise ConfigEntryError(
            translation_domain=DOMAIN,
            translation_key="migration_incomplete_entry",
        )
    data = dict(entry.data)
    data.pop(CONF_STATISTICS_STATE_VERSION)
    hass.config_entries.async_update_entry(
        entry,
        data=data,
        minor_version=CONFIG_ENTRY_MINOR_VERSION,
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
