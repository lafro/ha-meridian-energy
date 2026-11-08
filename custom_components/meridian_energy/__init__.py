"""Meridian Energy integration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.device_registry import DeviceEntry
from homeassistant.helpers.start import async_at_started

from .api import MeridianApiClient
from .const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    DOMAIN,
    UNCONFIRMED_REPLAYS,
    UNSAVED_COVERAGE,
)
from .coordinator import MeridianDataCoordinator
from .models import MeridianTokenSet
from .statistics import account_key

PLATFORMS = [Platform.SENSOR]
CONFIG_ENTRY_VERSION = 4
CONFIG_ENTRY_MINOR_VERSION = 1


@dataclass(slots=True)
class MeridianRuntimeData:
    """Runtime objects for a Meridian config entry."""

    client: MeridianApiClient
    coordinator: MeridianDataCoordinator


type MeridianConfigEntry = ConfigEntry[MeridianRuntimeData]


def _unconfirmed_replays(
    hass: HomeAssistant, entry: MeridianConfigEntry
) -> dict[tuple[str, str], datetime]:
    """
    Return where the entry's coordinators keep their unconfirmed deep replays.

    Each coordinator updates it in place (``MeridianDataCoordinator``), so
    after a reload of the entry, including the one reauthentication makes,
    the new one still reaches back to the oldest hour
    such a replay wrote: it reads the newest stored hours from the recorder,
    which usage saved without its cost has moved on. A restart of Home
    Assistant forgets it.
    """
    replays: dict[str, dict[tuple[str, str], datetime]] = hass.data.setdefault(
        DOMAIN, {}
    ).setdefault(UNCONFIRMED_REPLAYS, {})
    return replays.setdefault(entry.entry_id, {})


def _unsaved_coverage(
    hass: HomeAssistant, entry: MeridianConfigEntry
) -> dict[tuple[str, str], dict[date, datetime]]:
    """
    Return where the entry's coordinators keep their lanes' unconfirmed dates.

    The dates whose coverage import Home Assistant did not confirm, with
    when each is tried again: the recorder may have saved their rows but
    not the shift of the newer rows after them, which no stored state
    shows, so each is replayed in full when it comes back
    (``lanes.LaneState.unsaved``). Each coordinator updates it in place,
    so after a reload of the entry the new one still does that. A restart
    of Home Assistant forgets them.
    """
    unsaved: dict[str, dict[tuple[str, str], dict[date, datetime]]] = (
        hass.data.setdefault(DOMAIN, {}).setdefault(UNSAVED_COVERAGE, {})
    )
    return unsaved.setdefault(entry.entry_id, {})


async def async_setup_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """
    Set up Meridian Energy from a config entry.

    Setup only renews the stored session and caches the accounts, every
    active or dormant one on the login, so Home Assistant start-up does not
    wait for the measurement sync. The first sync, which imports statistics,
    runs as a background task once Home Assistant has started, or straight
    away when it is already running, as right after the setup flow. It first
    waits for the recorder to save what was queued before it, such as the
    last import before a reload.
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
        unconfirmed=_unconfirmed_replays(hass, entry),
        unsaved=_unsaved_coverage(hass, entry),
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


async def async_remove_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> None:
    """
    Forget what a removed entry kept across its reloads.

    Its unconfirmed deep replays (``_unconfirmed_replays``) and its lanes'
    unconfirmed coverage dates (``_unsaved_coverage``).
    """
    for kept in (UNCONFIRMED_REPLAYS, UNSAVED_COVERAGE):
        hass.data.get(DOMAIN, {}).get(kept, {}).pop(entry.entry_id, None)


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """
    Refuse entries an earlier version set up; accept a later minor version.

    Entries before version 4 chose their accounts and imported in the setup
    flow; this version follows every account and imports in the background,
    so such an entry is set up again rather than converted. The translated
    ``ConfigEntryError`` leaves it in ``migration_error`` with a reason the
    user can act on. A higher minor version of version 4 is accepted
    unchanged: Home Assistant treats minor versions as backward compatible,
    so rolling back to this release must still load an entry a later 4.x
    release wrote. Home Assistant itself refuses a higher major version.
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
