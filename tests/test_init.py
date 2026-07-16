"""Tests for config-entry setup and token persistence."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
from homeassistant.core import CoreState
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.meridian_energy import (
    MeridianDataCoordinator,
    MeridianRuntimeData,
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
    CONF_STATISTICS_STATE_VERSION,
    DOMAIN,
    NAME,
    STATISTICS_STATE_VERSION,
)
from custom_components.meridian_energy.models import (
    AccountSyncResult,
    MeridianAccount,
    MeridianMeterPoint,
    MeridianProperty,
    MeridianSyncData,
    MeridianTokenSet,
    PropertySyncResult,
    SyncMode,
)
from custom_components.meridian_energy.statistics import (
    account_key,
    consumption_ids,
    generation_ids,
    property_key,
)


def _entry(
    *, version: int = 1, statistics_state_version: int | None = STATISTICS_STATE_VERSION
) -> MockConfigEntry:
    data = {
        "email": "person@example.com",
        CONF_REFRESH_TOKEN: "old-refresh",
        CONF_FIREBASE_USER_ID: "old-user",
        CONF_SELECTED_ACCOUNTS: ["synthetic-account"],
    }
    if statistics_state_version is not None:
        data[CONF_STATISTICS_STATE_VERSION] = statistics_state_version
    return MockConfigEntry(
        domain=DOMAIN,
        data=data,
        version=version,
        minor_version=1,
    )


def _account(*, feed_in: bool = False) -> MeridianAccount:
    return MeridianAccount(
        number="synthetic-account",
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id="synthetic-property",
                address="Synthetic address",
                meter_points=(
                    MeridianMeterPoint(
                        id="synthetic-meter",
                        market_identifier="synthetic-icp",
                        has_feed_in=feed_in,
                    ),
                ),
            ),
        ),
    )


def _sync_data(*, feed_in: bool = False) -> MeridianSyncData:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    key = account_key("synthetic-account")
    return MeridianSyncData(
        account_count=1,
        property_count=1,
        results=(
            PropertySyncResult(
                property_key="safe-property-key",
                account_key=key,
                consumption_rows=24,
                generation_rows=0,
                latest_reading=now,
                estimated_rows=0,
                sync_mode=SyncMode.TIP,
                requested_since=now - timedelta(hours=24),
                consumption_pages=1,
                generation_pages=0,
                consumption_received_rows=24,
                generation_received_rows=0,
                consumption_retained_rows=24,
                generation_retained_rows=0,
                oldest_estimated=None,
                newest_estimated=None,
                quality_counts=(("ACTUAL", 24),),
                observed_rows_per_hour=1.0,
            ),
        ),
        account_results=(
            AccountSyncResult(
                account_key=key,
                billing_period=None,
                current_bill_usage=Decimal(1),
                current_bill_cost=Decimal("0.25"),
                current_bill_export=None,
                current_bill_credit=None,
                has_feed_in=feed_in,
                usage_complete=False,
                cost_complete=False,
                export_complete=False,
                credit_complete=False,
            ),
        ),
        synced_at=now,
        sync_mode=SyncMode.TIP,
        topology_refreshed=True,
        topology_cache_age_seconds=0,
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
        patch.object(hass.config_entries, "async_reload", AsyncMock()) as reload_entry,
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
        await hass.async_block_till_done()

    assert isinstance(entry.runtime_data, MeridianRuntimeData)
    coordinator.async_config_entry_first_refresh.assert_awaited_once()
    forward.assert_awaited_once()
    assert entry.data[CONF_REFRESH_TOKEN] == "rotated-refresh"
    assert "id_token" not in entry.data
    reload_entry.assert_not_awaited()


@pytest.mark.asyncio
async def test_setup_repairs_external_statistic_states_once(hass) -> None:
    entry = _entry(statistics_state_version=None)
    entry.add_to_hass(hass)
    coordinator = MagicMock()
    coordinator.accounts = (_account(),)
    coordinator.async_config_entry_first_refresh = AsyncMock()
    with (
        patch("custom_components.meridian_energy.MeridianApiClient"),
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch(
            "custom_components.meridian_energy.async_repair_external_statistics_states",
            new=AsyncMock(return_value=True),
        ) as repair,
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        assert await async_setup_entry(hass, entry) is True
        assert await async_setup_entry(hass, entry) is True

    key = property_key("synthetic-account", "synthetic-property")
    repair.assert_awaited_once_with(
        hass, statistic_ids={*consumption_ids(key), *generation_ids(key)}
    )
    assert entry.data[CONF_STATISTICS_STATE_VERSION] == STATISTICS_STATE_VERSION


@pytest.mark.asyncio
async def test_setup_leaves_repair_marker_unset_when_repair_is_incomplete(hass) -> None:
    entry = _entry(statistics_state_version=None)
    entry.add_to_hass(hass)
    coordinator = MagicMock()
    coordinator.accounts = (_account(),)
    coordinator.async_config_entry_first_refresh = AsyncMock()

    with (
        patch("custom_components.meridian_energy.MeridianApiClient"),
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch(
            "custom_components.meridian_energy.async_repair_external_statistics_states",
            new=AsyncMock(return_value=False),
        ) as repair,
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        assert await async_setup_entry(hass, entry) is True

    repair.assert_awaited_once()
    assert CONF_STATISTICS_STATE_VERSION not in entry.data


@pytest.mark.asyncio
async def test_setup_retries_repair_for_malformed_state_version(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            "email": "person@example.com",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
            CONF_SELECTED_ACCOUNTS: ["synthetic-account"],
            CONF_STATISTICS_STATE_VERSION: None,
        },
        version=3,
    )
    entry.add_to_hass(hass)
    coordinator = MagicMock()
    coordinator.accounts = (_account(),)
    coordinator.async_config_entry_first_refresh = AsyncMock()

    with (
        patch("custom_components.meridian_energy.MeridianApiClient"),
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch(
            "custom_components.meridian_energy.async_repair_external_statistics_states",
            new=AsyncMock(return_value=True),
        ) as repair,
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        assert await async_setup_entry(hass, entry) is True

    repair.assert_awaited_once()
    assert entry.data[CONF_STATISTICS_STATE_VERSION] == STATISTICS_STATE_VERSION


@pytest.mark.asyncio
async def test_setup_refreshes_billing_if_startup_finishes_during_repair(hass) -> None:
    hass.set_state(CoreState.starting)
    entry = _entry(statistics_state_version=None)
    entry.add_to_hass(hass)
    coordinator = MagicMock()
    coordinator.accounts = (_account(),)
    coordinator.async_config_entry_first_refresh = AsyncMock()
    coordinator.async_refresh_billing_totals = AsyncMock()

    async def finish_startup_during_repair(*_args, **_kwargs) -> bool:
        hass.set_state(CoreState.running)
        return True

    with (
        patch("custom_components.meridian_energy.MeridianApiClient"),
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch(
            "custom_components.meridian_energy.async_repair_external_statistics_states",
            new=AsyncMock(side_effect=finish_startup_during_repair),
        ),
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        assert await async_setup_entry(hass, entry) is True
        await hass.async_block_till_done()

    coordinator.async_refresh_billing_totals.assert_awaited_once()


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
            CONF_STATISTICS_STATE_VERSION: STATISTICS_STATE_VERSION,
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
async def test_migrate_options_flow_entry_without_account_selection(hass) -> None:
    """Migrate entries that predate account selection without inventing a value."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            "email": "person@example.com",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
        },
        version=2,
    )
    entry.add_to_hass(hass)

    assert await async_migrate_entry(hass, entry) is True
    assert CONF_SELECTED_ACCOUNTS not in entry.data
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


class TestPublicConfigEntryLifecycle:
    """Exercise the real config-entry and sensor-platform lifecycle."""

    @pytest.fixture
    def mock_recorder_before_hass(self, recorder_db_url: str) -> None:
        """Prepare Recorder storage before the Home Assistant fixture starts."""
        del recorder_db_url

    @pytest.mark.asyncio
    async def test_load_reload_and_unload(self, recorder_mock, hass) -> None:
        """Load, reload, and unload through Home Assistant's public APIs."""
        del recorder_mock
        entry = _entry(version=3)
        entry.add_to_hass(hass)
        topology = {"feed_in": True}

        async def _first_refresh(coordinator: MeridianDataCoordinator) -> None:
            feed_in = topology["feed_in"]
            coordinator._topology = (_account(feed_in=feed_in),)
            coordinator.async_set_updated_data(_sync_data(feed_in=feed_in))

        with patch.object(
            MeridianDataCoordinator,
            "async_config_entry_first_refresh",
            _first_refresh,
        ):
            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.LOADED
            first_coordinator = entry.runtime_data.coordinator
            assert len(first_coordinator._listeners) == 8

            registry = er.async_get(hass)
            entry_entities = er.async_entries_for_config_entry(registry, entry.entry_id)
            assert len(entry_entities) == 10
            provisional_entry = next(
                item
                for item in entry_entities
                if item.unique_id.endswith("_estimated_readings")
            )
            provisional_state = hass.states.get(provisional_entry.entity_id)
            assert provisional_state is not None
            assert provisional_state.state == "0"
            assert provisional_state.attributes["state_class"] == "measurement"
            assert "unit_of_measurement" not in provisional_state.attributes

            topology["feed_in"] = False
            assert await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.LOADED
            assert len(first_coordinator._listeners) == 0
            reloaded_coordinator = entry.runtime_data.coordinator
            assert reloaded_coordinator is not first_coordinator
            assert len(reloaded_coordinator._listeners) == 6
            assert len(er.async_entries_for_config_entry(registry, entry.entry_id)) == 8

            assert await hass.config_entries.async_unload(entry.entry_id)
            await hass.async_block_till_done()

        assert entry.state is ConfigEntryState.NOT_LOADED
        assert len(reloaded_coordinator._listeners) == 0
