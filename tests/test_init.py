"""Tests for config-entry setup and token persistence."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics as ha_async_add_external_statistics,
)
from homeassistant.components.recorder.statistics import (
    get_last_statistics as ha_get_last_statistics,
)
from homeassistant.config_entries import SOURCE_REAUTH, ConfigEntryState
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, STATE_UNAVAILABLE
from homeassistant.core import CoreState
from homeassistant.exceptions import (
    ConfigEntryError,
)
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    snapshot_platform,
)
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_recorder_block_till_done,
)

from custom_components.meridian_energy import (
    MeridianDataCoordinator,
    MeridianRuntimeData,
    async_migrate_entry,
    async_remove_config_entry_device,
    async_setup_entry,
    async_unload_entry,
)
from custom_components.meridian_energy.api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
)
from custom_components.meridian_energy.config_flow import MeridianEnergyConfigFlow
from custom_components.meridian_energy.const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
)
from custom_components.meridian_energy.models import (
    AccountSyncResult,
    BillFigures,
    MeridianAccount,
    MeridianBill,
    MeridianBillingPeriod,
    MeridianMeterPoint,
    MeridianPayment,
    MeridianPrices,
    MeridianProperty,
    MeridianSyncData,
    MeridianTokenSet,
    PropertySyncResult,
)
from custom_components.meridian_energy.statistics import account_key, energy_metadata


def _entry(
    *,
    version: int = 4,
    minor_version: int = 1,
    data: dict[str, object] | None = None,
) -> MockConfigEntry:
    entry_data = data or {
        "email": "person@example.com",
        CONF_REFRESH_TOKEN: "old-refresh",
        CONF_FIREBASE_USER_ID: "old-user",
    }
    return MockConfigEntry(
        domain=DOMAIN,
        data=entry_data,
        version=version,
        minor_version=minor_version,
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


def _sync_data(*, feed_in: bool = False, prices: bool = False) -> MeridianSyncData:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    key = account_key("synthetic-account")
    return MeridianSyncData(
        account_count=1,
        property_count=1,
        results=(
            PropertySyncResult(
                property_key="safe-property-key",
                account_key=key,
                latest_reading=now,
                estimated_rows=0,
                oldest_estimated=None,
                newest_estimated=None,
                quality_counts=(("ACTUAL", 24),),
            ),
        ),
        account_results=(
            AccountSyncResult(
                account_key=key,
                billing_period=None,
                has_feed_in=feed_in,
                bill=BillFigures(usage=Decimal(1), cost=Decimal("0.25")),
                price_sensors=prices,
            ),
        ),
        synced_at=now,
    )


@contextmanager
def _patched_meridian(
    topology: Callable[[], tuple[MeridianAccount, ...]],
    data: Callable[[], MeridianSyncData],
) -> Iterator[None]:
    """Replace Meridian I/O while keeping Home Assistant's real lifecycle."""

    async def _prepare(coordinator: MeridianDataCoordinator) -> None:
        coordinator._topology = topology()
        coordinator._topology_cached_at = datetime.now(UTC)

    async def _update(coordinator: MeridianDataCoordinator) -> MeridianSyncData:
        del coordinator
        return data()

    with (
        patch.object(MeridianDataCoordinator, "async_prepare_topology", _prepare),
        patch.object(MeridianDataCoordinator, "_async_update_data", _update),
    ):
        yield


def _mock_coordinator() -> MagicMock:
    coordinator = MagicMock()
    coordinator.async_prepare_topology = AsyncMock()
    coordinator.async_refresh = AsyncMock()
    return coordinator


@pytest.mark.asyncio
async def test_setup_entry_and_rotating_token_persistence(hass) -> None:
    entry = _entry()
    entry.add_to_hass(hass)
    client = MagicMock()
    coordinator = _mock_coordinator()

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

    await hass.async_block_till_done(wait_background_tasks=True)
    assert isinstance(entry.runtime_data, MeridianRuntimeData)
    coordinator.async_prepare_topology.assert_awaited_once()
    coordinator.async_refresh.assert_awaited_once()
    forward.assert_awaited_once()
    assert entry.data[CONF_REFRESH_TOKEN] == "rotated-refresh"
    assert "id_token" not in entry.data
    reload_entry.assert_not_awaited()


@pytest.mark.asyncio
async def test_setup_defers_first_sync_until_home_assistant_started(
    hass, caplog
) -> None:
    """During start-up, setup returns after the topology check alone."""
    hass.set_state(CoreState.starting)
    entry = _entry()
    entry.add_to_hass(hass)
    coordinator = _mock_coordinator()

    with (
        patch("custom_components.meridian_energy.MeridianApiClient"),
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        assert await async_setup_entry(hass, entry) is True
        await hass.async_block_till_done(wait_background_tasks=True)
        coordinator.async_prepare_topology.assert_awaited_once()
        coordinator.async_refresh.assert_not_awaited()

        hass.set_state(CoreState.running)
        hass.bus.async_fire(EVENT_HOMEASSISTANT_STARTED)
        await hass.async_block_till_done(wait_background_tasks=True)

    coordinator.async_refresh.assert_awaited_once()
    await entry._async_process_on_unload(hass)
    assert "Unable to remove unknown job listener" not in caplog.text


@pytest.mark.asyncio
async def test_unload_before_start_cancels_the_pending_first_sync(hass) -> None:
    hass.set_state(CoreState.starting)
    entry = _entry()
    entry.add_to_hass(hass)
    coordinator = _mock_coordinator()

    with (
        patch("custom_components.meridian_energy.MeridianApiClient"),
        patch(
            "custom_components.meridian_energy.MeridianDataCoordinator",
            return_value=coordinator,
        ),
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        await async_setup_entry(hass, entry)
        await entry._async_process_on_unload(hass)
        hass.set_state(CoreState.running)
        hass.bus.async_fire(EVENT_HOMEASSISTANT_STARTED)
        await hass.async_block_till_done(wait_background_tasks=True)

    coordinator.async_refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_token_callback_does_not_rewrite_unchanged_data(hass) -> None:
    entry = _entry()
    entry.add_to_hass(hass)
    coordinator = _mock_coordinator()
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
async def test_removing_an_entry_removes_its_repair_notice(hass) -> None:
    """The notice that updates are failing goes with its entry; another's stays."""
    entry = _entry()
    other = _entry()
    for item in (entry, other):
        item.add_to_hass(hass)
        ir.async_create_issue(
            hass,
            DOMAIN,
            f"sync_failing_{item.entry_id}",
            is_fixable=False,
            severity=ir.IssueSeverity.WARNING,
            translation_key="sync_failing",
            translation_placeholders={"reason": "Synthetic"},
        )
    registry = ir.async_get(hass)

    await hass.config_entries.async_remove(entry.entry_id)

    assert registry.async_get_issue(DOMAIN, f"sync_failing_{entry.entry_id}") is None
    assert registry.async_get_issue(DOMAIN, f"sync_failing_{other.entry_id}")


# What an entry before version 4 held: the accounts it was set up to import.
_VERSION_3_DATA = {
    "email": "person@example.com",
    CONF_REFRESH_TOKEN: "old-refresh",
    CONF_FIREBASE_USER_ID: "old-user",
    "selected_accounts": ["synthetic-account"],
    "auto_add_accounts": True,
}


@pytest.mark.asyncio
@pytest.mark.parametrize(("version", "minor_version"), [(1, 1), (2, 1), (3, 0), (3, 1)])
async def test_migrate_refuses_entries_set_up_before_version_4(
    hass, version: int, minor_version: int
) -> None:
    """
    An entry an earlier version set up is set up again, not converted.

    It chose its accounts and imported in the setup flow; this version
    follows every account and imports in the background.
    """
    entry = _entry(
        version=version, minor_version=minor_version, data=dict(_VERSION_3_DATA)
    )

    with pytest.raises(ConfigEntryError) as raised:
        await async_migrate_entry(hass, entry)

    assert raised.value.translation_domain == DOMAIN
    assert raised.value.translation_key == "entry_too_old"
    assert (entry.version, entry.minor_version) == (version, minor_version)
    assert dict(entry.data) == _VERSION_3_DATA


@pytest.mark.asyncio
async def test_migrate_accepts_a_current_entry(hass) -> None:
    assert await async_migrate_entry(hass, _entry()) is True


@pytest.mark.asyncio
async def test_migrate_takes_the_version_the_flow_creates_and_refuses_the_one_before(
    hass,
) -> None:
    """
    Migration and the setup flow agree on the current version.

    A flow bumped alone would create entries migration refuses, or leave
    migration accepting the version before it unchanged.
    """
    version = MeridianEnergyConfigFlow.VERSION
    minor_version = MeridianEnergyConfigFlow.MINOR_VERSION
    assert version == CONFIG_ENTRY_VERSION

    current = _entry(version=version, minor_version=minor_version)
    assert await async_migrate_entry(hass, current) is True
    with pytest.raises(ConfigEntryError) as raised:
        await async_migrate_entry(hass, _entry(version=version - 1, minor_version=1))
    assert raised.value.translation_key == "entry_too_old"


@pytest.mark.asyncio
async def test_migrate_keeps_a_newer_minor_version_loadable(hass) -> None:
    """A rollback to this release still loads an entry from a later 4.x release."""
    entry = _entry(minor_version=2)
    original_data = dict(entry.data)

    assert await async_migrate_entry(hass, entry) is True

    assert entry.version == 4
    assert entry.minor_version == 2
    assert dict(entry.data) == original_data


@pytest.mark.asyncio
async def test_manual_device_removal_only_allows_stale_meridian_devices(hass) -> None:
    entry = _entry()
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
    async def test_a_version_3_entry_stops_at_migration_error(
        self, recorder_mock, hass
    ) -> None:
        """
        An entry an earlier version set up stops before setup touches anything.

        Its statistics stay as they were, for the entry set up again to
        continue.
        """
        del recorder_mock
        entry = _entry(version=3, data=dict(_VERSION_3_DATA))
        entry.add_to_hass(hass)
        stat_id = "meridian_energy:consumption_migration_gate"
        ha_async_add_external_statistics(
            hass,
            energy_metadata(stat_id, "Migration gate"),
            [{"start": datetime(2026, 8, 1, tzinfo=UTC), "state": 3.0, "sum": 9.0}],
        )
        instance = get_instance(hass)
        await async_recorder_block_till_done(hass)
        before = await instance.async_add_executor_job(
            ha_get_last_statistics, hass, 1, stat_id, False, {"state", "sum"}
        )

        with patch(
            "custom_components.meridian_energy.MeridianApiClient",
            side_effect=AssertionError("migration failure must prevent client setup"),
        ):
            assert not await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()

        assert entry.state is ConfigEntryState.MIGRATION_ERROR
        assert entry.error_reason_translation_key == "entry_too_old"
        assert entry.version == 3
        assert dict(entry.data) == _VERSION_3_DATA
        after = await instance.async_add_executor_job(
            ha_get_last_statistics, hass, 1, stat_id, False, {"state", "sum"}
        )
        assert after == before

    @pytest.mark.asyncio
    async def test_a_login_with_no_account_retries_setup(
        self, recorder_mock, hass
    ) -> None:
        """
        No active or dormant account makes Home Assistant try setup again.

        Its reason says so, rather than that Meridian sent data it could not
        use.
        """
        del recorder_mock
        entry = _entry()
        entry.add_to_hass(hass)
        client = MagicMock()
        client.async_get_accounts = AsyncMock(return_value=())

        with patch(
            "custom_components.meridian_energy.MeridianApiClient", return_value=client
        ):
            assert not await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()

        assert entry.state is ConfigEntryState.SETUP_RETRY
        assert entry.error_reason_translation_key == "no_accounts"
        client.async_get_accounts.assert_awaited_once()
        assert await hass.config_entries.async_unload(entry.entry_id)

    @pytest.mark.asyncio
    async def test_newer_minor_version_entry_sets_up_after_rollback(
        self, recorder_mock, hass
    ) -> None:
        """Home Assistant's own migration step accepts a newer minor version."""
        del recorder_mock
        entry = _entry(minor_version=2)
        entry.add_to_hass(hass)

        with _patched_meridian(lambda: (_account(),), _sync_data):
            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done(wait_background_tasks=True)

        assert entry.state is ConfigEntryState.LOADED
        assert entry.minor_version == 2
        assert await hass.config_entries.async_unload(entry.entry_id)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("entity_registry_enabled_by_default")
    async def test_entity_snapshot(
        self, recorder_mock, hass, entity_registry, snapshot, freezer
    ) -> None:
        """Snapshot every entity's registry entry and state: feed-in, all-day plan."""
        del recorder_mock
        freezer.move_to("2026-07-15T01:00:00+00:00")
        entry = _entry()
        entry.add_to_hass(hass)

        def _data() -> MeridianSyncData:
            data = _sync_data(feed_in=True, prices=True)
            account_result = replace(
                data.account_results[0],
                billing_period=MeridianBillingPeriod(
                    period_length="MONTHLY",
                    period_length_multiplier=1,
                    is_fixed=True,
                    start=date(2026, 7, 1),
                    end=date(2026, 7, 31),
                    next_billing_date=date(2026, 8, 1),
                    period_start_day=1,
                ),
                bill=BillFigures(
                    usage=Decimal(1),
                    cost=Decimal("0.25"),
                    export=Decimal("2.5"),
                    credit=Decimal("0.42"),
                    estimate=Decimal("80.5"),
                ),
                total_due=Decimal(40),
                last_bill=MeridianBill(
                    issued=date(2026, 7, 3),
                    start=date(2026, 6, 1),
                    end=date(2026, 6, 30),
                ),
                last_payment=MeridianPayment(
                    amount_cents=5000, paid_on=date(2026, 7, 5), status="CLEARED"
                ),
                prices=MeridianPrices(
                    plan="Synthetic Plan",
                    daily_charge=Decimal(2),
                    anytime=Decimal("0.3"),
                ),
            )
            return replace(data, account_results=(account_result,))

        with _patched_meridian(lambda: (_account(feed_in=True),), _data):
            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done(wait_background_tasks=True)

        await snapshot_platform(hass, entity_registry, snapshot, entry.entry_id)
        assert await hass.config_entries.async_unload(entry.entry_id)

    @pytest.mark.asyncio
    async def test_load_reload_and_unload(self, recorder_mock, hass) -> None:
        """Load, reload, and unload through Home Assistant's public APIs."""
        del recorder_mock
        entry = _entry()
        entry.add_to_hass(hass)
        topology = {"feed_in": True}

        with _patched_meridian(
            lambda: (_account(feed_in=topology["feed_in"]),),
            lambda: _sync_data(feed_in=topology["feed_in"]),
        ):
            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done(wait_background_tasks=True)
            assert entry.state is ConfigEntryState.LOADED
            first_coordinator = entry.runtime_data.coordinator
            # Twelve sensors and the two feed-in ones, three of them disabled,
            # and the platform's own listener.
            assert len(first_coordinator._listeners) == 12

            registry = er.async_get(hass)
            entry_entities = er.async_entries_for_config_entry(registry, entry.entry_id)
            assert len(entry_entities) == 14
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

            # The bill sensors keep no long-term statistics: no state class
            # and no last_reset.
            for suffix in (
                "_bill_usage",
                "_bill_cost",
                "_bill_export",
                "_bill_credit",
                "_bill_estimate",
                "_total_due",
                "_last_payment",
            ):
                bill_entry = next(
                    item for item in entry_entities if item.unique_id.endswith(suffix)
                )
                assert not bill_entry.capabilities
                bill_state = hass.states.get(bill_entry.entity_id)
                assert bill_state is not None
                assert "state_class" not in bill_state.attributes
                assert "last_reset" not in bill_state.attributes

            topology["feed_in"] = False
            assert await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done(wait_background_tasks=True)
            assert entry.state is ConfigEntryState.LOADED
            assert len(first_coordinator._listeners) == 0
            reloaded_coordinator = entry.runtime_data.coordinator
            assert reloaded_coordinator is not first_coordinator
            assert len(reloaded_coordinator._listeners) == 10
            assert (
                len(er.async_entries_for_config_entry(registry, entry.entry_id)) == 12
            )

            assert await hass.config_entries.async_unload(entry.entry_id)
            await hass.async_block_till_done()

        assert entry.state is ConfigEntryState.NOT_LOADED
        assert len(reloaded_coordinator._listeners) == 0

    @pytest.mark.asyncio
    async def test_setup_does_not_wait_for_the_first_sync(
        self, recorder_mock, hass
    ) -> None:
        """Entities appear unavailable at once and fill in when the sync ends."""
        del recorder_mock
        entry = _entry()
        entry.add_to_hass(hass)
        sync_started = asyncio.Event()
        release_sync = asyncio.Event()

        async def _slow_update(
            coordinator: MeridianDataCoordinator,
        ) -> MeridianSyncData:
            del coordinator
            sync_started.set()
            await release_sync.wait()
            return _sync_data()

        with (
            _patched_meridian(lambda: (_account(),), _sync_data),
            patch.object(MeridianDataCoordinator, "_async_update_data", _slow_update),
        ):
            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.LOADED
            await sync_started.wait()

            registry = er.async_get(hass)
            entities = er.async_entries_for_config_entry(registry, entry.entry_id)
            assert len(entities) == 12
            provisional = next(
                item
                for item in entities
                if item.unique_id.endswith("_estimated_readings")
            )
            state = hass.states.get(provisional.entity_id)
            assert state is not None
            assert state.state == STATE_UNAVAILABLE

            release_sync.set()
            await hass.async_block_till_done(wait_background_tasks=True)

        state = hass.states.get(provisional.entity_id)
        assert state is not None
        assert state.state == "0"
        assert len(er.async_entries_for_config_entry(registry, entry.entry_id)) == 12
        assert await hass.config_entries.async_unload(entry.entry_id)

    @pytest.mark.asyncio
    async def test_unreachable_meridian_retries_setup(
        self, recorder_mock, hass
    ) -> None:
        del recorder_mock
        entry = _entry()
        entry.add_to_hass(hass)

        with patch.object(
            MeridianApiClient,
            "async_get_accounts",
            AsyncMock(side_effect=MeridianConnectionError()),
        ):
            assert not await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()

        assert entry.state is ConfigEntryState.SETUP_RETRY
        assert entry.error_reason_translation_key == "cannot_connect"
        assert await hass.config_entries.async_unload(entry.entry_id)

    @pytest.mark.asyncio
    async def test_rejected_session_starts_reauthentication(
        self, recorder_mock, hass
    ) -> None:
        del recorder_mock
        entry = _entry()
        entry.add_to_hass(hass)

        with patch.object(
            MeridianApiClient,
            "async_get_accounts",
            AsyncMock(side_effect=MeridianAuthenticationError()),
        ):
            assert not await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()

        assert entry.state is ConfigEntryState.SETUP_ERROR
        flows = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
        assert [flow["context"]["source"] for flow in flows] == [SOURCE_REAUTH]
