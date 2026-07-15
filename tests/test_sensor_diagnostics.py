"""Tests for safe diagnostic entities and downloads."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.meridian_energy.const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    DOMAIN,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.diagnostics import (
    async_get_config_entry_diagnostics,
)
from custom_components.meridian_energy.models import (
    AccountSyncResult,
    MeridianAccount,
    MeridianBillingPeriod,
    MeridianMeterPoint,
    MeridianProperty,
    MeridianSyncData,
    PropertySyncResult,
    SyncMode,
)
from custom_components.meridian_energy.sensor import (
    _remove_stale_devices,
    async_setup_entry,
)
from custom_components.meridian_energy.statistics import account_key

ACCOUNT_NUMBER = "synthetic-account"
ACCOUNT_KEY = account_key(ACCOUNT_NUMBER)


def _account() -> MeridianAccount:
    return MeridianAccount(
        number=ACCOUNT_NUMBER,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id="synthetic-property",
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint(
                        id="synthetic-meter",
                        market_identifier="synthetic-icp",
                        has_feed_in=False,
                    ),
                ),
            ),
        ),
    )


def _data() -> MeridianSyncData:
    reading = datetime.now(UTC) - timedelta(days=1)
    return MeridianSyncData(
        account_count=1,
        property_count=1,
        results=(
            PropertySyncResult(
                property_key="hashed-key",
                account_key=ACCOUNT_KEY,
                consumption_rows=24,
                generation_rows=0,
                latest_reading=reading,
                estimated_rows=2,
                sync_mode=SyncMode.TIP,
                requested_since=reading - timedelta(days=1),
                consumption_pages=1,
                generation_pages=0,
                consumption_received_rows=24,
                generation_received_rows=0,
                consumption_retained_rows=24,
                generation_retained_rows=0,
                oldest_estimated=reading - timedelta(hours=1),
                newest_estimated=reading,
                quality_counts=(("ACTUAL", 22), ("ESTIMATE", 2)),
                observed_rows_per_hour=1.0,
            ),
        ),
        account_results=(
            AccountSyncResult(
                account_key=ACCOUNT_KEY,
                billing_period=MeridianBillingPeriod(
                    period_length="MONTHLY",
                    period_length_multiplier=1,
                    is_fixed=True,
                    start=date(2026, 7, 1),
                    end=date(2026, 7, 31),
                    next_billing_date=date(2026, 8, 1),
                    period_start_day=1,
                ),
                current_bill_usage=Decimal("123.4"),
                current_bill_cost=Decimal("45.67"),
                current_bill_export=None,
                current_bill_credit=None,
                has_feed_in=False,
                billing_data_complete=True,
            ),
        ),
        synced_at=datetime.now(UTC),
        sync_mode=SyncMode.TIP,
        topology_refreshed=False,
        topology_cache_age_seconds=3600,
    )


def _entry(coordinator) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="private@example.com",
        data={
            "email": "private@example.com",
            CONF_REFRESH_TOKEN: "private-refresh",
            CONF_FIREBASE_USER_ID: "private-user",
        },
        version=1,
        minor_version=1,
    )
    entry.runtime_data = SimpleNamespace(coordinator=coordinator, client=MagicMock())
    return entry


@pytest.mark.asyncio
async def test_sensor_values_and_device_identifier_are_redacted(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._topology = (_account(),)
    coordinator.data = _data()
    entry = _entry(coordinator)
    entities = []

    await async_setup_entry(hass, entry, entities.extend)

    assert len(entities) == 8
    assert entities[0].native_value == coordinator.data.synced_at
    assert entities[1].native_value == coordinator.data.results[0].latest_reading
    assert entities[2].native_value == 2
    assert entities[3].native_value == Decimal("123.4")
    assert entities[4].native_value == Decimal("45.67")
    assert entities[5].native_value == date(2026, 7, 1)
    assert entities[6].native_value == date(2026, 7, 31)
    assert entities[7].native_value == date(2026, 8, 1)
    assert entities[3].extra_state_attributes == {
        "billing_period_start": date(2026, 7, 1),
        "billing_period_end": date(2026, 7, 31),
        "next_billing_date": date(2026, 8, 1),
        "data_complete": True,
    }
    assert entities[3].last_reset == datetime(2026, 6, 30, 12, tzinfo=UTC)
    assert entities[0].last_reset is None
    coordinator.data = replace(
        coordinator.data,
        account_results=(
            replace(coordinator.data.account_results[0], billing_period=None),
        ),
    )
    assert entities[3].last_reset is None
    assert entities[2].extra_state_attributes == {
        "oldest_provisional_interval": coordinator.data.results[0].oldest_estimated,
        "newest_provisional_interval": coordinator.data.results[0].newest_estimated,
        "reconciliation_window_start": coordinator.data.results[0].requested_since,
        "upstream_quality_counts": {"ACTUAL": 22, "ESTIMATE": 2},
        "last_sync_mode": SyncMode.TIP,
    }
    assert entities[0].extra_state_attributes is None
    identifiers = entities[0].device_info["identifiers"]
    assert "private-user" not in str(identifiers)
    assert entities[0].device_info["name"] == "Meridian Energy"
    assert entities[0].device_info["model"] == "Electricity account"
    assert [entity.entity_description.translation_key for entity in entities[:3]] == [
        "last_sync",
        "latest_meter_data",
        "estimated_readings",
    ]
    assert all(not hasattr(entity, "_attr_name") for entity in entities)
    await entry._async_process_on_unload(hass)


@pytest.mark.asyncio
async def test_solar_entities_and_multiple_account_device_name(hass) -> None:
    account = _account()
    solar_meter = replace(account.properties[0].meter_points[0], has_feed_in=True)
    account = replace(
        account,
        properties=(replace(account.properties[0], meter_points=(solar_meter,)),),
    )
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    second = replace(_account(), number="second-account")
    coordinator._topology = (account, second)
    data = _data()
    coordinator.data = replace(
        data,
        account_results=(
            replace(
                data.account_results[0],
                has_feed_in=True,
                current_bill_export=Decimal(10),
                current_bill_credit=Decimal(2),
            ),
            replace(
                data.account_results[0],
                account_key=account_key("second-account"),
            ),
        ),
    )
    entities = []
    entry = _entry(coordinator)

    await async_setup_entry(hass, entry, entities.extend)

    assert len(entities) == 18
    assert entities[5].native_value == Decimal(10)
    assert entities[6].native_value == Decimal(2)
    assert entities[0].device_info["name"] == "Meridian Energy — 1 Synthetic Street"
    await entry._async_process_on_unload(hass)


@pytest.mark.asyncio
async def test_feed_in_entities_are_added_when_topology_changes(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._topology = (_account(),)
    coordinator.data = _data()
    entry = _entry(coordinator)
    entities = []

    await async_setup_entry(hass, entry, entities.extend)
    assert len(entities) == 8

    coordinator.async_set_updated_data(
        replace(
            coordinator.data,
            account_results=(
                replace(
                    coordinator.data.account_results[0],
                    has_feed_in=True,
                    current_bill_export=Decimal(2),
                    current_bill_credit=Decimal("0.4"),
                ),
            ),
        )
    )

    assert len(entities) == 10
    assert entities[-2].native_value == Decimal(2)
    assert entities[-1].native_value == Decimal("0.4")
    coordinator._topology = ()
    coordinator.async_set_updated_data(
        replace(coordinator.data, results=(), account_results=())
    )
    assert len(entities) == 10
    await entry._async_process_on_unload(hass)


def test_stale_device_cleanup_removes_only_owned_stale_entities(hass) -> None:
    entry = _entry(MagicMock())
    unrelated = SimpleNamespace(id="unrelated", identifiers={("other", "id")})
    active = SimpleNamespace(id="active", identifiers={(DOMAIN, ACCOUNT_KEY)})
    stale = SimpleNamespace(id="stale", identifiers={(DOMAIN, "stale-key")})
    owned = SimpleNamespace(entity_id="sensor.owned", config_entry_id=entry.entry_id)
    shared = SimpleNamespace(entity_id="sensor.shared", config_entry_id="other")
    device_registry = MagicMock()
    entity_registry = MagicMock()

    with (
        patch(
            "custom_components.meridian_energy.sensor.dr.async_get",
            return_value=device_registry,
        ),
        patch(
            "custom_components.meridian_energy.sensor.dr.async_entries_for_config_entry",
            return_value=[unrelated, active, stale],
        ),
        patch(
            "custom_components.meridian_energy.sensor.er.async_get",
            return_value=entity_registry,
        ),
        patch(
            "custom_components.meridian_energy.sensor.er.async_entries_for_device",
            return_value=[owned, shared],
        ),
    ):
        _remove_stale_devices(hass, entry, {ACCOUNT_KEY})

    entity_registry.async_remove.assert_called_once_with("sensor.owned")
    device_registry.async_update_device.assert_called_once_with(
        "stale", remove_config_entry_id=entry.entry_id
    )


@pytest.mark.asyncio
async def test_diagnostics_exclude_all_sensitive_fields(hass) -> None:
    coordinator = MagicMock()
    coordinator.data = _data()
    coordinator.last_update_success = True
    entry = _entry(coordinator)

    diagnostics = await async_get_config_entry_diagnostics(hass, entry)
    serialized = str(diagnostics)

    assert diagnostics["coordinator"]["account_count"] == 1
    assert diagnostics["coordinator"]["sync_mode"] == SyncMode.TIP
    assert diagnostics["coordinator"]["property_results"][0]["api_pages"] == {
        "consumption": 1,
        "generation": 0,
    }
    assert (
        diagnostics["coordinator"]["property_results"][0]["requested_window_end"]
        == coordinator.data.synced_at
    )
    assert "private@example.com" not in serialized
    assert "private-refresh" not in serialized
    assert "private-user" not in serialized
    assert "hashed-key" not in serialized
