"""Account-scoped sensors for Meridian Energy."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import EntityCategory, UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import MeridianConfigEntry
from .const import DOMAIN, NAME
from .coordinator import MeridianDataCoordinator
from .models import AccountSyncResult, MeridianAccount, MeridianSyncData
from .statistics import account_key

NativeValue = date | datetime | Decimal | int | None
ValueFn = Callable[[MeridianSyncData, str], NativeValue]


@dataclass(frozen=True, kw_only=True)
class MeridianSensorDescription(SensorEntityDescription):
    """Description of one account-scoped Meridian sensor."""

    value_fn: ValueFn
    conditional_feed_in: bool = False


DESCRIPTIONS = (
    MeridianSensorDescription(
        key="last_sync",
        translation_key="last_sync",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data, _key: data.synced_at,
    ),
    MeridianSensorDescription(
        key="latest_meter_data",
        translation_key="latest_meter_data",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data, key: max(
            (
                item.latest_reading
                for item in data.results
                if item.account_key == key and item.latest_reading is not None
            ),
            default=None,
        ),
    ),
    MeridianSensorDescription(
        key="estimated_readings",
        translation_key="estimated_readings",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data, key: sum(
            item.estimated_rows for item in data.results if item.account_key == key
        ),
    ),
    MeridianSensorDescription(
        key="current_bill_usage",
        translation_key="current_bill_usage",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=1,
        value_fn=lambda data, key: _account_result(data, key).current_bill_usage,
    ),
    MeridianSensorDescription(
        key="current_bill_cost",
        translation_key="current_bill_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement="NZD",
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=2,
        value_fn=lambda data, key: _account_result(data, key).current_bill_cost,
    ),
    MeridianSensorDescription(
        key="current_bill_export",
        translation_key="current_bill_export",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=1,
        conditional_feed_in=True,
        value_fn=lambda data, key: _account_result(data, key).current_bill_export,
    ),
    MeridianSensorDescription(
        key="current_bill_credit",
        translation_key="current_bill_credit",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement="NZD",
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=2,
        conditional_feed_in=True,
        value_fn=lambda data, key: _account_result(data, key).current_bill_credit,
    ),
    MeridianSensorDescription(
        key="billing_period_start",
        translation_key="billing_period_start",
        device_class=SensorDeviceClass.DATE,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda data, key: _billing_value(data, key, "start"),
    ),
    MeridianSensorDescription(
        key="billing_period_end",
        translation_key="billing_period_end",
        device_class=SensorDeviceClass.DATE,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda data, key: _billing_value(data, key, "end"),
    ),
    MeridianSensorDescription(
        key="next_billing_date",
        translation_key="next_billing_date",
        device_class=SensorDeviceClass.DATE,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda data, key: _billing_value(data, key, "next_billing_date"),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: MeridianConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up one device and a coherent entity set per selected account."""
    del hass
    coordinator = entry.runtime_data.coordinator
    multiple = len(coordinator.accounts) > 1
    entities: list[MeridianAccountSensor] = []
    for account in coordinator.accounts:
        key = account_key(account.number)
        result = _account_result(coordinator.data, key)
        for description in DESCRIPTIONS:
            if description.conditional_feed_in and not result.has_feed_in:
                continue
            entities.append(
                MeridianAccountSensor(
                    coordinator,
                    account,
                    key,
                    description,
                    multiple_accounts=multiple,
                )
            )
    async_add_entities(entities)


class MeridianAccountSensor(CoordinatorEntity[MeridianDataCoordinator], SensorEntity):
    """A privacy-conscious sensor belonging to one Meridian account device."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: MeridianDataCoordinator,
        account: MeridianAccount,
        key: str,
        description: MeridianSensorDescription,
        *,
        multiple_accounts: bool,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._description = description
        self._account_key = key
        self._attr_unique_id = f"{key}_{description.key}"
        device_name = NAME
        if multiple_accounts:
            address = account.properties[0].address if account.properties else "Account"
            device_name = f"{NAME} — {address}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, key)},
            entry_type=DeviceEntryType.SERVICE,
            manufacturer="Meridian Energy",
            model="Electricity account",
            name=device_name,
            configuration_url="https://app.meridianenergy.nz/",
        )

    @property
    def native_value(self) -> NativeValue:
        """Return this account's current value."""
        return self._description.value_fn(self.coordinator.data, self._account_key)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Expose useful, non-sensitive context for diagnostics and billing."""
        key = self.entity_description.key
        if key == "estimated_readings":
            return self._provisional_attributes()
        if key in {
            "current_bill_usage",
            "current_bill_cost",
            "current_bill_export",
            "current_bill_credit",
        }:
            result = _account_result(self.coordinator.data, self._account_key)
            period = result.billing_period
            return {
                "billing_period_start": period.start if period else None,
                "billing_period_end": period.end if period else None,
                "next_billing_date": period.next_billing_date if period else None,
                "data_complete": result.billing_data_complete,
            }
        return None

    def _provisional_attributes(self) -> dict[str, Any]:
        data = self.coordinator.data
        results = [
            item for item in data.results if item.account_key == self._account_key
        ]
        quality_counts: Counter[str] = Counter()
        for result in results:
            quality_counts.update(dict(result.quality_counts))
        return {
            "oldest_provisional_interval": min(
                (
                    item.oldest_estimated
                    for item in results
                    if item.oldest_estimated is not None
                ),
                default=None,
            ),
            "newest_provisional_interval": max(
                (
                    item.newest_estimated
                    for item in results
                    if item.newest_estimated is not None
                ),
                default=None,
            ),
            "reconciliation_window_start": min(
                (item.requested_since for item in results), default=None
            ),
            "upstream_quality_counts": dict(sorted(quality_counts.items())),
            "last_sync_mode": data.sync_mode,
        }


def _account_result(data: MeridianSyncData, key: str) -> AccountSyncResult:
    """Find a coordinator account result by its non-sensitive key."""
    return next(item for item in data.account_results if item.account_key == key)


def _billing_value(data: MeridianSyncData, key: str, field: str) -> date | None:
    """Return one date from an account's current billing metadata."""
    period = _account_result(data, key).billing_period
    return getattr(period, field) if period is not None else None
