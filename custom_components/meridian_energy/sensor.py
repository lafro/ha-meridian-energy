"""Account-scoped sensors for Meridian Energy."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import EntityCategory, UnitOfEnergy
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import MeridianConfigEntry
from .const import DOMAIN, NAME
from .coordinator import MeridianDataCoordinator
from .models import AccountSyncResult, MeridianAccount, MeridianSyncData
from .statistics import account_key

PARALLEL_UPDATES = 0
_NZ = ZoneInfo("Pacific/Auckland")

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
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
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
    """Set up and maintain one device per selected Meridian account."""
    coordinator = entry.runtime_data.coordinator
    created_sensors: set[tuple[str, str]] = set()

    @callback
    def _update_entities() -> None:
        """Add newly discovered entities and remove stale account devices."""
        new_entities: list[MeridianAccountSensor] = []
        current_account_keys: set[str] = set()
        desired_sensor_keys: set[tuple[str, str]] = set()
        conditional_sensor_keys: set[tuple[str, str]] = set()
        multiple_properties = (
            sum(len(account.properties) for account in coordinator.accounts) > 1
        )

        for account in coordinator.accounts:
            key = account_key(account.number)
            current_account_keys.add(key)
            result = _account_result(coordinator.data, key)
            for description in DESCRIPTIONS:
                sensor_key = (key, description.key)
                if description.conditional_feed_in:
                    conditional_sensor_keys.add(sensor_key)
                if description.conditional_feed_in and not result.has_feed_in:
                    continue
                desired_sensor_keys.add(sensor_key)
                if sensor_key in created_sensors:
                    continue
                created_sensors.add(sensor_key)
                new_entities.append(
                    MeridianAccountSensor(
                        coordinator,
                        account,
                        key,
                        description,
                        disambiguate=multiple_properties,
                    )
                )

        if new_entities:
            async_add_entities(new_entities)

        _remove_stale_devices(hass, entry, current_account_keys)

        # The entity registry survives an integration reload while
        # ``created_sensors`` does not. Reconcile every conditional key so stale
        # feed-in entities are removed even on a fresh platform setup.
        stale_sensor_keys = (created_sensors | conditional_sensor_keys) - (
            desired_sensor_keys
        )
        _remove_stale_entities(hass, entry, stale_sensor_keys)
        created_sensors.difference_update(stale_sensor_keys)

    _update_entities()
    entry.async_on_unload(coordinator.async_add_listener(_update_entities))


@callback
def _remove_stale_entities(
    hass: HomeAssistant,
    entry: MeridianConfigEntry,
    stale_sensor_keys: set[tuple[str, str]],
) -> None:
    """Remove account entities that no longer apply to current topology."""
    entity_registry = er.async_get(hass)
    for account_key_value, sensor_key in stale_sensor_keys:
        entity_id = entity_registry.async_get_entity_id(
            "sensor", DOMAIN, f"{account_key_value}_{sensor_key}"
        )
        if entity_id is None:
            continue
        entity_entry = entity_registry.async_get(entity_id)
        if entity_entry is not None and entity_entry.config_entry_id == entry.entry_id:
            entity_registry.async_remove(entity_id)


@callback
def _remove_stale_devices(
    hass: HomeAssistant,
    entry: MeridianConfigEntry,
    current_account_keys: set[str],
) -> None:
    """Remove entities and detach devices for accounts no longer selected."""
    device_registry = dr.async_get(hass)
    entity_registry = er.async_get(hass)
    for device_entry in dr.async_entries_for_config_entry(
        device_registry, entry.entry_id
    ):
        domain_keys = {
            identifier[1]
            for identifier in device_entry.identifiers
            if identifier[0] == DOMAIN
        }
        if not domain_keys or not domain_keys.isdisjoint(current_account_keys):
            continue
        for entity_entry in er.async_entries_for_device(
            entity_registry,
            device_entry.id,
            include_disabled_entities=True,
        ):
            if entity_entry.config_entry_id == entry.entry_id:
                entity_registry.async_remove(entity_entry.entity_id)
        device_registry.async_update_device(
            device_entry.id, remove_config_entry_id=entry.entry_id
        )


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
        disambiguate: bool,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._description = description
        self._account_key = key
        self._attr_unique_id = f"{key}_{description.key}"
        device_name = NAME
        if disambiguate:
            address = _one_line_address(
                account.properties[0].address if account.properties else "Account"
            )
            device_name = f"{NAME} — {address}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, key)},
            entry_type=DeviceEntryType.SERVICE,
            manufacturer="Meridian Energy",
            model="MyMeridian",
            name=device_name,
            configuration_url="https://app.meridianenergy.nz/",
        )

    @property
    def native_value(self) -> NativeValue:
        """Return this account's current value."""
        return self._description.value_fn(self.coordinator.data, self._account_key)

    @property
    def last_reset(self) -> datetime | None:
        """Return the retailer billing-period boundary for bill-to-date totals."""
        if self.entity_description.key not in {
            "current_bill_usage",
            "current_bill_cost",
            "current_bill_export",
            "current_bill_credit",
        }:
            return None
        period = _account_result(
            self.coordinator.data, self._account_key
        ).billing_period
        if period is None or period.start is None:
            return None
        return datetime.combine(period.start, time.min, tzinfo=_NZ).astimezone(UTC)

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


def _one_line_address(value: str) -> str:
    """Normalize an address for compact local-only display."""
    return " ".join(value.split())
