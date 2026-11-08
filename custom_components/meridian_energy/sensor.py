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

NativeValue = date | datetime | Decimal | int | None
_FEED_IN = "feed_in"
_PRICES = "prices"


@dataclass(frozen=True, kw_only=True)
class MeridianSensorDescription(SensorEntityDescription):
    """
    Description of one account-scoped Meridian sensor.

    ``condition`` names what the sensor needs: a feed-in register, or an
    all-day plan for the price sensors.
    """

    value_fn: Callable[[MeridianSyncData, AccountSyncResult], NativeValue]
    attrs_fn: Callable[[AccountSyncResult], dict[str, Any]] | None = None
    condition: str | None = None


@dataclass(frozen=True, kw_only=True)
class _EnergyDescription(MeridianSensorDescription):
    """A sensor in kWh."""

    device_class: SensorDeviceClass | None = SensorDeviceClass.ENERGY
    native_unit_of_measurement: str | None = UnitOfEnergy.KILO_WATT_HOUR
    suggested_display_precision: int | None = 1


@dataclass(frozen=True, kw_only=True)
class _MoneyDescription(MeridianSensorDescription):
    """A sensor in NZD."""

    device_class: SensorDeviceClass | None = SensorDeviceClass.MONETARY
    native_unit_of_measurement: str | None = "NZD"
    suggested_display_precision: int | None = 2


@dataclass(frozen=True, kw_only=True)
class _PriceDescription(MeridianSensorDescription):
    """A price, with no device class: the monetary class takes a currency alone."""

    condition: str | None = _PRICES
    attrs_fn: Callable[[AccountSyncResult], dict[str, Any]] | None = lambda result: {
        "plan": result.prices.plan if result.prices else None
    }


@dataclass(frozen=True, kw_only=True)
class _BillingDateDescription(MeridianSensorDescription):
    """A date of the billing period, disabled at first."""

    device_class: SensorDeviceClass | None = SensorDeviceClass.DATE
    entity_category: EntityCategory | None = EntityCategory.DIAGNOSTIC
    entity_registry_enabled_default: bool = False


# No sensor but the diagnostic count has a state class, so Home Assistant
# keeps no long-term statistics for them: the external statistics are the
# long-term record.
DESCRIPTIONS = (
    MeridianSensorDescription(
        key="last_sync",
        translation_key="last_sync",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data, _result: data.synced_at,
    ),
    MeridianSensorDescription(
        key="latest_meter_data",
        translation_key="latest_meter_data",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data, result: max(
            (
                item.latest_reading
                for item in data.results
                if item.account_key == result.account_key
                and item.latest_reading is not None
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
        value_fn=lambda data, result: sum(
            item.estimated_rows
            for item in data.results
            if item.account_key == result.account_key
        ),
    ),
    _EnergyDescription(
        key="bill_usage",
        translation_key="bill_usage",
        value_fn=lambda _data, result: result.bill.usage,
    ),
    _MoneyDescription(
        key="bill_cost",
        translation_key="bill_cost",
        value_fn=lambda _data, result: result.bill.cost,
    ),
    _EnergyDescription(
        key="bill_export",
        translation_key="bill_export",
        condition=_FEED_IN,
        value_fn=lambda _data, result: result.bill.export,
    ),
    _MoneyDescription(
        key="bill_credit",
        translation_key="bill_credit",
        condition=_FEED_IN,
        value_fn=lambda _data, result: result.bill.credit,
    ),
    _MoneyDescription(
        key="bill_estimate",
        translation_key="bill_estimate",
        value_fn=lambda _data, result: result.bill.estimate,
    ),
    _MoneyDescription(
        key="total_due",
        translation_key="total_due",
        value_fn=lambda _data, result: result.total_due,
        attrs_fn=lambda result: {"credit": result.credit},
    ),
    MeridianSensorDescription(
        key="last_bill",
        translation_key="last_bill",
        device_class=SensorDeviceClass.DATE,
        value_fn=lambda _data, result: (
            result.last_bill.issued if result.last_bill else None
        ),
        attrs_fn=lambda result: {
            "period_start": result.last_bill.start if result.last_bill else None,
            "period_end": result.last_bill.end if result.last_bill else None,
        },
    ),
    _MoneyDescription(
        key="last_payment",
        translation_key="last_payment",
        value_fn=lambda _data, result: (
            Decimal(result.last_payment.amount_cents) / 100
            if result.last_payment
            else None
        ),
        attrs_fn=lambda result: {
            "date": result.last_payment.paid_on if result.last_payment else None,
            "status": result.last_payment.status if result.last_payment else None,
        },
    ),
    _PriceDescription(
        key="daily_charge",
        translation_key="daily_charge",
        native_unit_of_measurement="NZD/day",
        value_fn=lambda _data, result: (
            result.prices.daily_charge if result.prices else None
        ),
    ),
    _PriceDescription(
        key="anytime_price",
        translation_key="anytime_price",
        native_unit_of_measurement="NZD/kWh",
        value_fn=lambda _data, result: result.prices.anytime if result.prices else None,
    ),
    _BillingDateDescription(
        key="billing_period_start",
        translation_key="billing_period_start",
        value_fn=lambda _data, result: (
            result.billing_period.start if result.billing_period else None
        ),
    ),
    _BillingDateDescription(
        key="billing_period_end",
        translation_key="billing_period_end",
        value_fn=lambda _data, result: (
            result.billing_period.end if result.billing_period else None
        ),
    ),
    _BillingDateDescription(
        key="next_billing_date",
        translation_key="next_billing_date",
        value_fn=lambda _data, result: (
            result.billing_period.next_billing_date if result.billing_period else None
        ),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: MeridianConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up and maintain one device per Meridian account."""
    coordinator = entry.runtime_data.coordinator
    created_sensors: set[tuple[str, str]] = set()

    @callback
    def _update_entities() -> None:
        """
        Add the sensors an account now has, and remove those it no longer has.

        A sensor whose condition can't be told yet, as the price sensors'
        before Meridian has said what plan an account is on, is left as it is.
        """
        new_entities: list[MeridianAccountSensor] = []
        current_account_keys: set[str] = set()
        desired: set[tuple[str, str]] = set()
        conditional: set[tuple[str, str]] = set()
        undecided: set[tuple[str, str]] = set()

        for account in coordinator.accounts:
            key = account_key(account.number)
            current_account_keys.add(key)
            met = _conditions(coordinator.data, account, key)
            if met is None:
                # Not in the sync's results yet: left as it is until it is.
                undecided.update((key, item.key) for item in DESCRIPTIONS)
                continue
            for description in DESCRIPTIONS:
                sensor_key = (key, description.key)
                if description.condition is not None:
                    conditional.add(sensor_key)
                    if met.get(description.condition) is None:
                        undecided.add(sensor_key)
                        continue
                    if not met[description.condition]:
                        continue
                desired.add(sensor_key)
                if sensor_key in created_sensors:
                    continue
                created_sensors.add(sensor_key)
                new_entities.append(
                    MeridianAccountSensor(coordinator, key, description)
                )

        if new_entities:
            async_add_entities(new_entities)

        _remove_stale_devices(hass, entry, current_account_keys)

        # The entity registry survives an integration reload while
        # ``created_sensors`` does not. Reconcile every conditional key so
        # stale entities are removed even on a fresh platform setup.
        stale = (created_sensors | conditional) - desired - undecided
        _remove_stale_entities(hass, entry, stale)
        created_sensors.difference_update(stale)

    _update_entities()
    entry.async_on_unload(coordinator.async_add_listener(_update_entities))


def _conditions(
    data: MeridianSyncData | None, account: MeridianAccount, key: str
) -> dict[str, bool | None] | None:
    """
    Return whether an account meets each sensor condition, None if unknown.

    Before the first sync, the feed-in register comes from the cached
    accounts, and the plan is unknown. None when the sync's results lack
    the account.
    """
    if data is None:
        return {_FEED_IN: _account_has_feed_in(account), _PRICES: None}
    result = _account_result(data, key)
    if result is None:
        return None
    return {_FEED_IN: result.has_feed_in, _PRICES: result.price_sensors}


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
    """Remove entities and detach devices for accounts Meridian no longer lists."""
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
        key: str,
        description: MeridianSensorDescription,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._description = description
        self._account_key = key
        self._attr_unique_id = f"{key}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, key)},
            entry_type=DeviceEntryType.SERVICE,
            manufacturer="Meridian Energy",
            model="MyMeridian",
            name=NAME,
            configuration_url="https://app.meridianenergy.nz/",
        )

    @property
    def native_value(self) -> NativeValue:
        """Return this account's current value."""
        data = self.coordinator.data
        result = _account_result(data, self._account_key)
        if result is None:
            return None
        return self._description.value_fn(data, result)

    @property
    def available(self) -> bool:
        """Return whether this account has a coherent coordinator result."""
        return (
            super().available
            and _account_result(self.coordinator.data, self._account_key) is not None
        )

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Expose useful, non-sensitive context for diagnostics and billing."""
        result = _account_result(self.coordinator.data, self._account_key)
        if result is None:
            return None
        if self.entity_description.key == "estimated_readings":
            return self._provisional_attributes()
        if self._description.attrs_fn is None:
            return None
        return self._description.attrs_fn(result)

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
            "upstream_quality_counts": dict(sorted(quality_counts.items())),
        }


def _account_result(
    data: MeridianSyncData | None, key: str
) -> AccountSyncResult | None:
    """Find a coordinator account result by its non-sensitive key."""
    if data is None:
        return None
    return next(
        (item for item in data.account_results if item.account_key == key), None
    )


def _account_has_feed_in(account: MeridianAccount) -> bool:
    """Return whether any meter on the account reports feed-in metering."""
    return any(
        meter.has_feed_in
        for property_data in account.properties
        for meter in property_data.meter_points
    )
