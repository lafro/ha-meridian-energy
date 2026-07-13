"""Diagnostic sensors for Meridian Energy."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import MeridianConfigEntry
from .const import CONF_FIREBASE_USER_ID, DOMAIN, NAME
from .coordinator import MeridianDataCoordinator
from .models import MeridianSyncData


@dataclass(frozen=True, kw_only=True)
class MeridianDiagnosticDescription(SensorEntityDescription):
    """Description of a coordinator-backed diagnostic sensor."""

    key: str
    translation_key: str
    value_fn: Callable[[MeridianSyncData], datetime | int | None]
    device_class: SensorDeviceClass | None = None


DESCRIPTIONS = (
    MeridianDiagnosticDescription(
        key="last_sync",
        translation_key="last_sync",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda data: data.synced_at,
    ),
    MeridianDiagnosticDescription(
        key="latest_meter_data",
        translation_key="latest_meter_data",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda data: max(
            (result.latest_reading for result in data.results if result.latest_reading),
            default=None,
        ),
    ),
    MeridianDiagnosticDescription(
        key="estimated_readings",
        translation_key="estimated_readings",
        value_fn=lambda data: sum(result.estimated_rows for result in data.results),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: MeridianConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up safe, non-billing diagnostic entities."""
    del hass
    identifier = _account_identifier(entry)
    async_add_entities(
        MeridianDiagnosticSensor(
            entry.runtime_data.coordinator, identifier, description
        )
        for description in DESCRIPTIONS
    )


class MeridianDiagnosticSensor(
    CoordinatorEntity[MeridianDataCoordinator], SensorEntity
):
    """A diagnostic sensor that never exposes credentials or account identifiers."""

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: MeridianDataCoordinator,
        identifier: str,
        description: MeridianDiagnosticDescription,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._description = description
        self._attr_unique_id = f"{identifier}_{description.key}"
        self._attr_device_class = description.device_class
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, identifier)},
            entry_type=DeviceEntryType.SERVICE,
            manufacturer="Meridian Energy",
            model="MyMeridian account",
            name=f"{NAME} account",
            configuration_url="https://app.meridianenergy.nz/",
        )
        self._attr_translation_key = description.translation_key

    @property
    def native_value(self) -> datetime | int | None:
        """Return the current diagnostic value."""
        return self._description.value_fn(self.coordinator.data)


def _account_identifier(entry: ConfigEntry) -> str:
    user_id = str(entry.data[CONF_FIREBASE_USER_ID]).encode()
    return sha256(user_id).hexdigest()[:16]
