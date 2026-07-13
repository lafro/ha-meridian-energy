"""Tests for safe diagnostic entities and downloads."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

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
    MeridianSyncData,
    PropertySyncResult,
)
from custom_components.meridian_energy.sensor import async_setup_entry


def _data() -> MeridianSyncData:
    reading = datetime.now(UTC) - timedelta(days=1)
    return MeridianSyncData(
        account_count=1,
        property_count=1,
        results=(
            PropertySyncResult(
                property_key="hashed-key",
                consumption_rows=24,
                generation_rows=0,
                latest_reading=reading,
                estimated_rows=2,
            ),
        ),
        synced_at=datetime.now(UTC),
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
    coordinator.data = _data()
    entry = _entry(coordinator)
    entities = []

    await async_setup_entry(hass, entry, entities.extend)

    assert len(entities) == 3
    assert entities[0].native_value == coordinator.data.synced_at
    assert entities[1].native_value == coordinator.data.results[0].latest_reading
    assert entities[2].native_value == 2
    identifiers = entities[0].device_info["identifiers"]
    assert "private-user" not in str(identifiers)


@pytest.mark.asyncio
async def test_diagnostics_exclude_all_sensitive_fields(hass) -> None:
    coordinator = MagicMock()
    coordinator.data = _data()
    coordinator.last_update_success = True
    entry = _entry(coordinator)

    diagnostics = await async_get_config_entry_diagnostics(hass, entry)
    serialized = str(diagnostics)

    assert diagnostics["coordinator"]["account_count"] == 1
    assert "private@example.com" not in serialized
    assert "private-refresh" not in serialized
    assert "private-user" not in serialized
    assert "hashed-key" not in serialized
