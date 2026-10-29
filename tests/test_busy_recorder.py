"""
A recorder too busy to confirm an update's hours in time, as during a backup.

The database is on disk, as in a real installation, so it can be locked as
a backup locks it. Every value is synthetic.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy import sensor as sensor_module
from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import MeridianAccount
from custom_components.meridian_energy.statistics import consumption_ids, property_key

from .kraken_fake import FakeKraken
from .test_lanes_recorder import _assert_consistent, _floor_hour, _missing_hours
from .test_units_recorder import DAY, HOUR, _entry, _property, _refresh
from .test_windows_recorder import (
    NOW,
    PROPERTY,
    _rows,
    mock_recorder_before_hass,  # noqa: F401 - the recorder fixture's setup
)


@pytest.fixture
def persistent_database() -> bool:
    """Keep the database on disk, where it can be locked."""
    return True


@pytest.mark.asyncio
async def test_hours_saved_too_slowly_fail_the_update_and_are_tried_again(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The database is locked through an update, as during a long backup.

    Home Assistant does not confirm the update's hours in time, so it fails
    and the sensors are unavailable. The next update, after the lock, is
    the unit's next try: it succeeds, the sensors are back, and every hour
    is stored with every running sum consistent.
    """
    del recorder_mock
    number = "A-TEST-U4"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    client = MagicMock()
    client.async_get_accounts = AsyncMock(
        return_value=(MeridianAccount(number, "ACTIVE", (_property(PROPERTY),)),)
    )
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(side_effect=kraken.async_get_measurements)
    coordinator = MeridianDataCoordinator(hass, client)
    entry = _entry(coordinator)
    entities: list[Any] = []
    await _refresh(hass, coordinator, [kraken], NOW)
    await sensor_module.async_setup_entry(hass, entry, entities.extend)
    await _refresh(hass, coordinator, [kraken], NOW + HOUR)
    assert all(entity.available for entity in entities)

    instance = get_instance(hass)
    assert await instance.lock_database()
    try:
        with (
            patch.object(statistics_module, "CONFIRM_TIMEOUT_SECONDS", 0.2),
            patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
        ):
            async with asyncio.timeout(60):
                await _refresh(hass, coordinator, [kraken], NOW + 2 * HOUR, saved=False)
        assert not coordinator.last_update_success
        assert coordinator.last_exception.translation_key == "statistics_write"
        assert not any(entity.available for entity in entities)
    finally:
        assert instance.unlock_database()
    await async_wait_recording_done(hass)

    await _refresh(hass, coordinator, [kraken], NOW + 3 * HOUR)
    await entry._async_process_on_unload(hass)

    assert coordinator.last_update_success
    assert all(entity.available for entity in entities)
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, NOW - 10 * DAY, _floor_hour(NOW + 3 * HOUR)) == []
    await _assert_consistent(hass, energy_id, cost_id)
