"""Statistics behaviour against a real Home Assistant recorder (no mocks).

The rest of the statistics tests patch the recorder. These tests use the
``recorder_mock`` fixture, which runs the real recorder thread on an in-memory
SQLite database, so recorder thread-safety rules are enforced.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_metadata,
)
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy.models import MeridianMeasurement
from custom_components.meridian_energy.statistics import (
    _energy_metadata,
    async_clear_statistics,
    async_has_statistics,
    async_import_measurements,
    consumption_ids,
    property_key,
)

_START = datetime(2026, 8, 1, tzinfo=UTC)


@pytest.fixture
def mock_recorder_before_hass(recorder_db_url: str) -> None:
    """Prepare Recorder storage before the Home Assistant fixture starts."""
    del recorder_db_url


def _measurements(hours: int) -> list[MeridianMeasurement]:
    return [
        MeridianMeasurement(
            start=_START + timedelta(hours=offset),
            end=_START + timedelta(hours=offset + 1),
            value_kwh=Decimal("1.25"),
            quality="ACTUAL",
            direction="CONSUMPTION",
            channel_id="synthetic-meter:synthetic-register",
            cost_cents=Decimal(40),
        )
        for offset in range(hours)
    ]


async def _metadata_ids(hass: HomeAssistant, statistic_ids: set[str]) -> set[str]:
    metadata = await get_instance(hass).async_add_executor_job(
        lambda: get_metadata(hass, statistic_ids=statistic_ids)
    )
    return set(metadata)


@pytest.mark.asyncio
async def test_clear_statistics_removes_only_requested_ids(recorder_mock, hass) -> None:
    """Clearing runs on the recorder thread and completes before returning."""
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-1", "P-TEST-1"))
    unrelated_id = "meridian_energy:consumption_unrelated000"
    await async_import_measurements(
        hass,
        stat_energy_id=energy_id,
        stat_cost_id=cost_id,
        energy_name="Meridian grid import",
        cost_name="Meridian grid import cost",
        measurements=_measurements(3),
    )
    async_add_external_statistics(
        hass,
        _energy_metadata(unrelated_id, "Unrelated"),
        [{"start": _START, "state": 1.0, "sum": 1.0}],
    )
    await async_wait_recording_done(hass)
    assert await _metadata_ids(hass, {energy_id, cost_id, unrelated_id}) == {
        energy_id,
        cost_id,
        unrelated_id,
    }

    await async_clear_statistics(hass, {cost_id, energy_id})

    # No extra wait: the helper must only return once the recorder has cleared.
    assert not await async_has_statistics(hass, energy_id)
    assert not await async_has_statistics(hass, cost_id)
    assert await _metadata_ids(hass, {energy_id, cost_id, unrelated_id}) == {
        unrelated_id
    }
    assert await async_has_statistics(hass, unrelated_id)


@pytest.mark.asyncio
async def test_clear_statistics_with_no_ids_leaves_history(recorder_mock, hass) -> None:
    """An empty rollback set is a no-op."""
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-2", "P-TEST-2"))
    await async_import_measurements(
        hass,
        stat_energy_id=energy_id,
        stat_cost_id=cost_id,
        energy_name="Meridian grid import",
        cost_name="Meridian grid import cost",
        measurements=_measurements(2),
    )
    await async_wait_recording_done(hass)

    await async_clear_statistics(hass, set())

    assert await async_has_statistics(hass, energy_id)
    assert await async_has_statistics(hass, cost_id)
