"""
Statistics reads and the barrier against a real Home Assistant recorder.

These tests use the ``recorder_mock`` fixture, which runs the real recorder
thread on an in-memory SQLite database, so its thread-safety rules hold.
Every value is synthetic.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from homeassistant.components.recorder.statistics import async_add_external_statistics
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy.statistics import (
    _async_run_removal,
    async_stored_hours,
    consumption_ids,
    cost_metadata,
    energy_metadata,
    property_key,
)

from .recorder_hold import RecorderHold

_START = datetime(2026, 8, 1, tzinfo=UTC)


@pytest.fixture
def mock_recorder_before_hass(recorder_db_url: str) -> None:
    """Prepare Recorder storage before the Home Assistant fixture starts."""
    del recorder_db_url


def _queue_rows(hass: HomeAssistant, energy_id: str, cost_id: str, hours: int) -> None:
    """Queue ``hours`` rows of energy and cost from ``_START``, sums from zero."""
    for metadata, state in (
        (energy_metadata(energy_id, "Meridian grid import"), 1.25),
        (cost_metadata(cost_id, "Meridian grid import cost"), 0.4),
    ):
        async_add_external_statistics(
            hass,
            metadata,
            [
                {
                    "start": _START + timedelta(hours=offset),
                    "state": state,
                    "sum": state * (offset + 1),
                }
                for offset in range(hours)
            ],
        )


async def test_stored_hours_are_the_hours_with_a_numeric_sum(
    recorder_mock: object, hass: HomeAssistant
) -> None:
    """
    The stored hours of each statistic are those with a running sum, in the span.

    A row without a sum is no stored hour: a write would not continue from
    it. Hours before the span's start, or from its end on, and a statistic
    with no rows, give nothing.
    """
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-1", "P-TEST-1"))
    empty_id = "meridian_energy:consumption_empty0000000"
    hours = [_START + timedelta(hours=offset) for offset in range(6)]
    async_add_external_statistics(
        hass,
        energy_metadata(energy_id, "Meridian grid import"),
        [
            {"start": hour, "state": 1.0}
            if offset == 2
            else {"start": hour, "state": 1.0, "sum": float(offset)}
            for offset, hour in enumerate(hours)
        ],
    )
    async_add_external_statistics(
        hass,
        cost_metadata(cost_id, "Meridian grid import cost"),
        [
            {"start": hours[offset], "state": 0.4, "sum": 0.4 * offset}
            for offset in (0, 3, 4)
        ],
    )
    await async_wait_recording_done(hass)

    stored = await async_stored_hours(
        hass, {energy_id, cost_id, empty_id}, hours[1], hours[4]
    )

    assert stored == {
        energy_id: frozenset({hours[1], hours[3]}),
        cost_id: frozenset({hours[3]}),
        empty_id: frozenset(),
    }


async def test_the_barrier_returns_once_queued_rows_are_saved(
    recorder_mock: object, hass: HomeAssistant
) -> None:
    """A removal of nothing waits for the rows queued before it, and keeps them."""
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-2", "P-TEST-2"))
    _queue_rows(hass, energy_id, cost_id, 2)

    assert await _async_run_removal(hass, []) is True

    # No other wait: the rows queued before the barrier are saved by now.
    stored = await async_stored_hours(
        hass, {energy_id, cost_id}, _START, _START + timedelta(hours=2)
    )
    expected = frozenset({_START, _START + timedelta(hours=1)})
    assert stored == {energy_id: expected, cost_id: expected}


async def test_the_barrier_gives_up_while_the_recorder_is_held(
    recorder_mock: object, hass: HomeAssistant
) -> None:
    """A recorder busy past the wait leaves the barrier False, and the rows queued."""
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-3", "P-TEST-3"))
    hold = RecorderHold(hass)
    hold.hold_once()
    try:
        _queue_rows(hass, energy_id, cost_id, 1)
        assert await _async_run_removal(hass, [], seconds=0.1) is False
    finally:
        hold.release()
    await async_wait_recording_done(hass)

    stored = await async_stored_hours(
        hass, {energy_id}, _START, _START + timedelta(hours=1)
    )
    assert stored == {energy_id: frozenset({_START})}
