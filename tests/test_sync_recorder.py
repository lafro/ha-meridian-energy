"""
The sync against a real recorder: a first import, an outage, a busy recorder, a restart.

Each case runs the real coordinator, client, transport and statistics
write on the ``recorder_mock`` recorder (SQLite in memory), in front of a
synthetic Meridian service, and checks what is stored: every hour once,
each running sum the hour before's plus its own state. Every value is
synthetic.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from functools import partial
from itertools import pairwise
from typing import Any
from unittest.mock import patch

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import statistics_during_period
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.statistics import consumption_ids, property_key
from custom_components.meridian_energy.windows import local_midnight

from .meridian_http import ACCOUNT_NUMBER, FakeMeridianSession, property_id
from .recorder_hold import RecorderHold
from .syncs import DAY, HOUR, TODAY, coordinator_for, measurement_dates, sync

# New Zealand midnight at the start of 16 July 2026: no hour of today has
# ended, and the 90 days back hold no daylight-saving change.
MIDNIGHT = local_midnight(TODAY)
ENERGY_ID, COST_ID = consumption_ids(
    property_key(ACCOUNT_NUMBER, property_id(ACCOUNT_NUMBER))
)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

type Stored = list[tuple[datetime, float, float | None]]


@pytest.fixture
def mock_recorder_before_hass(recorder_db_url: str) -> None:
    """Prepare Recorder storage before the Home Assistant fixture starts."""
    del recorder_db_url


async def _stored(hass: HomeAssistant, statistic_id: str) -> Stored:
    result = await get_instance(hass).async_add_executor_job(
        partial(
            statistics_during_period,
            hass,
            EPOCH,
            None,
            {statistic_id},
            "hour",
            None,
            {"state", "sum"},
        )
    )
    return [
        (datetime.fromtimestamp(row["start"], tz=UTC), row["state"], row.get("sum"))
        for row in result.get(statistic_id, [])
    ]


async def _synced(
    hass: HomeAssistant,
    coordinator: MeridianDataCoordinator,
    at: datetime,
    session: FakeMeridianSession,
) -> Any:
    data = await sync(coordinator, at, session)
    await async_wait_recording_done(hass)
    return data


def _assert_whole(rows: Stored, first: datetime, end: datetime, state: float) -> None:
    """Every hour from ``first`` until ``end`` once, its sum stepping by its state."""
    assert [row[0] for row in rows] == [
        first + HOUR * offset for offset in range(round((end - first) / HOUR))
    ]
    assert all(row[1] == pytest.approx(state) for row in rows)
    assert rows[0][2] == pytest.approx(state)
    for earlier, later in pairwise(rows):
        assert later[2] == pytest.approx(earlier[2] + later[1])


async def test_a_first_import_writes_90_days_with_13_requests(
    recorder_mock: Any, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    del recorder_mock
    caplog.set_level(logging.DEBUG)
    session = FakeMeridianSession(MIDNIGHT)
    coordinator = coordinator_for(hass, session)

    data = await _synced(hass, coordinator, MIDNIGHT, session)

    assert data.requests == 13
    energy = await _stored(hass, ENERGY_ID)
    assert len(energy) == 2160
    _assert_whole(energy, local_midnight(TODAY - 90 * DAY), MIDNIGHT, 0.5)
    _assert_whole(await _stored(hass, COST_ID), energy[0][0], MIDNIGHT, 0.15)
    assert [
        record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.INFO and "meridian" in record.name
    ] == []


async def test_a_30_day_outage_is_filled_in_and_the_sums_run_on(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    del recorder_mock
    session = FakeMeridianSession(MIDNIGHT, days=130)
    coordinator = coordinator_for(hass, session)
    await _synced(hass, coordinator, MIDNIGHT, session)
    session.kraken.requests.clear()

    later = MIDNIGHT + 30 * DAY
    await _synced(hass, coordinator, later, session)

    # The daily request's 21 dates, then the 9 dates before them.
    assert measurement_dates(session) == [
        (TODAY + 24 * DAY, TODAY + 30 * DAY),
        (TODAY + 17 * DAY, TODAY + 23 * DAY),
        (TODAY + 10 * DAY, TODAY + 16 * DAY),
        (TODAY + 3 * DAY, TODAY + 9 * DAY),
        (TODAY, TODAY + 2 * DAY),
    ]
    _assert_whole(
        await _stored(hass, ENERGY_ID), local_midnight(TODAY - 90 * DAY), later, 0.5
    )


async def test_a_busy_recorder_leaves_the_update_working_and_heals_next_time(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    del recorder_mock
    session = FakeMeridianSession(MIDNIGHT)
    coordinator = coordinator_for(hass, session)
    hold = RecorderHold(hass)
    hold.hold_once()
    try:
        with patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2):
            await sync(coordinator, MIDNIGHT, session)
    finally:
        hold.release()
    await async_wait_recording_done(hass)
    assert await _stored(hass, ENERGY_ID) == []
    assert coordinator.data is None or coordinator.last_update_success
    assert coordinator.unit_diagnostics()[0]["history_complete"] is False

    # The next update fetches the recent dates and fills in the rest.
    await _synced(hass, coordinator, MIDNIGHT + HOUR, session)
    _assert_whole(
        await _stored(hass, ENERGY_ID),
        local_midnight(TODAY - 90 * DAY),
        MIDNIGHT + HOUR,
        0.5,
    )
    assert coordinator.unit_diagnostics()[0]["history_complete"] is True


async def test_a_restart_asks_for_21_dates_and_leaves_the_history_as_it_is(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    del recorder_mock
    session = FakeMeridianSession(MIDNIGHT)
    await _synced(hass, coordinator_for(hass, session), MIDNIGHT, session)
    before = await _stored(hass, ENERGY_ID)
    session.kraken.requests.clear()

    restarted = coordinator_for(hass, session)
    data = await _synced(hass, restarted, MIDNIGHT + 2 * HOUR, session)

    assert data.requests == 3
    after = await _stored(hass, ENERGY_ID)
    assert after[: len(before)] == before
    _assert_whole(after, local_midnight(TODAY - 90 * DAY), MIDNIGHT + 2 * HOUR, 0.5)
