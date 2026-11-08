"""
The statistics write, ``async_write_hours``, against a real recorder.

Each case runs on the ``recorder_mock`` recorder (SQLite in memory) and
checks what is stored: every hour's running sum is the stored hour before's
plus its own state, from zero or from the newest stored row with a sum
before what was rewritten. Every value is synthetic.
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from datetime import time as day_start
from decimal import Decimal
from functools import partial
from itertools import pairwise
from typing import Any
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder import statistics as recorder_statistics
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    statistics_during_period,
)
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy.statistics import (
    _cost_metadata,
    _energy_metadata,
    async_write_hours,
    consumption_ids,
    property_key,
)

from .recorder_hold import RecorderHold

HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
NZ = ZoneInfo("Pacific/Auckland")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# New Zealand midnight of 1 May 2026 (NZST, UTC+12): 90 days on from it hold
# no daylight-saving change, so they are 2,160 hours.
START = datetime(2026, 4, 30, 12, tzinfo=UTC)
ENERGY_ID, COST_ID = consumption_ids(property_key("A-TEST-1", "P-TEST-1"))

type Stored = list[tuple[datetime, float, float | None]]


@pytest.fixture
def mock_recorder_before_hass(recorder_db_url: str) -> None:
    """Prepare Recorder storage before the Home Assistant fixture starts."""
    del recorder_db_url


def _energy_meta() -> StatisticMetaData:
    return _energy_metadata(ENERGY_ID, "Meridian grid import")


def _cost_meta() -> StatisticMetaData:
    return _cost_metadata(COST_ID, "Meridian grid import cost")


def _energy(first: datetime, count: int) -> dict[datetime, Decimal]:
    """Synthetic kWh that change from hour to hour, so a misplaced hour shows."""
    return {
        first + HOUR * offset: Decimal(offset % 7 + 1) / 4 for offset in range(count)
    }


def _cost(first: datetime, count: int) -> dict[datetime, Decimal]:
    """Synthetic NZD that change from hour to hour."""
    return {
        first + HOUR * offset: Decimal(offset % 5 + 2) / 10 for offset in range(count)
    }


def _between(
    values: dict[datetime, Decimal], first: datetime, end: datetime
) -> dict[datetime, Decimal]:
    return {hour: value for hour, value in values.items() if first <= hour < end}


def _nz_days(first: date, days: int) -> list[datetime]:
    """Return the UTC hours of some New Zealand dates, 23, 24 or 25 a date."""
    start = datetime.combine(first, day_start(), NZ).astimezone(UTC)
    end = datetime.combine(first + DAY * days, day_start(), NZ).astimezone(UTC)
    return [start + HOUR * offset for offset in range(int((end - start) / HOUR))]


async def _stored(hass: HomeAssistant, statistic_id: str) -> Stored:
    """Return every stored hourly row of a statistic: start, state, sum."""
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


async def _seed(
    hass: HomeAssistant, metadata: StatisticMetaData, rows: list[StatisticData]
) -> None:
    """Store rows as another writer, or an earlier release, left them."""
    async_add_external_statistics(hass, metadata, rows)
    await async_wait_recording_done(hass)


def _chained(values: dict[datetime, Decimal], base: float = 0.0) -> list[StatisticData]:
    """Rows whose running sums step by their states, from ``base``."""
    rows: list[StatisticData] = []
    total = Decimal(str(base))
    for hour in sorted(values):
        total += values[hour]
        rows.append({"start": hour, "state": float(values[hour]), "sum": float(total)})
    return rows


def _assert_steps(rows: Stored, base: float = 0.0) -> None:
    """Assert each running sum is the one before plus its own state."""
    total = base
    for start, state, stored_sum in rows:
        total += state
        assert stored_sum == pytest.approx(total, abs=1e-6), start


def _assert_states(rows: Stored, values: dict[datetime, Decimal]) -> None:
    assert [start for start, _state, _sum in rows] == sorted(values)
    for start, state, _sum in rows:
        assert state == pytest.approx(float(values[start]), abs=1e-9), start


@contextmanager
def _spy_writes() -> Iterator[MagicMock]:
    """Record every ``async_add_external_statistics`` call, which still runs."""
    with patch.object(
        statistics_module,
        "async_add_external_statistics",
        wraps=async_add_external_statistics,
    ) as spy:
        yield spy


def _written(spy: MagicMock) -> dict[str, list[datetime]]:
    """Return the hours each call wrote, by statistic."""
    written: dict[str, list[datetime]] = {}
    for call in spy.call_args_list:
        _hass, metadata, rows = call.args
        assert metadata["statistic_id"] not in written, "one call per statistic"
        written[metadata["statistic_id"]] = [row["start"] for row in rows]
    return written


async def test_a_first_write_of_90_days_is_one_call_per_statistic(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    """2,160 hours of energy and cost, each one transaction, sums from zero."""
    del recorder_mock
    energy, cost = _energy(START, 2160), _cost(START, 2160)

    with _spy_writes() as spy:
        assert (
            await async_write_hours(
                hass, [(_energy_meta(), energy), (_cost_meta(), cost)]
            )
            is True
        )
    await async_wait_recording_done(hass)

    assert _written(spy) == {ENERGY_ID: sorted(energy), COST_ID: sorted(cost)}
    for statistic_id, values in ((ENERGY_ID, energy), (COST_ID, cost)):
        rows = await _stored(hass, statistic_id)
        assert len(rows) == 2160
        _assert_states(rows, values)
        _assert_steps(rows)


async def test_an_estimate_rewritten_inside_the_span(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    """
    The span starts at the first hour that changed, and runs to the newest.

    The hours before it are stored as given, so they are not written again.
    """
    del recorder_mock
    readings = _energy(START, 74)
    assert await async_write_hours(
        hass, [(_energy_meta(), _between(readings, START, START + 72 * HOUR))]
    )
    await async_wait_recording_done(hass)
    before = await _stored(hass, ENERGY_ID)
    changed = START + 60 * HOUR
    # Meridian's actual reading replaces its estimate; two hours are new.
    readings[changed] += Decimal("0.5")
    recent = _between(readings, START + 48 * HOUR, START + 74 * HOUR)

    with _spy_writes() as spy:
        assert await async_write_hours(hass, [(_energy_meta(), recent)])
    await async_wait_recording_done(hass)

    assert _written(spy) == {
        ENERGY_ID: [START + HOUR * offset for offset in range(60, 74)]
    }
    rows = await _stored(hass, ENERGY_ID)
    assert rows[:60] == before[:60]
    _assert_states(rows, readings)
    _assert_steps(rows)


async def test_an_older_window_fills_in_behind_newer_rows(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    """
    Older windows rewrite every newer row, so every later sum follows on.

    The 21 newest dates come first, then two older windows of 7, as the
    fill asks for them. Each import is slow and no test wait comes between
    the writes: only the write's own barrier lets it read the rows the one
    before queued, and without it the newer rows would keep sums from zero.
    """
    del recorder_mock
    energy, cost = _energy(START, 2160), _cost(START, 2160)
    real_import = recorder_statistics.import_statistics

    def slow_import(*args: Any) -> bool:
        time.sleep(0.2)
        return real_import(*args)

    with patch(
        "homeassistant.components.recorder.tasks.statistics.import_statistics",
        side_effect=slow_import,
    ):
        for first_day, days in ((69, 21), (62, 7), (55, 7)):
            window_start = START + DAY * first_day
            window_end = window_start + DAY * days
            assert await async_write_hours(
                hass,
                [
                    (_energy_meta(), _between(energy, window_start, window_end)),
                    (_cost_meta(), _between(cost, window_start, window_end)),
                ],
            )
        await async_wait_recording_done(hass)

    for statistic_id, values in ((ENERGY_ID, energy), (COST_ID, cost)):
        rows = await _stored(hass, statistic_id)
        _assert_states(rows, _between(values, START + DAY * 55, START + DAY * 90))
        _assert_steps(rows)
        assert all(
            later_sum is not None
            and earlier_sum is not None
            and later_sum >= earlier_sum
            for (_s1, _v1, earlier_sum), (_s2, _v2, later_sum) in pairwise(rows)
        )


async def test_a_re_add_continues_the_sums_of_kept_rows(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    """
    Kept rows more than 14 days before the span still carry its sums on.

    The newest kept row with a running sum is found by searching back past
    the 14 days read, passing over a newer row without a sum. The kept rows
    are left as they were.
    """
    del recorder_mock
    kept = _chained(_energy(START, 240), base=1000.0)
    await _seed(hass, _energy_meta(), kept)
    no_sum: StatisticData = {"start": START + 11 * DAY, "state": 2.0}
    await _seed(hass, _energy_meta(), [no_sum])
    before = await _stored(hass, ENERGY_ID)
    new = _energy(START + 40 * DAY, 72)

    assert await async_write_hours(hass, [(_energy_meta(), new)])
    await async_wait_recording_done(hass)

    rows = await _stored(hass, ENERGY_ID)
    assert rows[:241] == before
    _assert_states(rows[241:], new)
    _assert_steps(rows[241:], base=kept[-1]["sum"])


@pytest.mark.parametrize(
    "seam",
    [
        pytest.param({"sum_offset": -5.0}, id="sum-steps-wrong"),
        pytest.param({"no_sum": True}, id="row-without-sum"),
    ],
)
async def test_a_seam_inside_the_range_read_heals(
    recorder_mock: Any, hass: HomeAssistant, seam: dict[str, Any]
) -> None:
    """
    A stored row whose sum does not step by its state starts the span.

    The seam is in the 14 days read before the hours given, which match
    what is stored: the seam alone makes the write, from it to the newest.
    """
    del recorder_mock
    readings = _energy(START, 240)
    rows = _chained(readings)
    for row in rows[150:]:
        row["sum"] = row["sum"] + seam.get("sum_offset", 0.0)
    if seam.get("no_sum"):
        del rows[150]["sum"]
    await _seed(hass, _energy_meta(), rows)
    before = await _stored(hass, ENERGY_ID)

    with _spy_writes() as spy:
        assert await async_write_hours(
            hass,
            [
                (
                    _energy_meta(),
                    _between(readings, START + 168 * HOUR, START + 240 * HOUR),
                )
            ],
        )
    await async_wait_recording_done(hass)

    assert _written(spy) == {
        ENERGY_ID: [START + HOUR * offset for offset in range(150, 240)]
    }
    healed = await _stored(hass, ENERGY_ID)
    assert healed[:150] == before[:150]
    _assert_states(healed, readings)
    _assert_steps(healed)


async def test_the_23_and_25_hour_new_zealand_days(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    """
    Every UTC hour of a daylight-saving date is written once, none lost.

    5 April 2026 has 25 hours in New Zealand, and 27 September 23. The
    September dates are written months after April's, so their sums follow
    on from April's newest row.
    """
    del recorder_mock
    april = _nz_days(date(2026, 4, 4), 3)
    september = _nz_days(date(2026, 9, 26), 3)
    values = {
        hour: Decimal(index % 9 + 1) / 8 for index, hour in enumerate(april + september)
    }

    assert await async_write_hours(
        hass, [(_energy_meta(), _between(values, april[0], april[-1] + HOUR))]
    )
    assert await async_write_hours(
        hass, [(_energy_meta(), _between(values, september[0], september[-1] + HOUR))]
    )
    await async_wait_recording_done(hass)

    rows = await _stored(hass, ENERGY_ID)
    assert Counter(start.astimezone(NZ).date() for start, _state, _sum in rows) == {
        date(2026, 4, 4): 24,
        date(2026, 4, 5): 25,
        date(2026, 4, 6): 24,
        date(2026, 9, 26): 24,
        date(2026, 9, 27): 23,
        date(2026, 9, 28): 24,
    }
    _assert_states(rows, values)
    _assert_steps(rows)


async def test_a_barrier_timeout_writes_nothing(
    recorder_mock: Any, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """A recorder busy past the barrier's limit: no read, no write, one DEBUG."""
    del recorder_mock
    caplog.set_level(logging.DEBUG, logger=statistics_module.__name__)
    readings = _energy(START, 24)
    assert await async_write_hours(hass, [(_energy_meta(), readings)])
    await async_wait_recording_done(hass)
    before = await _stored(hass, ENERGY_ID)
    readings[START] += Decimal(1)
    caplog.clear()

    hold = RecorderHold(hass)
    hold.hold_once()
    try:
        with (
            patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
            patch.object(
                statistics_module,
                "statistics_during_period",
                wraps=statistics_during_period,
            ) as reads,
            _spy_writes() as spy,
        ):
            written = await async_write_hours(
                hass,
                [(_energy_meta(), readings), (_cost_meta(), _cost(START, 24))],
            )
    finally:
        hold.release()
    await async_wait_recording_done(hass)

    assert written is False
    reads.assert_not_called()
    spy.assert_not_called()
    assert await _stored(hass, ENERGY_ID) == before
    assert await _stored(hass, COST_ID) == []
    assert [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.name == statistics_module.__name__
    ] == [
        (
            logging.DEBUG,
            (
                "The recorder stayed busy for 0.2 s, so these hours were not "
                "written; a later update writes them"
            ),
        )
    ]


async def test_energy_and_cost_are_written_independently(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    """
    Each statistic has its own span and its own predecessor.

    The energy is stored as given, so it is not written. The cost misses a
    day Meridian did not price: the new cost follows on from the newest
    cost row before it, and the day neither given nor stored stays out.
    """
    del recorder_mock
    energy, cost = _energy(START, 120), _cost(START, 120)
    await _seed(hass, _energy_meta(), _chained(energy))
    await _seed(hass, _cost_meta(), _chained(_between(cost, START, START + 3 * DAY)))
    energy_before = await _stored(hass, ENERGY_ID)
    last_day = START + 4 * DAY

    with _spy_writes() as spy:
        assert await async_write_hours(
            hass,
            [
                (_energy_meta(), _between(energy, last_day, START + 5 * DAY)),
                (_cost_meta(), _between(cost, last_day, START + 5 * DAY)),
            ],
        )
    await async_wait_recording_done(hass)

    assert _written(spy) == {
        COST_ID: [last_day + HOUR * offset for offset in range(24)]
    }
    assert await _stored(hass, ENERGY_ID) == energy_before
    rows = await _stored(hass, COST_ID)
    _assert_states(
        rows,
        _between(cost, START, START + 3 * DAY)
        | _between(cost, last_day, START + 5 * DAY),
    )
    _assert_steps(rows)


async def test_a_manual_adjust_sum_inside_the_span_is_overwritten(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    """
    An "Adjust sum" a user made inside a rewritten span does not survive.

    The documented trade-off: the span's sums are rebuilt from the states.
    """
    del recorder_mock
    readings = _energy(START, 72)
    assert await async_write_hours(hass, [(_energy_meta(), readings)])
    await async_wait_recording_done(hass)
    get_instance(hass).async_adjust_statistics(
        ENERGY_ID, START + 50 * HOUR, 10.0, "kWh"
    )
    await async_wait_recording_done(hass)
    adjusted = await _stored(hass, ENERGY_ID)
    assert adjusted[-1][2] == pytest.approx(float(sum(readings.values())) + 10.0)
    readings[START + 40 * HOUR] += Decimal("0.5")

    assert await async_write_hours(
        hass,
        [(_energy_meta(), _between(readings, START + 24 * HOUR, START + 72 * HOUR))],
    )
    await async_wait_recording_done(hass)

    rows = await _stored(hass, ENERGY_ID)
    _assert_states(rows, readings)
    _assert_steps(rows)
    assert rows[-1][2] == pytest.approx(float(sum(readings.values())))


async def test_hours_stored_as_given_write_nothing(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    del recorder_mock
    readings = _energy(START, 48)
    assert await async_write_hours(hass, [(_energy_meta(), readings)])
    await async_wait_recording_done(hass)

    with _spy_writes() as spy:
        assert await async_write_hours(
            hass, [(_energy_meta(), _between(readings, START + DAY, START + 2 * DAY))]
        )

    spy.assert_not_called()


async def test_no_hours_given_waits_for_nothing(
    recorder_mock: Any, hass: HomeAssistant
) -> None:
    del recorder_mock
    instance = get_instance(hass)

    with (
        patch.object(
            instance, "async_clear_statistics", wraps=instance.async_clear_statistics
        ) as barrier,
        _spy_writes() as spy,
    ):
        assert await async_write_hours(hass, [(_energy_meta(), {}), (_cost_meta(), {})])

    barrier.assert_not_called()
    spy.assert_not_called()
