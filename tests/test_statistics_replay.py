"""
The one statistics write primitive, ``statistics.async_replay``, on a real recorder.

Every case writes through ``async_replay`` on the ``recorder_mock`` recorder
and then checks the running sums of energy and cost: each stored hour's sum
must be the previous stored hour's plus its own state, and the first stored
hour's sum its state. The only exception a case allows is a manual "Adjust
sum" a user made after the hours replayed, which must survive, unless it
was made at the first stored hour after them, which the replay makes follow
on again. Every value is synthetic.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from typing import Any
from unittest.mock import patch

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder import statistics as recorder_statistics
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy.const import DOMAIN
from custom_components.meridian_energy.models import MeridianMeasurement
from custom_components.meridian_energy.statistics import (
    _UNSETTLED,
    WRITE_LOCKS,
    StatisticsAnchorError,
    StatisticsWriteError,
    _cost_metadata,
    _energy_metadata,
    async_account_period_totals,
    async_latest_numeric_statistic_start,
    async_replay,
    consumption_ids,
    generation_ids,
    property_key,
)

from .recorder_hold import RecorderHold

T0 = datetime(2026, 7, 1, tzinfo=UTC)
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
_TOLERANCE = 1e-6
_NAMES = {
    "energy_name": "Meridian grid import",
    "cost_name": "Meridian grid import cost",
}


@pytest.fixture
def mock_recorder_before_hass(recorder_db_url: str) -> None:
    """Prepare Recorder storage before the Home Assistant fixture starts."""
    del recorder_db_url


def _m(
    index: int,
    kwh: str = "1",
    cents: str | None = "10",
    *,
    channel: str = "aggregate",
    quality: str = "ACTUAL",
) -> MeridianMeasurement:
    start = T0 + index * HOUR
    return MeridianMeasurement(
        start=start,
        end=start + HOUR,
        value_kwh=Decimal(kwh),
        quality=quality,
        direction="CONSUMPTION",
        channel_id=channel,
        cost_cents=None if cents is None else Decimal(cents),
    )


def _ids(name: str) -> tuple[str, str]:
    return consumption_ids(property_key(name, "synthetic-property"))


async def _replay(
    hass: HomeAssistant,
    name: str,
    measurements: list[MeridianMeasurement],
    **kwargs: Any,
) -> tuple[int, int]:
    energy_id, cost_id = _ids(name)
    return await async_replay(
        hass,
        stat_energy_id=energy_id,
        stat_cost_id=cost_id,
        measurements=measurements,
        **_NAMES,
        **kwargs,
    )


async def _rows(hass: HomeAssistant, statistic_id: str) -> list[tuple[float, ...]]:
    """Return a statistic's hourly rows as (start, state, sum)."""
    stats = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        T0 - timedelta(days=1000),
        None,
        {statistic_id},
        "hour",
        None,
        {"state", "sum"},
    )
    return [
        (row["start"], float(row["state"]), float(row["sum"]))
        for row in stats.get(statistic_id, [])
    ]


async def _rows_with_or_without_sums(
    hass: HomeAssistant, statistic_id: str
) -> list[dict[str, Any]]:
    """Return a statistic's hourly rows, those without a sum included."""
    stats = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        T0 - timedelta(days=1000),
        None,
        {statistic_id},
        "hour",
        None,
        {"state", "sum"},
    )
    return list(stats.get(statistic_id, []))


def _bad_steps(rows: list[tuple[float, ...]]) -> list[float]:
    """
    Return the starts of rows whose sum breaks the prefix-sum invariant.

    The first row's sum must be its state; each later row's sum the previous
    row's plus its own state.
    """
    bad = [
        rows[0][0] for _ in [0] if rows and abs(rows[0][2] - rows[0][1]) > _TOLERANCE
    ]
    return bad + [
        later[0]
        for earlier, later in pairwise(rows)
        if abs((later[2] - earlier[2]) - later[1]) > _TOLERANCE
    ]


async def _assert_prefix_sums(hass: HomeAssistant, name: str) -> None:
    """Assert the invariant on a property's energy and cost statistics."""
    for statistic_id in _ids(name):
        rows = await _rows(hass, statistic_id)
        assert _bad_steps(rows) == [], statistic_id


def _hours(rows: list[tuple[float, ...]]) -> list[int]:
    return [round((row[0] - T0.timestamp()) / 3600) for row in rows]


async def _plain_write(
    hass: HomeAssistant, statistic_id: str, rows: list[tuple[int, float, float]]
) -> None:
    """Write rows exactly as given, as an earlier release or another writer might."""
    metadata = (
        _cost_metadata(statistic_id, "Meridian grid import cost")
        if "cost" in statistic_id
        else _energy_metadata(statistic_id, "Meridian grid import")
    )
    async_add_external_statistics(
        hass,
        metadata,
        [
            {"start": T0 + index * HOUR, "state": state, "sum": total}
            for index, state, total in rows
        ],
    )
    await async_wait_recording_done(hass)


@pytest.mark.asyncio
async def test_a_first_import_writes_running_sums_from_zero(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    written = await _replay(hass, "A-TEST-1", [_m(i, "1.25", "40") for i in range(3)])

    assert written == (3, 3)
    energy_id, cost_id = _ids("A-TEST-1")
    # Confirmed before it returned: no wait for the recorder is needed.
    assert [row[2] for row in await _rows(hass, energy_id)] == [1.25, 2.5, 3.75]
    assert [row[2] for row in await _rows(hass, cost_id)] == pytest.approx(
        [0.4, 0.8, 1.2]
    )


@pytest.mark.asyncio
async def test_nothing_to_replay_writes_nothing(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    assert await _replay(hass, "A-TEST-1", []) == (0, 0)
    assert await _rows(hass, _ids("A-TEST-1")[0]) == []


@pytest.mark.asyncio
async def test_a_first_prepend_with_no_predecessor_gives_sum_equal_state(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Older history written behind newer rows starts its sums from zero.

    Every stored row after it is shifted by one constant, so the newest sum
    is the total of every state, and no step is left inconsistent.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i, "1") for i in range(48, 72)])
    await _replay(hass, "A-TEST-1", [_m(i, "2", "20") for i in range(48)])

    energy_id, cost_id = _ids("A-TEST-1")
    energy = await _rows(hass, energy_id)
    assert _hours(energy) == list(range(72))
    assert energy[0][2] == energy[0][1] == 2.0
    assert energy[-1][2] == pytest.approx(48 * 2 + 24 * 1)
    cost = await _rows(hass, cost_id)
    assert cost[0][2] == pytest.approx(0.2)
    assert cost[-1][2] == pytest.approx(48 * 0.2 + 24 * 0.1)
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_a_backfill_between_stored_rows_shifts_only_the_rows_after_it(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(24)])
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(48, 72)])
    before = await _rows(hass, _ids("A-TEST-1")[0])
    assert before[23][2] == 24.0

    await _replay(hass, "A-TEST-1", [_m(i, "3") for i in range(24, 48)])

    after = await _rows(hass, _ids("A-TEST-1")[0])
    assert after[:24] == before[:24]
    assert after[-1][2] == pytest.approx(24 + 72 + 24)
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_weeks_backfilled_newest_first_keep_every_sum_consistent(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Each run imports one older week behind the newer stored ones.

    The shape of a backfill that fills history newest first: every week is
    written behind rows already stored, from no predecessor.
    """
    del recorder_mock
    for week in range(4):
        start = -(week + 1) * 7 * 24
        await _replay(
            hass,
            "A-TEST-1",
            [_m(i, f"{0.25 + (i % 7) * 0.1:.2f}") for i in range(start, start + 168)],
        )
        await _assert_prefix_sums(hass, "A-TEST-1")
    energy = await _rows(hass, _ids("A-TEST-1")[0])
    assert len(energy) == 4 * 168
    assert energy[-1][2] == pytest.approx(sum(row[1] for row in energy))


@pytest.mark.asyncio
async def test_the_newest_day_then_a_gap_then_older_history(
    recorder_mock, hass: HomeAssistant
) -> None:
    """The newest day alone first, then the days before it, then older ones."""
    del recorder_mock
    for start, hours in ((480, 24), (264, 216), (0, 264)):
        await _replay(hass, "A-TEST-1", [_m(i) for i in range(start, start + hours)])
        await _assert_prefix_sums(hass, "A-TEST-1")
    energy = await _rows(hass, _ids("A-TEST-1")[0])
    assert _hours(energy) == list(range(504))
    assert energy[-1][2] == pytest.approx(504.0)


@pytest.mark.asyncio
async def test_replays_do_not_depend_on_home_assistants_time_zone(
    recorder_mock, hass: HomeAssistant
) -> None:
    """Hourly lookups by time do not move with the zone Home Assistant is set to."""
    del recorder_mock
    await hass.config.async_set_time_zone("America/New_York")
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(168, 336)])
    await _replay(hass, "A-TEST-1", [_m(i, "2") for i in range(168)])
    await _assert_prefix_sums(hass, "A-TEST-1")
    energy = await _rows(hass, _ids("A-TEST-1")[0])
    assert energy[-1][2] == pytest.approx(168 * 2 + 168)


@pytest.mark.asyncio
async def test_hours_the_replay_was_not_given_keep_their_stored_states(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A replay rewrites every stored hour from its first to its last.

    Hours between them it was not given keep their stored state, so their
    sums follow on from the new rows instead of keeping stale ones.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i, "1", "10") for i in range(72)])

    revised = [_m(i, "2", "30") for i in range(10, 20)] + [
        _m(i, "2", "30") for i in range(40, 50)
    ]
    written = await _replay(hass, "A-TEST-1", revised)

    assert written == (40, 40)
    energy = await _rows(hass, _ids("A-TEST-1")[0])
    assert [row[1] for row in energy[20:40]] == [1.0] * 20
    assert energy[-1][2] == pytest.approx(52 + 20 * 2)
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_an_hour_without_a_cost_keeps_its_stored_cost(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A cost that came back as None counts as not fetched for the cost replay.

    Its stored cost is kept, and the cost sums after it follow on from the
    revised costs before it: no negative step, as an import that skipped the
    hour left.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i, "1", "10") for i in range(10)])

    await _replay(
        hass, "A-TEST-1", [_m(i, "1", None if i == 4 else "5") for i in range(10)]
    )

    cost = await _rows(hass, _ids("A-TEST-1")[1])
    assert [row[1] for row in cost] == pytest.approx([0.05] * 4 + [0.1] + [0.05] * 5)
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_no_cost_at_all_writes_the_usage_alone(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    assert await _replay(hass, "A-TEST-1", [_m(i, cents=None) for i in range(3)]) == (
        3,
        0,
    )
    assert await _rows(hass, _ids("A-TEST-1")[1]) == []


@pytest.mark.asyncio
async def test_without_the_cost_only_the_usage_is_replayed(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i, "1", "10") for i in range(5)])

    written = await _replay(
        hass, "A-TEST-1", [_m(i, "2", "99") for i in range(5)], import_cost=False
    )

    assert written == (5, 0)
    cost = await _rows(hass, _ids("A-TEST-1")[1])
    assert [row[1] for row in cost] == pytest.approx([0.1] * 5)
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_cost_after_replays_only_the_cost_of_later_hours(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i, "1", "10") for i in range(6)])

    written = await _replay(
        hass,
        "A-TEST-1",
        [_m(i, "2", "50") for i in range(6)],
        cost_after=T0 + 2 * HOUR,
    )

    assert written == (6, 3)
    cost = await _rows(hass, _ids("A-TEST-1")[1])
    assert [row[1] for row in cost] == pytest.approx([0.1] * 3 + [0.5] * 3)
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_the_predecessor_is_found_across_a_gap_longer_than_14_days(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(48)])
    later = 48 + 30 * 24

    await _replay(hass, "A-TEST-1", [_m(i, "2") for i in range(later, later + 24)])

    energy = await _rows(hass, _ids("A-TEST-1")[0])
    assert energy[48][2] == pytest.approx(48 + 2)
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_e8_a_replay_behind_more_than_2232_newer_rows_keeps_its_predecessor(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The predecessor is looked up by time, not among the newest rows.

    The old lookup read the newest 2,232 rows, the 90 days of an initial
    import and 72 hours more, and took a replay behind more than that for
    one with no history before it, starting its sums from zero again.
    """
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    await _plain_write(hass, energy_id, [(i, 1.0, float(i + 1)) for i in range(2600)])

    await _replay(hass, "A-TEST-1", [_m(i, "3", None) for i in range(100, 110)])

    energy = await _rows(hass, energy_id)
    assert len(energy) == 2600
    assert energy[100][2] == pytest.approx(103.0)
    assert energy[-1][2] == pytest.approx(2600 + 10 * 2)
    assert _bad_steps(energy) == []


@pytest.mark.asyncio
async def test_a_replay_with_only_rows_older_than_800_days_before_it_refuses(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    No predecessor within 800 days, but older rows exist: refuse and warn.

    Starting from zero would put the new rows below the old ones.
    """
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    old = -900 * 24
    await _plain_write(hass, energy_id, [(old, 1.0, 5.0)])

    with (
        caplog.at_level(logging.DEBUG, logger=statistics_module.__name__),
        pytest.raises(StatisticsAnchorError),
    ):
        await _replay(hass, "A-TEST-1", [_m(i) for i in range(3)])

    assert _hours(await _rows(hass, energy_id)) == [old]
    warnings = [
        record for record in caplog.records if record.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert "running total" in warnings[0].getMessage()

    # Said once: a second refusal of the same statistic logs at DEBUG.
    with (
        caplog.at_level(logging.DEBUG, logger=statistics_module.__name__),
        pytest.raises(StatisticsAnchorError),
    ):
        await _replay(hass, "A-TEST-1", [_m(i) for i in range(3)])
    assert (
        len([record for record in caplog.records if record.levelno == logging.WARNING])
        == 1
    )


@pytest.mark.parametrize(("days", "continues"), [(790, True), (805, False)])
@pytest.mark.asyncio
async def test_the_predecessor_is_looked_for_back_to_800_days_before_the_replay(
    recorder_mock, hass: HomeAssistant, days: int, continues: bool
) -> None:
    """
    The search reaches back to 800 days before the replay's first hour.

    That is 14 days, then back to 800 days before it, not 800 days more.
    """
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    await _plain_write(hass, energy_id, [(-days * 24, 1.0, 5.0)])

    if continues:
        await _replay(hass, "A-TEST-1", [_m(i) for i in range(3)])
        assert (await _rows(hass, energy_id))[-1][2] == pytest.approx(8.0)
    else:
        with pytest.raises(StatisticsAnchorError):
            await _replay(hass, "A-TEST-1", [_m(i) for i in range(3)])
        assert len(await _rows(hass, energy_id)) == 1


@pytest.mark.parametrize(
    ("without_sum", "with_sum"),
    [
        pytest.param(-1, None, id="near-without-sum"),
        pytest.param(-20 * 24, None, id="far-without-sum"),
        pytest.param(-1, -900 * 24, id="near-without-sum-sum-beyond-800-days"),
    ],
)
@pytest.mark.asyncio
async def test_a_replay_after_rows_without_a_running_total_refuses(
    recorder_mock, hass: HomeAssistant, without_sum: int, with_sum: int | None
) -> None:
    """
    Stored rows before the replay with no numeric sum are not taken for none.

    With no row with a sum within 800 days before the span, rows stored
    before it, with a sum or without, refuse the replay, and the
    coordinator finds no running total to continue either.
    """
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    if with_sum is not None:
        await _plain_write(hass, energy_id, [(with_sum, 1.0, 5.0)])
    async_add_external_statistics(
        hass,
        _energy_metadata(energy_id, "Meridian grid import"),
        [{"start": T0 + without_sum * HOUR, "state": 1.0}],
    )
    await async_wait_recording_done(hass)

    assert await async_latest_numeric_statistic_start(hass, energy_id) is None
    with pytest.raises(StatisticsAnchorError):
        await _replay(hass, "A-TEST-1", [_m(i) for i in range(3)])
    assert len(await _rows_with_or_without_sums(hass, energy_id)) == (
        1 if with_sum is None else 2
    )


@pytest.mark.parametrize("hours_back", [3, 20 * 24, 700 * 24])
@pytest.mark.asyncio
async def test_a_numeric_predecessor_older_than_rows_without_a_sum_is_used(
    recorder_mock, hass: HomeAssistant, hours_back: int
) -> None:
    """
    Rows without a running total are passed over, however far back the last sum is.

    Another writer, such as Home Assistant's statistics import, can store
    rows without a sum. The replay continues from the newest row with one
    within 800 days, as the newest numeric row the coordinator looks up is
    found (``async_latest_numeric_statistic_start``): until now, a row
    without a sum in the 14 days before the span refused the replay when
    the row with a sum was further back, while the coordinator took the
    statistic for one with a running total to continue.
    """
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    await _plain_write(hass, energy_id, [(-hours_back, 1.0, 7.0)])
    async_add_external_statistics(
        hass,
        _energy_metadata(energy_id, "Meridian grid import"),
        [{"start": T0 - HOUR, "state": 1.0}],
    )
    await async_wait_recording_done(hass)

    assert await async_latest_numeric_statistic_start(hass, energy_id) == (
        T0 - hours_back * HOUR
    )
    await _replay(hass, "A-TEST-1", [_m(i, cents=None) for i in range(2)])

    stats = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        T0,
        None,
        {energy_id},
        "hour",
        None,
        {"sum"},
    )
    assert [row["sum"] for row in stats[energy_id]] == [8.0, 9.0]


@pytest.mark.parametrize(
    "replayed",
    [
        pytest.param(range(10), id="full-window-before-the-adjustment"),
        pytest.param(range(-48, 0), id="backfill-before-every-row"),
        pytest.param(range(48, 60), id="backfill-into-a-gap"),
    ],
)
@pytest.mark.asyncio
async def test_a_manual_adjustment_after_the_span_survives_a_replay(
    recorder_mock, hass: HomeAssistant, replayed: range
) -> None:
    """
    A user's "Adjust sum" made after the hours replayed is kept.

    The rows after the replayed span are shifted by one constant, not
    summed again from their states, so the user's -5 kWh step at hour 30 is
    the only inconsistent step left, whether the replay rewrote stored
    hours (a reconciliation) or added older ones (a backfill).
    """
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(48)])
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(60, 72)])
    get_instance(hass).async_adjust_statistics(energy_id, T0 + 30 * HOUR, -5.0, "kWh")
    await async_wait_recording_done(hass)

    await _replay(hass, "A-TEST-1", [_m(i, "2") for i in replayed])

    energy = await _rows(hass, energy_id)
    assert _bad_steps(energy) == [(T0 + 30 * HOUR).timestamp()]
    states = sum(row[1] for row in energy)
    assert energy[-1][2] == pytest.approx(states - 5.0)
    assert _bad_steps(await _rows(hass, _ids("A-TEST-1")[1])) == []


@pytest.mark.parametrize(
    ("replayed", "adjusted", "kept"),
    [
        pytest.param(range(48, 60), 60, False, id="backfill-first-hour-after"),
        pytest.param(range(48, 60), 61, True, id="backfill-second-hour-after"),
        pytest.param(range(10), 10, False, id="rewrite-first-hour-after"),
        pytest.param(range(10), 11, True, id="rewrite-second-hour-after"),
    ],
)
@pytest.mark.asyncio
async def test_an_adjustment_at_the_first_hour_after_the_span_is_undone(
    recorder_mock, hass: HomeAssistant, replayed: range, adjusted: int, kept: bool
) -> None:
    """
    The shift makes the first stored row after the span follow on again.

    Its step becomes that row's state (delta = the last row's new sum + the
    next row's state - the next row's old sum), so an "Adjust sum" a user
    made at exactly that hour is undone there and in every row after it,
    even when the replay writes the values already stored. One made at any
    later hour is kept: the rows after the first are shifted by the same
    amount, so their steps stay as they were.
    """
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(48)])
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(60, 72)])
    get_instance(hass).async_adjust_statistics(
        energy_id, T0 + adjusted * HOUR, -5.0, "kWh"
    )
    await async_wait_recording_done(hass)

    await _replay(hass, "A-TEST-1", [_m(i) for i in replayed])

    energy = await _rows(hass, energy_id)
    states = sum(row[1] for row in energy)
    if kept:
        assert _bad_steps(energy) == [(T0 + adjusted * HOUR).timestamp()]
        assert energy[-1][2] == pytest.approx(states - 5.0)
    else:
        assert _bad_steps(energy) == []
        assert energy[-1][2] == pytest.approx(states)


@pytest.mark.asyncio
async def test_two_writers_are_serialised_by_the_lock(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Two replays of one statistic, as the setup flow's and an entry's, take turns.

    The first, a backfill, is held at its first read, inside its locks. The
    second, a revision started meanwhile, neither reads nor queues anything
    until the first has confirmed its writes and released them; it then
    reads what the first wrote, so every sum is consistent. Left to timing
    alone, the two would often end consistent without the lock too, so the
    test holds the first instead.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(48, 96)])
    locks = hass.data[DOMAIN][WRITE_LOCKS]
    energy_id, cost_id = _ids("A-TEST-1")
    assert set(locks) == {energy_id, cost_id}
    real_read = statistics_module._async_read_rows
    real_add = statistics_module.async_add_external_statistics
    release = asyncio.Event()
    calls = {"reads": 0, "writes": 0}

    async def held_read(*args: Any, **kwargs: Any) -> Any:
        calls["reads"] += 1
        if calls["reads"] == 1:
            await release.wait()
        return await real_read(*args, **kwargs)

    def counted_add(*args: Any, **kwargs: Any) -> None:
        calls["writes"] += 1
        real_add(*args, **kwargs)

    with (
        patch.object(statistics_module, "_async_read_rows", held_read),
        patch.object(statistics_module, "async_add_external_statistics", counted_add),
    ):
        first = hass.async_create_task(
            _replay(hass, "A-TEST-1", [_m(i, "2") for i in range(48)])
        )
        for _ in range(10):
            await asyncio.sleep(0)
        assert calls == {"reads": 1, "writes": 0}
        assert locks[energy_id].locked()
        assert locks[cost_id].locked()
        second = hass.async_create_task(
            _replay(hass, "A-TEST-1", [_m(i, "3") for i in range(80, 96)])
        )
        for _ in range(10):
            await asyncio.sleep(0)
        assert calls == {"reads": 1, "writes": 0}

        release.set()
        await asyncio.gather(first, second)

    assert not any(lock.locked() for lock in locks.values())
    energy = await _rows(hass, energy_id)
    assert energy[-1][2] == pytest.approx(48 * 2 + 32 + 16 * 3)
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.parametrize("read", range(4))
@pytest.mark.asyncio
async def test_a_cancellation_before_the_writes_are_queued_writes_nothing(
    recorder_mock, hass: HomeAssistant, read: int
) -> None:
    """
    A replay cancelled at any read before it queues its writes writes nothing.

    Building the rows and queueing the energy rows, the cost rows and both
    shifts has no await between them, so it cannot be cancelled part-way:
    the energy is never written without its cost.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(24, 48)])
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(-30 * 24, -29 * 24)])
    before = [await _rows(hass, statistic_id) for statistic_id in _ids("A-TEST-1")]
    real_read = statistics_module._async_read_rows
    calls = {"count": 0}

    async def cancelled_at(*args: Any, **kwargs: Any) -> Any:
        calls["count"] += 1
        if calls["count"] > read:
            raise asyncio.CancelledError
        return await real_read(*args, **kwargs)

    with (
        patch.object(statistics_module, "_async_read_rows", cancelled_at),
        pytest.raises(asyncio.CancelledError),
    ):
        # Older rows before a gap of more than 14 days: the first read, the
        # one further back, then the reads that confirm the writes.
        await _replay(hass, "A-TEST-1", [_m(i, "2", "30") for i in range(24)])
    await async_wait_recording_done(hass)

    after = [await _rows(hass, statistic_id) for statistic_id in _ids("A-TEST-1")]
    if read < 2:
        assert after == before
    else:
        # Cancelled while confirming: both statistics were written and shifted.
        assert _hours(after[0]) == _hours(after[1])
        assert len(after[0]) == len(before[0]) + 24
    await _assert_prefix_sums(hass, "A-TEST-1")
    assert not any(lock.locked() for lock in hass.data[DOMAIN][WRITE_LOCKS].values())


@pytest.mark.asyncio
async def test_every_write_and_shift_is_queued_in_one_turn_of_the_event_loop(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Nothing can run, or cancel the replay, between its energy and cost writes.

    The energy rows, the cost rows and both shifts are queued in one turn of
    the event loop: a callback scheduled when the first is queued has not
    run by the time the last is. An await anywhere between them, even one
    that yields once, would let it run first, and would let a cancellation
    (the dialog closed, a time limit, a reload) land there, leaving the
    energy written without its cost.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(24, 48)])
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(-30 * 24, -29 * 24)])
    energy_id, cost_id = _ids("A-TEST-1")
    recorder = get_instance(hass)
    real_add = statistics_module.async_add_external_statistics
    real_adjust = recorder.async_adjust_statistics
    turn = {"passed": False}
    queued: list[tuple[str, str, bool]] = []

    def note(kind: str, statistic_id: str) -> None:
        if not queued:
            hass.loop.call_soon(turn.__setitem__, "passed", True)
        queued.append((kind, statistic_id, turn["passed"]))

    def add(hass_: HomeAssistant, metadata: Any, rows: Any) -> None:
        note("rows", metadata["statistic_id"])
        real_add(hass_, metadata, rows)

    def adjust(statistic_id: str, *args: Any) -> None:
        note("shift", statistic_id)
        real_adjust(statistic_id, *args)

    with (
        patch.object(statistics_module, "async_add_external_statistics", add),
        patch.object(recorder, "async_adjust_statistics", adjust),
    ):
        await _replay(hass, "A-TEST-1", [_m(i, "2", "30") for i in range(24)])

    assert queued == [
        ("rows", energy_id, False),
        ("rows", cost_id, False),
        ("shift", energy_id, False),
        ("shift", cost_id, False),
    ]
    assert turn["passed"] is True
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_a_cancellation_while_waiting_for_the_lock_writes_nothing(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(3)])
    energy_id, _cost_id = _ids("A-TEST-1")
    lock = hass.data[DOMAIN][WRITE_LOCKS][energy_id]
    await lock.acquire()
    try:
        waiting = hass.async_create_task(
            _replay(hass, "A-TEST-1", [_m(i, "5") for i in range(3)])
        )
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
    finally:
        lock.release()
    await async_wait_recording_done(hass)
    assert [row[1] for row in await _rows(hass, energy_id)] == [1.0] * 3


@pytest.mark.asyncio
async def test_a_barrier_that_times_out_reports_a_statistics_write_failure(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Writes the recorder does not confirm in three barriers fail the replay.

    The lock is released, so the next replay can heal what the recorder
    saves later: here it does, once released.
    """
    del recorder_mock
    hold = RecorderHold(hass)
    hold.hold_once()
    try:
        with (
            patch.object(statistics_module, "CONFIRM_TIMEOUT_SECONDS", 0.05),
            pytest.raises(StatisticsWriteError),
        ):
            await _replay(hass, "A-TEST-1", [_m(i) for i in range(3)])
    finally:
        hold.release()
    await async_wait_recording_done(hass)

    assert not any(lock.locked() for lock in hass.data[DOMAIN][WRITE_LOCKS].values())
    assert _hours(await _rows(hass, _ids("A-TEST-1")[0])) == [0, 1, 2]
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_the_replay_after_an_unconfirmed_one_waits_for_its_writes(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A replay never reads the running totals an unconfirmed one left queued.

    The first replay, a revision, is not confirmed behind a held recorder:
    it raises and releases its locks with its rows still queued. The next
    replay of the same statistics, started at once, waits behind the
    recorder before its first read, so it continues the revision's sums,
    rather than the stale ones, which would leave a seam where the two meet
    once both are saved. Reads run on the recorder's executor, which the
    hold does not stop, so without the wait it would read at once.
    """
    del recorder_mock
    energy_id, cost_id = _ids("A-TEST-1")
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(48)])
    hold = RecorderHold(hass)
    hold.hold_once()
    try:
        with (
            patch.object(statistics_module, "CONFIRM_TIMEOUT_SECONDS", 0.05),
            pytest.raises(StatisticsWriteError),
        ):
            await _replay(hass, "A-TEST-1", [_m(i, "2", "20") for i in range(10, 48)])
        hold.release_after(0.3)
        written = await _replay(
            hass, "A-TEST-1", [_m(i, "3", "30") for i in range(40, 51)]
        )
    finally:
        hold.release()

    assert written == (11, 11)
    await _assert_prefix_sums(hass, "A-TEST-1")
    assert (await _rows(hass, energy_id))[-1][2] == pytest.approx(10 + 30 * 2 + 11 * 3)
    assert (await _rows(hass, cost_id))[-1][2] == pytest.approx(1 + 30 * 0.2 + 11 * 0.3)
    assert not hass.data[DOMAIN][_UNSETTLED]


@pytest.mark.asyncio
async def test_a_replay_that_cannot_wait_for_unconfirmed_writes_writes_nothing(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    When the recorder has still not saved them, the next replay writes nothing.

    Its wait behind the recorder times out as a replay's own barriers do, so
    it raises ``StatisticsWriteError`` before it reads, rather than continue
    the stale sums. The statistics stay marked: a later replay waits again,
    and, once the recorder has caught up, writes them consistently.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(48)])
    hold = RecorderHold(hass)
    hold.hold_once()
    real_read = statistics_module._async_read_rows
    reads: list[datetime] = []

    async def counted_read(hass_: HomeAssistant, ids: Any, start: datetime, end: Any):
        reads.append(start)
        return await real_read(hass_, ids, start, end)

    try:
        with patch.object(statistics_module, "CONFIRM_TIMEOUT_SECONDS", 0.05):
            with pytest.raises(StatisticsWriteError):
                await _replay(
                    hass, "A-TEST-1", [_m(i, "2", "20") for i in range(10, 48)]
                )
            reads.clear()
            with (
                patch.object(statistics_module, "_async_read_rows", counted_read),
                pytest.raises(StatisticsWriteError),
            ):
                await _replay(
                    hass, "A-TEST-1", [_m(i, "3", "30") for i in range(40, 51)]
                )
    finally:
        hold.release()

    assert reads == []
    assert set(hass.data[DOMAIN][_UNSETTLED]) == set(_ids("A-TEST-1"))
    assert not any(lock.locked() for lock in hass.data[DOMAIN][WRITE_LOCKS].values())
    assert await _replay(
        hass, "A-TEST-1", [_m(i, "3", "30") for i in range(40, 51)]
    ) == (11, 11)
    await _assert_prefix_sums(hass, "A-TEST-1")
    assert not hass.data[DOMAIN][_UNSETTLED]


@pytest.mark.asyncio
async def test_a_write_the_recorder_retries_is_confirmed_after_another_barrier(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    An import the recorder had to queue again lands behind the first barrier.

    The read-back then misses it, and the replay waits behind a second
    barrier, which is behind the retry. The retry is slow, as it is when the
    lock error that queued it again is still clearing, so the first
    read-back runs before it is saved: one barrier would fail the replay.
    """
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    real_import = recorder_statistics.import_statistics
    real_joins_hold = statistics_module._async_joins_hold
    calls: list[str] = []
    joins: list[bool] = []

    def retried_once(instance: Any, metadata: Any, statistics: Any, table: Any) -> Any:
        if metadata["statistic_id"] == energy_id:
            calls.append("energy")
            if len(calls) == 1:
                return False
            time.sleep(0.5)
        return real_import(instance, metadata, statistics, table)

    async def joins_hold(hass_: HomeAssistant, replays: Any) -> bool:
        held = await real_joins_hold(hass_, replays)
        joins.append(held)
        return held

    with (
        patch(
            "homeassistant.components.recorder.tasks.statistics.import_statistics",
            side_effect=retried_once,
        ),
        patch.object(statistics_module, "_async_joins_hold", joins_hold),
    ):
        assert await _replay(hass, "A-TEST-1", [_m(i) for i in range(3)]) == (3, 3)

    assert calls == ["energy", "energy"]
    assert joins == [False, True]
    await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_a_dropped_write_fails_the_replay_and_the_next_one_heals_it(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Rows the recorder never saved fail the replay that wrote them.

    The caller then keeps them as not yet written, and replays them at its
    next run, from the same first hour: the sums are whole again.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(48)])
    energy_id, _cost_id = _ids("A-TEST-1")
    real_adjust = recorder_statistics.adjust_statistics
    dropped: list[str] = []

    def dropping(instance: Any, statistic_id: str, *args: Any) -> bool:
        if statistic_id == energy_id and not dropped:
            dropped.append(statistic_id)
            return True
        return real_adjust(instance, statistic_id, *args)

    revised = [_m(i, "2") for i in range(10, 20)]
    with (
        patch(
            "homeassistant.components.recorder.tasks.statistics.adjust_statistics",
            side_effect=dropping,
        ),
        pytest.raises(StatisticsWriteError),
    ):
        await _replay(hass, "A-TEST-1", revised)
    assert dropped == [energy_id]
    assert _bad_steps(await _rows(hass, energy_id)) == [(T0 + 20 * HOUR).timestamp()]

    await _replay(hass, "A-TEST-1", revised)

    await _assert_prefix_sums(hass, "A-TEST-1")
    assert (await _rows(hass, energy_id))[-1][2] == pytest.approx(48 + 10)


@pytest.mark.asyncio
async def test_a_dropped_import_is_caught_at_its_last_row(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The first row of a dropped import may read back as written: the last does not.

    Only the newest hour changed, so the stored first row already has the
    running sum the replay wrote for it.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i, cents=None) for i in range(10)])
    energy_id, _cost_id = _ids("A-TEST-1")
    real_import = recorder_statistics.import_statistics

    def dropped(instance: Any, metadata: Any, statistics: Any, table: Any) -> bool:
        if metadata["statistic_id"] == energy_id:
            return True
        return real_import(instance, metadata, statistics, table)

    revised = [_m(i, cents=None) for i in range(9)] + [_m(9, "2", None)]
    with (
        patch(
            "homeassistant.components.recorder.tasks.statistics.import_statistics",
            side_effect=dropped,
        ),
        pytest.raises(StatisticsWriteError),
    ):
        await _replay(hass, "A-TEST-1", revised)

    await _replay(hass, "A-TEST-1", revised)
    assert (await _rows(hass, energy_id))[-1][2] == pytest.approx(11.0)


@pytest.mark.asyncio
async def test_a_dropped_import_with_the_same_last_sum_is_caught_by_its_state(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A dropped import whose first row and last sum read back as written fails.

    The weekly reconciliation replays its whole window, so its first row
    can be stored already, and revisions inside the span can leave its last
    running sum as it was: here hour 8 doubles and hour 9 drops to nothing.
    Only the last row's state then tells the dropped import from a saved
    one. Taken for saved, the revision would be cached as imported, and
    lost until the next weekly reconciliation; the next replay of the same
    rows writes it.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(10)])
    energy_id, _cost_id = _ids("A-TEST-1")
    real_import = recorder_statistics.import_statistics
    dropped: list[str] = []

    def dropping(instance: Any, metadata: Any, statistics: Any, table: Any) -> bool:
        if metadata["statistic_id"] == energy_id and not dropped:
            dropped.append(energy_id)
            return True
        return real_import(instance, metadata, statistics, table)

    revised = [_m(i) for i in range(8)] + [_m(8, "2"), _m(9, "0")]
    with (
        patch(
            "homeassistant.components.recorder.tasks.statistics.import_statistics",
            side_effect=dropping,
        ),
        pytest.raises(StatisticsWriteError),
    ):
        await _replay(hass, "A-TEST-1", revised)
    assert dropped == [energy_id]
    stored = await _rows(hass, energy_id)
    assert [row[1] for row in stored[-2:]] == [1.0, 1.0]
    assert stored[-1][2] == pytest.approx(10.0)

    assert await _replay(hass, "A-TEST-1", revised) == (10, 10)

    await _assert_prefix_sums(hass, "A-TEST-1")
    stored = await _rows(hass, energy_id)
    assert [row[1] for row in stored[-2:]] == [2.0, 0.0]
    assert stored[-1][2] == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_rows_another_writer_moved_meanwhile_fail_the_junction_check(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The read-back checks the joins with the stored rows on both sides.

    A shift queued by another writer between the reads and the writes, as a
    user's adjustment, moves the predecessor away from the first row
    written: the replay reports it rather than leave a seam unnoticed.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(24)])
    energy_id, _cost_id = _ids("A-TEST-1")
    real_read = statistics_module._async_read_rows
    moved = {"done": False}

    async def moved_after_reading(*args: Any, **kwargs: Any) -> Any:
        rows = await real_read(*args, **kwargs)
        if not moved["done"]:
            moved["done"] = True
            get_instance(hass).async_adjust_statistics(
                energy_id, T0 + 5 * HOUR, 3.0, "kWh"
            )
        return rows

    with (
        patch.object(statistics_module, "_async_read_rows", moved_after_reading),
        pytest.raises(StatisticsWriteError),
    ):
        await _replay(hass, "A-TEST-1", [_m(i, "2") for i in range(12, 24)])


@pytest.mark.asyncio
async def test_rollback_compatibility_an_earlier_release_continues_the_sums(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Statistics replayed here stay consistent when an earlier 3.x release continues them.

    Earlier releases anchor an import on the newest stored row before its
    first hour and write only the rows they are given. After replays that
    filled a gap behind newer rows and shifted the rows after it, such an
    import of the newest hours still continues every sum consistently.
    """
    del recorder_mock
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(24)])
    await _replay(hass, "A-TEST-1", [_m(i) for i in range(48, 72)])
    await _replay(hass, "A-TEST-1", [_m(i, "2", "20") for i in range(24, 48)])

    energy_id, cost_id = _ids("A-TEST-1")
    for statistic_id, state in ((energy_id, 1.5), (cost_id, 0.15)):
        await _legacy_import(
            hass, statistic_id, {T0 + i * HOUR: state for i in range(60, 80)}
        )

    await _assert_prefix_sums(hass, "A-TEST-1")
    energy = await _rows(hass, energy_id)
    assert _hours(energy) == list(range(80))
    assert energy[-1][2] == pytest.approx(24 + 48 + 12 + 20 * 1.5)


async def _legacy_import(
    hass: HomeAssistant, statistic_id: str, states: dict[datetime, float]
) -> None:
    """Import rows as releases before 0.3.0 did: anchored on the newest rows."""
    first = min(states)
    result = await get_instance(hass).async_add_executor_job(
        get_last_statistics, hass, 90 * 24 + 72, statistic_id, False, {"sum"}
    )
    candidates = [
        row
        for row in result.get(statistic_id, [])
        if row["start"] < first.timestamp() and row.get("sum") is not None
    ]
    total = Decimal(
        str(max(candidates, key=lambda row: row["start"])["sum"]) if candidates else 0
    )
    rows = []
    for start in sorted(states):
        total += Decimal(str(states[start]))
        rows.append({"start": start, "state": states[start], "sum": float(total)})
    await _plain_write_rows(hass, statistic_id, rows)


async def _plain_write_rows(
    hass: HomeAssistant, statistic_id: str, rows: list[dict[str, Any]]
) -> None:
    metadata = (
        _cost_metadata(statistic_id, "Meridian grid import cost")
        if "cost" in statistic_id
        else _energy_metadata(statistic_id, "Meridian grid import")
    )
    async_add_external_statistics(hass, metadata, rows)
    await async_wait_recording_done(hass)


@pytest.mark.parametrize("seed", range(6))
@pytest.mark.asyncio
async def test_random_replays_keep_every_running_sum_consistent(
    recorder_mock, hass: HomeAssistant, seed: int
) -> None:
    """
    The invariant holds over any sequence of replays.

    Windows of random span and position, older or newer than what is
    stored, with revised values, hours left out and costs missing, as the
    syncs' paths produce them: tips, reconciliations, backfills behind newer
    rows and catch-ups after a gap.
    """
    del recorder_mock
    rng = random.Random(seed)
    for _ in range(8):
        start = rng.randrange(-200, 400)
        span = rng.randrange(1, 120)
        rows = [
            _m(
                i,
                f"{rng.randrange(0, 300) / 100:.2f}",
                None if rng.random() < 0.1 else str(rng.randrange(0, 90)),
            )
            for i in range(start, start + span)
            if rng.random() > 0.05
        ]
        await _replay(hass, "A-TEST-1", rows)
        await _assert_prefix_sums(hass, "A-TEST-1")


@pytest.mark.asyncio
async def test_the_newest_numeric_start_is_found_behind_rows_without_a_sum(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The newest row with a running total is looked up by time, not row count.

    The old lookup read the newest 2,232 rows only.
    """
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    await _plain_write(hass, energy_id, [(0, 1.0, 1.0)])
    async_add_external_statistics(
        hass,
        _energy_metadata(energy_id, "Meridian grid import"),
        [{"start": T0 + i * HOUR, "state": 1.0} for i in range(1, 2500)],
    )
    await async_wait_recording_done(hass)

    assert await async_latest_numeric_statistic_start(hass, energy_id) == T0


@pytest.mark.asyncio
async def test_the_newest_numeric_start_is_none_without_any_running_total(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    energy_id, _cost_id = _ids("A-TEST-1")
    assert await async_latest_numeric_statistic_start(hass, energy_id) is None
    async_add_external_statistics(
        hass,
        _energy_metadata(energy_id, "Meridian grid import"),
        [{"start": T0, "state": 1.0}],
    )
    await async_wait_recording_done(hass)
    assert await async_latest_numeric_statistic_start(hass, energy_id) is None


@pytest.mark.asyncio
async def test_bill_completeness_checks_the_cost_steps_not_only_the_hours(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A cost whose running total does not follow its states is not complete.

    Every hour has a cost row, so a check of the hours alone took it for
    complete, and reported a total the stored states do not add up to.
    """
    del recorder_mock
    key = property_key("A-TEST-1", "synthetic-property")
    await _replay(hass, "A-TEST-1", [_m(i, "1", "10") for i in range(10)])

    async def totals() -> Any:
        return await async_account_period_totals(
            hass,
            property_keys=(key,),
            start=T0,
            end=T0 + 10 * HOUR,
            include_generation=False,
        )

    complete = await totals()
    assert complete.cost_complete is True
    assert complete.cost is not None
    assert float(complete.cost) == pytest.approx(1.0)

    _energy_id, cost_id = _ids("A-TEST-1")
    get_instance(hass).async_adjust_statistics(cost_id, T0 + 4 * HOUR, -0.25, "NZD")
    await async_wait_recording_done(hass)

    seamed = await totals()
    assert seamed.usage_complete is True
    assert seamed.usage == Decimal(10)
    assert seamed.cost_complete is False
    assert seamed.cost is None


@pytest.mark.asyncio
async def test_an_hour_a_credit_takes_below_zero_lowers_the_running_and_bill_cost(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    An hour whose credits outweigh its charges keeps its negative cost.

    Its row stores the negative state, the running total steps down by it,
    and the bill's cost, summed from the rows' changes, is lowered by it.
    """
    del recorder_mock
    key = property_key("A-TEST-1", "synthetic-property")
    cents = ["10", "10", "-25", "10"]
    await _replay(hass, "A-TEST-1", [_m(i, "1", c) for i, c in enumerate(cents)])

    _energy_id, cost_id = _ids("A-TEST-1")
    rows = await _rows(hass, cost_id)
    assert [row[1] for row in rows] == pytest.approx([0.1, 0.1, -0.25, 0.1])
    assert [row[2] for row in rows] == pytest.approx([0.1, 0.2, -0.05, 0.05])
    totals = await async_account_period_totals(
        hass,
        property_keys=(key,),
        start=T0,
        end=T0 + 4 * HOUR,
        include_generation=False,
    )
    assert totals.cost_complete is True
    assert totals.cost is not None
    assert float(totals.cost) == pytest.approx(0.05)


@pytest.mark.parametrize("generation", [False, True], ids=["usage", "export"])
@pytest.mark.parametrize("seam", ["earlier_release", "adjust_sum"])
@pytest.mark.asyncio
async def test_bill_completeness_checks_the_energy_steps_too(
    recorder_mock, hass: HomeAssistant, generation: bool, seam: str
) -> None:
    """
    Usage or export whose running total does not follow its states is not complete.

    Their totals are summed from the stored changes, as the cost's are, so
    a seam an earlier release left in the energy statistic, or an "Adjust
    sum" a user made there, would change the total while every hour is
    stored. The money total, which goes with it, is withheld too.
    """
    del recorder_mock
    key = property_key("A-TEST-1", "synthetic-property")
    energy_id, money_id = (generation_ids if generation else consumption_ids)(key)
    names = (
        ("Meridian grid export", "Meridian grid export credit")
        if generation
        else ("Meridian grid import", "Meridian grid import cost")
    )
    energy_rows = [
        {"start": T0 + i * HOUR, "state": 1.0, "sum": float(i + 1)} for i in range(10)
    ]
    if seam == "earlier_release":
        # Sums that jump by 5 kWh at hour 5, as a replay from a wrong
        # running total left them, while the hourly states stay 1 kWh.
        for row in energy_rows[5:]:
            row["sum"] += 5.0
    async_add_external_statistics(
        hass, _energy_metadata(energy_id, names[0]), energy_rows
    )
    async_add_external_statistics(
        hass,
        _cost_metadata(money_id, names[1]),
        [
            {"start": T0 + i * HOUR, "state": 0.1, "sum": round(0.1 * (i + 1), 6)}
            for i in range(10)
        ],
    )
    await async_wait_recording_done(hass)
    if seam == "adjust_sum":
        get_instance(hass).async_adjust_statistics(
            energy_id, T0 + 4 * HOUR, -3.0, "kWh"
        )
        await async_wait_recording_done(hass)

    totals = await async_account_period_totals(
        hass,
        property_keys=(key,),
        start=T0,
        end=T0 + 10 * HOUR,
        include_generation=generation,
    )

    if generation:
        assert (totals.export, totals.export_complete) == (None, False)
        assert (totals.credit, totals.credit_complete) == (None, False)
    else:
        assert (totals.usage, totals.usage_complete) == (None, False)
        assert (totals.cost, totals.cost_complete) == (None, False)


@pytest.mark.asyncio
async def test_bill_totals_do_not_wait_without_end_for_a_busy_recorder(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The wait before the totals read has a time limit, then reads anyway.

    The recorder is held with other work queued behind it, as a backlog
    would be: a wait for its queue to empty, as before 0.3.0, would last
    until the hold ends. The totals come back long before that, from the
    rows already saved.
    """
    del recorder_mock
    key = property_key("A-TEST-1", "synthetic-property")
    await _replay(hass, "A-TEST-1", [_m(i, "1", "10") for i in range(4)])
    hold = RecorderHold(hass)
    hold.hold_once()
    other_id, _other_cost_id = _ids("A-TEST-2")
    async_add_external_statistics(
        hass,
        _energy_metadata(other_id, "Meridian grid import"),
        [{"start": T0, "state": 1.0, "sum": 1.0}],
    )
    try:
        with patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.05):
            async with asyncio.timeout(5):
                totals = await async_account_period_totals(
                    hass,
                    property_keys=(key,),
                    start=T0,
                    end=T0 + 4 * HOUR,
                    include_generation=False,
                )
    finally:
        hold.release()
    await async_wait_recording_done(hass)
    assert totals.usage == Decimal(4)
    assert len(await _rows(hass, other_id)) == 1
