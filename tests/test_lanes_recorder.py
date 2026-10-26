"""
The lanes' coverage against a real recorder: older history filled in over updates.

A lane is one direction of one property seen as a history to complete back
to its target: 90 days, or the start of the billing period when that is
older. After its own walk, each update asks for at most 6 windows more of
the days still missing, newest first, and imports each behind the newer
stored rows. Every case checks the running sums: every stored hour's sum is
the previous hour's plus its own state, for usage and for cost. Every value
is synthetic.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy.api import (
    MeridianAuthenticationError,
    MeridianGraphQLError,
    MeridianRateLimitError,
    MeridianUpstreamError,
)
from custom_components.meridian_energy.const import (
    INITIAL_BACKFILL,
    POLL_BUDGET_SECONDS,
    SETUP_IMPORT_BUDGET_SECONDS,
    UPDATE_INTERVAL,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.lanes import (
    COVERAGE_WINDOWS,
    coverage_target,
    revision_day,
)
from custom_components.meridian_energy.models import (
    MeridianAccount,
    MeridianBillingPeriod,
    MeridianMeterPoint,
    MeridianProperty,
    SyncMode,
)
from custom_components.meridian_energy.statistics import (
    StatisticsWriteError,
    async_add_external_statistics,
    async_replay,
    consumption_ids,
    generation_ids,
    property_key,
)
from custom_components.meridian_energy.windows import NZ, local_midnight, plan_windows

from .kraken_fake import FakeKraken
from .test_windows_recorder import (
    NOW,
    PROPERTY,
    _bad_steps,
    _rows,
    mock_recorder_before_hass,  # noqa: F401 - the recorder fixture's setup
)

HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
TODAY = NOW.astimezone(NZ).date()


def _account(number: str, *, feed_in: bool = False) -> MeridianAccount:
    return MeridianAccount(
        number=number,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id=PROPERTY,
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("synthetic-meter", "synthetic-icp", feed_in),
                ),
            ),
        ),
    )


class _Service:
    """
    A measurements service that records requests and fails chosen windows.

    ``krakens`` answers a direction from a history of its own, as a grid
    export that never exported.
    """

    def __init__(
        self, kraken: FakeKraken, krakens: dict[str, FakeKraken] | None = None
    ) -> None:
        self.kraken = kraken
        self.krakens = krakens or {}
        self.fail: dict[date, BaseException] = {}
        self.requests: list[tuple[date, date, str]] = []

    async def async_get_measurements(self, **kwargs: Any) -> Any:
        direction = str(kwargs["direction"])
        self.requests.append((kwargs["start_on"], kwargs["end_on"], direction))
        if (error := self.fail.get(kwargs["start_on"])) is not None:
            raise error
        kraken = self.krakens.get(direction, self.kraken)
        return await kraken.async_get_measurements(**kwargs)


def _coordinator(
    hass: HomeAssistant,
    number: str,
    service: _Service,
    *,
    billing: MeridianBillingPeriod | None = None,
    feed_in: bool = False,
    unsaved: dict[tuple[str, str], dict[date, datetime]] | None = None,
    unconfirmed: dict[tuple[str, str], datetime] | None = None,
) -> MeridianDataCoordinator:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(
        return_value=(_account(number, feed_in=feed_in),)
    )
    client.async_get_billing_period = AsyncMock(return_value=billing)
    client.async_get_measurements = AsyncMock(
        side_effect=service.async_get_measurements
    )
    return MeridianDataCoordinator(
        hass, client, unsaved=unsaved, unconfirmed=unconfirmed
    )


async def _sync(
    hass: HomeAssistant, coordinator: MeridianDataCoordinator, now: datetime
) -> Any:
    """Run one hourly update at ``now``, with its time budget."""
    with (
        patch.object(coordinator_module, "_utcnow", return_value=now),
        patch.object(coordinator_module.random, "uniform", return_value=3.5),
    ):
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
    await async_wait_recording_done(hass)
    return data


async def _restarted(
    hass: HomeAssistant, number: str, service: _Service, **kwargs: Any
) -> MeridianDataCoordinator:
    """
    Return a new coordinator once its first sync, a restart's, has run.

    A first sync asks for no older history: Home Assistant shows the
    entities once it returns, and it knows no billing period yet. Its
    requests are forgotten, so ``service.requests`` holds those of the
    updates after it, the first of which a test may run at the same time:
    an hourly update, of one request, whose walk changes nothing.
    """
    coordinator = _coordinator(hass, number, service, **kwargs)
    data = await _sync(hass, coordinator, service.kraken.now)
    assert data.coverage_windows == data.revision_windows == 0
    service.requests.clear()
    return coordinator


async def _seed_recent(
    hass: HomeAssistant,
    number: str,
    kraken: FakeKraken,
    days: int,
    *,
    direction: str = "CONSUMPTION",
    since: datetime | None = None,
) -> tuple[str, str]:
    """
    Store only the last ``days`` days of the history, as a partial setup does.

    Or the history from ``since`` on. The rows are those Meridian returns
    for them, written by the replay.
    """
    key = property_key(number, PROPERTY)
    energy_id, cost_id = (
        consumption_ids(key) if direction == "CONSUMPTION" else generation_ids(key)
    )
    if since is None:
        since = kraken.now - timedelta(days=days)
    rows = []
    day, last = since.astimezone(NZ).date(), kraken.now.astimezone(NZ).date()
    while day <= last:
        # A week at a time, as a page holds 744 readings at most.
        end = min(day + 6 * DAY, last)
        page = await kraken.async_get_measurements(
            account_number=number,
            property_id=PROPERTY,
            direction=direction,
            start_on=day,
            end_on=end,
            last=744,
            before=None,
        )
        rows += [
            item
            for item in page.measurements
            if item.start >= since and (item.end or item.start) <= kraken.now
        ]
        day = end + DAY
    await async_replay(
        hass,
        stat_energy_id=energy_id,
        stat_cost_id=cost_id,
        energy_name="Meridian grid import",
        cost_name="Meridian grid import cost",
        measurements=rows,
    )
    await async_wait_recording_done(hass)
    return energy_id, cost_id


def _ceil_hour(value: datetime) -> datetime:
    floored = value.replace(minute=0, second=0, microsecond=0)
    return floored if floored == value else floored + HOUR


def _missing_hours(
    rows: list[tuple[float, ...]], start: datetime, end: datetime
) -> list[datetime]:
    """Return the hours from ``start`` until ``end`` with no stored row."""
    present = {row[0] for row in rows}
    missing, hour = [], _ceil_hour(start)
    while hour < end:
        if hour.timestamp() not in present:
            missing.append(hour)
        hour += HOUR
    return missing


def _floor_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


async def _assert_consistent(hass: HomeAssistant, *statistic_ids: str) -> None:
    for statistic_id in statistic_ids:
        assert _bad_steps(await _rows(hass, statistic_id)) == []


# ------------------------------------------------------- filling over runs


@pytest.mark.asyncio
async def test_a_90_day_history_is_filled_in_over_several_updates(
    recorder_mock, hass: HomeAssistant, waits: AsyncMock
) -> None:
    """
    Only the last 20 days are stored: the updates fetch the other 70.

    The restart's first sync asks for none of them: Home Assistant shows
    the entities once it returns. The update after it asks for 6 windows
    after its own walk, newest first, a week at most each, 3.5 s apart (2-5
    s at random); the next asks for the other 5, and the one after for
    none: the history then reaches back to midnight 90 days back with no
    hour missing and every running sum consistent.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-L1", kraken, 20)
    coordinator = _coordinator(hass, "A-TEST-L1", service)
    data = await _sync(hass, coordinator, NOW)
    assert data.sync_mode is SyncMode.RESTART
    assert data.coverage_windows == 0
    assert len(service.requests) == 3
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is False
    # A clock that moves only as the coordinator waits, so the pauses do not
    # depend on how long the test takes.
    clock = {"now": 1000.0}

    async def wait(seconds: float) -> None:
        clock["now"] += seconds

    waits.side_effect = wait
    at = NOW + HOUR
    kraken.now = at
    with patch.object(
        coordinator_module, "monotonic", side_effect=lambda: clock["now"]
    ):
        data = await _sync(hass, coordinator, at)

    assert data.sync_mode is SyncMode.TIP
    assert data.coverage_windows == COVERAGE_WINDOWS
    walk, coverage = service.requests[3:4], service.requests[4:]
    assert len(walk) == 1
    assert len(coverage) == COVERAGE_WINDOWS
    assert coverage == sorted(coverage, reverse=True)
    assert all((end - start).days < 7 for start, end, _direction in coverage)
    # Each window ends the day before the newer one starts.
    assert all(older[1] == newer[0] - DAY for newer, older in pairwise(coverage))
    pauses = [call.args[0] for call in waits.await_args_list][-COVERAGE_WINDOWS:]
    assert pauses == [pytest.approx(3.5)] * COVERAGE_WINDOWS
    await _assert_consistent(hass, energy_id, cost_id)

    later = at + HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    assert data.sync_mode is SyncMode.TIP
    assert data.coverage_windows == 5
    rows = await _rows(hass, energy_id)
    target = coverage_target(later, None)
    assert target == local_midnight((later - INITIAL_BACKFILL).astimezone(NZ).date())
    assert rows[0][0] == target.timestamp()
    assert _missing_hours(rows, target, _floor_hour(later)) == []
    await _assert_consistent(hass, energy_id, cost_id)
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True

    last = later + HOUR
    kraken.now = last
    sent = len(service.requests)
    data = await _sync(hass, coordinator, last)
    assert data.coverage_windows == 0
    assert len(service.requests) - sent == 1


@pytest.mark.asyncio
async def test_a_lane_forgets_the_dates_its_target_has_moved_past(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Two days after the history was completed, its target is two days later.

    The lane no longer keeps the dates before it, which it never asks for
    again, so what it keeps does not grow day after day.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-L4", kraken, 20)
    coordinator = _coordinator(hass, "A-TEST-L4", service)
    for at in (NOW, NOW + HOUR, NOW + 2 * HOUR):
        kraken.now = at
        await _sync(hass, coordinator, at)
    lane = coordinator._lanes[(property_key("A-TEST-L4", PROPERTY), "CONSUMPTION")]
    first = coverage_target(NOW + 2 * HOUR, None).astimezone(NZ).date()
    assert min(lane.fetched) == first

    later = NOW + 2 * DAY
    kraken.now = later
    data = await _sync(hass, coordinator, later)

    assert data.coverage_windows == 0
    assert min(lane.fetched) == first + 2 * DAY
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_each_lane_asks_for_at_most_6_windows_an_update(
    recorder_mock, hass: HomeAssistant
) -> None:
    """Grid import and export each take 6, one after the other, in turns."""
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-L2", kraken, 20)
    await _seed_recent(hass, "A-TEST-L2", kraken, 20, direction="GENERATION")
    coordinator = await _restarted(hass, "A-TEST-L2", service, feed_in=True)

    data = await _sync(hass, coordinator, NOW)

    assert data.coverage_windows == 2 * COVERAGE_WINDOWS
    # After each direction's one request of the hourly update.
    directions = [direction for _start, _end, direction in service.requests[2:]]
    assert directions == ["CONSUMPTION", "GENERATION"] * COVERAGE_WINDOWS


@pytest.mark.asyncio
async def test_a_restart_goes_on_from_what_the_recorder_holds(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A new coordinator knows nothing of the last one: it reads the recorder.

    After one update filled 6 weeks, a restart's first sync sends its own 3
    requests and asks for no older history, so the entities get their state
    without waiting for it. The update after asks for the other 5 windows
    only, starting where the first left off.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-L3", kraken, 20)
    data = await _sync(hass, await _restarted(hass, "A-TEST-L3", service), NOW)
    assert data.coverage_windows == COVERAGE_WINDOWS
    left_off = service.requests[-1][0]

    later = NOW + HOUR
    kraken.now = later
    sent = len(service.requests)
    coordinator = _coordinator(hass, "A-TEST-L3", service)
    data = await _sync(hass, coordinator, later)
    assert data.sync_mode is SyncMode.RESTART
    assert data.coverage_windows == 0
    assert len(service.requests) - sent == 3

    last = later + HOUR
    kraken.now = last
    sent = len(service.requests)
    data = await _sync(hass, coordinator, last)
    assert data.coverage_windows == 5
    assert service.requests[sent + 1][1] == left_off - DAY
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, coverage_target(last, None), _floor_hour(last)) == []
    await _assert_consistent(hass, energy_id, cost_id)


# ---------------------------------------------------------------- outages


def _alternating(start: datetime, _channel: int) -> str:
    return "1.5" if start.hour % 2 else "0.5"


@pytest.mark.parametrize("days", [3, 20])
@pytest.mark.asyncio
async def test_an_outage_is_caught_up_with_no_negative_step(
    recorder_mock, hass: HomeAssistant, days: int
) -> None:
    del recorder_mock
    start = NOW - days * DAY
    kraken = FakeKraken(
        start, history_start=NOW - 400 * DAY, future_rows=False, value=_alternating
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, f"A-TEST-O{days}", service)
    energy_id, cost_id = consumption_ids(property_key(f"A-TEST-O{days}", PROPERTY))
    await _sync(hass, coordinator, start)
    kraken.now = NOW

    data = await _sync(hass, coordinator, NOW)

    assert data.units_failed == data.units_degraded == 0
    assert data.coverage_windows == 0
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, NOW - INITIAL_BACKFILL, _floor_hour(NOW)) == []
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_20_day_outage_whose_oldest_week_fails_is_completed_later(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The newer weeks are imported at once, and the days before them later.

    The catch-up's third week fails: its two newer weeks are imported all
    the same, behind no stored row, and the unit owes its reconciliation.
    Two hours later that runs, and the failed week's older days, which
    waited an hour, are asked for one at a time. A day later the last of
    them, 14 days back by then, follows: no hour is left missing.
    """
    del recorder_mock
    start = NOW - 20 * DAY
    kraken = FakeKraken(
        start, history_start=NOW - 400 * DAY, future_rows=False, value=_alternating
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-O20F", service)
    energy_id, cost_id = consumption_ids(property_key("A-TEST-O20F", PROPERTY))
    await _sync(hass, coordinator, start)
    frontier = datetime.fromtimestamp((await _rows(hass, energy_id))[-1][0], UTC)
    kraken.now = NOW
    third = frontier.astimezone(NZ).date()
    service.fail = {third: MeridianUpstreamError(504, gateway=True)}

    data = await _sync(hass, coordinator, NOW)

    assert data.units_degraded == 1
    rows = await _rows(hass, energy_id)
    assert datetime.fromtimestamp(rows[-1][0], UTC) == _floor_hour(NOW) - HOUR
    assert _missing_hours(rows, frontier + HOUR, _floor_hour(NOW))
    await _assert_consistent(hass, energy_id, cost_id)

    service.fail = {}
    later = NOW + 2 * HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    # The reconciliation the unit owed ran, and it recovered.
    assert data.units_degraded == 0
    assert coordinator.unit_diagnostics()[0]["status"] == "ok"
    assert data.coverage_windows == COVERAGE_WINDOWS
    day_windows = service.requests[-COVERAGE_WINDOWS:]
    assert all(start_on == end_on for start_on, end_on, _ in day_windows)
    await _assert_consistent(hass, energy_id, cost_id)

    last = NOW + DAY
    kraken.now = last
    await _sync(hass, coordinator, last)
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, last - INITIAL_BACKFILL, _floor_hour(last)) == []
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_catch_up_ended_by_an_empty_week_over_stored_hours_leaves_it_to_ask(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    After 20 days without updates, Meridian returns nothing for the oldest week.

    That week holds the newest stored hours, so its empty answer ends the
    catch-up's walk, as an empty window over stored hours does: nothing
    shows Meridian read it in full. None of its dates counts as fetched, or
    as empty, though most are older than 14 days by then: the lane does not
    reach its target, and once Meridian returns the week the next update's
    backfill asks for its dates and fills them.
    """
    del recorder_mock
    start = NOW - 20 * DAY
    kraken = FakeKraken(
        start, history_start=NOW - 400 * DAY, future_rows=False, value=_alternating
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-O20E", service)
    energy_id, cost_id = consumption_ids(property_key("A-TEST-O20E", PROPERTY))
    await _sync(hass, coordinator, start)
    first = start.astimezone(NZ).date()
    week = {first + offset * DAY for offset in range(7)}
    kraken.now = NOW
    kraken.gaps = [(local_midnight(first), local_midnight(first + 7 * DAY))]

    data = await _sync(hass, coordinator, NOW)

    lane = coordinator._lanes[(property_key("A-TEST-O20E", PROPERTY), "CONSUMPTION")]
    assert lane.fetched.isdisjoint(week)
    assert lane.empty.isdisjoint(week)
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is False
    rows = await _rows(hass, energy_id)
    assert _missing_hours(
        rows, local_midnight(first + DAY), local_midnight(first + 6 * DAY)
    )

    kraken.gaps = []
    later = NOW + 2 * HOUR
    kraken.now = later
    sent = len(service.requests)
    data = await _sync(hass, coordinator, later)

    assert data.coverage_windows
    asked = {
        day
        for start_on, end_on, _ in service.requests[sent:]
        for day in week
        if start_on <= day <= end_on
    }
    assert asked >= {
        day for day in week if day < (later - 14 * DAY).astimezone(NZ).date()
    }
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, coverage_target(later, None), _floor_hour(later)) == []
    await _assert_consistent(hass, energy_id, cost_id)
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


# ------------------------------------------------------- the bill's start


@pytest.mark.asyncio
async def test_a_quarterly_billing_period_92_days_back_gets_complete_bill_totals(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The 90-day import leaves the period's first days out; the next update adds them.

    The bill totals need every hour from midnight at the period's start.
    The first update learns the period, and the next one's lane reaches
    back to that midnight with one window, so the totals are complete.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    start = TODAY - 92 * DAY
    billing = MeridianBillingPeriod(
        period_length="QUARTERLY",
        period_length_multiplier=1,
        is_fixed=True,
        start=start,
        end=TODAY + DAY,
        next_billing_date=TODAY + 2 * DAY,
        period_start_day=start.day,
    )
    coordinator = _coordinator(hass, "A-TEST-B1", service, billing=billing)

    data = await _sync(hass, coordinator, NOW)
    assert data.sync_mode is SyncMode.INITIAL
    (first,) = data.account_results
    assert first.usage_complete is False
    assert first.current_bill_usage is None

    later = NOW + HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)

    assert data.coverage_windows == 1
    (totals,) = data.account_results
    assert totals.usage_complete is True
    assert totals.cost_complete is True
    hours = (_floor_hour(later) - local_midnight(start)) / HOUR
    assert float(totals.current_bill_usage or 0) == pytest.approx(0.5 * hours)
    assert float(totals.current_bill_cost or 0) == pytest.approx(0.15 * hours)


@pytest.mark.asyncio
async def test_a_billing_period_that_has_ended_moves_no_target(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian still gives a period that started 120 days back and ended yesterday.

    It is no longer the current one, so its start does not move the lanes'
    target: the updates complete 90 days and ask for nothing older.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-B2", kraken, 20)
    start = TODAY - 120 * DAY
    billing = MeridianBillingPeriod(
        period_length="QUARTERLY",
        period_length_multiplier=1,
        is_fixed=True,
        start=start,
        end=TODAY - DAY,
        next_billing_date=TODAY,
        period_start_day=start.day,
    )
    coordinator = _coordinator(hass, "A-TEST-B2", service, billing=billing)

    windows: list[int] = []
    at = NOW
    for update in range(4):
        at = NOW + update * HOUR
        kraken.now = at
        data = await _sync(hass, coordinator, at)
        windows.append(data.coverage_windows)

    # None in the restart's first sync, which learns the period.
    assert windows == [0, COVERAGE_WINDOWS, 5, 0]
    oldest = (at - INITIAL_BACKFILL).astimezone(NZ).date()
    assert min(start_on for start_on, _end, _direction in service.requests) == oldest
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


def _quarter(start: date) -> MeridianBillingPeriod:
    """Return a current quarterly billing period that started on ``start``."""
    return MeridianBillingPeriod(
        period_length="QUARTERLY",
        period_length_multiplier=1,
        is_fixed=True,
        start=start,
        end=TODAY + DAY,
        next_billing_date=TODAY + 2 * DAY,
        period_start_day=start.day,
    )


@pytest.mark.asyncio
async def test_a_partial_setup_with_a_quarterly_period_completes_its_bill_totals(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Setup keeps 7 weeks, as its 8th fails; the billing period started 95 days back.

    Setup asks for no billing period, and the entry's first sync, which
    learns it, asks for no older history: so the entry's first coverage
    reaches back to the period's midnight already. The updates after it
    fill every date from there, the failed week's a day at a time. The bill
    totals are complete, and the lane reaches its target, only once no hour
    from that midnight on is missing.
    """
    del recorder_mock
    number = "A-TEST-PS"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    start = TODAY - 95 * DAY
    eighth = plan_windows(NOW - INITIAL_BACKFILL, NOW)[7]
    service.fail = {eighth.start_on: MeridianUpstreamError(503, gateway=False)}
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(number),))
    client.async_get_billing_period = AsyncMock(return_value=_quarter(start))
    client.async_get_measurements = AsyncMock(
        side_effect=service.async_get_measurements
    )
    setup = MeridianDataCoordinator(hass, client)
    with (
        patch.object(coordinator_module, "_utcnow", return_value=NOW),
        patch.object(coordinator_module.random, "uniform", return_value=3.5),
    ):
        data = await setup.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )
    await async_wait_recording_done(hass)
    assert data.units_degraded == 1
    client.async_get_billing_period.assert_not_awaited()
    service.fail = {}
    entry = MeridianDataCoordinator(
        hass,
        client,
        topology=setup.accounts,
        topology_at=NOW,
        reconciled_at=NOW,
        memory=setup.memory,
    )

    updates: list[tuple[int, bool, bool]] = []
    at = NOW
    for update in range(5):
        at = NOW + timedelta(minutes=5) + update * HOUR
        kraken.now = at
        data = await _sync(hass, entry, at)
        (totals,) = data.account_results
        updates.append(
            (
                data.coverage_windows,
                totals.usage_complete,
                entry.unit_diagnostics()[0]["reaches_target"],
            )
        )

    assert updates == [
        (0, False, False),
        (COVERAGE_WINDOWS, False, False),
        (COVERAGE_WINDOWS, False, False),
        (1, True, True),
        (0, True, True),
    ]
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, local_midnight(start), _floor_hour(at)) == []
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_billing_period_learned_after_the_backfill_finds_no_date_half_asked(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian's billing service fails at the restart's first sync, then answers.

    So the first coverage, not knowing the period, reaches back 90 days:
    to midnight on that date, which it asks for whole. The update after it,
    which knows the period started 95 days back, asks for the 5 dates
    before that one alone. No hour of the 90th date is left out, as it
    would be had the date been fetched from 90 days back to the hour only,
    and the bill totals are complete.
    """
    del recorder_mock
    number = "A-TEST-B4"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, number, kraken, 60)
    start = TODAY - 95 * DAY
    coordinator = _coordinator(hass, number, service, billing=_quarter(start))
    asked = coordinator.client.async_get_billing_period
    asked.side_effect = MeridianUpstreamError(503, gateway=False)
    data = await _sync(hass, coordinator, NOW)
    assert data.account_results[0].billing_period is None
    asked.side_effect = None

    later = NOW + HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    ninety = (later - INITIAL_BACKFILL).astimezone(NZ).date()
    assert min(start_on for start_on, _end, _ in service.requests) == ninety
    assert data.account_results[0].usage_complete is False

    last = later + HOUR
    kraken.now = last
    sent = len(service.requests)
    data = await _sync(hass, coordinator, last)

    assert data.coverage_windows == 1
    assert service.requests[sent + 1 :] == [(start, ninety - DAY, "CONSUMPTION")]
    (totals,) = data.account_results
    assert totals.usage_complete is True
    assert totals.cost_complete is True
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, local_midnight(start), _floor_hour(last)) == []
    await _assert_consistent(hass, energy_id, cost_id)
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


# --------------------------------------------------------- history start


@pytest.mark.asyncio
async def test_a_week_with_no_readings_does_not_stop_the_backfill(
    recorder_mock, hass: HomeAssistant
) -> None:
    """A meter that sent nothing for a week: the readings before it come too."""
    del recorder_mock
    gap_start = TODAY - 50 * DAY
    kraken = FakeKraken(
        NOW,
        history_start=NOW - 400 * DAY,
        future_rows=False,
        gaps=[(local_midnight(gap_start), local_midnight(gap_start + 7 * DAY))],
    )
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-G1", kraken, 30)
    coordinator = await _restarted(hass, "A-TEST-G1", service)

    await _sync(hass, coordinator, NOW)
    later = NOW + HOUR
    kraken.now = later
    await _sync(hass, coordinator, later)

    rows = await _rows(hass, energy_id)
    target = coverage_target(later, None)
    missing = _missing_hours(rows, target, _floor_hour(later))
    assert missing
    assert {hour.astimezone(NZ).date() for hour in missing} == {
        gap_start + offset * DAY for offset in range(7)
    }
    assert rows[0][0] == target.timestamp()
    await _assert_consistent(hass, energy_id, cost_id)
    unit = coordinator.unit_diagnostics()[0]
    assert unit["reaches_target"] is True
    assert unit["history_start_found"] is False


@pytest.mark.asyncio
async def test_two_empty_weeks_before_the_oldest_reading_stop_the_backfill(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The history starts 40 days back: the backfill stops after two empty weeks.

    The week holding the start comes back in part, the next two with
    nothing, and the backfill asks for nothing older, then or later. Its
    4 windows cost one request more, for the first date of the window
    holding the start with the date before; the empty weeks cost none.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 40 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-G2", kraken, 30)
    coordinator = await _restarted(hass, "A-TEST-G2", service)

    data = await _sync(hass, coordinator, NOW)

    assert data.coverage_windows == 4
    # The hourly update's own 1, then the backfill's.
    assert len(service.requests) == 1 + 4 + 1
    rows = await _rows(hass, energy_id)
    assert rows[0][0] == _ceil_hour(NOW - 40 * DAY).timestamp()
    unit = coordinator.unit_diagnostics()[0]
    assert unit["history_start_found"] is True
    assert unit["reaches_target"] is True
    later = NOW + HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    assert data.coverage_windows == 0
    await _assert_consistent(hass, energy_id, cost_id)


# -------------------------------------------------------------- failures


@pytest.mark.asyncio
async def test_a_failing_older_week_waits_and_the_backfill_goes_on_past_it(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A window of the backfill that fails fails nothing else.

    The update succeeds; the window's days wait an hour, and the next update
    asks for the weeks older than them. Once the wait has passed they are
    asked for a day at a time, and the history is complete.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-F1", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-F1", service)
    first_window = (NOW - 20 * DAY).astimezone(NZ).date()
    failing = first_window - 13 * DAY
    service.fail = {failing: MeridianUpstreamError(504, gateway=True)}

    data = await _sync(hass, coordinator, NOW)

    assert data.units_failed == 0
    assert data.coverage_windows == 2
    service.fail = {}
    soon = NOW + 30 * 60 * timedelta(seconds=1)
    kraken.now = soon
    data = await _sync(hass, coordinator, soon)
    assert data.coverage_windows == COVERAGE_WINDOWS
    assert all(start < failing for start, _end, _ in service.requests[-6:])

    for hours in (2, 3, 4):
        at = NOW + hours * HOUR
        kraken.now = at
        await _sync(hass, coordinator, at)
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, at - INITIAL_BACKFILL, _floor_hour(at)) == []
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_rate_limit_stops_the_backfill_and_fails_nothing(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-F2", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-F2", service)
    first_window = (NOW - 20 * DAY).astimezone(NZ).date()
    service.fail = {first_window - 13 * DAY: MeridianRateLimitError(3600)}

    data = await _sync(hass, coordinator, NOW)

    assert data.coverage_windows == 1
    assert data.units_failed == 0
    service.fail = {}
    later = NOW + HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    assert data.coverage_windows == COVERAGE_WINDOWS


# ----------------------------------------------------- daily reconciliation


@pytest.mark.asyncio
async def test_hours_missing_in_the_last_14_days_widen_the_daily_reconciliation(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian published a day late with no estimates for it first.

    The daily reconciliation reaches back to the first hour missing between
    stored hours, 5 days back, instead of 48 hours: one update fills it.
    """
    del recorder_mock
    gap = local_midnight(TODAY - 5 * DAY)
    kraken = FakeKraken(
        NOW,
        history_start=NOW - 400 * DAY,
        future_rows=False,
        gaps=[(gap, gap + DAY)],
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-E5", service)
    energy_id, cost_id = consumption_ids(property_key("A-TEST-E5", PROPERTY))
    await _sync(hass, coordinator, NOW)
    kraken.gaps = ()
    later = NOW + DAY
    kraken.now = later
    sent = len(service.requests)

    data = await _sync(hass, coordinator, later)

    assert data.sync_mode is SyncMode.TARGETED_RECONCILIATION
    oldest = min(start for start, _end, _ in service.requests[sent:])
    assert oldest <= (gap - timedelta(hours=6)).astimezone(NZ).date()
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, later - INITIAL_BACKFILL, _floor_hour(later)) == []
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_late_week_whose_start_ages_past_14_days_is_still_reconciled(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian publishes a week of readings two days late, from 12 to 5 days back.

    By then the week's first hours are more than 14 days back. The backfill
    asks for those, and the daily reconciliation for the rest of the week,
    from 14 days back: no hour of it is left missing for the weekly
    reconciliation to fetch, days later.
    """
    del recorder_mock
    gap = local_midnight(TODAY - 12 * DAY)
    kraken = FakeKraken(
        NOW,
        history_start=NOW - 400 * DAY,
        future_rows=False,
        gaps=[(gap, gap + 7 * DAY)],
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-E5L", service)
    energy_id, cost_id = consumption_ids(property_key("A-TEST-E5L", PROPERTY))
    await _sync(hass, coordinator, NOW)
    kraken.gaps = ()
    later = NOW + 2 * DAY
    kraken.now = later
    sent = len(service.requests)

    data = await _sync(hass, coordinator, later)

    assert data.sync_mode is SyncMode.TARGETED_RECONCILIATION
    # The 14 days now start during the week's first date.
    assert gap < later - 14 * DAY < gap + DAY
    oldest = min(start for start, _end, _ in service.requests[sent:])
    assert oldest <= gap.astimezone(NZ).date()
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, gap, _floor_hour(later)) == []
    await _assert_consistent(hass, energy_id, cost_id)


# -------------------------------------------------------------- the load


@pytest.mark.asyncio
async def test_each_mode_sends_as_many_requests_as_before_once_complete(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A load guard: a complete history adds no request to any mode.

    The initial import sends 13, the hourly update 1, the daily one 1, the
    weekly one 3 and a restart 3, as before lanes, and the hourly update
    after the restart 1. Only the update after the initial import asks for
    one window more, once: the hours of the date 90 days back before the
    import's cutoff, as the lane's target is that date's midnight.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-LOAD", service)
    counts: list[tuple[SyncMode, int, int]] = []
    for at, restart in (
        (NOW, False),
        (NOW + HOUR, False),
        (NOW + 2 * HOUR, False),
        (NOW + DAY, False),
        (NOW + 7 * DAY, False),
        (NOW + 7 * DAY + HOUR, True),
        (NOW + 7 * DAY + 2 * HOUR, False),
    ):
        if restart:
            coordinator = _coordinator(hass, "A-TEST-LOAD", service)
        kraken.now = at
        sent = len(service.requests)
        data = await _sync(hass, coordinator, at)
        counts.append(
            (data.sync_mode, len(service.requests) - sent, data.coverage_windows)
        )

    assert counts == [
        (SyncMode.INITIAL, 13, 0),
        (SyncMode.TIP, 1 + 1, 1),
        (SyncMode.TIP, 1, 0),
        (SyncMode.TARGETED_RECONCILIATION, 1, 0),
        (SyncMode.FULL_RECONCILIATION, 3, 0),
        (SyncMode.RESTART, 3, 0),
        (SyncMode.TIP, 1, 0),
    ]
    energy_id, _cost_id = consumption_ids(property_key("A-TEST-LOAD", PROPERTY))
    rows = await _rows(hass, energy_id)
    assert rows[0][0] == coverage_target(NOW + HOUR, None).timestamp()


@pytest.mark.asyncio
async def test_an_initial_import_leaves_the_older_days_to_the_next_update(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A load guard: no update walks 90 days and asks for coverage as well.

    A grid export found empty, whose readings come in an hourly update with
    their history behind them, queues its initial import for the next
    update; its coverage waits for that, rather than ask for weeks the
    initial import then walks again. With a quarterly billing period 120
    days back, the initial import's update asks for no coverage either:
    its lane asks for the 30 days older than its 90 at the update after,
    in 5 windows, and the history then reaches the period's start.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    never = FakeKraken(
        NOW, history_start=NOW - 400 * DAY, withdrawn_from=NOW - 400 * DAY
    )
    service = _Service(kraken, {"GENERATION": never})
    start = TODAY - 120 * DAY
    billing = MeridianBillingPeriod(
        period_length="QUARTERLY",
        period_length_multiplier=1,
        is_fixed=True,
        start=start,
        end=TODAY + DAY,
        next_billing_date=TODAY + 2 * DAY,
        period_start_day=start.day,
    )
    coordinator = _coordinator(
        hass, "A-TEST-LX", service, billing=billing, feed_in=True
    )
    counts: list[int] = []
    for update in range(5):
        at = NOW + update * HOUR
        kraken.now = never.now = at
        if update == 1:
            # The grid export's readings come in, with their history.
            service.krakens = {}
        sent = len(service.requests)
        await _sync(hass, coordinator, at)
        counts.append(
            sum(
                direction == "GENERATION"
                for _start, _end, direction in service.requests[sent:]
            )
        )

    assert counts == [13, 1, 13, 1 + 5, 1]
    energy_id, cost_id = generation_ids(property_key("A-TEST-LX", PROPERTY))
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, local_midnight(start), _floor_hour(at)) == []
    await _assert_consistent(hass, energy_id, cost_id)


# ------------------------------------------------------------- revision


@pytest.mark.asyncio
async def test_the_monthly_revision_corrects_an_estimate_older_than_14_days(
    recorder_mock, hass: HomeAssistant, revision_pass: None
) -> None:
    """
    An hour 30 days back that Meridian has revised since is written again.

    On the lane's day of the month its updates ask again for every stored
    day from 90 days to 14 days back, 6 windows at a time, and write only
    the hours that changed: here one, whose running sums follow on.
    """
    del recorder_mock, revision_pass
    number = "A-TEST-R1"
    key = property_key(number, PROPERTY)
    at = datetime(2026, 11, revision_day(f"{key}:CONSUMPTION"), 3, 30, tzinfo=UTC)
    revised = _floor_hour(at - 30 * DAY)
    values = {"revised": "0.5"}

    def value(start: datetime, _channel: int) -> str:
        return values["revised"] if start == revised else "0.5"

    seed_at = at - DAY
    kraken = FakeKraken(
        seed_at, history_start=at - 400 * DAY, future_rows=False, value=value
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, number, service)
    energy_id, cost_id = consumption_ids(key)
    data = await _sync(hass, coordinator, seed_at)
    assert data.revision_windows == 0
    values["revised"] = "2.5"
    kraken.now = at

    data = await _sync(hass, coordinator, at)

    assert data.revision_windows == COVERAGE_WINDOWS
    rows = await _rows(hass, energy_id)
    (row,) = [row for row in rows if row[0] == revised.timestamp()]
    assert row[1] == pytest.approx(2.5)
    await _assert_consistent(hass, energy_id, cost_id)
    # The history is complete all along: a pass still running changes nothing.
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True
    for hours, windows in ((1, 5), (2, 0)):
        later = at + hours * HOUR
        kraken.now = later
        data = await _sync(hass, coordinator, later)
        assert data.revision_windows == windows
        assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_the_monthly_revision_keeps_a_correction_on_hours_it_does_not_change(
    recorder_mock, hass: HomeAssistant, revision_pass: None
) -> None:
    """
    A complete history, with Adjust sum used on an hour 40 days back.

    Meridian has revised nothing: the pass asks again for every stored day,
    and its windows give the replay none of their hours, as none changed.
    So it writes nothing older than 14 days, and the correction is kept.
    """
    del recorder_mock, revision_pass
    number = "A-TEST-R4"
    key = property_key(number, PROPERTY)
    at = datetime(2026, 11, revision_day(f"{key}:CONSUMPTION"), 3, 30, tzinfo=UTC)
    seed_at = at - DAY
    kraken = FakeKraken(seed_at, history_start=at - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    coordinator = _coordinator(hass, number, service)
    energy_id, cost_id = consumption_ids(key)
    await _sync(hass, coordinator, seed_at)
    adjusted = _floor_hour(at - 40 * DAY)
    get_instance(hass).async_adjust_statistics(energy_id, adjusted, -5.0, "kWh")
    await async_wait_recording_done(hass)
    assert _bad_steps(await _rows(hass, energy_id)) == [adjusted.timestamp()]
    real = coordinator_module.async_replay
    replayed: list[datetime] = []

    async def replay(*args: Any, **kwargs: Any) -> tuple[int, int]:
        replayed.append(min(item.start for item in kwargs["measurements"]))
        return await real(*args, **kwargs)

    windows: list[int] = []
    with patch.object(coordinator_module, "async_replay", side_effect=replay):
        for hours in range(3):
            now = at + hours * HOUR
            kraken.now = now
            data = await _sync(hass, coordinator, now)
            windows.append(data.revision_windows)

    assert windows == [COVERAGE_WINDOWS, 5, 0]
    assert all(first >= at - 14 * DAY for first in replayed)
    assert _bad_steps(await _rows(hass, energy_id)) == [adjusted.timestamp()]
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.asyncio
async def test_the_monthly_revision_writes_a_cost_meridian_has_revised(
    recorder_mock, hass: HomeAssistant, revision_pass: None
) -> None:
    """
    Meridian has revised the cost of an hour 30 days back, not its usage.

    The pass compares each hour's cost as well as its usage with what is
    stored: that hour alone is written again, with its new cost, and the
    cost's running sums follow on.
    """
    del recorder_mock, revision_pass
    number = "A-TEST-R5"
    key = property_key(number, PROPERTY)
    at = datetime(2026, 11, revision_day(f"{key}:CONSUMPTION"), 3, 30, tzinfo=UTC)
    revised = _floor_hour(at - 30 * DAY)
    cents = {"revised": "15"}

    def cost(start: datetime, _channel: int) -> str:
        return cents["revised"] if start == revised else "15"

    seed_at = at - DAY
    kraken = FakeKraken(
        seed_at, history_start=at - 400 * DAY, future_rows=False, cost=cost
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, number, service)
    energy_id, cost_id = consumption_ids(key)
    await _sync(hass, coordinator, seed_at)
    energy_before = await _rows(hass, energy_id)
    cents["revised"] = "45"
    kraken.now = at

    data = await _sync(hass, coordinator, at)

    assert data.revision_windows == COVERAGE_WINDOWS
    costs = await _rows(hass, cost_id)
    (row,) = [row for row in costs if row[0] == revised.timestamp()]
    assert row[1] == pytest.approx(0.45)
    assert all(
        row[1] == pytest.approx(0.15)
        for row in costs
        if row[0] != revised.timestamp() and row[0] < (at - 14 * DAY).timestamp()
    )
    assert _bad_steps(costs) == []
    # The usage, which did not change, is as it was.
    energy = await _rows(hass, energy_id)
    assert energy[: len(energy_before)] == energy_before


@pytest.mark.asyncio
async def test_the_monthly_revision_checks_again_where_a_refusal_started_the_history(
    recorder_mock, hass: HomeAssistant, revision_pass: None
) -> None:
    """
    A start found by a refusal is asked about again by the monthly pass.

    The initial import finds the start of a 40-day history by Meridian
    refusing the week before it. On the lane's revision day the pass forgets
    that, as it forgets the empty days: its coverage asks for that week
    again, Meridian refuses it again, and the start is found again, with
    nothing older asked for after.
    """
    del recorder_mock, revision_pass
    number = "A-TEST-R2"
    key = property_key(number, PROPERTY)
    at = datetime(2026, 11, revision_day(f"{key}:CONSUMPTION"), 3, 30, tzinfo=UTC)
    seed_at = at - DAY
    start = _floor_hour(at - 40 * DAY)
    kraken = FakeKraken(
        seed_at,
        history_start=start,
        future_rows=False,
        pre_supply_error="KT-CT-4321",
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, number, service)
    energy_id, cost_id = consumption_ids(key)
    await _sync(hass, coordinator, seed_at)
    lane = coordinator._lanes[(key, "CONSUMPTION")]
    assert lane.refused_through is not None
    kraken.now = at
    sent = len(service.requests)

    data = await _sync(hass, coordinator, at)

    assert data.revision_windows
    # Found again, by a refusal of the week its coverage asked for this time.
    refused_through = lane.refused_through
    assert refused_through is not None
    assert refused_through < start.astimezone(NZ).date()
    asked = service.requests[sent:]
    assert any(end == refused_through for _start, end, _direction in asked)
    assert coordinator.unit_diagnostics()[0]["history_start_found"] is True
    later = at + HOUR
    kraken.now = later
    sent = len(service.requests)
    await _sync(hass, coordinator, later)
    assert all(
        start_on > refused_through for start_on, _end, _ in service.requests[sent:]
    )
    rows = await _rows(hass, energy_id)
    assert rows[0][0] >= start.timestamp()
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_the_monthly_revision_leaves_a_complete_history_reported_complete(
    recorder_mock, hass: HomeAssistant, revision_pass: None
) -> None:
    """
    A 50-day history with an hour Meridian does not have on 8 days 20 to 34 days back.

    Once complete, the lane reaches its target and its start is found. The
    monthly pass asks again for those 8 days, a window each, and for where
    the history starts, so its first update cannot get past them; nothing in
    the history changed, so what the lane reports does not change either,
    at any of its updates.
    """
    del recorder_mock, revision_pass
    number = "A-TEST-R3"
    key = property_key(number, PROPERTY)
    at = datetime(2026, 11, revision_day(f"{key}:CONSUMPTION"), 3, 30, tzinfo=UTC)
    hour = _floor_hour(at)
    gaps = [(hour - days * DAY, hour - days * DAY + HOUR) for days in range(20, 36, 2)]
    seed_at = at - DAY
    kraken = FakeKraken(
        seed_at, history_start=at - 50 * DAY, future_rows=False, gaps=gaps
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, number, service)
    energy_id, cost_id = consumption_ids(key)
    for now in (seed_at, seed_at + HOUR):
        kraken.now = now
        await _sync(hass, coordinator, now)
    (lane,) = coordinator.unit_diagnostics()
    assert (lane["reaches_target"], lane["history_start_found"]) == (True, True)

    windows: list[tuple[int, int]] = []
    for update in range(5):
        now = at + update * HOUR
        kraken.now = now
        data = await _sync(hass, coordinator, now)
        windows.append((data.coverage_windows, data.revision_windows))
        (lane,) = coordinator.unit_diagnostics()
        assert (lane["reaches_target"], lane["history_start_found"]) == (True, True)

    # The 8 days alone and the 2 weeks before the history's start, then the
    # stored days, a week at a time.
    assert windows == [(6, 0), (5, 1), (0, 6), (0, 4), (0, 0)]
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_refusal_before_the_oldest_reading_starts_the_history(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian refuses weeks before the supply started: the backfill stops there.

    The week holding the start comes back in part, the week before it is
    refused, and nothing older is asked for, then or later.
    """
    del recorder_mock
    kraken = FakeKraken(
        NOW,
        history_start=NOW - 40 * DAY,
        future_rows=False,
        pre_supply_error="KT-CT-4321",
    )
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-H1", kraken, 30)
    coordinator = await _restarted(hass, "A-TEST-H1", service)

    data = await _sync(hass, coordinator, NOW)

    assert data.coverage_windows == 3
    assert data.history_start is True
    unit = coordinator.unit_diagnostics()[0]
    assert unit["history_start_found"] is True
    assert unit["reaches_target"] is True
    later = NOW + HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    assert data.coverage_windows == 0
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(lambda: MeridianUpstreamError(503, gateway=False), id="503"),
        pytest.param(lambda: ValueError("synthetic unusable data"), id="data"),
    ],
)
@pytest.mark.asyncio
async def test_a_failing_window_stops_its_lane_alone(
    recorder_mock, hass: HomeAssistant, failure: Any
) -> None:
    """
    A failure other than a gateway timeout stops that lane, not its property's.

    The grid export's second window fails; its grid import goes on to its
    6 windows, and the update succeeds.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-F3", kraken, 20)
    await _seed_recent(hass, "A-TEST-F3", kraken, 20, direction="GENERATION")
    coordinator = await _restarted(hass, "A-TEST-F3", service, feed_in=True)
    first_window = (NOW - 20 * DAY).astimezone(NZ).date()
    failing = first_window - 13 * DAY
    real = service.async_get_measurements

    async def measurements(**kwargs: Any) -> Any:
        if kwargs["direction"] == "GENERATION" and kwargs["start_on"] == failing:
            service.requests.append((kwargs["start_on"], kwargs["end_on"], "x"))
            raise failure()
        return await real(**kwargs)

    coordinator.client.async_get_measurements = AsyncMock(side_effect=measurements)

    data = await _sync(hass, coordinator, NOW)

    assert data.units_failed == 0
    assert data.coverage_windows == COVERAGE_WINDOWS + 2
    lanes = coordinator.unit_diagnostics()
    assert [lane["reaches_target"] for lane in lanes] == [False, False]


@pytest.mark.parametrize("failing", ["coverage", "walk"])
@pytest.mark.asyncio
async def test_a_gateway_timeout_stops_its_propertys_backfill(
    recorder_mock, hass: HomeAssistant, failing: str
) -> None:
    """
    Meridian's gateway times out on the grid export: the grid import asks no more.

    The property cools down after a gateway timeout, so its other lane
    asks for no older history in that update either, whether the timeout
    came on the export's first backfill window, after the import's first,
    or on the export's own walk, before any backfill. The import's walk
    still goes through, and the next update asks for the backfill again.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-GT", kraken, 20)
    await _seed_recent(hass, "A-TEST-GT", kraken, 20, direction="GENERATION")
    coordinator = await _restarted(hass, "A-TEST-GT", service, feed_in=True)
    real = service.async_get_measurements
    # The hourly update's, after the restart's first sync.
    walk_requests = 1
    calls = {"GENERATION": 0}

    async def measurements(**kwargs: Any) -> Any:
        if kwargs["direction"] == "GENERATION":
            calls["GENERATION"] += 1
            nth = calls["GENERATION"]
            if nth == (walk_requests + 1 if failing == "coverage" else 1):
                service.requests.append((kwargs["start_on"], kwargs["end_on"], "x"))
                raise MeridianUpstreamError(504, gateway=True)
        return await real(**kwargs)

    coordinator.client.async_get_measurements = AsyncMock(side_effect=measurements)
    clock = {"now": 1000.0}

    with patch.object(
        coordinator_module, "monotonic", side_effect=lambda: clock["now"]
    ):
        data = await _sync(hass, coordinator, NOW)
        # The export's walk failed, or its backfill, which fails nothing.
        assert data.units_failed == (failing == "walk")
        imports = [
            request for request in service.requests if request[2] == "CONSUMPTION"
        ]
        if failing == "coverage":
            assert data.coverage_windows == 2
            timed_out = [request[2] for request in service.requests].index("x")
            assert "CONSUMPTION" not in [
                request[2] for request in service.requests[timed_out:]
            ]
            assert len(imports) == walk_requests + 1
        else:
            assert data.coverage_windows == 0
            assert len(imports) == walk_requests

        # An hour later the property's cooldown is over.
        clock["now"] += 3600
        later = NOW + HOUR
        kraken.now = later
        coordinator.client.async_get_measurements = AsyncMock(side_effect=real)
        data = await _sync(hass, coordinator, later)
    assert data.coverage_windows == 2 * COVERAGE_WINDOWS


@pytest.mark.asyncio
async def test_an_unconfirmed_backfill_window_is_asked_for_again(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Home Assistant did not confirm a window's import: it waits, then goes again.

    Its usage may have been saved without its cost, so it is asked for once
    its wait has passed even though its usage reads as stored.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-W1", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-W1", service)
    real = coordinator_module.async_replay
    calls = {"count": 0}

    async def replay(*args: Any, **kwargs: Any) -> tuple[int, int]:
        calls["count"] += 1
        if calls["count"] == 1:
            raise StatisticsWriteError("synthetic: not confirmed")
        return await real(*args, **kwargs)

    with patch.object(coordinator_module, "async_replay", side_effect=replay):
        data = await _sync(hass, coordinator, NOW)

    # The hourly update's walk changed nothing, so it wrote nothing: the
    # first replay was the first window's, which failed.
    assert data.coverage_windows == 1
    assert data.units_failed == 0
    lane = coordinator._lanes[(property_key("A-TEST-W1", PROPERTY), "CONSUMPTION")]
    assert lane.bad
    assert len(lane.bad) == 7
    later = NOW + 2 * HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    # Its days, once their hour has passed, one at a time.
    assert data.coverage_windows == COVERAGE_WINDOWS
    assert all(start == end for start, end, _ in service.requests[-6:])
    assert len(lane.bad) == 1
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_an_unconfirmed_date_left_out_again_waits_again(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A window's import was saved but not confirmed, and a date of it comes back short.

    The window's usage is stored, so none of its dates misses an hour, but
    the recorder may have lost the shift of the rows after them. Asked for
    again alone once their wait has passed, the others are replayed in
    full and confirmed. Meridian now leaves out one stored hour of one
    date, so the stored-aware rules leave out that date's rows before it:
    its replay did not run in full, and it stays unconfirmed, waits again,
    and keeps the lane from reaching its target.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-W2", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-W2", service)
    lane_key = (property_key("A-TEST-W2", PROPERTY), "CONSUMPTION")
    real = coordinator_module.async_replay
    calls = {"count": 0}

    async def replay(*args: Any, **kwargs: Any) -> tuple[int, int]:
        calls["count"] += 1
        written = await real(*args, **kwargs)
        if calls["count"] == 1:
            raise StatisticsWriteError("synthetic: not confirmed")
        return written

    with patch.object(coordinator_module, "async_replay", side_effect=replay):
        data = await _sync(hass, coordinator, NOW)

    assert data.coverage_windows == 1
    lane = coordinator._lanes[lane_key]
    week = set(lane.unsaved)
    assert len(week) == 7
    newest = max(week)
    short = newest - DAY
    hole = local_midnight(short) + 12 * HOUR
    kraken.gaps = ((hole, hole + HOUR),)
    later = NOW + 2 * HOUR
    kraken.now = later

    data = await _sync(hass, coordinator, later)

    assert data.coverage_windows == COVERAGE_WINDOWS
    assert short in lane.unsaved
    assert short in lane.bad
    assert lane.tries[short] == 2
    assert newest not in lane.unsaved
    assert newest not in lane.bad
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is False
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_deep_walk_over_an_unconfirmed_date_confirms_it(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    After a reload, a date's coverage import and an older deep replay wait.

    The entry kept both: a date 30 days back whose import Home Assistant
    did not confirm, and the oldest hour of a deep replay 32 days back that
    was not confirmed either. The update's walk reaches back to that hour
    and replays every date it fetched, that one included, which confirms
    it: the lane no longer keeps it as unconfirmed, nor does the entry for
    the next reload, and once the coverage has filled the older weeks the
    lane reaches its target.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-DW", kraken, 31)
    lane_key = (property_key("A-TEST-DW", PROPERTY), "CONSUMPTION")
    day = TODAY - 30 * DAY
    unsaved = {lane_key: {day: NOW}}
    unconfirmed = {lane_key: local_midnight(day - 2 * DAY)}
    coordinator = _coordinator(
        hass, "A-TEST-DW", service, unsaved=unsaved, unconfirmed=unconfirmed
    )

    data = await _sync(hass, coordinator, NOW)

    assert data.units_failed == 0
    assert unconfirmed == {}
    lane = coordinator._lanes[lane_key]
    assert day in lane.fetched
    assert day not in lane.unsaved
    assert day not in lane.tries
    assert unsaved == {}
    for hours in (1, 2):
        later = NOW + hours * HOUR
        kraken.now = later
        data = await _sync(hass, coordinator, later)
        assert data.coverage_windows
    assert unsaved == {}
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_a_rejected_session_in_the_backfill_fails_the_update(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-A1", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-A1", service)
    first_window = (NOW - 20 * DAY).astimezone(NZ).date()
    service.fail = {first_window - 6 * DAY: MeridianAuthenticationError("synthetic")}

    with pytest.raises(ConfigEntryAuthFailed):
        await _sync(hass, coordinator, NOW)


@pytest.mark.asyncio
async def test_the_time_budget_stops_the_backfill_and_fails_nothing(
    recorder_mock, hass: HomeAssistant
) -> None:
    """At 80 s a request, the hourly update's walk and one window fit its 4 minutes."""
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-T1", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-T1", service)
    clock = {"now": 1000.0}

    def monotonic() -> float:
        return clock["now"]

    real = service.async_get_measurements

    async def slow(**kwargs: Any) -> Any:
        clock["now"] += 80.0
        return await real(**kwargs)

    coordinator.client.async_get_measurements = AsyncMock(side_effect=slow)
    with patch.object(coordinator_module, "monotonic", side_effect=monotonic):
        data = await _sync(hass, coordinator, NOW)

    assert data.units_failed == 0
    assert data.coverage_windows == 1
    assert len(service.requests) == 1 + 1


@pytest.mark.asyncio
async def test_a_short_history_costs_no_backfill_but_once_after_a_restart(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A history of 20 days: the initial import finds its start, and nothing more.

    Its two empty weeks mark where the history starts, so the updates after
    it ask for nothing older. A restart forgets that, and the update after
    its first sync asks for the two weeks before the oldest stored day once
    more: three windows here, as the first starts on that day, whose hours
    before the history's start are missing, and one request more, for the
    first date of that window with the date before. The two empty weeks
    cost none: they are what the weeks before a history's start return.
    The restart's first sync sends its 3 requests alone, and the hourly
    updates 1.

    The restart's INFO lines say that the history may only be finding where
    it starts, and then that it is complete back to there: not that it
    was missing, nor that it now reaches back 90 days.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 20 * DAY, future_rows=False)
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-S1", service)

    windows: list[int] = []
    requests: list[int] = []
    for at, restart in (
        (NOW, False),
        (NOW + HOUR, False),
        (NOW + 2 * HOUR, True),
        (NOW + 3 * HOUR, False),
        (NOW + 4 * HOUR, False),
    ):
        if restart:
            coordinator = _coordinator(hass, "A-TEST-S1", service)
            caplog.clear()
            caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
        kraken.now = at
        sent = len(service.requests)
        data = await _sync(hass, coordinator, at)
        windows.append(data.coverage_windows)
        requests.append(len(service.requests) - sent)

    assert windows == [0, 0, 0, 3, 0]
    assert requests == [5, 1, 3, 1 + 3 + 1, 1]
    assert coordinator.unit_diagnostics()[0]["history_start_found"] is True
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO
        and record.name.startswith("custom_components.meridian_energy")
    ]
    started = [message for message in messages if "older Meridian" in message]
    complete = [message for message in messages if "is now complete" in message]
    assert len(started) == len(complete) == 1
    assert "or where it starts is not known yet" in started[0]
    assert "to where the history starts" in complete[0]
    assert not [message for message in messages if "reaches back 90 days" in message]


# ------------------------------------------------- failures that come back


def _failing_on(
    service: _Service, failing: date, error: BaseException, hits: list[int]
) -> Any:
    """Return a measurements call that fails every window holding ``failing``."""
    real = service.async_get_measurements
    update = {"n": 0}

    async def measurements(**kwargs: Any) -> Any:
        if kwargs["start_on"] <= failing <= kwargs["end_on"]:
            hits.append(update["n"])
            service.requests.append((kwargs["start_on"], kwargs["end_on"], "x"))
            raise error
        return await real(**kwargs)

    measurements.update = update  # type: ignore[attr-defined]
    return measurements


@pytest.mark.asyncio
async def test_a_revision_window_that_keeps_failing_waits_and_the_pass_goes_on(
    recorder_mock, hass: HomeAssistant, revision_pass: None
) -> None:
    """
    A stored day 30 days back fails at every request: the pass goes past it.

    Its window fails at the pass's first update; the day is then asked for
    alone, each time its wait has passed, 1 h, then 2 h, 4 h and 8 h, as it
    keeps failing while the other days come back. The pass revises every
    other stored day back to the target, and then ends. The day's hours are
    all stored, so the history still reaches the target.
    """
    del recorder_mock, revision_pass
    number = "A-TEST-RB"
    key = property_key(number, PROPERTY)
    at = datetime(2026, 11, revision_day(f"{key}:CONSUMPTION"), 3, 30, tzinfo=UTC)
    seed_at = at - DAY
    kraken = FakeKraken(seed_at, history_start=at - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    coordinator = _coordinator(hass, number, service)
    await _sync(hass, coordinator, seed_at)
    failing = (at - 30 * DAY).astimezone(NZ).date()
    hits: list[int] = []
    measurements = _failing_on(
        service, failing, MeridianUpstreamError(500, gateway=False), hits
    )
    coordinator.client.async_get_measurements = AsyncMock(side_effect=measurements)
    lane = coordinator._lanes[(key, "CONSUMPTION")]

    for update in range(20):
        measurements.update["n"] = update
        now = at + update * HOUR
        kraken.now = now
        data = await _sync(hass, coordinator, now)
        assert data.units_failed == 0

    assert hits == [0, 1, 3, 7, 15]
    assert set(lane.bad) == {failing}
    assert lane.tries[failing] == 5
    assert not lane.revising
    target = coverage_target(now, None).astimezone(NZ).date()
    asked = lane.revised | lane.fetched
    assert min(asked) <= target + DAY
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_a_coverage_replay_whose_shift_was_lost_is_healed_when_tried_again(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The recorder saved a window's rows but not the shift of the newer rows.

    Home Assistant did not confirm the import, so the window's days wait
    and are asked for again a day at a time: each is replayed in full,
    which writes the shift again, and every running sum follows on.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-SEAM", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-SEAM", service)
    real = statistics_module._queue_writes
    calls = {"count": 0}

    def queue(hass_: HomeAssistant, replays: list[Any]) -> None:
        calls["count"] += 1
        if calls["count"] != 1:
            real(hass_, replays)
            return
        # The rows are saved; the shift of the rows after them is lost.
        for replay in replays:
            async_add_external_statistics(hass_, replay.metadata, replay.rows)

    with patch.object(statistics_module, "_queue_writes", side_effect=queue):
        data = await _sync(hass, coordinator, NOW)

    assert data.coverage_windows == 1
    assert _bad_steps(await _rows(hass, energy_id))
    lane = coordinator._lanes[(property_key("A-TEST-SEAM", PROPERTY), "CONSUMPTION")]
    assert len(lane.unsaved) == 7
    for hours in (2, 3, 4):
        at = NOW + hours * HOUR
        kraken.now = at
        await _sync(hass, coordinator, at)
    await _assert_consistent(hass, energy_id, cost_id)
    assert not lane.bad
    assert not lane.unsaved
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_a_coverage_replay_whose_shift_was_lost_is_healed_after_a_reload(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The same lost shift, then a reload of the integration before the retry.

    The entry keeps the dates whose coverage import was not confirmed, with
    when each is tried again, across its reloads, as it keeps its deep
    replays' oldest hours: the new coordinator replays them in full, so the
    seam is healed rather than left for good, which no stored state shows.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-SEAM2", kraken, 20)
    unsaved: dict[tuple[str, str], dict[date, datetime]] = {}
    coordinator = await _restarted(hass, "A-TEST-SEAM2", service, unsaved=unsaved)
    real = statistics_module._queue_writes
    calls = {"count": 0}

    def queue(hass_: HomeAssistant, replays: list[Any]) -> None:
        calls["count"] += 1
        if calls["count"] != 2:
            real(hass_, replays)
            return
        for replay in replays:
            async_add_external_statistics(hass_, replay.metadata, replay.rows)

    with patch.object(statistics_module, "_queue_writes", side_effect=queue):
        await _sync(hass, coordinator, NOW)

    assert _bad_steps(await _rows(hass, energy_id))
    key = (property_key("A-TEST-SEAM2", PROPERTY), "CONSUMPTION")
    assert len(unsaved[key]) == 7
    assert set(unsaved[key].values()) == {NOW + HOUR}

    # A reload: a new coordinator, with the entry's mapping. Its first sync
    # asks for no older history, and keeps the mapping as it is.
    coordinator = _coordinator(hass, "A-TEST-SEAM2", service, unsaved=unsaved)
    for hours in (2, 3, 4, 5):
        at = NOW + hours * HOUR
        kraken.now = at
        await _sync(hass, coordinator, at)
        if hours == 2:
            assert len(unsaved[key]) == 7

    await _assert_consistent(hass, energy_id, cost_id)
    assert unsaved == {}
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


def _losing_the_second_shift() -> Any:
    """Patch the recorder writes so the second replay saves its rows, not its shift."""
    real = statistics_module._queue_writes
    calls = {"count": 0}

    def queue(hass_: HomeAssistant, replays: list[Any]) -> None:
        calls["count"] += 1
        if calls["count"] != 2:
            real(hass_, replays)
            return
        for replay in replays:
            async_add_external_statistics(hass_, replay.metadata, replay.rows)

    return patch.object(statistics_module, "_queue_writes", side_effect=queue)


@pytest.mark.asyncio
async def test_a_lost_shift_whose_date_comes_back_empty_waits_to_be_replayed(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The same lost shift; tried again, its newest date comes back with no readings.

    That date's usage is stored, so no hour of it reads as missing, but no
    replay wrote it again: as for a walk's empty window over stored hours,
    the empty answer is not taken as the date done. It stays unconfirmed
    and waits 2 h, the lane does not reach its target meanwhile, and once
    Meridian returns the date again it is replayed in full, and every
    running sum follows on.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-SEAM3", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-SEAM3", service)
    with _losing_the_second_shift():
        await _sync(hass, coordinator, NOW)
    assert _bad_steps(await _rows(hass, energy_id))
    lane = coordinator._lanes[(property_key("A-TEST-SEAM3", PROPERTY), "CONSUMPTION")]
    newest = max(lane.unsaved)
    kraken.gaps = ((local_midnight(newest), local_midnight(newest + DAY)),)
    at = NOW + 2 * HOUR
    kraken.now = at

    await _sync(hass, coordinator, at)

    assert newest in lane.unsaved
    assert lane.tries[newest] == 2
    assert newest not in lane.fetched
    assert newest not in lane.empty
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is False
    kraken.gaps = ()
    for hours in (3, 4, 5):
        at = NOW + hours * HOUR
        kraken.now = at
        await _sync(hass, coordinator, at)
    await _assert_consistent(hass, energy_id, cost_id)
    assert not lane.unsaved
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_a_deep_walk_finding_an_unconfirmed_week_empty_leaves_it_unconfirmed(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    After a reload, a deep walk reaches back across the week whose shift was lost.

    Meridian answers that week, and the day before it, with no readings, so
    the walk stops at it and its replay starts after it: none of its dates
    was written again, so none counts as fetched, empty or confirmed. They
    wait, then are asked for alone, and once Meridian returns them they
    are replayed in full.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-SEAM4", kraken, 20)
    unsaved: dict[tuple[str, str], dict[date, datetime]] = {}
    unconfirmed: dict[tuple[str, str], datetime] = {}
    coordinator = await _restarted(
        hass, "A-TEST-SEAM4", service, unsaved=unsaved, unconfirmed=unconfirmed
    )
    with _losing_the_second_shift():
        await _sync(hass, coordinator, NOW)
    key = (property_key("A-TEST-SEAM4", PROPERTY), "CONSUMPTION")
    week = sorted(unsaved[key])
    assert len(week) == 7
    # An older deep replay was not confirmed either: the walks reach back to it.
    unconfirmed[key] = local_midnight(week[0] - 3 * DAY)
    kraken.gaps = ((local_midnight(week[0] - DAY), local_midnight(week[-1] + DAY)),)
    coordinator = _coordinator(
        hass, "A-TEST-SEAM4", service, unsaved=unsaved, unconfirmed=unconfirmed
    )
    at = NOW + 2 * HOUR
    kraken.now = at

    data = await _sync(hass, coordinator, at)

    assert data.units_failed == 0
    lane = coordinator._lanes[key]
    assert set(week) <= lane.unsaved
    assert lane.fetched.isdisjoint(week)
    assert lane.empty.isdisjoint(week)
    assert set(week) <= set(unsaved[key])
    kraken.gaps = ()
    for hours in (5, 6, 7):
        at = NOW + hours * HOUR
        kraken.now = at
        await _sync(hass, coordinator, at)
    await _assert_consistent(hass, energy_id, cost_id)
    assert unsaved == {}
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_a_lost_shift_is_healed_after_a_reload_cancelled_its_confirmation(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The same lost shift, but a reload cancels the update while it waits to confirm.

    Unloading an entry cancels its update, here while the window's replay
    waits for a busy recorder to confirm it, its writes already queued. The
    window's dates count as not confirmed all the same, so the entry keeps
    them, and the reloaded entry replays them in full.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-SEAM5", kraken, 20)
    unsaved: dict[tuple[str, str], dict[date, datetime]] = {}
    coordinator = await _restarted(hass, "A-TEST-SEAM5", service, unsaved=unsaved)
    real_confirm = statistics_module._async_confirm_replay
    calls = {"count": 0}
    waiting = asyncio.Event()

    async def confirm(hass_: HomeAssistant, replays: list[Any]) -> None:
        calls["count"] += 1
        if calls["count"] != 2:
            await real_confirm(hass_, replays)
            return
        waiting.set()
        await asyncio.Event().wait()

    with (
        _losing_the_second_shift(),
        patch.object(statistics_module, "_async_confirm_replay", side_effect=confirm),
        patch.object(coordinator_module, "_utcnow", return_value=NOW),
        patch.object(coordinator_module.random, "uniform", return_value=3.5),
    ):
        task = hass.async_create_task(
            coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        )
        await waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await async_wait_recording_done(hass)

    assert _bad_steps(await _rows(hass, energy_id))
    key = (property_key("A-TEST-SEAM5", PROPERTY), "CONSUMPTION")
    assert len(unsaved[key]) == 7
    coordinator = _coordinator(hass, "A-TEST-SEAM5", service, unsaved=unsaved)
    for hours in (2, 3, 4, 5):
        at = NOW + hours * HOUR
        kraken.now = at
        await _sync(hass, coordinator, at)

    await _assert_consistent(hass, energy_id, cost_id)
    assert unsaved == {}
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_a_lost_shift_older_than_90_days_is_healed_after_a_reload(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A 120-day billing period; the backfill's week before the 90 days loses its shift.

    The entry keeps the week's dates across a reload. The reloaded entry's
    first sync cannot get the billing period, so its next update does not
    know the period reaches back so far: its lane's target reaches back to
    those dates all the same, as to any date whose import was not
    confirmed, so it does not forget them. They are replayed in full, the
    seam is healed, and the bill totals are complete.
    """
    del recorder_mock
    number = "A-TEST-SEAM6"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    start = TODAY - 120 * DAY
    energy_id, cost_id = await _seed_recent(
        hass, number, kraken, 0, since=local_midnight(start + 7 * DAY)
    )
    billing = replace(_quarter(start), period_length_multiplier=2)
    unsaved: dict[tuple[str, str], dict[date, datetime]] = {}
    coordinator = await _restarted(
        hass, number, service, billing=billing, unsaved=unsaved
    )
    real = statistics_module._queue_writes

    def queue(hass_: HomeAssistant, replays: list[Any]) -> None:
        if min(replay.first for replay in replays) >= NOW - 100 * DAY:
            real(hass_, replays)
            return
        # The rows are saved; the shift of the newer rows is lost.
        for replay in replays:
            async_add_external_statistics(hass_, replay.metadata, replay.rows)

    with patch.object(statistics_module, "_queue_writes", side_effect=queue):
        data = await _sync(hass, coordinator, NOW)
    assert data.coverage_windows == 1
    key = (property_key(number, PROPERTY), "CONSUMPTION")
    week = sorted(unsaved[key])
    assert week == [start + offset * DAY for offset in range(7)]
    assert _bad_steps(await _rows(hass, energy_id))

    # A reload, whose first sync Meridian's billing service fails.
    coordinator = _coordinator(hass, number, service, billing=billing, unsaved=unsaved)
    asked = coordinator.client.async_get_billing_period
    asked.side_effect = MeridianUpstreamError(503, gateway=False)
    at = NOW + 2 * HOUR
    kraken.now = at
    await _sync(hass, coordinator, at)
    asked.side_effect = None
    assert sorted(unsaved[key]) == week

    at = NOW + 3 * HOUR
    kraken.now = at
    sent = len(service.requests)
    data = await _sync(hass, coordinator, at)
    # Billing not known when it planned: its lane still asked for the week.
    assert asked.await_count == 2
    assert {start_on for start_on, _end, _ in service.requests[sent:]} >= set(week[1:])
    for hours in (4, 5):
        at = NOW + hours * HOUR
        kraken.now = at
        data = await _sync(hass, coordinator, at)

    await _assert_consistent(hass, energy_id, cost_id)
    assert unsaved == {}
    (totals,) = data.account_results
    assert totals.usage_complete is True
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_days_whose_rows_an_import_left_out_are_asked_for_again(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian no longer returns one stored hour 35 days back.

    A restart's walk reaches back 90 days, and its import leaves out every
    row older than that hour. Those days are not taken as fetched: the
    coverage of the next update asks for the missing ones, newest first
    (the update that walked 90 days asks for no more), and the history is
    complete, with the hour that was stored kept as it was.
    """
    del recorder_mock
    old = NOW - 30 * DAY
    kraken = FakeKraken(old, history_start=NOW - 400 * DAY, future_rows=False)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-CUT", kraken, 10)
    hole = _floor_hour(old - 5 * DAY)
    kraken.gaps = [(hole, hole + HOUR)]
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-CUT", service)

    windows: list[int] = []
    for update in range(3):
        at = NOW + update * HOUR
        kraken.now = at
        data = await _sync(hass, coordinator, at)
        windows.append(data.coverage_windows)

    # The 50 days the walk's import left out, asked for at the next update,
    # then the rest.
    assert windows == [0, COVERAGE_WINDOWS, 2]
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, at - INITIAL_BACKFILL, _floor_hour(at)) == []
    await _assert_consistent(hass, energy_id, cost_id)
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


@pytest.mark.asyncio
async def test_days_a_catch_up_left_out_are_completed_by_the_same_update(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    After 20 days without updates, the catch-up's import leaves out its oldest days.

    The cost was stored three days less far than the usage, so the walk
    reaches back 2 days before the newest stored hour, and Meridian no
    longer returns one stored hour of the newest stored day: the import
    leaves out every row up to it, among them an hour of the day before
    that was never stored. The walk fetched that day, but its import did
    not keep it, so the same update's coverage asks for it, and the hour
    is stored by the end of the update.
    """
    del recorder_mock
    start = NOW - 20 * DAY
    day = start.astimezone(NZ).date()
    missing = local_midnight(day - DAY) + 10 * HOUR
    withdrawn = local_midnight(day) + 3 * HOUR
    costed = local_midnight(day - 3 * DAY) + 12 * HOUR
    kraken = FakeKraken(
        start,
        history_start=NOW - 400 * DAY,
        future_rows=False,
        gaps=[(missing, missing + HOUR)],
        cost=lambda hour, _channel: None if hour > costed else "15",
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-CUT2", service)
    energy_id, cost_id = consumption_ids(property_key("A-TEST-CUT2", PROPERTY))
    await _sync(hass, coordinator, start)
    assert missing.timestamp() not in {row[0] for row in await _rows(hass, energy_id)}
    kraken.now = NOW
    kraken.gaps = ((withdrawn, withdrawn + HOUR),)
    kraken.cost = lambda _hour, _channel: "15"

    data = await _sync(hass, coordinator, NOW)

    assert data.coverage_windows == 1
    assert service.requests[-1][:2] == (day - DAY, day - DAY)
    rows = await _rows(hass, energy_id)
    assert missing.timestamp() in {row[0] for row in rows}
    assert _missing_hours(rows, NOW - INITIAL_BACKFILL, _floor_hour(NOW)) == []
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.parametrize("withdrawn_hour", [10, 23])
@pytest.mark.asyncio
async def test_a_hole_meridian_no_longer_returns_waits_between_tries(
    recorder_mock, hass: HomeAssistant, withdrawn_hour: int
) -> None:
    """
    A date 40 days back misses its 03:00, and Meridian no longer returns its 10:00.

    The import of its window leaves out every row up to the stored 10:00, so
    the missing hour stays missing at every try: the date waits after each,
    1 h, then 2 h, rather than be asked for at every update, and its lane
    does not claim to reach its target. So it does when Meridian no longer
    returns the date's 23:00, its newest stored hour: nothing of the window
    is imported then. As the date's last hour did not come back, each try
    also asks for the date with the next one, which shows it was not read
    without its last hour.
    """
    del recorder_mock
    day = TODAY - 40 * DAY
    missing = local_midnight(day) + 3 * HOUR
    withdrawn = local_midnight(day) + withdrawn_hour * HOUR
    kraken = FakeKraken(
        NOW,
        history_start=NOW - 400 * DAY,
        future_rows=False,
        gaps=[(missing, missing + HOUR)],
    )
    service = _Service(kraken)
    energy_id, cost_id = consumption_ids(property_key("A-TEST-HOLE", PROPERTY))
    await _sync(hass, _coordinator(hass, "A-TEST-HOLE", service), NOW)
    kraken.gaps = ((withdrawn, withdrawn + HOUR),)
    # A restart: the new coordinator's lane knows nothing of the date, and
    # its first sync asks for none of it.
    coordinator = _coordinator(hass, "A-TEST-HOLE", service)

    asked: list[int] = []
    for update in range(1, 6):
        at = NOW + update * HOUR
        kraken.now = at
        sent = len(service.requests)
        await _sync(hass, coordinator, at)
        asked.append(
            sum(
                start <= day <= end
                for start, end, _direction in service.requests[sent:]
            )
        )

    tries = [0, 1, 1, 0, 1]
    assert asked == ([2 * n for n in tries] if withdrawn_hour == 23 else tries)
    rows = await _rows(hass, energy_id)
    assert missing.timestamp() not in {row[0] for row in rows}
    assert withdrawn.timestamp() in {row[0] for row in rows}
    await _assert_consistent(hass, energy_id, cost_id)
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is False


@pytest.mark.asyncio
async def test_after_a_restart_a_refused_window_holding_the_first_reading_finds_it(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The history starts at 21:00 on its first date, and Meridian refuses dates before.

    After a restart, the window ending on that date starts before the
    supply and is refused: its dates are asked for alone, and the refusal
    of the date before the first marks the start. Once a failure has made
    the window's dates wait, the dates before the start found later no
    longer wait either, and the lane reaches its target.
    """
    del recorder_mock
    start = _floor_hour(NOW - 40 * DAY) + 5 * HOUR
    first = start.astimezone(NZ).date()
    kraken = FakeKraken(
        NOW,
        history_start=start,
        future_rows=False,
        pre_supply_error="KT-CT-4321",
        pre_supply_start_on=True,
    )
    service = _Service(kraken)
    await _sync(hass, _coordinator(hass, "A-TEST-ST", service), NOW)

    kraken.now = NOW + HOUR
    coordinator = await _restarted(hass, "A-TEST-ST", service)
    later = NOW + 2 * HOUR
    kraken.now = later
    caplog.set_level(logging.INFO, logger=coordinator_module.__name__)
    caplog.clear()
    data = await _sync(hass, coordinator, later)

    assert data.units_failed == 0
    assert data.coverage_windows == 1
    lane = coordinator._lanes[(property_key("A-TEST-ST", PROPERTY), "CONSUMPTION")]
    assert not lane.bad
    assert lane.refused_through == first - DAY
    unit = coordinator.unit_diagnostics()[0]
    assert unit["history_start_found"] is True
    assert unit["reaches_target"] is True
    assert not [
        record for record in caplog.records if "older usage" in record.getMessage()
    ]

    # Another restart: this time the window fails once, and its dates wait.
    kraken.now = NOW + 3 * HOUR
    coordinator = await _restarted(hass, "A-TEST-ST", service)
    service.fail = {first - 6 * DAY: MeridianUpstreamError(503, gateway=False)}
    at = NOW + 4 * HOUR
    kraken.now = at
    await _sync(hass, coordinator, at)
    lane = coordinator._lanes[(property_key("A-TEST-ST", PROPERTY), "CONSUMPTION")]
    assert len(lane.bad) == 7
    service.fail = {}
    at = NOW + 6 * HOUR
    kraken.now = at
    await _sync(hass, coordinator, at)
    assert not lane.bad
    unit = coordinator.unit_diagnostics()[0]
    assert unit["history_start_found"] is True
    assert unit["reaches_target"] is True


@pytest.mark.asyncio
async def test_a_date_refused_for_another_reason_does_not_start_the_history(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian refuses every request holding one older date, with a coded error.

    The week holding it is asked for by its dates, and the date before the
    refused one has readings: the refusal is of another kind, and the week
    fails. Asked for alone once its wait has passed, the refused date is no
    start either: the date before it is asked for too, and its readings
    show the history goes on. Every other date is imported; that one stays
    missing and waits, and no start of the history is found.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-RD", kraken, 20)
    refused = (NOW - 20 * DAY).astimezone(NZ).date() - 3 * DAY
    hits: list[int] = []
    measurements = _failing_on(
        service, refused, MeridianGraphQLError("measurements", ("KT-CT-9999",)), hits
    )
    coordinator = _coordinator(hass, "A-TEST-RD", service)
    coordinator.client.async_get_measurements = AsyncMock(side_effect=measurements)
    lane_key = (property_key("A-TEST-RD", PROPERTY), "CONSUMPTION")

    for update in range(9):
        measurements.update["n"] = update
        at = NOW + update * HOUR
        kraken.now = at
        data = await _sync(hass, coordinator, at)
        assert data.units_failed == 0
        assert coordinator._lanes[lane_key].refused_through is None

    # After the restart's first sync, the week and then its date alone, and
    # later the date alone each time its wait has passed: 1 h, 2 h, then 4 h.
    assert hits == [1, 1, 2, 4, 8]
    rows = await _rows(hass, energy_id)
    missing = _missing_hours(rows, at - INITIAL_BACKFILL, _floor_hour(at))
    assert {hour.astimezone(NZ).date() for hour in missing} == {refused}
    assert set(coordinator._lanes[lane_key].bad) == {refused}
    unit = coordinator.unit_diagnostics()[0]
    assert unit["history_start_found"] is False
    assert unit["reaches_target"] is False
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_walk_failed_at_a_refused_dates_check_makes_that_week_wait(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    An initial import's third week is refused, and so is its oldest date alone.

    The date before that one has readings, so the refusal was of another
    kind and the walk fails there. The week it failed at is not imported:
    its dates older than 14 days wait, then are asked for one at a time.
    The date before it came back, and it does not wait.
    """
    del recorder_mock
    third = plan_windows(NOW - INITIAL_BACKFILL, NOW)[2]
    kraken = FakeKraken(
        NOW,
        history_start=NOW - 400 * DAY,
        future_rows=False,
        codes={third.start_on: "KT-CT-9999"},
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-WF", service)

    data = await _sync(hass, coordinator, NOW)

    assert data.units_degraded == 1
    assert service.requests[-1][:2] == (third.start_on - DAY, third.start_on - DAY)
    lane = coordinator._lanes[(property_key("A-TEST-WF", PROPERTY), "CONSUMPTION")]
    revised_from = (NOW - 14 * DAY).astimezone(NZ).date()
    week = {third.start_on + offset * DAY for offset in range(third.days)}
    assert set(lane.bad) == {day for day in week if day < revised_from}
    assert third.start_on - DAY not in lane.bad


# ------------------------------------------------------- windows misread


@pytest.mark.parametrize(
    "misread",
    [
        pytest.param({"exclusive_end_on": True}, id="end_on_excluded"),
        pytest.param({"shift_days": -1}, id="a_day_early"),
    ],
)
@pytest.mark.asyncio
async def test_a_window_read_without_its_last_date_fails_rather_than_leave_it_out(
    recorder_mock, hass: HomeAssistant, misread: dict[str, Any]
) -> None:
    """
    Meridian reads ``endOn`` as excluded, or the dates a day early, in every window.

    A backfill window then comes back without its last date, though the
    stored history goes on after it. Asked for that date and the next
    together, Meridian returns readings for it: the window fails, and none
    of its dates counts as fetched, or as empty. Asked for again, each date
    alone, they fail the same way. The updates succeed, nothing older than
    the stored history is imported, and the lane does not reach its target.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-XE", kraken, 20)
    oldest = (NOW - 20 * DAY).astimezone(NZ).date()
    for name, value in misread.items():
        setattr(kraken, name, value)
    coordinator = _coordinator(hass, "A-TEST-XE", service)
    lane_key = (property_key("A-TEST-XE", PROPERTY), "CONSUMPTION")

    covered = 0
    for update in range(4):
        at = NOW + update * HOUR
        kraken.now = at
        data = await _sync(hass, coordinator, at)
        assert data.units_failed == 0
        covered += data.coverage_windows

    assert covered
    lane = coordinator._lanes[lane_key]
    assert lane.bad
    assert all(day >= oldest for day in lane.fetched | lane.empty)
    rows = await _rows(hass, energy_id)
    assert rows[0][0] >= (NOW - 20 * DAY).timestamp()
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is False
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.parametrize(
    "misread",
    ["exclusive_start_hour", "exclusive_start_on"],
    ids=["first_hour_excluded", "start_on_excluded"],
)
@pytest.mark.asyncio
async def test_a_window_read_without_its_first_hour_or_date_fails(
    recorder_mock, hass: HomeAssistant, misread: str
) -> None:
    """
    Meridian reads ``startOn``'s first hour, or the whole date, as excluded.

    After the restart's first sync, an update with Meridian reading the
    windows right fills 6 weeks of the backfill. From then on each window
    comes back without its first hour or its first date, though the history
    goes on before it, as the target is older. Asked for the date before it
    and that date together, Meridian returns readings for it: the window
    fails, and none of its dates counts as fetched, or as empty. Asked for
    again, each date alone, they fail the same way, the date alone coming
    back empty when its whole date is left out. The updates succeed, no
    date counts as fetched with an hour missing, and the lane does not
    reach its target.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-XS", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-XS", service)
    lane_key = (property_key("A-TEST-XS", PROPERTY), "CONSUMPTION")
    data = await _sync(hass, coordinator, NOW)
    assert data.coverage_windows == COVERAGE_WINDOWS
    imported = await _rows(hass, energy_id)
    setattr(kraken, misread, True)

    covered = 0
    for update in range(1, 5):
        at = NOW + update * HOUR
        kraken.now = at
        data = await _sync(hass, coordinator, at)
        assert data.units_failed == 0
        assert data.units_degraded == 0
        covered += data.coverage_windows

    assert covered
    lane = coordinator._lanes[lane_key]
    assert lane.bad
    rows = await _rows(hass, energy_id)
    assert rows[0][0] == imported[0][0]
    stored = {datetime.fromtimestamp(row[0], UTC) for row in rows}
    target = coverage_target(at, None)
    assert not [
        day
        for day in lane.fetched | lane.empty
        if any(
            hour not in stored
            for hour in _hours_of(day)
            if target <= hour < _floor_hour(at)
        )
    ]
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is False
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_meter_gap_before_a_dates_first_hour_costs_no_check(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A meter sent nothing from two days before a date to 01:00 on it.

    That date's week failed, so its dates wait while the backfill goes on
    past it: the week before returns nothing for its last two dates, which
    the lane keeps as empty. Asked for again alone, the date comes back
    without its first hour, and the date before is known to have no
    readings: no request checks how Meridian read it, and it is imported.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(hass, "A-TEST-MG", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-MG", service)
    lane_key = (property_key("A-TEST-MG", PROPERTY), "CONSUMPTION")
    day = (NOW - 20 * DAY).astimezone(NZ).date() - 6 * DAY
    kraken.gaps = ((local_midnight(day - 2 * DAY), local_midnight(day) + HOUR),)
    service.fail = {day: MeridianUpstreamError(503, gateway=False)}
    data = await _sync(hass, coordinator, NOW)
    assert data.coverage_windows == 1
    service.fail = {}
    soon = NOW + timedelta(minutes=30)
    kraken.now = soon
    await _sync(hass, coordinator, soon)
    lane = coordinator._lanes[lane_key]
    assert {day - DAY, day - 2 * DAY} <= lane.empty
    assert day in lane.bad

    requests: list[tuple[date, date, str]] = []
    for hours in (2, 3):
        at = NOW + hours * HOUR
        kraken.now = at
        sent = len(service.requests)
        await _sync(hass, coordinator, at)
        requests += service.requests[sent:]

    assert (day, day, "CONSUMPTION") in requests
    assert (day - DAY, day, "CONSUMPTION") not in requests
    assert day in lane.fetched
    assert day not in lane.bad
    rows = await _rows(hass, energy_id)
    missing = _missing_hours(rows, local_midnight(day), _floor_hour(at))
    assert missing == [local_midnight(day)]
    await _assert_consistent(hass, energy_id, cost_id)


@pytest.mark.asyncio
async def test_a_billing_periods_first_hour_left_out_is_asked_for_again(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The window that reaches a quarterly period's start comes back without 00:00.

    The bill totals need that hour, and the date before the period's start
    may have readings too, so Meridian is asked for it with the period's
    first date: the hour comes back, the window fails and waits, and the
    totals stay unknown rather than complete without it. Once Meridian
    reads the window right again, the next update after its wait fills it.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    start = TODAY - 92 * DAY
    billing = MeridianBillingPeriod(
        period_length="QUARTERLY",
        period_length_multiplier=1,
        is_fixed=True,
        start=start,
        end=TODAY + DAY,
        next_billing_date=TODAY + 2 * DAY,
        period_start_day=start.day,
    )
    coordinator = _coordinator(hass, "A-TEST-B3", service, billing=billing)
    lane_key = (property_key("A-TEST-B3", PROPERTY), "CONSUMPTION")
    data = await _sync(hass, coordinator, NOW)
    assert data.sync_mode is SyncMode.INITIAL
    kraken.exclusive_start_hour = True
    later = NOW + HOUR
    kraken.now = later
    sent = len(service.requests)

    data = await _sync(hass, coordinator, later)

    assert data.coverage_windows == 1
    (window, edge) = service.requests[sent + 1 :]
    assert window[0] == start
    assert edge[:2] == (start - DAY, start)
    assert start in coordinator._lanes[lane_key].bad
    (totals,) = data.account_results
    assert totals.usage_complete is False
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is False

    kraken.exclusive_start_hour = False
    last = later + 2 * HOUR
    kraken.now = last
    data = await _sync(hass, coordinator, last)
    (totals,) = data.account_results
    assert totals.usage_complete is True
    assert coordinator.unit_diagnostics()[0]["reaches_target"] is True


def _hours_of(day: date) -> list[datetime]:
    """Return the hours of a New Zealand date, in UTC."""
    hour, end, hours = local_midnight(day), local_midnight(day + DAY), []
    while hour < end:
        hours.append(hour)
        hour += HOUR
    return hours


@pytest.mark.parametrize("refusal", [True, False], ids=["refused", "unavailable"])
@pytest.mark.asyncio
async def test_a_refused_edge_check_shows_nothing_of_the_window_edge(
    recorder_mock, hass: HomeAssistant, refusal: bool
) -> None:
    """
    The history starts at midnight, and Meridian refuses requests spanning it.

    The week before the start comes back empty, right before a stored hour,
    so its last date is asked for with the first date of the history: that
    request is refused, as one starting before the supply may be, which
    shows nothing about how Meridian read the week. The week counts as
    empty, the next one does too, and the history starts there. Should
    Meridian's service fail that request instead, the week fails, and its
    dates wait.
    """
    del recorder_mock
    first = TODAY - 30 * DAY
    kraken = FakeKraken(NOW, history_start=local_midnight(first), future_rows=False)
    service = _Service(kraken)
    energy_id, cost_id = await _seed_recent(
        hass, "A-TEST-EDGE", kraken, 0, since=local_midnight(first)
    )
    real = service.async_get_measurements
    refused: list[tuple[date, date]] = []

    async def measurements(**kwargs: Any) -> Any:
        if kwargs["start_on"] < first <= kwargs["end_on"]:
            refused.append((kwargs["start_on"], kwargs["end_on"]))
            if refusal:
                raise MeridianGraphQLError("measurements", ("KT-CT-4321",))
            raise MeridianUpstreamError(500, gateway=False)
        return await real(**kwargs)

    coordinator = await _restarted(hass, "A-TEST-EDGE", service)
    coordinator.client.async_get_measurements = AsyncMock(side_effect=measurements)

    data = await _sync(hass, coordinator, NOW)

    assert data.units_failed == 0
    assert data.coverage_windows == (2 if refusal else 1)
    assert refused == [(first - DAY, first)]
    unit = coordinator.unit_diagnostics()[0]
    assert unit["history_start_found"] is refusal
    assert unit["reaches_target"] is refusal
    lane = coordinator._lanes[(property_key("A-TEST-EDGE", PROPERTY), "CONSUMPTION")]
    assert len(lane.bad) == (0 if refusal else 7)
    await _assert_consistent(hass, energy_id, cost_id)


# ------------------------------------------------------------ rate limits


@pytest.mark.asyncio
async def test_a_rate_limit_in_the_backfill_is_waited_out(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian asks for 4 hours: the next update waits so long, as does the backfill.

    The update succeeds with its own data. An update run sooner all the
    same, as one a user asked for, sends its own request but none for the
    backfill; once the wait has passed, the backfill goes on, and the
    update after it waits an hour again.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-RL", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-RL", service)
    first_window = (NOW - 20 * DAY).astimezone(NZ).date()
    service.fail = {first_window - 13 * DAY: MeridianRateLimitError(4 * 3600)}

    data = await _sync(hass, coordinator, NOW)

    assert data.units_failed == 0
    assert data.coverage_windows == 1
    assert coordinator.update_interval == timedelta(hours=4)
    service.fail = {}
    soon = NOW + HOUR
    kraken.now = soon
    sent = len(service.requests)
    data = await _sync(hass, coordinator, soon)
    assert data.coverage_windows == 0
    assert len(service.requests) - sent == 1
    assert coordinator.update_interval == UPDATE_INTERVAL

    later = NOW + 4 * HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    assert data.coverage_windows == COVERAGE_WINDOWS
    assert coordinator.update_interval == UPDATE_INTERVAL


@pytest.mark.asyncio
async def test_kraken_rate_limits_in_a_row_in_the_backfill_wait_twice_as_long(
    recorder_mock, hass: HomeAssistant
) -> None:
    """Kraken's in-band rate limit gives no wait: an hour, then 2 h, then back."""
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-RL2", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-RL2", service)
    first_window = (NOW - 20 * DAY).astimezone(NZ).date()
    service.fail = {first_window - 13 * DAY: MeridianRateLimitError(3600)}

    intervals: list[timedelta | None] = []
    for at in (NOW, NOW + HOUR):
        kraken.now = at
        await _sync(hass, coordinator, at)
        intervals.append(coordinator.update_interval)
    service.fail = {}
    at = NOW + 3 * HOUR
    kraken.now = at
    data = await _sync(hass, coordinator, at)
    intervals.append(coordinator.update_interval)

    assert intervals == [HOUR, 2 * HOUR, UPDATE_INTERVAL]
    assert data.coverage_windows == COVERAGE_WINDOWS


@pytest.mark.asyncio
async def test_a_short_rate_limit_in_the_backfill_never_brings_the_next_update_sooner(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian asks for a minute, at 4 updates in a row: each next one waits its hour.

    Its wait, doubled at each rate limit in a row, holds the backfill alone:
    an update run a minute after the fourth, as one a user asked for, sends
    none of it, and once the wait has passed the backfill goes on.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-RL3", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-RL3", service)
    first_window = (NOW - 20 * DAY).astimezone(NZ).date()
    limited = first_window - 13 * DAY
    service.fail = {limited: MeridianRateLimitError(60)}

    at = NOW
    for hours in range(4):
        at = NOW + hours * HOUR
        kraken.now = at
        data = await _sync(hass, coordinator, at)
        assert data.units_failed == 0
        assert service.requests[-1][0] == limited
        assert coordinator.update_interval == UPDATE_INTERVAL

    # The fourth in a row waits 8 minutes.
    soon = at + timedelta(minutes=1)
    kraken.now = soon
    data = await _sync(hass, coordinator, soon)
    assert data.coverage_windows == 0
    service.fail = {}
    later = at + HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    assert data.coverage_windows == COVERAGE_WINDOWS
    assert coordinator.update_interval == UPDATE_INTERVAL


@pytest.mark.asyncio
async def test_after_a_rate_limit_in_the_backfill_the_update_asks_for_no_billing(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Meridian just asked Home Assistant to wait: the update sends nothing more.

    The billing period is asked for only once the wait has passed, whether
    nothing is cached, as after a restart whose first sync could not get
    it, or what is cached is a day old; meanwhile the cached period, if
    any, is kept. So a second rate limit, which would fail the update and
    make every sensor unavailable, is never met, and the update shows its
    own data.
    """
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-RLB", kraken, 20)
    coordinator = _coordinator(hass, "A-TEST-RLB", service)
    billing = MeridianBillingPeriod(
        period_length="MONTHLY",
        period_length_multiplier=1,
        is_fixed=False,
        start=TODAY - 10 * DAY,
        end=TODAY + 20 * DAY,
        next_billing_date=TODAY + 21 * DAY,
        period_start_day=(TODAY - 10 * DAY).day,
    )
    asked = coordinator.client.async_get_billing_period
    # The restart's first sync: Meridian's billing service fails.
    asked.side_effect = MeridianUpstreamError(503, gateway=False)
    data = await _sync(hass, coordinator, NOW)
    assert asked.await_count == 1
    assert data.account_results[0].billing_period is None

    # The update after it: nothing is cached.
    asked.side_effect = MeridianRateLimitError(3600)
    older = {
        TODAY - offset * DAY: MeridianRateLimitError(3600) for offset in range(15, 120)
    }
    service.fail = dict(older)
    soon = NOW + HOUR
    kraken.now = soon
    data = await _sync(hass, coordinator, soon)
    assert data.units_failed == 0
    assert service.requests[-1][0] in older
    assert asked.await_count == 1
    (result,) = data.account_results
    assert result.billing_period is None

    asked.side_effect = None
    asked.return_value = billing
    service.fail = {}
    later = NOW + 3 * HOUR
    kraken.now = later
    data = await _sync(hass, coordinator, later)
    assert asked.await_count == 2
    assert data.account_results[0].billing_period == billing

    # A day later the cached period has expired.
    asked.side_effect = MeridianRateLimitError(3600)
    service.fail = dict(older)
    day_later = later + DAY
    kraken.now = day_later
    sent = len(service.requests)
    data = await _sync(hass, coordinator, day_later)
    assert data.units_failed == 0
    assert service.requests[-1][0] in older
    assert len(service.requests) > sent + 1
    assert asked.await_count == 2
    assert data.account_results[0].billing_period == billing


# ---------------------------------------------------------------- pacing


@pytest.mark.asyncio
async def test_the_backfill_pauses_after_each_request_ends_however_long_it_took(
    recorder_mock, hass: HomeAssistant, waits: AsyncMock
) -> None:
    """At 10 s a request, each backfill request still starts 3.5 s after the last."""
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-P1", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-P1", service)
    clock = {"now": 1000.0}

    async def wait(seconds: float) -> None:
        clock["now"] += seconds

    real = service.async_get_measurements

    async def slow(**kwargs: Any) -> Any:
        clock["now"] += 10.0
        return await real(**kwargs)

    coordinator.client.async_get_measurements = AsyncMock(side_effect=slow)
    waits.side_effect = wait
    with patch.object(
        coordinator_module, "monotonic", side_effect=lambda: clock["now"]
    ):
        data = await _sync(hass, coordinator, NOW)

    assert data.coverage_windows == COVERAGE_WINDOWS
    pauses = [call.args[0] for call in waits.await_args_list]
    assert pauses[-COVERAGE_WINDOWS:] == [pytest.approx(3.5)] * COVERAGE_WINDOWS


@pytest.mark.asyncio
async def test_the_backfill_pauses_are_drawn_between_2_and_5_seconds(
    recorder_mock, hass: HomeAssistant, waits: AsyncMock
) -> None:
    """Each backfill request starts 2 to 5 s after the last, at random."""
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    await _seed_recent(hass, "A-TEST-P2", kraken, 20)
    coordinator = await _restarted(hass, "A-TEST-P2", service)
    clock = {"now": 1000.0}
    bounds: list[tuple[float, float]] = []

    async def wait(seconds: float) -> None:
        clock["now"] += seconds

    def uniform(low: float, high: float) -> float:
        bounds.append((low, high))
        return high

    waits.side_effect = wait
    with (
        patch.object(coordinator_module, "_utcnow", return_value=NOW),
        patch.object(coordinator_module.random, "uniform", side_effect=uniform),
        patch.object(coordinator_module, "monotonic", side_effect=lambda: clock["now"]),
    ):
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
    await async_wait_recording_done(hass)

    assert data.coverage_windows == COVERAGE_WINDOWS
    assert bounds == [(2.0, 5.0)] * COVERAGE_WINDOWS
    pauses = [call.args[0] for call in waits.await_args_list]
    assert pauses[-COVERAGE_WINDOWS:] == [pytest.approx(5.0)] * COVERAGE_WINDOWS


# ------------------------------------------------------ nothing to complete


@pytest.mark.asyncio
async def test_a_grid_export_with_no_readings_reaches_its_target(
    recorder_mock, hass: HomeAssistant
) -> None:
    """A feed-in meter that never exported: nothing is left to complete."""
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    never = FakeKraken(
        NOW, history_start=NOW - 400 * DAY, withdrawn_from=NOW - 400 * DAY
    )
    service = _Service(kraken, {"GENERATION": never})
    coordinator = _coordinator(hass, "A-TEST-X1", service, feed_in=True)

    for update in range(2):
        at = NOW + update * HOUR
        kraken.now = never.now = at
        await _sync(hass, coordinator, at)

    assert [
        (unit["direction"], unit["status"], unit["reaches_target"])
        for unit in coordinator.unit_diagnostics()
    ] == [("consumption", "ok", True), ("generation", "ok", True)]


@pytest.mark.asyncio
async def test_a_supply_that_ended_before_the_target_reaches_it(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    kraken = FakeKraken(
        NOW,
        history_start=NOW - 400 * DAY,
        future_rows=False,
        withdrawn_from=NOW - 120 * DAY,
    )
    service = _Service(kraken)
    coordinator = _coordinator(hass, "A-TEST-X2", service)

    for update in range(2):
        at = NOW + update * HOUR
        kraken.now = at
        await _sync(hass, coordinator, at)

    (unit,) = coordinator.unit_diagnostics()
    assert (unit["status"], unit["reaches_target"]) == ("ok", True)


# ------------------------------------------------------- a missed revision


@pytest.mark.asyncio
async def test_a_revision_pass_missed_on_its_day_runs_at_the_next_update(
    recorder_mock, hass: HomeAssistant, revision_pass: None
) -> None:
    """
    Home Assistant was off on the lane's day: the pass runs the day after.

    Once that pass has ended, the days after it start no other this month.
    """
    del recorder_mock, revision_pass
    number = "A-TEST-R2"
    key = property_key(number, PROPERTY)
    at = datetime(2026, 11, revision_day(f"{key}:CONSUMPTION"), 3, 30, tzinfo=UTC)
    seed_at = at - DAY
    kraken = FakeKraken(seed_at, history_start=at - 400 * DAY, future_rows=False)
    service = _Service(kraken)
    coordinator = _coordinator(hass, number, service)
    await _sync(hass, coordinator, seed_at)

    windows: list[int] = []
    for when in (at + DAY, at + DAY + HOUR, at + 2 * DAY, at + 5 * DAY):
        kraken.now = when
        data = await _sync(hass, coordinator, when)
        windows.append(data.revision_windows)

    assert windows == [COVERAGE_WINDOWS, 5, 0, 0]


# --------------------------------------------------------------- privacy

_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


@pytest.mark.asyncio
async def test_the_backfill_and_the_revision_log_nothing_private(
    recorder_mock,
    hass: HomeAssistant,
    revision_pass: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Debug logs of a backfill and a revision pass carry no date or identifier.

    The grid import's history starts 40 days back, and Meridian refuses the
    weeks before it; the grid export's starts there too, and its older
    weeks come back empty, which log no page line: one would give away
    where the history starts. One window fails once. On the grid import's
    revision day, its pass asks again for the stored days.
    """
    del recorder_mock, revision_pass
    number = "A-TEST-PRIV"
    key = property_key(number, PROPERTY)
    at = datetime(2026, 11, revision_day(f"{key}:CONSUMPTION"), 3, 30, tzinfo=UTC)
    start = _floor_hour(at - 40 * DAY) + 5 * HOUR
    refusing = FakeKraken(
        at, history_start=start, future_rows=False, pre_supply_error="KT-CT-4321"
    )
    empty_before = FakeKraken(at, history_start=start, future_rows=False)
    service = _Service(refusing, {"GENERATION": empty_before})
    await _seed_recent(hass, number, refusing, 30)
    await _seed_recent(hass, number, empty_before, 30, direction="GENERATION")
    coordinator = _coordinator(hass, number, service, feed_in=True)
    older = (at - 25 * DAY).astimezone(NZ).date()
    real = service.async_get_measurements
    failed: list[date] = []

    async def measurements(**kwargs: Any) -> Any:
        if (
            kwargs["direction"] == "CONSUMPTION"
            and kwargs["end_on"] < older
            and not failed
        ):
            failed.append(kwargs["end_on"])
            raise MeridianUpstreamError(500, gateway=False)
        return await real(**kwargs)

    coordinator.client.async_get_measurements = AsyncMock(side_effect=measurements)
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")

    coverage = revision = 0
    for hours in (0, 2, 3, 4):
        when = at + hours * HOUR
        refusing.now = empty_before.now = when
        data = await _sync(hass, coordinator, when)
        coverage += data.coverage_windows
        revision += data.revision_windows

    assert failed
    assert coverage
    assert revision
    records = [
        record
        for record in caplog.records
        if record.name.startswith("custom_components.meridian_energy")
    ]
    messages = [record.getMessage() for record in records]
    for expected in (
        "Some older Meridian usage history may be missing",
        "Meridian did not return some older usage history",
        "Meridian rejected a window older than every reading",
        "Meridian Energy asks Meridian again for older usage history",
        "mode=coverage",
        "mode=revision",
    ):
        assert any(expected in message for message in messages), expected
    private = (
        number,
        key,
        PROPERTY,
        "synthetic-meter",
        "synthetic-icp",
        "Synthetic Street",
        "synthetic-register",
        "0000000001SYN00",
        "arrayconnection",
        "YXJyYXljb25uZWN0aW9u",
        *refusing.payload_dates(),
        *refusing.cursors,
        *empty_before.cursors,
    )
    formatter = logging.Formatter()
    for record in records:
        formatted = formatter.format(record)
        for text in private:
            assert text not in formatted, (text, record.getMessage())
        assert not _DATE.search(record.getMessage()), record.getMessage()
    # Empty windows older than every stored reading log no page line.
    first = start.astimezone(NZ).date()
    assert any(
        direction == "GENERATION" and end_on < first
        for _start_on, end_on, direction in service.requests
    )
    assert not [
        message
        for message in messages
        if message.startswith("Meridian measurements page:")
        and "mode=coverage" in message
        and " rows=0 " in message
    ]
