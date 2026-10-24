"""
The frontier rule and the degrade rule against a real recorder.

The frontier is the newest stored energy hour. Every mode's walk reaches
back to it, up to 90 days, and a walk that stops early is imported only
when the windows it fetched reach it. The cases are those the design was
checked with: a stale restart whose third week fails (F1), a fresh restart
whose second week fails (F2), and an outage of 20 days without a restart
(F3); and a degraded restart whose weekly reconciliation runs once its
backoff has passed. Each checks the running sums: every stored hour's sum is
the previous hour's plus its own state. Every value is synthetic.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from itertools import pairwise
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy.api import (
    MeridianConnectionError,
    MeridianUpstreamError,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import MeasurementPage, SyncMode
from custom_components.meridian_energy.statistics import consumption_ids, property_key
from custom_components.meridian_energy.transport import MeridianHttpError
from custom_components.meridian_energy.windows import NZ

from .kraken_fake import FakeKraken, local_midnight
from .test_windows_recorder import (
    NOW,
    PROPERTY,
    _account,
    _alternating,
    _bad_steps,
    _newest,
    _revised,
    _rows,
    _seed,
    mock_recorder_before_hass,  # noqa: F401 - the recorder fixture's setup
)

HOUR = timedelta(hours=1)
DAY = timedelta(days=1)


class _Failing:
    """A measurements service that fails the windows starting on chosen dates."""

    def __init__(self, kraken: FakeKraken) -> None:
        self.kraken = kraken
        self.fail_start_on: set[date] = set()
        self.requests: list[tuple[date, date]] = []

    async def async_get_measurements(self, **kwargs: Any) -> MeasurementPage:
        self.requests.append((kwargs["start_on"], kwargs["end_on"]))
        if kwargs["start_on"] in self.fail_start_on:
            raise MeridianConnectionError("synthetic")
        return await self.kraken.async_get_measurements(**kwargs)


def _coordinator(
    hass: HomeAssistant, number: str, service: _Failing
) -> MeridianDataCoordinator:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(number),))
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(
        side_effect=service.async_get_measurements
    )
    return MeridianDataCoordinator(hass, client)


async def _run(
    hass: HomeAssistant, coordinator: MeridianDataCoordinator, now: datetime
) -> Any:
    with patch.object(coordinator_module, "_utcnow", return_value=now):
        data = await coordinator.async_fetch_and_import()
    await async_wait_recording_done(hass)
    return data


def _missing_hours(
    rows: list[tuple[float, ...]], start: datetime, end: datetime
) -> int:
    """Return how many hours from ``start`` until ``end`` have no stored row."""
    present = {row[0] for row in rows}
    missing, hour = 0, start
    while hour < end:
        missing += hour.timestamp() not in present
        hour += HOUR
    return missing


@pytest.mark.asyncio
async def test_f1_a_stale_walk_that_fails_short_of_the_frontier_imports_its_weeks(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    20 days without updates, and the third week fails.

    The two weeks that came back do not reach the newest stored hour, and
    are imported all the same: the next update's reconciliation and its
    lane's coverage fill in the days between, behind them, with every
    running sum consistent and no hour missing. Until now nothing was
    imported until the walk reached that hour.
    """
    del recorder_mock
    energy_id, cost_id = await _seed(hass, "A-TEST-F1", NOW - 20 * DAY)
    frontier = _newest(await _rows(hass, energy_id))
    service = _Failing(FakeKraken(NOW, history_start=NOW - 420 * DAY))
    today = NOW.astimezone(NZ).date()
    service.fail_start_on = {today - 20 * DAY}
    coordinator = _coordinator(hass, "A-TEST-F1", service)

    data = await _run(hass, coordinator, NOW)

    rows = await _rows(hass, energy_id)
    assert data.units_degraded == 1
    assert _newest(rows) > frontier
    assert _missing_hours(rows, frontier + HOUR, NOW - DAY) > 0
    assert _bad_steps(rows) == []
    service.fail_start_on = set()
    for later in (NOW + HOUR, NOW + 2 * HOUR):
        service.kraken.now = later
        data = await _run(hass, coordinator, later)

    rows = await _rows(hass, energy_id)
    assert data.units_failed == data.units_degraded == 0
    assert _missing_hours(rows, frontier + HOUR, later - DAY) == 0
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.asyncio
async def test_f1_a_week_that_fails_its_checks_does_not_count_as_fetched(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    20 days without updates, and the week holding the frontier fails its checks.

    The second and third weeks have no readings for their last date, which
    two weeks in a row fail as Meridian leaving the end date out would. The
    third week's rows are dropped, and the two newer weeks are imported:
    they do not count as reaching the newest stored hour, so the days
    between are left missing, for the next updates to fill in, which leave
    no hour missing.
    """
    del recorder_mock
    energy_id, cost_id = await _seed(hass, "A-TEST-F1C", NOW - 20 * DAY)
    frontier = _newest(await _rows(hass, energy_id))
    today = NOW.astimezone(NZ).date()
    kraken = FakeKraken(
        NOW,
        history_start=NOW - 420 * DAY,
        gaps=[
            (local_midnight(day), local_midnight(day + DAY))
            for day in (today - 7 * DAY, today - 14 * DAY)
        ],
    )
    service = _Failing(kraken)
    coordinator = _coordinator(hass, "A-TEST-F1C", service)

    data = await _run(hass, coordinator, NOW)

    assert len(service.requests) == 3
    assert data.units_degraded == 1
    rows = await _rows(hass, energy_id)
    assert _missing_hours(rows, frontier + HOUR, NOW - DAY) > 0
    kraken.gaps = ()
    for later in (NOW + HOUR, NOW + 2 * HOUR):
        kraken.now = later
        data = await _run(hass, coordinator, later)

    rows = await _rows(hass, energy_id)
    assert data.units_failed == data.units_degraded == 0
    assert _missing_hours(rows, frontier + HOUR, later - DAY) == 0
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.asyncio
async def test_f2_a_fresh_walk_that_fails_at_its_second_week_imports_the_first(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A restart a day later, and the second week fails: the first is imported.

    The newest week reaches the newest stored hour, so its revised readings
    are imported from the stored hour before them, and the direction is
    degraded. The weeks before it keep their rows and sums.
    """
    del recorder_mock
    energy_id, cost_id = await _seed(hass, "A-TEST-F2", NOW, value=_alternating)
    later = NOW + DAY
    service = _Failing(FakeKraken(later, history_start=NOW - 400 * DAY, value=_revised))
    today = later.astimezone(NZ).date()
    service.fail_start_on = {today - 13 * DAY}
    coordinator = _coordinator(hass, "A-TEST-F2", service)

    data = await _run(hass, coordinator, later)

    rows = await _rows(hass, energy_id)
    assert data.sync_mode is SyncMode.RESTART
    assert data.units_degraded == 1
    revised = [row for row in rows if row[1] == 2.5]
    # The newest week: six days and the 16 hours of today that have ended.
    assert len(revised) == 6 * 24 + 16
    assert min(row[0] for row in revised) >= (later - 7 * DAY).timestamp()
    assert _newest(rows) == later.replace(minute=0) - HOUR
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.asyncio
async def test_f3_an_outage_of_20_days_without_a_restart_leaves_no_hour_missing(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The weekly reconciliation after 20 days reaches back to the frontier.

    Its own span is 14 days: without the frontier rule, the 6 days before it
    stayed missing for good. Older than 14 days, the walk is imported as one
    replay of what it fetched, which the 14-day cache could not compare.
    """
    del recorder_mock
    energy_id, cost_id = await _seed(hass, "A-TEST-F3", NOW)
    frontier = _newest(await _rows(hass, energy_id))
    service = _Failing(FakeKraken(NOW, history_start=NOW - 400 * DAY))
    coordinator = _coordinator(hass, "A-TEST-F3", service)
    first = await _run(hass, coordinator, NOW)
    assert first.sync_mode is SyncMode.RESTART

    later = NOW + 20 * DAY
    service.kraken.now = later
    service.requests.clear()
    data = await _run(hass, coordinator, later)

    rows = await _rows(hass, energy_id)
    assert data.sync_mode is SyncMode.FULL_RECONCILIATION
    assert len(service.requests) == 3
    assert data.results[0].requested_since <= frontier
    assert _missing_hours(rows, frontier + HOUR, later - DAY) == 0
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.asyncio
async def test_f3_an_outage_of_20_days_fetches_the_costs_that_lagged_the_usage(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The hours whose cost had not come before the outage get it after.

    Meridian publishes each hour's cost 6 hours after its usage, so the
    newest stored cost hour is older than the newest stored usage hour.
    After 20 days without an update, the walk reaches back to the cost's
    newest stored hour too, and the cost statistic has no hour missing.
    Until now the walk started at the newest usage hour: the 5 hours
    between kept no cost row for good, as no later update reached back that
    far.
    """
    del recorder_mock
    number = "A-TEST-F3C"
    lag = 6 * HOUR
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    kraken.cost = lambda start, _channel: (
        None if start + HOUR > kraken.now - lag else "15"
    )
    coordinator = _coordinator(hass, number, _Failing(kraken))
    first = await _run(hass, coordinator, NOW)
    assert first.sync_mode is SyncMode.INITIAL
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    energy_newest = _newest(await _rows(hass, energy_id))
    cost_newest = _newest(await _rows(hass, cost_id))
    assert cost_newest == energy_newest - lag

    later = NOW + 20 * DAY
    kraken.now = later
    data = await _run(hass, coordinator, later)

    costs = await _rows(hass, cost_id)
    assert data.sync_mode is SyncMode.FULL_RECONCILIATION
    assert data.results[0].requested_since <= cost_newest
    assert _missing_hours(costs, cost_newest, later - DAY) == 0
    assert _missing_hours(await _rows(hass, energy_id), energy_newest, later - DAY) == 0
    assert _bad_steps(costs) == []


class _TimingOut(_Failing):
    """A measurements service whose weeks older than the newest time out."""

    def __init__(self, kraken: FakeKraken) -> None:
        super().__init__(kraken)
        self.timing_out = False

    async def async_get_measurements(self, **kwargs: Any) -> MeasurementPage:
        start_on, end_on = kwargs["start_on"], kwargs["end_on"]
        today = self.kraken.now.astimezone(NZ).date()
        if self.timing_out and (end_on - start_on).days > 2 and end_on < today - DAY:
            self.requests.append((start_on, end_on))
            error = MeridianUpstreamError(504, gateway=True)
            error.__cause__ = MeridianHttpError(504, {})
            raise error
        return await super().async_get_measurements(**kwargs)


@pytest.mark.asyncio
async def test_f3_an_outage_whose_older_weeks_time_out_catches_up_by_day(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    20 days without updates, and every week but the newest times out.

    The walk back to the frontier stops at its second week; the newest week
    is imported all the same. An hour later the reconciliation the unit owes
    asks for the second week by day, its probe first, and the lane's
    coverage asks for the week between the newest stored hour and it, which
    times out too. Its dates are asked for one at a time once their wait
    has passed: the 20 days arrive with no hour missing and every running
    sum consistent. Until now the probe of the second week was forgotten in
    the update that met it, and the walk never got past the older weeks.
    """
    del recorder_mock
    energy_id, cost_id = await _seed(hass, "A-TEST-F3L", NOW)
    frontier = _newest(await _rows(hass, energy_id))
    service = _TimingOut(FakeKraken(NOW, history_start=NOW - 400 * DAY))
    coordinator = _coordinator(hass, "A-TEST-F3L", service)
    await _run(hass, coordinator, NOW)

    later = NOW + 20 * DAY
    service.timing_out = True
    days: list[list[int]] = []
    for hours in (0, 1, 3, 4):
        when = later + hours * HOUR
        service.kraken.now = when
        service.requests.clear()
        try:
            await _run(hass, coordinator, when)
        except UpdateFailed:
            assert _newest(await _rows(hass, energy_id)) == frontier
        days.append([(end - start).days + 1 for start, end in service.requests])

    rows = await _rows(hass, energy_id)
    assert days == [
        # The newest week is imported, and the second times out.
        [7, 7],
        # The reconciliation the unit owes: the second week by day and the
        # cutoff's date; then the week between the newest stored hour and
        # them, which times out too.
        [7] + [1] * 7 + [1] + [7],
        # The hourly update, then that week's dates one at a time, after
        # their wait, 6 at most in an update.
        [2] + [1] * 6,
        [2, 1],
    ]
    (unit,) = coordinator.unit_diagnostics()
    assert unit["status"] == "ok"
    assert _missing_hours(rows, frontier + HOUR, later + 4 * HOUR - DAY) == 0
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.asyncio
async def test_a_degraded_restart_imports_the_newer_weeks_and_reconciles_later(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A degraded restart keeps every step positive, and its reconciliation follows.

    Only the newest week is imported, every hour's state and running-sum
    step positive and consistent. An hour later, its backoff passed, the
    unit runs the weekly reconciliation it owes, over the full 14 days.
    """
    del recorder_mock
    energy_id, _ = await _seed(hass, "A-TEST-DG", NOW, value=_alternating)
    later = NOW + DAY
    service = _Failing(FakeKraken(later, history_start=NOW - 400 * DAY, value=_revised))
    today = later.astimezone(NZ).date()
    service.fail_start_on = {today - 13 * DAY}
    coordinator = _coordinator(hass, "A-TEST-DG", service)

    data = await _run(hass, coordinator, later)

    rows = await _rows(hass, energy_id)
    assert data.units_degraded == 1
    assert all(row[1] > 0 for row in rows)
    assert all(later_row[2] - earlier[2] > 0 for earlier, later_row in pairwise(rows))
    assert _bad_steps(rows) == []
    week_start = (later - 7 * DAY).timestamp()
    assert all(
        row[1] != 2.5 for row in rows if row[0] < week_start - DAY.total_seconds()
    )

    service.fail_start_on = set()
    service.requests.clear()
    reconciled_at = later + HOUR
    service.kraken.now = reconciled_at
    data = await _run(hass, coordinator, reconciled_at)

    rows = await _rows(hass, energy_id)
    assert data.sync_mode is SyncMode.TIP
    assert data.units_degraded == 0
    # The weekly reconciliation it owed: two weeks and the cutoff's date.
    assert len(service.requests) == 3
    fortnight = (reconciled_at - 14 * DAY + HOUR).timestamp()
    assert all(row[1] == 2.5 for row in rows if row[0] >= fortnight)
    assert _bad_steps(rows) == []
    (unit,) = coordinator.unit_diagnostics()
    assert unit["status"] == "ok"
    assert unit["last_failure"]["reason"] == "cannot_connect"


@pytest.mark.parametrize(
    ("stopped_days", "weeks_back", "backfill"), [(30, 5, 0), (100, 13, 1)]
)
@pytest.mark.asyncio
async def test_a_direction_whose_readings_stopped_sends_one_request_an_hour(
    recorder_mock,
    hass: HomeAssistant,
    stopped_days: int,
    weeks_back: int,
    backfill: int,
) -> None:
    """
    A supply that ended is searched back to its newest hour once, not hourly.

    The first update walks back to the newest stored hour, or 90 days, and
    finds nothing newer. The hourly and daily updates then stop at the empty
    newest week: one request each, as before the frontier rule. When the
    readings come back, the next update walks back to that hour again and
    imports from it, with no hour missing that 90 days can reach. A walk cut
    at 90 days back, to the hour, leaves the hours of that date before it to
    the same update's backfill: one request more.
    """
    del recorder_mock
    number = f"A-TEST-Q{stopped_days}"
    stopped = NOW - stopped_days * DAY
    energy_id, cost_id = await _seed(hass, number, stopped)
    frontier = _newest(await _rows(hass, energy_id))
    kraken = FakeKraken(
        NOW, history_start=NOW - 400 * DAY, gaps=[(frontier + HOUR, NOW + 30 * DAY)]
    )
    service = _Failing(kraken)
    coordinator = _coordinator(hass, number, service)

    sent = []
    for when in (NOW, NOW + HOUR, NOW + 2 * HOUR, NOW + DAY + HOUR):
        service.kraken.now = when
        service.requests.clear()
        data = await _run(hass, coordinator, when)
        sent.append((data.sync_mode, len(service.requests)))

    assert sent == [
        (SyncMode.RESTART, 13),
        (SyncMode.TIP, 1),
        (SyncMode.TIP, 1),
        (SyncMode.TARGETED_RECONCILIATION, 1),
    ]
    assert _newest(await _rows(hass, energy_id)) == frontier

    later = NOW + DAY + 2 * HOUR
    service.kraken.now = later
    service.kraken.gaps = ()
    service.requests.clear()
    data = await _run(hass, coordinator, later)

    rows = await _rows(hass, energy_id)
    assert data.sync_mode is SyncMode.TIP
    # Back to the newest stored hour, or 90 days.
    assert len(service.requests) == weeks_back + backfill
    assert data.coverage_windows == backfill
    reach = max(frontier, (later - 89 * DAY).replace(minute=0)) + HOUR
    assert _missing_hours(rows, reach, later - DAY) == 0
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []
