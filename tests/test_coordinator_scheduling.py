"""
Tests for how a sync schedules its units, and what each owes when it waits.

A degraded unit keeps owing its reconciliation through the hourly updates
that run while it waits; a unit that skips a reconciliation, waiting or
unreached, owes it; the units the time budget did not reach go first next
time; a property cooling down after a gateway timeout lets the others go
first; a walk that cannot be imported until it is complete gets the setup's
time budget. The coordinator runs against the synthetic service of
``test_coordinator_units``. Every value is synthetic.
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from custom_components.meridian_energy.api import MeridianConnectionError
from custom_components.meridian_energy.const import (
    POLL_BUDGET_SECONDS,
    SETUP_IMPORT_BUDGET_SECONDS,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeasurementPage,
    MeridianAccount,
    SyncMode,
)
from custom_components.meridian_energy.statistics import consumption_ids, property_key

from .kraken_fake import FakeKraken
from .test_coordinator_units import (
    _LOGGER_NAME,
    ACCOUNT,
    ACTIVE,
    DAY,
    ENDED,
    HOUR,
    NOW,
    TODAY,
    _account,
    _client,
    _Clock,
    _gateway_timeout,
    _messages,
    _running,
    _Service,
    _service,
)

PROPERTIES = tuple(f"synthetic-property-{number}" for number in range(1, 5))
FULL = SyncMode.FULL_RECONCILIATION


# ------------------------------------------------- what a degraded unit owes


@pytest.mark.asyncio
async def test_a_degraded_unit_keeps_owing_its_reconciliation_through_hourly_updates(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The hourly updates during the wait leave the unit degraded and owing.

    The second week keeps failing: the restart degrades the unit, which
    waits 1 h; its weekly reconciliation then degrades again and waits 2 h,
    then 4 h. The hourly updates between import the newest hours and change
    none of that. Once the old week comes back, the reconciliation it owed
    runs in full, and the unit works again, said once.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    failing = {"on": True}
    service.fail = lambda request: (
        MeridianConnectionError("synthetic")
        if failing["on"] and request.start_on <= TODAY - 13 * DAY
        else None
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    seen = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR) as importer:
        for hours in range(8):
            failing["on"] = hours < 7
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            before = len(service.requests)
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
            (unit,) = coordinator.unit_diagnostics()
            seen.append(
                (
                    len(service.requests) - before,
                    unit["status"],
                    unit["owed_reconciliation"],
                    unit["retry_in_s"],
                )
            )

    # The second week is resent once after a pause: 3 requests for the
    # restart and for each weekly reconciliation that fails. At 23:30, the
    # last, 14 days back starts at midnight: two weeks, no date before.
    assert seen == [
        (3, "degraded", FULL, 3600),
        (3, "degraded", FULL, 7200),
        (1, "degraded", FULL, 3600),
        (3, "degraded", FULL, 14400),
        (1, "degraded", FULL, 10800),
        (1, "degraded", FULL, 7200),
        (1, "degraded", FULL, 3600),
        (2, "ok", None, None),
    ]
    assert importer.await_count == 8
    recovered = [
        message
        for message in _messages(caplog, logging.INFO)
        if message.startswith("Meridian usage history updates for one direction")
    ]
    assert len(recovered) == 1


@pytest.mark.asyncio
async def test_a_unit_waiting_out_a_failure_owes_the_reconciliation_it_skipped(
    hass,
) -> None:
    """
    The weekly reconciliation falls due while one property waits: it owes it.

    The others run it and the cadence counts it done; the waiting unit runs
    it as soon as it is tried again, instead of the hourly update.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, ACTIVE, ENDED)
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ACTIVE, ENDED))
    )
    failing = {"on": False}
    service.fail = lambda request: (
        _gateway_timeout() if failing["on"] and request.property_id == ENDED else None
    )

    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import()
        failing["on"] = True
        for hours in (1, 2):
            now["value"] = NOW + hours * HOUR
            await coordinator.async_fetch_and_import()
        failing["on"] = False
        # The weekly reconciliation falls due while the ended property waits.
        coordinator._last_full_reconciliation = NOW - 8 * DAY
        now["value"] = NOW + 3 * HOUR
        weekly = await coordinator.async_fetch_and_import()
        assert weekly.sync_mode is FULL
        assert weekly.units_deferred == 1
        assert coordinator.unit_diagnostics()[1]["owed_reconciliation"] is FULL
        service.requests.clear()
        now["value"] = NOW + 4 * HOUR
        hourly = await coordinator.async_fetch_and_import()

    assert hourly.sync_mode is SyncMode.TIP
    assert len(service.sent(ACTIVE)) == 1
    # The weekly reconciliation it skipped: two weeks and the cutoff's date.
    assert len(service.sent(ENDED)) == 3
    assert [unit["status"] for unit in coordinator.unit_diagnostics()] == ["ok", "ok"]


# --------------------------------------------- units the budget did not reach


@pytest.mark.asyncio
async def test_the_units_the_budget_did_not_reach_go_first_next_time(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    At 25 s a request, an hourly update reaches 7 of 8 directions.

    The one it did not reach goes first at the next update, so every
    direction is fetched within two updates; the first time, one INFO line
    says so, and diagnostics count the updates that did not reach it.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, *PROPERTIES)
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(*PROPERTIES, feed_in=True))
    )

    reached: list[set[tuple[str, str]]] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        service.latency = 25.0
        for hours in (1, 2, 3):
            now["value"] = NOW + hours * HOUR
            before = len(service.requests)
            data = await coordinator.async_fetch_and_import(
                time_budget=POLL_BUDGET_SECONDS
            )
            assert data.units_deferred == 1
            reached.append(
                {
                    (request.property_id, request.direction)
                    for request in service.requests[before:]
                }
            )
            if hours == 1:
                not_reached = [
                    unit["not_reached"] for unit in coordinator.unit_diagnostics()
                ]
                assert not_reached == [0] * 7 + [1]

    every = {
        (prop, direction)
        for prop in PROPERTIES
        for direction in ("CONSUMPTION", "GENERATION")
    }
    assert all(len(units) == 7 for units in reached)
    assert reached[0] | reached[1] == every
    assert reached[1] | reached[2] == every
    assert (PROPERTIES[-1], "GENERATION") in reached[1]
    assert _messages(caplog, logging.WARNING) == []
    (line,) = [
        message
        for message in _messages(caplog, logging.INFO)
        if message.startswith("Meridian answered too slowly for this update")
    ]
    assert "direction=GENERATION account=1/1 property=4/4" in line


@pytest.mark.asyncio
async def test_a_unit_the_budget_did_not_reach_owes_the_weekly_reconciliation(
    hass,
) -> None:
    """The weekly reconciliation runs for it when it is next reached."""
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, *PROPERTIES)
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(*PROPERTIES, feed_in=True))
    )

    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        coordinator._last_full_reconciliation = NOW - 8 * DAY
        service.latency = 25.0
        now["value"] = NOW + HOUR
        weekly = await coordinator.async_fetch_and_import(
            time_budget=POLL_BUDGET_SECONDS
        )
        assert weekly.sync_mode is FULL
        owed = [unit["owed_reconciliation"] for unit in coordinator.unit_diagnostics()]
        # Seven were degraded by the budget after their newest week, and
        # the eighth was not reached: all eight owe the reconciliation.
        assert owed == [FULL] * 8
        service.latency = 1.0
        service.requests.clear()
        now["value"] = NOW + 3 * HOUR
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    sent = Counter(
        (request.property_id, request.direction) for request in service.requests
    )
    assert sent[(PROPERTIES[-1], "GENERATION")] == 3
    assert [unit["status"] for unit in coordinator.unit_diagnostics()] == ["ok"] * 8


# ------------------------------------------- a property cooling down waits


@pytest.mark.parametrize("first_sync_done", [False, True])
@pytest.mark.asyncio
async def test_an_ended_property_with_two_directions_does_not_block_the_active_one(
    hass, first_sync_done: bool
) -> None:
    """
    Its cooldown after a gateway timeout no longer holds up the active property.

    Every request for the ended property, listed first, times out. Its grid
    export waits out the cooldown behind the active property's requests,
    instead of before them, so the active property's newest week starts
    within 65 s at every update and is imported. Before any update has
    succeeded, after a restart say, the first one succeeds too.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, ACTIVE, ENDED)
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE, feed_in=True))
    )
    starts: list[float] = []
    started = clock.now

    async def timed(**kwargs: Any) -> MeasurementPage:
        if kwargs["property_id"] == ACTIVE:
            starts.append(clock.now - started)
        return await service.async_get_measurements(**kwargs)

    coordinator.client.async_get_measurements = AsyncMock(side_effect=timed)
    sent_to_active: list[int] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR) as importer:
        if first_sync_done:
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        service.fail = lambda request: (
            _gateway_timeout() if request.property_id == ENDED else None
        )
        for hours in range(1, 7):
            now["value"] = NOW + hours * HOUR
            started = clock.now
            before = len(starts)
            data = await coordinator.async_fetch_and_import(
                time_budget=POLL_BUDGET_SECONDS
            )
            sent_to_active.append(len(starts) - before)
            assert starts[before] <= 65.0
            assert data.results[1].consumption_rows == 1

    assert all(sent_to_active)
    assert importer.await_count >= 2 * 6


@pytest.mark.parametrize(("latency", "active"), [(1.0, 26), (5.0, 21), (34.0, 3)])
@pytest.mark.asyncio
async def test_setup_imports_the_active_property_when_an_ended_one_with_export_fails(
    hass, latency: float, active: int
) -> None:
    """
    The ended property's probes wait behind the active property's weeks.

    Its grid import's probe and its grid export's newest week and probe
    each wait out a cooldown of 75 s; the active property's 26 weeks go
    first, so setup imports them all within its 4 minutes at 1 s a request,
    and its newest weeks of both directions when slower.
    """
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    service.latency = latency
    service.fail = lambda request: (
        _gateway_timeout() if request.property_id == ENDED else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE, feed_in=True))
    )

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert len(service.sent(ACTIVE)) == active
    assert importer.await_count == 2
    assert data.units_failed + data.units_deferred == 2


@pytest.mark.asyncio
async def test_a_walk_waiting_for_its_cooldown_lets_another_property_go_on(
    hass,
) -> None:
    """
    In setup, a week that timed out waits 75 s; another property uses them.

    The first property's third week times out once. Its probe waits out the
    cooldown, while the second property's walk goes on rather than wait
    with it; the probe then follows, and the first walk ends in weeks.
    """
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    third_week = TODAY - 20 * DAY
    failed: list[bool] = []

    def once(request: Any) -> BaseException | None:
        if (
            request.property_id == ACTIVE
            and request.start_on == third_week
            and not failed
        ):
            failed.append(True)
            return _gateway_timeout()
        return None

    service.fail = once
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ACTIVE, ENDED))
    )

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    order = [request.property_id for request in service.requests]
    timed_out = next(
        index
        for index, request in enumerate(service.requests)
        if request.start_on == third_week
    )
    assert order[: timed_out + 1] == [ACTIVE, ENDED, ACTIVE, ACTIVE]
    # Every one of the second property's older weeks, then the probe.
    assert order[timed_out + 1 : timed_out + 13] == [ENDED] * 12
    assert service.requests[timed_out + 13].days == 1
    assert order[timed_out + 13 :] == [ACTIVE] * (len(order) - timed_out - 13)
    assert data.units_failed == 0
    assert importer.await_count == 2
    # The cooldown's wait, less the time the second property's weeks took.
    assert max(clock.waits) < 75.0


# ------------------------------------------------ walks that must be whole


@pytest.mark.parametrize("latency", [14.0, 20.0, 34.0])
@pytest.mark.asyncio
async def test_an_update_with_a_first_import_has_the_deep_walk_budget(
    hass, latency: float
) -> None:
    """
    A direction without statistics has 15 minutes for its first import.

    Its 13 weeks, after the grid import's hourly update, fit no 4-minute
    budget once a request takes more than about 13 s. A walk cut short
    imports the weeks it fetched, but one that never completes would never
    show that a supply has ended. With the deep walk's budget of 15
    minutes, it completes.
    """
    clock = _Clock()
    service = _service(clock)
    service.latency = latency
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(feed_in=True))
    )
    energy_id, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))
    started = clock.now

    def latest(statistic_id: str) -> Any:
        return NOW - 2 * HOUR if statistic_id == energy_id else None

    with _running(clock, latest=latest) as importer:
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    sent = Counter(request.direction for request in service.requests)
    assert sent == {"CONSUMPTION": 3, "GENERATION": 13}
    assert data.units_failed == data.units_degraded == 0
    assert importer.await_count == 2
    # Requests started after the first 160 s, which 4 minutes would not allow.
    assert clock.now - started - latency > POLL_BUDGET_SECONDS - 80


@pytest.mark.asyncio
async def test_setup_keeps_the_weeks_each_property_fetched_in_its_time(
    hass,
) -> None:
    """
    Setup finishes with one property whole and the newer weeks of the other.

    At 10 s a request, the two properties' 26 weeks do not fit setup's 4
    minutes: the first is imported whole, and the second, cut short, keeps
    the weeks it fetched, newest first. The entry's updates fill in the
    rest. Imported all or nothing, the second would have imported nothing.
    """
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    service.latency = 10.0
    account = _account(ACTIVE, ENDED)
    setup = MeridianDataCoordinator(hass, _client(service, account))
    with _running(clock) as importer:
        data = await setup.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert (data.units_failed, data.units_degraded) == (0, 1)
    assert importer.await_count == 2
    assert len(service.sent(ACTIVE)) == 13
    assert 1 < len(service.sent(ENDED)) < 13


@pytest.mark.asyncio
async def test_an_ended_property_neither_slows_the_hourly_updates_nor_cuts_the_weekly(
    hass,
) -> None:
    """
    At 15 s a request, an ended property with old statistics costs one request.

    Its first update searches 90 days and finds nothing newer than its
    statistics, which are 100 days old; its hourly updates then ask for its
    newest week alone. Its weekly reconciliation searches again, with the
    deep walk's budget, and the active property's runs in full first.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, ACTIVE, ENDED)
    service.latency = 15.0
    service.krakens[ENDED].gaps = ((NOW - 100 * DAY, NOW + 30 * DAY),)
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE))
    )
    ended_id, _ = consumption_ids(property_key(ACCOUNT, ENDED))

    def latest(statistic_id: str) -> Any:
        return NOW - 100 * DAY if statistic_id == ended_id else NOW - 2 * HOUR

    sent: list[tuple[SyncMode, int, int]] = []
    with _running(clock, lambda: now["value"], latest=latest):
        for when in (NOW, NOW + HOUR, NOW + 2 * HOUR, NOW + 8 * DAY):
            now["value"] = when
            before = len(service.requests)
            data = await coordinator.async_fetch_and_import(
                time_budget=POLL_BUDGET_SECONDS
            )
            requests = service.requests[before:]
            sent.append(
                (
                    data.sync_mode,
                    sum(request.property_id == ENDED for request in requests),
                    sum(request.property_id == ACTIVE for request in requests),
                )
            )
            assert data.units_failed == data.units_degraded == 0

    assert sent == [
        (SyncMode.RESTART, 13, 3),
        (SyncMode.TIP, 1, 1),
        (SyncMode.TIP, 1, 1),
        (FULL, 13, 3),
    ]
    assert [unit["status"] for unit in coordinator.unit_diagnostics()] == ["ok", "ok"]


@pytest.mark.asyncio
async def test_a_quiet_ended_property_leaves_the_hourly_update_its_four_minutes(
    hass,
) -> None:
    """
    An ended property's hourly update stops at its empty newest week: 4 minutes.

    Its statistics are 100 days old, so its cutoff is 90 days back, but its
    first update found nothing newer: its walk ends at its newest week, and
    does not count as one that reaches back over 14 days, which gets 15
    minutes. At 70 s a request, the hourly update reaches three of the four
    properties, as its 4 minutes allow; the fourth goes first next time.
    """
    clock = _Clock()
    now = {"value": NOW}
    others = PROPERTIES[:3]
    service = _service(clock, ENDED, *others)
    service.krakens[ENDED].gaps = ((NOW - 100 * DAY, NOW + 30 * DAY),)
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, *others))
    )
    ended_id, _ = consumption_ids(property_key(ACCOUNT, ENDED))

    def latest(statistic_id: str) -> Any:
        return NOW - 100 * DAY if statistic_id == ended_id else NOW - 2 * HOUR

    with _running(clock, lambda: now["value"], latest=latest):
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        now["value"] = NOW + HOUR
        service.latency = 70.0
        service.requests.clear()
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert data.sync_mode is SyncMode.TIP
    assert [request.property_id for request in service.requests] == [
        ENDED,
        *others[:2],
    ]
    assert data.units_deferred == 1
    assert coordinator._deadline is not None
    assert coordinator._deadline - coordinator._started_at == POLL_BUDGET_SECONDS


class _Revised(FakeKraken):
    """A history with one estimate, made actual with another reading once revised."""

    def __init__(self, now: datetime, estimate: datetime, **kwargs: Any) -> None:
        super().__init__(now, **kwargs)
        self.estimate = estimate
        self.revised = False

    def node(self, start: datetime, channel: int, direction: str) -> dict[str, Any]:
        node = super().node(start, channel, direction)
        if start == self.estimate:
            node["value"] = "0.7" if self.revised else "0.4"
            if not self.revised:
                node["metaData"]["utilityFilters"]["readingQuality"] = "ESTIMATE"
        return node


@pytest.mark.asyncio
async def test_a_direction_whose_readings_stopped_still_gets_its_estimates_revised(
    hass,
) -> None:
    """
    Readings stopped 10 days ago; the daily update still revises a 12-day estimate.

    The restart imports the days before readings stopped, one an estimate.
    Its hourly update then stops at the empty newest week, as for a quiet
    direction. The daily reconciliation reaches back to that estimate,
    older than the newest stored hour, so it walks past the empty week and
    imports the revised reading, rather than stop there until the weekly
    reconciliation, by when the estimate would be more than 14 days old.
    """
    clock = _Clock()
    now = {"value": NOW}
    frontier = (NOW - 10 * DAY).replace(minute=0)
    estimate = (NOW - 12 * DAY).replace(minute=0)
    kraken = _Revised(NOW, estimate, gaps=((frontier + HOUR, NOW + 60 * DAY),))
    service = _Service(clock, {ACTIVE: kraken})
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    sent: list[tuple[SyncMode, int]] = []
    with _running(clock, lambda: now["value"], latest=frontier) as importer:
        for when in (NOW, NOW + HOUR, NOW + DAY + HOUR):
            kraken.revised = when > NOW + HOUR
            now["value"] = when
            before = len(service.requests)
            data = await coordinator.async_fetch_and_import(
                time_budget=POLL_BUDGET_SECONDS
            )
            sent.append((data.sync_mode, len(service.requests) - before))

    assert sent == [
        (SyncMode.RESTART, 3),
        (SyncMode.TIP, 1),
        (SyncMode.TARGETED_RECONCILIATION, 2),
    ]
    assert importer.await_count == 2
    revised = [
        item
        for item in importer.await_args.kwargs["measurements"]
        if item.start == estimate
    ]
    assert [(item.quality, str(item.value_kwh)) for item in revised] == [
        ("ACTUAL", "0.7")
    ]


# --------------------------------------------- what setup hands its entry


class _Provisional(FakeKraken):
    """A history whose last 5 days are estimates."""

    def node(self, start: datetime, channel: int, direction: str) -> dict[str, Any]:
        node = super().node(start, channel, direction)
        if start >= self.now - 5 * DAY:
            node["metaData"]["utilityFilters"]["readingQuality"] = "ESTIMATE"
        return node


def _entry(
    hass, service: _Service, account: MeridianAccount, setup: MeridianDataCoordinator
) -> MeridianDataCoordinator:
    """Return the entry's coordinator, with what a finished setup hands it."""
    return MeridianDataCoordinator(
        hass,
        _client(service, account),
        topology=(account,),
        topology_at=NOW,
        reconciled_at=NOW,
        memory=setup.memory,
    )


@pytest.mark.asyncio
async def test_a_unit_setup_left_failing_is_not_reported_failing_again(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Setup's warning says the first update tries it again; that one warns no more.

    The entry's first update, minutes later, tries the ended property again:
    it still fails, which setup has said already. Once it works, one INFO
    line says so.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, ACTIVE, ENDED)
    failing = {"on": True}
    service.fail = lambda request: (
        _gateway_timeout() if failing["on"] and request.property_id == ENDED else None
    )
    account = _account(ENDED, ACTIVE)
    setup = MeridianDataCoordinator(hass, _client(service, account))
    with _running(clock):
        await setup.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )
    (warning,) = _messages(caplog, logging.WARNING)
    assert warning.endswith(
        "and this one is tried again by the first update after setup"
    )

    entry = _entry(hass, service, account, setup)
    imported, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))
    with _running(
        clock,
        lambda: now["value"],
        latest=lambda statistic_id: NOW - HOUR if statistic_id == imported else None,
    ):
        before = len(service.sent(ENDED))
        now["value"] = NOW + 5 * timedelta(minutes=1)
        data = await entry.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        assert len(service.sent(ENDED)) - before == 1
        assert data.units_failed == 1
        failing["on"] = False
        now["value"] = NOW + 3 * HOUR
        await entry.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert len(_messages(caplog, logging.WARNING)) == 1
    assert any(
        message.startswith(
            "Meridian usage history updates for one direction are working again"
        )
        for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.asyncio
async def test_the_entry_sizes_its_first_page_for_the_meters_registers(hass) -> None:
    """
    Setup hands over the rows an hour holds: one request for three registers.

    The entry's first update, the hourly one, asks for its two days in one
    page holding every register's readings. Starting from one register's,
    it would need two more pages.
    """
    clock = _Clock()
    service = _Service(clock, {ACTIVE: FakeKraken(NOW, channels=3)})
    account = _account()
    setup = MeridianDataCoordinator(hass, _client(service, account))
    with _running(clock):
        await setup.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    entry = _entry(hass, service, account, setup)
    service.requests.clear()
    with _running(clock, NOW + 5 * timedelta(minutes=1), latest=NOW - HOUR):
        data = await entry.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert data.sync_mode is SyncMode.TIP
    assert [request.before for request in service.requests] == [False]
    (result,) = data.results
    assert result.consumption_received_rows == 2 * 24 * 3
    assert data.units_failed == 0


@pytest.mark.asyncio
async def test_setups_provisional_rows_are_counted_and_revised_after_it(
    hass,
) -> None:
    """
    The entry knows the estimates setup imported, as after a 14-day restart.

    Its first update counts them in the provisional intervals, and its first
    daily reconciliation reaches back to the oldest of them, 5 days, rather
    than waiting a week for the weekly one.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _Service(clock, {ACTIVE: _Provisional(NOW)})
    account = _account()
    setup = MeridianDataCoordinator(hass, _client(service, account))
    with _running(clock):
        await setup.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    entry = _entry(hass, service, account, setup)
    seen = []
    with _running(clock, lambda: now["value"], latest=lambda _id: NOW - HOUR):
        for when in (NOW + 5 * timedelta(minutes=1), NOW + DAY + HOUR):
            now["value"] = service.krakens[ACTIVE].now = when
            data = await entry.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
            (result,) = data.results
            seen.append(
                (
                    data.sync_mode,
                    result.estimated_rows,
                    result.oldest_estimated,
                    result.requested_since,
                )
            )

    (tip, rows, oldest, _), (targeted, _, _, since) = seen
    assert tip is SyncMode.TIP
    assert rows >= 5 * 24 - 1
    assert oldest is not None
    assert oldest <= NOW - 5 * DAY + HOUR
    assert targeted is SyncMode.TARGETED_RECONCILIATION
    # The oldest estimate, less the 6-hour margin: not the 2-day minimum.
    assert since <= oldest - 6 * HOUR


@pytest.mark.asyncio
async def test_the_first_update_after_setup_does_not_import_a_stale_direction_again(
    hass,
) -> None:
    """
    Readings that ended 20 days ago: setup imported 70 days, and that stands.

    Until now the entry's first update saw statistics older than 14 days and
    imported the 90 days again, 13 requests. The frontier rule reaches back
    to the newest stored hour in any mode, and setup found nothing newer:
    the hourly update asks for the newest week alone.
    """
    clock = _Clock()
    service = _Service(
        clock, {ACTIVE: FakeKraken(NOW, gaps=[(NOW - 20 * DAY, NOW + 3 * DAY)])}
    )
    account = _account()
    setup = MeridianDataCoordinator(hass, _client(service, account))
    with _running(clock):
        await setup.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )
    (newest,) = setup.imported_newest.values()
    assert newest < NOW - 20 * DAY

    entry = _entry(hass, service, account, setup)
    service.requests.clear()
    with _running(clock, NOW + 5 * timedelta(minutes=1), latest=newest) as importer:
        data = await entry.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert data.sync_mode is SyncMode.TIP
    assert len(service.requests) == 1
    importer.assert_not_awaited()
