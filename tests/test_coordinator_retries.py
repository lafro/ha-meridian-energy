"""
Tests for what a unit's failures lead to across hourly updates.

The size ladder of an hourly update spans updates: a week that times out
is asked for by day at the next one, and two such weeks keep days until
the walk is done. A degraded unit whose reconciliation fails at once keeps
its hourly update. Until a first sync succeeds, its own retries are spaced
out, and no unit counts its failures towards its wait. A quiet direction
whose readings come back gets the time budget of a deep walk. A failed
setup names the request of the failure it raises. The coordinator runs
against the synthetic service of ``test_coordinator_units``. Every value
is synthetic.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.meridian_energy.api import MeridianGraphQLError
from custom_components.meridian_energy.const import (
    FIRST_SYNC_RETRY_SCHEDULE,
    POLL_BUDGET_SECONDS,
    REVISION_OVERLAP,
    SETUP_IMPORT_BUDGET_SECONDS,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import MeridianAccount, SyncMode
from custom_components.meridian_energy.statistics import (
    StatisticsAnchorError,
    StatisticsWriteError,
    consumption_ids,
    generation_ids,
    property_key,
)
from custom_components.meridian_energy.units import UnitState
from custom_components.meridian_energy.windows import NZ, local_midnight, plan_windows

from .test_coordinator_units import (
    _LOGGER_NAME,
    ACCOUNT,
    ACTIVE,
    DAY,
    ENDED,
    GRID_IMPORT,
    HOUR,
    NOW,
    TODAY,
    _account,
    _client,
    _Clock,
    _gateway_timeout,
    _messages,
    _property,
    _Request,
    _running,
    _service,
    _unavailable,
)

FULL = SyncMode.FULL_RECONCILIATION


def _days(since: datetime, now: datetime) -> int:
    """Return how many New Zealand dates a walk from ``since`` asks for."""
    return sum(window.days for window in plan_windows(since, now))


def _hours_imported(importer: AsyncMock, statistic_id: str) -> list[list[datetime]]:
    """Return the hours of each import of one energy statistic, in order."""
    return [
        [item.start.astimezone(UTC) for item in call.kwargs["measurements"]]
        for call in importer.await_args_list
        if call.kwargs["stat_energy_id"] == statistic_id
    ]


# ------------------------------------------- the ladder across hourly updates


@pytest.mark.asyncio
async def test_weeks_that_keep_timing_out_are_asked_for_by_day_at_later_updates(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    An initial import whose weeks time out and whose days come back goes on.

    The grid export has no statistics. The first update stops at its newest
    week; the next asks for that week by day and imports it, then the
    second week times out too, so later updates ask for every window by
    day: the hourly update its two days, and the weekly reconciliation the
    import owes, once its wait has passed, its 14 days. Its lane's coverage
    asks for the older days. Until now the first week was asked for whole
    again at every other try, and no history came.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    service.fail = lambda request: (
        _gateway_timeout()
        if request.direction == "GENERATION" and request.days > 3
        else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(feed_in=True))
    )
    energy_id, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))
    export_id, _ = generation_ids(property_key(ACCOUNT, ACTIVE))

    def latest(statistic_id: str) -> datetime | None:
        return NOW - 2 * HOUR if statistic_id == energy_id else None

    reconciliation = NOW + 3 * HOUR
    sent: list[list[int]] = []
    statuses: list[str] = []
    with _running(clock, lambda: now["value"], latest=latest) as importer:
        for hours in range(5):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            before = len(service.requests)
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
            sent.append(
                [
                    request.days
                    for request in service.requests[before:]
                    if request.direction == "GENERATION"
                ]
            )
            statuses.append(coordinator.unit_diagnostics()[1]["status"])

    assert sent == [
        [7],
        [1] * 7 + [7],
        [1, 1],
        [1] * _days(reconciliation - REVISION_OVERLAP, reconciliation),
        [1, 1],
    ]
    assert statuses == ["failed", "degraded", "degraded", "ok", "ok"]
    newest_week = _hours_imported(importer, export_id)[0]
    assert newest_week[0] == local_midnight(TODAY - 6 * DAY)
    assert any(
        message.startswith(
            "A second window of usage history needed requests by day; the next "
            "updates ask for those windows one day at a time"
        )
        for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.asyncio
async def test_older_weeks_that_keep_timing_out_are_asked_for_by_day_later(
    hass,
) -> None:
    """
    The newest week answers and the older ones time out: the import goes on.

    The grid export has no statistics. The first update stops at its second
    week, and imports its newest. The next is the weekly reconciliation the
    import owes: it asks for the second week by day, whose probe comes
    back, and the cutoff's date, so the unit recovers; its lane's coverage
    asks for the older days. Until now the probe of the second week was
    forgotten in the update that met it, a week being counted from its
    date, which is a week old: the second week was asked for whole again at
    every other try, and no history came.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    service.fail = lambda request: (
        _gateway_timeout()
        if request.direction == "GENERATION"
        and request.days > 3
        and request.end_on < TODAY - 3 * DAY
        else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(feed_in=True))
    )
    energy_id, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))
    export_id, _ = generation_ids(property_key(ACCOUNT, ACTIVE))

    def latest(statistic_id: str) -> datetime | None:
        return NOW - 2 * HOUR if statistic_id == energy_id else None

    sent: list[list[int]] = []
    statuses: list[str] = []
    with _running(clock, lambda: now["value"], latest=latest) as importer:
        for hours in range(4):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            before = len(service.requests)
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
            sent.append(
                [
                    request.days
                    for request in service.requests[before:]
                    if request.direction == "GENERATION"
                ]
            )
            statuses.append(coordinator.unit_diagnostics()[1]["status"])

    assert sent == [[7, 7], [7] + [1] * 7 + [1], [2], [2]]
    assert statuses == ["degraded", "ok", "ok", "ok"]
    newest_week = _hours_imported(importer, export_id)[0]
    assert newest_week[0] == local_midnight(TODAY - 6 * DAY)


@pytest.mark.asyncio
async def test_a_weekly_reconciliation_whose_weeks_time_out_ends_by_day(
    hass,
) -> None:
    """
    The weekly reconciliation completes within hours when weeks time out.

    Its newest week times out, so it waits an hour; it then asks for that
    week by day and the second week times out, so it imports the newest
    week, is degraded and waits 2 h, with hourly updates by day between.
    The reconciliation then asks for its 15 days by day and completes, and
    so does the hourly update after it, for its two days.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    failing = {"on": False}
    service.fail = lambda request: (
        _gateway_timeout() if failing["on"] and request.days > 3 else None
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    seen: list[tuple[str, list[int], str]] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        failing["on"] = True
        coordinator._last_full_reconciliation = NOW - 8 * DAY
        for hours in range(1, 6):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            before = len(service.requests)
            try:
                await coordinator.async_fetch_and_import(
                    time_budget=POLL_BUDGET_SECONDS
                )
                outcome = "imported"
            except UpdateFailed as err:
                outcome = str(err.translation_key)
            (unit,) = coordinator.unit_diagnostics()
            seen.append(
                (
                    outcome,
                    [request.days for request in service.requests[before:]],
                    unit["status"],
                )
            )

    weekly = NOW + 4 * HOUR
    assert seen == [
        ("upstream_timeout", [7], "failed"),
        ("imported", [1] * 7 + [7], "degraded"),
        ("imported", [1, 1], "degraded"),
        ("imported", [1] * _days(weekly - REVISION_OVERLAP, weekly), "ok"),
        ("imported", [1, 1], "ok"),
    ]


@pytest.mark.asyncio
async def test_a_window_asked_for_whole_again_after_its_days_came_back_keeps_days(
    hass,
) -> None:
    """
    The hourly update's own window, which times out whole, keeps its days.

    After the restart, the hourly update's two days time out whole and come
    back by day. Asked for whole again at the next update, they time out
    again: that is the second window that needed days, so the updates after
    it keep asking by day, instead of failing every other hour.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    failing = {"on": False}
    service.fail = lambda request: (
        _gateway_timeout() if failing["on"] and request.days > 1 else None
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    seen: list[tuple[str, list[int], int | None]] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        failing["on"] = True
        for hours in range(1, 6):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            # No reconciliation falls due: the hourly update alone.
            coordinator._last_full_reconciliation = now["value"]
            coordinator._last_targeted_reconciliation = now["value"]
            before = len(service.requests)
            splits: int | None = None
            try:
                data = await coordinator.async_fetch_and_import(
                    time_budget=POLL_BUDGET_SECONDS
                )
                outcome, splits = "imported", data.splits
            except UpdateFailed as err:
                outcome = str(err.translation_key)
            seen.append(
                (
                    outcome,
                    [request.days for request in service.requests[before:]],
                    splits,
                )
            )

    # Each update that asks for the window by day counts it in its summary:
    # by the probe the last one left, then by the days kept.
    assert seen == [
        ("upstream_timeout", [2], None),
        ("imported", [1, 1], 1),
        ("upstream_timeout", [2], None),
        ("imported", [1, 1], 1),
        ("imported", [1, 1], 1),
    ]


@pytest.mark.asyncio
async def test_a_probe_the_time_budget_stopped_is_sent_at_the_next_update(
    hass,
) -> None:
    """
    A probe that never went out is still owed: the next try sends it.

    The second week times out whenever it is asked for whole. The restart
    stops there, and the reconciliation it owes, at 170 s a request, runs
    out of time after the newest week, before the probe went out. The
    reconciliation after that asks for the week by day, and completes.
    Until now the probe was forgotten when it was planned, so that week was
    asked for whole again, timed out again, and the probe came back only
    for the try after, to be cut short again by a slow Meridian.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    second_week = TODAY - 7 * DAY
    service.fail = lambda request: (
        _gateway_timeout()
        if request.days == 7 and request.end_on == second_week
        else None
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))
    state = coordinator._unit_states.setdefault(GRID_IMPORT, UnitState())

    seen: list[tuple[list[tuple[int, int]], str, bool]] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        for hours, latency in ((0, 1.0), (1, 170.0), (3, 1.0)):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            service.latency = latency
            before = len(service.requests)
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
            seen.append(
                (
                    [
                        (request.days, (TODAY - request.end_on).days)
                        for request in service.requests[before:]
                    ],
                    state.status,
                    state.probe_on == second_week,
                )
            )

    assert seen == [
        ([(7, 0), (7, 7)], "degraded", True),
        ([(7, 0)], "degraded", True),
        ([(7, 0)] + [(1, days) for days in range(7, 15)], "ok", False),
    ]


# -------------------------------------------- a degraded unit's reconciliation


@pytest.mark.asyncio
async def test_a_reconciliation_that_fails_at_once_leaves_the_hourly_update_running(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A degraded unit whose owed reconciliation fails at its newest week stays degraded.

    The restart's second week times out once: degraded, the reconciliation
    waits an hour. It then times out on its newest week, so that update
    imports nothing and fails; the reconciliation waits 2 h, and the hourly
    update in between still runs and is imported, its two days whole. The
    reconciliation then asks for the newest week by day and completes. Until
    now the whole unit waited instead, its hourly update included.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)

    def fail(request: _Request) -> BaseException | None:
        if request.days < 7:
            return None
        if now["value"] == NOW and request.start_on == TODAY - 13 * DAY:
            return _gateway_timeout()
        if now["value"] == NOW + HOUR and request.end_on == TODAY:
            return _gateway_timeout()
        return None

    service.fail = fail
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    seen: list[tuple[str, list[int], str, int | None]] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR) as importer:
        for hours in range(4):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            before = len(service.requests)
            imports = importer.await_count
            try:
                await coordinator.async_fetch_and_import(
                    time_budget=POLL_BUDGET_SECONDS
                )
                outcome = "imported" if importer.await_count > imports else "-"
            except UpdateFailed as err:
                outcome = str(err.translation_key)
            (unit,) = coordinator.unit_diagnostics()
            seen.append(
                (
                    outcome,
                    [request.days for request in service.requests[before:]],
                    unit["status"],
                    unit["retry_in_s"],
                )
            )

    assert seen == [
        ("imported", [7, 7], "degraded", 3600),
        ("upstream_timeout", [7], "degraded", 7200),
        ("imported", [2], "degraded", 3600),
        ("imported", [1] * 7 + [7, 1], "ok", None),
    ]
    assert _messages(caplog, logging.WARNING) == []
    assert any(
        message.startswith(
            "Meridian's usage history did not come back for a reconciliation"
        )
        for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.parametrize(
    ("error", "reason", "said"),
    [
        (
            StatisticsWriteError("synthetic: not confirmed"),
            "statistics_write",
            (
                "Home Assistant did not confirm the statistics a reconciliation "
                "of Meridian's usage history wrote (statistics_write)"
            ),
        ),
        (
            StatisticsAnchorError("synthetic: refused"),
            "statistics_refused",
            (
                "A reconciliation of Meridian's usage history was not imported, "
                "as older statistics have no running total it could continue "
                "(statistics_refused)"
            ),
        ),
    ],
    ids=["unconfirmed", "refused"],
)
@pytest.mark.asyncio
async def test_a_reconciliation_whose_replay_fails_does_not_blame_meridian(
    hass,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    reason: str,
    said: str,
) -> None:
    """
    A degraded unit's reconciliation that Meridian answered says what failed.

    The restart's second week times out once: degraded, the reconciliation
    waits an hour. Meridian then answers every week of it, but its replay is
    not confirmed, or is refused. The unit stays degraded, the hourly update
    goes on, and the one INFO line says that Home Assistant failed to save
    it, not that Meridian's history did not come back.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    service.fail = lambda request: (
        _gateway_timeout()
        if now["value"] == NOW
        and request.days == 7
        and request.start_on == TODAY - 13 * DAY
        else None
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    async def replay(*_args: object, **_kwargs: object) -> tuple[int, int]:
        if now["value"] == NOW + HOUR:
            raise error
        return (1, 1)

    seen: list[tuple[str, str]] = []
    with _running(
        clock,
        lambda: now["value"],
        latest=NOW - 2 * HOUR,
        importer=AsyncMock(side_effect=replay),
    ):
        for hours in range(2):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            try:
                await coordinator.async_fetch_and_import(
                    time_budget=POLL_BUDGET_SECONDS
                )
                outcome = "imported"
            except UpdateFailed as err:
                outcome = str(err.translation_key)
            (unit,) = coordinator.unit_diagnostics()
            seen.append((outcome, unit["status"]))

    assert seen == [("imported", "degraded"), (reason, "degraded")]
    assert _messages(caplog, logging.WARNING) == []
    (message,) = [
        message
        for message in _messages(caplog, logging.INFO)
        if "reconciliation" in message
    ]
    assert message == (
        f"{said}; this update imported nothing for this direction, its hourly "
        "updates go on, and the reconciliation is tried again in about 2 h "
        "(direction=CONSUMPTION)"
    )
    assert "Meridian's usage history did not come back" not in message


@pytest.mark.asyncio
async def test_a_degraded_unit_whose_weeks_keep_timing_out_reconciles_by_day(
    hass,
) -> None:
    """
    The reconciliation a degraded unit owes completes when only its days answer.

    After the restart's degrade, every week times out: the reconciliation
    fails at its newest week, and the hourly update between asks for its two
    days whole, leaving that week's probe to the next reconciliation. That
    one asks for the week by day; the second week times out, so it imports
    the newest week and waits 4 h, while the hourly updates ask by day. The
    reconciliation then asks for every day of its span and completes.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)

    def fail(request: _Request) -> BaseException | None:
        if now["value"] == NOW:
            restart = request.start_on == TODAY - 13 * DAY
            return _gateway_timeout() if restart else None
        return _gateway_timeout() if request.days > 3 else None

    service.fail = fail
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    seen: list[tuple[str, list[int], str, int | None]] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        for hours in range(8):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            before = len(service.requests)
            try:
                await coordinator.async_fetch_and_import(
                    time_budget=POLL_BUDGET_SECONDS
                )
                outcome = "imported"
            except UpdateFailed as err:
                outcome = str(err.translation_key)
            (unit,) = coordinator.unit_diagnostics()
            seen.append(
                (
                    outcome,
                    [request.days for request in service.requests[before:]],
                    unit["status"],
                    unit["retry_in_s"],
                )
            )

    last = NOW + 7 * HOUR
    assert seen == [
        ("imported", [7, 7], "degraded", 3600),
        ("upstream_timeout", [7], "degraded", 7200),
        ("imported", [2], "degraded", 3600),
        ("imported", [1] * 7 + [7], "degraded", 14400),
        ("imported", [1, 1], "degraded", 10800),
        ("imported", [1, 1], "degraded", 7200),
        ("imported", [1, 1], "degraded", 3600),
        ("imported", [1] * _days(last - REVISION_OVERLAP, last), "ok", None),
    ]


@pytest.mark.asyncio
async def test_a_degraded_units_failed_hourly_update_waits_an_hour(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    One failed hourly update of a degraded unit waits 1 h, not the day it once did.

    Every week from 7 days back times out, so each reconciliation imports
    the newest week only and waits longer: 1 h, 2 h, then 4 h. An hourly
    update between fails with HTTP 503, and its resend too: the unit fails
    and waits an hour, counting that failure alone, while its
    reconciliation keeps its wait. The next hourly update imports, and the
    unit is degraded again. When the reconciliation that falls due fails
    on its newest week as well, the hourly updates still go on. Until now
    the failed hourly update took the reconciliation's count: it waited
    16 h, and with one direction every update failed until then.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    hourly_fails = {4, 6}

    def fail(request: _Request) -> BaseException | None:
        hour = (now["value"] - NOW) // HOUR
        today = now["value"].astimezone(NZ).date()
        if hour == 7 or (hour in hourly_fails and request.days == 2):
            return _unavailable()
        if (today - request.end_on).days >= 7:
            return _gateway_timeout()
        return None

    service.fail = fail
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    seen: list[tuple[str, list[int], str, int | None]] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        for hours in range(9):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            before = len(service.requests)
            try:
                await coordinator.async_fetch_and_import(
                    time_budget=POLL_BUDGET_SECONDS
                )
                outcome = "imported"
            except UpdateFailed as err:
                outcome = str(err.translation_key)
            (unit,) = coordinator.unit_diagnostics()
            assert unit["owed_reconciliation"] is FULL
            seen.append(
                (
                    outcome,
                    [request.days for request in service.requests[before:]],
                    unit["status"],
                    unit["retry_in_s"],
                )
            )

    assert seen == [
        ("imported", [7, 7], "degraded", 3600),
        # The probe of the second week times out too.
        ("imported", [7, 1], "degraded", 7200),
        ("imported", [2], "degraded", 3600),
        ("imported", [7, 7], "degraded", 14400),
        # The hourly update and its resend: an hour's wait of its own.
        ("upstream_unavailable", [2, 2], "failed", 3600),
        ("imported", [2], "degraded", 7200),
        ("upstream_unavailable", [2, 2], "failed", 3600),
        # The reconciliation falls due and fails on its newest week: it
        # waits 8 h, and the hourly update is due at the next update.
        ("upstream_unavailable", [7, 7], "failed", 0),
        ("imported", [2], "degraded", 7 * 3600),
    ]
    warnings = _messages(caplog, logging.WARNING)
    assert len(warnings) == 2
    assert all(
        warning.endswith("and this one is tried again in about 1 h")
        for warning in warnings
    )
    recovered = [
        message
        for message in _messages(caplog, logging.INFO)
        if message.startswith("Meridian usage history updates for one direction")
    ]
    assert len(recovered) == 2


@pytest.mark.asyncio
async def test_a_degraded_units_hourly_updates_failing_in_a_row_wait_longer(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Hourly updates of a degraded unit that fail in a row wait 1 h, then 2 h.

    As above, each reconciliation imports the newest week only and waits
    longer. Two hourly updates in a row then fail with HTTP 503, and their
    resends too: the first waits an hour, the second two hours, counting
    those failures alone, so the update between them and the reconciliation
    is deferred. Only the first logs a warning. The reconciliation keeps its
    own wait meanwhile: once it falls due, it imports the newest week again,
    and the unit is degraded again, with one INFO line. Until now no test
    had two failed hourly updates in a row.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)

    def fail(request: _Request) -> BaseException | None:
        hour = (now["value"] - NOW) // HOUR
        today = now["value"].astimezone(NZ).date()
        if hour in {4, 5} and request.days == 2:
            return _unavailable()
        if (today - request.end_on).days >= 7:
            return _gateway_timeout()
        return None

    service.fail = fail
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    seen: list[tuple[str, list[int], str, int | None]] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        for hours in range(8):
            now["value"] = service.krakens[ACTIVE].now = NOW + hours * HOUR
            before = len(service.requests)
            try:
                await coordinator.async_fetch_and_import(
                    time_budget=POLL_BUDGET_SECONDS
                )
                outcome = "imported"
            except UpdateFailed as err:
                outcome = str(err.translation_key)
            (unit,) = coordinator.unit_diagnostics()
            seen.append(
                (
                    outcome,
                    [request.days for request in service.requests[before:]],
                    unit["status"],
                    unit["retry_in_s"],
                )
            )

    assert seen == [
        ("imported", [7, 7], "degraded", 3600),
        ("imported", [7, 1], "degraded", 7200),
        ("imported", [2], "degraded", 3600),
        ("imported", [7, 7], "degraded", 14400),
        ("upstream_unavailable", [2, 2], "failed", 3600),
        # The second in a row: two hours, and no second warning.
        ("upstream_unavailable", [2, 2], "failed", 7200),
        ("upstream_unavailable", [], "failed", 3600),
        # Both waits have passed: the reconciliation it owes runs, and the
        # probe of its second week times out too.
        ("imported", [7, 1], "degraded", 8 * 3600),
    ]
    (warning,) = _messages(caplog, logging.WARNING)
    assert warning.endswith("and this one is tried again in about 1 h")
    recovered = [
        message
        for message in _messages(caplog, logging.INFO)
        if message.startswith("Meridian usage history updates for one direction")
    ]
    assert len(recovered) == 1


# ---------------------------------------------------- the first sync's failures


@pytest.mark.asyncio
async def test_a_first_sync_that_fails_counts_no_failure_towards_a_units_wait(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Until a first sync succeeds, every attempt tries each unit again.

    After a restart Meridian times out for both properties: the first sync
    is tried again after 10 minutes, then 30, and each warning says the
    direction is tried again by the next update; diagnostics give no wait
    of their own until then. Once the active property works, the ended one,
    still failing, waits 1 hour, not the 4 hours three failed attempts
    would have counted.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, ACTIVE, ENDED)
    down = {"on": True}
    service.fail = lambda request: (
        _gateway_timeout() if down["on"] or request.property_id == ENDED else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE))
    )

    retries: list[float | None] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        for minutes in (0, 10):
            now["value"] = NOW + timedelta(minutes=minutes)
            with pytest.raises(UpdateFailed) as failed:
                await coordinator.async_fetch_and_import(
                    time_budget=POLL_BUDGET_SECONDS
                )
            retries.append(failed.value.retry_after)
            assert [
                (unit["status"], unit["retry_in_s"])
                for unit in coordinator.unit_diagnostics()
            ] == [("failed", None), ("failed", None)]
        down["on"] = False
        now["value"] = NOW + timedelta(minutes=40)
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        ended, active = coordinator.unit_diagnostics()

    assert retries == [600, 1800]
    assert data.units_failed == 1
    assert (ended["status"], ended["retry_in_s"]) == ("failed", 3600)
    assert active["status"] == "ok"
    warnings = _messages(caplog, logging.WARNING)
    assert len(warnings) == 2
    assert all(
        warning.endswith("this one is tried again by the next update")
        for warning in warnings
    )


@pytest.mark.parametrize(
    "properties",
    [(ACTIVE, ENDED), (ENDED, ACTIVE)],
    ids=["refused-listed-first", "refused-listed-second"],
)
@pytest.mark.asyncio
async def test_a_refused_unit_does_not_hold_a_failed_first_sync_to_the_hourly_update(
    hass, properties: tuple[str, str]
) -> None:
    """
    A first sync fails with a failure a retry may clear, whatever the order.

    One property's grid import is refused (``statistics_refused``), which no
    retry clears, and Meridian is unavailable for the other. The first
    sync fails with Meridian's failure, tried again after 10 minutes, as
    when the refused property is listed second: until now the refusal of
    a property listed first left the other waiting an hour.
    """
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    service.fail = lambda request: (
        _unavailable() if request.property_id == ENDED else None
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, _account(*properties)))
    refused_id, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))

    async def replay(*_args: object, **kwargs: object) -> tuple[int, int]:
        if kwargs["stat_energy_id"] == refused_id:
            raise StatisticsAnchorError("synthetic: refused")
        return (1, 1)

    with (
        _running(clock, latest=NOW - 2 * HOUR, importer=AsyncMock(side_effect=replay)),
        pytest.raises(UpdateFailed) as failed,
    ):
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert failed.value.translation_key == "upstream_unavailable"
    assert failed.value.retry_after == FIRST_SYNC_RETRY_SCHEDULE[0]
    reasons = {
        unit["last_failure"]["reason"] for unit in coordinator.unit_diagnostics()
    }
    assert reasons == {"statistics_refused", "upstream_unavailable"}


# ------------------------------------------------- a quiet direction's budget


@pytest.mark.asyncio
async def test_a_quiet_direction_whose_readings_return_has_the_deep_budget(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Readings that come back for a quiet direction get the 15-minute budget.

    Its readings stopped 80 days ago: the first update searches its 90 days
    and the hourly update then asks for the newest week alone. Once readings
    come back, the update walks back to the newest stored hour, 12 weeks, at
    20 s a request: 4 minutes, which the 4-minute budget would cut short
    after 9 requests. It has the deep walk's 15 minutes and imports them
    all, rather than wait an hour for the rest.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    frontier = (NOW - 80 * DAY).replace(minute=0)
    service = _service(clock, gaps=((frontier + HOUR, NOW + 60 * DAY),))
    kraken = service.krakens[ACTIVE]
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    seen: list[tuple[int, int, int]] = []
    with _running(clock, lambda: now["value"], latest=frontier) as importer:
        for hours, latency, gaps in (
            (0, 1.0, ((frontier + HOUR, NOW + 60 * DAY),)),
            (1, 1.0, ((frontier + HOUR, NOW + 60 * DAY),)),
            (2, 20.0, ()),
        ):
            now["value"] = kraken.now = NOW + hours * HOUR
            kraken.gaps = gaps
            service.latency = latency
            before = len(service.requests)
            imports = importer.await_count
            data = await coordinator.async_fetch_and_import(
                time_budget=POLL_BUDGET_SECONDS
            )
            seen.append(
                (
                    len(service.requests) - before,
                    importer.await_count - imports,
                    data.units_failed,
                )
            )

    assert seen == [(13, 1, 0), (1, 0, 0), (12, 1, 0)]
    assert _messages(caplog, logging.WARNING) == []


@pytest.mark.asyncio
async def test_a_deep_walk_after_an_import_still_has_its_budget_without_it(
    hass,
) -> None:
    """
    A quiet direction's deep walk that starts after an import has all of its time.

    The second property's readings stopped 80 days ago, so both its
    directions are quiet. At the third update they are back, but its grid
    import's newest week times out: the property cools down, and its grid
    export's newest week waits behind the first property's import, whose
    replay takes 200 s of the update's clock, as one confirmed only behind a
    busy recorder can. The grid export's readings then come back, and its
    walk back to the newest stored hour, 12 weeks at 50 s a request, has
    the deep walk's 15 minutes with the replay's 200 s left out. Counted,
    they would stop it a week short of that hour, importing nothing.
    """
    clock = _Clock()
    now = {"value": NOW}
    frontier = (NOW - 80 * DAY).replace(minute=0)
    service = _service(clock, ACTIVE, ENDED)
    quiet = service.krakens[ENDED]
    quiet.gaps = ((frontier + HOUR, NOW + 60 * DAY),)
    account = MeridianAccount(
        number=ACCOUNT,
        status="ACTIVE",
        properties=(_property(ACTIVE), _property(ENDED, feed_in=True)),
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, account))
    active_id, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))
    export_id, _ = generation_ids(property_key(ACCOUNT, ENDED))
    slow = {"on": False}

    async def replay(*_args: object, **kwargs: object) -> tuple[int, int]:
        if slow["on"] and kwargs["stat_energy_id"] == active_id:
            slow["on"] = False
            clock.now += 200.0
        return (1, 1)

    def latest(statistic_id: str) -> datetime:
        return NOW - 2 * HOUR if statistic_id == active_id else frontier

    sent: list[int] = []
    with _running(
        clock,
        lambda: now["value"],
        latest=latest,
        importer=AsyncMock(side_effect=replay),
    ) as importer:
        for hours in range(3):
            now["value"] = NOW + hours * HOUR
            for kraken in service.krakens.values():
                kraken.now = now["value"]
            if hours == 2:
                quiet.gaps = ()
                service.latency = 50.0
                service.fail = lambda request: (
                    _gateway_timeout()
                    if request.property_id == ENDED
                    and request.direction == "CONSUMPTION"
                    else None
                )
                slow["on"] = True
            before = len(service.requests)
            data = await coordinator.async_fetch_and_import(
                time_budget=POLL_BUDGET_SECONDS
            )
            sent.append(
                sum(
                    request.direction == "GENERATION"
                    for request in service.requests[before:]
                )
            )

    assert not slow["on"]
    assert sent[1:] == [1, 12]
    assert data.units_failed == 1
    statuses = [unit["status"] for unit in coordinator.unit_diagnostics()]
    assert statuses == ["ok", "failed", "ok"]
    # Imported back to the newest stored hour, with the readings after it.
    imported = _hours_imported(importer, export_id)[-1]
    assert min(imported) <= frontier < max(imported)


@pytest.mark.asyncio
async def test_readings_published_more_than_a_week_late_keep_coming(hass) -> None:
    """
    A walk that imported readings newer than its frontier leaves the unit not quiet.

    The newest stored hour is 20 days old, and Meridian has published up to
    10 days ago, then one more day at each update, all of them older than
    the newest week, which stays empty. Each update walks back to the new
    frontier and imports the new day. Only an update that found nothing
    newer leaves the unit quiet, and the next asks for its newest week
    alone. Until now the import moved the frontier first, so every walk
    that imported counted as finding nothing newer, and the next updates
    stopped at the empty newest week until the weekly reconciliation.
    """
    clock = _Clock()
    now = {"value": NOW}
    frontier = (NOW - 20 * DAY).replace(minute=0)
    service = _service(clock)
    kraken = service.krakens[ACTIVE]
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))
    energy_id, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))

    seen: list[tuple[int, int, bool]] = []
    with _running(clock, lambda: now["value"], latest=frontier) as importer:
        for hours, published in ((0, 10), (1, 9), (2, 8), (3, 8), (4, 8)):
            now["value"] = kraken.now = NOW + hours * HOUR
            kraken.gaps = (((NOW - published * DAY).replace(minute=0), NOW + DAY),)
            before = len(service.requests)
            imports = importer.await_count
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
            unit = coordinator._unit_states[
                (property_key(ACCOUNT, ACTIVE), "CONSUMPTION")
            ]
            seen.append(
                (
                    len(service.requests) - before,
                    importer.await_count - imports,
                    unit.quiet_frontier is not None,
                )
            )

    assert seen == [
        (13, 1, False),
        (2, 1, False),
        (2, 1, False),
        (2, 0, True),
        (1, 0, True),
    ]
    newest = coordinator._frontier[energy_id]
    assert newest == (NOW - 8 * DAY).replace(minute=0) - HOUR


# -------------------------------------------- which request a failure names


@pytest.mark.asyncio
async def test_a_failed_setup_names_the_request_of_the_failure_it_raises(
    hass,
) -> None:
    """
    The request a failed setup names is that of the unit whose failure it raises.

    The active property, listed first, fails at its newest week with
    unusable data; the ended one's requests then time out. Setup fails with
    the first property's failure, and names its request, in flight when it
    failed, not the ended property's last one.
    """
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)

    def fail(request: _Request) -> BaseException | None:
        if request.property_id == ENDED:
            return _gateway_timeout()
        if request.end_on == TODAY:
            return MeridianGraphQLError("measurements", ("UNKNOWN",))
        return None

    service.fail = fail
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ACTIVE, ENDED))
    )

    with _running(clock), pytest.raises(UpdateFailed) as raised:
        await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert raised.value.translation_key == "invalid_data"
    assert isinstance(raised.value.__cause__, MeridianGraphQLError)
    # The ended property was asked for after the active one failed.
    assert service.requests[-1].property_id == ENDED
    call = coordinator.last_request
    assert call is not None
    assert (call.property_index, call.direction) == (1, "CONSUMPTION")
    assert (call.window, call.window_count) == (1, 13)
    assert call.finished_at is None


@pytest.mark.asyncio
async def test_a_failure_its_unit_recorded_leaves_no_request_in_flight(hass) -> None:
    """
    A failed request whose unit recorded the failure is no longer in flight.

    The ended property, listed second, times out on its newest week and on
    the probe after the cooldown, the setup's last request; the active
    property imports, so setup goes on. A cancellation while the history is
    saved then names no request in flight.
    """
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    service.fail = lambda request: (
        _gateway_timeout() if request.property_id == ENDED else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ACTIVE, ENDED))
    )

    with _running(clock):
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert data.units_failed == 1
    assert [request.days for request in service.sent(ENDED)] == [7, 1]
    assert service.requests[-1].property_id == ENDED
    call = coordinator.last_request
    assert call is not None
    assert call.property_index == 2
    assert call.finished_at is not None
