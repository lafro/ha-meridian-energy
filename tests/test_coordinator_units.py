"""
Tests for the sync's units: the size ladder, isolation, backoff and the frontier.

A unit is one direction of one property. The coordinator runs against a
synthetic service (``kraken_fake.FakeKraken``, one history per property) that
can fail chosen requests, with a clock that only the requests and the
coordinator's own waits move on, so cooldowns and budgets are exact. Every
value is synthetic.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy.api import (
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianGraphQLError,
    MeridianRateLimitError,
    MeridianUpstreamError,
)
from custom_components.meridian_energy.const import (
    FIRST_SYNC_RETRY_SCHEDULE,
    INITIAL_BACKFILL,
    POLL_BUDGET_SECONDS,
    SETUP_IMPORT_BUDGET_SECONDS,
)
from custom_components.meridian_energy.coordinator import (
    MeridianDataCoordinator,
    RunBudgetError,
)
from custom_components.meridian_energy.models import (
    MeasurementPage,
    MeridianAccount,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianProperty,
    SyncMode,
)
from custom_components.meridian_energy.statistics import (
    consumption_ids,
    generation_ids,
    property_key,
)
from custom_components.meridian_energy.transport import MeridianHttpError
from custom_components.meridian_energy.windows import NZ

from .kraken_fake import FakeKraken, local_midnight

# 16:30 on Friday 16 October 2026 in New Zealand.
NOW = datetime(2026, 10, 16, 3, 30, tzinfo=UTC)
TODAY = NOW.astimezone(NZ).date()
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
ACCOUNT = "A-TEST-1"
ACTIVE = "synthetic-property"
ENDED = "synthetic-ended-property"
GRID_IMPORT = (property_key(ACCOUNT, ACTIVE), "CONSUMPTION")
_LOGGER_NAME = coordinator_module.__name__
GATEWAY_SECONDS = 60.0


def _property(property_id: str, *, feed_in: bool = False) -> MeridianProperty:
    return MeridianProperty(
        id=property_id,
        address="1 Synthetic Street",
        meter_points=(
            MeridianMeterPoint(f"meter-{property_id}", "synthetic-icp", feed_in),
        ),
    )


def _account(*property_ids: str, feed_in: bool = False) -> MeridianAccount:
    return MeridianAccount(
        number=ACCOUNT,
        status="ACTIVE",
        properties=tuple(
            _property(property_id, feed_in=feed_in)
            for property_id in property_ids or (ACTIVE,)
        ),
    )


def _gateway_timeout() -> MeridianUpstreamError:
    error = MeridianUpstreamError(504, gateway=True)
    error.__cause__ = MeridianHttpError(504, {})
    return error


def _unavailable() -> MeridianUpstreamError:
    error = MeridianUpstreamError(503, gateway=False)
    error.__cause__ = MeridianHttpError(503, {})
    return error


def _dates_missing(*days: date) -> tuple[tuple[datetime, datetime], ...]:
    """Return gaps of whole New Zealand dates, for ``FakeKraken``'s ``gaps``."""
    return tuple((local_midnight(day), local_midnight(day + DAY)) for day in days)


class _Clock:
    """A monotonic clock that only the requests and the waits move on."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.waits: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def wait(self, seconds: float) -> None:
        self.waits.append(round(seconds, 3))
        self.now += seconds


@dataclass
class _Request:
    """One measurements request, without its cursor."""

    property_id: str
    direction: str
    start_on: date
    end_on: date
    before: bool

    @property
    def days(self) -> int:
        return (self.end_on - self.start_on).days + 1


@dataclass
class _Service:
    """
    A synthetic measurements service, one history per property.

    ``fail`` may return an error for a request instead of its answer. A
    request takes ``latency`` seconds, a gateway timeout 60 s. The
    directions in ``empty`` have no readings at all, as for an export
    register that never exported.
    """

    clock: _Clock
    krakens: dict[str, FakeKraken]
    fail: Callable[[_Request], BaseException | None] = lambda _request: None
    latency: float = 1.0
    requests: list[_Request] = field(default_factory=list)
    empty: frozenset[str] = frozenset()

    async def async_get_measurements(self, **kwargs: Any) -> MeasurementPage:
        request = _Request(
            kwargs["property_id"],
            kwargs["direction"],
            kwargs["start_on"],
            kwargs["end_on"],
            kwargs.get("before") is not None,
        )
        self.requests.append(request)
        error = self.fail(request)
        gateway = isinstance(error, MeridianUpstreamError) and error.gateway
        self.clock.now += GATEWAY_SECONDS if gateway else self.latency
        if error is not None:
            raise error
        if request.direction in self.empty:
            return MeasurementPage((), False, None)
        return await self.krakens[request.property_id].async_get_measurements(**kwargs)

    def sent(self, property_id: str | None = None) -> list[_Request]:
        return [
            request
            for request in self.requests
            if property_id is None or request.property_id == property_id
        ]


def _service(
    clock: _Clock, *property_ids: str, now: datetime = NOW, **kraken: Any
) -> _Service:
    return _Service(
        clock,
        {
            property_id: FakeKraken(now, **kraken)
            for property_id in property_ids or (ACTIVE,)
        },
    )


def _client(service: _Service, account: MeridianAccount) -> MagicMock:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(account,))
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(
        side_effect=service.async_get_measurements
    )
    return client


@contextmanager
def _running(
    clock: _Clock,
    now: Callable[[], datetime] | datetime = NOW,
    *,
    latest: Callable[[str], datetime | None] | datetime | None = None,
    importer: AsyncMock | None = None,
) -> Iterator[AsyncMock]:
    """Fix both clocks and the stored statistics; yield the importer."""
    importer = importer or AsyncMock(return_value=(1, 1))
    clock_now = now if callable(now) else (lambda: now)

    async def latest_start(_hass: Any, statistic_id: str) -> datetime | None:
        return latest(statistic_id) if callable(latest) else latest

    with (
        patch.object(coordinator_module, "_utcnow", side_effect=clock_now),
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(coordinator_module, "_async_wait", side_effect=clock.wait),
        patch.object(coordinator_module.random, "uniform", return_value=3.5),
        patch.object(
            coordinator_module,
            "async_latest_numeric_statistic_start",
            new=latest_start,
        ),
        patch.object(coordinator_module, "async_replay", importer),
    ):
        yield importer


def _expected_hours(kraken: FakeKraken, since: datetime) -> list[datetime]:
    """Return the hours an unbounded request kept: from the cutoff, ended by now."""
    return [
        start
        for start, _channel in kraken.history()
        if start >= since and start + HOUR <= kraken.now
    ]


def _imported_hours(importer: AsyncMock, call: int = -1) -> list[datetime]:
    measurements: tuple[MeridianMeasurement, ...] = importer.await_args_list[
        call
    ].kwargs["measurements"]
    return [item.start.astimezone(UTC) for item in measurements]


def _messages(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == _LOGGER_NAME and record.levelno == level
    ]


def _day_list(start: date, count: int) -> list[tuple[date, date]]:
    """Return ``count`` one-day windows, newest first, from ``start``."""
    return [(start - offset * DAY, start - offset * DAY) for offset in range(count)]


def _windows(service: _Service) -> list[tuple[date, date]]:
    return [(request.start_on, request.end_on) for request in service.requests]


# ------------------------------------------------------------- the ladder


@pytest.mark.asyncio
async def test_a_healthy_walk_needs_no_ladder(hass) -> None:
    clock = _Clock()
    service = _service(clock)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert len(service.requests) == 13
    assert all(request.days in {7, 6, 1} for request in service.requests)
    assert data.splits == 0
    assert _imported_hours(importer) == _expected_hours(
        service.krakens[ACTIVE], NOW - INITIAL_BACKFILL
    )


@pytest.mark.asyncio
async def test_one_transient_failure_then_back_to_weeks(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A week that times out once is asked for by its days, then weeks again.

    After the cooldown its newest day goes alone, the probe; the probe
    answered, the week's other days follow, and the 12 older weeks are
    asked for as weeks: 20 requests, not the 92 of days kept for good, which
    fit setup's 4 minutes at 1 s a request after the 60 s timeout and its
    75 s cooldown.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    service = _service(clock)
    failed: list[_Request] = []

    def once(request: _Request) -> BaseException | None:
        if failed:
            return None
        failed.append(request)
        return _gateway_timeout()

    service.fail = once
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert len(service.requests) == 20
    week = service.requests[0]
    assert (week.start_on, week.end_on) == (TODAY - 6 * DAY, TODAY)
    assert _windows(service)[1:8] == _day_list(TODAY, 7)
    assert all(request.days == 7 for request in service.requests[8:-1])
    # The probe waited out the gateway cooldown after the 60 s failure.
    assert clock.waits[0] == 75.0
    assert data.splits == 1
    assert data.retries == 0
    assert _imported_hours(importer) == _expected_hours(
        service.krakens[ACTIVE], NOW - INITIAL_BACKFILL
    )
    assert any(
        message.startswith(
            "Meridian did not answer a request for a week of usage history in time"
        )
        for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.parametrize(
    ("latency", "requests"), [pytest.param(4.0, 8, id="4s"), pytest.param(34.0, 2)]
)
@pytest.mark.asyncio
async def test_one_gateway_timeout_leaves_setup_less_time_for_the_newest_week(
    hass, latency: float, requests: int
) -> None:
    """
    The timeout and its cooldown take 135 s of setup's 4 minutes.

    At 4 s a request, the probe and the week's other 6 days still start
    within its first 160 s: setup keeps the newest week, and the entry's
    updates fetch the rest. At 34 s only the probe does, so the newest week
    cannot come back: setup fails with the gateway timeout, not with its
    time budget, as the timeout cost the time, and the failure form offers
    to go on without history. The timeout is raised as it came, its cause
    kept, as the failure warning names it. In 0.4.0, with 15 minutes, it
    completed.
    """
    clock = _Clock()
    service = _service(clock)
    service.latency = latency
    failed: list[_Request] = []
    timeout = _gateway_timeout()
    transport = timeout.__cause__

    def once(request: _Request) -> BaseException | None:
        if failed:
            return None
        failed.append(request)
        return timeout

    service.fail = once
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with (
        _running(clock),
        pytest.raises(UpdateFailed) if latency > 5 else nullcontext() as raised,
    ):
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert len(service.requests) == requests
    if raised is None:
        assert data.units_degraded == 1
        return
    assert raised.value.translation_key == "upstream_timeout"
    assert raised.value.__cause__ is timeout
    assert timeout.__cause__ is transport


@pytest.mark.asyncio
async def test_size_is_the_lever_keeps_days_after_a_second_laddered_week(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Every week times out and every day comes back: the 90 days arrive by day.

    The first week is laddered and the walk goes back to weeks; the second
    needs the ladder too, so the rest of the walk asks for days: 93
    requests, each hour once.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    service = _service(clock)
    service.fail = lambda request: _gateway_timeout() if request.days > 1 else None
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert len(service.requests) == 1 + 7 + 1 + 7 + 77
    assert sum(request.days > 1 for request in service.requests) == 2
    assert data.splits == 2
    hours = _imported_hours(importer)
    assert len(hours) == len(set(hours))
    assert hours == _expected_hours(service.krakens[ACTIVE], NOW - INITIAL_BACKFILL)
    assert any(
        message.startswith("A second week of usage history needed requests by day")
        for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.asyncio
async def test_size_is_not_the_lever_stopping_after_two_requests_and_the_cooldown(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """A probe that times out too ends the walk: 2 requests and about 195 s."""
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    service = _service(clock)
    service.fail = lambda _request: _gateway_timeout()
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))
    started = clock.now

    with _running(clock), pytest.raises(UpdateFailed) as raised:
        await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert raised.value.translation_key == "upstream_timeout"
    assert [request.days for request in service.requests] == [7, 1]
    assert clock.waits == [75.0]
    assert clock.now - started == 195.0
    assert any(
        message.startswith(
            "Meridian did not answer a request for one day of usage history in "
            "time either, so smaller requests do not help"
        )
        for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.asyncio
async def test_a_bad_day_in_the_newest_week_fails_setup(hass) -> None:
    """
    A day that times out after the day after it came back is a bad day.

    In the newest week, setup has not imported its newest week in full, so
    it fails, and the flow removes what it imported of that week.
    """
    clock = _Clock()
    service = _service(clock)
    bad_day = TODAY - 3 * DAY
    service.fail = _bad_day_fail(bad_day)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock) as importer, pytest.raises(UpdateFailed) as raised:
        await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert raised.value.translation_key == "upstream_timeout"
    assert service.requests[-1].start_on == bad_day
    assert _imported_hours(importer)[0] == local_midnight(bad_day + DAY)


@pytest.mark.asyncio
async def test_a_bad_day_in_an_older_week_leaves_setup_the_days_after_it(
    hass,
) -> None:
    """
    In an older week, setup keeps the newest week and the days after the bad one.

    The direction has no statistics: until now it stayed all or nothing and
    failed. It is degraded, and the entry's updates fill in the rest.
    """
    clock = _Clock()
    service = _service(clock)
    bad_day = TODAY - 8 * DAY
    service.fail = _bad_day_fail(bad_day)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert service.requests[-1].start_on == bad_day
    assert data.units_degraded == 1
    assert _imported_hours(importer) == _expected_hours(
        service.krakens[ACTIVE], local_midnight(bad_day + DAY)
    )


def _bad_day_fail(bad_day: date) -> Callable[[_Request], BaseException | None]:
    """Time out the week holding ``bad_day`` once, and that day alone always."""
    weeks_failed: set[date] = set()

    def fail(request: _Request) -> BaseException | None:
        if request.start_on == bad_day:
            return _gateway_timeout()
        if (
            request.days > 1
            and request.start_on <= bad_day <= request.end_on
            and request.start_on not in weeks_failed
        ):
            weeks_failed.add(request.start_on)
            return _gateway_timeout()
        return None

    return fail


@pytest.mark.asyncio
async def test_a_week_that_fails_its_checks_is_left_out_of_a_partial_import(
    hass,
) -> None:
    """
    A week whose rows failed their checks was dropped: the newer weeks are kept.

    The second and third weeks have no readings for their last date, which
    two weeks in a row fail as Meridian leaving the end date out would. So
    setup imports the two newest weeks, and the entry's updates ask for the
    rest.
    """
    clock = _Clock()
    service = _service(clock, gaps=_dates_missing(TODAY - 7 * DAY, TODAY - 14 * DAY))
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert len(service.requests) == 3
    assert data.units_degraded == 1
    (unit,) = coordinator.unit_diagnostics()
    assert unit["last_failure"]["reason"] == "invalid_data"
    assert _imported_hours(importer) == _expected_hours(
        service.krakens[ACTIVE], local_midnight(TODAY - 13 * DAY)
    )


@pytest.mark.asyncio
async def test_each_direction_keeps_the_weeks_it_fetched_before_it_failed(
    hass,
) -> None:
    """
    With statistics or without, a walk that fails imports the weeks before.

    Statistics kept from an earlier installation end 20 days ago. The grid
    import fails at its fourth week, after its newer weeks reached those
    statistics; the grid export, which has none, fails at its second week.
    Both are degraded, and setup finishes: each imported its newest week.
    """
    clock = _Clock()
    service = _service(clock)
    failing = {("CONSUMPTION", TODAY - 21 * DAY), ("GENERATION", TODAY - 7 * DAY)}
    service.fail = lambda request: (
        MeridianGraphQLError("measurements", ("UNKNOWN",))
        if (request.direction, request.end_on) in failing
        else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(feed_in=True))
    )
    energy_id, _ = consumption_ids(GRID_IMPORT[0])

    def latest(statistic_id: str) -> datetime | None:
        return NOW - 20 * DAY if statistic_id == energy_id else None

    with _running(clock, latest=latest) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert [unit["status"] for unit in coordinator.unit_diagnostics()] == [
        "degraded",
        "degraded",
    ]
    assert data.units_degraded == 2
    export = importer.await_args_list[-1].kwargs["measurements"]
    assert min(item.start for item in export) >= local_midnight(TODAY - 6 * DAY)


@pytest.mark.parametrize("empty", ["ended-property", "never-exported"])
@pytest.mark.asyncio
async def test_the_time_limit_leaves_setup_the_weeks_each_walk_fetched(
    hass, empty: str
) -> None:
    """
    A walk with no readings, fetched last, does not cut the others' history.

    At 64 s a request, setup's 4 minutes allow 3 requests: the newest week
    of the ended property, or of the export register that never exported,
    and 2 weeks of the active grid import. Setup keeps the 2 weeks, and the
    entry's updates ask for the rest.
    """
    clock = _Clock()
    ended = empty == "ended-property"
    service = _service(clock, ACTIVE, ENDED) if ended else _service(clock)
    service.latency = 64.0
    if ended:
        service.krakens[ENDED].withdrawn_from = NOW - 100 * DAY
        account = _account(ENDED, ACTIVE)
    else:
        service.empty = frozenset({"GENERATION"})
        account = _account(feed_in=True)
    coordinator = MeridianDataCoordinator(hass, _client(service, account))

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert len(service.requests) == 3
    assert data.units_failed == 0
    assert data.units_degraded == 2
    importer.assert_awaited_once()
    assert _imported_hours(importer) == _expected_hours(
        service.krakens[ACTIVE], local_midnight(TODAY - 13 * DAY)
    )


@pytest.mark.asyncio
async def test_a_window_whose_second_page_times_out_is_planned_again_in_days(
    hass,
) -> None:
    """
    Its first page's rows are dropped with it, so its days never meet them.

    Aggregate rows, which carry no register, would fail the check that an
    hour's readings can be told apart if a day met them again: here every
    hour comes back once. Every week's second page times out, so the next
    week's timeout and cooldown leave setup's 4 minutes no time for more
    than the newest week, by its days.
    """
    clock = _Clock()
    service = _service(clock, aggregate=True)
    service.fail = lambda request: (
        _gateway_timeout() if request.before and request.days > 1 else None
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with (
        _running(clock) as importer,
        # Pages of 100 rows: a week needs two.
        patch.object(coordinator_module, "page_size", return_value=100),
    ):
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert service.requests[0].days == 7
    assert service.requests[1].before
    assert data.splits >= 1
    hours = _imported_hours(importer)
    assert len(hours) == len(set(hours))
    assert hours == _expected_hours(
        service.krakens[ACTIVE], local_midnight(TODAY - 6 * DAY)
    )


@pytest.mark.asyncio
async def test_a_resend_that_times_out_still_cools_the_property_down(hass) -> None:
    """
    A week fails with HTTP 503, and its resend times out.

    The timeout of the resend starts the property's 75-second pause as a
    first timeout does, so the probe waits it out instead of going out a
    moment later, while the service may still be working on the resend.
    """
    clock = _Clock()
    service = _service(clock)
    answers = iter([_unavailable(), _gateway_timeout()])
    service.fail = lambda _request: next(answers, None)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    # The week, its resend, then its newest day alone.
    assert _windows(service)[:3] == [
        (TODAY - 6 * DAY, TODAY),
        (TODAY - 6 * DAY, TODAY),
        (TODAY, TODAY),
    ]
    # The resend's pause, then the cooldown after the resend's 60 s timeout.
    assert clock.waits[:2] == [3.5, 75.0]
    assert (data.retries, data.splits) == (1, 1)
    assert _imported_hours(importer) == _expected_hours(
        service.krakens[ACTIVE], NOW - INITIAL_BACKFILL
    )


@pytest.mark.asyncio
async def test_a_gateway_timeout_in_polling_is_probed_by_day_at_the_next_poll(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    An hourly update stops a unit at its first gateway timeout.

    The update's next run is the cooldown: it asks for the week that timed
    out by its days, starting with its newest, and then goes back to weeks.
    With statistics, the newer week that came back reaches the newest hour
    stored, so it is imported and the unit is degraded until then.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    second_week = TODAY - 13 * DAY
    service.fail = lambda request: (
        _gateway_timeout()
        if request.days > 1 and request.start_on == second_week
        else None
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR) as importer:
        first = await coordinator.async_fetch_and_import()
        assert first.sync_mode is SyncMode.RESTART
        assert first.units_degraded == 1
        assert [request.days for request in service.requests] == [7, 7]
        # Left to the next update: nothing was asked for by day yet.
        assert first.splits == 0
        assert _imported_hours(importer)[0] >= NOW - 7 * DAY
        (unit,) = coordinator.unit_diagnostics()
        assert unit["status"] == "degraded"
        assert unit["owed_reconciliation"] == SyncMode.FULL_RECONCILIATION
        assert unit["retry_in_s"] == 3600

        service.requests.clear()
        now["value"] = NOW + HOUR
        second = await coordinator.async_fetch_and_import()

    # The weekly reconciliation it owed: the newest week, the week that
    # timed out by its days, and the cutoff's date. The summary counts that
    # week as asked for again by day.
    assert second.units_degraded == 0
    assert second.splits == 1
    assert _windows(service) == [
        (TODAY - 6 * DAY, TODAY),
        *_day_list(TODAY - 7 * DAY, 7),
        (TODAY - 14 * DAY, TODAY - 14 * DAY),
    ]
    (unit,) = coordinator.unit_diagnostics()
    assert unit["status"] == "ok"
    assert any(
        message.startswith(
            "Meridian usage history updates for one direction are working again"
        )
        for message in _messages(caplog, logging.INFO)
    )
    assert _messages(caplog, logging.WARNING) == []


# -------------------------------------------------------------- isolation


@pytest.mark.asyncio
async def test_every_units_newest_window_runs_before_any_older_one(hass) -> None:
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    account = _account(ENDED, ACTIVE, feed_in=True)
    coordinator = MeridianDataCoordinator(hass, _client(service, account))

    with _running(clock):
        await coordinator.async_fetch_and_import(setup_import=True)

    newest = [
        (request.property_id, request.direction) for request in service.requests[:4]
    ]
    assert newest == [
        (ENDED, "CONSUMPTION"),
        (ENDED, "GENERATION"),
        (ACTIVE, "CONSUMPTION"),
        (ACTIVE, "GENERATION"),
    ]
    assert all(request.end_on == TODAY for request in service.requests[:4])
    assert all(request.end_on < TODAY for request in service.requests[4:])
    assert len(service.requests) == 4 * 13


@pytest.mark.asyncio
async def test_a_newest_week_that_waited_for_a_cooldown_still_comes_first(
    hass,
) -> None:
    """
    The other direction's newest week goes before the older weeks.

    In setup, the grid import's newest week times out once, so the property
    waits 75 s, and its grid export's newest week with it. Once the pause
    has passed, the grid import's probe comes back, then the grid export's
    newest week goes, then the rest of the grid import's week by day, and
    only then the older weeks. At 3 s a request, setup's 4 minutes then
    stop the walks after one older week, and setup keeps the weeks each
    fetched. Until 0.3.1 the grid import walked all its weeks first: the
    grid export never sent a request.
    """
    clock = _Clock()
    service = _service(clock)
    service.latency = 3.0
    failed: list[_Request] = []

    def once(request: _Request) -> BaseException | None:
        if failed:
            return None
        failed.append(request)
        return _gateway_timeout()

    service.fail = once
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(feed_in=True))
    )

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    sent = [
        (request.direction, request.days, (TODAY - request.end_on).days)
        for request in service.requests
    ]
    assert sent[:3] == [
        ("CONSUMPTION", 7, 0),
        ("CONSUMPTION", 1, 0),
        ("GENERATION", 7, 0),
    ]
    assert sent[3:9] == [("CONSUMPTION", 1, days) for days in range(1, 7)]
    assert sent[9:] == [("CONSUMPTION", 7, 7)]
    assert (data.units_failed, data.units_degraded) == (0, 2)
    assert importer.await_count == 2


@pytest.mark.asyncio
async def test_an_ended_property_listed_first_does_not_block_the_active_one(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The active property's newest window is published every hourly update.

    Every request for the ended property times out. Its unit fails at the
    first, logs one warning and waits 1 h, then 2 h; the active property's
    hourly update starts within 65 s each time and is imported.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, ACTIVE, ENDED)
    service.fail = lambda request: (
        _gateway_timeout() if request.property_id == ENDED else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE))
    )
    starts: list[float] = []

    async def timed(**kwargs: Any) -> MeasurementPage:
        if kwargs["property_id"] == ACTIVE:
            starts.append(clock.now - started)
        return await service.async_get_measurements(**kwargs)

    coordinator.client.async_get_measurements = AsyncMock(side_effect=timed)
    sent_to_ended: list[int] = []
    deferred: list[int] = []
    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR) as importer:
        for poll in range(4):
            now["value"] = NOW + poll * HOUR
            started = clock.now
            before = len(service.sent(ENDED))
            data = await coordinator.async_fetch_and_import(
                time_budget=POLL_BUDGET_SECONDS
            )
            sent_to_ended.append(len(service.sent(ENDED)) - before)
            deferred.append(data.units_deferred)
            assert data.results[1].consumption_rows == 1
            assert starts[-1] <= 65.0

    # Tried at the first update, again an hour later, then waits 2 h.
    assert sent_to_ended == [1, 1, 0, 1]
    assert deferred == [0, 0, 1, 0]
    assert importer.await_count == 4
    (warning,) = _messages(caplog, logging.WARNING)
    assert warning.startswith(
        "Meridian usage history updates are failing for one direction "
        "(direction=CONSUMPTION account=1/1 property=1/2 reason=upstream_timeout "
        "status=504)"
    )


@pytest.mark.parametrize(
    ("latency", "ended", "active", "degraded"),
    [(1.0, [7, 1], 13, 0), (44.0, [7], 3, 1)],
)
@pytest.mark.asyncio
async def test_an_ended_property_listed_first_lets_setup_import_the_active_one(
    hass, latency: float, ended: list[int], active: int, degraded: int
) -> None:
    """
    Setup succeeds when at least one property imported every direction.

    At 1 s a request, the ended property's two timeouts and its cooldown,
    which passes while the active property's weeks are fetched, leave
    setup's 4 minutes room for the active property's 13 weeks. So it does
    at 44 s a request: the ended property's timeout leaves room for 3 of
    them, newest first, and the entry's updates fetch the rest.
    """
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    service.latency = latency
    service.fail = lambda request: (
        _gateway_timeout() if request.property_id == ENDED else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE))
    )

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    # The ended property's week, then its newest day after the cooldown.
    assert [request.days for request in service.sent(ENDED)] == ended
    assert len(service.sent(ACTIVE)) == active
    assert (data.units_failed, data.units_degraded) == (1, degraded)
    importer.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_property_without_readings_does_not_finish_setup_for_the_others(
    hass,
) -> None:
    """
    A property whose 90 days are empty imported nothing: setup still fails.

    The ended property's supply ended 120 days ago, so its 13 weeks come
    back empty; one date of the active property's newest week never
    answers in time, by week or alone. Taken for a property that imported
    its history, the empty one would create the entry with no history at
    all. Setup fails with the active property's error instead, as it does
    for the active property alone.
    """
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    service.krakens[ENDED].withdrawn_from = NOW - 120 * DAY
    bad_day = TODAY - 3 * DAY
    service.fail = lambda request: (
        _gateway_timeout()
        if request.property_id == ACTIVE
        and request.start_on <= bad_day <= request.end_on
        else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE))
    )

    with _running(clock) as importer, pytest.raises(UpdateFailed) as raised:
        await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert raised.value.translation_key == "upstream_timeout"
    assert len(service.sent(ENDED)) == 13
    assert importer.await_count == 1


@pytest.mark.asyncio
async def test_setup_finishes_when_no_property_has_readings(hass) -> None:
    """Every property's 90 days empty, and none failing: setup finishes, as before."""
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    for kraken in service.krakens.values():
        kraken.withdrawn_from = NOW - 120 * DAY
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE))
    )

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert len(service.requests) == 2 * 13
    assert data.units_failed == 0
    importer.assert_not_awaited()


@pytest.mark.asyncio
async def test_setup_fails_when_every_property_has_a_failing_direction(hass) -> None:
    """A property imported for one direction only does not complete setup."""
    clock = _Clock()
    service = _service(clock)
    service.fail = lambda request: (
        MeridianConnectionError("synthetic")
        if request.direction == "GENERATION"
        else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(feed_in=True))
    )

    with _running(clock), pytest.raises(UpdateFailed) as raised:
        await coordinator.async_fetch_and_import(setup_import=True)

    assert raised.value.translation_key == "cannot_connect"


@pytest.mark.asyncio
async def test_a_failed_initial_unit_backs_off(hass) -> None:
    """
    A direction with no statistics that keeps failing waits 1 h, then 2 h.

    Its grid import, which has statistics, is updated every hour meanwhile;
    until now the failing initial import was repeated in full every hour.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    service.fail = lambda request: (
        MeridianGraphQLError("measurements", ("UNKNOWN",))
        if request.direction == "GENERATION"
        else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(feed_in=True))
    )
    energy_id, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))

    def latest(statistic_id: str) -> datetime | None:
        return NOW - 2 * HOUR if statistic_id == energy_id else None

    sent: list[int] = []
    with _running(clock, lambda: now["value"], latest=latest):
        for poll in range(5):
            now["value"] = NOW + poll * HOUR
            before = len(service.requests)
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
            sent.append(
                sum(
                    request.direction == "GENERATION"
                    for request in service.requests[before:]
                )
            )

    # Its newest week fails; then nothing while it waits.
    assert sent == [1, 1, 0, 1, 0]


@pytest.mark.asyncio
async def test_an_initial_import_failing_at_an_older_week_keeps_the_newer_ones(
    hass,
) -> None:
    """
    A direction with no statistics imports its four newer weeks, and goes on.

    Its fifth week fails: until now nothing was imported and the 90 days
    were asked for again after an hour. The four weeks are kept, the
    reconciliation it owes runs once its hour has passed, and the hourly
    updates go on; its lane's coverage asks for the older days.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    service.fail = lambda request: (
        MeridianGraphQLError("measurements", ("UNKNOWN",))
        if request.direction == "GENERATION" and request.end_on < TODAY - 21 * DAY
        else None
    )
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(feed_in=True))
    )
    energy_id, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))

    def latest(statistic_id: str) -> datetime | None:
        return NOW - 2 * HOUR if statistic_id == energy_id else None

    sent: list[int] = []
    statuses: list[str] = []
    with _running(clock, lambda: now["value"], latest=latest):
        for poll in range(3):
            now["value"] = NOW + poll * HOUR
            before = len(service.requests)
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
            sent.append(
                sum(
                    request.direction == "GENERATION"
                    for request in service.requests[before:]
                )
            )
            statuses.append(coordinator.unit_diagnostics()[1]["status"])

    # Five weeks; then the weekly reconciliation it owes; then the hourly update.
    assert sent == [5, 3, 1]
    assert statuses == ["degraded", "ok", "ok"]


@pytest.mark.asyncio
async def test_a_backfill_that_imported_its_newest_week_only_owes_14_days(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A degraded backfill keeps its newest week, and owes the reconciliation.

    The first sync finds no readings, then Meridian publishes 60 days. The
    next hourly update imports the newest day and queues the backfill,
    whose second week times out: its newest week is imported and the unit is
    degraded. Its hourly update runs meanwhile; once its wait of an hour has
    passed, the weekly reconciliation it owes reaches back 14 days. The
    older days are left to its lane's coverage, a few weeks at each update,
    rather than the 90 days walked again at once.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    kraken = service.krakens[ACTIVE]
    kraken.gaps = ((NOW - 400 * DAY, NOW + 30 * DAY),)
    published = NOW - 60 * DAY
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    def older_weeks_time_out(request: _Request) -> BaseException | None:
        return _gateway_timeout() if request.start_on <= TODAY - 7 * DAY else None

    syncs: list[tuple[int, int, str]] = []
    imported: list[list[datetime]] = []
    expected: list[list[datetime]] = []
    with _running(clock, lambda: now["value"]) as importer:
        first = await coordinator.async_fetch_and_import(
            time_budget=POLL_BUDGET_SECONDS
        )
        assert first.sync_mode is SyncMode.INITIAL
        kraken.gaps = ((NOW - 400 * DAY, published),)
        for offset, fail in (
            (HOUR, None),
            (2 * HOUR, older_weeks_time_out),
            (2 * HOUR + 30 * timedelta(minutes=1), None),
            (3 * HOUR, None),
            (4 * HOUR, None),
        ):
            now["value"] = kraken.now = NOW + offset
            service.requests.clear()
            service.fail = fail or (lambda _request: None)
            data = await coordinator.async_fetch_and_import(
                time_budget=POLL_BUDGET_SECONDS
            )
            (unit,) = coordinator.unit_diagnostics()
            syncs.append((len(service.requests), data.units_degraded, unit["status"]))
            imported.append(_imported_hours(importer))
            expected.append(_expected_hours(kraken, now["value"] - INITIAL_BACKFILL))

    assert syncs[:3] == [
        # The hourly update finds the newest day, and queues the backfill.
        (1, 0, "ok"),
        # The backfill: the newest week, then the older one times out.
        (2, 1, "degraded"),
        # Its wait has not passed: the hourly update alone.
        (1, 0, "degraded"),
    ]
    assert imported[1][0] >= local_midnight(TODAY - 6 * DAY)
    # The weekly reconciliation it owed: 14 days, the week that timed out
    # asked for by day, as the size ladder of hourly updates does.
    assert syncs[3] == (1 + 7 + 1, 0, "ok")
    assert imported[3][0] >= now["value"] - 15 * DAY
    assert imported[3] == [hour for hour in expected[3] if hour >= imported[3][0]]
    # Then the hourly update alone: nothing more is owed.
    assert syncs[4] == (1, 0, "ok")
    assert any(
        message.startswith(
            "Meridian usage history updates for one direction are working again"
        )
        for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.asyncio
async def test_every_unit_waiting_fails_the_update_with_their_failure(hass) -> None:
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import()
        service.fail = lambda _request: _gateway_timeout()
        # Failed at the next two updates, the unit then waits 2 h.
        for hours in (1, 2):
            now["value"] = NOW + hours * HOUR
            with pytest.raises(UpdateFailed) as failed:
                await coordinator.async_fetch_and_import()
            assert failed.value.translation_key == "upstream_timeout"
        before = len(service.requests)
        now["value"] = NOW + 3 * HOUR
        with pytest.raises(UpdateFailed) as waiting:
            await coordinator.async_fetch_and_import()

    assert len(service.requests) == before
    assert waiting.value.translation_key == "upstream_timeout"
    assert waiting.value.translation_placeholders == {"detail": "HTTP 504"}
    assert waiting.value.retry_after is None


@pytest.mark.asyncio
async def test_every_unit_waiting_after_the_time_limit_fails_the_update_with_it(
    hass,
) -> None:
    """
    A unit the time limit cut short waits, and the update says why.

    After 20 days without updates, the weekly reconciliation walks back to
    the newest hour stored, with 15 minutes: at 830 s a page, with pages of
    100 rows, the time limit stops it before its newest week has come back
    whole, and it imports nothing. The unit waits 1 h, then 2 h; an update
    while it waits fails with the time limit's message, as the failure that
    left it waiting did.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))
    later = NOW + 20 * DAY

    with (
        _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR),
        patch.object(coordinator_module, "page_size", return_value=100),
    ):
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        service.latency = 830.0
        for hours in (0, 1):
            now["value"] = service.krakens[ACTIVE].now = later + hours * HOUR
            with pytest.raises(UpdateFailed) as failed:
                await coordinator.async_fetch_and_import(
                    time_budget=POLL_BUDGET_SECONDS
                )
            assert failed.value.translation_key == "upstream_timeout"
            assert isinstance(failed.value.__cause__, RunBudgetError)
        before = len(service.requests)
        now["value"] = service.krakens[ACTIVE].now = later + 2 * HOUR
        with pytest.raises(UpdateFailed) as waiting:
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert len(service.requests) == before
    assert waiting.value.translation_key == "upstream_timeout"
    assert waiting.value.translation_placeholders == {
        "detail": "the update's time limit ran out"
    }
    (unit,) = coordinator.unit_diagnostics()
    assert (unit["status"], unit["last_failure"]["reason"]) == ("failed", "time_budget")


@pytest.mark.parametrize(
    ("error", "raised"),
    [
        (MeridianAuthenticationError("synthetic"), ConfigEntryAuthFailed),
        (MeridianRateLimitError(120), UpdateFailed),
    ],
)
@pytest.mark.asyncio
async def test_a_rejected_session_or_a_rate_limit_stops_the_whole_sync(
    hass, error: Exception, raised: type[Exception]
) -> None:
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    service.fail = lambda request: error if request.property_id == ENDED else None
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE))
    )

    with _running(clock) as importer, pytest.raises(raised):
        await coordinator.async_fetch_and_import()

    assert [request.property_id for request in service.requests] == [ENDED]
    importer.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_time_budget_stops_every_walk_and_publishes_what_imported(
    hass,
) -> None:
    """
    No request starts once less than 80 s of an update's 240 s remain.

    Both properties' newest weeks come first; the first walks on, and the
    second, stopped by the budget after its newest week, imports that week:
    it reaches the newest hour stored, and the unit owes the rest.
    """
    clock = _Clock()
    service = _service(clock, ACTIVE, ENDED)
    service.latency = 50.0
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ENDED, ACTIVE))
    )

    with _running(clock, latest=NOW - 2 * HOUR) as importer:
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert data.sync_mode is SyncMode.RESTART
    assert [request.property_id for request in service.requests] == [
        ENDED,
        ACTIVE,
        ENDED,
        ENDED,
    ]
    assert data.units_degraded == 1
    assert importer.await_count == 2
    assert _imported_hours(importer)[0] >= NOW - 7 * DAY


@pytest.mark.asyncio
async def test_the_time_budget_fails_an_update_that_imported_nothing(hass) -> None:
    """Its newest week takes two pages of 830 s: the second never starts."""
    clock = _Clock()
    service = _service(clock)
    service.latency = 830.0
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with (
        _running(clock),
        patch.object(coordinator_module, "page_size", return_value=100),
        pytest.raises(UpdateFailed) as raised,
    ):
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert raised.value.translation_key == "upstream_timeout"
    assert raised.value.translation_placeholders == {
        "detail": "the update's time limit ran out"
    }
    assert raised.value.retry_after == FIRST_SYNC_RETRY_SCHEDULE[0]
    assert isinstance(raised.value.__cause__, RunBudgetError)


@pytest.mark.asyncio
async def test_hourly_updates_have_a_240_second_budget(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    with patch.object(
        coordinator, "async_fetch_and_import", AsyncMock(return_value="data")
    ) as fetch:
        assert await coordinator._async_update_data() == "data"
    fetch.assert_awaited_once_with(time_budget=240)


@pytest.mark.asyncio
async def test_a_failing_first_sync_waits_longer_each_time(hass) -> None:
    clock = _Clock()
    service = _service(clock)
    service.fail = lambda _request: MeridianConnectionError("synthetic")
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    waits = []
    with _running(clock):
        for _ in range(4):
            with pytest.raises(UpdateFailed) as raised:
                await coordinator.async_fetch_and_import()
            waits.append(raised.value.retry_after)

    assert waits == [600, 1800, 3600, 3600]


@pytest.mark.asyncio
async def test_a_rate_limit_in_polling_waits_twice_as_long_each_time_in_a_row(
    hass,
) -> None:
    clock = _Clock()
    service = _service(clock)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    waits = []
    with _running(clock, latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import()
        service.fail = lambda _request: MeridianRateLimitError(3600)
        for _ in range(7):
            with pytest.raises(UpdateFailed) as raised:
                await coordinator.async_fetch_and_import()
            waits.append(raised.value.retry_after)
        service.fail = lambda _request: None
        await coordinator.async_fetch_and_import()
        service.fail = lambda _request: MeridianRateLimitError(3600)
        with pytest.raises(UpdateFailed) as again:
            await coordinator.async_fetch_and_import()

    assert waits == [3600, 7200, 14400, 28800, 57600, 86400, 86400]
    assert again.value.retry_after == 3600


# ----------------------------------------------------------- the frontier


@pytest.mark.parametrize(
    ("mode", "frontier", "since"),
    [
        pytest.param(SyncMode.TIP, NOW - 2 * HOUR, NOW - DAY, id="tip-fresh"),
        pytest.param(SyncMode.TIP, NOW - 20 * DAY, NOW - 20 * DAY, id="tip-stale"),
        pytest.param(
            SyncMode.FULL_RECONCILIATION,
            NOW - 20 * DAY,
            NOW - 20 * DAY,
            id="full-stale",
        ),
        pytest.param(
            SyncMode.FULL_RECONCILIATION, NOW - 100 * DAY, NOW - 90 * DAY, id="capped"
        ),
        pytest.param(SyncMode.INITIAL, NOW - 20 * DAY, NOW - 90 * DAY, id="initial"),
    ],
)
def test_every_modes_cutoff_reaches_back_to_the_frontier(
    hass, mode: SyncMode, frontier: datetime, since: datetime
) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    key = property_key(ACCOUNT, ACTIVE)
    coordinator._frontier[consumption_ids(key)[0]] = frontier
    coordinator._frontier[generation_ids(key)[0]] = NOW - 5 * DAY

    assert coordinator._requested_since((key, "CONSUMPTION"), mode, NOW) == since


@pytest.mark.parametrize(
    ("mode", "frontier", "cost_frontier", "since"),
    [
        # Within the mode's own span, the cost frontier changes nothing.
        pytest.param(
            SyncMode.TIP, NOW - 2 * HOUR, NOW - 8 * HOUR, NOW - DAY, id="tip-lagging"
        ),
        pytest.param(
            SyncMode.TIP, NOW - 2 * HOUR, NOW - 60 * DAY, NOW - DAY, id="tip-stopped"
        ),
        # Moved back to the frontier, the walk reaches the cost's too.
        pytest.param(
            SyncMode.TIP,
            NOW - 20 * DAY,
            NOW - 20 * DAY - 6 * HOUR,
            NOW - 20 * DAY - 6 * HOUR,
            id="stale-lagging",
        ),
        # A cost statistic that stopped for good: 48 h at most.
        pytest.param(
            SyncMode.FULL_RECONCILIATION,
            NOW - 20 * DAY,
            NOW - 60 * DAY,
            NOW - 22 * DAY,
            id="stale-stopped",
        ),
        pytest.param(
            SyncMode.FULL_RECONCILIATION,
            NOW - 20 * DAY,
            NOW - 2 * HOUR,
            NOW - 20 * DAY,
            id="cost-newer",
        ),
        pytest.param(
            SyncMode.FULL_RECONCILIATION,
            NOW - 89 * DAY,
            NOW - 91 * DAY,
            NOW - 90 * DAY,
            id="capped",
        ),
    ],
)
def test_a_cutoff_moved_back_to_the_frontier_reaches_the_cost_frontier(
    hass, mode: SyncMode, frontier: datetime, cost_frontier: datetime, since: datetime
) -> None:
    """Meridian may publish an hour's cost after its usage: those hours are asked."""
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    energy_id, cost_id = consumption_ids(GRID_IMPORT[0])
    coordinator._frontier[energy_id] = frontier
    coordinator._frontier[cost_id] = cost_frontier

    assert coordinator._requested_since(GRID_IMPORT, mode, NOW) == since


@pytest.mark.parametrize("cost_rows", [1, 0])
@pytest.mark.asyncio
async def test_an_import_moves_the_cost_frontier_to_its_newest_costed_hour(
    hass, cost_rows: int
) -> None:
    """
    The cost frontier moves on with the hours whose cost was written.

    Meridian has published the cost of every hour but the newest 6. Nothing
    moves it when the importer wrote no cost row.
    """
    clock = _Clock()
    service = _service(clock)
    kraken = service.krakens[ACTIVE]
    newest_costed = NOW.replace(minute=0) - 7 * HOUR
    kraken.cost = lambda start, _channel: None if start > newest_costed else "15"
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))
    energy_id, cost_id = consumption_ids(GRID_IMPORT[0])
    stored = NOW - 10 * HOUR

    with _running(
        clock, latest=stored, importer=AsyncMock(return_value=(1, cost_rows))
    ) as importer:
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    newest = max(_imported_hours(importer))
    assert coordinator._frontier[energy_id] == newest
    assert coordinator._frontier[cost_id] == (newest_costed if cost_rows else stored)


@pytest.mark.asyncio
async def test_a_walk_that_stops_short_of_the_frontier_imports_its_newer_weeks(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Stale statistics, and the third week fails: the two newer weeks are kept.

    Until now nothing was imported until the walk reached the newest stored
    hour. The days between are left to the reconciliation the unit owes and
    to its lane's coverage, which waits an hour before it asks for the
    failed week's older days, one at a time.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    service = _service(clock)
    failing = TODAY - 20 * DAY
    service.fail = lambda request: (
        MeridianConnectionError("synthetic") if request.start_on == failing else None
    )
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))
    energy_id, _ = consumption_ids(GRID_IMPORT[0])

    with _running(clock, latest=NOW - 20 * DAY) as importer:
        data = await coordinator.async_fetch_and_import()

    assert data.units_degraded == 1
    assert _imported_hours(importer)[0] == local_midnight(TODAY - 13 * DAY)
    assert coordinator._frontier[energy_id] == max(_imported_hours(importer))
    lane = coordinator._lanes[GRID_IMPORT]
    fourteen = (NOW - 14 * DAY).astimezone(NZ).date()
    assert set(lane.bad) == {
        failing + offset * DAY
        for offset in range(7)
        if failing + offset * DAY < fourteen
    }
    assert all(retry == NOW + HOUR for retry in lane.bad.values())
    assert any(
        message.startswith("Meridian's older usage history did not come back")
        for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.asyncio
async def test_a_week_that_fails_its_checks_does_not_reach_the_frontier(hass) -> None:
    """
    20 days after the newest stored hour, the week holding it fails its checks.

    Its rows were dropped, so the two newer weeks alone do not reach that
    hour. They are imported all the same: the replay writes the days
    between later, behind them, as the unit's lane asks for them. The
    frontier moves on to the newest hour imported.
    """
    clock = _Clock()
    frontier = (NOW - 20 * DAY).replace(minute=0)
    service = _service(clock, gaps=_dates_missing(TODAY - 7 * DAY, TODAY - 14 * DAY))
    coordinator = MeridianDataCoordinator(
        hass,
        _client(service, _account()),
        topology=(_account(),),
        topology_at=NOW - HOUR,
        reconciled_at=NOW - HOUR,
    )
    energy_id, _ = consumption_ids(property_key(ACCOUNT, ACTIVE))

    with _running(clock, latest=frontier) as importer:
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    # The hourly update reaches back to the frontier, in the third week.
    assert len(service.requests) == 3
    assert data.units_degraded == 1
    assert _imported_hours(importer)[0] == local_midnight(TODAY - 13 * DAY)
    assert coordinator._frontier[energy_id] == max(_imported_hours(importer))
    (unit,) = coordinator.unit_diagnostics()
    assert (unit["status"], unit["last_failure"]["reason"]) == (
        "degraded",
        "invalid_data",
    )


# ---------------------------------------------------------- the cache (E3)


@pytest.mark.asyncio
async def test_the_cache_is_unchanged_after_an_import_raises(hass) -> None:
    """
    Rows an import that failed was given are offered again by the next update.

    The cache holds what was last imported: committed before the import
    returned, a failed import's rows would read as unchanged next time and
    never be written.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, now=NOW + HOUR)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))
    key = (property_key(ACCOUNT, ACTIVE), "CONSUMPTION")
    importer = AsyncMock(return_value=(1, 1))

    # The hour that ends after the first update, before the second.
    new_hour = NOW.replace(minute=0)
    with _running(
        clock, lambda: now["value"], latest=NOW - 2 * HOUR, importer=importer
    ):
        await coordinator.async_fetch_and_import()
        cached = dict(coordinator._measurement_cache[key])
        assert all(hour < new_hour for hour, _channel in cached)
        importer.side_effect = ValueError("Meridian measurement is not aligned")
        now["value"] = NOW + HOUR
        with pytest.raises(UpdateFailed):
            await coordinator.async_fetch_and_import()
        assert new_hour in _imported_hours(importer)
        assert coordinator._measurement_cache[key] == cached
        importer.side_effect = None
        importer.return_value = (1, 1)
        now["value"] = NOW + 2 * HOUR
        await coordinator.async_fetch_and_import()

    assert new_hour in _imported_hours(importer)
    assert any(
        hour == new_hour for hour, _channel in coordinator._measurement_cache[key]
    )


# --------------------------------------------- what setup hands its entry


@pytest.mark.asyncio
async def test_a_handed_over_topology_is_used_without_a_request(hass) -> None:
    clock = _Clock()
    service = _service(clock)
    client = _client(service, _account())
    coordinator = MeridianDataCoordinator(
        hass,
        client,
        selected_accounts=frozenset({ACCOUNT}),
        topology=(_account(),),
        topology_at=NOW,
    )

    with _running(clock):
        await coordinator.async_prepare_topology()
        accounts, refreshed = await coordinator._async_get_topology(NOW)

    client.async_get_accounts.assert_not_awaited()
    assert accounts == (_account(),)
    assert not refreshed


@pytest.mark.asyncio
async def test_a_topology_error_refreshes_a_handed_over_topology(hass) -> None:
    clock = _Clock()
    service = _service(clock)
    errors = [MeridianGraphQLError("measurements", ("PROPERTY_NOT_FOUND",))]
    service.fail = lambda _request: errors.pop() if errors else None
    client = _client(service, _account())
    coordinator = MeridianDataCoordinator(hass, client, topology=(_account(),))

    with _running(clock):
        data = await coordinator.async_fetch_and_import(setup_import=True)

    client.async_get_accounts.assert_awaited_once()
    assert data.topology_refreshed


@pytest.mark.asyncio
async def test_the_first_sync_after_setup_continues_with_the_hourly_update(
    hass,
) -> None:
    """Setup's import counts as the last reconciliation: one request, not 3."""
    clock = _Clock()
    service = _service(clock)
    client = _client(service, _account())
    coordinator = MeridianDataCoordinator(
        hass,
        client,
        topology=(_account(),),
        topology_at=NOW - HOUR,
        reconciled_at=NOW - HOUR,
    )

    with _running(clock, latest=NOW - 2 * HOUR):
        data = await coordinator.async_fetch_and_import()

    assert data.sync_mode is SyncMode.TIP
    assert len(service.requests) == 1
    client.async_get_accounts.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_first_sync_after_setup_imports_a_direction_setup_could_not(
    hass,
) -> None:
    """A direction without statistics is imported in full, as before."""
    clock = _Clock()
    service = _service(clock)
    coordinator = MeridianDataCoordinator(
        hass,
        _client(service, _account()),
        topology=(_account(),),
        reconciled_at=NOW - HOUR,
    )

    with _running(clock):
        data = await coordinator.async_fetch_and_import()

    assert data.sync_mode is SyncMode.INITIAL
    assert len(service.requests) == 13


@pytest.mark.parametrize(
    ("empty", "kraken"),
    [
        pytest.param((), {"withdrawn_from": NOW - 100 * DAY}, id="supply-ended"),
        pytest.param(("GENERATION",), {}, id="never-exported"),
    ],
)
@pytest.mark.asyncio
async def test_the_first_sync_after_setup_does_not_search_an_empty_direction_again(
    hass,
    caplog: pytest.LogCaptureFixture,
    empty: tuple[str, ...],
    kraken: dict[str, Any],
) -> None:
    """
    A direction setup searched in full and found empty gets the hourly update.

    Setup hands the entry the directions it searched, so the entry's first
    sync sends one request for each instead of the same walk again, and says
    no second time that it found nothing. The grid export that never
    exported is on a property whose grid import setup imported.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    service = _service(clock, **kraken)
    service.empty = frozenset(empty)
    feed_in = "GENERATION" in empty
    account = _account(feed_in=feed_in)
    setup = MeridianDataCoordinator(
        hass, _client(service, account), topology=(account,)
    )
    with _running(clock):
        await setup.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )
    stored = setup.imported_newest
    service.requests.clear()
    entry = MeridianDataCoordinator(
        hass,
        _client(service, account),
        topology=(account,),
        topology_at=NOW,
        reconciled_at=NOW,
        memory=setup.memory,
    )

    with _running(clock, NOW + 5 * timedelta(minutes=1), latest=stored.get):
        data = await entry.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert data.sync_mode is SyncMode.TIP
    assert [request.direction for request in service.requests] == (
        ["CONSUMPTION", "GENERATION"] if feed_in else ["CONSUMPTION"]
    )
    assert (
        sum(
            message.startswith("No Meridian usage history was found")
            for message in _messages(caplog, logging.INFO)
        )
        == 1
    )


@pytest.mark.parametrize("ended_days", [None, 30], ids=["active", "ended"])
@pytest.mark.asyncio
async def test_setup_leaves_a_direction_quiet_only_when_its_newest_week_was_empty(
    hass, ended_days: int | None
) -> None:
    """
    Setup has no frontier to compare with: its newest week decides.

    A supply whose readings ended 30 days ago has an empty newest week, so
    setup leaves it quiet, and the entry's first update asks for that week
    alone rather than walk back 30 days to its newest reading. An active
    supply's readings are new: it is not quiet, and its first update asks
    for its last two days.
    """
    clock = _Clock()
    service = _service(
        clock,
        **(
            {}
            if ended_days is None
            else {"withdrawn_from": NOW.replace(minute=0) - ended_days * DAY}
        ),
    )
    account = _account()
    setup = MeridianDataCoordinator(
        hass, _client(service, account), topology=(account,)
    )
    with _running(clock):
        await setup.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )
    memory = setup.memory
    assert (memory.unit_states[GRID_IMPORT].quiet_frontier is not None) is (
        ended_days is not None
    )
    stored = setup.imported_newest
    service.requests.clear()
    entry = MeridianDataCoordinator(
        hass,
        _client(service, account),
        topology=(account,),
        topology_at=NOW,
        reconciled_at=NOW,
        memory=memory,
    )

    with _running(clock, NOW + 5 * timedelta(minutes=1), latest=stored.get):
        data = await entry.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert data.sync_mode is SyncMode.TIP
    assert [request.days for request in service.requests] == (
        [2] if ended_days is None else [7]
    )


# ----------------------------------------------------------- diagnostics


def test_unit_diagnostics_without_a_topology_are_empty(hass) -> None:
    coordinator = MeridianDataCoordinator(
        hass, MagicMock(), selected_accounts=frozenset({"A-TEST-2"})
    )
    coordinator._topology = (_account(),)
    assert coordinator.unit_diagnostics() == []


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (RunBudgetError(), "time_budget"),
        (_gateway_timeout(), "upstream_timeout"),
        (MeridianUpstreamError(503, gateway=False), "upstream_unavailable"),
        (MeridianConnectionError("synthetic"), "cannot_connect"),
        (MeridianGraphQLError("measurements", ("KT-CT-1188",)), "invalid_data"),
        (ValueError("synthetic"), "invalid_data"),
    ],
)
def test_a_unit_failure_is_named_as_the_sync_would_name_it(
    error: Exception, reason: str
) -> None:
    failure = MeridianDataCoordinator._unit_failure(error, NOW)
    assert failure.reason == reason
    assert failure.cause == type(error).__name__
    assert failure.at == NOW


@pytest.mark.asyncio
async def test_a_failing_update_still_names_the_unit_once(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    An hourly update whose only unit fails warns once, whatever comes next.

    Home Assistant logs the failed update itself, once; the warning names
    which direction of which property, and stays the only one while the
    unit keeps failing.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import()
        service.fail = lambda _request: _gateway_timeout()
        for hours in (1, 2, 4):
            now["value"] = NOW + hours * HOUR
            with pytest.raises(UpdateFailed):
                await coordinator.async_fetch_and_import()

    (warning,) = _messages(caplog, logging.WARNING)
    assert "direction=CONSUMPTION account=1/1 property=1/1" in warning
    assert warning.endswith("tried again in about 1 h")


@pytest.mark.parametrize(
    "stop",
    [
        pytest.param(MeridianRateLimitError(3600), id="rate-limit"),
        pytest.param(
            MeridianGraphQLError("measurements", ("PROPERTY_NOT_FOUND",)),
            id="missing-property",
        ),
    ],
)
@pytest.mark.asyncio
async def test_a_unit_that_starts_failing_warns_although_the_sync_then_stops(
    hass, caplog: pytest.LogCaptureFixture, stop: Exception
) -> None:
    """
    An hourly update logs the warning when the unit fails, not at its end.

    The ended property's data is unusable; then the active one's second
    week meets a rate limit, which stops the update, or a missing property,
    which refreshes the accounts and syncs again. Either way the ended
    property's one warning is logged, and its later failures log no second
    one.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, ENDED, ACTIVE)
    stopping = [stop]

    def fail(request: _Request) -> BaseException | None:
        if request.property_id == ENDED:
            return ValueError("synthetic unusable data")
        if request.start_on > TODAY - 7 * DAY:
            return None
        return stopping.pop() if stopping else None

    service.fail = fail
    account = _account(ENDED, ACTIVE)
    coordinator = MeridianDataCoordinator(
        hass, _client(service, account), topology=(account,)
    )

    failed: list[tuple[int, str | None]] = []
    with _running(clock, lambda: now["value"]):
        for hours in (0, 2, 5, 12):
            now["value"] = NOW + hours * HOUR
            try:
                # No time budget: each failure takes 60 s here.
                await coordinator.async_fetch_and_import()
            except UpdateFailed as err:
                failed.append((hours, err.translation_key))

    rate_limited = isinstance(stop, MeridianRateLimitError)
    assert failed == ([(0, "rate_limited")] if rate_limited else [])
    assert [unit["status"] for unit in coordinator.unit_diagnostics()] == [
        "failed",
        "ok",
    ]
    (warning,) = _messages(caplog, logging.WARNING)
    assert "direction=CONSUMPTION account=1/1 property=1/2" in warning


@pytest.mark.asyncio
async def test_setup_warns_for_a_unit_that_failed_before_the_accounts_were_refreshed(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A missing property makes setup sync again: a unit failing twice warns once.

    It failed before the refresh, and fails again after it; once setup is
    known to go on, its warning is logged.
    """
    clock = _Clock()
    service = _service(clock, ENDED, ACTIVE)
    missing = [MeridianGraphQLError("measurements", ("PROPERTY_NOT_FOUND",))]

    def fail(request: _Request) -> BaseException | None:
        if request.property_id == ENDED:
            return ValueError("synthetic unusable data")
        if request.start_on > TODAY - 7 * DAY:
            return None
        return missing.pop() if missing else None

    service.fail = fail
    account = _account(ENDED, ACTIVE)
    client = _client(service, account)
    coordinator = MeridianDataCoordinator(hass, client, topology=(account,))

    with _running(clock):
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    client.async_get_accounts.assert_awaited_once()
    assert data.units_failed == 1
    (warning,) = _messages(caplog, logging.WARNING)
    assert "direction=CONSUMPTION account=1/1 property=1/2" in warning
    assert warning.endswith("tried again by the first update after setup")


@pytest.mark.asyncio
async def test_a_failed_unit_that_imports_its_newer_weeks_says_it_works_again(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A unit that failed and then imported its newer weeks logs one INFO line.

    The weekly reconciliation fails on its newest week: one warning, and the
    unit waits an hour. Tried again, its newest week comes back and its
    second does not: it is degraded, its hourly updates go on, and one INFO
    line says it works again, so the warning is not left standing.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        coordinator._last_full_reconciliation = NOW - 8 * DAY
        service.fail = lambda _request: MeridianConnectionError("synthetic")
        now["value"] = NOW + HOUR
        with pytest.raises(UpdateFailed):
            await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
        service.fail = lambda request: (
            MeridianConnectionError("synthetic") if request.end_on < TODAY else None
        )
        now["value"] = NOW + 2 * HOUR
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert data.units_degraded == 1
    assert len(_messages(caplog, logging.WARNING)) == 1
    recovered = [
        message
        for message in _messages(caplog, logging.INFO)
        if message.startswith("Meridian usage history updates for one direction")
    ]
    assert len(recovered) == 1


@pytest.mark.asyncio
async def test_setup_says_nothing_of_a_failure_its_second_sync_left_degraded(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A unit that failed before the accounts were refreshed, then was degraded.

    Its statistics end 10 days ago. Before the refresh its newest week
    fails, and it imports nothing; after it, its oldest date fails, and its
    two weeks are imported. Its failure never warned, so it warns no more
    once setup goes on, and logs no line that it works again either. Until
    now setup warned for it, as if it still failed.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    service = _service(clock, ENDED, ACTIVE)
    missing = [MeridianGraphQLError("measurements", ("PROPERTY_NOT_FOUND",))]

    def fail(request: _Request) -> BaseException | None:
        if request.property_id == ENDED:
            failing = TODAY if missing else TODAY - 14 * DAY
            return ValueError("synthetic") if request.end_on == failing else None
        if request.start_on > TODAY - 7 * DAY or not missing:
            return None
        return missing.pop()

    service.fail = fail
    account = _account(ENDED, ACTIVE)
    client = _client(service, account)
    coordinator = MeridianDataCoordinator(hass, client, topology=(account,))
    ended_id, _ = consumption_ids(property_key(ACCOUNT, ENDED))

    def latest(statistic_id: str) -> datetime | None:
        return NOW - 10 * DAY if statistic_id == ended_id else None

    with _running(clock, latest=latest) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    client.async_get_accounts.assert_awaited_once()
    assert (data.units_failed, data.units_degraded) == (0, 1)
    assert importer.await_count == 2
    assert _messages(caplog, logging.WARNING) == []
    assert not any(
        "are working again" in message for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.asyncio
async def test_setup_says_nothing_of_a_failure_its_second_sync_put_right(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A unit that failed before the accounts were refreshed, then imported.

    Its failure never warned, so it logs no line that it works again either.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    clock = _Clock()
    service = _service(clock, ENDED, ACTIVE)
    missing = [MeridianGraphQLError("measurements", ("PROPERTY_NOT_FOUND",))]

    def fail(request: _Request) -> BaseException | None:
        if not missing:
            return None
        if request.property_id == ENDED:
            return ValueError("synthetic unusable data")
        if request.start_on > TODAY - 7 * DAY:
            return None
        return missing.pop()

    service.fail = fail
    account = _account(ENDED, ACTIVE)
    client = _client(service, account)
    coordinator = MeridianDataCoordinator(hass, client, topology=(account,))

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    client.async_get_accounts.assert_awaited_once()
    assert data.units_failed == 0
    assert importer.await_count == 2
    assert _messages(caplog, logging.WARNING) == []
    assert not any(
        "are working again" in message for message in _messages(caplog, logging.INFO)
    )


@pytest.mark.asyncio
async def test_a_failed_setup_attempt_leaves_the_warning_to_the_flow(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    clock = _Clock()
    service = _service(clock)
    service.fail = lambda _request: _gateway_timeout()
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock), pytest.raises(UpdateFailed):
        await coordinator.async_fetch_and_import(setup_import=True)

    assert _messages(caplog, logging.WARNING) == []


@pytest.mark.asyncio
async def test_a_unit_the_budget_reached_before_its_first_request_did_not_fail(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    It waits for the next update unchanged: no warning, and no backoff.

    The first property's hourly update takes 170 s, so less than 80 s of the
    240 s are left before the second's first request could start.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock, ACTIVE, ENDED)
    coordinator = MeridianDataCoordinator(
        hass, _client(service, _account(ACTIVE, ENDED))
    )

    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import()
        service.requests.clear()
        service.latency = 170.0
        now["value"] = NOW + HOUR
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert [request.property_id for request in service.requests] == [ACTIVE]
    assert data.units_deferred == 1
    assert data.units_failed == 0
    assert _messages(caplog, logging.WARNING) == []
    assert [unit["status"] for unit in coordinator.unit_diagnostics()] == ["ok", "ok"]
