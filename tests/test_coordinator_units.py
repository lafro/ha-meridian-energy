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
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy import sensor as sensor_module
from custom_components.meridian_energy.api import (
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianGraphQLError,
    MeridianRateLimitError,
    MeridianUpstreamError,
)
from custom_components.meridian_energy.const import (
    DOMAIN,
    FIRST_SYNC_RETRY_SCHEDULE,
    INITIAL_BACKFILL,
    POLL_BUDGET_SECONDS,
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
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert len(service.requests) == 13
    assert all(request.days in {7, 6, 1} for request in service.requests)
    assert data.splits == 0
    assert _imported_hours(importer) == _expected_hours(
        service.krakens[ACTIVE], NOW - INITIAL_BACKFILL
    )


@pytest.mark.asyncio
async def test_a_week_that_fails_its_checks_is_left_out_of_a_partial_import(
    hass,
) -> None:
    """
    A week whose rows failed their checks was dropped: the newer weeks are kept.

    The second and third weeks have no readings for their last date, which
    two weeks in a row fail as Meridian leaving the end date out would. So
    the update imports the two newest weeks, and later updates ask for the
    rest.
    """
    clock = _Clock()
    service = _service(clock, gaps=_dates_missing(TODAY - 7 * DAY, TODAY - 14 * DAY))
    coordinator = MeridianDataCoordinator(hass, _client(service, _account()))

    with _running(clock) as importer:
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

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
    Both are degraded, and the update succeeds: each imported its newer weeks.
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
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

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

    At 64 s a request, 4 minutes allow 3 requests: the newest week of the
    ended property, or of the export register that never exported, and 2
    weeks of the active grid import. The update keeps the 2 weeks, and later
    updates ask for the rest. The deep walk's own budget is left out, so the
    4 minutes hold.
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

    with (
        _running(clock) as importer,
        patch.object(coordinator_module, "DEEP_WALK_BUDGET_SECONDS", 0),
    ):
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    assert len(service.requests) == 3
    assert data.units_failed == 0
    assert data.units_degraded == 2
    importer.assert_awaited_once()
    assert _imported_hours(importer) == _expected_hours(
        service.krakens[ACTIVE], local_midnight(TODAY - 13 * DAY)
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
        await coordinator.async_fetch_and_import()

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
async def test_a_reload_asks_again_at_once_for_a_waiting_unit(hass) -> None:
    """
    A unit's wait is kept in memory only, so a reload or restart ends it.

    Either builds a new coordinator with no setup hand-off (test_init's reload
    test), whose first update asks Meridian at once.
    """
    clock = _Clock()
    now = {"value": NOW}
    service = _service(clock)
    client = _client(service, _account())
    coordinator = MeridianDataCoordinator(hass, client)

    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR):
        await coordinator.async_fetch_and_import()
        service.fail = lambda _request: _gateway_timeout()
        for hours in (1, 2):
            now["value"] = NOW + hours * HOUR
            with pytest.raises(UpdateFailed):
                await coordinator.async_fetch_and_import()
        before = len(service.requests)
        now["value"] = NOW + 3 * HOUR
        with pytest.raises(UpdateFailed):
            await coordinator.async_fetch_and_import()
        assert len(service.requests) == before

        service.fail = lambda _request: None
        reloaded = MeridianDataCoordinator(hass, client)
        data = await reloaded.async_fetch_and_import()

    assert len(service.requests) > before
    assert data.windows_ok > 0


@pytest.mark.parametrize("failing", ["property", "export"])
@pytest.mark.asyncio
async def test_one_failing_unit_leaves_the_updates_to_the_others(
    hass, failing: str
) -> None:
    """
    A second property, or a property's export, that keeps failing fails nothing.

    Every update succeeds, so every entity stays available, the failing
    unit's too, and the healthy unit imports its newest hours at each of
    them while the failing one waits out its failures.
    """
    second = "synthetic-second-property"
    clock = _Clock()
    now = {"value": NOW}
    if failing == "property":
        service = _service(clock, ACTIVE, second)
        account = _account(ACTIVE, second)

        def broken(request: _Request) -> bool:
            return request.property_id == second

    else:
        service = _service(clock)
        account = _account(feed_in=True)

        def broken(request: _Request) -> bool:
            return request.direction == "GENERATION"

    coordinator = MeridianDataCoordinator(hass, _client(service, account))
    entry = MockConfigEntry(domain=DOMAIN, title="person@example.com")
    entry.runtime_data = SimpleNamespace(coordinator=coordinator, client=MagicMock())
    entities: list[Any] = []

    with _running(clock, lambda: now["value"], latest=NOW - 2 * HOUR) as importer:
        await coordinator.async_refresh()
        await sensor_module.async_setup_entry(hass, entry, entities.extend)
        assert all(entity.available for entity in entities)
        service.fail = lambda request: _gateway_timeout() if broken(request) else None
        seen = []
        for hours in (1, 2, 3):
            now["value"] = NOW + hours * HOUR
            imports = importer.await_count
            asked = len([request for request in service.requests if broken(request)])
            await coordinator.async_refresh()
            data = coordinator.data
            seen.append(
                (
                    [
                        call.kwargs["stat_energy_id"]
                        for call in importer.await_args_list[imports:]
                    ],
                    len([request for request in service.requests if broken(request)])
                    > asked,
                    data.units_failed,
                    data.units_deferred,
                    coordinator.last_update_success,
                    [
                        entity.entity_description.key
                        for entity in entities
                        if not entity.available
                    ],
                )
            )
    await entry._async_process_on_unload(hass)

    # Failed at 1 and 2 h, the failing unit then waits 2 h and asks nothing.
    healthy = [f"meridian_energy:consumption_{property_key(ACCOUNT, ACTIVE)}"]
    assert seen == [
        (healthy, True, 1, 0, True, []),
        (healthy, True, 1, 0, True, []),
        (healthy, False, 0, 1, True, []),
    ]
    assert len(entities) == (8 if failing == "property" else 10)


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
    coordinator = _hourly(
        MeridianDataCoordinator(hass, _client(service, _account())), _account()
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


# ------------------------------------------------------------ the accounts


def _cached(
    coordinator: MeridianDataCoordinator, *accounts: MeridianAccount
) -> MeridianDataCoordinator:
    """Cache accounts as if the coordinator had fetched them at ``NOW``."""
    coordinator._topology = accounts
    coordinator._topology_cached_at = NOW
    return coordinator


def _hourly(
    coordinator: MeridianDataCoordinator, *accounts: MeridianAccount
) -> MeridianDataCoordinator:
    """Make the next sync an hourly update, an hour after a reconciliation."""
    _cached(coordinator, *accounts)
    coordinator._initial_refresh_complete = True
    coordinator._last_full_reconciliation = NOW - HOUR
    coordinator._last_targeted_reconciliation = NOW - HOUR
    return coordinator


@pytest.mark.asyncio
async def test_a_topology_error_refreshes_cached_accounts(hass) -> None:
    clock = _Clock()
    service = _service(clock)
    errors = [MeridianGraphQLError("measurements", ("PROPERTY_NOT_FOUND",))]
    service.fail = lambda _request: errors.pop() if errors else None
    client = _client(service, _account())
    coordinator = _cached(MeridianDataCoordinator(hass, client), _account())

    with _running(clock):
        data = await coordinator.async_fetch_and_import()

    client.async_get_accounts.assert_awaited_once()
    assert data.topology_refreshed


# ----------------------------------------------------------- diagnostics


def test_unit_diagnostics_without_accounts_are_empty(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
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
    coordinator = _cached(
        MeridianDataCoordinator(hass, _client(service, account)), account
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
