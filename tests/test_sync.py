"""
The sync: its request sets, the fill, cooldowns, rate limits, outcomes, messages.

Each test runs the real coordinator, client and transport in front of a
synthetic Meridian service at the HTTP level (``meridian_http``), with the
statistics kept in memory (``syncs.Statistics``). Every value is synthetic.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from aiohttp import ClientConnectionError
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import translation
from homeassistant.helpers.update_coordinator import UpdateFailed
from sqlalchemy.exc import OperationalError

from custom_components.meridian_energy.const import DOMAIN, UPDATE_INTERVAL
from custom_components.meridian_energy.coordinator import _Memory
from custom_components.meridian_energy.statistics import (
    consumption_ids,
    generation_ids,
    property_key,
)
from custom_components.meridian_energy.windows import local_midnight, nz_date

from .kraken_fake import FakeKraken
from .meridian_http import (
    ACCOUNT_NUMBER,
    FakeMeridianSession,
    Response,
    account_data,
    json_response,
    property_id,
)
from .syncs import (
    DAY,
    HOUR,
    NOW,
    TARGET,
    TODAY,
    Statistics,
    coordinator_for,
    hours_of,
    measurement_dates,
    refresh,
    sync,
)

_INTEGRATION = "custom_components.meridian_energy"
KEY = property_key(ACCOUNT_NUMBER, property_id(ACCOUNT_NUMBER))
ENERGY_ID, COST_ID = consumption_ids(KEY)
EXPORT_ID, CREDIT_ID = generation_ids(KEY)
_HTML = {"Content-Type": "text/html"}
_GATEWAY_PAGE = Response(b"<html>gateway</html>", status=504, headers=_HTML)
_STOPPED = (
    "Meridian Energy stopped filling in older grid import history until Home "
    "Assistant restarts: Meridian refused a request for older dates"
)


def _rate_limit(seconds: str = "120") -> Response:
    return Response(b"", status=429, headers={"Retry-After": seconds})


def _coded(code: str) -> Response:
    return json_response({"errors": [{"extensions": {"errorCode": code}}]})


@pytest.fixture(autouse=True)
def statistics(recorder_stand_in: SimpleNamespace) -> Statistics:
    """Keep what each test writes in memory, by statistic and hour."""
    return Statistics().attach(recorder_stand_in)


@pytest.fixture(autouse=True)
async def translations(hass: HomeAssistant) -> None:
    """Load the integration's texts, so failures read as users see them."""
    hass.config.components.add(DOMAIN)
    await translation.async_load_integrations(hass, {DOMAIN})


def _own(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name.startswith(_INTEGRATION) and record.levelno >= level
    ]


def _fill_windows(first: Any, last: Any) -> list[tuple[Any, Any]]:
    """Return the fill windows for the dates from ``last`` back to ``first``."""
    windows = []
    end = last
    while end >= first:
        start = max(first, end - 6 * DAY)
        windows.append((start, end))
        end = start - DAY
    return windows


async def test_a_first_sync_asks_for_21_recent_dates_then_fills_90_days(
    hass: HomeAssistant, statistics: Statistics, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW)
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    recent = _fill_windows(TODAY - 20 * DAY, TODAY)
    assert measurement_dates(session) == recent + _fill_windows(
        TARGET, TODAY - 21 * DAY
    )
    assert len(recent) == 3
    assert data.requests == 13
    (written,) = statistics.writes
    expected = [
        hour for hour in hours_of([*_days(TARGET, TODAY)]) if hour < NOW - HOUR / 2
    ]
    assert sorted(written[ENERGY_ID]) == expected
    assert sorted(written[COST_ID]) == expected
    assert set(written[ENERGY_ID].values()) == {Decimal("0.5")}
    # Cents become dollars.
    assert set(written[COST_ID].values()) == {Decimal("0.15")}
    assert _own(caplog, logging.INFO) == []
    # Every missing date came back in this sync.
    assert coordinator.unit_diagnostics()[0]["history_complete"] is True


def _days(first: Any, last: Any) -> list[Any]:
    return [first + offset * DAY for offset in range((last - first).days + 1)]


async def test_later_updates_ask_for_3_dates_and_the_daily_one_for_21_again(
    hass: HomeAssistant, statistics: Statistics
) -> None:
    session = FakeMeridianSession(NOW)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    session.kraken.requests.clear()

    data = await sync(coordinator, NOW + HOUR, session)
    assert measurement_dates(session) == [(TODAY - 2 * DAY, TODAY)]
    assert data.requests == 1

    session.kraken.requests.clear()
    await sync(coordinator, NOW + DAY, session)
    assert measurement_dates(session) == _fill_windows(TODAY - 19 * DAY, TODAY + DAY)
    assert len(statistics.writes) == 3


async def test_names_and_both_directions_of_two_properties(
    hass: HomeAssistant, recorder_stand_in: SimpleNamespace
) -> None:
    second = "A-TEST-2"
    session = FakeMeridianSession(
        NOW, accounts=(ACCOUNT_NUMBER, second), feed_in=(ACCOUNT_NUMBER,)
    )
    # An address over two lines, padded, reads as one.
    padded = account_data(second)
    padded["properties"][0]["address"] = "  1 Synthetic Street,\n   unit A-TEST-2\n"
    session.hook = lambda operation, variables: (
        json_response({"data": {"account": padded}})
        if operation == "account" and variables["accountNumber"] == second
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.requests == 3 * 13
    names = [
        (statistics[0]["name"], statistics[1]["name"])
        for call in recorder_stand_in.write.await_args_list
        for statistics in [[metadata for metadata, _hours in call.args[1]]]
    ]
    assert names == [
        (
            "Meridian grid import — 1 Synthetic Street",
            "Meridian grid import cost — 1 Synthetic Street",
        ),
        (
            "Meridian grid export — 1 Synthetic Street",
            "Meridian grid export credit — 1 Synthetic Street",
        ),
        (
            "Meridian grid import — 1 Synthetic Street, unit A-TEST-2",
            "Meridian grid import cost — 1 Synthetic Street, unit A-TEST-2",
        ),
    ]
    assert {item["direction"] for item in session.kraken.requests} == {
        "CONSUMPTION",
        "GENERATION",
    }


async def test_a_gap_is_filled_and_complete_dates_are_not_asked_for(
    hass: HomeAssistant, statistics: Statistics
) -> None:
    """A 30-day gap, as after an outage, is fetched in windows of its own dates."""
    session = FakeMeridianSession(NOW)
    gap = _days(TODAY - 60 * DAY, TODAY - 31 * DAY)
    stored = [day for day in _days(TARGET, TODAY - 3 * DAY) if day not in gap]
    statistics.store((ENERGY_ID, COST_ID), hours_of(stored))
    coordinator = coordinator_for(hass, session)
    # The daily request has succeeded: only the last 3 dates are recent.
    coordinator._memory[(KEY, "CONSUMPTION")] = _Memory(daily_at=NOW - HOUR)

    await sync(coordinator, NOW, session)

    assert measurement_dates(session) == [
        (TODAY - 2 * DAY, TODAY),
        *_fill_windows(TODAY - 60 * DAY, TODAY - 31 * DAY),
    ]


async def test_a_date_missing_only_its_cost_is_filled(
    hass: HomeAssistant, statistics: Statistics
) -> None:
    session = FakeMeridianSession(NOW)
    days = _days(TARGET, TODAY - 3 * DAY)
    statistics.store((ENERGY_ID,), hours_of(days))
    statistics.store(
        (COST_ID,), hours_of(day for day in days if day != TODAY - 9 * DAY)
    )
    coordinator = coordinator_for(hass, session)
    coordinator._memory[(KEY, "CONSUMPTION")] = _Memory(daily_at=NOW - HOUR)

    await sync(coordinator, NOW, session)

    assert measurement_dates(session)[1:] == [(TODAY - 9 * DAY, TODAY - 9 * DAY)]


async def test_hours_meridian_did_not_return_are_not_asked_for_again(
    hass: HomeAssistant, statistics: Statistics
) -> None:
    """A gap in Meridian's own history is asked for once per start, not hourly."""
    gap = (local_midnight(TODAY - 40 * DAY), local_midnight(TODAY - 38 * DAY))
    kraken = FakeKraken(NOW, days=100, gaps=[gap], cost=_no_cost_on(TODAY - 50 * DAY))
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    assert TODAY - 40 * DAY not in set(_stored_days(statistics, ENERGY_ID))
    session.kraken.requests.clear()

    await sync(coordinator, NOW + HOUR, session)

    assert measurement_dates(session) == [(TODAY - 2 * DAY, TODAY)]
    memory = coordinator._memory[(KEY, "CONSUMPTION")]
    assert memory.complete is True


def _no_cost_on(day: Any) -> Any:
    start, end = local_midnight(day), local_midnight(day + DAY)
    return lambda hour, _channel: None if start <= hour < end else "15"


def _stored_days(statistics: Statistics, statistic_id: str) -> set[Any]:
    return {nz_date(hour) for hour in statistics.hours[statistic_id]}


async def test_the_history_start_ends_the_fill_for_this_run(
    hass: HomeAssistant,
) -> None:
    """Two empty windows older than every reading mark where the history starts."""
    session = FakeMeridianSession(NOW, days=30)
    coordinator = coordinator_for(hass, session)

    await sync(coordinator, NOW, session)

    # 3 recent windows, two more with readings, then two empty ones.
    assert len(measurement_dates(session)) == 7
    memory = coordinator._memory[(KEY, "CONSUMPTION")]
    assert memory.history_start == TODAY - 34 * DAY
    session.kraken.requests.clear()

    await sync(coordinator, NOW + HOUR, session)
    assert measurement_dates(session) == [(TODAY - 2 * DAY, TODAY)]


async def test_a_unit_with_no_readings_waits_quietly_for_the_accounts(
    hass: HomeAssistant, statistics: Statistics, caplog: pytest.LogCaptureFixture
) -> None:
    """A dormant account's empty history is searched once, not every hour."""
    caplog.set_level(logging.INFO)
    kraken = FakeKraken(NOW, withdrawn_from=NOW - 400 * DAY)
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)
    assert data.requests == 13
    assert statistics.writes == []

    session.kraken.requests.clear()
    await sync(coordinator, NOW + HOUR, session)
    assert session.kraken.requests == []

    # The accounts are a day old: asked for again, they wake the unit.
    await sync(coordinator, NOW + DAY + HOUR, session)
    assert len(measurement_dates(session)) == 3
    assert _own(caplog, logging.INFO) == []


async def test_a_meter_with_late_readings_and_a_stored_history_stays_awake(
    hass: HomeAssistant, statistics: Statistics
) -> None:
    """No readings for 3 dates, as when a meter can't send them, is not quiet."""
    kraken = FakeKraken(NOW, withdrawn_from=local_midnight(TODAY - 2 * DAY))
    session = FakeMeridianSession(NOW, kraken=kraken)
    statistics.store((ENERGY_ID, COST_ID), hours_of(_days(TARGET, TODAY - 3 * DAY)))
    coordinator = coordinator_for(hass, session)
    coordinator._memory[(KEY, "CONSUMPTION")] = _Memory(daily_at=NOW - HOUR)

    await sync(coordinator, NOW, session)
    assert coordinator._memory[(KEY, "CONSUMPTION")].quiet is False

    session.kraken.requests.clear()
    await sync(coordinator, NOW + HOUR, session)
    assert measurement_dates(session) == [(TODAY - 2 * DAY, TODAY)]


async def test_requests_are_paced_a_second_apart(
    hass: HomeAssistant, waits: Any
) -> None:
    session = FakeMeridianSession(NOW)
    coordinator = coordinator_for(hass, session)

    await sync(coordinator, NOW, session)

    paces = [call.args[0] for call in waits.await_args_list]
    assert len(paces) == 12
    assert all(0 < wait <= 1 for wait in paces)


@pytest.mark.parametrize(
    ("failure", "detail"),
    [
        (_GATEWAY_PAGE, "HTTP 504"),
        (json_response({}, status=503), "HTTP 503"),
        (json_response({}, status=524), "HTTP 524"),
        (Response(b"", error=ClientConnectionError()), "network error"),
        (Response(b"", error=TimeoutError()), "timed out"),
    ],
)
async def test_a_passing_failure_is_resent_once_after_a_short_wait(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
    waits: Any,
    failure: Response,
    detail: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    session = FakeMeridianSession(NOW)
    failures = iter([failure])
    session.hook = lambda operation, _variables: (
        next(failures, None) if operation == "measurements" else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.requests == 14
    resend = [call.args[0] for call in waits.await_args_list if call.args[0] > 1]
    assert len(resend) == 1
    assert 2 <= resend[0] <= 5
    assert f"Resending a request that failed ({detail})" in caplog.messages


@pytest.mark.parametrize("status", [500, 400])
async def test_a_failure_that_will_not_pass_is_not_resent(
    hass: HomeAssistant, status: int
) -> None:
    session = FakeMeridianSession(NOW)
    session.hook = lambda operation, _variables: (
        json_response({}, status=status) if operation == "measurements" else None
    )
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed):
        await sync(coordinator, NOW, session)

    assert session.operations.count("measurements") == 1


async def test_a_failed_fill_window_waits_longer_each_time_and_says_so_after_a_day(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    session = FakeMeridianSession(NOW)
    failing = (TODAY - 35 * DAY).isoformat()
    session.hook = lambda operation, variables: (
        _GATEWAY_PAGE
        if operation == "measurements" and variables["endOn"] == failing
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    # The fill ends at the failed window, which was sent twice; the update works.
    assert data.requests == 3 + 2 + 2
    memory = coordinator._memory[(KEY, "CONSUMPTION")]
    assert memory.cooldowns[TODAY - 35 * DAY].wait == HOUR

    session.kraken.requests.clear()
    await sync(coordinator, NOW + HOUR / 2, session)
    # Waiting: the older dates go on without it.
    assert (TODAY - 41 * DAY, TODAY - 35 * DAY) not in measurement_dates(session)
    assert (TODAY - 48 * DAY, TODAY - 42 * DAY) in measurement_dates(session)
    (unit,) = coordinator.unit_diagnostics()
    assert (unit["history_complete"], unit["fill_failure"]) == (
        False,
        "cannot_connect (HTTP 504)",
    )

    for wait in (2, 4, 8, 16, 24, 24):
        await sync(coordinator, memory.cooldowns[TODAY - 35 * DAY].until, session)
        assert memory.cooldowns[TODAY - 35 * DAY].wait == wait * HOUR
        if wait == 16:
            # 15 hours after the first failure: not yet a day.
            assert [m for m in _own(caplog, logging.INFO) if "for a day" in m] == []
    lines = [m for m in _own(caplog, logging.INFO) if "for a day" in m]
    assert lines == [
        (
            "Meridian Energy has been unable to fetch some older grid import history "
            "for a day; it keeps trying"
        )
    ]

    session.hook = lambda _operation, _variables: None
    await sync(coordinator, memory.cooldowns[TODAY - 35 * DAY].until, session)
    assert memory.cooldowns == {}
    assert memory.fill_noted is False
    (unit,) = coordinator.unit_diagnostics()
    assert (unit["history_complete"], unit["fill_failure"]) == (True, None)


async def test_a_fill_window_of_one_property_of_several_says_which_after_a_day(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    second = "A-TEST-2"
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, second))
    failing = (property_id(second), (TODAY - 35 * DAY).isoformat())
    session.hook = lambda operation, variables: (
        _GATEWAY_PAGE
        if operation == "measurements"
        and (variables["propertyId"], variables["endOn"]) == failing
        else None
    )
    coordinator = coordinator_for(hass, session)

    await sync(coordinator, NOW, session)
    memory = coordinator._memory[(property_key(second, failing[0]), "CONSUMPTION")]
    for _ in range(5):
        await sync(coordinator, memory.cooldowns[TODAY - 35 * DAY].until, session)

    assert (
        "Older grid import history for property 2 of 2 failed (no answer from "
        "Meridian: HTTP 504); asking again in 60 min"
    ) in _own(caplog, logging.DEBUG)
    assert [m for m in _own(caplog, logging.INFO) if "for a day" in m] == [
        (
            "Meridian Energy has been unable to fetch some older grid import history "
            "for property 2 of 2 for a day; it keeps trying"
        )
    ]


async def test_waits_end_with_their_dates_once_they_age_past_the_target(
    hass: HomeAssistant, statistics: Statistics, caplog: pytest.LogCaptureFixture
) -> None:
    """The oldest window fails until its dates are older than the 90 days."""
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW, days=200)
    oldest = (TARGET + 6 * DAY).isoformat()
    session.hook = lambda operation, variables: (
        _GATEWAY_PAGE
        if operation == "measurements" and variables["startOn"] <= oldest
        else None
    )
    coordinator = coordinator_for(hass, session)

    at = NOW
    await sync(coordinator, at, session)
    memory = coordinator._memory[(KEY, "CONSUMPTION")]
    assert memory.cooldowns
    for _ in range(20):
        at += 12 * HOUR
        await sync(coordinator, at, session)
        if not memory.cooldowns:
            break

    assert nz_date(at) - 90 * DAY > TARGET + 6 * DAY
    (unit,) = coordinator.unit_diagnostics()
    assert (unit["history_complete"], unit["fill_failure"]) == (True, None)
    assert memory.fill_noted is False

    # A new gap whose windows fail for over a day is said again, once.
    caplog.clear()
    today = nz_date(at)
    gap = [today - days * DAY for days in range(50, 40, -1)]
    for statistic_id in (ENERGY_ID, COST_ID):
        for hour in hours_of(gap):
            del statistics.hours[statistic_id][hour]
    first, last = gap[0].isoformat(), gap[-1].isoformat()
    session.hook = lambda operation, variables: (
        _GATEWAY_PAGE
        if operation == "measurements"
        and first <= variables["startOn"]
        and variables["endOn"] <= last
        else None
    )
    for hours in range(1, 35):
        await sync(coordinator, at + hours * HOUR, session)
    assert len([m for m in _own(caplog, logging.INFO) if "for a day" in m]) == 1


async def test_waits_end_once_the_daily_request_stores_their_dates(
    hass: HomeAssistant, statistics: Statistics, recorder_stand_in: SimpleNamespace
) -> None:
    """A fill window fails until the daily request asks for its dates itself."""
    session = FakeMeridianSession(NOW)
    # A barrier that timed out: the first sync writes nothing.
    recorder_stand_in.write.side_effect = None
    recorder_stand_in.write.return_value = False
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    memory = coordinator._memory[(KEY, "CONSUMPTION")]
    statistics.attach(recorder_stand_in)

    failing = (TODAY - 3 * DAY).isoformat()
    session.hook = lambda operation, variables: (
        _GATEWAY_PAGE
        if operation == "measurements" and variables["endOn"] == failing
        else None
    )
    for hours in range(1, 17):
        await sync(coordinator, NOW + hours * HOUR, session)
    assert TODAY - 3 * DAY in memory.cooldowns
    session.hook = lambda _operation, _variables: None

    for hours in (24, 25):
        await sync(coordinator, NOW + hours * HOUR, session)
        (unit,) = coordinator.unit_diagnostics()
        assert (unit["history_complete"], unit["fill_failure"]) == (True, None)
        assert memory.cooldowns == {}


async def test_a_refusal_older_than_every_reading_stops_the_fill(
    hass: HomeAssistant, statistics: Statistics, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    kraken = FakeKraken(NOW, days=30, pre_supply_error="KT-CT-4321")
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)

    await sync(coordinator, NOW, session)

    # 3 recent windows, two more with readings, and the one refused.
    assert len(measurement_dates(session)) == 6
    assert coordinator._memory[(KEY, "CONSUMPTION")].fill_stopped is True
    assert _own(caplog, logging.INFO) == [_STOPPED]
    session.kraken.requests.clear()
    statistics.hours.clear()
    await sync(coordinator, NOW + HOUR, session)
    assert measurement_dates(session) == [(TODAY - 2 * DAY, TODAY)]


async def test_a_stopped_fill_of_one_property_of_several_says_which(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    kraken = FakeKraken(NOW, days=30, pre_supply_error="KT-CT-4321")
    session = FakeMeridianSession(
        NOW, kraken=kraken, accounts=(ACCOUNT_NUMBER, "A-TEST-2")
    )

    await sync(coordinator_for(hass, session), NOW, session)

    assert _own(caplog, logging.INFO) == [
        (
            "Meridian Energy stopped filling in older grid import history for "
            f"property {place} of 2 until Home Assistant restarts: Meridian refused "
            "a request for older dates"
        )
        for place in (1, 2)
    ]


async def test_a_fill_stopped_by_a_refusal_leaves_the_history_incomplete(
    hass: HomeAssistant,
) -> None:
    """A new install whose second fill window is refused once stops at 28 dates."""
    kraken = FakeKraken(NOW, days=100, codes={TODAY - 34 * DAY: "KT-CT-4321"})
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)

    await sync(coordinator, NOW, session)

    (unit,) = coordinator.unit_diagnostics()
    assert (unit["fill_stopped"], unit["history_complete"]) == (True, False)


async def test_a_refusal_inside_the_history_only_waits(
    hass: HomeAssistant, statistics: Statistics
) -> None:
    kraken = FakeKraken(NOW, days=100, codes={TODAY - 34 * DAY: "KT-CT-4321"})
    session = FakeMeridianSession(NOW, kraken=kraken)
    statistics.store((ENERGY_ID, COST_ID), hours_of([TARGET]))
    coordinator = coordinator_for(hass, session)

    await sync(coordinator, NOW, session)

    memory = coordinator._memory[(KEY, "CONSUMPTION")]
    assert memory.fill_stopped is False
    assert TODAY - 34 * DAY in memory.cooldowns


async def test_a_recent_window_refused_before_the_supply_starts_is_its_history_start(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """A supply younger than 21 days: the window before it is refused."""
    caplog.set_level(logging.INFO)
    kraken = FakeKraken(NOW, days=5, pre_supply_error="KT-CT-4321")
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.requests == 2
    (unit,) = coordinator.unit_diagnostics()
    assert (unit["fill_stopped"], unit["history_complete"]) == (True, False)

    # The next day's request is refused too; the stop is said once.
    await sync(coordinator, NOW + DAY, session)
    assert _own(caplog, logging.INFO) == [_STOPPED]


async def test_a_fill_stopped_after_a_complete_history_leaves_it_incomplete(
    hass: HomeAssistant,
) -> None:
    """A young supply, complete at first, whose window before it is refused later."""
    kraken = FakeKraken(NOW, days=10)
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    assert coordinator.unit_diagnostics()[0]["history_complete"] is True

    kraken.pre_supply_error = "KT-CT-4321"
    await sync(coordinator, NOW + DAY, session)

    (unit,) = coordinator.unit_diagnostics()
    assert (unit["fill_stopped"], unit["history_complete"]) == (True, False)


async def test_a_window_refused_across_the_supply_start_stops_the_fill_after_a_restart(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Only hours stored before a refused window count, not those inside it."""
    caplog.set_level(logging.INFO)
    kraken = FakeKraken(
        NOW,
        history_start=local_midnight(TODAY - 8 * DAY),
        pre_supply_error="KT-CT-4321",
        pre_supply_start_on=True,
    )
    session = FakeMeridianSession(NOW, kraken=kraken)
    await sync(coordinator_for(hass, session), NOW, session)
    caplog.clear()
    session.kraken.requests.clear()

    restarted = coordinator_for(hass, session)
    await sync(restarted, NOW + DAY, session)

    # The window holding the supply's first dates is refused for its start.
    assert measurement_dates(session) == [
        (TODAY - 5 * DAY, TODAY + DAY),
        (TODAY - 12 * DAY, TODAY - 6 * DAY),
    ]
    (unit,) = restarted.unit_diagnostics()
    assert (unit["fill_stopped"], unit["history_complete"]) == (True, False)
    assert _own(caplog, logging.INFO) == [_STOPPED]

    session.kraken.requests.clear()
    await sync(restarted, NOW + DAY + HOUR, session)
    assert measurement_dates(session) == [(TODAY - DAY, TODAY + DAY)]


@pytest.mark.parametrize(
    "refusal",
    [
        _coded("KT-CT-4321"),
        Response(b"<html>Forbidden</html>", status=403, headers=_HTML),
    ],
)
async def test_a_recent_window_refused_with_older_history_stored_fails_its_unit(
    hass: HomeAssistant,
    statistics: Statistics,
    caplog: pytest.LogCaptureFixture,
    refusal: Response,
) -> None:
    """One refusal after a start never stops the fill when older hours are stored."""
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW)
    gap = _days(TODAY - 40 * DAY, TODAY - 31 * DAY)
    stored = [day for day in _days(TARGET, TODAY - DAY) if day not in gap]
    statistics.store((ENERGY_ID, COST_ID), hours_of(stored))
    refused = (TODAY - 7 * DAY).isoformat()
    session.hook = lambda operation, variables: (
        refusal
        if operation == "measurements" and variables["endOn"] == refused
        else None
    )
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == "invalid_data"
    assert coordinator._memory[(KEY, "CONSUMPTION")].fill_stopped is False
    assert _own(caplog, logging.INFO) == []

    # Once Meridian answers, the 21 dates are asked for again, then the gap.
    session.hook = lambda _operation, _variables: None
    session.kraken.requests.clear()
    await sync(coordinator, NOW + HOUR, session)
    assert measurement_dates(session) == _fill_windows(
        TODAY - 20 * DAY, TODAY
    ) + _fill_windows(TODAY - 40 * DAY, TODAY - 31 * DAY)


async def test_a_recent_window_refused_when_the_store_cannot_be_read_fails_its_unit(
    hass: HomeAssistant, recorder_stand_in: SimpleNamespace
) -> None:
    kraken = FakeKraken(NOW, days=5, pre_supply_error="KT-CT-4321")
    session = FakeMeridianSession(NOW, kraken=kraken)
    recorder_stand_in.stored.side_effect = OperationalError("SELECT", {}, Exception())
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_placeholders == {"detail": "KT-CT-4321"}
    assert coordinator._memory[(KEY, "CONSUMPTION")].fill_stopped is False


async def test_a_rate_limit_stops_the_sync_writes_what_came_and_waits(
    hass: HomeAssistant, statistics: Statistics
) -> None:
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    limit = (TODAY - 42 * DAY).isoformat()
    session.hook = lambda operation, variables: (
        _rate_limit()
        if operation == "measurements" and variables["endOn"] == limit
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    # Grid import's recent dates and three fill windows came back and were
    # written; grid export was never asked for.
    assert data.requests == 7
    assert {item["direction"] for item in session.kraken.requests} == {"CONSUMPTION"}
    assert len(statistics.writes) == 1
    assert len(statistics.writes[0][ENERGY_ID]) > 0
    assert coordinator.update_interval == UPDATE_INTERVAL
    # No billing request after a rate limit, though none is cached.
    assert "billingPeriods" not in session.operations

    # Meridian's 2 minutes are less than the hour between updates.
    session.hook = lambda _operation, _variables: _rate_limit("86400")
    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW + HOUR, session)
    assert failure.value.translation_key == "rate_limited"
    # Twice Meridian's wait, as the second in a row, up to a day.
    assert failure.value.retry_after == 86400


async def test_a_rate_limit_after_an_import_makes_the_next_update_wait(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.hook = lambda operation, variables: (
        _rate_limit("7200")
        if operation == "measurements"
        and variables["readingDirectionType"] == "GENERATION"
        else None
    )
    coordinator = coordinator_for(hass, session)

    await sync(coordinator, NOW, session)
    assert coordinator.update_interval == timedelta(hours=2)
    assert "billingPeriods" not in session.operations

    session.hook = lambda _operation, _variables: None
    await sync(coordinator, NOW + 2 * HOUR, session)
    assert coordinator.update_interval == UPDATE_INTERVAL


async def test_a_rate_limit_after_the_newest_window_lets_the_update_succeed(
    hass: HomeAssistant, statistics: Statistics, caplog: pytest.LogCaptureFixture
) -> None:
    session = FakeMeridianSession(NOW)
    limited = (TODAY - 7 * DAY).isoformat()
    session.hook = lambda operation, variables: (
        _rate_limit()
        if operation == "measurements" and variables["endOn"] == limited
        else None
    )
    coordinator = coordinator_for(hass, session)

    await refresh(coordinator, NOW, session)

    assert coordinator.last_update_success
    assert coordinator._failed_updates == 0
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
    (written,) = statistics.writes
    assert sorted(written[ENERGY_ID]) == _ended(TODAY - 6 * DAY, TODAY)
    # Meridian's 2 minutes are less than the hour between updates.
    assert coordinator.update_interval == UPDATE_INTERVAL

    # The 21 dates were not all fetched, so they are asked for again.
    session.hook = lambda _operation, _variables: None
    session.kraken.requests.clear()
    await sync(coordinator, NOW + HOUR, session)
    assert measurement_dates(session)[:3] == _fill_windows(TODAY - 20 * DAY, TODAY)


def _ended(first: Any, last: Any) -> list[Any]:
    """Return the hours of some dates that have ended by ``NOW``, in order."""
    return [hour for hour in hours_of(_days(first, last)) if hour + HOUR <= NOW]


async def test_a_rate_limit_outranks_another_unit_s_failure(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, "A-TEST-2"))
    session.hook = lambda operation, variables: (
        None
        if operation != "measurements"
        else _GATEWAY_PAGE
        if variables["propertyId"] == property_id(ACCOUNT_NUMBER)
        else _rate_limit("7200")
    )
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == "rate_limited"
    assert failure.value.retry_after == 7200


async def test_an_in_band_rate_limit_waits_an_hour_then_longer(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW)
    session.hook = lambda operation, _variables: (
        _coded("KT-CT-1199") if operation == "measurements" else None
    )
    coordinator = coordinator_for(hass, session)
    waits = []
    for update in range(3):
        with pytest.raises(UpdateFailed) as failure:
            await sync(coordinator, NOW + update * HOUR, session)
        waits.append(failure.value.retry_after)
        # A rate limit is never resent.
        assert session.operations.count("measurements") == update + 1
    assert waits == [3600, 7200, 14400]


@pytest.mark.parametrize(
    ("answer", "error"),
    [
        (_GATEWAY_PAGE, UpdateFailed),
        (json_response({"error": "Synthetic"}, status=401), ConfigEntryAuthFailed),
    ],
)
async def test_what_came_before_a_failure_or_an_ended_sign_in_is_written(
    hass: HomeAssistant, statistics: Statistics, answer: Response, error: type
) -> None:
    session = FakeMeridianSession(NOW)
    failing = (TODAY - 7 * DAY).isoformat()
    session.hook = lambda operation, variables: (
        answer
        if operation == "measurements" and variables["endOn"] == failing
        else None
    )
    coordinator = coordinator_for(hass, session)

    with pytest.raises(error):
        await sync(coordinator, NOW, session)

    (written,) = statistics.writes
    assert sorted(written[ENERGY_ID]) == _ended(TODAY - 6 * DAY, TODAY)
    assert sorted(written[COST_ID]) == _ended(TODAY - 6 * DAY, TODAY)


_REFUSED = "Meridian refused the request or sent an answer Meridian Energy can't read"


@pytest.mark.parametrize(
    ("answer", "phrase", "key"),
    [
        (_GATEWAY_PAGE, "no answer from Meridian: HTTP 504", "cannot_connect"),
        (
            Response(b"", error=TimeoutError()),
            "no answer from Meridian: timed out",
            "cannot_connect",
        ),
        (
            Response(b"<html>ok</html>", headers=_HTML),
            f"{_REFUSED}: not JSON",
            "invalid_data",
        ),
        (_coded("KT-CT-4321"), f"{_REFUSED}: KT-CT-4321", "invalid_data"),
    ],
)
async def test_one_unit_failing_warns_once_and_says_when_it_works_again(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
    answer: Response,
    phrase: str,
    key: str,
) -> None:
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, "A-TEST-2"))
    failing = property_id("A-TEST-2")
    session.hook = lambda operation, variables: (
        answer
        if operation == "measurements" and variables["propertyId"] == failing
        else None
    )
    coordinator = coordinator_for(hass, session)

    await refresh(coordinator, NOW, session)
    await refresh(coordinator, NOW + HOUR, session)

    assert coordinator.last_update_success
    assert _own(caplog, logging.INFO) == [
        (
            "Meridian Energy couldn't update grid import for property 2 of 2 "
            f"({phrase}); the rest keeps updating"
        )
    ]
    assert [r.levelno for r in caplog.records if r.levelno >= logging.WARNING] == [
        logging.WARNING
    ]
    failures = [unit["last_failure"] for unit in coordinator.unit_diagnostics()]
    assert failures == [None, key]

    session.hook = lambda _operation, _variables: None
    await refresh(coordinator, NOW + 2 * HOUR, session)
    await refresh(coordinator, NOW + 3 * HOUR, session)
    assert _own(caplog, logging.INFO)[1:] == [
        "Meridian Energy is updating grid import for property 2 of 2 again"
    ]
    failures = [unit["last_failure"] for unit in coordinator.unit_diagnostics()]
    assert failures == [None, None]


async def test_the_only_unit_failing_gives_home_assistant_s_error_alone(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW)
    session.hook = lambda operation, _variables: (
        _GATEWAY_PAGE if operation == "measurements" else None
    )
    coordinator = coordinator_for(hass, session)

    await refresh(coordinator, NOW, session)

    assert not coordinator.last_update_success
    assert [m for m in _own(caplog, logging.INFO) if "Error fetching" not in m] == []
    errors = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert [(r.levelno, r.name) for r in errors] == [
        (logging.ERROR, f"{_INTEGRATION}.coordinator")
    ]
    assert errors[0].getMessage() == (
        "Error fetching Meridian Energy data: Couldn't get an answer from Meridian "
        "(HTTP 504). Meridian Energy tries again automatically"
    )


async def test_a_failed_first_sync_is_tried_again_sooner_than_hourly(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW)
    session.hook = lambda operation, _variables: (
        _GATEWAY_PAGE if operation == "measurements" else None
    )
    coordinator = coordinator_for(hass, session)
    waits = []
    for update in range(5):
        with pytest.raises(UpdateFailed) as failure:
            await sync(coordinator, NOW + update * HOUR, session)
        waits.append(failure.value.retry_after)
    assert waits == [600, 1800, 3600, 3600, 3600]

    session.hook = lambda _operation, _variables: None
    await sync(coordinator, NOW + 5 * HOUR, session)
    session.hook = lambda operation, _variables: (
        _GATEWAY_PAGE if operation == "measurements" else None
    )
    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW + 6 * HOUR, session)
    assert failure.value.retry_after is None


async def test_readings_that_cannot_be_told_apart_fail_their_unit(
    hass: HomeAssistant,
) -> None:
    kraken = FakeKraken(NOW, days=100, channels=2, aggregate=True)
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == "invalid_data"
    assert failure.value.translation_placeholders == {"detail": "unexpected data"}


async def test_several_channels_take_several_pages(
    hass: HomeAssistant, statistics: Statistics
) -> None:
    kraken = FakeKraken(NOW, days=100, channels=3)
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.requests == 13 * 3
    (written,) = statistics.writes
    assert set(written[ENERGY_ID].values()) == {Decimal("1.5")}


async def test_a_page_that_does_not_move_on_fails_its_unit(hass: HomeAssistant) -> None:
    kraken = FakeKraken(NOW, days=100, channels=3)
    session = FakeMeridianSession(NOW, kraken=kraken)
    first: dict[str, Any] = {}

    def same_page(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation != "measurements":
            return None
        body = kraken.graphql({**variables, "before": None} | first)
        first.setdefault("before", None)
        return json_response(body)

    session.hook = same_page
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)
    assert failure.value.translation_key == "invalid_data"


async def test_each_request_logs_one_debug_line_with_its_window(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    session = FakeMeridianSession(NOW)
    coordinator = coordinator_for(hass, session)

    await sync(coordinator, NOW, session)

    lines = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("Meridian request: operation=measurements")
    ]
    assert len(lines) == 13
    assert lines[0].startswith(
        "Meridian request: operation=measurements grid_import window=-6..0d page=1 "
        "status=200 content=json bytes="
    )
    assert "window=-90..-84d" in lines[-1]


async def test_a_busy_recorder_or_a_failed_read_still_lets_the_update_succeed(
    hass: HomeAssistant, recorder_stand_in: SimpleNamespace
) -> None:
    session = FakeMeridianSession(NOW)
    # A barrier that timed out: nothing is written.
    recorder_stand_in.write.side_effect = None
    recorder_stand_in.write.return_value = False
    recorder_stand_in.stored.side_effect = OperationalError("SELECT", {}, Exception())
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)
    assert data.requests == 3
    assert coordinator._memory[(KEY, "CONSUMPTION")].newest is None
    assert coordinator.unit_diagnostics()[0]["history_complete"] is False

    recorder_stand_in.write.side_effect = OperationalError("SELECT", {}, Exception())
    await sync(coordinator, NOW + HOUR, session)


@pytest.mark.parametrize("failure", ["write skipped", "write failed", "read failed"])
async def test_a_write_or_read_that_failed_leaves_the_history_incomplete(
    hass: HomeAssistant, recorder_stand_in: SimpleNamespace, failure: str
) -> None:
    session = FakeMeridianSession(NOW)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    assert coordinator.unit_diagnostics()[0]["history_complete"] is True

    error = OperationalError("SELECT", {}, Exception())
    if failure == "write skipped":
        recorder_stand_in.write.side_effect = None
        recorder_stand_in.write.return_value = False
    elif failure == "write failed":
        recorder_stand_in.write.side_effect = error
    else:
        recorder_stand_in.stored.side_effect = error
    await sync(coordinator, NOW + HOUR, session)

    assert coordinator.unit_diagnostics()[0]["history_complete"] is False


async def test_the_sync_summary_counts_estimates_from_the_recent_qualities(
    hass: HomeAssistant,
) -> None:
    estimated = [local_midnight(TODAY - 5 * DAY) + HOUR * hour for hour in range(3)]
    kraken = FakeKraken(NOW, days=100, estimated=estimated)
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    (result,) = data.results
    # Today's ended hours are estimates too, as Meridian's are until midnight.
    assert result.estimated_rows == 3 + 12
    assert result.oldest_estimated == estimated[0]
    assert result.newest_estimated == NOW - HOUR * 1.5
    assert result.latest_reading == NOW - HOUR / 2
    assert dict(result.quality_counts) == {"ACTUAL": 20 * 24 - 3, "ESTIMATE": 15}


async def test_an_expired_account_list_that_fails_is_kept_and_asked_for_again(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)

    session.hook = lambda operation, _variables: (
        _coded("KT-CT-4321") if operation == "accountsList" else None
    )
    for update in range(2):
        await sync(coordinator, NOW + DAY + update * HOUR, session)
    assert session.operations.count("accountsList") == 3

    session.hook = lambda _operation, _variables: None
    await sync(coordinator, NOW + DAY + 2 * HOUR, session)
    assert session.operations.count("accountsList") == 4
    await sync(coordinator, NOW + DAY + 3 * HOUR, session)
    assert session.operations.count("accountsList") == 4
    assert _own(caplog, logging.INFO) == [
        (
            "Meridian Energy couldn't refresh the list of accounts; it keeps using the "
            "one it has"
        ),
        "Meridian Energy has refreshed the list of accounts again",
    ]


@pytest.mark.parametrize(
    ("answer", "key"),
    [
        (_rate_limit(), "rate_limited"),
        (json_response({}, status=401), None),
    ],
)
async def test_a_rate_limit_or_an_ended_sign_in_on_the_accounts_still_stops_it(
    hass: HomeAssistant, answer: Response, key: str | None
) -> None:
    session = FakeMeridianSession(NOW)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    session.hook = lambda operation, _variables: (
        answer if operation == "accountsList" else None
    )

    with pytest.raises(Exception) as failure:  # noqa: PT011 - either kind
        await sync(coordinator, NOW + DAY, session)

    assert getattr(failure.value, "translation_key", None) == (key or "invalid_auth")


async def test_accounts_meridian_no_longer_lists_are_forgotten(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, "A-TEST-2"))
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    assert len(coordinator._memory) == 2

    session.accounts = (ACCOUNT_NUMBER,)
    data = await sync(coordinator, NOW + DAY, session)

    assert data.account_count == 1
    assert list(coordinator._memory) == [(KEY, "CONSUMPTION")]


@pytest.mark.parametrize("properties", [[], None])
async def test_no_account_or_property_to_follow_fails_as_no_accounts(
    hass: HomeAssistant, properties: list[Any] | None
) -> None:
    session = FakeMeridianSession(NOW)
    if properties is None:
        session.accounts = ()
    else:
        session.hook = lambda operation, _variables: (
            json_response({"data": {"account": {**account_data(), "properties": []}}})
            if operation == "account"
            else None
        )
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == "no_accounts"
