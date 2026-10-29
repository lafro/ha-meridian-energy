"""
Failing units against a real recorder: what the sensors show, and the history.

A unit is one direction of one property. While one keeps failing, every
update succeeds and every sensor stays available; the Current bill totals
that need its hours show unknown, or stop changing, until it catches up.
While every unit fails, or waits after failing, the update fails and every
sensor is unavailable; the update that succeeds again fills in the hours
missed. Every value is synthetic.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy import sensor as sensor_module
from custom_components.meridian_energy.api import MeridianUpstreamError
from custom_components.meridian_energy.const import DOMAIN
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeridianAccount,
    MeridianBillingPeriod,
    MeridianMeterPoint,
    MeridianProperty,
)
from custom_components.meridian_energy.statistics import (
    account_key,
    consumption_ids,
    generation_ids,
    property_key,
)
from custom_components.meridian_energy.transport import MeridianHttpError
from custom_components.meridian_energy.windows import NZ

from .kraken_fake import FakeKraken
from .test_lanes_recorder import (
    _assert_consistent,
    _floor_hour,
    _missing_hours,
    _seed_recent,
)
from .test_windows_recorder import (
    NOW,
    PROPERTY,
    _rows,
    mock_recorder_before_hass,  # noqa: F401 - the recorder fixture's setup
)

HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
TODAY = NOW.astimezone(NZ).date()
FIRST, SECOND = "synthetic-property-1", "synthetic-property-2"
TOTALS = ("usage", "cost", "export", "credit")
USAGE, COST, EXPORT, CREDIT = range(4)
FIRST_KEY, SECOND_KEY = account_key("A-TEST-U1"), account_key("A-TEST-U2")


def _property(property_id: str, *, feed_in: bool = False) -> MeridianProperty:
    return MeridianProperty(
        id=property_id,
        address="1 Synthetic Street",
        meter_points=(
            MeridianMeterPoint(f"meter-{property_id}", "synthetic-icp", feed_in),
        ),
    )


def _gateway_timeout() -> MeridianUpstreamError:
    error = MeridianUpstreamError(504, gateway=True)
    error.__cause__ = MeridianHttpError(504, {})
    return error


def _entry(coordinator: MeridianDataCoordinator) -> MockConfigEntry:
    entry = MockConfigEntry(domain=DOMAIN, title="person@example.com")
    entry.runtime_data = SimpleNamespace(coordinator=coordinator, client=MagicMock())
    return entry


async def _refresh(
    hass: HomeAssistant,
    coordinator: MeridianDataCoordinator,
    krakens: list[FakeKraken],
    now: datetime,
    *,
    saved: bool = True,
) -> None:
    """
    Run one update at ``now``, as Home Assistant schedules it.

    Then wait for the recorder to save what it queued, unless ``saved`` is
    false, as while the database is locked.
    """
    for kraken in krakens:
        kraken.now = now
    with (
        patch.object(coordinator_module, "_utcnow", return_value=now),
        patch.object(coordinator_module.random, "uniform", return_value=3.5),
    ):
        await coordinator.async_refresh()
    if saved:
        await async_wait_recording_done(hass)


_Totals = dict[str, tuple[float | None, ...]]


async def _fail_for_six_hours(
    hass: HomeAssistant, accounts: tuple[MeridianAccount, ...], *, export: bool
) -> tuple[_Totals, list[_Totals], list[_Totals]]:
    """
    Fail the second property, or the export, from 2 to 7 hours on.

    Return each account's four bill totals, by account key, an hour on
    while every unit is healthy, at each update while one fails, and at
    the 3 updates after it is back, 8 to 10 hours on. Every update must
    succeed with every sensor available.
    """
    krakens = {
        property_id: FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
        for property_id in (FIRST, SECOND)
    }
    failing = {"on": False}

    async def measurements(**kwargs: Any) -> Any:
        broken = (
            kwargs["direction"] == "GENERATION"
            if export
            else kwargs["property_id"] == SECOND
        )
        if failing["on"] and broken:
            raise _gateway_timeout()
        return await krakens[kwargs["property_id"]].async_get_measurements(**kwargs)

    start = TODAY - 10 * DAY
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=accounts)
    client.async_get_billing_period = AsyncMock(
        return_value=MeridianBillingPeriod(
            period_length="MONTHLY",
            period_length_multiplier=1,
            is_fixed=True,
            start=start,
            end=TODAY + 20 * DAY,
            next_billing_date=TODAY + 21 * DAY,
            period_start_day=start.day,
        )
    )
    client.async_get_measurements = AsyncMock(side_effect=measurements)
    coordinator = MeridianDataCoordinator(hass, client)
    entry = _entry(coordinator)
    entities: list[Any] = []

    async def refresh(now: datetime) -> _Totals:
        await _refresh(hass, coordinator, list(krakens.values()), now)
        assert coordinator.last_update_success
        assert all(entity.available for entity in entities)
        values = {
            (entity._account_key, entity.entity_description.key): entity.native_value
            for entity in entities
        }
        return {
            key: tuple(
                None
                if (value := values.get((key, f"current_bill_{name}"))) is None
                else float(value)
                for name in TOTALS
            )
            for key in (account_key(account.number) for account in accounts)
        }

    await refresh(NOW)
    await sensor_module.async_setup_entry(hass, entry, entities.extend)
    healthy = await refresh(NOW + HOUR)
    failing["on"] = True
    during = [await refresh(NOW + hours * HOUR) for hours in range(2, 8)]
    failing["on"] = False
    after = [await refresh(NOW + hours * HOUR) for hours in range(8, 11)]
    await entry._async_process_on_unload(hass)
    return healthy, during, after


def _hourly(start: float | None, per_hour: float) -> list[Any]:
    """The usage total at each update while the unit fails: an hour more each."""
    return [pytest.approx((start or 0) + per_hour * hours) for hours in range(1, 7)]


@pytest.mark.asyncio
async def test_a_failing_property_leaves_its_accounts_usage_and_cost_unknown(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Both properties are on one account, whose totals need both.

    Its usage and cost show unknown while the second property fails, and
    at the update after, when that unit is still waiting; at its next try,
    9 hours on, it catches up, and both are known and changing again.
    """
    del recorder_mock
    accounts = (
        MeridianAccount("A-TEST-U1", "ACTIVE", (_property(FIRST), _property(SECOND))),
    )
    _healthy, during, after = await _fail_for_six_hours(hass, accounts, export=False)

    assert all(totals[FIRST_KEY][:2] == (None, None) for totals in [*during, after[0]])
    assert None not in after[1][FIRST_KEY][:2]
    assert after[2][FIRST_KEY][USAGE] == pytest.approx(
        (after[1][FIRST_KEY][USAGE] or 0) + 1.0
    )


@pytest.mark.asyncio
async def test_a_failing_property_on_an_account_of_its_own_stops_its_totals(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The second property is the second account's only one.

    That account's totals stay as they were while it fails, and the first
    account's go on changing; at the unit's next try, 9 hours on, the two
    are equal again.
    """
    del recorder_mock
    accounts = (
        MeridianAccount("A-TEST-U1", "ACTIVE", (_property(FIRST),)),
        MeridianAccount("A-TEST-U2", "ACTIVE", (_property(SECOND),)),
    )
    healthy, during, after = await _fail_for_six_hours(hass, accounts, export=False)

    assert all(
        totals[SECOND_KEY] == healthy[SECOND_KEY] for totals in [*during, after[0]]
    )
    assert [totals[FIRST_KEY][USAGE] for totals in during] == _hourly(
        healthy[FIRST_KEY][USAGE], 0.5
    )
    assert after[1][SECOND_KEY] == after[1][FIRST_KEY]
    assert None not in after[1][SECOND_KEY][:2]


@pytest.mark.asyncio
async def test_a_failing_export_stops_the_export_and_credit_totals(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    One property exports, and its export fails.

    The export and credit totals stay as they were while it fails, and the
    usage and cost go on changing; at the export's next try, 9 hours on,
    the export is back level with the usage, as Meridian's two histories
    are.
    """
    del recorder_mock
    accounts = (
        MeridianAccount("A-TEST-U1", "ACTIVE", (_property(FIRST, feed_in=True),)),
    )
    healthy, during, after = await _fail_for_six_hours(hass, accounts, export=True)

    frozen = healthy[FIRST_KEY][EXPORT:]
    assert None not in frozen
    assert all(totals[FIRST_KEY][EXPORT:] == frozen for totals in [*during, after[0]])
    assert [totals[FIRST_KEY][USAGE] for totals in during] == _hourly(
        healthy[FIRST_KEY][USAGE], 0.5
    )
    assert after[1][FIRST_KEY][EXPORT:] == after[1][FIRST_KEY][:2]


@pytest.mark.asyncio
async def test_after_an_outage_the_sensors_wait_for_the_next_try(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Every usage request fails for 33 hours; Meridian is back from hour 34.

    The units try again 1, 2, 4, 8, 16 and then 24 hours apart, so the
    sensors stay unavailable from hour 34, when Meridian is back, until the
    try at hour 56. That update fills in every hour missed, in both
    directions, with every running sum consistent.
    """
    del recorder_mock
    number = "A-TEST-U3"
    start = NOW - 58 * HOUR
    kraken = FakeKraken(start, history_start=NOW - 400 * DAY, future_rows=False)
    await _seed_recent(hass, number, kraken, 20)
    await _seed_recent(hass, number, kraken, 20, direction="GENERATION")
    down = {"on": False}
    asked: list[datetime] = []

    async def measurements(**kwargs: Any) -> Any:
        asked.append(kraken.now)
        if down["on"]:
            raise _gateway_timeout()
        return await kraken.async_get_measurements(**kwargs)

    exporting = MeridianProperty(
        id=PROPERTY,
        address="1 Synthetic Street",
        meter_points=(MeridianMeterPoint("synthetic-meter", "synthetic-icp", True),),
    )
    client = MagicMock()
    client.async_get_accounts = AsyncMock(
        return_value=(MeridianAccount(number, "ACTIVE", (exporting,)),)
    )
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(side_effect=measurements)
    coordinator = MeridianDataCoordinator(hass, client)
    entry = _entry(coordinator)
    entities: list[Any] = []
    await _refresh(hass, coordinator, [kraken], start)
    await sensor_module.async_setup_entry(hass, entry, entities.extend)
    assert entities
    assert all(entity.available for entity in entities)

    available = {}
    for hour in range(1, 59):
        down["on"] = hour < 34
        await _refresh(hass, coordinator, [kraken], start + hour * HOUR)
        available[hour] = all(entity.available for entity in entities)
        assert coordinator.last_update_success is available[hour]
    await entry._async_process_on_unload(hass)

    tries = sorted({int((at - start) / HOUR) for at in asked} - {0})
    assert tries == [1, 2, 4, 8, 16, 32, 56, 57, 58]
    assert [hour for hour, ok in available.items() if not ok] == list(range(1, 56))
    key = property_key(number, PROPERTY)
    for energy_id, cost_id in (consumption_ids(key), generation_ids(key)):
        rows = await _rows(hass, energy_id)
        assert _missing_hours(rows, start - 10 * DAY, _floor_hour(NOW)) == []
        await _assert_consistent(hass, energy_id, cost_id)
