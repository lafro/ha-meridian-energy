"""
Every sync path through the one write primitive, against a real recorder.

Each case runs the coordinator against ``kraken_fake.FakeKraken`` on the
``recorder_mock`` recorder, so every import goes through
``statistics.async_replay``, and checks the running sums of energy and cost
after every sync: each stored hour's sum must be the previous hour's plus
its own state. Every value is synthetic.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder import statistics as recorder_statistics
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    statistics_during_period,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

import custom_components.meridian_energy as integration_module
from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy.const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONF_SELECTED_ACCOUNTS,
    DOMAIN,
    FIRST_SYNC_RETRY_SCHEDULE,
    POLL_BUDGET_SECONDS,
    SETUP_IMPORT_BUDGET_SECONDS,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeridianAccount,
    MeridianMeterPoint,
    MeridianProperty,
    SyncMode,
)
from custom_components.meridian_energy.statistics import (
    StatisticsAnchorError,
    StatisticsWriteError,
    _cost_metadata,
    _energy_metadata,
    consumption_ids,
    generation_ids,
    property_key,
)
from custom_components.meridian_energy.units import STATUS_FAILED

from .kraken_fake import FakeKraken
from .test_windows_recorder import (
    NOW,
    PROPERTY,
    _account,
    _bad_steps,
    _rows,
    mock_recorder_before_hass,  # noqa: F401 - the recorder fixture's setup
)

MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)


def _account_with_export(number: str) -> MeridianAccount:
    """Return an account whose one property has feed-in: grid import and export."""
    return MeridianAccount(
        number=number,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id=PROPERTY,
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("synthetic-meter", "synthetic-icp", True),
                ),
            ),
        ),
    )


def _coordinator(
    hass: HomeAssistant, number: str, kraken: FakeKraken
) -> MeridianDataCoordinator:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(number),))
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(side_effect=kraken.async_get_measurements)
    return MeridianDataCoordinator(hass, client)


async def _sync(
    hass: HomeAssistant,
    coordinator: MeridianDataCoordinator,
    kraken: FakeKraken,
    now: datetime,
) -> Any:
    kraken.now = now
    with patch.object(coordinator_module, "_utcnow", return_value=now):
        data = await coordinator.async_fetch_and_import()
    await async_wait_recording_done(hass)
    return data


def _hour_index(start: datetime) -> int:
    return int(start.timestamp() // 3600)


@pytest.mark.asyncio
async def test_every_sync_mode_keeps_the_energy_and_cost_sums_consistent(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Setup, the hourly, daily and weekly updates, a restart and a long outage.

    Each revises every value Meridian returns and leaves out a different
    hour's cost, so each replays over stored rows, and the hours without a
    cost keep the cost stored for them, if any.
    """
    del recorder_mock
    number = "A-TEST-PATHS"
    version = {"value": 0}

    def value(start: datetime, _channel: int) -> str:
        return f"{0.25 + ((_hour_index(start) + version['value']) % 5) * 0.25:.2f}"

    def cost(start: datetime, _channel: int) -> str | None:
        if _hour_index(start) % 41 == version["value"]:
            return None
        return str(10 + version["value"])

    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, value=value, cost=cost)
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    coordinator = _coordinator(hass, number, kraken)
    steps = [
        (NOW, SyncMode.INITIAL, False),
        (NOW + HOUR, SyncMode.TIP, False),
        (NOW + DAY + HOUR, SyncMode.TARGETED_RECONCILIATION, False),
        (NOW + 7 * DAY + 2 * HOUR, SyncMode.FULL_RECONCILIATION, False),
        (NOW + 8 * DAY, SyncMode.RESTART, True),
        (NOW + 30 * DAY, SyncMode.FULL_RECONCILIATION, False),
    ]
    for index, (at, mode, restart) in enumerate(steps):
        version["value"] = index
        if restart:
            coordinator = _coordinator(hass, number, kraken)
        data = await _sync(hass, coordinator, kraken, at)

        assert data.sync_mode is mode
        energy = await _rows(hass, energy_id)
        costs = await _rows(hass, cost_id)
        assert _bad_steps(energy) == [], mode
        assert _bad_steps(costs) == [], mode
        assert datetime.fromtimestamp(energy[-1][0], NOW.tzinfo) >= at - 2 * HOUR


@pytest.mark.asyncio
async def test_a_dropped_write_fails_its_unit_and_the_next_sync_heals_it(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    Rows the recorder never saved are offered again, from the same hour.

    The replay reads back a last row with its old state, so the unit fails
    with ``statistics_write``: nothing that records what was imported moves
    on, neither the measurement cache nor the frontier. The next sync finds
    the same rows changed and replays them, and every sum is whole. A replay
    within the last 14 days needs no more than that: no hour it wrote is
    remembered for the next walks to reach back to.
    """
    del recorder_mock
    number = "A-TEST-DROP"
    revised = {"on": False}
    kraken = FakeKraken(
        NOW,
        history_start=NOW - 400 * DAY,
        value=lambda _start, _channel: "2.5" if revised["on"] else "0.5",
    )
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    coordinator = _coordinator(hass, number, kraken)
    await _sync(hass, coordinator, kraken, NOW)
    cache = {key: dict(rows) for key, rows in coordinator._measurement_cache.items()}
    frontier = dict(coordinator._frontier)

    revised["on"] = True
    real_import = recorder_statistics.import_statistics
    dropped: list[str] = []

    def dropping(instance: Any, metadata: Any, statistics: Any, table: Any) -> bool:
        if metadata["statistic_id"] == energy_id and not dropped:
            dropped.append(energy_id)
            return True
        return real_import(instance, metadata, statistics, table)

    with (
        patch(
            "homeassistant.components.recorder.tasks.statistics.import_statistics",
            side_effect=dropping,
        ),
        pytest.raises(UpdateFailed) as failed,
    ):
        await _sync(hass, coordinator, kraken, NOW + DAY + HOUR)

    assert failed.value.translation_key == "statistics_write"
    assert dropped == [energy_id]
    assert coordinator._measurement_cache == cache
    assert coordinator._frontier == frontier
    assert coordinator.memory.unconfirmed == {}
    (unit,) = coordinator.unit_diagnostics()
    assert unit["status"] == STATUS_FAILED
    assert unit["last_failure"]["reason"] == "statistics_write"

    data = await _sync(hass, coordinator, kraken, NOW + DAY + 3 * HOUR)

    assert data.sync_mode is SyncMode.TARGETED_RECONCILIATION
    energy = await _rows(hass, energy_id)
    assert _bad_steps(energy) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []
    since = (NOW + DAY + 3 * HOUR - 2 * DAY).timestamp()
    assert {row[1] for row in energy if row[0] >= since} == {2.5}


@pytest.mark.asyncio
async def test_a_dropped_initial_import_is_imported_again_in_full(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    An initial import the recorder never saved stays owed, all 90 days of it.

    The record of a finished initial import moves on only once its replay
    is confirmed: had it moved on before, the next sync would no longer be
    an initial import, and would reach back 14 days only, leaving the rest
    of the history the recorder dropped missing for good.
    """
    del recorder_mock
    number = "A-TEST-DROP-INITIAL"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    coordinator = _coordinator(hass, number, kraken)
    real_import = recorder_statistics.import_statistics
    dropped: list[str] = []

    def dropping(instance: Any, metadata: Any, statistics: Any, table: Any) -> bool:
        if metadata["statistic_id"] == energy_id and not dropped:
            dropped.append(energy_id)
            return True
        return real_import(instance, metadata, statistics, table)

    with (
        patch(
            "homeassistant.components.recorder.tasks.statistics.import_statistics",
            side_effect=dropping,
        ),
        pytest.raises(UpdateFailed) as failed,
    ):
        await _sync(hass, coordinator, kraken, NOW)

    assert failed.value.translation_key == "statistics_write"
    # A write Home Assistant did not confirm may be saved soon, as once a
    # backup ends: the first sync is tried again after 10 minutes.
    assert failed.value.retry_after == FIRST_SYNC_RETRY_SCHEDULE[0]
    assert dropped == [energy_id]
    assert await _rows(hass, energy_id) == []

    data = await _sync(hass, coordinator, kraken, NOW + 10 * MINUTE)

    assert data.sync_mode is SyncMode.INITIAL
    energy = await _rows(hass, energy_id)
    assert energy[0][0] <= (NOW - 89 * DAY).timestamp()
    assert _bad_steps(energy) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@contextmanager
def _dropping_once(statistic_id: str) -> Iterator[list[str]]:
    """Have the recorder drop the next import of one statistic, as for good."""
    real_import = recorder_statistics.import_statistics
    dropped: list[str] = []

    def dropping(instance: Any, metadata: Any, statistics: Any, table: Any) -> bool:
        if metadata["statistic_id"] == statistic_id and not dropped:
            dropped.append(statistic_id)
            return True
        return real_import(instance, metadata, statistics, table)

    with patch(
        "homeassistant.components.recorder.tasks.statistics.import_statistics",
        side_effect=dropping,
    ):
        yield dropped


async def _assert_cost_follows_energy(
    hass: HomeAssistant, energy_id: str, cost_id: str, first: float
) -> None:
    """Check that both statistics hold the same hours from ``first``, consistently."""
    energy = await _rows(hass, energy_id)
    costs = await _rows(hass, cost_id)
    assert energy[0][0] == first
    assert [row[0] for row in costs] == [row[0] for row in energy]
    assert _bad_steps(energy) == []
    assert _bad_steps(costs) == []


@pytest.mark.asyncio
async def test_a_retried_initial_import_writes_the_cost_its_first_attempt_dropped(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The retry reaches back to the oldest hour the unconfirmed replay wrote.

    The recorder saved the usage of an initial import but dropped its cost,
    so the unit failed with ``statistics_write``. Its retry comes 3 days
    later: reaching back only its own 90 days, it would leave the usage the
    first attempt saved for its 3 oldest days with no cost, for good, since
    no later update reaches back that far. Once the retry is confirmed, the
    next initial import reaches back its own 90 days again.
    """
    del recorder_mock
    number = "A-TEST-DROP-COST"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    key = property_key(number, PROPERTY)
    energy_id, cost_id = consumption_ids(key)
    coordinator = _coordinator(hass, number, kraken)

    with _dropping_once(cost_id) as dropped, pytest.raises(UpdateFailed) as failed:
        await _sync(hass, coordinator, kraken, NOW)

    assert failed.value.translation_key == "statistics_write"
    assert dropped == [cost_id]
    saved = await _rows(hass, energy_id)
    assert saved[0][0] <= (NOW - 89 * DAY).timestamp()
    assert await _rows(hass, cost_id) == []
    assert coordinator.memory.unconfirmed == {
        (key, "CONSUMPTION"): datetime.fromtimestamp(saved[0][0], UTC)
    }

    data = await _sync(hass, coordinator, kraken, NOW + 3 * DAY)

    assert data.sync_mode is SyncMode.INITIAL
    await _assert_cost_follows_energy(hass, energy_id, cost_id, saved[0][0])
    assert coordinator.memory.unconfirmed == {}


@pytest.mark.asyncio
async def test_setup_hands_the_hours_it_left_half_saved_to_the_entry(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    A unit that failed in a setup that succeeded is healed from the same hour.

    With two properties, setup succeeds once one of them has imported, here
    while the recorder dropped the other's cost. The entry's coordinator
    takes over the oldest hour that replay wrote (``SyncMemory``), so its
    backfill of that cost, an hour after setup, reaches back to it rather
    than to its own 90 days.
    """
    del recorder_mock
    number = "A-TEST-SETUP-DROP"
    other = "synthetic-property-2"
    account = MeridianAccount(
        number=number,
        status="ACTIVE",
        properties=tuple(
            MeridianProperty(
                id=property_id,
                address=f"{index} Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("synthetic-meter", "synthetic-icp", False),
                ),
            )
            for index, property_id in enumerate((PROPERTY, other), start=1)
        ),
    )
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(account,))
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(side_effect=kraken.async_get_measurements)
    energy_id, cost_id = consumption_ids(property_key(number, other))
    setup = MeridianDataCoordinator(hass, client, topology=(account,))

    kraken.now = NOW
    with (
        patch.object(coordinator_module, "_utcnow", return_value=NOW),
        _dropping_once(cost_id) as dropped,
    ):
        await setup.async_fetch_and_import(
            setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
        )
    await async_wait_recording_done(hass)

    assert dropped == [cost_id]
    saved = await _rows(hass, energy_id)
    assert await _rows(hass, cost_id) == []
    entry = MeridianDataCoordinator(
        hass,
        client,
        topology=(account,),
        topology_at=NOW,
        reconciled_at=NOW,
        memory=setup.memory,
    )

    for at in (NOW + HOUR, NOW + 2 * HOUR):
        await _sync(hass, entry, kraken, at)

    await _assert_cost_follows_energy(hass, energy_id, cost_id, saved[0][0])
    assert entry.memory.unconfirmed == {}


@pytest.mark.asyncio
async def test_a_long_outage_retried_writes_the_cost_its_first_attempt_dropped(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    An update after a long outage is healed from the same hour too.

    After 120 days without an update, the frontier rule reaches back the 90
    days it can; the recorder drops that replay's cost. Reaching back its
    own 90 days, the retry 2 days later would leave the 2 oldest days of
    usage it saved with no cost, since the cost's running total would then
    continue from before the outage, below them.
    """
    del recorder_mock
    number = "A-TEST-DROP-OUTAGE"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    key = property_key(number, PROPERTY)
    energy_id, cost_id = consumption_ids(key)
    coordinator = _coordinator(hass, number, kraken)
    await _sync(hass, coordinator, kraken, NOW - 120 * DAY)

    with _dropping_once(cost_id) as dropped, pytest.raises(UpdateFailed) as failed:
        await _sync(hass, coordinator, kraken, NOW)

    assert failed.value.translation_key == "statistics_write"
    assert dropped == [cost_id]
    outage = (NOW - 120 * DAY).timestamp()
    saved = [row for row in await _rows(hass, energy_id) if row[0] > outage]
    assert saved[0][0] <= (NOW - 89 * DAY).timestamp()

    data = await _sync(hass, coordinator, kraken, NOW + 2 * DAY)

    assert data.sync_mode is not SyncMode.INITIAL
    energy = await _rows(hass, energy_id)
    costs = await _rows(hass, cost_id)
    after = {row[0] for row in energy if row[0] >= saved[0][0]}
    assert {row[0] for row in costs if row[0] >= saved[0][0]} == after
    assert _bad_steps(energy) == []
    assert _bad_steps(costs) == []
    assert coordinator.memory.unconfirmed == {}


@pytest.mark.asyncio
async def test_a_deep_replay_cancelled_while_confirming_is_written_again(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The update after a long outage is cancelled while its replay waits to confirm.

    Unloading an entry cancels its update, as here while a busy recorder
    has not confirmed the deep replay, whose writes are queued: the
    recorder may yet save one statistic of it and not the other. Its
    oldest hour is kept as for a replay not confirmed, so the reloaded
    entry's walk reaches back to it, and forgets it once that replay is
    confirmed.
    """
    del recorder_mock
    number = "A-TEST-CANCEL-OUTAGE"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    key = property_key(number, PROPERTY)
    energy_id, cost_id = consumption_ids(key)
    coordinator = _coordinator(hass, number, kraken)
    await _sync(hass, coordinator, kraken, NOW - 120 * DAY)
    waiting = asyncio.Event()

    async def confirm(_hass: HomeAssistant, _replays: list[Any]) -> None:
        waiting.set()
        await asyncio.Event().wait()

    kraken.now = NOW
    with (
        patch.object(statistics_module, "_async_confirm_replay", side_effect=confirm),
        patch.object(coordinator_module, "_utcnow", return_value=NOW),
    ):
        task = hass.async_create_task(coordinator.async_fetch_and_import())
        await waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await async_wait_recording_done(hass)

    unconfirmed = coordinator.memory.unconfirmed
    assert unconfirmed[(key, "CONSUMPTION")] <= NOW - 89 * DAY
    reloaded = MeridianDataCoordinator(
        hass, coordinator.client, unconfirmed=unconfirmed
    )
    data = await _sync(hass, reloaded, kraken, NOW + HOUR)

    assert data.sync_mode is not SyncMode.INITIAL
    assert unconfirmed == {}
    assert _bad_steps(await _rows(hass, energy_id)) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.asyncio
async def test_a_reload_still_writes_the_cost_a_deep_replay_left_unsaved(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The entry keeps a deep replay's unconfirmed hour across a reload.

    The entry's first sync is an initial import, whose cost the recorder
    drops. The entry is then reloaded, as the user, a reauthentication or a
    reconfiguration does, which builds a new coordinator: that one finds the
    usage saved up to now, so its first sync is a 14-day restart, and left
    the cost of the 76 days before with no row, for good. The hour is kept
    for the entry (``hass.data[DOMAIN]["unconfirmed_replays"]``), and every
    walk reaches back to it, whatever its mode.
    """
    del recorder_mock
    number = "A-TEST-DROP-RELOAD"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(number),))
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(side_effect=kraken.async_get_measurements)
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="person@example.com",
        data={
            "email": "person@example.com",
            CONF_REFRESH_TOKEN: "synthetic-refresh",
            CONF_FIREBASE_USER_ID: "synthetic-user",
            CONF_SELECTED_ACCOUNTS: [number],
        },
        version=3,
        minor_version=1,
    )
    entry.add_to_hass(hass)
    now = {"value": NOW}

    with (
        patch.object(integration_module, "MeridianApiClient", return_value=client),
        patch.object(coordinator_module, "_utcnow", side_effect=lambda: now["value"]),
    ):
        with _dropping_once(cost_id) as dropped:
            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done(wait_background_tasks=True)
        first = entry.runtime_data.coordinator
        assert dropped == [cost_id]
        assert not first.last_update_success
        saved = await _rows(hass, energy_id)
        assert await _rows(hass, cost_id) == []

        now["value"] = kraken.now = NOW + HOUR
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)
        reloaded = entry.runtime_data.coordinator
        assert reloaded is not first
        assert reloaded.last_update_success
        assert reloaded.data.sync_mode is SyncMode.RESTART
        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

    await async_wait_recording_done(hass)
    await _assert_cost_follows_energy(hass, energy_id, cost_id, saved[0][0])
    assert reloaded.memory.unconfirmed == {}


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (StatisticsWriteError("synthetic: not confirmed"), "statistics_write"),
        (StatisticsAnchorError("synthetic: refused"), "statistics_refused"),
    ],
    ids=["unconfirmed", "refused"],
)
@pytest.mark.asyncio
async def test_a_unit_waiting_after_a_statistics_failure_reports_it(
    recorder_mock, hass: HomeAssistant, error: Exception, reason: str
) -> None:
    """
    An update in which every unit waits after a statistics failure says so.

    It does not fall back to "could not reach Meridian": the unit waits
    because of what the recorder did, or would not let it do.
    """
    del recorder_mock
    number = "A-TEST-WAIT"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    coordinator = _coordinator(hass, number, kraken)
    await _sync(hass, coordinator, kraken, NOW)

    with (
        patch.object(coordinator_module, "async_replay", side_effect=error),
        pytest.raises(UpdateFailed) as failed,
    ):
        await _sync(hass, coordinator, kraken, NOW + HOUR)
    assert failed.value.translation_key == reason

    with pytest.raises(UpdateFailed) as waiting:
        await _sync(hass, coordinator, kraken, NOW + HOUR + 30 * MINUTE)
    assert waiting.value.translation_key == reason
    (unit,) = coordinator.unit_diagnostics()
    assert unit["last_failure"]["reason"] == reason


@pytest.mark.asyncio
async def test_a_refused_replay_fails_as_refused_and_is_not_retried_soon(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Older statistics with no running total within reach refuse the import.

    That is not a write Home Assistant failed to confirm, and it does not
    clear by itself: every retry refuses again until those statistics
    change. So the first update reports it as refused, and waits for the
    hourly update rather than retry 10 minutes later. Nothing is written,
    so no hour is remembered for the next walks to reach back to. It logs
    one warning for the statistic, besides the unit's own warning that it is
    failing.
    """
    del recorder_mock
    number = "A-TEST-REFUSE"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    energy_id, _cost_id = consumption_ids(property_key(number, PROPERTY))
    old = (NOW - 1000 * DAY).replace(minute=0)
    async_add_external_statistics(
        hass,
        _energy_metadata(energy_id, "Meridian grid import"),
        [{"start": old, "state": 1.0, "sum": 1.0}],
    )
    await async_wait_recording_done(hass)
    coordinator = _coordinator(hass, number, kraken)
    caplog.set_level(logging.WARNING, logger="custom_components.meridian_energy")

    with pytest.raises(UpdateFailed) as failed:
        await _sync(hass, coordinator, kraken, NOW)

    assert failed.value.translation_key == "statistics_refused"
    assert failed.value.retry_after is None
    assert [
        record.name
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.name.startswith("custom_components.meridian_energy")
    ] == [statistics_module.__name__, coordinator_module.__name__]
    assert coordinator.memory.unconfirmed == {}
    (unit,) = coordinator.unit_diagnostics()
    assert unit["status"] == STATUS_FAILED
    assert unit["last_failure"]["reason"] == "statistics_refused"
    assert unit["last_failure"]["cause"] == "StatisticsAnchorError"
    stored = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        old - DAY,
        None,
        {energy_id},
        "hour",
        None,
        {"sum"},
    )
    assert [row["start"] for row in stored[energy_id]] == [old.timestamp()]


@pytest.mark.asyncio
async def test_the_weekly_reconciliation_rewrites_its_whole_window(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The weekly reconciliation replays all of its 14 days, changed or not.

    A seam in the running totals inside them, as an earlier release could
    leave, is rewritten although Meridian returns the same values as before
    for those hours: a replay from the earliest hour that changed would
    start after it, and leave it in place.
    """
    del recorder_mock
    number = "A-TEST-WEEKLY"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    statistic_ids = consumption_ids(property_key(number, PROPERTY))
    coordinator = _coordinator(hass, number, kraken)
    await _sync(hass, coordinator, kraken, NOW)

    seam = (NOW - 3 * DAY).replace(minute=0).timestamp()
    for statistic_id in statistic_ids:
        rows = await _rows(hass, statistic_id)
        async_add_external_statistics(
            hass,
            (_cost_metadata if "cost" in statistic_id else _energy_metadata)(
                statistic_id, "Meridian grid import"
            ),
            [
                {
                    "start": datetime.fromtimestamp(start, NOW.tzinfo),
                    "state": state,
                    "sum": total - 3.0,
                }
                for start, state, total in rows
                if start >= seam
            ],
        )
    await async_wait_recording_done(hass)
    for statistic_id in statistic_ids:
        assert _bad_steps(await _rows(hass, statistic_id)) == [seam], statistic_id

    data = await _sync(hass, coordinator, kraken, NOW + 7 * DAY + 2 * HOUR)

    assert data.sync_mode is SyncMode.FULL_RECONCILIATION
    for statistic_id in statistic_ids:
        assert _bad_steps(await _rows(hass, statistic_id)) == [], statistic_id


@pytest.mark.asyncio
async def test_a_replay_waiting_for_the_recorder_leaves_the_time_budget_alone(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The time a replay waits for the recorder is not taken from Meridian's budget.

    The weekly reconciliation walks both directions of a property with
    feed-in, and the first direction's replay takes 200 s of the update's
    clock, as one confirmed only behind a busy recorder (a backup, say) can.
    Counted against the 240 s budget, it would leave less than the 80 s a
    request needs, so the other direction's older windows would not be asked
    for: it would fail with ``time_budget``, as if Meridian had been slow.
    The replay's time is left out of the budget, so both directions import.
    """
    del recorder_mock
    number = "A-TEST-BUDGET"
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY)
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account_with_export(number),))
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(side_effect=kraken.async_get_measurements)
    coordinator = MeridianDataCoordinator(hass, client)
    await _sync(hass, coordinator, kraken, NOW)

    clock = {"now": 1000.0}
    real_replay = coordinator_module.async_replay
    replayed: list[str] = []

    async def slow_replay(*args: Any, **kwargs: Any) -> tuple[int, int]:
        replayed.append(kwargs["stat_energy_id"])
        if len(replayed) == 1:
            clock["now"] += 200.0
        return await real_replay(*args, **kwargs)

    at = NOW + 7 * DAY + 2 * HOUR
    kraken.now = at
    with (
        patch.object(coordinator_module, "_utcnow", return_value=at),
        patch.object(coordinator_module, "monotonic", side_effect=lambda: clock["now"]),
        patch.object(coordinator_module, "async_replay", slow_replay),
    ):
        data = await coordinator.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)
    await async_wait_recording_done(hass)

    assert data.sync_mode is SyncMode.FULL_RECONCILIATION
    assert len(replayed) == 2
    assert [
        (unit["status"], unit["last_failure"])
        for unit in coordinator.unit_diagnostics()
    ] == [("ok", None), ("ok", None)]
    key = property_key(number, PROPERTY)
    for statistic_id in (*consumption_ids(key), *generation_ids(key)):
        rows = await _rows(hass, statistic_id)
        assert _bad_steps(rows) == [], statistic_id
        assert datetime.fromtimestamp(rows[-1][0], NOW.tzinfo) >= at - 2 * HOUR
