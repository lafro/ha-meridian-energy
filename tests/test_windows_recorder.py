"""
Bounded windows and the stored-aware import rules against a real recorder.

Every case runs the coordinator, the shipped merge and the importer on the
``recorder_mock`` recorder, against ``kraken_fake.FakeKraken``, then checks
the running sums: each stored hour's sum must be the previous hour's plus its
own state, for energy and for cost. The cases are those the design was
checked with: a stale restart across a hole or an empty window where nothing
was stored (S1, S2), an empty window or a hole over stored rows (A, D),
newest rows no longer returned (T), no statistics at all across a gap and a
short history (B, C), a stale restart across two empty weeks before its
stored rows (S3), and a cost Meridian no longer returns, or that stored
hours newer than every cost returned would be stranded by. Every value is
synthetic.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    statistics_during_period,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy.const import (
    INITIAL_BACKFILL,
    REVISION_OVERLAP,
    TARGETED_RECONCILIATION_MINIMUM,
    TIP_WINDOW,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeridianAccount,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianProperty,
    SyncMode,
)
from custom_components.meridian_energy.statistics import (
    _cost_metadata,
    async_replay,
    consumption_ids,
    property_key,
)
from custom_components.meridian_energy.windows import NZ, local_midnight

from .kraken_fake import FakeKraken

# 16:30 on Friday 16 October 2026 in New Zealand.
NOW = datetime(2026, 10, 16, 3, 30, tzinfo=UTC)
HOUR = timedelta(hours=1)
PROPERTY = "synthetic-property"
_LOGGER_NAME = coordinator_module.__name__


@pytest.fixture
def mock_recorder_before_hass(recorder_db_url: str) -> None:
    """Prepare Recorder storage before the Home Assistant fixture starts."""
    del recorder_db_url


def _account(number: str) -> MeridianAccount:
    return MeridianAccount(
        number=number,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id=PROPERTY,
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("synthetic-meter", "synthetic-icp", False),
                ),
            ),
        ),
    )


def _coordinator(
    hass: HomeAssistant, number: str, kraken: FakeKraken
) -> tuple[MeridianDataCoordinator, MagicMock]:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(number),))
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(side_effect=kraken.async_get_measurements)
    return MeridianDataCoordinator(hass, client), client


async def _sync(
    hass: HomeAssistant, number: str, kraken: FakeKraken, now: datetime
) -> tuple[MeridianDataCoordinator, MagicMock, Any]:
    """Run one sync of a new coordinator at ``now``, as after a restart."""
    coordinator, client = _coordinator(hass, number, kraken)
    with patch.object(coordinator_module, "_utcnow", return_value=now):
        data = await coordinator.async_fetch_and_import()
    await async_wait_recording_done(hass)
    return coordinator, client, data


async def _rows(hass: HomeAssistant, statistic_id: str) -> list[tuple[float, ...]]:
    """Return a statistic's hourly rows as (start, state, sum)."""
    stats = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        NOW - timedelta(days=500),
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


def _bad_steps(rows: list[tuple[float, ...]]) -> list[float]:
    """Return the starts of rows whose sum is not the previous sum plus their state."""
    return [
        later[0]
        for earlier, later in pairwise(rows)
        if abs((later[2] - earlier[2]) - later[1]) > 1e-6
    ]


async def _seed(
    hass: HomeAssistant,
    number: str,
    at: datetime,
    *,
    value: Callable[[datetime, int], str] | None = None,
) -> tuple[str, str]:
    """Import a 90-day history at ``at``, as setup would; return its statistics."""
    kraken = FakeKraken(at, history_start=at - timedelta(days=400), value=value)
    *_, data = await _sync(hass, number, kraken, at)
    assert data.sync_mode is SyncMode.INITIAL
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    assert _bad_steps(await _rows(hass, energy_id)) == []
    return energy_id, cost_id


def _alternating(start: datetime, _channel: int) -> str:
    return "1.5" if start.hour % 2 else "0.5"


def _revised(_start: datetime, _channel: int) -> str:
    """Every value changed, so a stale running sum shows as a bad step."""
    return "2.5"


def _newest(rows: list[tuple[float, ...]]) -> datetime:
    return datetime.fromtimestamp(rows[-1][0], UTC)


def _count(rows: list[tuple[float, ...]], start: datetime, end: datetime) -> int:
    return sum(1 for row in rows if start.timestamp() <= row[0] < end.timestamp())


_SPANS = {
    "90d": INITIAL_BACKFILL,
    "14d": REVISION_OVERLAP,
    "48h": TARGETED_RECONCILIATION_MINIMUM,
    "24h": TIP_WINDOW,
}


@pytest.mark.parametrize(
    ("channels", "span"),
    [
        *(
            pytest.param(channels, span, id=f"{name}-{channels}")
            for channels in (1, 2)
            for name, span in _SPANS.items()
        ),
        pytest.param(24, INITIAL_BACKFILL, id="90d-24"),
    ],
)
@pytest.mark.asyncio
async def test_windowed_imports_store_what_an_unbounded_request_would(
    recorder_mock, hass: HomeAssistant, channels: int, span: timedelta
) -> None:
    """
    The same rows, hours and running sums as one request for the whole span.

    The windowed fetch is imported into one statistic, and the rows a
    request without bounds would have returned into another. A meter with
    24 channels needs 19 pages for its newest week, sent before the
    channels are known.
    """
    del recorder_mock
    since = NOW - span
    kraken = FakeKraken(NOW, channels=channels, value=_alternating)
    coordinator, _client = _coordinator(hass, "A-TEST-1", kraken)
    with patch.object(coordinator_module, "_utcnow", return_value=NOW):
        windowed = await coordinator._async_fetch_since(
            account_number="A-TEST-1",
            property_id=PROPERTY,
            direction="CONSUMPTION",
            since=since,
        )
    unbounded = await kraken.async_get_measurements(
        account_number="A-TEST-1",
        property_id=PROPERTY,
        direction="CONSUMPTION",
        start_on=kraken.history_start.astimezone(NZ).date(),
        end_on=NOW.astimezone(NZ).date(),
        last=len(kraken.history()),
    )
    expected: tuple[MeridianMeasurement, ...] = tuple(
        item
        for item in unbounded.measurements
        if item.start >= since and (item.end or item.start) <= NOW
    )
    ids = {}
    for name, measurements in (
        ("windowed", windowed.measurements),
        ("unbounded", expected),
    ):
        ids[name] = consumption_ids(property_key(name, PROPERTY))
        await async_replay(
            hass,
            stat_energy_id=ids[name][0],
            stat_cost_id=ids[name][1],
            energy_name="Meridian grid import",
            cost_name="Meridian grid import cost",
            measurements=measurements,
        )
    await async_wait_recording_done(hass)

    for index in (0, 1):
        stored = await _rows(hass, ids["windowed"][index])
        assert stored
        assert stored == await _rows(hass, ids["unbounded"][index])
        assert _bad_steps(stored) == []


@pytest.mark.asyncio
async def test_s1_a_stale_restart_imports_past_a_hole_where_nothing_was_stored(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    The newest stored row is 20 days old, and Meridian lacks an hour 10 days back.

    Nothing was ever stored at that hour, so the hole strands nothing: the
    history between the old newest row and the hole is imported.
    """
    del recorder_mock
    energy_id, _ = await _seed(hass, "A-TEST-S1", NOW - timedelta(days=20))
    newest = _newest(await _rows(hass, energy_id))
    hole = NOW.replace(minute=0) - timedelta(days=10)
    kraken = FakeKraken(
        NOW, history_start=NOW - timedelta(days=420), gaps=[(hole, hole + HOUR)]
    )

    *_, data = await _sync(hass, "A-TEST-S1", kraken, NOW)

    rows = await _rows(hass, energy_id)
    assert data.sync_mode is SyncMode.RESTART
    assert data.results[0].consumption_walk.stopped_at_gap is False
    assert _count(rows, newest + HOUR, hole) > 200
    assert _newest(rows) == NOW.replace(minute=0) - HOUR
    assert _bad_steps(rows) == []


@pytest.mark.asyncio
async def test_s2_a_stale_restart_crosses_an_empty_window_where_nothing_was_stored(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    energy_id, _ = await _seed(hass, "A-TEST-S2", NOW - timedelta(days=20))
    newest = _newest(await _rows(hass, energy_id))
    empty = NOW.astimezone(NZ).date() - timedelta(days=13)
    kraken = FakeKraken(
        NOW, history_start=NOW - timedelta(days=420), empty_start_on={empty}
    )

    _, client, _ = await _sync(hass, "A-TEST-S2", kraken, NOW)

    rows = await _rows(hass, energy_id)
    assert client.async_get_measurements.await_count == 13
    assert _count(rows, newest + HOUR, local_midnight(empty)) > 0
    assert _bad_steps(rows) == []


@pytest.mark.asyncio
async def test_s3_a_stale_restart_crosses_two_empty_weeks_to_its_stored_rows(
    recorder_mock, hass: HomeAssistant
) -> None:
    """
    40 days without updates, and Meridian has no readings from 35 to 14 days ago.

    Two empty weeks after a reading would mark the start of the history, but
    the rows stored before them show it goes on: the walk crosses them, and
    the readings between the stored rows and the gap are imported, as one
    request for the 90 days would import them.
    """
    del recorder_mock
    energy_id, cost_id = await _seed(hass, "A-TEST-S3", NOW - timedelta(days=40))
    newest = _newest(await _rows(hass, energy_id))
    gap = (NOW - timedelta(days=35), NOW - timedelta(days=14))
    kraken = FakeKraken(NOW, history_start=NOW - timedelta(days=420), gaps=[gap])

    _, client, data = await _sync(hass, "A-TEST-S3", kraken, NOW)

    rows = await _rows(hass, energy_id)
    walk = data.results[0].consumption_walk
    assert client.async_get_measurements.await_count == 13
    assert (walk.windows_empty, walk.stopped_at_gap) == (2, False)
    assert (walk.history_start, data.history_start) == (False, False)
    between = sum(1 for start, _channel in kraken.history() if newest < start < gap[0])
    assert between > 100
    assert _count(rows, newest + HOUR, gap[0]) == between
    assert _newest(rows) == NOW.replace(minute=0) - HOUR
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.asyncio
async def test_a_an_empty_window_over_stored_rows_stops_the_walk(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Importing older windows across it would strand the stored rows inside."""
    del recorder_mock
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    energy_id, _ = await _seed(hass, "A-TEST-A", NOW, value=_alternating)
    later = NOW + timedelta(days=1)
    empty = later.astimezone(NZ).date() - timedelta(days=13)
    kraken = FakeKraken(
        later,
        history_start=NOW - timedelta(days=400),
        empty_start_on={empty},
        value=_revised,
    )

    _, client, data = await _sync(hass, "A-TEST-A", kraken, later)

    rows = await _rows(hass, energy_id)
    assert data.sync_mode is SyncMode.RESTART
    assert client.async_get_measurements.await_count == 2
    assert data.results[0].consumption_walk.stopped_at_gap is True
    assert any(row[1] == 2.5 for row in rows)
    assert _bad_steps(rows) == []
    assert any(
        record.getMessage().startswith("Meridian returned no readings for a window")
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_d_a_hole_over_stored_rows_cuts_the_import_after_it(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Only the hours after the hole are imported, and it says so once at INFO.

    The weekly reconciliation an hour later meets the same hole, and says so
    at debug level.
    """
    del recorder_mock
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)
    energy_id, _ = await _seed(hass, "A-TEST-D", NOW, value=_alternating)
    later = NOW + timedelta(days=1)
    today = later.astimezone(NZ).date()
    hole = (
        local_midnight(today - timedelta(days=9)),
        local_midnight(today - timedelta(days=6)),
    )
    history_start = NOW - timedelta(days=400)
    kraken = FakeKraken(later, history_start=history_start, gaps=[hole], value=_revised)

    coordinator, client, data = await _sync(hass, "A-TEST-D", kraken, later)

    rows = await _rows(hass, energy_id)
    assert client.async_get_measurements.await_count == 3
    assert data.results[0].consumption_walk.stopped_at_gap is True
    # Only the hours after the hole were rewritten.
    revised = [row for row in rows if row[1] == 2.5]
    assert revised
    assert min(row[0] for row in revised) >= hole[1].timestamp()
    assert _bad_steps(rows) == []

    coordinator._last_full_reconciliation = None
    full = await _update(
        hass,
        coordinator,
        client,
        FakeKraken(
            later + HOUR, history_start=history_start, gaps=[hole], value=_revised
        ),
    )

    assert full.sync_mode is SyncMode.FULL_RECONCILIATION
    assert full.results[0].consumption_walk.stopped_at_gap is True
    assert _bad_steps(await _rows(hass, energy_id)) == []
    lines = [
        record
        for record in caplog.records
        if record.name == _LOGGER_NAME
        and record.getMessage().startswith(
            "Meridian's history has a gap where Home Assistant has statistics"
        )
    ]
    assert [record.levelno for record in lines] == [logging.INFO, logging.DEBUG]
    assert "direction=CONSUMPTION" in lines[0].getMessage()


@pytest.mark.asyncio
async def test_t_newest_rows_no_longer_returned_skip_the_import(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Stored hours after the newest row returned would keep their old sums.

    So nothing is imported for the direction this sync, and it says so once.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    energy_id, _ = await _seed(hass, "A-TEST-T", NOW, value=_alternating)
    before = await _rows(hass, energy_id)
    later = NOW + HOUR
    kraken = FakeKraken(
        later,
        history_start=NOW - timedelta(days=400),
        withdrawn_from=NOW.replace(minute=0) - timedelta(hours=10),
        value=_revised,
    )

    *_, data = await _sync(hass, "A-TEST-T", kraken, later)

    assert data.results[0].consumption_rows == 0
    assert await _rows(hass, energy_id) == before
    (record,) = [
        record
        for record in caplog.records
        if record.getMessage().startswith("Meridian no longer returned the newest")
    ]
    assert record.levelno == logging.INFO


@pytest.mark.asyncio
async def test_b_without_statistics_both_sides_of_a_gap_are_imported(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    gap = (NOW - timedelta(days=40), NOW - timedelta(days=30))
    kraken = FakeKraken(NOW, history_start=NOW - timedelta(days=400), gaps=[gap])

    _, client, data = await _sync(hass, "A-TEST-B", kraken, NOW)

    energy_id, cost_id = consumption_ids(property_key("A-TEST-B", PROPERTY))
    rows = await _rows(hass, energy_id)
    assert data.sync_mode is SyncMode.INITIAL
    assert client.async_get_measurements.await_count == 13
    assert _count(rows, NOW - INITIAL_BACKFILL, gap[0]) > 1000
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.asyncio
async def test_c_a_short_history_stops_after_two_empty_windows(
    recorder_mock, hass: HomeAssistant
) -> None:
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - timedelta(days=20))

    _, client, data = await _sync(hass, "A-TEST-C", kraken, NOW)

    energy_id, _ = consumption_ids(property_key("A-TEST-C", PROPERTY))
    rows = await _rows(hass, energy_id)
    assert client.async_get_measurements.await_count == 5
    assert data.history_start is True
    assert rows[0][1] == pytest.approx(rows[0][2])
    assert _bad_steps(rows) == []


@pytest.mark.asyncio
async def test_a_cost_no_longer_returned_imports_the_usage_alone(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The newest hours come back without a cost Home Assistant has stored.

    The importer writes cost rows only for hours with a cost, so those hours
    would keep their old running cost while the revised cost of the hours
    before them was written: the cost of every hour up to the newest of them
    waits, the usage imports. No hour after them has a cost yet, so the cost
    statistic is left as it was, consistent.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    energy_id, cost_id = await _seed(hass, "A-TEST-K", NOW, value=_alternating)
    cost_before = await _rows(hass, cost_id)
    later = NOW + HOUR
    withheld = NOW.replace(minute=0) - timedelta(hours=5)
    kraken = FakeKraken(
        later,
        history_start=NOW - timedelta(days=400),
        value=_revised,
        cost=lambda start, _channel: None if start >= withheld else "20",
    )

    *_, data = await _sync(hass, "A-TEST-K", kraken, later)

    energy = await _rows(hass, energy_id)
    costs = await _rows(hass, cost_id)
    assert data.results[0].consumption_rows > 0
    assert any(row[1] == 2.5 for row in energy)
    assert _bad_steps(energy) == []
    assert _bad_steps(costs) == []
    assert costs == cost_before
    (record,) = [
        record
        for record in caplog.records
        if record.getMessage().startswith("Meridian returned readings without a cost")
    ]
    assert record.levelno == logging.INFO


@pytest.mark.asyncio
async def test_stored_costs_newer_than_every_cost_returned_keep_the_cost_as_it_was(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The cost statistic has hours the usage statistic lacks, newer than any returned.

    A cost import would rewrite the cost of the hours returned, and leave
    those newer stored hours with their old running sums. So the update
    imports the usage alone, and the cost statistic stays as it was,
    though every cost Meridian returned changed.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    number = "A-TEST-K3"
    energy_id, cost_id = await _seed(hass, number, NOW - 5 * HOUR)
    seeded = await _rows(hass, cost_id)
    newest_cost = seeded[-1][2]
    # Five more hours of cost, with no usage stored for them.
    first_extra = _newest(seeded) + HOUR
    async_add_external_statistics(
        hass,
        _cost_metadata(cost_id, "Meridian grid import cost"),
        [
            {
                "start": first_extra + offset * HOUR,
                "state": 0.15,
                "sum": newest_cost + 0.15 * (offset + 1),
            }
            for offset in range(5)
        ],
    )
    await async_wait_recording_done(hass)
    cost_before = await _rows(hass, cost_id)
    assert _bad_steps(cost_before) == []
    kraken = FakeKraken(
        NOW,
        history_start=NOW - timedelta(days=400),
        withdrawn_from=first_extra + 2 * HOUR,
        value=_revised,
        cost=lambda _start, _channel: "25",
    )

    *_, data = await _sync(hass, number, kraken, NOW)

    energy = await _rows(hass, energy_id)
    assert data.sync_mode is SyncMode.RESTART
    assert data.results[0].consumption_rows > 0
    assert any(row[1] == 2.5 for row in energy)
    assert _newest(energy) == first_extra + HOUR
    assert _bad_steps(energy) == []
    assert await _rows(hass, cost_id) == cost_before
    assert len(_info(caplog, "Meridian no longer returned the cost of the newest")) == 1


def _starts(rows: list[tuple[float, ...]], since: datetime) -> list[float]:
    return [row[0] for row in rows if row[0] >= since.timestamp()]


async def _update(
    hass: HomeAssistant,
    coordinator: MeridianDataCoordinator,
    client: MagicMock,
    kraken: FakeKraken,
) -> Any:
    """Run the next sync of a running coordinator against ``kraken``."""
    client.async_get_measurements.side_effect = kraken.async_get_measurements
    with patch.object(coordinator_module, "_utcnow", return_value=kraken.now):
        data = await coordinator.async_fetch_and_import()
    await async_wait_recording_done(hass)
    return data


def _info(caplog: pytest.LogCaptureFixture, prefix: str) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == _LOGGER_NAME
        and record.levelno == logging.INFO
        and record.getMessage().startswith(prefix)
    ]


@pytest.mark.asyncio
async def test_the_cost_after_hours_that_lost_theirs_is_written_at_once(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Hours after the newest stored hour that lost its cost get theirs at once.

    One coordinator, as in a running Home Assistant. An hour after setup,
    five stored hours come back without their cost. That update imports
    their usage, keeps their stored cost, and writes the cost of the new
    hour after them from the stored cost sum of the newest of them. The
    next update writes the cost of its new hour too: no hour is left
    without one. The history starts at setup's cutoff, 90 days back: the
    first update's backfill, asking for the hours of that date before it,
    imports none.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    number = "A-TEST-W"
    history_start = NOW - INITIAL_BACKFILL
    first_new = NOW.replace(minute=0)
    withheld = first_new - 5 * HOUR

    def cost(start: datetime, _channel: int) -> str | None:
        return None if withheld <= start < first_new else "15"

    coordinator, client = _coordinator(
        hass, number, FakeKraken(NOW, history_start=history_start)
    )
    with patch.object(coordinator_module, "_utcnow", return_value=NOW):
        setup = await coordinator.async_fetch_and_import()
    await async_wait_recording_done(hass)
    assert setup.sync_mode is SyncMode.INITIAL
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    cost_after_setup = await _rows(hass, cost_id)

    cut = await _update(
        hass,
        coordinator,
        client,
        FakeKraken(NOW + HOUR, history_start=history_start, cost=cost),
    )

    costs = await _rows(hass, cost_id)
    assert cut.sync_mode is SyncMode.TIP
    assert cut.results[0].consumption_rows > 0
    assert costs[:-1] == cost_after_setup
    assert _newest(costs) == first_new
    assert _bad_steps(costs) == []
    assert len(_info(caplog, "Meridian returned readings without a cost")) == 1

    later = await _update(
        hass,
        coordinator,
        client,
        FakeKraken(NOW + 2 * HOUR, history_start=history_start, cost=cost),
    )

    energy = await _rows(hass, energy_id)
    costs = await _rows(hass, cost_id)
    assert later.sync_mode is SyncMode.TIP
    assert _newest(energy) == first_new + HOUR
    assert _starts(costs, withheld) == _starts(energy, withheld)
    assert _bad_steps(energy) == []
    assert _bad_steps(costs) == []


@pytest.mark.asyncio
async def test_a_stale_restart_writes_the_cost_after_hours_that_lost_theirs(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Home Assistant was off for 20 days, and stored costs older than 60 days lost.

    Meridian no longer returns the cost of hours older than 60 days, which
    were stored with one. The restart's 90-day import writes the usage of
    every hour, keeps those hours' stored cost, and writes the cost of every
    hour after them from the newest one's stored sum: the 20 days Home
    Assistant missed get their cost in that import, though later updates
    reach back only 14 days.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    number = "A-TEST-K2"
    energy_id, cost_id = await _seed(hass, number, NOW - timedelta(days=20))
    lost = NOW - timedelta(days=60)
    kraken = FakeKraken(
        NOW,
        history_start=NOW - timedelta(days=420),
        cost=lambda start, _channel: None if start < lost else "15",
    )

    *_, data = await _sync(hass, number, kraken, NOW)

    energy = await _rows(hass, energy_id)
    costs = await _rows(hass, cost_id)
    # Statistics 20 days old: the direction walks the 90 days of an initial import.
    assert data.results[0].consumption_walk.requested_days == 91
    assert _newest(energy) == NOW.replace(minute=0) - HOUR
    assert _starts(costs, lost) == _starts(energy, lost)
    assert _bad_steps(energy) == []
    assert _bad_steps(costs) == []
    assert len(_info(caplog, "Meridian returned readings without a cost")) == 1


@pytest.mark.asyncio
async def test_an_hour_whose_cost_is_still_to_come_imports_its_usage(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Meridian may publish an hour's usage before its cost: no cost is skipped.

    No cost is stored for that hour, so nothing waits and no INFO line says
    otherwise. The update after, which brings the cost, writes it.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    number = "A-TEST-L"
    history_start = NOW - timedelta(days=400)
    newest = NOW.replace(minute=0) - HOUR
    coordinator, client = _coordinator(
        hass, number, FakeKraken(NOW - HOUR, history_start=history_start)
    )
    with patch.object(coordinator_module, "_utcnow", return_value=NOW - HOUR):
        await coordinator.async_fetch_and_import()
    await async_wait_recording_done(hass)
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))

    lagging = await _update(
        hass,
        coordinator,
        client,
        FakeKraken(
            NOW,
            history_start=history_start,
            cost=lambda start, _channel: None if start >= newest else "15",
        ),
    )

    assert lagging.sync_mode is SyncMode.TIP
    assert _newest(await _rows(hass, energy_id)) == newest
    assert _newest(await _rows(hass, cost_id)) == newest - HOUR
    assert _info(caplog, "Meridian returned readings without a cost") == []

    await _update(
        hass, coordinator, client, FakeKraken(NOW + HOUR, history_start=history_start)
    )

    energy = await _rows(hass, energy_id)
    costs = await _rows(hass, cost_id)
    assert _newest(costs) == newest + HOUR
    assert _starts(costs, newest - HOUR) == _starts(energy, newest - HOUR)
    assert _bad_steps(costs) == []


@pytest.mark.parametrize(
    ("refused", "restart_requests"),
    [("before-the-supply", 3), ("starting-before-it", 8)],
)
@pytest.mark.asyncio
async def test_a_week_refused_before_the_supply_still_starts_later_syncs(
    recorder_mock, hass: HomeAssistant, refused: str, restart_requests: int
) -> None:
    """
    After setup, the restart and weekly reconciliations meet the same refusal.

    The supply began 10 days ago, so their 14 days still reach dates before
    it, which Meridian refuses as it did at setup: a week that ends before
    the supply start, or every request that starts before it, so the week
    holding it too, whose dates are then asked for alone. No stored hour is
    older than the dates refused, so they strand nothing: the newer dates
    are imported, as they were at setup, instead of every such sync failing.
    """
    del recorder_mock
    number = f"A-TEST-P{restart_requests}"
    history_start = NOW.replace(minute=0) - timedelta(days=10)
    setup_at = NOW - timedelta(days=3)
    refusal = {
        "history_start": history_start,
        "pre_supply_error": "KT-CT-4321",
        "pre_supply_start_on": refused == "starting-before-it",
    }

    *_, setup = await _sync(hass, number, FakeKraken(setup_at, **refusal), setup_at)
    assert setup.sync_mode is SyncMode.INITIAL
    assert setup.history_start is True

    kraken = FakeKraken(NOW, value=_revised, **refusal)
    coordinator, client, restart = await _sync(hass, number, kraken, NOW)

    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))
    rows = await _rows(hass, energy_id)
    assert restart.sync_mode is SyncMode.RESTART
    assert (restart.history_start, restart.windows_failed) == (True, 1)
    assert len(kraken.requests) == restart_requests
    assert {row[1] for row in rows} == {2.5}
    assert rows[0][0] == history_start.timestamp()
    assert _newest(rows) == NOW.replace(minute=0) - HOUR
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []

    # The weekly reconciliation, due an hour later, meets it too.
    coordinator._last_full_reconciliation = None
    full = await _update(hass, coordinator, client, FakeKraken(NOW + HOUR, **refusal))

    assert full.sync_mode is SyncMode.FULL_RECONCILIATION
    assert (full.history_start, full.windows_failed) == (True, 1)
    assert _bad_steps(await _rows(hass, energy_id)) == []


@pytest.mark.asyncio
async def test_an_hourly_update_with_statistics_older_than_its_span_fails_a_refusal(
    recorder_mock, hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Statistics older than an update's own hours still show it has a history.

    Setup stored 90 days, the newest hour more than a day before the next
    hourly update, as when Meridian publishes readings late. That update
    reads the hours stored in its own 25 hours, and finds none. Meridian
    refusing its window and today alone with a GraphQL error with a code is
    then no supply still to come: the update fails and stores nothing,
    rather than succeed and say there is no usage history yet.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    number = "A-TEST-H"
    energy_id, cost_id = await _seed(hass, number, NOW - timedelta(hours=30))
    before = await _rows(hass, energy_id)
    today = NOW.astimezone(NZ).date()
    kraken = FakeKraken(
        NOW, codes={today - timedelta(days=days): "KT-CT-4321" for days in range(2)}
    )
    coordinator, client = _coordinator(hass, number, kraken)
    coordinator._initial_refresh_complete = True
    coordinator._last_full_reconciliation = NOW - HOUR
    coordinator._last_targeted_reconciliation = NOW - HOUR

    with pytest.raises(UpdateFailed) as raised:
        await _update(hass, coordinator, client, kraken)
    await async_wait_recording_done(hass)

    assert raised.value.translation_key == "invalid_data"
    assert len(kraken.requests) == 2
    assert _newest(before) < NOW - TIP_WINDOW - HOUR
    assert await _rows(hass, energy_id) == before
    assert _bad_steps(await _rows(hass, cost_id)) == []
    assert _info(caplog, "Meridian rejected a window") == []


@pytest.mark.parametrize("window", [2, 7])
@pytest.mark.asyncio
async def test_a_week_refused_inside_the_history_is_imported_later(
    recorder_mock, hass: HomeAssistant, window: int
) -> None:
    """
    A week refused right after a complete week is not the start of the history.

    Meridian still has the refused week's newest day, so the refusal was not
    about dates before the supply. Setup keeps the newer weeks, and the
    updates after it fill in the rest, reconciliation and coverage, once
    Meridian answers: the 90 days, every running sum consistent.
    """
    del recorder_mock
    number = f"A-TEST-R{window}"
    refused = NOW.astimezone(NZ).date() - timedelta(days=7 * window - 1)
    kraken = FakeKraken(
        NOW, history_start=NOW - timedelta(days=400), codes={refused: "KT-CT-4321"}
    )
    coordinator, _client = _coordinator(hass, number, kraken)
    energy_id, cost_id = consumption_ids(property_key(number, PROPERTY))

    with patch.object(coordinator_module, "_utcnow", return_value=NOW):
        data = await coordinator.async_fetch_and_import(setup_import=True)
    await async_wait_recording_done(hass)

    assert data.units_degraded == 1
    assert data.history_start is False
    rows = await _rows(hass, energy_id)
    assert rows[0][0] > local_midnight(refused).timestamp()

    kraken.codes = {}
    for hours in (1, 2, 3):
        later = NOW + hours * HOUR
        kraken.now = later
        with patch.object(coordinator_module, "_utcnow", return_value=later):
            data = await coordinator.async_fetch_and_import()
        await async_wait_recording_done(hass)

    rows = await _rows(hass, energy_id)
    assert data.units_degraded == 0
    assert rows[0][0] < (later - INITIAL_BACKFILL + HOUR).timestamp()
    assert _newest(rows) == later.replace(minute=0) - HOUR
    assert len(rows) == len({row[0] for row in rows})
    assert (rows[-1][0] - rows[0][0]) / 3600 + 1 == len(rows)
    assert _bad_steps(rows) == []
    assert _bad_steps(await _rows(hass, cost_id)) == []


@pytest.mark.parametrize("setup_import", [True, False], ids=["setup", "after-setup"])
@pytest.mark.parametrize("channels", [1, 2])
@pytest.mark.parametrize("ended", [15, 60], ids=["ended-15d", "ended-60d"])
@pytest.mark.asyncio
async def test_a_history_that_ended_weeks_ago_stores_what_one_request_would(
    recorder_mock,
    hass: HomeAssistant,
    channels: int,
    ended: int,
    setup_import: bool,
) -> None:
    """
    A supply that ended weeks ago: its first import stores its 90 days in full.

    The newest weeks have no readings, and come before any reading, so they
    are no start of a history: the same rows, hours and running sums as one
    request for the 90 days, imported into another statistic. So it goes at
    setup, and for an account added after it, such as a closed one added
    through Reconfigure, whose first import is an update's.
    """
    del recorder_mock
    since = NOW - INITIAL_BACKFILL
    kraken = FakeKraken(
        NOW,
        channels=channels,
        value=_alternating,
        withdrawn_from=NOW.replace(minute=0) - timedelta(days=ended),
    )
    coordinator, _client = _coordinator(hass, "windowed", kraken)
    with patch.object(coordinator_module, "_utcnow", return_value=NOW):
        data = await coordinator.async_fetch_and_import(setup_import=setup_import)
    unbounded = await kraken.async_get_measurements(
        account_number="A-TEST-1",
        property_id=PROPERTY,
        direction="CONSUMPTION",
        start_on=kraken.history_start.astimezone(NZ).date(),
        end_on=NOW.astimezone(NZ).date(),
        last=len(kraken.history()),
    )
    ids = consumption_ids(property_key("unbounded", PROPERTY))
    await async_replay(
        hass,
        stat_energy_id=ids[0],
        stat_cost_id=ids[1],
        energy_name="Meridian grid import",
        cost_name="Meridian grid import cost",
        measurements=tuple(
            item for item in unbounded.measurements if item.start >= since
        ),
    )
    await async_wait_recording_done(hass)

    assert data.sync_mode is SyncMode.INITIAL
    assert data.results[0].consumption_walk.windows_requested == 13
    assert data.history_start is False
    for index, windowed_id in enumerate(
        consumption_ids(property_key("windowed", PROPERTY))
    ):
        stored = await _rows(hass, windowed_id)
        assert stored
        assert stored == await _rows(hass, ids[index])
        assert _bad_steps(stored) == []
