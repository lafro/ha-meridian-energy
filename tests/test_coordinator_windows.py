"""
Tests for the coordinator's walk through bounded measurement windows.

They run the coordinator against ``kraken_fake.FakeKraken``, which builds each
request's rows from its own window and fails on a request without both
bounds, a continuation whose arguments changed or a cursor sent twice. The
resend tests run the real client and transport, at the HTTP level. Every
value is synthetic.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy import windows as windows_module
from custom_components.meridian_energy.api import (
    MeridianApiClient,
    MeridianConnectionError,
    MeridianGraphQLError,
    MeridianRejectedError,
    MeridianUpstreamError,
)
from custom_components.meridian_energy.const import (
    FIREBASE_REFRESH_URL,
    INITIAL_BACKFILL,
    REVISION_OVERLAP,
    SETUP_IMPORT_BUDGET_SECONDS,
    TARGETED_RECONCILIATION_MINIMUM,
    TIP_WINDOW,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeasurementPage,
    MeridianAccount,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianProperty,
    MeridianTokenSet,
    SyncMode,
)
from custom_components.meridian_energy.statistics import consumption_ids, property_key
from custom_components.meridian_energy.transport import MeridianHttpError
from custom_components.meridian_energy.windows import NZ, local_midnight

from .kraken_fake import FakeKraken
from .meridian_http import FakeMeridianSession, Response, json_response

# 16:30 on Friday 16 October 2026 in New Zealand.
NOW = datetime(2026, 10, 16, 3, 30, tzinfo=UTC)
TODAY = NOW.astimezone(NZ).date()
HOUR = timedelta(hours=1)
ACCOUNT = "A-TEST-1"
PROPERTY = "synthetic-property"
_LOGGER_NAME = coordinator_module.__name__
# The coordinator's own wait, before the test fixtures replace it.
_REAL_WAIT = coordinator_module._async_wait


def _account(*, feed_in: bool = False) -> MeridianAccount:
    return MeridianAccount(
        number=ACCOUNT,
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


def _client(
    kraken: FakeKraken,
    *,
    feed_in: bool = False,
    measurements: Callable[..., Any] | None = None,
) -> MagicMock:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(feed_in=feed_in),))
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(
        side_effect=measurements or kraken.async_get_measurements
    )
    return client


@contextmanager
def _sync(
    now: datetime = NOW,
    *,
    latest: datetime | None = None,
    stored: dict[str, frozenset[datetime]] | None = None,
) -> Iterator[AsyncMock]:
    """Fix the clock, the stored statistics and the import; yield the importer."""
    importer = AsyncMock(return_value=(1, 1))
    with (
        patch.object(coordinator_module, "_utcnow", return_value=now),
        patch.object(
            coordinator_module,
            "async_latest_numeric_statistic_start",
            new=AsyncMock(return_value=latest),
        ),
        patch.object(
            coordinator_module,
            "async_stored_hours",
            new=AsyncMock(return_value=stored or {}),
        ),
        patch.object(coordinator_module, "async_replay", importer),
    ):
        yield importer


async def _fetch(
    coordinator: MeridianDataCoordinator,
    since: datetime,
    *,
    stored_energy: frozenset[datetime] = frozenset(),
    has_statistics: bool = False,
) -> Any:
    return await coordinator._async_fetch_since(
        account_number=ACCOUNT,
        property_id=PROPERTY,
        direction="CONSUMPTION",
        since=since,
        mode=SyncMode.INITIAL,
        stored_energy=stored_energy,
        has_statistics=has_statistics,
    )


def _windows(kraken: FakeKraken) -> list[tuple[date, date]]:
    """Return the distinct windows a fake was asked for, in order."""
    windows: list[tuple[date, date]] = []
    for request in kraken.requests:
        window = (
            date.fromisoformat(request["startOn"]),
            date.fromisoformat(request["endOn"]),
        )
        if not windows or windows[-1] != window:
            windows.append(window)
    return windows


def _records(caplog: pytest.LogCaptureFixture, prefix: str) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == _LOGGER_NAME and record.getMessage().startswith(prefix)
    ]


def _expected(
    kraken: FakeKraken, since: datetime, now: datetime = NOW
) -> list[tuple[datetime, int]]:
    """Return the rows an unbounded request kept: from the cutoff, ended by now."""
    return [
        (start, channel)
        for start, channel in kraken.history()
        if start >= since and start + HOUR <= now
    ]


def _keys(measurements: tuple[MeridianMeasurement, ...]) -> list[tuple[datetime, str]]:
    return [(item.start.astimezone(UTC), item.channel_id) for item in measurements]


def _rejected(status: int, *, session: bool = False) -> MeridianConnectionError:
    """Return the error the client raises for a client error (HTTP 4xx)."""
    error = (
        MeridianConnectionError(f"Meridian's session service returned HTTP {status}")
        if session
        else MeridianRejectedError(status)
    )
    error.__cause__ = MeridianHttpError(status, {})
    return error


# --------------------------------------------------------------- requests


@pytest.mark.asyncio
async def test_every_request_has_both_bounds_and_continuations_repeat_them(
    hass,
) -> None:
    """
    Every page sends its window's dates and size; a continuation adds before.

    Two channels over 90 days: the newest week needs two pages, so the fake
    sees continuation pages and checks that each repeats its window's
    arguments with a cursor that window's previous page issued.
    """
    kraken = FakeKraken(NOW, channels=2)
    client = _client(kraken)
    coordinator = MeridianDataCoordinator(hass, client)

    with _sync():
        await coordinator.async_fetch_and_import(setup_import=True)

    assert kraken.violations == []
    assert all(
        {"startOn", "endOn", "last"} <= set(request) for request in kraken.requests
    )
    continuations = [
        (previous, request)
        for previous, request in pairwise(kraken.requests)
        if "before" in request
    ]
    assert continuations
    for previous, request in continuations:
        assert {key: request[key] for key in ("startOn", "endOn", "last")} == {
            key: previous[key] for key in ("startOn", "endOn", "last")
        }


@pytest.mark.parametrize(
    ("span", "windows"),
    [
        (INITIAL_BACKFILL, 13),
        (REVISION_OVERLAP, 3),
        (TARGETED_RECONCILIATION_MINIMUM, 1),
        (TIP_WINDOW, 1),
    ],
    ids=["initial", "restart-and-full", "targeted", "tip"],
)
@pytest.mark.asyncio
async def test_windows_sent_in_every_mode_are_contiguous_and_do_not_overlap(
    hass, span: timedelta, windows: int
) -> None:
    kraken = FakeKraken(NOW)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - span)

    sent = _windows(kraken)
    assert len(sent) == windows
    assert sent[0][1] == TODAY
    assert sent[-1][0] == (NOW - span).astimezone(NZ).date()
    for (newer_start, _), (_, older_end) in pairwise(sent):
        assert older_end == newer_start - timedelta(days=1)
    assert all((end - start).days < 7 for start, end in sent)
    assert result.walk.windows_requested == windows
    assert result.walk.requested_days == sum(
        (end - start).days + 1 for start, end in sent
    )


@pytest.mark.parametrize("channels", [1, 2])
@pytest.mark.parametrize(
    "span",
    [INITIAL_BACKFILL, REVISION_OVERLAP, TARGETED_RECONCILIATION_MINIMUM, TIP_WINDOW],
    ids=["90d", "14d", "48h", "24h"],
)
@pytest.mark.asyncio
async def test_windowed_rows_equal_the_rows_of_an_unbounded_request(
    hass, channels: int, span: timedelta
) -> None:
    """Only the requests change: the rows kept are those one request would give."""
    kraken = FakeKraken(NOW, channels=channels)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - span)

    expected = _expected(kraken, NOW - span)
    assert len(result.measurements) == len(expected)
    assert [item.start.astimezone(UTC) for item in result.measurements] == [
        start for start, _channel in expected
    ]
    assert result.walk.rows_before_window == 0
    assert kraken.violations == []


@pytest.mark.asyncio
async def test_the_first_window_sets_the_page_size_of_the_rest(hass) -> None:
    """
    Two channels: the newest week takes two pages, every older week one.

    The page size of later windows follows the rows an hour held in the
    first window that returned readings, not a whole fetch's rows over its
    span, which publication lag would read too low.
    """
    kraken = FakeKraken(NOW, channels=2)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - INITIAL_BACKFILL)

    sizes = [request["last"] for request in kraken.requests]
    assert sizes[:2] == [214, 214]
    assert "before" in kraken.requests[1]
    assert len(kraken.requests) == 14
    # 167 or 168 hours, 2 rows each, with headroom.
    assert set(sizes[2:]) <= {422, 424}
    assert result.walk.continuation_pages == 1
    assert result.observed_rows_per_hour == 2.0


@pytest.mark.asyncio
async def test_tip_is_one_request_with_the_rest_of_today_left_out(hass) -> None:
    """Estimates for hours still to come arrive in the window, and are left out."""
    kraken = FakeKraken(NOW)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - TIP_WINDOW)

    (request,) = kraken.requests
    assert request["last"] == 64
    assert (request["startOn"], request["endOn"]) == (
        (TODAY - timedelta(days=1)).isoformat(),
        TODAY.isoformat(),
    )
    assert result.received_rows == 48
    # From the first whole hour after the cutoff to the last that has ended.
    assert len(result.measurements) == 23
    assert _keys(result.measurements) == [
        (start, result.measurements[0].channel_id)
        for start, _ in _expected(kraken, NOW - TIP_WINDOW)
    ]


@pytest.mark.asyncio
async def test_windows_across_the_end_of_daylight_saving_keep_every_hour(
    hass,
) -> None:
    """The week holding the extra hour gets 169 rows, each hour once."""
    now = datetime(2027, 4, 8, 3, 30, tzinfo=UTC)
    kraken = FakeKraken(now)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync(now):
        result = await _fetch(coordinator, now - REVISION_OVERLAP)

    assert kraken.materialized[0] == 169
    starts = [item.start.astimezone(UTC) for item in result.measurements]
    assert all(later - earlier == HOUR for earlier, later in pairwise(starts))
    assert _keys(result.measurements) == [
        (start, result.measurements[0].channel_id)
        for start, _ in _expected(kraken, now - REVISION_OVERLAP, now)
    ]


# ------------------------------------------------------------- row checks


@pytest.mark.asyncio
async def test_an_ignored_start_gives_the_same_rows_and_counts_those_before(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Meridian ignoring startOn returns older rows too: they are counted, not kept.

    One page per window still covers it, because a page reaching below its
    window ends the window. The INFO line comes once per coordinator.
    """
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)
    since = NOW - INITIAL_BACKFILL
    honoured = FakeKraken(NOW)
    ignored = FakeKraken(NOW, ignore_start_on=True)
    coordinator = MeridianDataCoordinator(hass, _client(ignored))

    with _sync():
        expected = await _fetch(MeridianDataCoordinator(hass, _client(honoured)), since)
        result = await _fetch(coordinator, since)
        await _fetch(coordinator, since)

    assert _keys(result.measurements) == _keys(expected.measurements)
    assert result.walk.rows_before_window > 0
    assert len(ignored.requests) == 2 * 13
    lines = _records(caplog, "Meridian returned readings from before")
    assert [record.levelno for record in lines] == [logging.INFO, logging.DEBUG]
    assert ACCOUNT not in lines[0].getMessage()


@pytest.mark.parametrize(
    ("reading", "message"),
    [
        ("day-later", "rows after the requested window"),
        ("day-earlier", "before a window that the older window did not return"),
        ("end-excluded", "no readings for the last date of two windows in a row"),
        ("first-hour-excluded", "edge hour at two window boundaries in a row"),
        ("last-hour-excluded", "edge hour at two window boundaries in a row"),
    ],
)
@pytest.mark.asyncio
async def test_dates_read_differently_stop_the_walk_where_they_show(
    hass, reading: str, message: str
) -> None:
    """
    Dates Meridian reads other than as asked fail, rather than lose a date a week.

    Read a day later, a window returns past rows after its end. Read a day
    earlier, the newer window's oldest date comes back before its start,
    and the older window lacks it. With ``endOn`` excluded, every window
    lacks its last date. Each would leave a date of every week out for good.
    With the first hour of ``startOn``, or the last of ``endOn``, read as
    excluded, every window lacks that hour, and so would every week. The
    walk stops at the window that shows it, which is dropped: the newer
    windows are imported, and the unit is degraded with unusable data.
    """
    kraken = FakeKraken(
        NOW,
        shift_days={"day-later": 1, "day-earlier": -1}.get(reading, 0),
        exclusive_end_on=reading == "end-excluded",
        exclusive_start_hour=reading == "first-hour-excluded",
        exclusive_end_hour=reading == "last-hour-excluded",
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert data.units_degraded == 1
    (unit,) = coordinator._run_units
    assert message in str(unit.error)
    assert unit.state.last_failure is not None
    assert unit.state.last_failure.reason == "invalid_data"
    imported = importer.await_args.kwargs["measurements"]
    windows = len(kraken.requests) - 1
    assert min(item.start for item in imported) >= local_midnight(
        TODAY - timedelta(days=7 * windows)
    )


@pytest.mark.asyncio
async def test_a_window_missing_its_last_date_once_is_a_gap(hass) -> None:
    """One window with no readings for its last date is a gap a meter may have."""
    last_date = TODAY - timedelta(days=13)
    kraken = FakeKraken(
        NOW,
        gaps=[
            (local_midnight(last_date), local_midnight(last_date + timedelta(days=1)))
        ],
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - INITIAL_BACKFILL)

    assert len(kraken.requests) == 13
    assert result.walk.history_start is False


def _whole_day(days_ago: int) -> tuple[datetime, datetime]:
    """Return the span of the New Zealand date ``days_ago`` days before today."""
    day = TODAY - timedelta(days=days_ago)
    return local_midnight(day), local_midnight(day + timedelta(days=1))


@pytest.mark.asyncio
async def test_last_dates_missing_in_weeks_apart_are_gaps(hass) -> None:
    """
    Two weeks apart without readings for their last date are imported.

    Which dates are a week's last depends on the day setup runs, so gaps a
    meter may have can fall on two of them; only weeks in a row fail.
    """
    kraken = FakeKraken(NOW, gaps=[_whole_day(7), _whole_day(21)])
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert len(kraken.requests) == 13
    assert data.windows_ok == 13
    imported = importer.await_args.kwargs["measurements"]
    assert [item.start.astimezone(UTC) for item in imported] == [
        start for start, _channel in _expected(kraken, NOW - INITIAL_BACKFILL)
    ]


@pytest.mark.asyncio
async def test_last_dates_missing_in_two_weeks_in_a_row_fail(hass) -> None:
    """
    With ``endOn`` read as excluded, every week lacks its last date.

    The third week shows it, and is dropped: the two newer are imported.
    """
    kraken = FakeKraken(NOW, gaps=[_whole_day(7), _whole_day(14)])
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert data.units_degraded == 1
    assert "two windows in a row" in str(coordinator._run_units[0].error)
    assert len(kraken.requests) == 3
    imported = importer.await_args.kwargs["measurements"]
    assert min(item.start for item in imported) == local_midnight(
        TODAY - timedelta(days=13)
    )


@pytest.mark.parametrize("edge", ["first", "last"])
@pytest.mark.parametrize(
    "span", [INITIAL_BACKFILL, REVISION_OVERLAP], ids=["90-days", "14-days"]
)
@pytest.mark.asyncio
async def test_an_edge_hour_left_out_of_every_window_fails(
    hass, edge: str, span: timedelta
) -> None:
    """
    A window read one hour short at either edge fails, rather than lose that hour.

    No other window returns the hour it leaves out, and its row count does
    not show it. Neighbouring windows do: at each boundary between them,
    one returns its edge hour and the other does not. At two boundaries in
    a row that fails, which the 14 days of a restart or a weekly
    reconciliation have too.
    """
    kraken = FakeKraken(
        NOW, exclusive_start_hour=edge == "first", exclusive_end_hour=edge == "last"
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync(), pytest.raises(ValueError, match="two window boundaries in a row"):
        await _fetch(coordinator, NOW - span)

    assert len(_windows(kraken)) == 3


@pytest.mark.asyncio
async def test_edge_hours_missing_at_boundaries_apart_or_on_both_sides_are_gaps(
    hass,
) -> None:
    """
    An edge hour missing at one boundary, or at boundaries apart, is a gap.

    So is an outage across a boundary, at two boundaries in a row: it leaves
    out the edge hours on both sides, which a bound read short does not.
    The 90 days are imported but for the hours missing.
    """
    first_hour = local_midnight(_start_on(1))
    last_hour = local_midnight(_start_on(4)) - HOUR
    gaps = [(first_hour, first_hour + HOUR), (last_hour, last_hour + HOUR)]
    gaps += [
        (
            local_midnight(_start_on(window)) - 2 * HOUR,
            local_midnight(_start_on(window)) + 2 * HOUR,
        )
        for window in (6, 7)
    ]
    kraken = FakeKraken(NOW, days=400, gaps=gaps)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert len(kraken.requests) == 13
    assert data.windows_ok == 13
    imported = importer.await_args.kwargs["measurements"]
    assert [item.start.astimezone(UTC) for item in imported] == [
        start for start, _channel in _expected(kraken, NOW - INITIAL_BACKFILL)
    ]


@pytest.mark.parametrize("edge", ["first", "last"])
@pytest.mark.asyncio
async def test_the_one_day_overlap_returns_an_edge_hour_left_out(
    hass, edge: str
) -> None:
    """
    With the overlap held in reserve on, an edge hour left out is not lost.

    The older window returns it too, from the day both windows ask for: no
    boundary check applies, and the rows are those of one request.
    """
    kraken = FakeKraken(
        NOW, exclusive_start_hour=edge == "first", exclusive_end_hour=edge == "last"
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync(), patch.object(windows_module, "WINDOW_OVERLAP_DAYS", 1):
        result = await _fetch(coordinator, NOW - REVISION_OVERLAP)

    assert [start for start, _ in _keys(result.measurements)] == [
        start for start, _ in _expected(kraken, NOW - REVISION_OVERLAP)
    ]


@pytest.mark.asyncio
async def test_each_page_logs_whether_its_windows_edge_hours_came_back(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A window without its last hour says so; its row count alone would not.

    Read one hour off, a week holds as many rows: its edge hours show it.
    """
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)
    last_hour = local_midnight(TODAY - timedelta(days=6)) - HOUR
    coordinator = MeridianDataCoordinator(hass, _client(FakeKraken(NOW)))
    gapped = MeridianDataCoordinator(
        hass, _client(FakeKraken(NOW, gaps=[(last_hour, last_hour + HOUR)]))
    )

    with _sync():
        await _fetch(coordinator, NOW - REVISION_OVERLAP)
        normal = [
            record.getMessage()
            for record in _records(caplog, "Meridian measurements page:")
        ]
        caplog.clear()
        await _fetch(gapped, NOW - REVISION_OVERLAP)
        missing = [
            record.getMessage()
            for record in _records(caplog, "Meridian measurements page:")
        ]

    assert " window=1/3 " in normal[0]
    assert " bounds=-6d..0d " in normal[0]
    assert " window=2/3 " in normal[1]
    assert " bounds=-13d..-7d " in normal[1]
    assert all("edge_hours=first:yes,last:yes" in line for line in normal)
    assert " before_window=0 edge_hours=first:yes,last:no " in missing[1]


@pytest.mark.asyncio
async def test_an_empty_page_that_claims_more_pages_is_an_error(hass) -> None:
    client = MagicMock()
    client.async_get_measurements = AsyncMock(
        return_value=MeasurementPage((), True, "synthetic-cursor")
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with _sync(), pytest.raises(ValueError, match="empty page"):
        await _fetch(coordinator, NOW - TIP_WINDOW)


@pytest.mark.asyncio
async def test_a_page_must_be_older_than_the_one_before(hass) -> None:
    """
    A cursor Meridian ignored returns the newest rows again: that is an error.

    Each answer carries a new cursor, so only the check that a page holds
    older readings than the one before can catch it.
    """
    kraken = FakeKraken(NOW, channels=2)
    pages: list[MeasurementPage] = []
    calls = {"count": 0}

    async def first_page_again(**kwargs: Any) -> MeasurementPage:
        calls["count"] += 1
        if not pages:
            pages.append(await kraken.async_get_measurements(**kwargs))
        return MeasurementPage(pages[0].measurements, True, f"cursor-{calls['count']}")

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=first_page_again)
    )

    with _sync(), pytest.raises(ValueError, match="no older than the one before"):
        await _fetch(coordinator, NOW - REVISION_OVERLAP)

    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_a_page_of_newer_readings_is_an_error(hass) -> None:
    kraken = FakeKraken(NOW)
    rows = (await kraken.async_get_measurements(**_window_arguments())).measurements
    client = MagicMock()
    client.async_get_measurements = AsyncMock(
        side_effect=[
            MeasurementPage(rows[-6:-4], True, "cursor-1"),
            MeasurementPage(rows[-4:-2], True, "cursor-2"),
        ]
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with _sync(), pytest.raises(ValueError, match="no older than the one before"):
        await _fetch(coordinator, NOW - timedelta(days=2))


@pytest.mark.parametrize(
    ("channels", "now", "history_hours", "requests"),
    [
        # 72 hours of 3 channels in the newest week: 216 rows, pages of 214.
        # The second page holds the oldest hour's other two channels, then
        # the two empty weeks before the history mark its start.
        (3, NOW, 72, 4),
        # 4 channels over 14 days at 17:30 with no estimates: the newest week
        # takes pages of 214, 214, 214 and 2, the last inside one hour.
        (4, NOW + HOUR, None, 6),
    ],
    ids=["3-channels", "4-channels-restart"],
)
@pytest.mark.asyncio
async def test_a_page_boundary_inside_an_hour_moves_the_window_on(
    hass,
    channels: int,
    now: datetime,
    history_hours: int | None,
    requests: int,
) -> None:
    """
    The next page holds the rest of that hour's channels: as old, but new.

    Each page's oldest reading equals the one before it, yet the page adds
    readings no earlier page held, so the window moves on.
    """
    end_of_today = local_midnight(now.astimezone(NZ).date() + timedelta(days=1))
    kraken = FakeKraken(
        now,
        channels=channels,
        history_start=(
            None if history_hours is None else end_of_today - history_hours * HOUR
        ),
        future_rows=history_hours is not None,
    )
    since = now - (INITIAL_BACKFILL if history_hours else REVISION_OVERLAP)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync(now):
        result = await _fetch(coordinator, since)

    expected = _expected(kraken, since, now)
    assert len(kraken.requests) == requests
    assert kraken.violations == []
    assert len(set(_keys(result.measurements))) == len(expected)
    assert [item.start.astimezone(UTC) for item in result.measurements] == [
        start for start, _channel in expected
    ]


@pytest.mark.asyncio
async def test_a_cursor_that_does_not_change_is_an_error(hass) -> None:
    newest = NOW.replace(minute=0) - HOUR
    kraken = FakeKraken(NOW)
    rows = [(await kraken.async_get_measurements(**_window_arguments())).measurements]
    client = MagicMock()
    client.async_get_measurements = AsyncMock(
        side_effect=[
            MeasurementPage(rows[0][-2:], True, "cursor"),
            MeasurementPage(rows[0][-4:-2], True, "cursor"),
        ]
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with _sync(), pytest.raises(ValueError, match="cursor did not advance"):
        await _fetch(coordinator, newest - timedelta(days=2))


def _window_arguments() -> dict[str, Any]:
    return {
        "account_number": ACCOUNT,
        "property_id": PROPERTY,
        "direction": "CONSUMPTION",
        "start_on": TODAY - timedelta(days=2),
        "end_on": TODAY,
        "last": 94,
    }


@pytest.mark.asyncio
async def test_a_window_whose_later_page_fails_fails_the_sync(hass) -> None:
    """
    A window whose second page fails ends the sync: nothing is imported or cached.

    Its first page came back with readings, so it is not where the history
    starts. Its rows wait in the window's own buffer, which is dropped; in
    this release a failed window always ends the sync or, at the start of
    the history, the walk, whose import holds only the windows completed
    before it (``test_a_window_rejected_before_the_supply_began_...``).
    """
    kraken = FakeKraken(NOW, channels=2)

    async def fail_continuations(**kwargs: Any) -> MeasurementPage:
        if kwargs["before"] is not None:
            raise _rejected(404)
        return await kraken.async_get_measurements(**kwargs)

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=fail_continuations)
    )

    with _sync() as importer, pytest.raises(UpdateFailed):
        await coordinator.async_fetch_and_import()

    importer.assert_not_awaited()
    assert coordinator._measurement_cache == {}


# ------------------------------------------------------------- stop rules


@pytest.mark.asyncio
async def test_two_empty_windows_mark_the_start_of_the_history(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Three weeks with readings, then two without: the walk stops there.

    The empty windows older than every reading log no page line, which would
    give away when the history starts; the summary says it was found.
    """
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)
    kraken = FakeKraken(NOW, days=20)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert len(kraken.requests) == 5
    assert data.history_start is True
    assert (data.windows_ok, data.windows_empty, data.windows_failed) == (3, 2, 0)
    assert data.results[0].consumption_walk.history_start is True
    pages = [
        record.getMessage()
        for record in _records(caplog, "Meridian measurements page:")
    ]
    assert len(pages) == 3
    assert not any(" window=4/13 " in line or " window=5/13 " in line for line in pages)
    (summary,) = _records(caplog, "Meridian sync completed:")
    assert " history_start=yes " in summary.getMessage()


@pytest.mark.asyncio
async def test_an_empty_window_inside_the_history_is_crossed(hass) -> None:
    """With nothing stored, a week with no readings strands nothing."""
    empty = TODAY - timedelta(days=13)
    kraken = FakeKraken(NOW, empty_start_on={empty})
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - INITIAL_BACKFILL)

    assert len(kraken.requests) == 13
    assert result.walk.windows_empty == 1
    assert result.walk.history_start is False
    assert min(item.start for item in result.measurements) < local_midnight(empty)


@pytest.mark.asyncio
async def test_an_empty_window_over_stored_hours_stops_the_walk(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    empty = TODAY - timedelta(days=13)
    stored = frozenset({local_midnight(empty) + HOUR * 5})
    kraken = FakeKraken(NOW, empty_start_on={empty})
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - REVISION_OVERLAP, stored_energy=stored)

    assert len(kraken.requests) == 2
    assert result.walk.stopped_at_gap is True
    (record,) = _records(caplog, "Meridian returned no readings for a window")
    assert record.levelno == logging.INFO
    assert "window=2/3" in record.getMessage()


@pytest.mark.asyncio
async def test_empty_windows_after_readings_and_before_stored_hours_are_crossed(
    hass,
) -> None:
    """
    Hours stored before two empty weeks show the history goes on past them.

    A direction whose statistics are more than 14 days old walks 90 days.
    Its readings stop 20 days back here; the walk goes on towards the hours
    it has stored, and stops at the empty week that holds them, rather than
    take the first two empty weeks for the start of the history.
    """
    stored = frozenset({local_midnight(TODAY - timedelta(days=60))})
    kraken = FakeKraken(NOW, days=20)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - INITIAL_BACKFILL, stored_energy=stored)

    # 60 days back falls in the ninth week, 62 to 56 days back.
    assert len(kraken.requests) == 9
    assert result.walk.windows_empty == 6
    assert result.walk.stopped_at_gap is True
    assert result.walk.history_start is False


@pytest.mark.parametrize(
    ("answer", "stored"),
    [("graphql", False), ("http", False), ("graphql", True)],
    ids=["graphql-error", "client-error", "statistics-only-newer"],
)
@pytest.mark.asyncio
async def test_a_window_rejected_before_the_supply_began_is_the_history_start(
    hass, caplog: pytest.LogCaptureFixture, answer: str, stored: bool
) -> None:
    """
    Meridian may refuse dates before the supply started rather than return none.

    Its web app never asks for dates before then. A window older than every
    reading so far and every hour stored, that Meridian rejects before it
    returned any reading of its own, then its newest date alone too, ends
    the walk: the newer windows are imported, exactly, and the code is
    logged. Stored hours newer than the window do not matter: after setup,
    they are the history setup imported. The rejected request counts as
    finished, and its window as done.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    history_start = NOW - timedelta(days=20)
    since = NOW - INITIAL_BACKFILL
    kraken = FakeKraken(
        NOW,
        history_start=history_start,
        pre_supply_error="KT-CT-4321" if answer == "graphql" else None,
    )

    async def refuse_before_the_supply(**kwargs: Any) -> MeasurementPage:
        if answer == "http" and local_midnight(kwargs["end_on"]) < history_start:
            raise _rejected(404)
        return await kraken.async_get_measurements(**kwargs)

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=refuse_before_the_supply)
    )
    fractions: list[float] = []
    energy_id = consumption_ids(property_key(ACCOUNT, PROPERTY))[0]

    with _sync(
        latest=NOW - timedelta(days=30) if stored else None,
        stored=(
            {energy_id: frozenset({history_start.replace(minute=0) + HOUR})}
            if stored
            else None
        ),
    ) as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, progress=fractions.append
        )

    # Three weeks, the refused one, then its newest date alone, refused too.
    assert coordinator.client.async_get_measurements.await_count == 5
    newest = coordinator.client.async_get_measurements.await_args.kwargs
    assert newest["start_on"] == newest["end_on"] == TODAY - timedelta(days=21)
    assert data.history_start is True
    assert data.windows_failed == 1
    importer.assert_awaited_once()
    imported = importer.await_args.kwargs["measurements"]
    assert [item.start.astimezone(UTC) for item in imported] == [
        start for start, _channel in _expected(kraken, since)
    ]
    assert coordinator.last_request is not None
    assert coordinator.last_request.finished_at is not None
    assert fractions == pytest.approx([1 / 13, 2 / 13, 3 / 13, 4 / 13, 1.0])
    (record,) = _records(caplog, "Meridian rejected a window older than every")
    assert record.levelno == logging.INFO
    assert ("KT-CT-4321" if answer == "graphql" else "HTTP 404") in record.getMessage()
    assert ACCOUNT not in record.getMessage()
    assert "Synthetic Street" not in record.getMessage()


@pytest.mark.parametrize(
    "case",
    [
        "query-limit",
        "no-error-code",
        "missing-property",
        "stored-inside",
        "stored-older",
        "no-reading-yet",
        "service-error",
        "session-service",
        "continuation-page",
    ],
)
@pytest.mark.asyncio
async def test_other_rejected_windows_fail_the_sync(hass, case: str) -> None:
    """
    Only a window older than every reading and stored hour is the start.

    A query over Kraken's limits, a GraphQL error without a code (a failure
    inside Meridian's service, say), a missing property, which refreshes the
    accounts and tries again first, a window at or after a stored hour, a
    rejection before any reading came back, a service error, a client error
    from Google's session service rather than Meridian's, and a window whose
    first page returned readings before a later page failed: each fails the
    walk, which imports the weeks before the rejected one, and the entry's
    updates ask for the rest again. A rejection before any reading came
    back leaves no week to import, so setup fails, and the setup retry can
    run it again.
    """
    history_start = NOW - timedelta(days=20)
    rejected = TODAY - timedelta(days=27)
    kraken = FakeKraken(
        NOW,
        history_start=(
            NOW - timedelta(days=400) if case == "continuation-page" else history_start
        ),
        pre_supply_error={
            "query-limit": "KT-CT-1188",
            "no-error-code": "UNKNOWN",
            "missing-property": "PROPERTY_NOT_FOUND",
        }.get(case, "KT-CT-4321"),
    )
    if case == "no-reading-yet":
        kraken.codes = {TODAY - timedelta(days=6): "KT-CT-4321"}

    async def measurements(**kwargs: Any) -> MeasurementPage:
        pre_supply = local_midnight(kwargs["end_on"]) < history_start
        if case == "service-error" and pre_supply:
            error = MeridianUpstreamError(500, gateway=False)
            error.__cause__ = MeridianHttpError(500, {})
            raise error
        if case == "session-service" and pre_supply:
            raise _rejected(404, session=True)
        if (
            case == "continuation-page"
            and kwargs["start_on"] == rejected
            and kwargs["before"] is not None
        ):
            raise MeridianGraphQLError("measurements", ("KT-CT-4321",))
        return await kraken.async_get_measurements(**kwargs)

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=measurements)
    )
    stored_hour = {
        "stored-inside": local_midnight(rejected) + 5 * HOUR,
        "stored-older": local_midnight(rejected) - 5 * HOUR,
    }.get(case)
    stored = (
        None
        if stored_hour is None
        else {
            consumption_ids(property_key(ACCOUNT, PROPERTY))[0]: frozenset(
                {stored_hour}
            )
        }
    )
    latest = NOW - timedelta(days=30) if stored else None
    if case == "missing-property":
        # The accounts cached when the entry was set up, as for any sync after
        # it: a missing property refreshes them, then tries again.
        with _sync():
            await coordinator.async_prepare_topology()

    with (
        _sync(latest=latest, stored=stored) as importer,
        # Pages of 100 rows: every week takes two.
        (
            patch.object(coordinator_module, "page_size", return_value=100)
            if case == "continuation-page"
            else nullcontext()
        ),
    ):
        if case == "no-reading-yet":
            with pytest.raises(UpdateFailed) as raised:
                await coordinator.async_fetch_and_import(setup_import=True)
            assert raised.value.translation_key == "invalid_data"
            importer.assert_not_awaited()
            return
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert data.units_degraded == 1
    (unit,) = coordinator._run_units
    assert unit.state.last_failure is not None
    assert unit.state.last_failure.reason == {
        "service-error": "upstream_unavailable",
        "session-service": "cannot_connect",
    }.get(case, "invalid_data")
    imported = importer.await_args.kwargs["measurements"]
    assert min(item.start for item in imported) >= local_midnight(
        rejected + timedelta(days=7)
    )
    if case == "missing-property":
        assert coordinator.client.async_get_accounts.await_count == 2
        # Every week of both tries, up to the refused one.
        assert len(kraken.requests) == 2 * 4
    if case == "continuation-page":
        # The rejected window's first page had come back with readings.
        *_, first, failed = coordinator.client.async_get_measurements.await_args_list
        assert first.kwargs["start_on"] == failed.kwargs["start_on"] == rejected
        assert first.kwargs["before"] is None
        assert failed.kwargs["before"] is not None


@pytest.mark.parametrize("feed_in", [False, True], ids=["import", "import-and-export"])
@pytest.mark.asyncio
async def test_an_initial_import_with_no_readings_says_so(
    hass, caplog: pytest.LogCaptureFixture, feed_in: bool
) -> None:
    """
    A new customer's empty history completes, with one INFO line a direction.

    With no reading returned yet, empty weeks are no start of a history:
    the walk goes on to the cutoff, as one request for the whole span would.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    kraken = FakeKraken(NOW, history_start=NOW + timedelta(days=2))
    coordinator = MeridianDataCoordinator(hass, _client(kraken, feed_in=feed_in))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    importer.assert_not_awaited()
    assert len(kraken.requests) == 13 * (1 + feed_in)
    assert data.history_start is False
    records = _records(caplog, "No Meridian usage history was found")
    assert [record.levelno for record in records] == [logging.INFO] * (1 + feed_in)
    assert [record.getMessage() for record in records] == [
        f"No Meridian usage history was found in the last 90 days "
        f"(direction={direction})"
        for direction in ("CONSUMPTION", "GENERATION")[: 1 + feed_in]
    ]


@pytest.mark.asyncio
async def test_an_initial_import_after_setup_searches_the_90_days_too(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A direction with no statistics is searched to the cutoff in every coordinator.

    Nothing records which directions setup searched: one with no statistics
    may be one setup found nothing for, or an account added later through
    Reconfigure, an account added on its own or a property that appeared
    since, which no setup searched. So the first sync after setup, a restart
    or a reload walks its 90 days, as setup does, and says once at INFO that
    it found nothing.
    """
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)
    kraken = FakeKraken(NOW, history_start=NOW + timedelta(days=2))
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import()
        again = await coordinator.async_fetch_and_import()

    importer.assert_not_awaited()
    assert data.sync_mode is SyncMode.INITIAL
    # The same coordinator does not search them again: the next sync is the
    # hourly update.
    assert again.sync_mode is SyncMode.TIP
    assert len(kraken.requests) == 13 + 1
    assert data.history_start is False
    (record,) = _records(caplog, "No Meridian usage history was found")
    assert record.levelno == logging.INFO
    assert record.getMessage() == (
        "No Meridian usage history was found in the last 90 days "
        "(direction=CONSUMPTION)"
    )


@pytest.mark.parametrize("setup_import", [True, False], ids=["setup", "after-setup"])
@pytest.mark.parametrize("ended", [15, 20, 60])
@pytest.mark.asyncio
async def test_a_supply_that_ended_weeks_ago_imports_its_history(
    hass, caplog: pytest.LogCaptureFixture, ended: int, setup_import: bool
) -> None:
    """
    Weeks with no readings before any reading are not the start of a history.

    A property whose supply ended, or a meter that sent nothing for a while,
    has empty newest weeks and readings before them. The walk goes on past
    them, and imports what one request for the 90 days would have: at setup,
    and in the first sync of an account added after setup, such as a closed
    account added through Reconfigure to see its history.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    since = NOW - INITIAL_BACKFILL
    kraken = FakeKraken(
        NOW, days=400, withdrawn_from=NOW.replace(minute=0) - timedelta(days=ended)
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=setup_import)

    assert data.sync_mode is SyncMode.INITIAL
    assert len(kraken.requests) == 13
    assert data.history_start is False
    imported = importer.await_args.kwargs["measurements"]
    assert [item.start.astimezone(UTC) for item in imported] == [
        start for start, _channel in _expected(kraken, since)
    ]
    assert _records(caplog, "No Meridian usage history was found") == []


@pytest.mark.asyncio
async def test_no_history_line_when_the_walk_stops_at_stored_hours(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A walk that stopped at an empty week over stored hours searched no further.

    So it says that, not that the 90 days hold no history.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    energy_id = consumption_ids(property_key(ACCOUNT, PROPERTY))[0]
    stored_hour = local_midnight(TODAY - timedelta(days=17))
    kraken = FakeKraken(
        NOW, days=400, withdrawn_from=NOW.replace(minute=0) - timedelta(days=30)
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync(
        latest=NOW - timedelta(days=20), stored={energy_id: frozenset({stored_hour})}
    ) as importer:
        data = await coordinator.async_fetch_and_import()

    importer.assert_not_awaited()
    # Statistics 20 days old: the direction walks the 90 days of an initial
    # import, and stops at its third week.
    walk = data.results[0].consumption_walk
    assert (walk.windows_requested, walk.requested_days) == (3, 21)
    assert walk.stopped_at_gap is True
    assert _records(caplog, "Meridian returned no readings for a window")
    assert _records(caplog, "No Meridian usage history was found") == []


@pytest.mark.asyncio
async def test_stored_costs_newer_than_every_cost_returned_skip_the_cost(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A stored cost newer than every hour returned with one would keep its sum.

    So the update writes the usage alone, says so once, and caches its rows
    without their cost, for the next update that fetches them to offer again.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    key = property_key(ACCOUNT, PROPERTY)
    cost_id = consumption_ids(key)[1]
    kraken = FakeKraken(NOW, days=400)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync(
        latest=NOW - timedelta(days=1),
        stored={cost_id: frozenset({NOW.replace(minute=0)})},
    ) as importer:
        await coordinator.async_fetch_and_import()

    assert importer.await_args.kwargs["import_cost"] is False
    (record,) = _records(caplog, "Meridian no longer returned the cost of the newest")
    assert record.levelno == logging.INFO
    cache = coordinator._measurement_cache[(key, "CONSUMPTION")]
    assert cache
    assert all(item.cost_cents is None for item in cache.values())


def _start_on(window: int) -> date:
    """Return the first date of a setup walk's window, counted from the newest."""
    return TODAY - timedelta(days=7 * window - 1)


@pytest.mark.parametrize("window", [2, 3, 7])
@pytest.mark.parametrize("code", ["KT-CT-4321", "UNKNOWN"])
@pytest.mark.asyncio
async def test_a_week_refused_after_a_complete_week_fails_the_sync(
    hass, window: int, code: str
) -> None:
    """
    A refusal right after a week complete to its first hour is not the start.

    Nothing then shows that the history starts after the refused week, so
    Meridian is asked for its days alone, newest first. Here it refuses only
    the requests that start on the week's first date: the day before that,
    asked for to confirm the refusal, has readings, so the refusal was about
    something else, a failure under load say. The walk fails there: the
    newer weeks, and the refused week's days after its first, are imported,
    and the unit is degraded; it is not taken for the start of the history,
    so the entry's updates ask for the rest. A GraphQL error with no code is
    never a refusal of dates before the supply: it fails at once.
    """
    refused = _start_on(window)
    kraken = FakeKraken(NOW, days=400, codes={refused: code})
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert data.units_degraded == 1
    assert data.history_start is False
    imported = importer.await_args.kwargs["measurements"]
    assert min(item.start for item in imported) >= local_midnight(refused)
    if code == "UNKNOWN":
        assert len(kraken.requests) == window
        return
    # The week, then each of its days alone, newest first, then the day before.
    days = [
        (request["startOn"], request["endOn"]) for request in kraken.requests[window:]
    ]
    assert days == [
        ((refused - timedelta(days=day)).isoformat(),) * 2 for day in range(-6, 2)
    ]


@pytest.mark.parametrize("refused", ["week-only", "its-first-date-too"])
@pytest.mark.asyncio
async def test_a_week_refused_after_a_week_short_of_its_first_hour_is_asked_by_days(
    hass, caplog: pytest.LogCaptureFixture, refused: str
) -> None:
    """
    A newer week short of its first hour does not make a refused week the start.

    One reading missing at 00:00 on the newer week's first date looks like a
    history that starts inside that week. So the refused week's days are
    asked for alone even then. When they all come back, the week is kept
    and the 90 days are imported but for that hour. When its first date is
    refused too and the day before comes back, the refusal was about
    something else: the walk fails there, with the newer week imported.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    since = NOW - INITIAL_BACKFILL
    first_hour = local_midnight(_start_on(1))
    refused_on = _start_on(2)
    kraken = FakeKraken(
        NOW,
        days=400,
        gaps=[(first_hour, first_hour + HOUR)],
        codes={refused_on: "KT-CT-9999"} if refused == "its-first-date-too" else None,
    )

    async def refuse_the_week(**kwargs: Any) -> MeasurementPage:
        if kwargs["start_on"] == refused_on and kwargs["end_on"] != refused_on:
            raise MeridianGraphQLError("measurements", ("KT-CT-9999",))
        return await kraken.async_get_measurements(**kwargs)

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=refuse_the_week)
    )
    days = [
        ((refused_on + timedelta(days=day)).isoformat(),) * 2
        for day in range(6, -1, -1)
    ]

    if refused == "week-only":
        with _sync() as importer:
            data = await coordinator.async_fetch_and_import(setup_import=True)

        assert data.history_start is False
        assert (data.windows_ok, data.windows_failed) == (13, 0)
        imported = importer.await_args.kwargs["measurements"]
        assert [item.start.astimezone(UTC) for item in imported] == [
            start for start, _channel in _expected(kraken, since)
        ]
        assert first_hour not in {item.start for item in imported}
        sent = [(request["startOn"], request["endOn"]) for request in kraken.requests]
        assert sent[1:8] == days
        assert coordinator.client.async_get_measurements.await_count == 13 + 7
        return

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert data.units_degraded == 1
    imported = importer.await_args.kwargs["measurements"]
    assert min(item.start for item in imported) >= local_midnight(refused_on)
    sent = [(request["startOn"], request["endOn"]) for request in kraken.requests]
    day_before = (refused_on - timedelta(days=1)).isoformat()
    assert sent[1:] == [*days, (day_before, day_before)]
    assert _records(caplog, "Meridian rejected a window") == []


@pytest.mark.parametrize("days_ago", [3, 10, 40, 85, 89])
@pytest.mark.asyncio
async def test_a_week_refused_across_the_supply_start_imports_from_it(
    hass, caplog: pytest.LogCaptureFixture, days_ago: int
) -> None:
    """
    Meridian may refuse every request that starts before the supply did.

    Its web app clamps the start to the supply start, so a week holding that
    date may be refused whole. The week's days are then asked for alone,
    newest first: those from the supply start come back, the day before it
    is refused, and so is the day before that, which confirms it; the
    history starts there. Setup imports every reading from the supply start.
    A refusal of the oldest window's first date needs no confirmation.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    since = NOW - INITIAL_BACKFILL
    supply_start = TODAY - timedelta(days=days_ago)
    kraken = FakeKraken(
        NOW,
        history_start=local_midnight(supply_start),
        pre_supply_error="KT-CT-4321",
        pre_supply_start_on=True,
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))
    fractions: list[float] = []

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, progress=fractions.append
        )

    assert data.history_start is True
    assert data.windows_failed == 1
    imported = importer.await_args.kwargs["measurements"]
    assert [item.start.astimezone(UTC) for item in imported] == [
        start for start, _channel in _expected(kraken, since)
    ]
    assert min(item.start for item in imported) == local_midnight(supply_start)
    assert fractions[-1] == pytest.approx(1.0)
    assert coordinator.last_request is not None
    assert coordinator.last_request.finished_at is not None
    (record,) = _records(caplog, "Meridian rejected a window older than every")
    assert "KT-CT-4321" in record.getMessage()
    refused = [
        (request["startOn"], request["endOn"])
        for request in kraken.requests
        if kraken.refuses(
            date.fromisoformat(request["startOn"]), date.fromisoformat(request["endOn"])
        )
    ]
    day_before = (supply_start - timedelta(days=1)).isoformat()
    assert (day_before, day_before) in refused
    if days_ago == 10:
        # Two weeks, then the refused week's days back to two before the start.
        assert len(kraken.requests) == 8


@pytest.mark.asyncio
async def test_a_reconciliation_across_the_supply_start_keeps_its_stored_hours(
    hass,
) -> None:
    """
    The restart and weekly reconciliations meet the refusal too.

    Home Assistant has stored hours from the supply start 10 days ago, inside
    the refused week: they are no reason against it, because the refused
    days before them hold none.
    """
    supply_start = TODAY - timedelta(days=10)
    kraken = FakeKraken(
        NOW,
        history_start=local_midnight(supply_start),
        pre_supply_error="KT-CT-4321",
        pre_supply_start_on=True,
    )
    stored = frozenset(
        local_midnight(supply_start) + HOUR * offset for offset in range(24 * 9)
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - REVISION_OVERLAP, stored_energy=stored)

    assert result.walk.history_start is True
    assert [item.start.astimezone(UTC) for item in result.measurements] == [
        start for start, _channel in _expected(kraken, NOW - REVISION_OVERLAP)
    ]


@pytest.mark.asyncio
async def test_a_refused_start_with_a_stored_hour_before_it_fails(hass) -> None:
    """A stored hour before the refused days shows the history goes on: it fails."""
    supply_start = TODAY - timedelta(days=10)
    kraken = FakeKraken(
        NOW,
        history_start=local_midnight(supply_start),
        pre_supply_error="KT-CT-4321",
        pre_supply_start_on=True,
    )
    stored = frozenset({local_midnight(supply_start) - 30 * HOUR})
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync(), pytest.raises(MeridianGraphQLError):
        await _fetch(coordinator, NOW - REVISION_OVERLAP, stored_energy=stored)


@pytest.mark.parametrize(
    ("window", "today_empty"), [(3, False), (1, True)], ids=["third", "newest"]
)
@pytest.mark.asyncio
async def test_a_week_refused_once_is_fetched_by_its_days(
    hass, window: int, today_empty: bool
) -> None:
    """
    A week refused once, whose days all come back, is kept, and the walk goes on.

    The refusal was not about its dates: the 90 days are imported in full.
    The newest week's dates are asked for alone too, before any reading came
    back, and today's having none yet is no start of the history.
    """
    since = NOW - INITIAL_BACKFILL
    refused = _start_on(window)
    kraken = FakeKraken(
        NOW,
        days=400,
        gaps=[(local_midnight(TODAY), NOW + timedelta(days=1))] if today_empty else (),
    )

    async def refuse_the_week(**kwargs: Any) -> MeasurementPage:
        if kwargs["start_on"] == refused and kwargs["end_on"] != refused:
            raise MeridianGraphQLError("measurements", ("KT-CT-4321",))
        return await kraken.async_get_measurements(**kwargs)

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=refuse_the_week)
    )

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert coordinator.client.async_get_measurements.await_count == 13 + 7
    assert data.history_start is False
    assert (data.windows_ok, data.windows_failed) == (13, 0)
    imported = importer.await_args.kwargs["measurements"]
    assert [item.start.astimezone(UTC) for item in imported] == [
        start for start, _channel in _expected(kraken, since)
    ]


@pytest.mark.parametrize(
    "case", ["graphql-error", "client-error", "stored-hours", "statistics"], ids=str
)
@pytest.mark.asyncio
async def test_a_refused_window_of_one_day_before_any_reading(
    hass, caplog: pytest.LogCaptureFixture, case: str
) -> None:
    """
    A one-day newest window refused before any reading is not asked for again.

    Its date alone is the same request, which a GraphQL error is never sent
    again as. Refused with a GraphQL error, with no statistics, it is taken
    for a supply that has not started yet: no history yet, and one INFO line.
    A client error, stored hours, or statistics with no hour inside the
    span fail the sync.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    kraken = FakeKraken(NOW, days=400, codes={TODAY: "KT-CT-4321"})

    async def measurements(**kwargs: Any) -> MeasurementPage:
        if case == "client-error":
            raise _rejected(400)
        return await kraken.async_get_measurements(**kwargs)

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=measurements)
    )
    stored = frozenset({NOW.replace(minute=0) - 30 * HOUR})

    with _sync():
        if case == "graphql-error":
            result = await _fetch(coordinator, NOW - HOUR)
        else:
            with pytest.raises((MeridianGraphQLError, MeridianRejectedError)):
                await _fetch(
                    coordinator,
                    NOW - HOUR,
                    stored_energy=stored if case == "stored-hours" else frozenset(),
                    has_statistics=case in {"stored-hours", "statistics"},
                )

    assert coordinator.client.async_get_measurements.await_count == 1
    if case != "graphql-error":
        return
    assert kraken.requests == [
        {"startOn": TODAY.isoformat(), "endOn": TODAY.isoformat(), "last": 34}
    ]
    assert result.measurements == ()
    assert result.walk.history_start is True
    (record,) = _records(caplog, "Meridian rejected a window before")
    assert record.levelno == logging.INFO
    assert record.getMessage() == (
        "Meridian rejected a window before returning any reading (KT-CT-4321); "
        "this is taken as no usage history yet (direction=CONSUMPTION)"
    )


@pytest.mark.parametrize(
    "newest_stored", [20, 30], ids=["inside-the-update", "older-than-the-update"]
)
@pytest.mark.asyncio
async def test_an_hourly_update_of_a_direction_with_statistics_fails_on_a_refusal(
    hass, caplog: pytest.LogCaptureFixture, newest_stored: int
) -> None:
    """
    A direction with statistics has a history: a refusal is no supply to come.

    The hourly update reads the hours stored in its own 25 hours alone, and
    while Meridian publishes readings more than a day late it finds none.
    Meridian refusing its window, then today alone, with a GraphQL error
    with a code fails the update all the same, as when an hour is stored
    inside it, rather than be taken for no usage history yet.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    kraken = FakeKraken(
        NOW,
        days=400,
        codes={TODAY - timedelta(days=days): "KT-CT-4321" for days in range(2)},
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))
    coordinator._initial_refresh_complete = True
    coordinator._last_full_reconciliation = NOW - 2 * HOUR
    coordinator._last_targeted_reconciliation = NOW - 2 * HOUR
    latest = NOW.replace(minute=0) - newest_stored * HOUR
    energy_id, _ = consumption_ids(property_key(ACCOUNT, PROPERTY))
    # The stored hours a real recorder returns for the update's 25 hours.
    stored = {energy_id: frozenset({latest})} if newest_stored < 25 else None

    with (
        _sync(latest=latest, stored=stored) as importer,
        pytest.raises(UpdateFailed) as raised,
    ):
        await coordinator.async_fetch_and_import()

    assert raised.value.translation_key == "invalid_data"
    importer.assert_not_awaited()
    assert [(item["startOn"], item["endOn"]) for item in kraken.requests] == [
        ((TODAY - timedelta(days=1)).isoformat(), TODAY.isoformat()),
        (TODAY.isoformat(), TODAY.isoformat()),
    ]
    assert _records(caplog, "Meridian rejected a window") == []


@pytest.mark.parametrize("refused", ["week-and-today", "probe-fails"])
@pytest.mark.asyncio
async def test_a_refused_week_whose_days_fail_too_fails_the_sync(
    hass, refused: str
) -> None:
    """
    A newest week refused, then its newest day too, with a client error, fails.

    Nothing came back before either refusal, and a client error is what a
    request Meridian cannot take at all gets: the sync fails. So does a day
    refused inside a week whose next older day, asked for to confirm it,
    fails another way.
    """
    kraken = FakeKraken(NOW, days=400)
    refused_day = TODAY - timedelta(days=3)

    async def measurements(**kwargs: Any) -> MeasurementPage:
        multi_day = kwargs["start_on"] != kwargs["end_on"]
        if refused == "week-and-today":
            raise _rejected(400)
        if kwargs["start_on"] == TODAY - timedelta(days=6) and multi_day:
            raise MeridianGraphQLError("measurements", ("KT-CT-4321",))
        if kwargs["start_on"] == refused_day:
            raise MeridianGraphQLError("measurements", ("KT-CT-4321",))
        if kwargs["start_on"] == refused_day - timedelta(days=1):
            error = MeridianUpstreamError(500, gateway=False)
            error.__cause__ = MeridianHttpError(500, {})
            raise error
        return await kraken.async_get_measurements(**kwargs)

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=measurements)
    )

    with _sync() as importer, pytest.raises(UpdateFailed) as raised:
        await coordinator.async_fetch_and_import(setup_import=True)

    importer.assert_not_awaited()
    calls = coordinator.client.async_get_measurements.await_args_list
    if refused == "week-and-today":
        assert raised.value.translation_key == "cannot_connect"
        assert len(calls) == 2
        assert calls[1].kwargs["start_on"] == calls[1].kwargs["end_on"] == TODAY
    else:
        assert raised.value.translation_key == "upstream_unavailable"
        assert len(calls) == 6


@pytest.mark.asyncio
async def test_a_failure_of_the_newest_day_of_a_refused_week_fails_the_sync(
    hass,
) -> None:
    """
    The request for the refused week's newest day failing otherwise fails too.

    The walk stops there, with the two newer weeks imported.
    """
    refused = _start_on(3)
    kraken = FakeKraken(NOW, days=400, codes={refused: "KT-CT-4321"})

    async def measurements(**kwargs: Any) -> MeasurementPage:
        if kwargs["start_on"] == kwargs["end_on"] == refused + timedelta(days=6):
            error = MeridianUpstreamError(500, gateway=False)
            error.__cause__ = MeridianHttpError(500, {})
            raise error
        return await kraken.async_get_measurements(**kwargs)

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=measurements)
    )

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert data.units_degraded == 1
    (unit,) = coordinator._run_units
    assert unit.state.last_failure is not None
    assert unit.state.last_failure.reason == "upstream_unavailable"
    importer.assert_awaited_once()
    assert coordinator.client.async_get_measurements.await_count == 4


@pytest.mark.asyncio
async def test_a_refused_window_of_one_day_is_not_asked_for_again(hass) -> None:
    """
    The restart's oldest window is one day: its newest day is the window itself.

    The supply began on the first date of the second week, so the first two
    weeks come back complete and the day before them is refused. Asking for
    that day alone would send the refused request again, which a GraphQL
    error never is: the refusal is the start of the history.
    """
    kraken = FakeKraken(
        NOW,
        history_start=local_midnight(_start_on(2)),
        pre_supply_error="KT-CT-4321",
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - REVISION_OVERLAP)

    assert _windows(kraken) == [
        (TODAY - timedelta(days=6), TODAY),
        (TODAY - timedelta(days=13), TODAY - timedelta(days=7)),
        (TODAY - timedelta(days=14), TODAY - timedelta(days=14)),
    ]
    assert len(kraken.requests) == 3
    assert result.walk.history_start is True


@pytest.mark.parametrize("answer", ["refused-again", "empty"])
@pytest.mark.asyncio
async def test_a_supply_that_began_on_a_windows_first_date_is_found(
    hass, caplog: pytest.LogCaptureFixture, answer: str
) -> None:
    """
    The supply began at midnight on the first date of the newest weeks.

    Those weeks come back complete, then Meridian refuses the week before.
    Asked for its newest day alone, Meridian refuses that too, or returns no
    readings: the history starts there, and the newer weeks are imported.
    The empty day logs no page line, which would show when the history starts.
    """
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)
    since = NOW - INITIAL_BACKFILL
    history_start = local_midnight(_start_on(2))
    kraken = FakeKraken(
        NOW,
        history_start=history_start,
        pre_supply_error="KT-CT-4321" if answer == "refused-again" else None,
        codes={} if answer == "refused-again" else {_start_on(3): "KT-CT-4321"},
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))
    fractions: list[float] = []

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(
            setup_import=True, progress=fractions.append
        )

    assert len(kraken.requests) == 4
    assert data.history_start is True
    assert data.windows_failed == 1
    assert fractions == pytest.approx([1 / 13, 2 / 13, 3 / 13, 1.0])
    imported = importer.await_args.kwargs["measurements"]
    assert [item.start.astimezone(UTC) for item in imported] == [
        start for start, _channel in _expected(kraken, since)
    ]
    pages = [
        record.getMessage()
        for record in _records(caplog, "Meridian measurements page:")
    ]
    assert len(pages) == 2
    assert coordinator.last_request is not None
    assert coordinator.last_request.finished_at is not None
    walk = data.results[0].consumption_walk
    assert (walk.windows_requested, walk.requested_days) == (4, 22)


def _two_properties() -> MeridianAccount:
    """Return an account with a property supplied and one whose supply is to come."""
    return MeridianAccount(
        number=ACCOUNT,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id=PROPERTY,
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("synthetic-meter", "synthetic-icp", False),
                ),
            ),
            MeridianProperty(
                id="synthetic-property-2",
                address="2 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("synthetic-meter-2", "synthetic-icp-2", False),
                ),
            ),
        ),
    )


@pytest.mark.parametrize("setup_import", [True, False], ids=["setup", "after-setup"])
@pytest.mark.parametrize(
    "starts_before", [False, True], ids=["ends-before-supply", "starts-before-supply"]
)
@pytest.mark.asyncio
async def test_a_property_whose_supply_has_not_started_fails_nothing(
    hass, caplog: pytest.LogCaptureFixture, setup_import: bool, starts_before: bool
) -> None:
    """
    A property listed before its supply starts, next to one supplied, syncs.

    Such as the new address of a house move, added to the account before the
    move. Meridian refuses every window it is asked for: perhaps any that
    ends before the supply, perhaps any that starts before it. Before any
    reading, the newest week and then today alone are refused with a
    GraphQL error: there is no history yet, with one INFO line. The other
    property's history is imported, and the sync succeeds, at setup and in
    every update after it, rather than fail for every property until the
    supply starts.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    active = FakeKraken(NOW, days=400)
    to_come = FakeKraken(
        NOW,
        history_start=NOW + timedelta(days=5),
        pre_supply_error="KT-CT-4321",
        pre_supply_start_on=starts_before,
    )

    async def measurements(**kwargs: Any) -> MeasurementPage:
        kraken = active if kwargs["property_id"] == PROPERTY else to_come
        return await kraken.async_get_measurements(**kwargs)

    client = _client(active, measurements=measurements)
    client.async_get_accounts = AsyncMock(return_value=(_two_properties(),))
    coordinator = MeridianDataCoordinator(hass, client)

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=setup_import)
        later = await coordinator.async_fetch_and_import()

    assert data.sync_mode is SyncMode.INITIAL
    assert later.sync_mode is SyncMode.TIP
    assert len(active.requests) == 13 + 1
    # The newest week and today alone, then the hourly update's two dates and
    # today alone again.
    assert [(item["startOn"], item["endOn"]) for item in to_come.requests] == [
        ((TODAY - timedelta(days=6)).isoformat(), TODAY.isoformat()),
        (TODAY.isoformat(), TODAY.isoformat()),
        ((TODAY - timedelta(days=1)).isoformat(), TODAY.isoformat()),
        (TODAY.isoformat(), TODAY.isoformat()),
    ]
    # The hourly update's rows of the supplied property are those already
    # imported: it imports nothing.
    (first,) = importer.await_args_list
    energy_id, _ = consumption_ids(property_key(ACCOUNT, PROPERTY))
    assert first.kwargs["stat_energy_id"] == energy_id
    assert [item.start.astimezone(UTC) for item in first.kwargs["measurements"]] == [
        start for start, _channel in _expected(active, NOW - INITIAL_BACKFILL)
    ]
    assert data.history_start is True
    info = [
        record.getMessage()
        for record in caplog.records
        if record.name == _LOGGER_NAME and record.levelno >= logging.INFO
    ]
    assert info == [
        (
            "Meridian rejected a window before returning any reading (KT-CT-4321); "
            "this is taken as no usage history yet (direction=CONSUMPTION)"
        )
    ]


@pytest.mark.parametrize("setup_import", [True, False], ids=["setup", "after-setup"])
@pytest.mark.asyncio
async def test_rows_still_to_come_are_no_reading(
    hass, caplog: pytest.LogCaptureFixture, setup_import: bool
) -> None:
    """
    A meter silent for 30 days, with Meridian's estimates for the rest of today.

    Those rows are still to come, so they are no reading: the newest week is
    empty, and so are the next three. Before any reading, empty weeks are no
    start of the history: the walk goes on to the readings before the
    silence, and imports what one request for the 90 days would have.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    since = NOW - INITIAL_BACKFILL
    current_hour = NOW.replace(minute=0)
    silent_from = current_hour - timedelta(days=30)
    kraken = FakeKraken(NOW, days=400, gaps=[(silent_from, current_hour)])
    assert kraken.window_rows(TODAY, TODAY)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=setup_import)

    assert len(kraken.requests) == 13
    assert data.history_start is False
    assert data.results[0].consumption_walk.windows_empty == 4
    imported = importer.await_args.kwargs["measurements"]
    assert [item.start.astimezone(UTC) for item in imported] == [
        start for start, _channel in _expected(kraken, since)
    ]
    assert max(item.start for item in imported) < silent_from
    assert _records(caplog, "No Meridian usage history was found") == []


@pytest.mark.asyncio
async def test_a_history_of_estimates_still_to_come_says_none_was_found(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """A supply whose only rows are today's estimates still to come has no history."""
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    kraken = FakeKraken(NOW, history_start=NOW.replace(minute=0))
    assert kraken.window_rows(TODAY, TODAY)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    importer.assert_not_awaited()
    assert len(kraken.requests) == 13
    assert data.history_start is False
    (record,) = _records(caplog, "No Meridian usage history was found")
    assert record.levelno == logging.INFO
    assert "in the last 90 days" in record.getMessage()


@pytest.mark.asyncio
async def test_empty_weeks_apart_inside_the_history_are_crossed(hass) -> None:
    """
    Only two empty weeks in a row mark the start of the history.

    Readings, an empty week, readings, another empty week, readings: the
    walk crosses both, and imports the weeks on either side of each.
    """
    since = NOW - INITIAL_BACKFILL
    empty = (_start_on(3), _start_on(5))
    kraken = FakeKraken(NOW, days=400, empty_start_on=empty)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert len(kraken.requests) == 13
    assert data.history_start is False
    assert data.results[0].consumption_walk.windows_empty == 2
    imported = [
        item.start.astimezone(UTC)
        for item in importer.await_args.kwargs["measurements"]
    ]
    assert imported == [
        start
        for start, _channel in _expected(kraken, since)
        if not any(
            local_midnight(day) <= start < local_midnight(day + timedelta(days=7))
            for day in empty
        )
    ]
    assert min(imported) < local_midnight(empty[1])


@pytest.mark.asyncio
async def test_gaps_across_window_boundaries_are_no_excluded_end_date(hass) -> None:
    """
    Two weeks in a row without their last date, after weeks without their first hour.

    A gap across the boundary of two weeks leaves the older week without
    its last date, and the newer one without its first hour: no end date
    read as excluded, which leaves every week's first hour in place. Two
    such gaps in a row are crossed, and the rows around them imported.
    """
    gaps = [
        (
            local_midnight(_start_on(window) - timedelta(days=1)),
            local_midnight(_start_on(window)) + 6 * HOUR,
        )
        for window in (1, 2)
    ]
    kraken = FakeKraken(NOW, days=400, gaps=gaps)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync():
        result = await _fetch(coordinator, NOW - INITIAL_BACKFILL)

    assert len(kraken.requests) == 13
    assert result.walk.history_start is False
    assert [item.start.astimezone(UTC) for item in result.measurements] == [
        start for start, _channel in _expected(kraken, NOW - INITIAL_BACKFILL)
    ]


@pytest.mark.parametrize(("channels", "first_week_pages"), [(21, 17), (24, 19)])
@pytest.mark.asyncio
async def test_a_meter_with_many_channels_fits_its_newest_week(
    hass, channels: int, first_week_pages: int
) -> None:
    """
    The newest week is sent before the channels are known, in pages of 214 rows.

    21 channels need 17 such pages for it, 24 need 19: more than 16, but no
    more than the 11,904 rows a window may hold. The rows imported are
    those of one request for the 90 days.
    """
    kraken = FakeKraken(NOW, days=400, channels=channels)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        await coordinator.async_fetch_and_import(setup_import=True)

    newest_week = [
        request
        for request in kraken.requests
        if request["startOn"] == (TODAY - timedelta(days=6)).isoformat()
    ]
    assert len(newest_week) == first_week_pages
    assert {request["last"] for request in newest_week} == {214}
    imported = importer.await_args.kwargs["measurements"]
    assert len({item.channel_id for item in imported}) == channels
    assert sorted(item.start.astimezone(UTC) for item in imported) == [
        start for start, _channel in _expected(kraken, NOW - INITIAL_BACKFILL)
    ]


@pytest.mark.asyncio
async def test_setup_in_the_hour_after_nz_midnight_sends_13_weeks(hass) -> None:
    """
    No 14th request asks for a date no row kept is on.

    At 00:30 on 23 October 2026, 90 days back is 23:30 on 24 July, in
    standard time: the first hour kept is midnight after it. Setup asks for
    13 weeks, 91 dates from 25 July, as it does an hour later, and imports
    what one request for the 90 days would have.
    """
    for minutes in (0, 60):
        now = datetime(2026, 10, 22, 11, 30, tzinfo=UTC) + timedelta(minutes=minutes)
        kraken = FakeKraken(now, days=400)
        coordinator = MeridianDataCoordinator(hass, _client(kraken))
        fractions: list[float] = []

        with _sync(now) as importer:
            data = await coordinator.async_fetch_and_import(
                setup_import=True, progress=fractions.append
            )

        walk = data.results[0].consumption_walk
        assert (len(kraken.requests), walk.windows_requested) == (13, 13)
        assert walk.requested_days == 91
        assert _windows(kraken)[-1][0] == date(2026, 7, 25)
        assert fractions == pytest.approx([index / 13 for index in range(1, 14)])
        imported = importer.await_args.kwargs["measurements"]
        assert [item.start.astimezone(UTC) for item in imported] == [
            start for start, _channel in _expected(kraken, now - INITIAL_BACKFILL, now)
        ]
    assert min(item.start for item in imported) >= local_midnight(date(2026, 7, 25))


# -------------------------------------------------------- Kraken's limits


@pytest.mark.asyncio
async def test_kraken_rate_limit_stops_the_sync_for_an_hour_without_a_resend(
    hass,
) -> None:
    kraken = FakeKraken(NOW, codes={TODAY - timedelta(days=6): "KT-CT-1199"})
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync(), pytest.raises(UpdateFailed) as raised:
        await coordinator.async_fetch_and_import(setup_import=True)

    assert raised.value.translation_key == "rate_limited"
    assert raised.value.retry_after == 3600
    assert len(kraken.requests) == 1


@pytest.mark.parametrize("code", ["KT-CT-1188", "KT-CT-1189"])
@pytest.mark.asyncio
async def test_a_query_over_krakens_limits_is_asked_for_by_day_once(
    hass, caplog: pytest.LogCaptureFixture, code: str
) -> None:
    """
    A week Kraken refuses as too large is asked for by its days, once.

    The rest of the walk then asks for days too, and imports the 90 days.
    A second refusal, of one of those days, stops the walk with unusable
    data, as one did at once until now; the same request is never resent.
    """
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    kraken = FakeKraken(NOW, week_codes={TODAY - timedelta(days=6): code})
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    # The week, then the 91 dates of the 90 days one at a time.
    assert len(kraken.requests) == 92
    assert all(
        request["startOn"] == request["endOn"] for request in kraken.requests[1:]
    )
    assert len({request["startOn"] for request in kraken.requests[1:]}) == 91
    assert data.splits == 1
    imported = importer.await_args.kwargs["measurements"]
    assert _keys(imported) == [
        (start, imported[0].channel_id)
        for start, _ in _expected(kraken, NOW - INITIAL_BACKFILL)
    ]
    (line,) = _records(caplog, "Meridian refused a request for a week")
    assert code in line.getMessage()

    refused = FakeKraken(NOW, codes={TODAY - timedelta(days=6): code})
    coordinator = MeridianDataCoordinator(hass, _client(refused))
    with _sync(), pytest.raises(UpdateFailed) as raised:
        await coordinator.async_fetch_and_import(setup_import=True)

    assert raised.value.translation_key == "invalid_data"
    assert code in str(raised.value.__cause__)
    # The week, then its days from today back to its first, refused again.
    assert [request["startOn"] for request in refused.requests] == [
        (TODAY - timedelta(days=offset)).isoformat() for offset in (6, *range(7))
    ]


@pytest.mark.asyncio
async def test_a_date_refused_in_a_walk_by_day_with_readings_before_it_is_no_start(
    hass,
) -> None:
    """
    A walk asked for by day meets a date refused with a code, readings before it.

    The date before it is asked for too, and its readings show the history
    goes on: the walk stops at the refused date, as at any failure, and
    imports the newer dates, rather than take that date for the start of
    the history and keep the newer dates alone for good.
    """
    refused_on = TODAY - timedelta(days=20)
    kraken = FakeKraken(
        NOW,
        week_codes={TODAY - timedelta(days=6): "KT-CT-1188"},
        codes={refused_on: "KT-CT-4321"},
    )
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync() as importer:
        data = await coordinator.async_fetch_and_import(setup_import=True)

    asked = [request["startOn"] for request in kraken.requests]
    assert asked[-2:] == [
        refused_on.isoformat(),
        (refused_on - timedelta(days=1)).isoformat(),
    ]
    assert data.history_start is False
    assert data.units_degraded == 1
    imported = importer.await_args.kwargs["measurements"]
    assert min(item.start for item in imported) == local_midnight(
        refused_on + timedelta(days=1)
    )


# ------------------------------------------------- resends, pacing, budget


class _Clock:
    """A monotonic clock that only the waits and the service's answers move on."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.waits: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def wait(self, seconds: float) -> None:
        self.waits.append(round(seconds, 3))
        self.now += seconds


def _http_client(session: FakeMeridianSession) -> MeridianApiClient:
    tokens = MeridianTokenSet(
        id_token="synthetic-id-token",
        refresh_token="synthetic-refresh",
        expires_at=datetime.now(UTC) + timedelta(days=1),
        user_id="synthetic-user",
    )
    return MeridianApiClient(session, tokens=tokens)  # type: ignore[arg-type]


@contextmanager
def _timed(clock: _Clock) -> Iterator[None]:
    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(coordinator_module, "_async_wait", side_effect=clock.wait),
        patch.object(coordinator_module.random, "uniform", return_value=3.5),
    ):
        yield


def _answers(
    session: FakeMeridianSession, *answers: Response, seconds: float = 0.0
) -> Callable[[], int]:
    """
    Answer the first measurements requests with ``answers``, then normally.

    Each measurements request moves the clock on by ``seconds``. Return a
    function that counts the measurements requests sent.
    """
    pending = list(answers)
    sent = {"count": 0}
    clock: _Clock | None = getattr(session, "clock", None)

    def hook(operation: str, _variables: dict[str, Any]) -> Response | None:
        if operation != "measurements":
            return None
        sent["count"] += 1
        if clock is not None:
            clock.now += seconds
        return pending.pop(0) if pending else None

    session.hook = hook
    return lambda: sent["count"]


def _session(clock: _Clock, **kraken: Any) -> FakeMeridianSession:
    session = FakeMeridianSession(NOW, kraken=FakeKraken(NOW, **kraken))
    session.clock = clock  # type: ignore[attr-defined]
    return session


def _status(status: int) -> Response:
    return Response(b"<html>synthetic</html>", status=status, headers={})


async def _run(
    hass, session: FakeMeridianSession, *, setup: bool, budget: float | None = None
) -> Any:
    coordinator = MeridianDataCoordinator(hass, _http_client(session))
    with _sync():
        return coordinator, await coordinator.async_fetch_and_import(
            setup_import=setup, time_budget=budget
        )


def _failed_pages(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the debug lines of the failed measurements pages."""
    return [
        record.getMessage()
        for record in _records(caplog, "Meridian measurements page failed:")
    ]


@pytest.mark.parametrize("setup", [True, False], ids=["setup", "polling"])
@pytest.mark.parametrize("status", [408, 502, 503, 520, 521, 522, 523])
@pytest.mark.asyncio
async def test_a_passing_failure_is_sent_once_more_after_a_short_pause(
    hass, caplog: pytest.LogCaptureFixture, setup: bool, status: int
) -> None:
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)
    clock = _Clock()
    session = _session(clock, days=3)
    sent = _answers(session, _status(status))

    with _timed(clock):
        _coordinator, data = await _run(hass, session, setup=setup)

    # Window 1 twice, then the two empty windows before the history start.
    assert sent() == 4
    assert data.retries == 1
    assert clock.waits[0] == 3.5
    (failed,) = _failed_pages(caplog)
    assert f" status={status} action=retry " in failed


@pytest.mark.asyncio
async def test_the_pause_before_a_resend_is_between_2_and_5_seconds(hass) -> None:
    clock = _Clock()
    session = _session(clock, days=3)
    _answers(session, _status(502))

    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(coordinator_module, "_async_wait", side_effect=clock.wait),
    ):
        _coordinator, data = await _run(hass, session, setup=False)

    assert data.retries == 1
    assert 2.0 <= clock.waits[0] <= 5.0


@pytest.mark.asyncio
async def test_a_second_passing_failure_fails_the_sync(hass) -> None:
    clock = _Clock()
    session = _session(clock, days=3)
    sent = _answers(session, _status(503), _status(503))

    with _timed(clock), pytest.raises(UpdateFailed) as raised:
        await _run(hass, session, setup=True)

    assert raised.value.translation_key == "upstream_unavailable"
    assert sent() == 2


@pytest.mark.parametrize("seconds", [5.0, 90.0])
@pytest.mark.asyncio
async def test_a_gateway_timeout_in_setup_probes_the_newest_day_after_the_cooldown(
    hass, caplog: pytest.LogCaptureFixture, seconds: float
) -> None:
    """
    No request goes to the property for 75 s, or as long as the failure took.

    The service behind the gateway may still be working on the first
    request, so the probe waits rather than stack a second one on it, and
    asks for the week's newest day alone; its other days follow one at a
    time, then the walk goes back to weeks. The failed request has ended by
    then: during the cooldown, a failure or a cancellation does not name it
    as still waiting for an answer.
    """
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)
    clock = _Clock()
    session = _session(clock, days=3)
    sent = _answers(session, _status(504), seconds=seconds)
    coordinator = MeridianDataCoordinator(hass, _http_client(session))
    during_waits: list[Any] = []

    async def wait(wait_seconds: float) -> None:
        during_waits.append(coordinator.last_request)
        await clock.wait(wait_seconds)

    with (
        _timed(clock),
        patch.object(coordinator_module, "_async_wait", side_effect=wait),
        _sync(),
    ):
        # Without a time budget: 10 requests of 90 s each would outlast it.
        data = await coordinator.async_fetch_and_import(setup_import=True)

    # The week, its 7 days, then the two empty weeks before the history.
    assert sent() == 10
    assert data.retries == 0
    assert data.splits == 1
    assert clock.waits[0] == max(75.0, seconds)
    failed = during_waits[0]
    assert (failed.operation, failed.window, failed.page) == ("measurements", 1, 1)
    assert failed.finished_at is not None
    assert failed.finished_at - failed.started_at == seconds
    (line,) = _failed_pages(caplog)
    assert " status=504 action=probe_1d " in line
    assert data.results[0].consumption_walk.windows_requested == 10


@pytest.mark.parametrize("status", [504, 524])
@pytest.mark.asyncio
async def test_a_gateway_timeout_in_polling_is_left_to_the_next_poll(
    hass, caplog: pytest.LogCaptureFixture, status: int
) -> None:
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)
    clock = _Clock()
    session = _session(clock, days=3)
    sent = _answers(session, _status(status))

    with _timed(clock), pytest.raises(UpdateFailed) as raised:
        await _run(hass, session, setup=False)

    assert raised.value.translation_key == "upstream_timeout"
    assert sent() == 1
    (line,) = _failed_pages(caplog)
    assert f" status={status} action=stop " in line


@pytest.mark.parametrize(
    "answer", ["rate_limit", "graphql_error", "rejected", "server_error"]
)
@pytest.mark.asyncio
async def test_rate_limits_graphql_errors_and_client_errors_are_never_resent(
    hass, answer: str
) -> None:
    """
    A server error other than a gateway's or an overloaded service's is not.

    A GraphQL error or a client error for the newest week may refuse dates
    before a supply that started in it: its newest date is asked for alone,
    a different request. When that is refused too, a client error fails the
    sync, and a GraphQL error is taken for a supply not started yet.
    """
    clock = _Clock()
    session = _session(clock, days=3)
    response = {
        "rate_limit": Response(b"", status=429, headers={"Retry-After": "120"}),
        "graphql_error": json_response(
            {"errors": [{"message": "x", "extensions": {"errorCode": "KT-CT-4321"}}]}
        ),
        "rejected": _status(400),
        "server_error": _status(500),
    }[answer]
    sent = _answers(session, response, response)
    asked: list[tuple[Any, ...]] = []
    answer_first = session.hook

    def record(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation == "measurements":
            asked.append(
                tuple(variables.get(name) for name in ("startOn", "endOn", "before"))
            )
        return answer_first(operation, variables)

    session.hook = record
    fails = pytest.raises(UpdateFailed) if answer != "graphql_error" else nullcontext()

    with _timed(clock), fails:
        await _run(hass, session, setup=True, budget=SETUP_IMPORT_BUDGET_SECONDS)

    assert sent() == (2 if answer in {"graphql_error", "rejected"} else 1)
    assert len(set(asked)) == len(asked)


@pytest.mark.asyncio
async def test_a_gateway_timeout_needs_no_session_refresh(hass) -> None:
    """A resend is a new call: it reuses the session, which is still valid."""
    clock = _Clock()
    session = _session(clock, days=3)
    _answers(session, _status(504))
    posts: list[str] = []
    real_post = session.post

    def post(url: str, **kwargs: Any) -> Any:
        posts.append(url)
        return real_post(url, **kwargs)

    session.post = post  # type: ignore[method-assign]

    with _timed(clock):
        await _run(hass, session, setup=True, budget=SETUP_IMPORT_BUDGET_SECONDS)

    assert not any(url.startswith(FIREBASE_REFRESH_URL) for url in posts)


@pytest.mark.asyncio
async def test_a_rejected_resend_gets_its_own_session_refresh(hass) -> None:
    """
    504, then 401, then 200: one forced refresh and no reauthentication.

    The probe after the 504 is a new call to the client, whose own loop
    renews a rejected session once; reusing that loop for it would have
    forced a refresh on the 504 and taken the 401 as a final rejection.
    """
    clock = _Clock()
    session = _session(clock, days=3)
    sent = _answers(session, _status(504), Response(b"", status=401, headers={}))
    posts: list[str] = []
    real_post = session.post

    def post(url: str, **kwargs: Any) -> Any:
        posts.append(url)
        return real_post(url, **kwargs)

    session.post = post  # type: ignore[method-assign]

    with _timed(clock):
        _coordinator, data = await _run(
            hass, session, setup=True, budget=SETUP_IMPORT_BUDGET_SECONDS
        )

    assert sum(url.startswith(FIREBASE_REFRESH_URL) for url in posts) == 1
    # The week, the probe rejected and sent again with a new session, the
    # week's other 6 days, then the two empty weeks.
    assert sent() == 11
    assert data.splits == 1


@pytest.mark.parametrize(
    ("status", "budget", "resent", "error"),
    [
        (504, 154.0, False, UpdateFailed),
        (504, 155.0, True, UpdateFailed),
        (503, 83.0, False, UpdateFailed),
        (503, 84.0, True, None),
    ],
)
@pytest.mark.asyncio
async def test_a_resend_needs_time_left_for_another_request(
    hass, status: int, budget: float, resent: bool, error: type[Exception] | None
) -> None:
    """
    A second request must still leave 80 s after its wait: 75 s, or 3.5 s here.

    After a gateway timeout that request is the probe of the week's newest
    day; after another passing failure, the request sent again. With just
    enough left, it goes, and then the next request cannot start: less than
    80 s remain. The probe's day alone is not the newest week, so setup
    fails; the week sent again is, so setup keeps it. Without, the gateway
    timeout's probe cannot start either, while the other failure stands.
    Setup fails with the gateway timeout, not the time budget: the timeout
    and its cooldown cost the time, and the failure form then offers to
    continue without history. Either failure is raised as it came, from
    the answer it was raised for.
    """
    clock = _Clock()
    session = _session(clock, days=3)
    sent = _answers(session, _status(status))

    with (
        _timed(clock),
        pytest.raises(error) if error else nullcontext() as raised,
    ):
        await _run(hass, session, setup=True, budget=budget)

    assert sent() == (2 if resent else 1)
    if raised is not None:
        cause = raised.value.__cause__
        assert isinstance(cause, MeridianUpstreamError)
        assert cause.status == status
        assert raised.value.translation_key == _upstream_key(status)
        assert isinstance(cause.__cause__, MeridianHttpError)
        assert cause.__cause__.status == status


def _upstream_key(status: int) -> str:
    return "upstream_timeout" if status == 504 else "upstream_unavailable"


@pytest.mark.asyncio
async def test_no_request_starts_with_less_than_80_seconds_left(hass) -> None:
    """
    The first request always starts; later ones need 80 s of the budget.

    What came back before is imported: setup keeps the weeks fetched.
    """
    clock = _Clock()
    session = _session(clock)
    sent = _answers(session, seconds=30.0)
    coordinator = MeridianDataCoordinator(hass, _http_client(session))

    with _timed(clock), _sync():
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=171.0
        )

    # Starts at 0, 30, 60 and 90 s; at 120 s only 51 s would remain.
    assert sent() == 4
    assert data.units_degraded == 1

    clock.now += 1000
    first = _session(clock)
    first_sent = _answers(first, seconds=30.0)
    coordinator = MeridianDataCoordinator(hass, _http_client(first))
    with _timed(clock), _sync():
        data = await coordinator.async_fetch_and_import(
            setup_import=True, time_budget=10.0
        )
    assert first_sent() == 1
    assert data.units_degraded == 1


@pytest.mark.asyncio
async def test_measurements_requests_start_at_least_a_second_apart(hass) -> None:
    clock = _Clock()
    session = _session(clock)
    sent = _answers(session, seconds=0.25)

    with _timed(clock):
        await _run(hass, session, setup=False)

    assert sent() == 13
    assert clock.waits == [0.75] * 12


@pytest.mark.asyncio
async def test_the_wait_between_requests_sleeps() -> None:
    with patch.object(coordinator_module.asyncio, "sleep", new=AsyncMock()) as sleep:
        await _REAL_WAIT(1.5)

    sleep.assert_awaited_once_with(1.5)


# ---------------------------------------------------------------- overlap


@pytest.mark.asyncio
async def test_the_one_day_overlap_held_in_reserve_counts_each_hour_once(
    hass,
) -> None:
    """
    Switched on, neighbouring windows share a day; its readings count once.

    It is held in reserve for a live check that finds Meridian's window edges
    shifted. Identical repeated readings are dropped, so the rows are the
    same as without it.
    """
    kraken = FakeKraken(NOW)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))

    with _sync(), patch.object(windows_module, "WINDOW_OVERLAP_DAYS", 1):
        result = await _fetch(coordinator, NOW - REVISION_OVERLAP)

    sent = _windows(kraken)
    for (newer_start, _), (_, older_end) in pairwise(sent):
        assert older_end == newer_start
    keys = _keys(result.measurements)
    assert len(keys) == len(set(keys))
    assert [start for start, _ in keys] == [
        start for start, _ in _expected(kraken, NOW - REVISION_OVERLAP)
    ]


@pytest.mark.asyncio
async def test_overlapping_windows_that_disagree_fail(hass) -> None:
    kraken = FakeKraken(NOW)
    sent = {"count": 0}

    async def revise_the_second_window(**kwargs: Any) -> MeasurementPage:
        page = await kraken.async_get_measurements(**kwargs)
        sent["count"] += 1
        if sent["count"] == 1:
            return page
        rows = tuple(
            MeridianMeasurement(
                start=item.start,
                end=item.end,
                value_kwh=item.value_kwh * 2,
                quality=item.quality,
                direction=item.direction,
                channel_id=item.channel_id,
                cost_cents=item.cost_cents,
            )
            for item in page.measurements
        )
        return MeasurementPage(rows, page.has_previous_page, page.start_cursor)

    coordinator = MeridianDataCoordinator(
        hass, _client(kraken, measurements=revise_the_second_window)
    )

    with (
        _sync(),
        patch.object(windows_module, "WINDOW_OVERLAP_DAYS", 1),
        pytest.raises(ValueError, match="different readings"),
    ):
        await _fetch(coordinator, NOW - REVISION_OVERLAP)


# --------------------------------------------------------------- progress


@pytest.mark.parametrize(
    ("days", "feed_in", "pre_supply_error", "reported"),
    [
        (400, False, None, [index / 13 for index in range(1, 14)]),
        # Three weeks with readings, two empty ones, then the rest skipped.
        (20, False, None, [1 / 13, 2 / 13, 3 / 13, 4 / 13, 5 / 13, 1.0]),
        # Three weeks with readings, one rejected, then the rest skipped.
        (20, False, "KT-CT-4321", [1 / 13, 2 / 13, 3 / 13, 4 / 13, 1.0]),
        (400, True, None, [index / 26 for index in range(1, 27)]),
    ],
    ids=["full-history", "short-history", "rejected-before-supply", "feed-in"],
)
@pytest.mark.asyncio
async def test_progress_counts_the_windows_done_of_those_planned(
    hass,
    days: int,
    feed_in: bool,
    pre_supply_error: str | None,
    reported: list[float],
) -> None:
    kraken = FakeKraken(NOW, days=days, pre_supply_error=pre_supply_error)
    coordinator = MeridianDataCoordinator(hass, _client(kraken, feed_in=feed_in))
    fractions: list[float] = []

    with _sync():
        await coordinator.async_fetch_and_import(
            setup_import=True, progress=fractions.append
        )

    assert fractions == pytest.approx(reported)


@pytest.mark.asyncio
async def test_a_sync_across_new_zealand_midnight_sends_the_windows_it_planned(
    hass,
) -> None:
    """
    A sync reads the clock once: the windows it sends are those it counted.

    Setup starts at 23:50 in New Zealand, and its walk starts after midnight.
    It still sends 13 weeks for its 90 days, ending on the date it started,
    and its progress reaches 1.0 with the last of them, not before.
    """
    started = datetime(2026, 10, 16, 10, 50, tzinfo=UTC)
    later = started + timedelta(minutes=12)
    assert later.astimezone(NZ).date() > started.astimezone(NZ).date()
    kraken = FakeKraken(started)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))
    fractions: list[float] = []
    readings = iter([started])

    with (
        _sync(started),
        patch.object(
            coordinator_module, "_utcnow", side_effect=lambda: next(readings, later)
        ),
    ):
        await coordinator.async_fetch_and_import(
            setup_import=True, progress=fractions.append
        )

    sent = _windows(kraken)
    assert len(sent) == len(kraken.requests) == 13
    assert sent[0][1] == started.astimezone(NZ).date()
    assert fractions == pytest.approx([index / 13 for index in range(1, 14)])


@pytest.mark.asyncio
async def test_statistics_that_turn_14_days_old_during_a_sync_keep_its_mode(
    hass,
) -> None:
    """
    A direction's mode, and so its windows, follow the sync's clock reading.

    After a restart, statistics just under 14 days old are walked back 14
    days, as the sync planned when it started, though they are more than
    14 days old by the time the walk starts.
    """
    started = NOW
    later = started + timedelta(minutes=12)
    kraken = FakeKraken(started)
    coordinator = MeridianDataCoordinator(hass, _client(kraken))
    fractions: list[float] = []
    readings = iter([started])

    with (
        _sync(started, latest=started - REVISION_OVERLAP + timedelta(minutes=6)),
        patch.object(
            coordinator_module, "_utcnow", side_effect=lambda: next(readings, later)
        ),
    ):
        data = await coordinator.async_fetch_and_import(progress=fractions.append)

    assert data.sync_mode is SyncMode.RESTART
    assert data.results[0].consumption_walk.requested_days == 15
    assert len(kraken.requests) == 3
    assert fractions == pytest.approx([1 / 3, 2 / 3, 1.0])


# ------------------------------------------------------- load on Meridian


@pytest.mark.parametrize(
    ("case", "requests"),
    [
        ("setup", 13),
        ("setup-20-day-history", 5),
        ("setup-no-history", 13),
        ("setup-two-channels", 14),
        ("setup-five-channels", 28),
        ("setup-nine-channels", 44),
        ("setup-21-channels", 77),
        ("setup-24-channels", 91),
        ("setup-gateway-timeouts", 2),
        ("restart", 3),
        ("restart-no-history", 13),
        ("full-reconciliation", 3),
        ("targeted-reconciliation", 1),
        ("tip", 1),
    ],
)
@pytest.mark.asyncio
async def test_requests_per_sync_mode(hass, case: str, requests: int) -> None:
    """
    The load each mode puts on Meridian, as a regression guard.

    One property, consumption only. Each request's list holds at most a
    week of rows per channel, where it used to hold the whole history. A
    page holds 744 rows at most, so a week takes one page for every 744
    rows it holds: two for five to eight channels, three for nine to
    thirteen. The newest week, read before the channels are known, takes
    pages of 214 rows: eight for nine channels, 17 for 21 and 19 for 24. A
    direction with no readings in 90 days, such as a property whose supply
    ended or an export register that never exported, walks all 13 weeks at
    setup, and again after each restart, which cannot tell it from one
    that no setup searched.
    """
    channels = {
        "setup-two-channels": 2,
        "setup-five-channels": 5,
        "setup-nine-channels": 9,
        "setup-21-channels": 21,
        "setup-24-channels": 24,
    }.get(case, 1)
    kraken = FakeKraken(
        NOW,
        days={"setup-20-day-history": 20}.get(case, 400),
        history_start=(
            NOW + timedelta(days=2) if case.endswith("no-history") else None
        ),
    )
    kraken.channels = channels

    async def measurements(**kwargs: Any) -> MeasurementPage:
        if case == "setup-gateway-timeouts":
            error = MeridianUpstreamError(504, gateway=True)
            error.__cause__ = MeridianHttpError(504, {})
            raise error
        return await kraken.async_get_measurements(**kwargs)

    client = _client(kraken, measurements=measurements)
    coordinator = MeridianDataCoordinator(hass, client)
    since = {
        "restart": REVISION_OVERLAP,
        "full-reconciliation": REVISION_OVERLAP,
        "targeted-reconciliation": TARGETED_RECONCILIATION_MINIMUM,
        "tip": TIP_WINDOW,
    }.get(case)

    with _sync():
        if since is not None:
            await _fetch(coordinator, NOW - since)
        elif case == "restart-no-history":
            data = await coordinator.async_fetch_and_import()
            assert data.sync_mode is SyncMode.INITIAL
        elif case == "setup-gateway-timeouts":
            with pytest.raises(UpdateFailed):
                await coordinator.async_fetch_and_import(
                    setup_import=True, time_budget=SETUP_IMPORT_BUDGET_SECONDS
                )
        else:
            await coordinator.async_fetch_and_import(setup_import=True)

    assert client.async_get_measurements.await_count == requests
    assert all(rows <= 169 * channels for rows in kraken.materialized)
    assert all(request["last"] <= 744 for request in kraken.requests)
