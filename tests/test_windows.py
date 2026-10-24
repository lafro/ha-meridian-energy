"""Tests for window planning, page sizes, window row checks and import rules."""

from __future__ import annotations

import math
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise

import pytest

from custom_components.meridian_energy.const import (
    INITIAL_BACKFILL,
    REVISION_OVERLAP,
    TARGETED_RECONCILIATION_MINIMUM,
    TIP_WINDOW,
)
from custom_components.meridian_energy.models import MeridianMeasurement
from custom_components.meridian_energy.windows import (
    NZ,
    CostPlan,
    MeasurementWindow,
    StoredHours,
    WindowRows,
    cost_plan,
    first_date,
    importable,
    local_midnight,
    page_limit,
    page_size,
    plan_windows,
    rows_per_hour,
)

# 16:30 on Friday 16 October 2026 in New Zealand.
NOW = datetime(2026, 10, 16, 3, 30, tzinfo=UTC)
HOUR = timedelta(hours=1)


def _row(
    start: datetime,
    *,
    channel: str = "synthetic-channel",
    quality: str = "ACTUAL",
    value: str = "0.5",
    cost: str | None = "15",
) -> MeridianMeasurement:
    return MeridianMeasurement(
        start=start,
        end=start + HOUR,
        value_kwh=Decimal(value),
        quality=quality,
        direction="CONSUMPTION",
        channel_id=channel,
        cost_cents=None if cost is None else Decimal(cost),
    )


@pytest.mark.parametrize(
    ("since", "count", "days"),
    [
        (INITIAL_BACKFILL, 13, 91),
        (REVISION_OVERLAP, 3, 15),
        (TARGETED_RECONCILIATION_MINIMUM, 1, 3),
        (TIP_WINDOW, 1, 2),
    ],
    ids=["initial", "restart-and-full", "targeted", "tip"],
)
def test_windows_cover_each_mode_newest_first_without_overlap(
    since: timedelta, count: int, days: int
) -> None:
    windows = plan_windows(NOW - since, NOW)

    assert len(windows) == count
    assert [window.index for window in windows] == list(range(1, count + 1))
    assert {window.count for window in windows} == {count}
    # The newest ends today and the oldest starts on the cutoff's NZ date.
    assert windows[0].end_on == NOW.astimezone(NZ).date()
    assert windows[-1].start_on == (NOW - since).astimezone(NZ).date()
    for newer, older in pairwise(windows):
        assert older.end_on == newer.start_on - timedelta(days=1)
        assert older.end == newer.start
    assert all(1 <= window.days <= 7 for window in windows)
    assert sum(window.days for window in windows) == days


@pytest.mark.parametrize(
    ("now", "since", "count", "days"),
    [
        # 00:30 on 23 October 2026, daylight time: 90 days back is 23:30 on
        # 24 July, standard time.
        (datetime(2026, 10, 22, 11, 30, tzinfo=UTC), INITIAL_BACKFILL, 13, 91),
        # 00:30 on 20 December 2026: 90 days back is 23:30 on 20 September.
        (datetime(2026, 12, 19, 11, 30, tzinfo=UTC), INITIAL_BACKFILL, 13, 91),
        # 00:30 on 28 September 2026, the day after the clocks went forward:
        # 24 hours back is 23:30 on the 26th.
        (datetime(2026, 9, 27, 11, 30, tzinfo=UTC), TIP_WINDOW, 1, 2),
    ],
    ids=["initial-october", "initial-december", "tip-after-the-change"],
)
def test_a_cutoff_in_the_hour_before_nz_midnight_asks_for_no_date_of_its_own(
    now: datetime, since: timedelta, count: int, days: int
) -> None:
    """
    No window asks for a date no row kept can be on.

    From the September change until about late December, the hour after New
    Zealand midnight puts a cutoff 90 days back at 23:30 on the day before
    the first date kept, in standard time: the first hour kept is midnight
    after it. The windows start on that date, so there are 13, not a 14th
    for that one date, whose rows would all have been left out.
    """
    cutoff = now - since
    assert cutoff.astimezone(NZ).hour == 23

    windows = plan_windows(cutoff, now)

    assert (len(windows), sum(window.days for window in windows)) == (count, days)
    assert windows[-1].start_on == cutoff.astimezone(NZ).date() + timedelta(days=1)
    assert windows[-1].start <= cutoff + HOUR


def test_the_first_date_is_that_of_the_first_whole_hour_kept() -> None:
    day = date(2026, 7, 25)
    midnight = local_midnight(day)

    assert first_date(midnight) == day
    assert first_date(midnight - HOUR) == day - timedelta(days=1)
    assert first_date(midnight - timedelta(minutes=30)) == day
    assert first_date(midnight + timedelta(minutes=1)) == day


def test_a_window_runs_from_nz_midnight_to_nz_midnight() -> None:
    window = MeasurementWindow(date(2026, 10, 10), date(2026, 10, 16), 1, 1)

    assert window.start == datetime(2026, 10, 9, 11, tzinfo=UTC)
    assert window.end == datetime(2026, 10, 16, 11, tzinfo=UTC)
    assert window.last_hour == datetime(2026, 10, 16, 10, tzinfo=UTC)
    assert window.hours == 168
    assert window.days == 7
    assert local_midnight(date(2026, 10, 10)) == window.start


def test_windows_across_daylight_saving_hold_167_or_169_hours() -> None:
    spring = plan_windows(NOW - INITIAL_BACKFILL, NOW)
    by_dates = {(window.start_on, window.end_on): window.hours for window in spring}
    # The clocks went forward on 27 September 2026.
    assert by_dates[(date(2026, 9, 26), date(2026, 10, 2))] == 167
    # And back on 4 April 2027.
    autumn_now = datetime(2027, 4, 8, 3, 30, tzinfo=UTC)
    autumn = plan_windows(autumn_now - REVISION_OVERLAP, autumn_now)
    assert [window.hours for window in autumn] == [169, 168, 24]


def test_the_one_day_overlap_held_in_reserve() -> None:
    """Switched on, each older window ends on the day the newer one starts."""
    windows = plan_windows(NOW - REVISION_OVERLAP, NOW, overlap=1)

    for newer, older in pairwise(windows):
        assert older.end_on == newer.start_on
    assert windows[-1].start_on == (NOW - REVISION_OVERLAP).astimezone(NZ).date()
    with pytest.raises(ValueError, match="overlap"):
        plan_windows(NOW - REVISION_OVERLAP, NOW, overlap=6)


def test_a_cutoff_after_now_still_gives_todays_window() -> None:
    (window,) = plan_windows(NOW + timedelta(days=2), NOW)

    assert window.start_on == window.end_on == NOW.astimezone(NZ).date()


@pytest.mark.parametrize(
    ("hours", "density", "size"),
    [
        (168, 1.0, 214),
        (24, 1.0, 34),
        (48, 1.0, 64),
        (168, 2.0, 424),
        (168, 0.4, 214),
        (1, 1.0, 24),
        (169, 8.0, 744),
    ],
)
def test_page_size_has_headroom_and_bounds(
    hours: float, density: float, size: int
) -> None:
    assert page_size(hours, density) == size


@pytest.mark.parametrize(
    ("last", "pages"), [(744, 16), (424, 29), (214, 56), (34, 351)]
)
def test_a_window_may_hold_16_pages_of_744_rows(last: int, pages: int) -> None:
    """A smaller page size allows as many more pages: 11,904 rows in all."""
    assert page_limit(last) == pages
    assert (pages - 1) * last < 16 * 744 <= pages * last


def test_a_first_window_of_many_channels_fits_its_pages() -> None:
    """
    The newest week is sent with pages for one channel, before they are known.

    A week of 70 channels, 11,830 rows, still fits; 16 pages of 214 rows,
    as before, held 20 channels at most.
    """
    last = page_size(168, 1.0)

    assert math.ceil(169 * 70 / last) <= page_limit(last)
    assert math.ceil(168 * 21 / last) > 16


def test_rows_per_hour_counts_channels() -> None:
    start = NOW.replace(minute=0)
    rows = tuple(
        _row(start - HOUR * hour, channel=channel)
        for hour in range(3)
        for channel in ("a", "b")
    )

    assert rows_per_hour(rows) == 2.0
    assert rows_per_hour(rows[:3]) == 1.5
    assert rows_per_hour(()) == 0.0


def _window_rows() -> WindowRows:
    window = MeasurementWindow(date(2026, 10, 3), date(2026, 10, 9), 2, 3)
    return WindowRows(window, since=window.start + 2 * HOUR, now=NOW)


def test_window_rows_leave_out_rows_before_the_window_and_the_cutoff() -> None:
    rows = _window_rows()
    start = rows.window.start

    kept, before_window = rows.add_page(
        (
            _row(start - 2 * HOUR),
            _row(start - HOUR),
            _row(start),  # inside the window, before the cutoff
            _row(start + 2 * HOUR),
            _row(rows.window.last_hour),
        )
    )

    assert (kept, before_window) == (2, 2)
    assert rows.before_window == 2
    assert rows.inside == 3
    assert rows.oldest == start
    assert rows.first_hour is True
    assert rows.last_hour is True
    assert rows.pages == 1
    assert rows.received == 5


def test_window_rows_fail_on_past_rows_after_the_window() -> None:
    rows = _window_rows()

    with pytest.raises(ValueError, match="after the requested window"):
        rows.add_page((_row(rows.window.end),))


def test_window_rows_leave_out_rows_still_to_come() -> None:
    now = NOW.replace(minute=0)
    (window,) = plan_windows(now - TIP_WINDOW, now)
    rows = WindowRows(window, since=now - TIP_WINDOW, now=now)

    kept, _ = rows.add_page(
        (_row(now - HOUR), _row(now), _row(window.last_hour), _row(window.end))
    )

    # The hour in progress and the rest of today are not over; tomorrow's row
    # is after the window but still to come, so it is left out, not an error.
    # Only the hour that is over is a reading; the last hour still came back.
    assert kept == 1
    assert rows.inside == 1
    assert rows.oldest == now - HOUR
    assert rows.last_hour is True


def test_a_window_of_rows_still_to_come_returned_no_reading() -> None:
    """
    Estimates for the rest of today are no reading: the window is empty.

    They still count for the pages' progress, and for the edge hours.
    """
    now = NOW.replace(minute=0)
    (window,) = plan_windows(now - timedelta(hours=1), now)
    rows = WindowRows(window, since=now - HOUR, now=now)
    future = (_row(now), _row(now + HOUR), _row(window.last_hour))

    kept, _ = rows.add_page(future)

    assert kept == 0
    assert (rows.inside, rows.oldest, rows.last_day) == (0, None, False)
    assert rows.last_hour is True
    assert rows.received == 3
    assert not rows.holds_new_rows(future)
    assert rows.returned({(now, "synthetic-channel")})


def test_window_rows_prefer_an_actual_reading_and_count_it_once() -> None:
    rows = _window_rows()
    hour = rows.window.start + 3 * HOUR

    first, _ = rows.add_page((_row(hour, quality="ESTIMATE"),))
    second, _ = rows.add_page((_row(hour, value="0.7"), _row(hour, value="0.7")))
    third, _ = rows.add_page((_row(hour, quality="ESTIMATE", value="0.1"),))

    assert (first, second, third) == (1, 0, 0)
    (kept,) = rows.rows.values()
    assert (kept.quality, kept.value_kwh) == ("ACTUAL", Decimal("0.7"))


@pytest.mark.parametrize(
    "page",
    [
        # An aggregate reading next to an identified channel, either way round.
        ("aggregate", "identified"),
        ("identified", "aggregate"),
        # Two aggregate readings of one quality cannot be told apart.
        ("aggregate", "aggregate"),
    ],
)
def test_window_rows_reject_hours_whose_readings_cannot_be_told_apart(
    page: tuple[str, str],
) -> None:
    rows = _window_rows()
    hour = rows.window.start + 3 * HOUR

    with pytest.raises(ValueError, match="ambiguous"):
        rows.add_page(tuple(_row(hour, channel=channel) for channel in page))


def test_window_rows_reject_one_channel_with_two_values() -> None:
    rows = _window_rows()
    hour = rows.window.start + 3 * HOUR

    with pytest.raises(ValueError, match="ambiguous"):
        rows.add_page((_row(hour), _row(hour, value="0.9")))


def test_window_rows_are_checked_within_their_window_only() -> None:
    """An hour seen by another window's rows is not this window's concern."""
    rows = _window_rows()
    other = _window_rows()
    hour = rows.window.start + 3 * HOUR
    rows.add_page((_row(hour, channel="aggregate"),))

    other.add_page((_row(hour, channel="aggregate"),))

    assert len(other.rows) == 1


def test_window_rows_keep_the_hours_before_the_window_and_its_last_date() -> None:
    """The hours older than the window are kept for the older window's check."""
    rows = _window_rows()
    start = rows.window.start

    rows.add_page((_row(start - HOUR, channel="a"), _row(start + 3 * HOUR)))

    assert rows.before_keys == {(start - HOUR, "a")}
    assert rows.last_day is False
    assert rows.returned({(start - HOUR, "a"), (start + 3 * HOUR, "synthetic-channel")})
    assert not rows.returned({(start - 2 * HOUR, "a")})

    rows.add_page((_row(local_midnight(rows.window.end_on)),))

    assert rows.last_day is True


def _day_rows(rows: WindowRows, day_on: date, *hours: int) -> WindowRows:
    """Return one of a window's dates fetched alone, with readings at these hours."""
    window = rows.window
    day = WindowRows(
        MeasurementWindow(day_on, day_on, window.index, window.count),
        since=window.start,
        now=NOW,
    )
    if hours:
        day.add_page(
            tuple(_row(local_midnight(day_on) + HOUR * hour) for hour in hours)
        )
    return day


def test_a_window_joins_the_rows_of_its_dates_fetched_alone() -> None:
    """
    Each date's rows join the window's, and its edges count where they are its.

    The window's first hour is 00:00 on its first date, which came back
    without it: the 00:00 of its last date is no first hour of the window.
    The dates' pages and rows received stay their own, so nothing is
    counted twice in what the walk asked for.
    """
    rows = _window_rows()
    window = rows.window
    for day in (
        _day_rows(rows, window.end_on, -1, 0, 23),
        _day_rows(rows, window.start_on + timedelta(days=1)),
        _day_rows(rows, window.start_on, -1, 23),
    ):
        rows.join(day)

    assert len(rows.rows) == 3
    assert rows.inside == 3
    assert rows.oldest == window.start + 23 * HOUR
    assert (rows.first_hour, rows.last_hour, rows.last_day) == (False, True, True)
    assert (rows.pages, rows.received, rows.before_window) == (0, 0, 0)
    assert rows.before_keys == {
        (local_midnight(window.end_on) - HOUR, "synthetic-channel"),
        (window.start - HOUR, "synthetic-channel"),
    }
    assert rows.returned({(window.start - HOUR, "synthetic-channel")})


def test_a_middle_date_fetched_alone_gives_its_window_no_edges() -> None:
    """A date between the window's first and last has edges of its own only."""
    rows = _window_rows()
    window = rows.window
    middle = _day_rows(rows, window.start_on + timedelta(days=3), 0, 23)
    assert (middle.first_hour, middle.last_hour, middle.last_day) == (True, True, True)

    rows.join(middle)
    rows.join(_day_rows(rows, window.start_on, 23))

    assert rows.inside == 3
    assert (rows.first_hour, rows.last_hour, rows.last_day) == (False, False, False)

    rows.join(_day_rows(rows, window.end_on, 0))

    assert (rows.first_hour, rows.last_hour, rows.last_day) == (False, False, True)


def _hours(*offsets: int) -> tuple[MeridianMeasurement, ...]:
    newest = NOW.replace(minute=0) - HOUR
    return tuple(
        _row(newest - HOUR * offset) for offset in sorted(offsets, reverse=True)
    )


def _stored(*offsets: int) -> frozenset[datetime]:
    newest = NOW.replace(minute=0) - HOUR
    return frozenset(newest - HOUR * offset for offset in offsets)


def test_without_stored_rows_both_sides_of_a_gap_are_imported() -> None:
    rows = _hours(0, 1, 5, 6)

    plan = importable(rows, StoredHours())

    assert plan.measurements == rows
    assert not plan.cut_at_hole
    assert not plan.tail_not_returned


def test_a_hole_with_nothing_stored_in_it_strands_nothing() -> None:
    rows = _hours(0, 1, 5, 6)

    plan = importable(rows, StoredHours(energy=_stored(0, 1, 5, 6, 7)))

    assert plan.measurements == rows
    assert not plan.cut_at_hole


def test_rows_up_to_a_stored_hour_not_returned_are_left_out() -> None:
    rows = _hours(0, 1, 2, 4, 5, 7, 8)

    plan = importable(rows, StoredHours(energy=_stored(*range(9))))

    # Hours 3 and 6 are stored but were not returned: keep what is newer
    # than the newest of them.
    assert plan.measurements == _hours(0, 1, 2)
    assert plan.cut_at_hole
    assert not plan.tail_not_returned
    # The newest stored hour left out, for the dates it leaves to a later try.
    assert plan.newest_hole == min(_stored(3))


def test_nothing_is_imported_when_the_newest_stored_hours_were_not_returned() -> None:
    rows = _hours(3, 4, 5)

    plan = importable(rows, StoredHours(energy=_stored(*range(6))))

    assert plan.measurements == ()
    assert plan.tail_not_returned
    assert not plan.cut_at_hole


def test_no_rows_import_nothing() -> None:
    assert importable((), StoredHours(energy=_stored(0))).measurements == ()


def _without_cost(*offsets: int) -> tuple[MeridianMeasurement, ...]:
    return tuple(replace(row, cost_cents=None) for row in _hours(*offsets))


def test_cost_imports_when_nothing_is_stored_or_every_hour_has_a_cost() -> None:
    rows = _hours(0, 1, 2)

    assert cost_plan(rows, frozenset()) == CostPlan()
    assert cost_plan(rows, _stored(0, 1, 2, 3)) == CostPlan()


def test_the_cost_is_cut_at_the_newest_stored_hour_without_one() -> None:
    """
    A stored hour imported without its cost keeps its old running cost.

    So the cost of every hour up to the newest such hour is left out, and
    that of the hours after it is written from its stored sum, as the usage
    is cut at a hole.
    """
    rows = tuple(
        sorted(
            (*_hours(0, 1, 3, 5), *_without_cost(2, 4)),
            key=lambda row: row.start,
        )
    )

    plan = cost_plan(rows, _stored(0, 1, 2, 3, 4, 5))

    assert plan == CostPlan(after=min(_stored(2)))
    assert not plan.skipped
    # With no cost stored at those hours, the holes strand nothing.
    assert cost_plan(rows, _stored(0, 1, 3, 5)) == CostPlan()


def test_cost_waits_when_stored_costs_are_newer_than_any_imported() -> None:
    assert cost_plan(_hours(3, 4), _stored(0, 1, 2, 3, 4)).skipped
    # After a cut, the hours with a cost after it decide.
    rows = (*_without_cost(4), *_hours(2, 3))
    assert cost_plan(rows, _stored(1, 2, 3, 4)).skipped


def test_no_cost_after_the_newest_hole_writes_none() -> None:
    """The newest stored hours came back without a cost: none is written."""
    rows = (*_hours(2, 3), *_without_cost(0, 1))

    plan = cost_plan(rows, _stored(0, 1, 2, 3))

    assert plan == CostPlan(after=min(_stored(0)))


def test_new_hours_whose_cost_is_still_to_come_import_their_usage() -> None:
    """
    Meridian may publish an hour's usage before its cost: nothing waits.

    No cost row would be written, and none is stored at those hours, so the
    cost statistic is left as it is, consistent.
    """
    assert cost_plan(_without_cost(0, 1), _stored(2, 3, 5)) == CostPlan()
