"""
Tests for the lanes' coverage rules: the target, missing days and their windows.

A lane is one direction of one property seen as a history to complete back
to its target. These rules are pure: they take the hours Home Assistant has
stored and what the lane remembers, and say which days to ask Meridian for.
Every value is synthetic.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

import pytest

from custom_components.meridian_energy import lanes as lanes_module
from custom_components.meridian_energy.lanes import (
    COVERAGE,
    COVERAGE_PAUSE_SECONDS,
    COVERAGE_WINDOWS,
    EMPTY_DAYS_AT_HISTORY_START,
    REVISION,
    LaneState,
    backfill_start,
    coverage_end,
    coverage_target,
    missing_days,
    missing_recent,
    plan_spans,
    reaches_target,
    revision_day,
    revision_days,
    revision_due,
)
from custom_components.meridian_energy.windows import NZ, local_midnight

# 16:30 on Friday 16 October 2026 in New Zealand.
NOW = datetime(2026, 10, 16, 3, 30, tzinfo=UTC)
TODAY = NOW.astimezone(NZ).date()
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
KEY = "synthetic-key:CONSUMPTION"


def _hours(start: datetime, end: datetime) -> frozenset[datetime]:
    """Return every whole hour from ``start`` until ``end``."""
    hours, hour = set(), start
    while hour < end:
        hours.add(hour)
        hour += HOUR
    return frozenset(hours)


def _days(first: date, last: date) -> frozenset[datetime]:
    """Return every hour of the New Zealand dates from ``first`` to ``last``."""
    return _hours(local_midnight(first), local_midnight(last + DAY))


# ------------------------------------------------------------- the target


def test_each_update_asks_for_6_windows_a_lane_2_to_5_s_apart() -> None:
    assert COVERAGE_WINDOWS == 6
    assert COVERAGE_PAUSE_SECONDS == (2.0, 5.0)


def test_the_target_is_midnight_90_days_back_without_a_billing_period() -> None:
    """
    New Zealand midnight at the start of the date 90 days back.

    So every date the coverage asks for is asked for whole, that one too: a
    target at the hour would let its date count as fetched with only its
    later hours, and a target moved back later never ask for the others.
    """
    ninety = (NOW - timedelta(days=90)).astimezone(NZ).date()
    assert coverage_target(NOW, None) == local_midnight(ninety)
    assert backfill_start(NOW) == local_midnight(ninety)


def test_a_billing_period_older_than_90_days_moves_the_target_to_its_midnight() -> None:
    start = TODAY - timedelta(days=92)
    assert coverage_target(NOW, start) == local_midnight(start)


def test_a_newer_billing_period_leaves_the_90_days() -> None:
    assert coverage_target(NOW, TODAY - timedelta(days=20)) == backfill_start(NOW)


def test_the_target_is_never_more_than_400_days_back() -> None:
    """At the first midnight from 400 days back on: a whole date still."""
    target = coverage_target(NOW, TODAY - timedelta(days=900))
    oldest = (NOW - timedelta(days=400)).astimezone(NZ).date()
    assert target == local_midnight(oldest + DAY)
    assert NOW - timedelta(days=400) <= target < NOW - timedelta(days=399)
    # When 400 days back is a midnight, it is that midnight.
    at = local_midnight(oldest) + timedelta(days=400)
    assert coverage_target(at, TODAY - timedelta(days=900)) == local_midnight(oldest)


def test_a_date_whose_import_was_not_confirmed_stays_within_the_target() -> None:
    """
    Its rows may be saved without the shift of the rows after them.

    So the target reaches back to it, however the billing period moves,
    until a replay writes it again; never more than 400 days back.
    """
    unconfirmed = TODAY - timedelta(days=120)
    assert coverage_target(NOW, None, unsaved=[unconfirmed]) == local_midnight(
        unconfirmed
    )
    newer = TODAY - timedelta(days=30)
    assert coverage_target(NOW, None, unsaved=[newer]) == backfill_start(NOW)
    ancient = TODAY - timedelta(days=500)
    assert coverage_target(NOW, None, unsaved=[ancient]) == coverage_target(
        NOW, TODAY - timedelta(days=900)
    )


def test_coverage_ends_after_the_date_14_days_back() -> None:
    """The reconciliations revise from 14 days back; the coverage that date's start."""
    fourteen = (NOW - timedelta(days=14)).astimezone(NZ).date()
    assert coverage_end(NOW, NOW, None) == fourteen + DAY
    # Never at or after the oldest date the update's own walk fetched.
    assert coverage_end(NOW, NOW, TODAY - 30 * DAY) == TODAY - 30 * DAY
    # Nor after the newest stored hour's date, as for a supply that ended.
    old = local_midnight(TODAY - 40 * DAY) + 5 * HOUR
    assert coverage_end(NOW, old, None) == TODAY - 39 * DAY


# ---------------------------------------------------------- missing days


def test_a_complete_history_has_no_missing_day() -> None:
    target = coverage_target(NOW, None)
    stored = _hours(target.replace(minute=0) - HOUR, NOW)
    state = LaneState()
    end = coverage_end(NOW, NOW, None)
    assert (
        missing_days(state, stored, target=target, end=end, frontier=NOW, now=NOW) == []
    )


def test_missing_days_come_newest_first_from_the_target_on() -> None:
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    stored = _days(end - 10 * DAY, TODAY)
    days = missing_days(
        LaneState(), stored, target=target, end=end, frontier=NOW, now=NOW
    )
    assert days[0] == end - 11 * DAY
    assert days[-1] == target.astimezone(NZ).date()
    assert days == sorted(days, reverse=True)
    assert len(days) == (end - 11 * DAY - target.astimezone(NZ).date()).days + 1


def test_the_target_date_is_missing_until_stored_from_its_midnight() -> None:
    """
    Stored from its midnight on, the target's date misses nothing.

    An initial import keeps its rows from 90 days back to the hour, so the
    coverage asks for the hours of that date before them, once.
    """
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    stored = _hours(target, NOW)
    assert (
        missing_days(LaneState(), stored, target=target, end=end, frontier=NOW, now=NOW)
        == []
    )
    to_the_hour = _hours((NOW - timedelta(days=90)).replace(minute=0) + HOUR, NOW)
    assert missing_days(
        LaneState(), to_the_hour, target=target, end=end, frontier=NOW, now=NOW
    ) == [target.astimezone(NZ).date()]


def test_a_billing_start_asks_for_its_whole_date() -> None:
    """From midnight on the billing period's first date, for the bill totals."""
    start = TODAY - 92 * DAY
    target = coverage_target(NOW, start)
    end = coverage_end(NOW, NOW, None)
    stored = _hours((NOW - timedelta(days=90)).replace(minute=0) + HOUR, NOW)
    days = missing_days(
        LaneState(), stored, target=target, end=end, frontier=NOW, now=NOW
    )
    assert days == [
        (NOW - timedelta(days=90)).astimezone(NZ).date() - offset * DAY
        for offset in range(len(days))
    ]
    assert days[-1] == start


def test_one_missing_hour_makes_its_date_missing() -> None:
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    gap = local_midnight(end - 20 * DAY) + 7 * HOUR
    stored = _hours(target.replace(minute=0), NOW) - {gap}
    assert missing_days(
        LaneState(), stored, target=target, end=end, frontier=NOW, now=NOW
    ) == [end - 20 * DAY]


def test_days_already_fetched_or_waiting_after_a_failure_are_not_asked_again() -> None:
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    stored = _days(end, TODAY)
    state = LaneState()
    state.took([end - DAY, end - 2 * DAY], empty=[end - 2 * DAY])
    state.fail([end - 3 * DAY], NOW)
    days = missing_days(state, stored, target=target, end=end, frontier=NOW, now=NOW)
    assert days[0] == end - 4 * DAY
    # Once its wait has passed, the failed date is asked for again.
    later = NOW + 2 * HOUR
    assert missing_days(state, stored, target=target, end=end, frontier=NOW, now=later)[
        0
    ] == (end - 3 * DAY)


def test_a_failed_date_due_at_the_next_hourly_update_may_go_a_minute_early() -> None:
    """
    Home Assistant schedules an update an hour after the last one ended.

    So the update after a failure may start a few seconds before the date's
    hour has passed: the date is asked for again then, rather than an hour
    later. Two minutes early, it still waits.
    """
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    stored = _hours(target.replace(minute=0), NOW)
    day = end - 5 * DAY
    state = LaneState()
    state.fail([day], NOW)
    retry = state.bad[day]

    def asked(at: datetime) -> list[date]:
        return missing_days(state, stored, target=target, end=end, frontier=NOW, now=at)

    assert not state.waits(day, retry - timedelta(seconds=30))
    assert asked(retry - timedelta(seconds=30)) == [day]
    assert state.waits(day, retry - timedelta(minutes=2))
    assert asked(retry - timedelta(minutes=2)) == []


def test_a_failed_date_is_asked_again_even_when_its_usage_is_stored() -> None:
    """An import Home Assistant did not confirm may have saved its usage alone."""
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    stored = _hours(target.replace(minute=0), NOW)
    state = LaneState()
    state.fail([end - 5 * DAY], NOW - 2 * HOUR)
    assert missing_days(
        state, stored, target=target, end=end, frontier=NOW, now=NOW
    ) == [end - 5 * DAY]


def test_only_dates_missing_an_hour_keep_a_lane_from_its_target() -> None:
    """
    A revision still to run, or failing for a stored date, leaves nothing missing.

    An hour Meridian does not have, on a date fetched in full, is not
    missing either; one on a date not fetched yet, or that waits after a
    failure, is, and so is any date whose import was not confirmed.
    """
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    gap = local_midnight(end - 20 * DAY) + 7 * HOUR
    complete = _hours(target.replace(minute=0), NOW)
    holed = complete - {gap}

    def reached(state: LaneState, stored: frozenset[datetime]) -> bool:
        return reaches_target(state, stored, target=target, end=end, frontier=NOW)

    assert reached(LaneState(revising=True), complete)
    failed_revision = LaneState(revising=True)
    failed_revision.fail([end - 30 * DAY], NOW)
    assert reached(failed_revision, complete)
    unconfirmed = LaneState()
    unconfirmed.fail([end - 30 * DAY], NOW, unsaved=True)
    assert not reached(unconfirmed, complete)
    assert not reached(LaneState(), holed)
    fetched = LaneState()
    fetched.took([end - 20 * DAY], empty=[])
    assert reached(fetched, holed)
    fetched.fail([end - 20 * DAY], NOW)
    assert not reached(fetched, holed)


def test_a_failure_in_a_row_waits_longer() -> None:
    state = LaneState()
    day = TODAY - 40 * DAY
    state.fail([day], NOW)
    assert state.bad[day] == NOW + HOUR
    state.fail([day], NOW)
    assert state.bad[day] == NOW + 2 * HOUR
    state.took([day], empty=[])
    assert day not in state.bad
    assert day not in state.tries


def test_a_date_that_keeps_failing_waits_longer_while_others_come_back() -> None:
    """Each date counts its own failures: another date's success resets none."""
    state = LaneState()
    failing, other = TODAY - 40 * DAY, TODAY - 50 * DAY
    for tries in range(1, 7):
        state.fail([failing], NOW)
        state.took([other], empty=[])
        assert state.tries[failing] == tries
    assert state.bad[failing] == NOW + 24 * HOUR
    # A date failing for the first time waits an hour, whatever the other's count.
    state.fail([other], NOW)
    assert state.bad[other] == NOW + HOUR


def test_an_unconfirmed_import_is_remembered_until_its_dates_come_back() -> None:
    state = LaneState()
    days = [TODAY - 40 * DAY, TODAY - 41 * DAY]
    state.fail(days, NOW, unsaved=True)
    state.fail([TODAY - 42 * DAY], NOW)
    assert state.unsaved == set(days)
    state.took(days[:1], empty=[])
    assert state.unsaved == set(days[1:])


# --------------------------------------------------------- history start


def test_two_empty_weeks_before_the_oldest_stored_day_start_the_history() -> None:
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    oldest = end - 20 * DAY
    stored = _days(oldest, TODAY)
    state = LaneState()
    empty = [oldest - offset * DAY for offset in range(1, EMPTY_DAYS_AT_HISTORY_START)]
    state.took(empty, empty=empty)
    # 13 empty days: the history may go on before them.
    assert state.history_start(oldest) is None
    assert missing_days(state, stored, target=target, end=end, frontier=NOW, now=NOW)
    last = oldest - EMPTY_DAYS_AT_HISTORY_START * DAY
    state.took([last], empty=[last])
    assert state.history_start(oldest) == oldest
    assert (
        missing_days(state, stored, target=target, end=end, frontier=NOW, now=NOW) == []
    )


def test_a_week_with_nothing_is_asked_across() -> None:
    """A meter that sent nothing for a week: older readings are still asked for."""
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    oldest = end - 20 * DAY
    stored = _days(oldest, TODAY)
    state = LaneState()
    week = [oldest - offset * DAY for offset in range(1, 8)]
    state.took(week, empty=week)
    days = missing_days(state, stored, target=target, end=end, frontier=NOW, now=NOW)
    assert state.history_start(oldest) is None
    assert days[0] == oldest - 8 * DAY


def test_empty_days_between_stored_days_start_no_history() -> None:
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    gap = [end - offset * DAY for offset in range(10, 40)]
    stored = _hours(target.replace(minute=0), NOW) - _days(gap[-1], gap[0])
    state = LaneState()
    state.took(gap, empty=gap)
    assert state.history_start(target.astimezone(NZ).date()) is None
    assert (
        missing_days(state, stored, target=target, end=end, frontier=NOW, now=NOW) == []
    )


def test_a_refusal_before_the_oldest_stored_day_starts_the_history() -> None:
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    oldest = end - 20 * DAY
    stored = _days(oldest, TODAY)
    state = LaneState(refused_through=oldest - 5 * DAY)
    assert state.history_start(oldest) == oldest - 4 * DAY
    days = missing_days(state, stored, target=target, end=end, frontier=NOW, now=NOW)
    assert days == [oldest - offset * DAY for offset in range(1, 5)]
    # Stored days older than a refusal show the history goes on.
    assert LaneState(refused_through=oldest).history_start(oldest) is None


# --------------------------------------------------------------- windows


def test_missing_days_are_asked_for_a_week_at_a_time_and_failed_ones_alone() -> None:
    start = TODAY - 60 * DAY
    days = [start - offset * DAY for offset in range(16)]
    state = LaneState()
    state.fail([start - 9 * DAY], NOW - 2 * HOUR)
    spans = plan_spans(state, days, [], now=NOW)
    assert spans == [
        (start - 6 * DAY, start, COVERAGE),
        (start - 8 * DAY, start - 7 * DAY, COVERAGE),
        (start - 9 * DAY, start - 9 * DAY, COVERAGE),
        (start - 15 * DAY, start - 10 * DAY, COVERAGE),
    ]


def test_days_apart_are_asked_for_in_windows_of_their_own() -> None:
    start = TODAY - 60 * DAY
    days = [start, start - 2 * DAY, start - 3 * DAY]
    spans = plan_spans(LaneState(), days, [start - 30 * DAY], now=NOW)
    assert spans == [
        (start, start, COVERAGE),
        (start - 3 * DAY, start - 2 * DAY, COVERAGE),
        (start - 30 * DAY, start - 30 * DAY, REVISION),
    ]


# -------------------------------------------------------------- revision


def test_the_revision_pass_starts_once_on_its_day_of_the_month(
    revision_pass: None,
) -> None:
    del revision_pass
    day = revision_day(KEY)
    assert 1 <= day <= 28
    assert revision_day(KEY) == day
    slot = date(2026, 11, day)
    state = LaneState(refused_through=slot - 60 * DAY)
    state.took([slot - 40 * DAY], empty=[slot - 40 * DAY])
    assert not state.start_revision(KEY, slot - DAY)
    assert state.start_revision(KEY, slot)
    assert state.revising
    # It asks again for days fetched before, whose readings may have come since,
    # and for where the history starts, found by empty days or by a refusal.
    assert not state.fetched
    assert not state.empty
    assert state.refused_through is None
    assert not state.start_revision(KEY, slot)
    # It runs once for its day: the days after it start no other.
    assert not state.start_revision(KEY, slot + DAY)


def test_a_revision_pass_leaves_what_the_lane_reports_as_it_was(
    revision_pass: None,
) -> None:
    """
    The pass forgets what was fetched, and where the history starts, to ask again.

    What the lane reports does not: a date whose missing hour Meridian does
    not have, and the empty days before the oldest stored day, count as
    before until the pass has asked for them again, whether that request is
    still to come or keeps failing. What the pass then finds counts instead,
    as older readings Meridian has published since.
    """
    del revision_pass
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    oldest = end - 40 * DAY
    holed = end - 20 * DAY
    gap = local_midnight(holed) + 7 * HOUR
    stored = _days(oldest, TODAY) - {gap}
    empty = [oldest - offset * DAY for offset in range(1, 15)]
    state = LaneState()
    state.took([holed, *empty], empty=empty)

    def reached(hours: frozenset[datetime]) -> bool:
        return reaches_target(state, hours, target=target, end=end, frontier=NOW)

    assert reached(stored)
    assert state.history_start(oldest) == oldest
    assert state.start_revision(KEY, date(2026, 11, revision_day(KEY)))

    # Asked for again: the holed date, and the days before the oldest one.
    days = missing_days(state, stored, target=target, end=end, frontier=NOW, now=NOW)
    assert days[0] == holed
    assert days[1:3] == empty[:2]
    assert state.history_start(oldest) is None
    assert state.history_start(oldest, known=True) == oldest
    assert reached(stored)
    state.fail([holed], NOW)
    assert reached(stored)
    state.took([holed], empty=[])
    assert holed not in state.known_fetched
    assert reached(stored)
    # Meridian now has readings for the day before the oldest one.
    state.took(empty[:1], empty=[])
    assert not reached(stored | _days(empty[0], empty[0]))
    assert state.copy().known_empty == set(empty[1:])
    state.forget_before(TODAY)
    assert not state.known_fetched
    assert not state.known_empty


@pytest.mark.parametrize("late", [1, 3])
def test_a_revision_pass_missed_on_its_day_starts_in_the_3_days_after(
    revision_pass: None, late: int
) -> None:
    """As when Home Assistant was off that day, or every update failed."""
    del revision_pass
    slot = date(2026, 11, revision_day(KEY))
    state = LaneState()
    assert state.start_revision(KEY, slot + late * DAY)
    assert state.revision_on == slot
    assert not state.start_revision(KEY, slot + 3 * DAY)


def test_a_revision_pass_missed_for_longer_waits_for_next_month(
    revision_pass: None,
) -> None:
    del revision_pass
    slot = date(2026, 11, revision_day(KEY))
    assert revision_due(KEY, slot + 4 * DAY) is None
    assert not LaneState().start_revision(KEY, slot + 4 * DAY)
    # From the month before: its day, then the 3 days after it, in December.
    december = date(2026, 12, revision_day(KEY))
    assert revision_due(KEY, december + 2 * DAY) == december
    assert revision_due(KEY, december - DAY) is None


def test_the_grace_after_a_revision_day_reaches_into_the_next_month() -> None:
    """A lane whose day is the 28th may start its pass early the next month."""
    with patch.object(lanes_module, "revision_day", return_value=28):
        assert revision_due(KEY, date(2027, 3, 2)) == date(2027, 2, 28)
        assert revision_due(KEY, date(2027, 3, 4)) is None


def test_the_revision_pass_asks_for_stored_days_newest_first_once() -> None:
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    stored = _hours(target.replace(minute=0), NOW)
    state = LaneState()
    assert revision_days(state, stored, target=target, end=end) == []
    state.revising = True
    days = revision_days(state, stored, target=target, end=end)
    assert days[0] == end - DAY
    assert days[-1] == target.astimezone(NZ).date()
    state.took(days[:7], empty=[], revised=True)
    assert revision_days(state, stored, target=target, end=end)[0] == end - 8 * DAY


def test_a_failed_revision_date_waits_and_the_pass_goes_on_past_it() -> None:
    """
    A stored date whose revision failed is its coverage's to ask for, alone.

    The pass leaves it out, waiting or not, and asks for the older dates;
    once its wait has passed, the missing dates hold it, and it goes alone.
    """
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, NOW, None)
    stored = _hours(target.replace(minute=0), NOW)
    state = LaneState(revising=True)
    failing = end - 3 * DAY
    state.fail([failing], NOW)
    days = revision_days(state, stored, target=target, end=end)
    assert failing not in days
    assert days[:3] == [end - DAY, end - 2 * DAY, end - 4 * DAY]
    assert (
        missing_days(state, stored, target=target, end=end, frontier=NOW, now=NOW) == []
    )
    later = NOW + 2 * HOUR
    missing = missing_days(
        state, stored, target=target, end=end, frontier=NOW, now=later
    )
    assert missing == [failing]
    spans = plan_spans(state, missing, days, now=later)
    assert spans[0] == (failing, failing, COVERAGE)
    assert all(not first <= failing <= last for first, last, _kind in spans[1:])


def test_missing_hours_between_stored_hours_of_the_last_14_days() -> None:
    hour = NOW.replace(minute=0)
    stored = _hours(hour - timedelta(days=20), hour)
    assert missing_recent(stored, NOW) is None
    gap = hour - timedelta(days=5)
    assert missing_recent(stored - {gap, gap + HOUR}, NOW) == gap
    # Hours missing before the oldest stored hour are not between stored hours.
    assert missing_recent(_hours(hour - timedelta(days=3), hour), NOW) is None
    assert missing_recent(frozenset(), NOW) is None


def test_a_gap_that_started_more_than_14_days_back_is_missing_from_then() -> None:
    """
    Stored hours from 30 to 20 days back, then from 10 days back on.

    The gap started before the 14 days the reconciliations revise, and the
    daily one reaches back to their start, the first whole hour 14 days
    back. A history that starts inside the 14 days has nothing before it,
    nor does one that ended before them.
    """
    hour = NOW.replace(minute=0)
    stored = _hours(hour - timedelta(days=30), hour - timedelta(days=20)) | _hours(
        hour - timedelta(days=10), hour
    )
    assert missing_recent(stored, NOW) == hour - timedelta(days=14) + HOUR
    # A gap inside the 14 days still counts from its own first hour.
    inner = hour - timedelta(days=5)
    assert missing_recent(stored - {inner}, NOW) == hour - timedelta(days=14) + HOUR
    late = _hours(hour - timedelta(days=30), hour - timedelta(days=14) + 3 * HOUR)
    assert missing_recent(late | _hours(hour - timedelta(days=9), hour), NOW) == (
        hour - timedelta(days=14) + 3 * HOUR
    )
    assert missing_recent(_hours(hour - timedelta(days=30), hour - 20 * DAY), NOW) is (
        None
    )


@pytest.mark.parametrize("days", [1, 50])
def test_a_copy_shares_nothing(days: int) -> None:
    state = LaneState()
    state.took([TODAY - days * DAY], empty=[TODAY - days * DAY])
    state.fail([TODAY - 2 * days * DAY], NOW)
    copy = state.copy()
    copy.took([TODAY - 3 * days * DAY], empty=[], revised=True)
    copy.bad.clear()
    assert TODAY - 3 * days * DAY not in state.fetched
    assert state.bad
    assert not state.revised


def test_hours_after_the_frontier_are_not_missing() -> None:
    """A supply that ended: the hours after its newest reading are the walks'."""
    target = coverage_target(NOW, None)
    frontier = local_midnight(TODAY - 40 * DAY) + 5 * HOUR
    stored = _hours(target.replace(minute=0), frontier + HOUR)
    end = coverage_end(NOW, frontier, None)
    assert end == TODAY - 39 * DAY
    days = missing_days(
        LaneState(), stored, target=target, end=end, frontier=frontier, now=NOW
    )
    assert days == []


def test_without_a_frontier_every_hour_counts_and_no_history_starts() -> None:
    """A lane with nothing stored yet: no frontier bounds it, nothing starts it."""
    target = coverage_target(NOW, None)
    end = coverage_end(NOW, None, None)
    assert end == (NOW - timedelta(days=14)).astimezone(NZ).date() + DAY
    days = missing_days(
        LaneState(), frozenset(), target=target, end=end, frontier=None, now=NOW
    )
    assert days[0] == end - DAY
    assert LaneState().history_start(None) is None
    assert LaneState(refused_through=TODAY - 50 * DAY).history_start(None) == (
        TODAY - 49 * DAY
    )


def test_dates_before_the_target_are_forgotten() -> None:
    old, kept = TODAY - 100 * DAY, TODAY - 50 * DAY
    state = LaneState()
    state.took([old, kept], empty=[old, kept], revised=True)
    state.fail([old, kept], NOW, unsaved=True)
    state.forget_before(TODAY - 90 * DAY)
    assert state.fetched == state.empty == state.revised == {kept}
    assert set(state.bad) == set(state.tries) == state.unsaved == {kept}
    # Before the history's start, only the waits are forgotten.
    state.took([old], empty=[old])
    state.forget_bad_before(TODAY - 40 * DAY)
    assert not state.bad
    assert old in state.empty
