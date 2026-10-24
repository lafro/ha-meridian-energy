"""Tests for what the coordinator remembers about each unit between syncs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from custom_components.meridian_energy.lanes import LaneState
from custom_components.meridian_energy.models import SyncMode
from custom_components.meridian_energy.units import (
    STATUS_DEGRADED,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_PENDING,
    SyncMemory,
    UnitFailure,
    UnitState,
    backoff,
    larger_mode,
    owed_reconciliation,
)

NOW = datetime(2026, 10, 16, 3, 30, tzinfo=UTC)
HOUR = timedelta(hours=1)


def _failure(at: datetime = NOW) -> UnitFailure:
    return UnitFailure(
        reason="upstream_timeout",
        cause="MeridianUpstreamError",
        http_status=504,
        at=at,
        detail="HTTP 504",
    )


@pytest.mark.parametrize(
    ("failures", "hours"),
    [(0, 1), (1, 1), (2, 2), (3, 4), (4, 8), (5, 16), (6, 24), (7, 24), (500, 24)],
)
def test_the_backoff_doubles_from_an_hour_up_to_a_day(
    failures: int, hours: int
) -> None:
    assert backoff(failures) == timedelta(hours=hours)


def test_the_larger_mode_reaches_further_back() -> None:
    assert larger_mode(SyncMode.TIP, None) is SyncMode.TIP
    assert larger_mode(SyncMode.TIP, SyncMode.FULL_RECONCILIATION) is (
        SyncMode.FULL_RECONCILIATION
    )
    assert larger_mode(SyncMode.INITIAL, SyncMode.FULL_RECONCILIATION) is (
        SyncMode.INITIAL
    )
    # A restart and the weekly reconciliation reach back as far: a tie keeps
    # the first.
    assert larger_mode(SyncMode.RESTART, SyncMode.FULL_RECONCILIATION) is (
        SyncMode.RESTART
    )
    assert larger_mode(
        SyncMode.TARGETED_RECONCILIATION, SyncMode.FULL_RECONCILIATION
    ) is (SyncMode.FULL_RECONCILIATION)


@pytest.mark.parametrize(
    ("mode", "owed"),
    [
        (SyncMode.TIP, None),
        (SyncMode.TARGETED_RECONCILIATION, SyncMode.TARGETED_RECONCILIATION),
        (SyncMode.FULL_RECONCILIATION, SyncMode.FULL_RECONCILIATION),
        (SyncMode.RESTART, SyncMode.FULL_RECONCILIATION),
        (SyncMode.INITIAL, SyncMode.FULL_RECONCILIATION),
    ],
)
def test_a_failed_sync_owes_the_reconciliation_of_its_span(
    mode: SyncMode, owed: SyncMode | None
) -> None:
    assert owed_reconciliation(mode) is owed


def test_a_failed_unit_waits_whole_and_logs_once_when_it_starts_failing() -> None:
    state = UnitState()
    assert state.status == STATUS_PENDING
    assert not state.deferred(NOW)

    assert state.fail(NOW, _failure(), SyncMode.TIP) is True
    assert state.status == STATUS_FAILED
    assert state.retry_at == NOW + HOUR
    assert state.owed is None
    assert state.deferred(NOW + timedelta(minutes=30))
    # Due a minute early: the hourly update runs an hour after the last ended.
    assert not state.deferred(NOW + timedelta(minutes=59))
    assert not state.reconciliation_waits(NOW)

    later = NOW + HOUR
    assert state.fail(later, _failure(later), SyncMode.FULL_RECONCILIATION) is False
    assert state.retry_at == later + 2 * HOUR
    assert state.owed is SyncMode.FULL_RECONCILIATION
    assert state.last_failure == _failure(later)

    assert state.succeed() is True
    assert state.status == STATUS_OK
    assert (state.failures, state.retry_at, state.owed) == (0, None, None)
    assert not state.waits_whole
    assert state.succeed() is False


def test_a_degraded_unit_waits_only_for_the_reconciliation_it_owes() -> None:
    state = UnitState()
    state.degrade(NOW, _failure(), SyncMode.RESTART)

    assert state.status == STATUS_DEGRADED
    assert not state.deferred(NOW + timedelta(minutes=10))
    assert state.reconciliation_waits(NOW + timedelta(minutes=10))
    assert state.owed is SyncMode.FULL_RECONCILIATION
    assert not state.reconciliation_waits(NOW + HOUR)

    state.degrade(NOW + HOUR, _failure(), SyncMode.TARGETED_RECONCILIATION)
    # The larger reconciliation is still owed, and the wait doubles.
    assert state.owed is SyncMode.FULL_RECONCILIATION
    assert state.retry_at == NOW + HOUR + 2 * HOUR
    assert state.succeed() is True


def test_failed_hourly_updates_wait_apart_from_the_reconciliation() -> None:
    """
    An hourly update that fails waits an hour, whatever the reconciliation's count.

    Four failed reconciliations make the next wait 8 h. A failed hourly
    update then waits 1 h, then 2 h, counting its own failures, while the
    reconciliation keeps its count and its wait; an hourly update that
    imports again leaves the unit degraded, as before it failed.
    """
    state = UnitState()
    for hours in range(4):
        state.degrade(NOW + hours * HOUR, _failure(), SyncMode.FULL_RECONCILIATION)
    reconciliation = NOW + 3 * HOUR + 8 * HOUR
    assert (state.failures, state.retry_at) == (4, reconciliation)
    assert state.hourly_goes_on

    later = NOW + 4 * HOUR
    assert state.fail_hourly(later, _failure(later)) is True
    assert state.status == STATUS_FAILED
    assert state.hourly_goes_on
    assert state.deferred(later + 30 * timedelta(minutes=1))
    assert not state.deferred(later + HOUR)
    assert state.next_try == later + HOUR
    assert (state.failures, state.retry_at) == (4, reconciliation)
    assert state.owed is SyncMode.FULL_RECONCILIATION
    assert state.fail_hourly(later + HOUR, _failure()) is False
    assert state.next_try == later + 3 * HOUR

    # The reconciliation then fails too: it waits longer, and the unit,
    # whose hourly updates go on, stays failed.
    state.defer_reconciliation(
        later + 3 * HOUR, _failure(), SyncMode.FULL_RECONCILIATION
    )
    assert state.status == STATUS_FAILED
    assert (state.failures, state.retry_at) == (5, later + 3 * HOUR + 16 * HOUR)

    assert state.recover_hourly() is True
    assert state.status == STATUS_DEGRADED
    assert (state.hourly_failures, state.hourly_retry_at) == (0, None)
    assert state.next_try == state.retry_at
    assert not state.deferred(later + 4 * HOUR)
    assert state.recover_hourly() is False

    # Waiting whole after a failed reconciliation ends the hourly updates' wait.
    state.fail_hourly(later + 4 * HOUR, _failure())
    state.fail(later + 5 * HOUR, _failure(), SyncMode.FULL_RECONCILIATION)
    assert not state.hourly_goes_on
    assert (state.hourly_failures, state.hourly_retry_at) == (0, None)
    state.degrade(later + 6 * HOUR, _failure(), SyncMode.FULL_RECONCILIATION)
    assert state.hourly_goes_on


@pytest.mark.parametrize("weeks_back", [0, 1, 4])
def test_the_size_ladder_of_hourly_updates_forgets_a_week_after_learning(
    weeks_back: int,
) -> None:
    """
    The week counts from the update that learned a date, not from the date.

    So a probe of the second week, or an older one, is remembered as long
    as one of the newest: counted from its own date, it was forgotten in
    the update that met it, and a walk whose older weeks time out never
    got past them.
    """
    today = NOW.date()
    day = today - timedelta(days=7 * weeks_back)
    state = UnitState()
    state.ladder(day, today)
    state.keep_days(day, today)
    assert state.recent_ladder(today) == day
    assert state.days_kept(today + timedelta(days=6)) == day
    # A sync that covers what is owed does not end it: the next walk, an
    # hourly update say, would ask for the same window whole again.
    state.succeed()
    assert state.days_kept(today) == day

    assert state.recent_ladder(today + timedelta(days=7)) is None
    assert state.days_kept(today + timedelta(days=7)) is None
    assert UnitState().days_kept(today) is None
    # Days already kept stay kept for a newer date, which a week renews.
    state.keep_days(day - timedelta(days=7), today + timedelta(days=3))
    assert state.days_kept(today + timedelta(days=9)) == day


def test_a_unit_owes_what_it_skipped_and_only_a_covering_sync_clears_it() -> None:
    state = UnitState()
    assert state.covers(SyncMode.TIP)
    state.owe(SyncMode.TIP)
    assert state.owed is None

    state.owe(SyncMode.TARGETED_RECONCILIATION)
    assert state.owed is SyncMode.TARGETED_RECONCILIATION
    assert not state.covers(SyncMode.TIP)
    state.owe(SyncMode.RESTART)
    assert state.owed is SyncMode.FULL_RECONCILIATION
    state.owe(SyncMode.TARGETED_RECONCILIATION)
    # The larger reconciliation stays owed.
    assert state.owed is SyncMode.FULL_RECONCILIATION
    assert not state.covers(SyncMode.TARGETED_RECONCILIATION)
    # A restart reaches back as far as the weekly reconciliation.
    assert state.covers(SyncMode.RESTART)
    assert state.covers(SyncMode.INITIAL)


def test_a_memory_copy_shares_no_state() -> None:
    state = UnitState()
    rows: dict[tuple[datetime, str], Any] = {}
    unconfirmed = {("synthetic-key", "CONSUMPTION"): NOW - timedelta(days=90)}
    lane = LaneState()
    memory = SyncMemory(
        unit_states={("synthetic-key", "CONSUMPTION"): state},
        measurements={("synthetic-key", "CONSUMPTION"): rows},
        row_density={("synthetic-key", "CONSUMPTION"): 1.0},
        unconfirmed=unconfirmed,
        lanes={("synthetic-key", "CONSUMPTION"): lane},
    )

    copy = memory.copy()
    state.fail(NOW, _failure(), SyncMode.TIP)
    rows[(NOW, "synthetic-channel")] = None
    unconfirmed.clear()
    lane.took([NOW.date()], empty=[NOW.date()])

    copied = copy.unit_states[("synthetic-key", "CONSUMPTION")]
    assert copied.status == STATUS_PENDING
    assert copy.measurements[("synthetic-key", "CONSUMPTION")] == {}
    assert copy.row_density == {("synthetic-key", "CONSUMPTION"): 1.0}
    assert copy.unconfirmed == {
        ("synthetic-key", "CONSUMPTION"): NOW - timedelta(days=90)
    }
    assert copy.lanes[("synthetic-key", "CONSUMPTION")].fetched == set()
