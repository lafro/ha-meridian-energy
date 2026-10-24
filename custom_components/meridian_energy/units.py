"""
Units of a sync, and what the coordinator remembers about each between syncs.

A unit is one direction (grid import or export) of one property of one
account. A failure in one unit is recorded as that unit's status, and the
sync goes on with the others; only a rejected session, a rate limit and the
end of the sync's time budget stop the whole sync. A unit that failed waits
before it is tried again, 1 h, then 2 h, doubling up to 24 h; one that
imported only its newer windows (degraded) keeps its hourly update and waits
the same way for the reconciliation it still owes, and so does one whose
reconciliation then fails before it fetched a window. When the hourly
update of such a unit fails, the unit waits 1 h, then 2 h, counting those
failures alone, while the reconciliation keeps its own wait. A unit that
skips a reconciliation, while it waits or because the time budget did not
reach it, owes it too. Until a first sync has succeeded, its own retries
are spaced out instead, and every attempt tries each unit. All of it is
kept in memory only: a restart starts every unit afresh, as before, except
that the setup import hands what it remembers to the new entry
(``SyncMemory``).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from .models import MeridianMeasurement, SyncMode

if TYPE_CHECKING:
    from .lanes import LaneState

STATUS_PENDING = "pending"
STATUS_OK = "ok"
STATUS_FAILED = "failed"
STATUS_DEGRADED = "degraded"
STATUS_DEFERRED = "deferred"

_FIRST_BACKOFF = timedelta(hours=1)
_LONGEST_BACKOFF = timedelta(hours=24)
# How long the size ladder of hourly updates remembers that a window needed
# requests by day, counted from the update that learned it: a week, the size
# of a window.
_LADDER_MEMORY = timedelta(days=7)
# A unit due again at the next hourly update may start a few seconds early:
# Home Assistant schedules the update an hour after the last one ended.
_EARLY_START = timedelta(minutes=1)

# How far back each mode reaches, for the larger of two modes: a restart and
# the weekly reconciliation both reach back 14 days.
_SPAN = {
    SyncMode.TIP: 0,
    SyncMode.TARGETED_RECONCILIATION: 1,
    SyncMode.RESTART: 2,
    SyncMode.FULL_RECONCILIATION: 2,
    SyncMode.INITIAL: 3,
}


def _recent(day: date | None, learned: date | None, today: date) -> date | None:
    """
    Return a date the size ladder remembers, unless it learned it a week ago.

    The week counts from the update that learned the date, not from the
    date: every window but the newest ends a week or more before today, so
    a date of an older window would be forgotten in the update that met it.
    """
    if day is None or learned is None or today - learned >= _LADDER_MEMORY:
        return None
    return day


def larger_mode(first: SyncMode, second: SyncMode | None) -> SyncMode:
    """Return whichever of two modes reaches further back; the first on a tie."""
    if second is None or _SPAN[first] >= _SPAN[second]:
        return first
    return second


def owed_reconciliation(mode: SyncMode) -> SyncMode | None:
    """
    Return the reconciliation a unit still owes after a sync in ``mode`` failed.

    The hourly update owes none: the next one repeats it. A restart, the
    weekly reconciliation and an initial import with statistics owe the
    weekly one, which revises the last 14 days; the daily one owes itself.
    """
    if mode is SyncMode.TIP:
        return None
    if mode is SyncMode.TARGETED_RECONCILIATION:
        return mode
    return SyncMode.FULL_RECONCILIATION


def backoff(failures: int) -> timedelta:
    """Return the wait after this many failed or degraded syncs in a row."""
    # 1 h doubled 5 times is past the longest wait, which caps it; a larger
    # power would only overflow.
    doubled: timedelta = _FIRST_BACKOFF * 2 ** min(5, max(0, failures - 1))
    return min(_LONGEST_BACKOFF, doubled)


@dataclass(frozen=True, slots=True)
class UnitFailure:
    """
    Why a unit last failed, without identifiers.

    ``reason`` is the failure's translation key (or ``time_budget``), and
    ``detail`` the placeholder its message takes, if any; ``cause`` names
    the error's class and ``http_status`` its status, when it had one.
    """

    reason: str
    cause: str
    http_status: int | None
    at: datetime
    detail: str | None = None


@dataclass(slots=True)
class UnitState:
    """
    What the coordinator remembers about one unit between syncs.

    ``retry_at`` is when a failed unit is tried again, or, for a degraded
    one, when the reconciliation it owes (``owed``) runs again; until then
    a degraded unit runs its hourly update. When that hourly update fails,
    the unit fails, and waits until ``hourly_retry_at``, counting its
    failed hourly updates in a row (``hourly_failures``) apart from the
    reconciliation's (``failures``), whose wait goes on unchanged: one
    timeout of an hourly update waits an hour, not as long as the
    reconciliation that kept failing. ``probe_on`` is the newest date
    of a window an hourly update could not fetch for a gateway timeout, and
    ``probe_days`` that window's length: the next update asks for the first
    window holding that date that is as long by day, starting with that
    probe, and forgets it once the probe has gone out, so one the time
    budget stopped before it went out is asked for at the update after; a
    shorter one, as the hourly update's two days, is asked for whole and
    leaves the probe to it. ``laddered_on`` is the date of the
    last such probe that came back: size was the lever there. A window that
    times out after it, older in the same walk or the same window asked for
    whole again at a later update, is the second that needs requests by day,
    and ``days_on`` becomes the newest date of the two: later updates ask
    for every window starting on or before it by day (``days_kept``). Each
    is forgotten a week after the update that learned it (``laddered_at``,
    ``days_at``), so a Meridian that has recovered is asked for weeks again;
    a window newer than ``days_on`` is asked for whole before that.
    ``not_reached`` counts the syncs in a row whose time budget ran out
    before the unit's first request: the next one starts with it.
    ``quiet_frontier`` is the newest stored hour when the last walk that
    ended without a failure found no reading newer than the newest stored
    hour it started from, as for a supply that ended: while it is still the
    newest, the hourly and daily updates stop at an empty newest window
    instead of walking back to it again.
    """

    status: str = STATUS_PENDING
    failures: int = 0
    retry_at: datetime | None = None
    waits_whole: bool = False
    owed: SyncMode | None = None
    last_failure: UnitFailure | None = None
    probe_on: date | None = None
    probe_days: int = 0
    laddered_on: date | None = None
    laddered_at: date | None = None
    days_on: date | None = None
    days_at: date | None = None
    not_reached: int = 0
    quiet_frontier: datetime | None = None
    hourly_failures: int = 0
    hourly_retry_at: datetime | None = None

    @property
    def hourly_goes_on(self) -> bool:
        """
        Return whether the unit's hourly updates go on while a reconciliation waits.

        So they do for a degraded unit, and for one whose hourly update
        failed since (``fail_hourly``): neither waits whole.
        """
        return self.status == STATUS_DEGRADED or (
            self.status == STATUS_FAILED and not self.waits_whole
        )

    @property
    def next_try(self) -> datetime | None:
        """
        Return when the unit is tried again, or its owed reconciliation runs.

        For a unit whose hourly update failed while its reconciliation
        waits, that is when its next hourly update may run.
        """
        if self.status == STATUS_FAILED and self.hourly_retry_at is not None:
            return self.hourly_retry_at
        return self.retry_at

    def deferred(self, now: datetime) -> bool:
        """Return whether a failed unit is still waiting to be tried again."""
        return (self.waits_whole and _before(self.retry_at, now)) or _before(
            self.hourly_retry_at, now
        )

    def reconciliation_waits(self, now: datetime) -> bool:
        """Return whether the reconciliation a degraded unit owes still waits."""
        return self.owed is not None and _before(self.retry_at, now)

    def days_kept(self, today: date) -> date | None:
        """Return the date on or before which windows are asked for by day, if any."""
        return _recent(self.days_on, self.days_at, today)

    def recent_ladder(self, today: date) -> date | None:
        """Return the date of the last probe that came back, within a week."""
        return _recent(self.laddered_on, self.laddered_at, today)

    def ladder(self, day: date, today: date) -> None:
        """Remember the date of a probe that came back, for a week from today."""
        self.laddered_on = day
        self.laddered_at = today

    def keep_days(self, day: date, today: date) -> None:
        """Ask for every window starting on or before ``day`` by day, for a week."""
        kept = self.days_kept(today)
        self.days_on = day if kept is None else max(kept, day)
        self.days_at = today

    def covers(self, mode: SyncMode) -> bool:
        """Return whether a sync in ``mode`` reaches as far back as what is owed."""
        return self.owed is None or larger_mode(mode, self.owed) is mode

    def owe(self, mode: SyncMode) -> None:
        """Owe the reconciliation a sync in ``mode`` would have run, but did not."""
        owed = owed_reconciliation(mode)
        self.owed = owed if self.owed is None else larger_mode(self.owed, owed)

    def succeed(self) -> bool:
        """
        Record a unit that fetched and imported in full; return if it recovered.

        Only for a sync that covers what the unit owes (``covers``): an
        hourly update while a degraded unit's reconciliation waits leaves it
        degraded, owing that reconciliation, with its wait unchanged.
        """
        recovered = self.status in {STATUS_FAILED, STATUS_DEGRADED}
        self.status = STATUS_OK
        self.failures = 0
        self.retry_at = None
        self.waits_whole = False
        self.owed = None
        self._hourly_ok()
        return recovered

    def recover_hourly(self) -> bool:
        """
        Record an hourly update that imported while the owed reconciliation waits.

        The unit stays degraded, owing that reconciliation, with its wait
        unchanged; one whose hourly update had failed since is degraded
        again. Return whether it was failing.
        """
        recovered = self.status == STATUS_FAILED
        if recovered:
            self.status = STATUS_DEGRADED
        self._hourly_ok()
        return recovered

    def fail(self, now: datetime, failure: UnitFailure, mode: SyncMode) -> bool:
        """Record a unit that imported nothing; return whether it started failing."""
        started = self.status != STATUS_FAILED
        self._wait(now, failure, mode, whole=True)
        self.status = STATUS_FAILED
        self._hourly_ok()
        return started

    def fail_hourly(self, now: datetime, failure: UnitFailure) -> bool:
        """
        Record a failed hourly update of a unit whose hourly updates go on.

        The unit fails, and its next hourly update waits 1 h, then 2 h,
        doubling up to 24 h, for its failed hourly updates in a row; the
        reconciliation it owes keeps its own count and wait. Return whether
        the unit started failing.
        """
        started = self.status != STATUS_FAILED
        self.hourly_failures += 1
        self.hourly_retry_at = now + backoff(self.hourly_failures)
        self.status = STATUS_FAILED
        self.last_failure = failure
        return started

    def degrade(self, now: datetime, failure: UnitFailure, mode: SyncMode) -> None:
        """Record a unit that imported its newer windows only."""
        self._wait(now, failure, mode, whole=False)
        self.status = STATUS_DEGRADED
        self._hourly_ok()

    def defer_reconciliation(
        self, now: datetime, failure: UnitFailure, mode: SyncMode
    ) -> None:
        """
        Record an owed reconciliation that failed before it fetched a window.

        The unit's hourly updates go on (``hourly_goes_on``), and the
        reconciliation waits longer; the unit stays degraded, or failed when
        its hourly update failed since.
        """
        self._wait(now, failure, mode, whole=False)

    def _hourly_ok(self) -> None:
        self.hourly_failures = 0
        self.hourly_retry_at = None

    def _wait(
        self, now: datetime, failure: UnitFailure, mode: SyncMode, *, whole: bool
    ) -> None:
        self.failures += 1
        self.retry_at = now + backoff(self.failures)
        self.waits_whole = whole
        self.owe(mode)
        self.last_failure = failure


def _before(at: datetime | None, now: datetime) -> bool:
    """Return whether a wait that ends at ``at`` has not passed by ``now``."""
    return at is not None and now < at - _EARLY_START


@dataclass(frozen=True, slots=True)
class SyncMemory:
    """
    What one coordinator remembers of its units, for another to go on from.

    The setup import hands it to the new entry's coordinator: each unit's
    state, so a unit that failed in setup is not reported as failing again,
    and one setup found quiet is not searched again at once; the rows it
    imported in the last 14 days, so the provisional ones are counted and
    revised as they would be after a restart; the rows an hour holds; and
    the units whose initial import searched every window it planned
    (``searched``), so one that found nothing, as for a supply that ended
    or an export register that never exported, is not searched again at
    once either; and, for a unit whose replay reached back more than 14
    days and was not confirmed, the oldest hour it wrote (``unconfirmed``),
    so the entry's walks reach back to it; and what each unit's walk showed
    of its history (``lanes``), as where it starts, so the entry's coverage
    does not ask for it again (``lanes.LaneState``).
    """

    unit_states: Mapping[tuple[str, str], UnitState]
    measurements: Mapping[
        tuple[str, str], Mapping[tuple[datetime, str], MeridianMeasurement]
    ]
    row_density: Mapping[tuple[str, str], float]
    searched: frozenset[tuple[str, str]] = frozenset()
    unconfirmed: Mapping[tuple[str, str], datetime] = field(default_factory=dict)
    lanes: Mapping[tuple[str, str], LaneState] = field(default_factory=dict)

    def copy(self) -> SyncMemory:
        """Return a copy that shares no mutable state with this one."""
        return SyncMemory(
            unit_states={
                key: replace(state) for key, state in self.unit_states.items()
            },
            measurements={key: dict(rows) for key, rows in self.measurements.items()},
            row_density=dict(self.row_density),
            searched=frozenset(self.searched),
            unconfirmed=dict(self.unconfirmed),
            lanes={key: lane.copy() for key, lane in self.lanes.items()},
        )
