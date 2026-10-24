"""
Lanes: how far back each unit's stored history reaches, and what completes it.

A lane is a unit (one direction, grid import or export, of one property of
one account; see ``units``) seen as a history to complete back to its
target (``coverage_target``): New Zealand midnight at the start of the
older of the date 90 days back and the first date of the account's current
billing period, never more than 400 days back, since the bill totals need
every hour from the period's start; and never after a date whose import
Home Assistant did not confirm, which is replayed in full.

What a lane holds is read from the recorder at every sync: no Store keeps
it, so a restart starts from what Home Assistant has stored, and so does a
reload, but for the days whose import was not confirmed, which an entry
keeps across its reloads. In memory (``LaneState``) a lane keeps only what
the recorder cannot tell: the days already asked for, whose missing hours
Meridian does not have; which of them returned nothing; a refusal of dates
before the supply started; the days whose request or import failed, each
with the time it is tried again and whether its import was confirmed; and
its monthly revision pass. Its cadence is its own: those retry times, its
revision day, and the reconciliation it owes (``units.UnitState``).

Each update but a coordinator's first, once its newest windows, its
reconciliation and any catch-up have been fetched and imported, asks for at
most 6 windows more of each lane (``COVERAGE_WINDOWS``), each request
starting 2 to 5 s after the last one ended: first the days still missing,
newest first (``missing_days``), a week at most to a window and one day
alone for a day whose request failed; then, during the lane's revision
pass, the days stored, for readings Meridian has revised since
(``revision_days``). Only the hours older than the 14 days the
reconciliations revise are asked for this way, and none of a date the
update's own walk fetched in full (``coverage_end``). A lane without
statistics has its initial import instead, whose 13 windows ask for 90
days in one update, and its coverage starts at the update after.

The history starts after a refusal of dates before the supply, confirmed
by the date before a refused one, or where 14 days in a row before the
oldest stored day returned nothing (``LaneState.history_start``): two empty
windows of a week, as the walks count them. A shorter gap, as a meter that
sent nothing for a week, is asked across, and readings older than it are
kept; empty days between stored days start nothing. A lane reaches its
target once no date misses an hour Meridian may still have
(``reaches_target``).
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from hashlib import sha256
from itertools import pairwise

from .const import (
    EMPTY_WINDOWS_AT_HISTORY_START,
    INITIAL_BACKFILL,
    REVISION_OVERLAP,
    WINDOW_DAYS,
)
from .units import backoff
from .windows import HOUR, NZ, ceil_hour, local_midnight

# Windows each lane may ask for in an update, after its own walk, and the
# pause between two of those requests, chosen at random.
COVERAGE_WINDOWS = 6
COVERAGE_PAUSE_SECONDS = (2.0, 5.0)
# The furthest back a lane's target may be, for a long billing period.
LONGEST_TARGET = timedelta(days=400)
# Days in a row with no readings, before the oldest stored day, that mark the
# start of the history: two empty windows of a week.
EMPTY_DAYS_AT_HISTORY_START = EMPTY_WINDOWS_AT_HISTORY_START * WINDOW_DAYS
# A lane's revision pass starts on one day of each month, 1 to 28, spread by
# the lane's key so that not every lane asks Meridian on the same day; or, when
# no update covered the lane that day, at the first that does in the 3 days
# after it.
REVISION_SLOTS = 28
REVISION_GRACE = timedelta(days=3)
# What a window of a lane's coverage is for.
COVERAGE = "coverage"
REVISION = "revision"
_DAY = timedelta(days=1)
# A day whose wait ends at the next hourly update may be tried a little
# early: Home Assistant schedules the update an hour after the last ended.
_EARLY_START = timedelta(minutes=1)


def backfill_start(now: datetime) -> datetime:
    """
    Return New Zealand midnight at the start of the date 90 days back.

    A lane's target without an older billing period: an initial import
    keeps its rows from 90 days back to the hour, so the lane's coverage
    then asks once for the hours of that date before them.
    """
    return local_midnight((now - INITIAL_BACKFILL).astimezone(NZ).date())


def coverage_target(
    now: datetime, billing_start: date | None, *, unsaved: Iterable[date] = ()
) -> datetime:
    """
    Return the oldest hour a lane's history should reach.

    New Zealand midnight at the start of the older of the date 90 days back
    and the billing period's first date, when one is known, never more than
    400 days back: then the first midnight after that. A midnight, so that
    every date the lane asks for is asked for whole, and a date fetched in
    full has no hour left out that a target moved back, as once the billing
    period is known, would need. Nor is it newer than a date whose import
    Home Assistant did not confirm (``LaneState.unsaved``): such a date is
    asked for, and replayed in full, however the billing period moves.
    """
    target = backfill_start(now)
    if billing_start is not None:
        target = min(target, local_midnight(billing_start))
    if dates := list(unsaved):
        target = min(target, local_midnight(min(dates)))
    oldest = (now - LONGEST_TARGET).astimezone(NZ).date()
    if local_midnight(oldest) < now - LONGEST_TARGET:
        oldest += _DAY
    return max(target, local_midnight(oldest))


def coverage_end(
    now: datetime, frontier: datetime | None, fetched_from: date | None
) -> date:
    """
    Return the first New Zealand date a lane's coverage does not ask for.

    That is the date after the one 14 days back: the reconciliations revise
    the hours from 14 days back on, and the coverage the hours of that date
    before them. Nor does it ask for a date after the newest stored hour's,
    the frontier: the walks reach back to it, and a supply that ended has
    no readings after it. Nor for the dates the update's own walk fetched
    in full, from ``fetched_from`` on.
    """
    end = (now - REVISION_OVERLAP).astimezone(NZ).date() + _DAY
    if frontier is not None:
        end = min(end, frontier.astimezone(NZ).date() + _DAY)
    if fetched_from is not None:
        end = min(end, fetched_from)
    return end


def revision_day(key: str) -> int:
    """Return the day of the month a lane's revision pass starts on, 1 to 28."""
    return int(sha256(key.encode()).hexdigest()[:8], 16) % REVISION_SLOTS + 1


def revision_due(key: str, today: date) -> date | None:
    """
    Return the revision day a lane's pass may start for today, if any.

    The lane's day of this month, from that day on, or of the month before
    until then, when today is that day or one of the 3 after it
    (``REVISION_GRACE``): a pass missed on its day, as when Home Assistant
    was off or every update failed, starts at the next update that covers
    the lane.
    """
    day = revision_day(key)
    if not 1 <= day <= REVISION_SLOTS:
        return None
    month = today if today.day >= day else today.replace(day=1) - _DAY
    slot = month.replace(day=day)
    return slot if today - slot <= REVISION_GRACE else None


def missing_recent(stored: Iterable[datetime], now: datetime) -> datetime | None:
    """
    Return the oldest hour missing between stored hours of the last 14 days.

    The daily reconciliation reaches back to it, as Meridian may publish a
    day's readings late without estimates for them first. A gap that
    started before those 14 days, after the newest stored hour older than
    them, counts from their first whole hour: the coverage asks only for
    the hours before it. Hours before the oldest stored hour are not
    between stored hours: a short history has nothing before it, and a
    history that ended before the 14 days has no gap in them.
    """
    line = now - REVISION_OVERLAP
    hours = sorted(stored)
    recent = [hour for hour in hours if hour >= line]
    if not recent:
        return None
    if older := [hour for hour in hours if hour < line]:
        expected = max(older[-1] + HOUR, ceil_hour(line))
        if recent[0] > expected:
            return expected
    return next(
        (
            earlier + HOUR
            for earlier, later in pairwise(recent)
            if later - earlier > HOUR
        ),
        None,
    )


@dataclass(slots=True)
class LaneState:
    """
    What a lane remembers between syncs, in memory only.

    ``fetched`` holds the New Zealand dates a coverage window or a walk
    fetched in full: an hour still missing there is one Meridian does not
    have, so it is not asked for again until the revision pass or a restart.
    ``empty`` holds those that returned no readings, for the history start.
    ``refused_through`` is the newest date of a window Meridian refused as
    before the supply started. ``bad`` maps each date whose request or
    import failed to when it is tried again, 1 h, then 2 h, doubling up to
    24 h for that date's own failures in a row (``tries``), so a date that
    keeps failing waits longer while other dates come back; ``unsaved``
    holds those of them whose import Home Assistant did not confirm, which
    are replayed in full when they come back. ``revising`` says a revision
    pass is running, which started for the revision day ``revision_on`` and
    has asked for the dates in ``revised``. A pass forgets which dates were
    fetched and where the history starts, to ask again; ``known_fetched``,
    ``known_empty`` and ``known_refused_through`` keep what it forgot until
    it has asked again, for what the lane reports alone (``reaches_target``,
    ``history_start(known=True)``). ``missing_recent`` is the oldest hour
    missing between stored hours of the last 14 days, read at the sync's
    start. ``reaches_target`` and ``history_start_found`` are what the
    lane's last coverage found, for diagnostics.
    """

    fetched: set[date] = field(default_factory=set)
    empty: set[date] = field(default_factory=set)
    refused_through: date | None = None
    bad: dict[date, datetime] = field(default_factory=dict)
    tries: dict[date, int] = field(default_factory=dict)
    unsaved: set[date] = field(default_factory=set)
    revision_on: date | None = None
    revising: bool = False
    revised: set[date] = field(default_factory=set)
    known_fetched: set[date] = field(default_factory=set)
    known_empty: set[date] = field(default_factory=set)
    known_refused_through: date | None = None
    missing_recent: datetime | None = None
    reaches_target: bool = False
    history_start_found: bool = False

    def copy(self) -> LaneState:
        """Return a copy that shares no mutable state with this one."""
        return LaneState(
            fetched=set(self.fetched),
            empty=set(self.empty),
            refused_through=self.refused_through,
            bad=dict(self.bad),
            tries=dict(self.tries),
            unsaved=set(self.unsaved),
            revision_on=self.revision_on,
            revising=self.revising,
            revised=set(self.revised),
            known_fetched=set(self.known_fetched),
            known_empty=set(self.known_empty),
            known_refused_through=self.known_refused_through,
            missing_recent=self.missing_recent,
            reaches_target=self.reaches_target,
            history_start_found=self.history_start_found,
        )

    def history_start(self, oldest: date | None, *, known: bool = False) -> date | None:
        """
        Return the first date of the history, once it is known.

        ``oldest`` is the oldest date with a stored hour. A refusal of dates
        before the supply started marks the date after it, unless stored
        days at or before it show the history goes on; otherwise 14 days in
        a row before the oldest stored day that returned nothing mark that
        day: two empty weeks, as the walks count them. With ``known``, for
        what the lane reports, the start a revision pass forgot counts too,
        until the pass finds one.
        """
        start = _history_start(self.refused_through, self.empty, oldest)
        if start is None and known:
            start = _history_start(
                self.refused_through or self.known_refused_through,
                self.empty | self.known_empty,
                oldest,
            )
        return start

    def restore_unsaved(self, retries: Mapping[date, datetime]) -> None:
        """
        Take over dates another coordinator's lane left unconfirmed.

        Each waits until its own retry time, then is asked for alone and
        replayed in full, as in the lane that failed it (``fail``).
        """
        for day, retry in retries.items():
            self.bad[day] = retry
            self.tries.setdefault(day, 1)
            self.unsaved.add(day)

    def unsaved_retries(self, now: datetime) -> dict[date, datetime]:
        """Return the dates not confirmed, with when each is tried again."""
        return {day: self.bad.get(day, now) for day in self.unsaved}

    def waits(self, day: date, now: datetime) -> bool:
        """Return whether a date whose request failed still waits to be tried again."""
        retry = self.bad.get(day)
        return retry is not None and now < retry - _EARLY_START

    def took(
        self, days: Collection[date], *, empty: Iterable[date], revised: bool = False
    ) -> None:
        """
        Record dates fetched in full and imported, and which returned nothing.

        They are no longer waiting after a failure, and their failures in a
        row start again; ``revised`` counts them as asked for by the
        revision pass too. What a pass forgot of them no longer counts.
        """
        self.fetched.update(days)
        self.empty.difference_update(days)
        self.empty.update(empty)
        self.known_fetched.difference_update(days)
        self.known_empty.difference_update(days)
        self.settle(days)
        if revised:
            self.revised.update(days)

    def settle(self, days: Iterable[date]) -> None:
        """
        Record dates imported in full and confirmed: none of them waits now.

        Their failures in a row start again, and an import of theirs that
        Home Assistant did not confirm no longer counts: the one that was
        confirmed wrote every hour of theirs it fetched again.
        """
        for day in days:
            self.bad.pop(day, None)
            self.tries.pop(day, None)
            self.unsaved.discard(day)

    def fail(
        self, days: Iterable[date], now: datetime, *, unsaved: bool = False
    ) -> None:
        """
        Record dates whose request or import failed: they wait, then go again.

        Each waits 1 h after its first failure, then twice as long after
        each one in a row, up to 24 h. ``unsaved`` says Home Assistant did
        not confirm their import, which may have saved part of it.
        """
        for day in days:
            tries = self.tries[day] = self.tries.get(day, 0) + 1
            self.bad[day] = now + backoff(tries)
            if unsaved:
                self.unsaved.add(day)

    def start_revision(self, key: str, today: date) -> bool:
        """
        Start the lane's revision pass, once a month, on or after its day.

        On the lane's day of the month, or at the first update that covers
        it in the 3 days after (``revision_due``). The pass asks again for
        every stored date from the target to 14 days back, and for the dates
        fetched before, whose readings Meridian may have published since, so
        it forgets them, and with them where the history starts, whether
        empty days or a refusal showed it: it asks for that again too. What
        it forgets is kept apart (``known_fetched``, ``known_empty``,
        ``known_refused_through``) until it has asked again, so a pass does
        not change what the lane reports. Return whether it started.
        """
        slot = revision_due(key, today)
        if slot is None or self.revision_on == slot:
            return False
        self.revision_on = slot
        self.revising = True
        self.revised.clear()
        self.known_fetched.update(day for day in self.fetched if day not in self.bad)
        self.known_empty.update(self.empty)
        if self.refused_through is not None:
            self.known_refused_through = self.refused_through
        self.fetched.clear()
        self.empty.clear()
        self.refused_through = None
        return True

    def forget_before(self, day: date) -> None:
        """Forget dates older than a lane's target: it never asks for them."""
        self.fetched = {item for item in self.fetched if item >= day}
        self.empty = {item for item in self.empty if item >= day}
        self.revised = {item for item in self.revised if item >= day}
        self.known_fetched = {item for item in self.known_fetched if item >= day}
        self.known_empty = {item for item in self.known_empty if item >= day}
        self.forget_bad_before(day)

    def forget_bad_before(self, day: date) -> None:
        """Stop waiting to ask for dates before a lane's target or history start."""
        self.bad = {item: at for item, at in self.bad.items() if item >= day}
        self.tries = {item: n for item, n in self.tries.items() if item >= day}
        self.unsaved = {item for item in self.unsaved if item >= day}


def _history_start(
    refused_through: date | None, empty: Collection[date], oldest: date | None
) -> date | None:
    """Return the history's first date that a refusal or empty days show."""
    if refused_through is not None and (oldest is None or refused_through < oldest):
        return refused_through + _DAY
    if oldest is None:
        return None
    day = oldest - _DAY
    for _offset in range(EMPTY_DAYS_AT_HISTORY_START):
        if day not in empty:
            return None
        day -= _DAY
    return oldest


def oldest_day(stored: Collection[datetime]) -> date | None:
    """Return the New Zealand date of the oldest stored hour, if any."""
    return min(stored).astimezone(NZ).date() if stored else None


def first_day(
    state: LaneState,
    stored: Collection[datetime],
    target: datetime,
    *,
    known: bool = False,
) -> date:
    """
    Return the oldest date a lane asks for: its target's, or the history's start.

    With ``known``, the oldest it reports on (``LaneState.history_start``).
    """
    first = target.astimezone(NZ).date()
    start = state.history_start(oldest_day(stored), known=known)
    return first if start is None else max(first, start)


def has_missing_hour(
    day: date,
    stored: Collection[datetime],
    target: datetime,
    frontier: datetime | None,
) -> bool:
    """
    Return whether a date has an hour that is not stored.

    From the target on, and up to the frontier, the newest stored hour:
    the hours after it are the walks' to fetch, or have no readings, as
    after a supply ended.
    """
    end = local_midnight(day + _DAY)
    if frontier is not None:
        end = min(end, frontier)
    hour = ceil_hour(max(target, local_midnight(day)))
    while hour < end:
        if hour not in stored:
            return True
        hour += HOUR
    return False


def missing_days(
    state: LaneState,
    stored: Collection[datetime],
    *,
    target: datetime,
    end: date,
    frontier: datetime | None,
    now: datetime,
) -> list[date]:
    """
    Return the dates a lane still asks for, newest first.

    A date before ``end`` and from the target's, or from the history's
    start, on: one with an hour from the target on, up to the frontier,
    that is not stored, unless the lane has fetched it already, or it waits
    after a failure;
    and one whose wait after a failure has passed, even when its usage is
    stored, since an import Home Assistant did not confirm may have saved
    its usage without its cost.
    """
    first = first_day(state, stored, target)
    days: list[date] = []
    day = end - _DAY
    while day >= first:
        if not state.waits(day, now) and (
            day in state.bad
            or (
                day not in state.fetched
                and has_missing_hour(day, stored, target, frontier)
            )
        ):
            days.append(day)
        day -= _DAY
    return days


def reaches_target(
    state: LaneState,
    stored: Collection[datetime],
    *,
    target: datetime,
    end: date,
    frontier: datetime | None,
) -> bool:
    """
    Return whether a lane's history is complete back to its target.

    Every date before ``end`` and from the target's, or the history's
    start, on has every hour from the target on, up to the frontier,
    stored, or was fetched in full, when Meridian does not have its missing
    hours; and no date waits after a failure with an hour missing, nor
    after an import Home Assistant did not confirm. A revision pass leaves
    that as it was: what it forgot to ask again counts until it has asked
    (``LaneState.start_revision``), and a stored date whose revision failed
    has nothing missing.
    """
    first = first_day(state, stored, target, known=True)
    day = end - _DAY
    while day >= first:
        if day in state.unsaved or (
            day not in state.known_fetched
            and (day not in state.fetched or day in state.bad)
            and has_missing_hour(day, stored, target, frontier)
        ):
            return False
        day -= _DAY
    return True


def revision_days(
    state: LaneState,
    stored: Collection[datetime],
    *,
    target: datetime,
    end: date,
) -> list[date]:
    """
    Return the stored dates a running revision pass still asks for, newest first.

    Those before ``end`` and from the target's, or the history's start, on
    that the pass has not asked for yet, nor the lane's coverage fetched
    since the pass started. A date whose request or import failed is not
    among them: it waits, then its coverage asks for it alone
    (``missing_days``), which revises it too, and the pass goes on past it.
    """
    if not state.revising:
        return []
    first = first_day(state, stored, target)
    days = {
        day
        for hour in stored
        if first <= (day := hour.astimezone(NZ).date()) < end
        and day not in state.revised
        and day not in state.fetched
        and day not in state.bad
    }
    return sorted(days, reverse=True)


def plan_spans(
    state: LaneState,
    missing: list[date],
    revision: list[date],
    *,
    now: datetime,
) -> list[tuple[date, date, str]]:
    """
    Return a lane's windows as (first date, last date, kind), newest first.

    The missing dates first, then the revision pass's: dates in a row join
    one window of a week at most, and a date tried again after a failure
    goes alone, as asking for less may then help.
    """
    spans: list[tuple[date, date, str]] = []
    for kind, days in ((COVERAGE, missing), (REVISION, revision)):
        newest: date | None = None
        oldest: date | None = None
        for day in days:
            alone = day in state.bad and not state.waits(day, now)
            if (
                newest is not None
                and oldest is not None
                and not alone
                and day == oldest - _DAY
                and (newest - day).days < WINDOW_DAYS
                and oldest not in state.bad
            ):
                oldest = day
                continue
            if newest is not None and oldest is not None:
                spans.append((oldest, newest, kind))
            newest = oldest = day
        if newest is not None and oldest is not None:
            spans.append((oldest, newest, kind))
    return spans
