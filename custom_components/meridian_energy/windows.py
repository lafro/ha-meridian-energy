"""
Bounded measurement windows: planning, page sizes, row checks and import rules.

Every measurements request asks for one window of whole New Zealand dates,
both ends included, at most a week long. A direction's windows are fetched
newest first, each ending the day before the next newer one starts, the
newest ending today and the oldest starting on the New Zealand date of the
first hour the sync keeps, the first whole hour from its cutoff. A window's
rows run from New Zealand midnight at the start of its first date to New
Zealand midnight after its last: 167 to 169 hours for a week, across a
daylight-saving change.

The stored-aware rules decide what a direction that already has statistics
imports when Meridian no longer returns readings Home Assistant has stored:
those stay as stored. Every import is a replay (``statistics.async_replay``),
which keeps the running sums consistent on its own: it writes again the
stored state of each hour in its span it was not given, and shifts the rows
after it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .const import (
    AGGREGATE_CHANNEL_ID,
    MAX_WINDOW_PAGES,
    MIN_PAGE_SIZE,
    PAGE_SIZE,
    WINDOW_DAYS,
    WINDOW_OVERLAP_DAYS,
)
from .models import MeridianMeasurement

NZ = ZoneInfo("Pacific/Auckland")
HOUR = timedelta(hours=1)
_ACTUAL = "ACTUAL"
# Headroom over the rows a window is expected to hold, and a few spare rows.
_PAGE_HEADROOM = 1.25
_PAGE_SPARE_ROWS = 4

MeasurementKey = tuple[datetime, str]


def local_midnight(day: date) -> datetime:
    """Return midnight in New Zealand at the start of a date, in UTC."""
    return datetime.combine(day, time.min, tzinfo=NZ).astimezone(UTC)


def ceil_hour(value: datetime) -> datetime:
    """Return the start of the first whole hour at or after a time."""
    floored = value.replace(minute=0, second=0, microsecond=0)
    return floored if floored == value else floored + HOUR


def first_date(since: datetime) -> date:
    """
    Return the New Zealand date of the first hour a sync from ``since`` keeps.

    Rows start on whole hours, and none before ``since`` is kept, so the
    first is the whole hour at or after it. Its date is that of ``since``
    except within the hour before New Zealand midnight: a cutoff at 23:30
    keeps nothing of its own date, so no window asks for it.
    """
    hour = since.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    if hour < since:
        hour += HOUR
    return hour.astimezone(NZ).date()


@dataclass(frozen=True, slots=True)
class MeasurementWindow:
    """One request window: New Zealand dates, both included, newest first."""

    start_on: date
    end_on: date
    index: int
    count: int

    @property
    def start(self) -> datetime:
        """Return the start of the window's first hour, in UTC."""
        return local_midnight(self.start_on)

    @property
    def end(self) -> datetime:
        """Return the end of the window's last hour, in UTC."""
        return local_midnight(self.end_on + timedelta(days=1))

    @property
    def last_hour(self) -> datetime:
        """Return the start of the window's last hour, 23:00 on its last date."""
        return self.end - HOUR

    @property
    def hours(self) -> float:
        """Return how many hours the window holds."""
        return (self.end - self.start) / HOUR

    @property
    def days(self) -> int:
        """Return how many dates the window covers."""
        return (self.end_on - self.start_on).days + 1


def plan_windows(
    since: datetime,
    now: datetime,
    *,
    days: int = WINDOW_DAYS,
    overlap: int | None = None,
) -> tuple[MeasurementWindow, ...]:
    """
    Return the windows that cover ``since`` to ``now``, newest first.

    The newest ends on today's New Zealand date and the oldest starts on the
    date of the first hour kept (``first_date``). Without ``overlap``
    (``WINDOW_OVERLAP_DAYS`` by default) each older window ends the day
    before the newer one starts; with an overlap of one day, on the day it
    starts.
    """
    if overlap is None:
        overlap = WINDOW_OVERLAP_DAYS
    if not 0 <= overlap < days - 1:
        raise ValueError("Invalid measurement window overlap")
    end = now.astimezone(NZ).date()
    first = min(first_date(since), end)
    spans: list[tuple[date, date]] = []
    while True:
        start = max(first, end - timedelta(days=days - 1))
        spans.append((start, end))
        if start <= first:
            break
        end = start - timedelta(days=1 - overlap)
    return tuple(
        MeasurementWindow(start, end, index, len(spans))
        for index, (start, end) in enumerate(spans, start=1)
    )


def page_size(hours: float, density: float) -> int:
    """
    Return the page size for a window, so one page normally holds it whole.

    ``density`` is the rows each hour holds (one per meter channel), at least
    one. The size has headroom, and stays between 24 and 744: a week of a
    meter with more than four channels, 840 rows or more, takes one page for
    every 744 rows it holds: two for five to eight channels, three for nine
    to thirteen.
    """
    requested = math.ceil(hours * max(1.0, density) * _PAGE_HEADROOM) + (
        _PAGE_SPARE_ROWS
    )
    return min(PAGE_SIZE, max(MIN_PAGE_SIZE, requested))


def page_limit(last: int) -> int:
    """
    Return how many pages of ``last`` rows one window may take.

    A window may hold 16 pages of 744 rows, 11,904 rows: 70 meter channels
    for a week. A smaller page size allows as many more pages, so a window
    sent before its channels are known, with pages for one channel, is not
    refused for needing more of them.
    """
    return max(MAX_WINDOW_PAGES, math.ceil(MAX_WINDOW_PAGES * PAGE_SIZE / last))


def rows_per_hour(measurements: tuple[MeridianMeasurement, ...]) -> float:
    """Return the rows each hour with readings holds: the number of channels."""
    hours = {item.start.astimezone(UTC) for item in measurements}
    return len(measurements) / len(hours) if hours else 0.0


class WindowRows:
    """
    The rows one window has returned so far, checked as each page arrives.

    A window keeps its own rows: they join the direction's rows only once
    the window is complete, so a failed window leaves nothing behind. Rows
    from before the window's start are counted and left out; their hours are
    kept, for the older window to be checked against. A past row at or after
    its end means Meridian read the window differently from how it was
    asked, which this fails on rather than import. Rows still to come, and
    rows before the sync's cutoff, are left out as before. A per-hour map of
    channels checks that an hour's readings can be told apart, within this
    window only.

    A reading is a row whose hour is over. Rows still to come, such as the
    estimates Meridian may return for the rest of today, are left out of
    ``inside``, ``oldest`` and ``last_day``: a window that holds nothing
    else returned no reading, and an empty week ends no walk early for
    them. They still count for the pages' progress checks, and for the edge
    hours a debug line reports.
    """

    def __init__(
        self, window: MeasurementWindow, *, since: datetime, now: datetime
    ) -> None:
        self.window = window
        self._since = since
        self._now = now
        self.rows: dict[MeasurementKey, MeridianMeasurement] = {}
        self._channels: dict[datetime, set[str]] = {}
        # Every hour and channel a page has held, kept or not.
        self._received: set[MeasurementKey] = set()
        # The hours and channels of the rows older than the window.
        self.before_keys: set[MeasurementKey] = set()
        self.pages = 0
        self.received = 0
        # The readings inside the window, and the oldest of their hours.
        self.inside = 0
        self.oldest: datetime | None = None
        self.before_window = 0
        # Whether any row for the window's first hour, or its last, came back.
        self.first_hour = False
        self.last_hour = False
        # Whether a reading on the window's last date came back.
        self.last_day = False

    def add_page(self, page_rows: tuple[MeridianMeasurement, ...]) -> tuple[int, int]:
        """
        Check and keep one page's rows.

        Return how many hours and channels the page added to those kept, and
        how many of its rows were older than the window. A second reading
        for an hour and channel already kept is not counted again.
        """
        self.pages += 1
        self.received += len(page_rows)
        kept_before = len(self.rows)
        before_window = 0
        last_day = local_midnight(self.window.end_on)
        for measurement in page_rows:
            start = measurement.start.astimezone(UTC)
            self._received.add((start, measurement.channel_id))
            interval_end = (measurement.end or measurement.start).astimezone(UTC)
            if start < self.window.start:
                before_window += 1
                self.before_keys.add((start, measurement.channel_id))
                continue
            if start >= self.window.end:
                if interval_end <= self._now:
                    raise ValueError(
                        "Meridian returned rows after the requested window"
                    )
                continue
            self.first_hour |= start == self.window.start
            self.last_hour |= start == self.window.last_hour
            if interval_end > self._now:
                continue
            self.inside += 1
            self.oldest = start if self.oldest is None else min(self.oldest, start)
            self.last_day |= start >= last_day
            if start < self._since:
                continue
            self._keep(start, measurement)
        self.before_window += before_window
        return len(self.rows) - kept_before, before_window

    def returned(self, keys: set[MeasurementKey]) -> bool:
        """Return whether the window's pages held all these hours and channels."""
        return keys <= self._received

    def join(self, day: WindowRows) -> None:
        """
        Add the rows of one of the window's dates, fetched alone, to the window's.

        Its rows are already checked; its pages and rows received stay its
        own, in the counts of what was asked for.
        """
        self.rows.update(day.rows)
        for hour, channels in day._channels.items():
            self._channels.setdefault(hour, set()).update(channels)
        self._received |= day._received
        self.before_keys |= day.before_keys
        self.inside += day.inside
        if day.oldest is not None:
            self.oldest = (
                day.oldest if self.oldest is None else min(self.oldest, day.oldest)
            )
        self.first_hour |= day.first_hour and day.window.start == self.window.start
        self.last_hour |= day.last_hour and day.window.end == self.window.end
        self.last_day |= day.last_day and day.window.end_on == self.window.end_on

    def holds_new_rows(self, page_rows: tuple[MeridianMeasurement, ...]) -> bool:
        """
        Return whether a page holds an hour and channel no earlier page held.

        A page boundary can fall inside an hour when a meter has several
        channels: the next page then starts with that hour's other channels,
        as old as the oldest row before it, and still moves the window on.
        """
        return any(
            (item.start.astimezone(UTC), item.channel_id) not in self._received
            for item in page_rows
        )

    def _keep(self, start: datetime, measurement: MeridianMeasurement) -> None:
        """Keep a reading, preferring an actual one, if its hour stays unambiguous."""
        channels = self._channels.setdefault(start, set())
        if (
            measurement.channel_id == AGGREGATE_CHANNEL_ID
            and channels - {AGGREGATE_CHANNEL_ID}
        ) or (
            measurement.channel_id != AGGREGATE_CHANNEL_ID
            and AGGREGATE_CHANNEL_ID in channels
        ):
            raise ValueError("Meridian measurement channels are ambiguous")
        key = (start, measurement.channel_id)
        existing = self.rows.get(key)
        if (
            existing is not None
            and existing.quality == measurement.quality
            and (
                measurement.channel_id == AGGREGATE_CHANNEL_ID
                or existing.value_kwh != measurement.value_kwh
            )
        ):
            raise ValueError("Meridian measurement channels are ambiguous")
        if existing is None or (
            existing.quality != _ACTUAL and measurement.quality == _ACTUAL
        ):
            self.rows[key] = measurement
            channels.add(measurement.channel_id)


@dataclass(frozen=True, slots=True)
class StoredHours:
    """The hours a direction's energy and cost statistics have stored sums for."""

    energy: frozenset[datetime] = frozenset()
    cost: frozenset[datetime] = frozenset()


@dataclass(frozen=True, slots=True)
class ImportPlan:
    """
    The rows a direction may import this sync, and why any were left out.

    ``cut_at_hole`` says rows at and before a stored hour Meridian did not
    return were left out, the newest such hour being ``newest_hole``;
    ``tail_not_returned`` that nothing is imported, since Meridian no longer
    returns stored hours newer than every row kept, which stay as stored.
    """

    measurements: tuple[MeridianMeasurement, ...]
    cut_at_hole: bool = False
    tail_not_returned: bool = False
    newest_hole: datetime | None = None


def importable(
    measurements: tuple[MeridianMeasurement, ...], stored: StoredHours
) -> ImportPlan:
    """
    Return the rows a direction imports, leaving stored rows not returned alone.

    A stored hour inside the fetched span that the fetch did not return
    suggests Meridian no longer returns the history around it as before, and
    so does a stored hour newer than every row fetched. So rows up to the
    newest such hole are left out, and when stored hours lie after what is
    left, nothing is imported. Until 0.4.0 this was also what kept the
    running sums consistent, since an import rewrote only the hours it was
    given; the replay now does that on its own. A hole with nothing stored
    in it leaves nothing behind: without stored rows, both sides of a gap
    are imported.
    """
    if not stored.energy or not measurements:
        return ImportPlan(measurements)
    fetched = {item.start.astimezone(UTC) for item in measurements}
    oldest, newest = min(fetched), max(fetched)
    holes = [
        hour
        for hour in stored.energy
        if oldest <= hour <= newest and hour not in fetched
    ]
    kept = measurements
    newest_hole = max(holes, default=None)
    if newest_hole is not None:
        kept = tuple(
            item for item in measurements if item.start.astimezone(UTC) > newest_hole
        )
    if max(stored.energy) > max(item.start.astimezone(UTC) for item in kept):
        return ImportPlan(
            (),
            cut_at_hole=bool(holes),
            tail_not_returned=True,
            newest_hole=newest_hole,
        )
    return ImportPlan(kept, cut_at_hole=bool(holes), newest_hole=newest_hole)


@dataclass(frozen=True, slots=True)
class CostPlan:
    """
    Which hours of an import may have their cost written.

    ``after`` is the newest hour with a stored cost that came back without
    one: only the cost of the hours after it is written, from its stored sum.
    ``skipped`` says no cost may be written at all.
    """

    after: datetime | None = None
    skipped: bool = False

    def writes(self, hour: datetime) -> bool:
        """Return whether the cost of an hour is written."""
        return not self.skipped and (self.after is None or hour > self.after)


def cost_plan(
    measurements: tuple[MeridianMeasurement, ...], stored_cost: frozenset[datetime]
) -> CostPlan:
    """
    Return which hours' cost can be written with these rows.

    The replay writes a cost only for an hour whose cost is complete: an
    hour whose cost is missing keeps the cost stored for it. Where a cost
    is stored for an hour that came back without one, Meridian no longer
    returns that cost as before, so, as the usage is cut at a hole, the
    cost of every hour up to the newest such hour is left as stored, and
    that of the hours after it is written, following on from its stored
    sum. Stored cost hours newer than every hour left with a cost: then no
    cost is written, and the usage alone is. When no row has a cost, as
    when Meridian publishes an hour's usage before its cost, no cost row is
    written at all, and nothing waits.
    """
    if not stored_cost:
        return CostPlan()
    hours = {item.start.astimezone(UTC) for item in measurements}
    without_cost = {
        item.start.astimezone(UTC) for item in measurements if item.cost_cents is None
    }
    after = max(without_cost & stored_cost, default=None)
    with_cost = {hour for hour in hours - without_cost if after is None or hour > after}
    skipped = bool(with_cost) and max(stored_cost) > max(with_cost)
    return CostPlan(after, skipped=skipped)
