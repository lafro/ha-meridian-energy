"""Measurements windows of whole New Zealand dates, and the readings kept from them."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import NamedTuple
from zoneinfo import ZoneInfo

from .const import AGGREGATE_CHANNEL_ID, WINDOW_DAYS
from .models import MeridianMeasurement

NZ = ZoneInfo("Pacific/Auckland")
HOUR = timedelta(hours=1)
_DAY = timedelta(days=1)
_ACTUAL = "ACTUAL"
# Room in ``last`` over the hours a window holds: a quarter, and 4 rows.
_PAGE_HEADROOM = 1.25
_PAGE_SPARE_ROWS = 4


def local_midnight(day: date) -> datetime:
    """Return midnight in New Zealand at the start of a date, in UTC."""
    return datetime.combine(day, time.min, tzinfo=NZ).astimezone(UTC)


def nz_date(moment: datetime) -> date:
    """Return the New Zealand date of a moment."""
    return moment.astimezone(NZ).date()


def date_hours(day: date) -> tuple[datetime, ...]:
    """Return the UTC starts of a New Zealand date's hours: 23, 24 or 25."""
    start = local_midnight(day)
    count = round((local_midnight(day + _DAY) - start) / HOUR)
    return tuple(start + HOUR * offset for offset in range(count))


@dataclass(frozen=True, slots=True)
class MeasurementWindow:
    """One request window: New Zealand dates, both included."""

    start_on: date
    end_on: date

    @property
    def start(self) -> datetime:
        """Return the start of the window's first hour, in UTC."""
        return local_midnight(self.start_on)

    @property
    def end(self) -> datetime:
        """Return the end of the window's last hour, in UTC."""
        return local_midnight(self.end_on + _DAY)

    @property
    def hours(self) -> int:
        """Return how many hours the window holds."""
        return round((self.end - self.start) / HOUR)

    @property
    def days(self) -> int:
        """Return how many dates the window covers."""
        return (self.end_on - self.start_on).days + 1

    @property
    def last(self) -> int:
        """Return the page size to ask for: the hours, a quarter more, and 4."""
        return math.ceil(self.hours * _PAGE_HEADROOM) + _PAGE_SPARE_ROWS

    def dates(self) -> list[date]:
        """Return the window's dates, oldest first."""
        return [self.start_on + _DAY * offset for offset in range(self.days)]


def windows_for(
    dates: Iterable[date], size: int = WINDOW_DAYS
) -> tuple[MeasurementWindow, ...]:
    """Group dates, newest first, into windows of up to ``size`` dates, gaps and all."""
    windows: list[MeasurementWindow] = []
    end_on: date | None = None
    start_on: date | None = None
    for day in sorted(set(dates), reverse=True):
        if end_on is not None and start_on is not None and (end_on - day).days < size:
            start_on = day
            continue
        if end_on is not None and start_on is not None:
            windows.append(MeasurementWindow(start_on, end_on))
        end_on = start_on = day
    if end_on is not None and start_on is not None:
        windows.append(MeasurementWindow(start_on, end_on))
    return tuple(windows)


class Reading(NamedTuple):
    """One hour's reading: kWh, cents (None when a channel had none) and quality."""

    energy: Decimal
    cost_cents: Decimal | None
    quality: str


def hourly_readings(
    rows: Iterable[MeridianMeasurement], window: MeasurementWindow, now: datetime
) -> dict[datetime, Reading]:
    """
    Return a window's readings by UTC hour start, its channels summed.

    Only rows inside the window whose hour has ended count. Each hour and
    channel keeps one row, an actual reading over an estimate. An hour is
    ambiguous, and refused (``ValueError``), when an aggregate row comes
    beside a register's, or one channel has two rows of one quality that
    differ, or two aggregate rows of one quality.
    """
    kept: dict[tuple[datetime, str], MeridianMeasurement] = {}
    channels: dict[datetime, set[str]] = {}
    for row in rows:
        start = _hour_start(row)
        if not window.start <= start < window.end:
            continue
        if (row.end or start + HOUR).astimezone(UTC) > now:
            continue
        _keep(kept, channels.setdefault(start, set()), start, row)
    readings: dict[datetime, Reading] = {}
    for (start, _channel), row in sorted(kept.items()):
        cost = row.cost_cents
        previous = readings.get(start)
        if previous is not None:
            cost = (
                None
                if cost is None or previous.cost_cents is None
                else previous.cost_cents + cost
            )
        readings[start] = Reading(
            energy=row.value_kwh + (previous.energy if previous else 0),
            cost_cents=cost,
            quality=(
                previous.quality
                if previous is not None and previous.quality != _ACTUAL
                else row.quality
            ),
        )
    return readings


def _hour_start(row: MeridianMeasurement) -> datetime:
    """Return a row's UTC hour start, refusing one off the hour or not finite."""
    start = row.start.astimezone(UTC)
    if start.minute or start.second or start.microsecond:
        raise ValueError("Meridian measurement is not aligned to an hour")
    if not row.value_kwh.is_finite():
        raise ValueError("Meridian measurement value is not finite")
    if row.cost_cents is not None and not row.cost_cents.is_finite():
        raise ValueError("Meridian measurement cost is not finite")
    return start


def _keep(
    kept: dict[tuple[datetime, str], MeridianMeasurement],
    channels: set[str],
    start: datetime,
    row: MeridianMeasurement,
) -> None:
    """Keep a row, an actual one over an estimate, if its hour stays unambiguous."""
    aggregate = row.channel_id == AGGREGATE_CHANNEL_ID
    if (aggregate and channels - {AGGREGATE_CHANNEL_ID}) or (
        not aggregate and AGGREGATE_CHANNEL_ID in channels
    ):
        raise ValueError("Meridian measurement channels are ambiguous")
    key = (start, row.channel_id)
    existing = kept.get(key)
    if (
        existing is not None
        and existing.quality == row.quality
        and (aggregate or existing.value_kwh != row.value_kwh)
    ):
        raise ValueError("Meridian measurement channels are ambiguous")
    if existing is None or (existing.quality != _ACTUAL and row.quality == _ACTUAL):
        kept[key] = row
        channels.add(row.channel_id)
