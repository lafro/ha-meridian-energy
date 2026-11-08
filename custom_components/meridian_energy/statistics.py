"""External statistics for Meridian Energy, written by ``async_write_hours``."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
from hashlib import sha256

from homeassistant.components.recorder import get_instance  # type: ignore[attr-defined]
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    statistics_during_period,
)
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.util.unit_conversion import EnergyConverter

from .const import (
    DOMAIN,
    STAT_CONSUMPTION,
    STAT_CONSUMPTION_COST,
    STAT_GENERATION,
    STAT_GENERATION_CREDIT,
)

_LOGGER = logging.getLogger(__name__)
_COST_UNIT = "NZD"
CLEAR_STATISTICS_TIMEOUT_SECONDS = 30
_HOUR = timedelta(hours=1)
# A write reads the stored rows from 14 days before its earliest hour.
_NEAR_LOOKBACK = timedelta(days=14)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# How far a stored value may be from the one expected: rounding of floats.
_SUM_TOLERANCE = 1e-6
# In hass.data[DOMAIN]: one lock per statistic, held from a write's barrier
# until its rows are queued, shared by every coordinator an entry has had.
WRITE_LOCKS = "write_locks"


def property_key(account_number: str, property_id: str) -> str:
    """Return a stable non-sensitive key for a Meridian property."""
    value = f"{account_number}:{property_id}".encode()
    return sha256(value).hexdigest()[:12]


def account_key(account_number: str) -> str:
    """Return a stable non-sensitive key for a Meridian account."""
    return sha256(account_number.encode()).hexdigest()[:12]


def statistic_id(kind: str, key: str) -> str:
    """Return a valid external statistic ID."""
    return f"{DOMAIN}:{kind}_{key}"


async def _async_run_removal(
    hass: HomeAssistant, statistic_ids: list[str], *, seconds: float | None = None
) -> bool:
    """Queue a removal on the recorder thread; return whether it ran in time."""
    done = asyncio.Event()

    def _clear_done() -> None:
        hass.loop.call_soon_threadsafe(done.set)

    get_instance(hass).async_clear_statistics(statistic_ids, on_done=_clear_done)
    try:
        async with asyncio.timeout(
            CLEAR_STATISTICS_TIMEOUT_SECONDS if seconds is None else seconds
        ):
            await done.wait()
    except TimeoutError:
        return False
    return True


async def async_stored_hours(
    hass: HomeAssistant, statistic_ids: set[str], start: datetime, end: datetime
) -> dict[str, frozenset[datetime]]:
    """
    Return each statistic's hours from ``start`` until ``end`` with a stored sum.

    One read on the recorder's executor for every statistic; none, no hours.
    """
    instance = get_instance(hass)
    result = await instance.async_add_executor_job(
        partial(
            statistics_during_period,
            hass,
            start,
            end,
            statistic_ids,
            "hour",
            None,
            {"sum"},
        )
    )
    return {
        stat_id: frozenset(
            datetime.fromtimestamp(_timestamp(row["start"]), tz=UTC)
            for row in result.get(stat_id, [])
            if row.get("sum") is not None
        )
        for stat_id in statistic_ids
    }


@dataclass(frozen=True, slots=True)
class _Row:
    """One stored hourly row: its start, its state and its running sum."""

    start: datetime
    state: float | None
    sum: float | None


async def async_write_hours(
    hass: HomeAssistant,
    statistics: Iterable[tuple[StatisticMetaData, Mapping[datetime, Decimal]]],
) -> bool:
    """
    Bring external statistics in line with the hourly states given for them.

    Each statistic comes with its hours' states, keyed by UTC hour start.
    One barrier first waits up to 30 s for the recorder's earlier work, so
    the reads normally include the writes queued before. On MySQL or
    MariaDB an import that met a lock error queues itself again behind that
    barrier and can be saved after the reads; the seam it leaves heals at
    the next write whose read covers that hour. On a timeout nothing is
    read or written, and this returns False. True means the rows were
    queued, not that they were saved. After the barrier, for each statistic:

    1. read its stored rows from 14 days before its earliest hour on;
    2. start the span at the earlier of its first hour not stored as given
       and the first row read whose sum does not step from the row before
       by its own state, a seam; with neither, leave it as it is;
    3. end the span at the later of its newest hour and the newest stored;
    4. continue the sums from the newest stored row with a sum before the
       span, searching back with no limit; with none, from zero;
    5. give each hour of the span its given state, else its stored one.

    The rows are queued with no await between them, one call (one recorder
    transaction) per statistic. A dropped write heals at a later one, which
    finds the stored rows differ. An "Adjust sum" in the span, or anywhere
    in the rows read (where it is a seam), is overwritten.

    Each statistic is given at most once per call; a caller merges a
    unit's hours into one mapping. Callers must not write one statistic
    from two calls at once: each would read the stored rows before the
    other's are saved, and the later one would leave a seam. The
    coordinator's refresh lock serialises its own writes; the per-statistic
    locks held here from the barrier until the rows are queued cover a
    refresh that outlives a reload of its entry.
    """
    given = [(metadata, hours) for metadata, hours in statistics if hours]
    ids = [metadata["statistic_id"] for metadata, _hours in given]
    if len(set(ids)) != len(ids):
        raise ValueError("Each statistic may be given once per call")
    if not given:
        return True
    locks: dict[str, asyncio.Lock] = hass.data.setdefault(DOMAIN, {}).setdefault(
        WRITE_LOCKS, {}
    )
    held: list[asyncio.Lock] = []
    try:
        for statistic_id in sorted(ids):
            lock = locks.setdefault(statistic_id, asyncio.Lock())
            await lock.acquire()
            held.append(lock)
        return await _async_write_locked(hass, given)
    finally:
        for lock in held:
            lock.release()


async def _async_write_locked(
    hass: HomeAssistant,
    given: list[tuple[StatisticMetaData, Mapping[datetime, Decimal]]],
) -> bool:
    """Write statistics whose locks are held: the barrier, the reads, the queue."""
    if not await _async_run_removal(hass, []):
        _LOGGER.debug(
            "The recorder stayed busy for %s s, so these hours were not written; "
            "a later update writes them",
            CLEAR_STATISTICS_TIMEOUT_SECONDS,
        )
        return False
    planned = [
        (metadata, await _async_rows_to_write(hass, metadata["statistic_id"], hours))
        for metadata, hours in given
    ]
    # From here until every write is queued, nothing is awaited.
    for metadata, rows in planned:
        if rows:
            # No date: logs carry no value from Meridian's answers.
            _LOGGER.debug(
                "Writing %d hours of %s statistics",
                len(rows),
                _statistic_kind(metadata["statistic_id"]),
            )
            async_add_external_statistics(hass, metadata, rows)
    return True


async def _async_rows_to_write(
    hass: HomeAssistant, statistic_id: str, hours: Mapping[datetime, Decimal]
) -> list[StatisticData]:
    """Return the rows that bring one statistic in line with its hours."""
    read_from = min(hours) - _NEAR_LOOKBACK
    stored = (await _async_read_rows(hass, {statistic_id}, read_from, None))[
        statistic_id
    ]
    start = _span_start(hours, stored)
    if start is None:
        return []
    predecessor = _newest_sum_before(stored, start)
    if predecessor is None:
        older = await _async_read_rows(hass, {statistic_id}, _EPOCH, read_from)
        predecessor = _newest_sum_before(older[statistic_id], start)
    states = {row.start: _decimal(row.state) for row in stored if row.start >= start}
    states.update((hour, value) for hour, value in hours.items() if hour >= start)
    total = Decimal(0) if predecessor is None else _decimal(predecessor.sum)
    rows: list[StatisticData] = []
    for hour in sorted(states):
        total += states[hour]
        rows.append({"start": hour, "state": float(states[hour]), "sum": float(total)})
    return rows


def _span_start(
    hours: Mapping[datetime, Decimal], stored: list[_Row]
) -> datetime | None:
    """Return the first hour not stored as given or the first seam, if any."""
    by_start = {row.start: row for row in stored}
    changed = (
        hour
        for hour in sorted(hours)
        if (row := by_start.get(hour)) is None
        or not _close(float(hours[hour]), row.state)
    )
    seams = (
        row.start
        for index, row in enumerate(stored)
        if row.sum is None or (index and not _steps_to(stored[index - 1], row))
    )
    return min(
        (hour for hour in (next(changed, None), next(seams, None)) if hour is not None),
        default=None,
    )


def _newest_sum_before(rows: list[_Row], start: datetime) -> _Row | None:
    """Return the newest of some rows, oldest first, with a sum before an hour."""
    return next(
        (row for row in reversed(rows) if row.start < start and row.sum is not None),
        None,
    )


async def _async_read_rows(
    hass: HomeAssistant,
    statistic_ids: set[str],
    start: datetime,
    end: datetime | None,
) -> dict[str, list[_Row]]:
    """Read stored hourly rows from ``start`` until ``end``, left out, oldest first."""
    instance = get_instance(hass)
    result = await instance.async_add_executor_job(
        partial(
            statistics_during_period,
            hass,
            start,
            end,
            statistic_ids,
            "hour",
            None,
            {"state", "sum"},
        )
    )
    return {
        statistic_id: [
            _Row(
                start=datetime.fromtimestamp(_timestamp(row["start"]), tz=UTC),
                state=row.get("state"),
                sum=row.get("sum"),
            )
            for row in result.get(statistic_id, [])
        ]
        for statistic_id in statistic_ids
    }


def _statistic_kind(statistic_id: str) -> str:
    """Return a statistic's kind, as ``consumption_cost``, without its key."""
    return statistic_id.partition(":")[2].rpartition("_")[0]


def _decimal(value: float | None) -> Decimal:
    """Return a stored float as a Decimal, a missing one as zero."""
    return Decimal(str(value or 0))


def _steps_to(earlier: _Row, later: _Row) -> bool:
    """Return whether a row's running sum is the earlier row's plus its state."""
    return (
        earlier.sum is not None
        and later.sum is not None
        and _close(later.sum - earlier.sum, later.state or 0.0)
    )


def _close(value: float | None, expected: float | None) -> bool:
    return (
        value is not None
        and expected is not None
        and abs(value - expected) <= _SUM_TOLERANCE
    )


def energy_metadata(stat_id: str, name: str) -> StatisticMetaData:
    """Return the metadata of an energy statistic, in kWh."""
    return {
        "has_sum": True,
        "mean_type": StatisticMeanType.NONE,
        "name": name,
        "source": DOMAIN,
        "statistic_id": stat_id,
        "unit_class": EnergyConverter.UNIT_CLASS,
        "unit_of_measurement": UnitOfEnergy.KILO_WATT_HOUR,
    }


def cost_metadata(stat_id: str, name: str) -> StatisticMetaData:
    """Return the metadata of a money statistic, in NZD."""
    return {
        "has_sum": True,
        "mean_type": StatisticMeanType.NONE,
        "name": name,
        "source": DOMAIN,
        "statistic_id": stat_id,
        "unit_class": None,
        "unit_of_measurement": "NZD",
    }


def _timestamp(value: float | datetime) -> float:
    return value.timestamp() if isinstance(value, datetime) else float(value)


def consumption_ids(key: str) -> tuple[str, str]:
    """Return consumption and consumption-cost statistic IDs."""
    return (
        statistic_id(STAT_CONSUMPTION, key),
        statistic_id(STAT_CONSUMPTION_COST, key),
    )


def generation_ids(key: str) -> tuple[str, str]:
    """Return generation and generation-credit statistic IDs."""
    return (
        statistic_id(STAT_GENERATION, key),
        statistic_id(STAT_GENERATION_CREDIT, key),
    )
