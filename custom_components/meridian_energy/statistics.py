"""
Home Assistant external-statistics import for Meridian Energy.

Every import writes through one primitive, ``async_replay``: the setup
flow's and every entry's. It rewrites one span of hours per statistic, from
the first hour it is given to the last, with running sums that follow on
from the newest stored row before the span, found by time; it fills the
hours it was not given with their stored states; and it shifts every stored
row after the span by one constant, so the first of them follows on from
the span, and the others keep their steps: an "Adjust sum" a user made
after that first row is kept, and one made at it is undone. Its writes are
confirmed before it returns, and the next replay of statistics whose writes
were not confirmed waits until the recorder has saved them before it reads.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import AsyncIterator, Iterable, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
from hashlib import sha256
from itertools import pairwise
from time import monotonic
from typing import cast

from homeassistant.components.recorder import get_instance  # type: ignore[attr-defined]
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    StatisticsRow,
    async_add_external_statistics,
    get_last_statistics,
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
from .models import BillingPeriodTotals, MeridianMeasurement

_LOGGER = logging.getLogger(__name__)
_CENTS_PER_DOLLAR = Decimal(100)
_COST_UNIT = "NZD"
CLEAR_STATISTICS_TIMEOUT_SECONDS = 30
_SECONDS_PER_HOUR = 3600
_HOUR = timedelta(hours=1)
# Where a replay looks for the stored row its running sums follow on from:
# the 14 days before its first hour, then back to 800 days before that hour.
_NEAR_LOOKBACK = timedelta(days=14)
_FAR_LOOKBACK = timedelta(days=800)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# A replay's writes are confirmed behind one barrier, and up to two more
# when what it reads back does not match yet, each waiting up to 30 s.
CONFIRM_BARRIERS = 3
CONFIRM_TIMEOUT_SECONDS = 30
# A replay of statistics whose last replay was not confirmed first waits
# behind two barriers, so it reads what that replay left queued once saved.
SETTLE_BARRIERS = 2
# The longest a replay is expected to take behind a busy recorder: its
# barriers, those it may wait behind first, and 30 s more for its reads. A
# setup attempt's time limit leaves at least this to a replay that has
# started (the coordinator's ``_async_uncounted``).
REPLAY_ALLOWANCE_SECONDS = (
    SETTLE_BARRIERS + CONFIRM_BARRIERS
) * CONFIRM_TIMEOUT_SECONDS + 30
# How far a stored running sum may be from the one expected, and the
# smallest shift worth writing: rounding of sums stored as floats.
_SUM_TOLERANCE = 1e-6
_SHIFT_EPSILON = 1e-9
# In hass.data[DOMAIN]: one lock per statistic, shared by the setup flow and
# every entry, held by one replay at a time; the statistics a replay refused
# to anchor, warned about once; and those whose last replay queued writes it
# did not confirm, which the next replay of them waits for before it reads.
WRITE_LOCKS = "write_locks"
_REFUSED = "replay_refused"
_UNSETTLED = "replay_unsettled"


class StatisticsWriteError(Exception):
    """A replay's writes were not confirmed, or it refused to write them."""


class StatisticsAnchorError(StatisticsWriteError):
    """
    A replay refused to write, with no stored running total to continue.

    Older rows exist but none with a running total in the 800 days before
    its span, or rows before the span have no running total at all. Nothing
    was written, and a retry refuses again until those statistics change.
    """


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


async def async_has_statistics(hass: HomeAssistant, stat_id: str) -> bool:
    """Return whether a statistic already has at least one row."""
    instance = get_instance(hass)
    result = await instance.async_add_executor_job(
        get_last_statistics,
        hass,
        1,
        stat_id,
        False,
        {"sum"},
    )
    return bool(result.get(stat_id))


async def async_has_numeric_statistics(hass: HomeAssistant, stat_id: str) -> bool:
    """Return whether a statistic contains at least one numeric cumulative row."""
    return await async_latest_numeric_statistic_start(hass, stat_id) is not None


async def async_latest_numeric_statistic_start(
    hass: HomeAssistant, stat_id: str
) -> datetime | None:
    """
    Return the newest trustworthy cumulative row timestamp, if one exists.

    The newest row, when it has a running sum; otherwise the newest row with
    one, looked up by time as a replay's predecessor is, in the 14 days up
    to the newest row and then back to 800 days before it, whatever the
    number of rows without a sum after it.
    """
    instance = get_instance(hass)
    result = await instance.async_add_executor_job(
        get_last_statistics, hass, 1, stat_id, False, {"sum"}
    )
    rows = result.get(stat_id, [])
    if not rows:
        return None
    newest = datetime.fromtimestamp(_timestamp(rows[0]["start"]), tz=UTC)
    if rows[0].get("sum") is not None:
        return newest
    end = newest + _HOUR
    for lookback in (_NEAR_LOOKBACK, _FAR_LOOKBACK):
        start = newest + _HOUR - lookback
        stored = await _async_read_rows(hass, {stat_id}, start, end)
        numeric = [row for row in stored[stat_id] if row.sum is not None]
        if numeric:
            return numeric[-1].start
        end = start
    return None


async def async_clear_statistics(hass: HomeAssistant, statistic_ids: set[str]) -> bool:
    """
    Remove statistics created by an incomplete first-install import.

    Deletion must run on the recorder thread, so queue it through the public
    Recorder API and wait for its completion callback, as Home Assistant's own
    ``recorder/clear_statistics`` command does. A timeout only stops the wait:
    the queued deletion still runs, and the caller's original error is kept.

    Return True once the recorder has finished the removal (or there was
    nothing to remove), and False when the wait timed out with it still queued.
    Nothing is logged: the caller reports a removal still queued, in the one
    warning or form for that setup.
    """
    if not statistic_ids:
        return True
    return await _async_run_removal(hass, sorted(statistic_ids))


async def async_wait_for_recorder(
    hass: HomeAssistant, *, attempts: int = 1, passes: int = 1
) -> bool:
    """
    Wait until the recorder has finished the work queued before this call.

    That includes a task it is running: a removal that an earlier setup
    queued has then run, so statistics read next are settled. The wait is a
    removal of no statistics, queued behind that work; the recorder runs its
    tasks one at a time and in order. The recorder's commit future would not
    do: it is None whenever the queue is empty, which it also is while the
    recorder runs the last task it took off it.

    On MySQL or MariaDB, an import that meets a lock error (a lock-wait
    timeout or a deadlock, say) queues itself again, at the back of the
    queue: behind a wait queued before it ran, which then returns with its
    rows still unsaved. With ``passes=2``, a second wait is queued once the
    first has run, and so behind such a retry. An import retried twice in a
    row can still be saved after it.

    Each attempt waits 30 s. A wait that timed out is tried again behind a
    new removal: one whose recorder task failed never calls back, and the
    next can. ``attempts`` counts every wait, of either pass. Return False
    when they ran out with that work still queued, or as soon as fewer are
    left than the passes still to run, since no wait then could succeed:
    with ``attempts`` equal to ``passes``, the first timeout ends the wait.
    The recorder still does that work later.
    """
    remaining = passes
    for attempt in range(attempts):
        if remaining > attempts - attempt:
            return False
        if await _async_run_removal(hass, []):
            remaining -= 1
            if not remaining:
                return True
    return False


async def async_confirm_visible(
    hass: HomeAssistant, newest: Mapping[str, datetime]
) -> bool:
    """
    Wait until the recorder has saved an import, then read its newest rows back.

    ``newest`` maps each energy statistic written to the newest hour written.
    Each replay of the import has confirmed its own writes before it
    returned, so this is a cheap last check, behind any work still queued,
    before the next reader (the entry's first sync) reads statistics on
    another thread: wait as ``async_wait_for_recorder`` does, then check
    that each statistic's newest row is readable. When the wait timed
    out or a row is missing, wait once more: the recorder may catch up a
    moment later, and an import it had to retry has queued itself again
    behind the first wait. This never raises: a second wait that timed out,
    a mismatch or a failed read is logged at INFO and the caller continues,
    because the recorder still saves the rows later, and the entry's first
    sync waits for the recorder again before it reads any statistics. A
    cancellation passes through, for the caller to handle: the setup dialog
    closing, or the setup time limit running out. Return whether every row
    was readable.

    Each barrier waits up to 30 s, so with the second one the whole wait can
    reach a minute; a timeout's message gives the time it waited in all.
    """
    if not newest:
        return True
    started = monotonic()
    attempts = 2
    try:
        for attempt in range(1, attempts + 1):
            if not await async_wait_for_recorder(hass):
                if attempt < attempts:
                    continue
                _LOGGER.info(
                    "Home Assistant had not finished saving the imported Meridian "
                    "history after %.0f s; setup continues, and the first update "
                    "waits until it is saved",
                    monotonic() - started,
                )
                return False
            if await _async_newest_rows_visible(hass, newest):
                return True
    except Exception as err:  # noqa: BLE001 - the check must never fail an import
        _LOGGER.info(
            "Could not read back the imported Meridian history (%s); setup "
            "continues, and the first update may import it again",
            type(err).__name__,
        )
        return False
    _LOGGER.info(
        "The imported Meridian history could not be read back after saving it; "
        "setup continues, and the first update may import it again"
    )
    return False


async def _async_newest_rows_visible(
    hass: HomeAssistant, newest: Mapping[str, datetime]
) -> bool:
    """Return whether each statistic's newest stored row is the one expected."""
    instance = get_instance(hass)
    for stat_id, start in newest.items():
        result = await instance.async_add_executor_job(
            get_last_statistics, hass, 1, stat_id, False, {"sum"}
        )
        rows = result.get(stat_id, [])
        if not rows or _timestamp(rows[0]["start"]) < start.timestamp():
            return False
    return True


async def _async_run_removal(
    hass: HomeAssistant, statistic_ids: list[str], *, seconds: float | None = None
) -> bool:
    """
    Queue a removal on the recorder thread; return whether it ran in time.

    It waits ``seconds``, by default ``CLEAR_STATISTICS_TIMEOUT_SECONDS``.
    """
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
    Return the hours each statistic has a stored running sum for, in a span.

    One read on the recorder's executor, for every statistic at once. Each
    statistic maps to the UTC starts of its hourly rows from ``start`` until
    ``end``, which is left out, whose sum is a number; a statistic with none
    maps to an empty set.
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


async def async_stored_states(
    hass: HomeAssistant, statistic_ids: set[str], start: datetime, end: datetime
) -> dict[str, dict[datetime, float]]:
    """
    Return each statistic's stored hourly states in a span, by hour.

    One read on the recorder's executor, for every statistic at once, of the
    hours from ``start`` until ``end``, which is left out, that have a state
    and a running sum: an hour stored without a sum is not one a replay
    continues, so it counts as not stored.
    """
    stored = await _async_read_rows(hass, statistic_ids, start, end)
    return {
        statistic_id: {
            row.start: row.state
            for row in rows
            if row.state is not None and row.sum is not None
        }
        for statistic_id, rows in stored.items()
    }


def changed_hours(
    measurements: Iterable[MeridianMeasurement],
    energy: Mapping[datetime, float],
    cost: Mapping[datetime, float],
) -> frozenset[datetime]:
    """
    Return the hours whose readings are not stored as fetched.

    An hour's usage is the sum of its rows, as a replay writes it, and so is
    its cost, which counts only when every row of the hour has one: a cost
    Meridian did not return is no change, since the replay keeps the stored
    one. An hour not stored at all has changed.
    """
    return frozenset(
        start
        for start, (value, amount) in _aggregate_measurements(measurements).items()
        if not _close(float(value), energy.get(start))
        or (
            amount is not None
            and not _close(float(amount / _CENTS_PER_DOLLAR), cost.get(start))
        )
    )


@dataclass(frozen=True, slots=True)
class _Row:
    """One stored hourly row: its start, its state and its running sum."""

    start: datetime
    state: float | None
    sum: float | None


@dataclass(slots=True)
class _Replay:
    """
    One statistic's part of a replay.

    ``states`` holds the hours it was given, at least one, from ``first``
    to ``last``; ``rows`` the rows it writes for that span, built once the
    stored rows are read. ``predecessor`` is the stored row its sums follow
    on from, if any, and ``following`` the first stored row after the span,
    which, with every row after it, is shifted by ``shift``.
    """

    statistic_id: str
    metadata: StatisticMetaData
    unit: str
    states: dict[datetime, Decimal]
    first: datetime = field(init=False)
    last: datetime = field(init=False)
    rows: list[StatisticData] = field(default_factory=list)
    predecessor: _Row | None = None
    following: _Row | None = None
    shift: float = 0.0

    def __post_init__(self) -> None:
        """Note the span's first and last hours once."""
        self.first = min(self.states)
        self.last = max(self.states)


async def async_replay(
    hass: HomeAssistant,
    *,
    stat_energy_id: str,
    stat_cost_id: str,
    energy_name: str,
    cost_name: str,
    measurements: Iterable[MeridianMeasurement],
    import_cost: bool = True,
    cost_after: datetime | None = None,
) -> tuple[int, int]:
    """
    Replay hourly readings into a direction's energy and cost statistics.

    This is the one write path of every import, the setup flow's and every
    entry's. For each statistic, the hours it is given span from the first
    to the last of them; an hour whose cost is None, or that ``cost_after``
    or ``import_cost`` False leaves out, is not given for the cost. The
    replay then, holding that statistic's lock (``WRITE_LOCKS``):

    1. reads the stored rows from 14 days before the span on, and takes the
       newest one before it with a running sum as its predecessor; failing
       that, the newest back to 800 days before the span; failing that,
       none, so the sums start from zero, unless rows are stored before the
       span all the same (older ones, or rows without a sum, which are
       passed over), when it refuses with ``StatisticsAnchorError`` and one
       WARNING, rather than put the new rows below them, and writes
       nothing. Month periods are never used: they return a newer row's sum
       when no predecessor exists;
    2. builds the rows of every hour in the span, given or stored, the
       stored ones with their stored state, summed from the predecessor;
    3. queues the energy rows, the cost rows and, for each statistic with
       stored rows after the span, one adjustment of every one of them by
       the same amount, so the first follows on from the last row written:
       its step becomes its state, which undoes an "Adjust sum" a user made
       at that hour, and heals a seam there, while one made at any later
       hour is kept;
    4. confirms the writes behind a barrier, reading back the rows on both
       sides of each join it wrote: the predecessor and the first row, the
       last row and the first row shifted. When they do not match yet, as
       for a write the recorder had to queue again, it tries again behind
       another barrier, twice at most, then raises ``StatisticsWriteError``.

    Steps 2 and 3 have no await between them, so a cancellation comes
    either before them, when nothing is written, or after them, when every
    write and shift is queued: never the energy without its cost. Each
    lock is released when the replay ends, however it ends. A replay that
    was not confirmed may leave its writes queued, so the next replay of
    those statistics first waits until the recorder has saved them
    (``_async_settle``), and never reads the running sums before. A caller
    that meets an error keeps its rows as not yet written, so its next run
    replays the same span, which heals what the recorder saved in part.

    Return the rows written to the energy and to the cost statistic.
    """
    aggregated = _aggregate_measurements(measurements)
    if not aggregated:
        return (0, 0)
    replays = [
        _Replay(
            stat_energy_id,
            _energy_metadata(stat_energy_id, energy_name),
            UnitOfEnergy.KILO_WATT_HOUR,
            {start: energy for start, (energy, _cost) in aggregated.items()},
        )
    ]
    costs = {
        start: cost / _CENTS_PER_DOLLAR
        for start, (_energy, cost) in aggregated.items()
        if import_cost
        and cost is not None
        and (cost_after is None or start > cost_after)
    }
    if costs:
        replays.append(
            _Replay(
                stat_cost_id, _cost_metadata(stat_cost_id, cost_name), _COST_UNIT, costs
            )
        )
    statistic_ids = [replay.statistic_id for replay in replays]
    unsettled: set[str] = hass.data.setdefault(DOMAIN, {}).setdefault(_UNSETTLED, set())
    async with _async_write_locks(hass, statistic_ids):
        await _async_settle(hass, unsettled, statistic_ids)
        stored = await _async_read_for_replay(hass, replays)
        # From here until every write is queued, nothing is awaited.
        for replay in replays:
            _build_rows(replay, stored[replay.statistic_id])
        unsettled.update(statistic_ids)
        _queue_writes(hass, replays)
        await _async_confirm_replay(hass, replays)
        unsettled.difference_update(statistic_ids)
    written = {replay.statistic_id: len(replay.rows) for replay in replays}
    return (written.get(stat_energy_id, 0), written.get(stat_cost_id, 0))


async def _async_settle(
    hass: HomeAssistant, unsettled: set[str], statistic_ids: list[str]
) -> None:
    """
    Wait for the writes an unconfirmed replay of these statistics left queued.

    A replay that was not confirmed (its barriers timed out, its read-back
    did not match, or it was cancelled while confirming) released its locks
    with its writes possibly still queued. Read now, the running sums would
    be those from before them, and the rows built on them would leave a
    seam once both were saved. So the next replay of those statistics waits
    behind two barriers first, as ``async_wait_for_recorder`` does with two
    passes, the second behind an import the recorder queued again. When one
    times out, the recorder is still busy: it raises ``StatisticsWriteError``
    before reading, so nothing is written, and the statistics stay marked.
    """
    if unsettled.isdisjoint(statistic_ids):
        return
    for _barrier in range(SETTLE_BARRIERS):
        if not await _async_run_removal(hass, [], seconds=CONFIRM_TIMEOUT_SECONDS):
            raise StatisticsWriteError(
                "Home Assistant has not saved the statistics written before"
            )
    unsettled.difference_update(statistic_ids)


@asynccontextmanager
async def _async_write_locks(
    hass: HomeAssistant, statistic_ids: list[str]
) -> AsyncIterator[None]:
    """Hold the write locks of some statistics, taken in one order by every writer."""
    locks: dict[str, asyncio.Lock] = hass.data.setdefault(DOMAIN, {}).setdefault(
        WRITE_LOCKS, {}
    )
    async with AsyncExitStack() as stack:
        for statistic_id in sorted(statistic_ids):
            lock = locks.setdefault(statistic_id, asyncio.Lock())
            await stack.enter_async_context(lock)
        yield


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


async def _async_read_for_replay(
    hass: HomeAssistant, replays: list[_Replay]
) -> dict[str, list[_Row]]:
    """
    Read what a replay needs, and find each statistic's predecessor.

    One read of every statistic from 14 days before the earliest span on,
    which holds the spans, the rows after them and, most often, the
    predecessors; then, only for a statistic with no stored row with a sum
    in it before its span, the rows back to 800 days before the earliest
    such span. Rows without a sum, which another writer can store, are
    passed over in both, as ``async_latest_numeric_statistic_start``
    passes them over. With no row with a sum found, a statistic with rows
    before its span in either read, or older ones (a third read, of
    whether it has any), refuses the replay rather than start the sums
    from zero above them. Return each statistic's rows from the first read.
    """
    ids = {replay.statistic_id for replay in replays}
    read_from = min(replay.first for replay in replays) - _NEAR_LOOKBACK
    stored = await _async_read_rows(hass, ids, read_from, None)
    searching = [
        replay
        for replay in replays
        if not _take_predecessor(replay, stored[replay.statistic_id])
    ]
    if not searching:
        return stored
    far_from = min(replay.first for replay in searching) - _FAR_LOOKBACK
    far = await _async_read_rows(
        hass, {replay.statistic_id for replay in searching}, far_from, read_from
    )
    searching = [
        replay
        for replay in searching
        if not _take_predecessor(replay, far[replay.statistic_id])
    ]
    for replay in searching:
        if _has_rows_before(replay, stored) or _has_rows_before(replay, far):
            _refuse(hass, replay)
    if searching:
        older = await _async_read_rows(
            hass, {replay.statistic_id for replay in searching}, _EPOCH, far_from
        )
        for replay in searching:
            if older[replay.statistic_id]:
                _refuse(hass, replay)
    return stored


def _take_predecessor(replay: _Replay, rows: list[_Row]) -> bool:
    """
    Take the newest row with a running sum before a span, if these rows have one.

    Return whether one was found. Rows without a sum are passed over.
    """
    numeric = [row for row in rows if row.start < replay.first and row.sum is not None]
    if numeric:
        replay.predecessor = numeric[-1]
        return True
    return False


def _has_rows_before(replay: _Replay, read: dict[str, list[_Row]]) -> bool:
    """Return whether a read holds any row before a replay's span, with a sum or not."""
    return any(row.start < replay.first for row in read[replay.statistic_id])


def _refuse(hass: HomeAssistant, replay: _Replay) -> None:
    """Refuse a replay with no running sum to follow on from; warn once a statistic."""
    refused: set[str] = hass.data.setdefault(DOMAIN, {}).setdefault(_REFUSED, set())
    level = logging.DEBUG if replay.statistic_id in refused else logging.WARNING
    refused.add(replay.statistic_id)
    _LOGGER.log(
        level,
        "Meridian Energy found statistics older than the hours it would import "
        "but no running total to continue in the 800 days before them, so it "
        "imports none rather than start the running total again from zero "
        "(statistic=%s)",
        _statistic_kind(replay.statistic_id),
    )
    raise StatisticsAnchorError("No stored running total to continue")


def _statistic_kind(statistic_id: str) -> str:
    """Return a statistic's kind, as ``consumption_cost``, without its key."""
    return statistic_id.partition(":")[2].rpartition("_")[0]


def _build_rows(replay: _Replay, stored: list[_Row]) -> None:
    """Build a statistic's rows for its span, and the shift of the rows after it."""
    first, last = replay.first, replay.last
    states = dict(replay.states)
    for row in stored:
        if first <= row.start <= last and row.start not in states:
            states[row.start] = _decimal(row.state)
    total = (
        Decimal(0) if replay.predecessor is None else _decimal(replay.predecessor.sum)
    )
    for start in sorted(states):
        total += states[start]
        replay.rows.append(
            {"start": start, "state": float(states[start]), "sum": float(total)}
        )
    replay.following = next(
        (row for row in stored if row.start > last and row.sum is not None), None
    )
    if replay.following is not None:
        replay.shift = float(
            total + _decimal(replay.following.state) - _decimal(replay.following.sum)
        )


def _decimal(value: float | None) -> Decimal:
    """Return a stored float as a Decimal, a missing one as zero."""
    return Decimal(str(value or 0))


def _queue_writes(hass: HomeAssistant, replays: list[_Replay]) -> None:
    """Queue a replay's rows, then its shifts, all on the recorder thread."""
    instance = get_instance(hass)
    for replay in replays:
        async_add_external_statistics(hass, replay.metadata, replay.rows)
    for replay in replays:
        if replay.following is not None and abs(replay.shift) > _SHIFT_EPSILON:
            instance.async_adjust_statistics(
                replay.statistic_id, replay.last + _HOUR, replay.shift, replay.unit
            )


async def _async_confirm_replay(hass: HomeAssistant, replays: list[_Replay]) -> None:
    """Wait for a replay's writes, then check its joins; raise when they never hold."""
    for _attempt in range(CONFIRM_BARRIERS):
        if await _async_run_removal(
            hass, [], seconds=CONFIRM_TIMEOUT_SECONDS
        ) and await _async_joins_hold(hass, replays):
            return
    raise StatisticsWriteError("Home Assistant did not confirm the statistics written")


async def _async_joins_hold(hass: HomeAssistant, replays: list[_Replay]) -> bool:
    """
    Return whether every join a replay wrote reads back as written.

    Only the joins: the predecessor and the first row written, and the last
    row written and the first row shifted, each a step equal to the later
    row's state. Two reads cover them for every statistic.
    """
    ids = {replay.statistic_id for replay in replays}
    head = await _async_read_rows(
        hass,
        ids,
        min(
            replay.first if replay.predecessor is None else replay.predecessor.start
            for replay in replays
        ),
        max(replay.first for replay in replays) + _HOUR,
    )
    tail = await _async_read_rows(
        hass,
        ids,
        min(replay.last for replay in replays),
        max(
            replay.last if replay.following is None else replay.following.start
            for replay in replays
        )
        + _HOUR,
    )
    return all(
        _head_holds(replay, head[replay.statistic_id])
        and _tail_holds(replay, tail[replay.statistic_id])
        for replay in replays
    )


def _head_holds(replay: _Replay, rows: list[_Row]) -> bool:
    """Return whether the first row written follows on from its predecessor."""
    first = _row_at(rows, replay.first)
    if first is None or not _close(first.sum, replay.rows[0]["sum"]):
        return False
    if replay.predecessor is None:
        return True
    before = [row for row in rows if row.start < replay.first and row.sum is not None]
    return bool(before) and _steps_to(before[-1], first)


def _tail_holds(replay: _Replay, rows: list[_Row]) -> bool:
    """Return whether the last row written is stored, and the rows after follow on."""
    last = _row_at(rows, replay.last)
    written = replay.rows[-1]
    if (
        last is None
        or not _close(last.sum, written["sum"])
        or not _close(last.state, written["state"])
    ):
        return False
    if replay.following is None:
        return True
    following = _row_at(rows, replay.following.start)
    return following is not None and _steps_to(last, following)


def _row_at(rows: list[_Row], start: datetime) -> _Row | None:
    return next((row for row in rows if row.start == start), None)


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


def _aggregate_measurements(
    measurements: Iterable[MeridianMeasurement],
) -> dict[datetime, tuple[Decimal, Decimal | None]]:
    """Aggregate multiple registers into one row per UTC hour."""
    energy: defaultdict[datetime, Decimal] = defaultdict(lambda: Decimal(0))
    cost: defaultdict[datetime, Decimal] = defaultdict(lambda: Decimal(0))
    cost_complete: defaultdict[datetime, bool] = defaultdict(lambda: True)
    for measurement in measurements:
        start = measurement.start.astimezone(UTC)
        if start.minute or start.second or start.microsecond:
            raise ValueError("Meridian measurement is not aligned to an hour")
        if not measurement.value_kwh.is_finite():
            raise ValueError("Meridian measurement value is not finite")
        energy[start] += measurement.value_kwh
        if measurement.cost_cents is None:
            cost_complete[start] = False
        else:
            if not measurement.cost_cents.is_finite():
                raise ValueError("Meridian measurement cost is not finite")
            cost[start] += measurement.cost_cents
    return {
        start: (energy[start], cost[start] if cost_complete[start] else None)
        for start in energy
    }


async def async_account_period_totals(
    hass: HomeAssistant,
    *,
    property_keys: tuple[str, ...],
    start: datetime,
    end: datetime,
    include_generation: bool,
) -> BillingPeriodTotals:
    """
    Return complete account totals for an exact UTC billing-period range.

    A barrier first waits up to 30 s for statistics still queued, then the
    rows are read whatever came of it: every replay has confirmed its own
    writes, and a row still missing shows as a gap, which leaves the total
    unknown. Each total is summed from the changes of its rows, so each
    row's running total must step from the one before by that row's own
    state: a stale running sum, as a seam an earlier release left, would
    make the total wrong. Usage and export are complete when their rows
    cover every hour from the start of the period and step so; a cost or
    credit when it has a row for each of those hours, which steps so too.
    That leaves a total unknown for the period too where a user adjusted
    its running total there, and the money total with the usage or export.
    """
    instance = get_instance(hass)
    if not await async_wait_for_recorder(hass):
        _LOGGER.debug(
            "Home Assistant had not finished saving statistics after 30 s; the "
            "bill totals read what it has saved"
        )
    statistic_ids: set[str] = set()
    pairs: list[tuple[str, str, bool]] = []
    for key in property_keys:
        consumption_energy, consumption_cost = consumption_ids(key)
        pairs.append((consumption_energy, consumption_cost, False))
        statistic_ids.update((consumption_energy, consumption_cost))
        if include_generation:
            generation_energy, generation_credit = generation_ids(key)
            pairs.append((generation_energy, generation_credit, True))
            statistic_ids.update((generation_energy, generation_credit))

    result = await instance.async_add_executor_job(
        partial(
            statistics_during_period,
            hass,
            start,
            end,
            statistic_ids,
            "hour",
            None,
            {"change", "state"},
        )
    )

    usage = Decimal(0)
    cost = Decimal(0)
    exported = Decimal(0)
    credit = Decimal(0)
    consumption_complete = True
    cost_complete = True
    generation_complete = True
    credit_complete = True
    saw_consumption = False
    saw_generation = False

    pair_rows = [
        (
            _change_rows(result.get(energy_id, []), start, end),
            _change_rows(result.get(money_id, []), start, end),
            generation,
            _steps_follow_states(result.get(energy_id, []), start, end),
            _steps_follow_states(result.get(money_id, []), start, end),
        )
        for energy_id, money_id, generation in pairs
    ]
    consumption_horizon = max(
        (
            row[0]
            for energy_rows, _money_rows, generation, *_consistent in pair_rows
            if not generation
            for row in energy_rows
        ),
        default=None,
    )
    generation_horizon = max(
        (
            row[0]
            for energy_rows, _money_rows, generation, *_consistent in pair_rows
            if generation
            for row in energy_rows
        ),
        default=None,
    )

    for (
        energy_rows,
        money_rows,
        generation,
        energy_consistent,
        money_consistent,
    ) in pair_rows:
        energy_starts = {row[0] for row in energy_rows}
        money_starts = {row[0] for row in money_rows}
        energy_complete = energy_consistent and _hourly_rows_are_contiguous(
            energy_starts,
            start,
            generation_horizon if generation else consumption_horizon,
        )
        if generation:
            if not energy_rows:
                continue
            saw_generation = True
            generation_complete &= energy_complete
            credit_complete &= energy_starts == money_starts and money_consistent
            exported += sum((row[1] for row in energy_rows), Decimal(0))
            credit += sum((row[1] for row in money_rows), Decimal(0))
        else:
            saw_consumption |= bool(energy_rows)
            consumption_complete &= energy_complete
            cost_complete &= energy_starts == money_starts and money_consistent
            usage += sum((row[1] for row in energy_rows), Decimal(0))
            cost += sum((row[1] for row in money_rows), Decimal(0))

    usage_complete = saw_consumption and consumption_complete
    cost_data_complete = usage_complete and cost_complete
    export_complete = saw_generation and generation_complete
    credit_data_complete = export_complete and credit_complete
    return BillingPeriodTotals(
        usage=usage if usage_complete else None,
        cost=cost if cost_data_complete else None,
        export=exported if export_complete else None,
        credit=credit if credit_data_complete else None,
        usage_complete=usage_complete,
        cost_complete=cost_data_complete,
        export_complete=export_complete,
        credit_complete=credit_data_complete,
    )


def _hourly_rows_are_contiguous(
    starts: set[float], period_start: datetime, expected_horizon: float | None
) -> bool:
    """Return whether published rows cover every UTC hour from period start."""
    if (
        not starts
        or expected_horizon is None
        or min(starts) != period_start.timestamp()
        or max(starts) != expected_horizon
    ):
        return False
    ordered = sorted(starts)
    return all(
        later - earlier == _SECONDS_PER_HOUR for earlier, later in pairwise(ordered)
    )


def _steps_follow_states(
    rows: list[StatisticsRow], start: datetime, end: datetime
) -> bool:
    """
    Return whether each row in a range steps from the one before by its state.

    The first row's change is from the newest stored row before the range,
    or from zero when there is none, as for a statistic's first row.
    """
    for row in rows:
        timestamp = _timestamp(cast("float | datetime", row["start"]))
        if not start.timestamp() <= timestamp < end.timestamp():
            continue
        if not _close(row.get("change"), row.get("state")):
            return False
    return True


def _change_rows(
    rows: list[StatisticsRow], start: datetime, end: datetime
) -> list[tuple[float, Decimal]]:
    """Return timestamp/change pairs constrained to the requested range."""
    result: list[tuple[float, Decimal]] = []
    for row in rows:
        timestamp = _timestamp(cast("float | datetime", row["start"]))
        change = row.get("change")
        if start.timestamp() <= timestamp < end.timestamp() and change is not None:
            result.append((timestamp, Decimal(str(change))))
    return result


def _energy_metadata(stat_id: str, name: str) -> StatisticMetaData:
    return {
        "has_sum": True,
        "mean_type": StatisticMeanType.NONE,
        "name": name,
        "source": DOMAIN,
        "statistic_id": stat_id,
        "unit_class": EnergyConverter.UNIT_CLASS,
        "unit_of_measurement": UnitOfEnergy.KILO_WATT_HOUR,
    }


def _cost_metadata(stat_id: str, name: str) -> StatisticMetaData:
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
