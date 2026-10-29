"""Data coordinator for Meridian Energy statistics."""

from __future__ import annotations

import asyncio
import logging
import math
import random
from collections import Counter, deque
from collections.abc import AsyncIterator, Callable, Collection, Iterable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from functools import partial
from time import monotonic
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CoreState, HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianError,
    MeridianGraphQLError,
    MeridianRateLimitError,
    MeridianRejectedError,
    MeridianUpstreamError,
    http_status,
)
from .const import (
    BILLING_CACHE_INTERVAL,
    CONF_AUTO_ADD_ACCOUNTS,
    CONF_SELECTED_ACCOUNTS,
    COST_LAG_MARGIN,
    DEEP_WALK_BUDGET_SECONDS,
    DOMAIN,
    EMPTY_WINDOWS_AT_HISTORY_START,
    FIRST_SYNC_RECORDER_WAITS,
    FIRST_SYNC_RETRY_SCHEDULE,
    FIRST_SYNC_RETRY_SECONDS,
    FULL_RECONCILIATION_INTERVAL,
    GATEWAY_COOLDOWN_SECONDS,
    INITIAL_BACKFILL,
    MAX_RETRY_AFTER_SECONDS,
    MEASUREMENTS_PACING_SECONDS,
    NAME,
    POLL_BUDGET_SECONDS,
    READING_CONSUMPTION,
    READING_GENERATION,
    RECONCILIATION_SAFETY_MARGIN,
    REQUEST_START_RESERVE_SECONDS,
    REVISION_OVERLAP,
    TARGETED_RECONCILIATION_INTERVAL,
    TARGETED_RECONCILIATION_MINIMUM,
    TIP_WINDOW,
    TOPOLOGY_CACHE_INTERVAL,
    TRANSIENT_RESEND_DELAY_SECONDS,
    TRANSIENT_STATUSES,
    UPDATE_INTERVAL,
)
from .graphql import QUERY_LIMIT_CODES, UNKNOWN_ERROR_CODE
from .lanes import (
    COVERAGE,
    COVERAGE_PAUSE_SECONDS,
    COVERAGE_WINDOWS,
    LaneState,
    coverage_end,
    coverage_target,
    first_day,
    has_missing_hour,
    missing_days,
    missing_recent,
    oldest_day,
    plan_spans,
    reaches_target,
    revision_days,
)
from .models import (
    AccountSyncResult,
    MeasurementFetchResult,
    MeasurementPage,
    MeridianAccount,
    MeridianBillingPeriod,
    MeridianMeasurement,
    MeridianProperty,
    MeridianSyncData,
    PropertySyncResult,
    RequestContext,
    SyncMode,
    WindowWalk,
)
from .statistics import (
    REPLAY_ALLOWANCE_SECONDS,
    StatisticsAnchorError,
    StatisticsWriteError,
    account_key,
    async_account_period_totals,
    async_latest_numeric_statistic_start,
    async_replay,
    async_stored_hours,
    async_stored_states,
    async_wait_for_recorder,
    changed_hours,
    consumption_ids,
    generation_ids,
    property_key,
)
from .transport import RequestTally, count_requests
from .units import (
    STATUS_DEFERRED,
    STATUS_DEGRADED,
    STATUS_FAILED,
    STATUS_OK,
    SyncMemory,
    UnitFailure,
    UnitState,
    larger_mode,
)
from .windows import (
    CostPlan,
    ImportPlan,
    MeasurementWindow,
    StoredHours,
    WindowRows,
    ceil_hour,
    cost_plan,
    first_date,
    importable,
    local_midnight,
    page_limit,
    page_size,
    plan_windows,
    rows_per_hour,
)

_LOGGER = logging.getLogger(__name__)
_NZ = ZoneInfo("Pacific/Auckland")
_ACTUAL = "ACTUAL"
_TOPOLOGY_ERROR_CODES = frozenset(
    {"ACCOUNT_NOT_FOUND", "METER_NOT_FOUND", "PROPERTY_NOT_FOUND", "NOT_FOUND"}
)

MeasurementKey = tuple[datetime, str]
CacheKey = tuple[str, str]
_HOUR = timedelta(hours=1)
_DAY = timedelta(days=1)


class _Position(NamedTuple):
    """The 1-based place of an account, and optionally a property, in a sync."""

    account_index: int
    account_count: int
    property_index: int | None = None
    property_count: int | None = None


def _place(index: int | None, count: int | None) -> str:
    """Return a 1-based position as ``i/n`` for a log line, or ``-``."""
    return "-" if index is None else f"{index}/{count}"


def format_seconds(value: float | None) -> str:
    """Return seconds to 0.1 s for a log line, or ``-`` when unknown."""
    return "-" if value is None else f"{value:.1f}s"


def _floor_hour(value: datetime) -> datetime:
    """Return the start of the hour a time falls in."""
    return value.replace(minute=0, second=0, microsecond=0)


def _hour_offset(start: datetime, current_hour: datetime) -> str:
    """Return a row's start as whole hours from the current hour, as ``-30h``."""
    return f"{math.floor((start - current_hour) / _HOUR):+d}h"


def _upstream_key(err: MeridianUpstreamError) -> str:
    """Return the translation key for an upstream failure."""
    return "upstream_timeout" if err.gateway else "upstream_unavailable"


# What a failed measurements request leads to: one resend after a short
# pause; after a gateway timeout, the window's newest day alone once the
# cooldown has passed; after a query over Kraken's limits, the window's days
# one at a time; or the end of the direction's walk.
_RESEND = "retry"
_PROBE = "probe_1d"
_SPLIT = "split_1d"
_STOP = "stop"
# The failure keys a unit waiting after a failure reports in polling, with
# the time budget's own reason, and the detail its message then gives.
_DEFERRED_KEYS = {
    "cannot_connect": "cannot_connect",
    "invalid_data": "invalid_data",
    "upstream_timeout": "upstream_timeout",
    "upstream_unavailable": "upstream_unavailable",
    "statistics_write": "statistics_write",
    "statistics_refused": "statistics_refused",
    "time_budget": "upstream_timeout",
}
_BUDGET_DETAIL = "the update's time limit ran out"
# What a degraded unit's reconciliation logs when Meridian answered but its
# replay failed, by the failure's reason; any other reason is Meridian's.
_RECONCILIATION_UNSAVED = {
    "statistics_write": (
        "Home Assistant did not confirm the statistics a reconciliation of "
        "Meridian's usage history wrote"
    ),
    "statistics_refused": (
        "A reconciliation of Meridian's usage history was not imported, as "
        "older statistics have no running total it could continue"
    ),
}
_SHARE_ROUNDING = 1e-6
# How one window of a lane's coverage ended: its lane goes on, its lane
# stops, its property's lanes stop, or every lane stops, for this update.
_GO_ON = "go_on"
_STOP_LANE = "stop_lane"
_STOP_PROPERTY = "stop_property"
_STOP_ALL = "stop_all"


class RunBudgetError(Exception):
    """Too little of a sync's time budget was left to start another request."""


class _UnitsDeferredError(Exception):
    """Every unit of a sync is waiting after a failure, so nothing was fetched."""

    def __init__(self, failure: UnitFailure | None) -> None:
        super().__init__("Every Meridian direction is waiting after a failure")
        self.failure = failure


def _is_gateway(err: BaseException) -> bool:
    """Return whether a failure is a gateway timeout: HTTP 504, 524 or no answer."""
    return isinstance(err, MeridianUpstreamError) and err.gateway


def _is_query_limit(err: BaseException) -> bool:
    """Return whether Kraken refused a query over its complexity or node limit."""
    return isinstance(err, MeridianGraphQLError) and bool(
        QUERY_LIMIT_CODES.intersection(err.codes)
    )


def _failure_reason(err: BaseException) -> str:
    """Return the failure key a unit's failure reports, as the sync's would."""
    if isinstance(err, RunBudgetError):
        return "time_budget"
    if isinstance(err, StatisticsAnchorError):
        return "statistics_refused"
    if isinstance(err, StatisticsWriteError):
        return "statistics_write"
    if isinstance(err, MeridianUpstreamError):
        return _upstream_key(err)
    if isinstance(err, MeridianConnectionError):
        return "cannot_connect"
    return "invalid_data"


async def _async_wait(seconds: float) -> None:
    """Wait before a request: pacing, a cooldown or a resend's pause."""
    await asyncio.sleep(seconds)


class _Progress:
    """
    How many of a sync's planned windows are done, reported as a fraction.

    A window counts once it has been fetched, and so do the windows a walk
    no longer needs when it stops early, at the start of the history say.
    """

    def __init__(self, report: Callable[[float], None]) -> None:
        self._report = report
        self.planned = 0
        self.done = 0.0

    def add(self, windows: float) -> None:
        """
        Count windows as done, and report the fraction done.

        A window fetched day by day counts a share of one for each day.
        """
        if windows:
            self.done += windows
            self._report(min(1.0, self.done / max(1, self.planned)))


class _PageLog:
    """
    The debug lines for one direction's measurements pages.

    Times appear only as offsets: rows in whole hours from the start of the
    current hour, a window's dates in New Zealand days from today. Cursors,
    variables and identifiers stay out.
    """

    def __init__(
        self,
        *,
        mode: str | None,
        direction: str,
        position: _Position | None,
        now: datetime,
    ) -> None:
        self._today = now.astimezone(_NZ).date()
        self._current_hour = _floor_hour(now)
        self._fields = (
            f"mode={mode or '-'} direction={direction} "
            f"account={_place(*position[:2]) if position else '-'} "
            f"property={_place(*position[2:]) if position else '-'}"
        )

    def _request(
        self, window: MeasurementWindow, page_number: int, *, before: bool, last: int
    ) -> str:
        """Return the fields that say what one page asked for."""
        return (
            f"{self._fields} window={window.index}/{window.count} "
            f"page={page_number} bounds={(window.start_on - self._today).days}d.."
            f"{(window.end_on - self._today).days}d "
            f"before={'yes' if before else 'no'} last={last}"
        )

    def page(
        self,
        rows: WindowRows,
        page_number: int,
        *,
        before: bool,
        last: int,
        page: MeasurementPage,
        kept: int,
        before_window: int,
        elapsed: float,
    ) -> None:
        """
        Log one page that was received and checked.

        ``edge_hours`` says whether the window's first hour (00:00 on its
        first date) and last hour (23:00 on its last) have come back in any
        of its pages so far: a window read one hour off still has as many
        rows, so its edges are the check that shows it.
        """
        starts = {item.start.astimezone(UTC) for item in page.measurements}
        _LOGGER.debug(
            "Meridian measurements page: %s rows=%d kept=%d before_window=%d "
            "edge_hours=first:%s,last:%s oldest=%s newest=%s more=%s elapsed=%.1fs",
            self._request(rows.window, page_number, before=before, last=last),
            len(page.measurements),
            kept,
            before_window,
            "yes" if rows.first_hour else "no",
            "yes" if rows.last_hour else "no",
            _hour_offset(min(starts), self._current_hour) if starts else "-",
            _hour_offset(max(starts), self._current_hour) if starts else "-",
            "yes" if page.has_previous_page else "no",
            elapsed,
        )

    def failure(
        self,
        window: MeasurementWindow,
        page_number: int,
        *,
        before: bool,
        last: int,
        err: Exception,
        elapsed: float,
        action: str,
    ) -> None:
        """Log one page that failed: its class, status and what happens next."""
        status = http_status(err)
        _LOGGER.debug(
            "Meridian measurements page failed: %s kind=%s status=%s action=%s "
            "elapsed=%.1fs",
            self._request(window, page_number, before=before, last=last),
            type(err).__name__,
            "-" if status is None else status,
            action,
            elapsed,
        )


def _fetched_hours(
    incoming: tuple[MeridianMeasurement, ...], cutoff: datetime
) -> dict[datetime, dict[str, MeridianMeasurement]]:
    """Group fetched rows from the cache's cutoff on by hour, then channel."""
    hours: dict[datetime, dict[str, MeridianMeasurement]] = {}
    for measurement in incoming:
        start = measurement.start.astimezone(UTC)
        if start >= cutoff:
            hours.setdefault(start, {})[measurement.channel_id] = measurement
    return hours


def _merge_hours(
    cache: dict[MeasurementKey, MeridianMeasurement],
    hours: dict[datetime, dict[str, MeridianMeasurement]],
) -> datetime | None:
    """
    Replace each fetched hour's cached rows; return the earliest hour that changed.

    A cached ACTUAL row is kept over an estimate of it; a cached row of a
    channel the fetch no longer returned for the hour is dropped.
    """
    cached: dict[datetime, dict[str, MeridianMeasurement]] = {}
    for (start, channel), measurement in cache.items():
        if start in hours:
            cached.setdefault(start, {})[channel] = measurement
    earliest_changed: datetime | None = None
    for start, rows in hours.items():
        before = cached.get(start, {})
        after = {
            channel: (
                existing
                if (existing := before.get(channel)) is not None
                and existing.quality == _ACTUAL
                and measurement.quality != _ACTUAL
                else measurement
            )
            for channel, measurement in rows.items()
        }
        if after == before:
            continue
        for channel in before:
            del cache[(start, channel)]
        for channel, measurement in after.items():
            cache[(start, channel)] = measurement
        earliest_changed = (
            start if earliest_changed is None else min(earliest_changed, start)
        )
    return earliest_changed


def _utcnow() -> datetime:
    """Return the current UTC time through a patchable seam."""
    return datetime.now(UTC)


class _RecorderBehindError(Exception):
    """The recorder had not saved the work queued before a first sync."""


class MeridianDataCoordinator(DataUpdateCoordinator[MeridianSyncData]):
    """Fetch Meridian data and import it into the recorder."""

    # A rate limit the lanes' coverage met in the sync in progress, and the
    # time before which no coverage runs after one.
    _coverage_rate_limit: MeridianRateLimitError | None
    _coverage_not_before: datetime | None
    # What the coordinator's owner keeps across coordinators: the oldest hour
    # of each unit's replay that reached back more than 14 days, until a
    # replay from it is confirmed (``_note_unconfirmed``), and each lane's
    # dates whose coverage import was not confirmed (``unsaved``).
    _unconfirmed: dict[CacheKey, datetime]
    _unsaved_coverage: dict[CacheKey, dict[date, datetime]]
    # When the sync's last measurements request started, and ended.
    _last_measurements_start: float | None
    _last_measurements_end: float | None

    def __init__(
        self,
        hass: HomeAssistant,
        client: MeridianApiClient,
        *,
        config_entry: ConfigEntry | None = None,
        selected_accounts: frozenset[str] | None = None,
        auto_add_accounts: bool = False,
        topology: tuple[MeridianAccount, ...] | None = None,
        topology_at: datetime | None = None,
        reconciled_at: datetime | None = None,
        memory: SyncMemory | None = None,
        unconfirmed: dict[CacheKey, datetime] | None = None,
        unsaved: dict[CacheKey, dict[date, datetime]] | None = None,
    ) -> None:
        """
        Create a coordinator, optionally with what setup already knows.

        ``topology`` is the accounts the setup flow found, cached as of
        ``topology_at`` (now by default), so neither the setup import nor the
        entry's setup asks Meridian for them again. ``reconciled_at`` is when
        the setup import's sync started: the entry's first sync then counts
        it as the last reconciliation, and continues directions with
        statistics in the mode the cadence asks for, the hourly update at
        first, instead of a 14-day restart or a new 90-day import.
        ``memory`` is what the setup import's coordinator remembered of each
        direction (``SyncMemory``). ``unconfirmed`` is where the coordinator
        keeps the oldest hour of each unit's deep replay that was not
        confirmed (``_note_unconfirmed``), updated in place: its owner keeps
        it across coordinators, the setup flow across its attempts and an
        entry across its reloads, as a new coordinator cannot tell those
        hours from the recorder. ``unsaved`` is where it keeps, likewise, the
        dates of each lane whose coverage import was not confirmed, with
        when each is tried again (``lanes.LaneState.unsaved``): an entry
        keeps it across its reloads.
        """
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=NAME,
            update_interval=UPDATE_INTERVAL,
        )
        self.client = client
        self._config_entry = config_entry
        self._selected_accounts = selected_accounts
        self._auto_add_accounts = auto_add_accounts
        self._topology: tuple[MeridianAccount, ...] | None = topology
        self._topology_cached_at: datetime | None = (
            None if topology is None else topology_at or _utcnow()
        )
        # A topology handed over by setup is filtered and pruned when first
        # read, as a fetched one is.
        self._topology_seeded = topology is not None
        self._reconciled_at = reconciled_at
        self._billing_periods: dict[str, MeridianBillingPeriod] = {}
        self._billing_cached_at: dict[str, datetime] = {}
        self._billing_unavailable: set[str] = set()
        self._measurement_cache: dict[
            CacheKey, dict[MeasurementKey, MeridianMeasurement]
        ] = {}
        self._row_density: dict[CacheKey, float] = {}
        self._known_statistics: dict[str, bool] = {}
        self._latest_statistics: dict[str, datetime | None] = {}
        self._backfill_attempted: set[CacheKey] = set()
        self._pending_backfill: set[CacheKey] = set()
        self._unconfirmed, self._unsaved_coverage = (
            {} if unconfirmed is None else unconfirmed,
            {} if unsaved is None else unsaved,
        )
        self._active_directions: set[CacheKey] = set()
        self._seen_directions: set[CacheKey] = set()
        self._initial_refresh_complete = False
        self._last_targeted_reconciliation = self._last_full_reconciliation = (
            reconciled_at
        )
        # The newest stored hour of each energy and cost statistic: read from
        # the recorder once, then moved on by each import.
        self._frontier: dict[str, datetime] = {}
        # What is remembered about each (property, direction) between syncs,
        # and the units of the sync in progress or last run.
        self._unit_states: dict[CacheKey, UnitState] = {}
        self._run_units: list[_Unit] = []
        # What each unit's history covers, and the work that completes it,
        # beyond what the recorder holds (``lanes.LaneState``).
        self._lanes: dict[CacheKey, LaneState] = {}
        self._restore(memory)
        self._topology_fresh = False
        self._first_sync_failures = self._rate_limits = 0
        self._coverage_rate_limit = self._coverage_not_before = None
        self.last_request: RequestContext | None = None
        self._requests = RequestTally()
        self._imported_newest: dict[str, datetime] = {}
        # Causes of an INFO line already logged by this coordinator: each is
        # logged once at INFO, then at DEBUG.
        self._reported: set[str] = set()
        # The sync in progress: whether it is the setup import, when its time
        # budget ends, how its windows went and when it may send next.
        self._setup_import = False
        # When it started, and the time its imports took, which its budget
        # does not count (``_async_uncounted``); and when it may resend.
        self._started_at = self._uncounted = self._resend_not_before = 0.0
        self._deadline: float | None = None
        # The setup attempt's own time limit, which moves on with the budget.
        self._time_limit: asyncio.Timeout | None = None
        # The pause between two requests of the lanes' coverage, at random
        # within these bounds, while it runs; else the usual pacing.
        self._pause: tuple[float, float] | None = None
        self._progress: _Progress | None = None
        self._calls_started = self._measurement_requests = 0
        self._last_measurements_start = self._last_measurements_end = None
        self._cooldown_until: dict[str, float] = {}
        self._retries = self._splits = 0
        self._windows_ok = self._windows_empty = self._windows_failed = 0
        self._coverage_windows = self._revision_windows = 0
        self._history_start = False
        # The setup import's units that started failing, warned about once
        # the import is known to go on (``_report_failing``).
        self._unreported: set[CacheKey] = set()

    def _restore(self, memory: SyncMemory | None) -> None:
        """Take over what another coordinator remembered of each direction."""
        if memory is None:
            return
        memory = memory.copy()
        self._unit_states.update(memory.unit_states)
        self._measurement_cache.update(
            (key, dict(rows)) for key, rows in memory.measurements.items()
        )
        self._row_density.update(memory.row_density)
        self._backfill_attempted.update(memory.searched)
        self._unconfirmed.update(memory.unconfirmed)
        self._lanes.update(memory.lanes)

    @property
    def accounts(self) -> tuple[MeridianAccount, ...]:
        """Return the selected, cached account topology."""
        return self._filtered_topology()

    @property
    def request_count(self) -> int:
        """Return how many HTTP requests the current or last sync sent."""
        return self._requests.sent

    @property
    def median_request_seconds(self) -> float | None:
        """Return the median time of that sync's requests that ended, if any."""
        return self._requests.median_seconds()

    @property
    def imported_newest(self) -> dict[str, datetime]:
        """
        Return the newest hour each energy statistic was given in the last sync.

        Keyed by statistic ID; only statistics that were written appear. The
        setup flow reads these rows back before it creates the entry.
        """
        return dict(self._imported_newest)

    @property
    def memory(self) -> SyncMemory:
        """
        Return a copy of what this coordinator remembers of each direction.

        The setup flow hands it to the new entry once the import succeeded:
        no rollback has run, so it matches what is stored.
        """
        return SyncMemory(
            unit_states=self._unit_states,
            measurements=self._measurement_cache,
            row_density=self._row_density,
            searched=frozenset(self._backfill_attempted),
            unconfirmed=self._unconfirmed,
            lanes=self._lanes,
        ).copy()

    @property
    def billing_metadata_unavailable_count(self) -> int:
        """Return how many selected accounts have a failed billing refresh."""
        return len(self._billing_unavailable)

    @property
    def billing_metadata_cache_age_seconds(self) -> float | None:
        """Return the age of the oldest billing metadata cache entry."""
        if not self._billing_cached_at:
            return None
        now = _utcnow()
        oldest_cached_at = min(self._billing_cached_at.values())
        return max(0.0, (now - oldest_cached_at).total_seconds())

    def unit_diagnostics(self) -> list[dict[str, Any]]:
        """
        Return each unit's status, without identifiers, in topology order.

        Units are named by their account's and property's positions and their
        direction. ``retry_in_s`` is how long a failed unit waits before it
        is tried again, or a degraded one's reconciliation: none until a
        first sync has succeeded, since every attempt until then tries each
        unit (``_async_plan_unit``), the first after setup's import
        included. ``not_reached`` is how many updates in a row ran out of
        time before its first request; ``last_failure`` gives the reason,
        the error's class, its HTTP status and its age. ``reaches_target``
        says the unit's history reaches back to its target with no hour
        missing that Meridian may still have, as its last coverage found
        (``lanes.reaches_target``), so no after a start until the update
        after the first sync has looked: a revision pass does not change it, and
        ``history_start_found`` that Meridian showed where it starts: both
        yes or no, never a number of days, which would tell how long the
        history is.
        """
        try:
            accounts = self.accounts
        except ValueError:
            return []
        now = _utcnow()
        waits = self._initial_refresh_complete
        units: list[dict[str, Any]] = []
        for account_index, account in enumerate(accounts, start=1):
            for property_index, property_data in enumerate(account.properties, start=1):
                key = property_key(account.number, property_data.id)
                for direction, _ in _directions(key, property_data):
                    state = self._unit_states.get((key, direction), UnitState())
                    lane = self._lanes.get((key, direction), LaneState())
                    failure = state.last_failure
                    retry_at = state.next_try if waits else None
                    units.append(
                        {
                            "account": f"{account_index}/{len(accounts)}",
                            "property": f"{property_index}/{len(account.properties)}",
                            "direction": direction.lower(),
                            "status": state.status,
                            "degraded": state.status == STATUS_DEGRADED,
                            "owed_reconciliation": state.owed,
                            "retry_in_s": (
                                round(max(0.0, (retry_at - now).total_seconds()))
                                if retry_at is not None
                                else None
                            ),
                            "not_reached": state.not_reached,
                            "reaches_target": lane.reaches_target,
                            "history_start_found": lane.history_start_found,
                            "last_failure": (
                                {
                                    "reason": failure.reason,
                                    "cause": failure.cause,
                                    "http_status": failure.http_status,
                                    "age_s": round(
                                        max(0.0, (now - failure.at).total_seconds())
                                    ),
                                }
                                if failure is not None
                                else None
                            ),
                        }
                    )
        return units

    async def async_prepare_topology(self) -> None:
        """
        Renew the stored session and cache the selected account topology.

        This is the only Meridian work done during config-entry setup. Errors
        are raised as config-entry exceptions so Home Assistant starts
        reauthentication or retries setup with its own backoff. Right after
        the setup flow, the accounts it found are cached already, and no
        request is made: the session the flow validated is renewed by the
        first sync.
        """
        self._begin_calls()
        try:
            await self._async_get_topology(_utcnow())
        except MeridianAuthenticationError as err:
            raise ConfigEntryAuthFailed(
                translation_domain=DOMAIN,
                translation_key="invalid_auth",
            ) from err
        except MeridianRateLimitError as err:
            raise ConfigEntryNotReady(
                translation_domain=DOMAIN,
                translation_key="rate_limited",
            ) from err
        except MeridianUpstreamError as err:
            raise ConfigEntryNotReady(
                translation_domain=DOMAIN,
                translation_key=_upstream_key(err),
                translation_placeholders={"detail": err.detail},
            ) from err
        except MeridianConnectionError as err:
            raise ConfigEntryNotReady(
                translation_domain=DOMAIN,
                translation_key="cannot_connect",
            ) from err
        except (MeridianError, ValueError) as err:
            raise ConfigEntryNotReady(
                translation_domain=DOMAIN,
                translation_key="invalid_data",
            ) from err

    async def _async_update_data(self) -> MeridianSyncData:
        return await self.async_fetch_and_import(time_budget=POLL_BUDGET_SECONDS)

    async def async_fetch_and_import(  # noqa: C901 - one branch per failure kind
        self,
        *,
        setup_import: bool = False,
        time_budget: float | None = None,
        time_limit: asyncio.Timeout | None = None,
        progress: Callable[[float], None] | None = None,
    ) -> MeridianSyncData:
        """
        Fetch Meridian data and import statistics for setup or polling.

        The setup import passes ``setup_import=True``. Its coordinator is
        discarded once the entry exists, so billing metadata and bill totals
        would only add a request, and a recorder wait, that could fail a
        finished import; the entry's first sync fetches them. And the flow
        has just waited for the recorder itself, before every attempt and in
        two passes, as the first sync does, so this sync does not. Only the
        setup import asks for a week again, by its days, after a gateway
        timeout, once the cooldown has passed: in polling, the next poll is
        the retry. Nor does the setup import fill in older history: the
        entry's updates do, a few weeks at each (``_async_cover_lanes``).

        With ``time_budget`` (seconds), no measurements request after the
        first starts once less than 80 s of it remain: ``RunBudgetError`` is
        raised instead, and what came back before is imported, as later
        updates fetch the rest. An update that walks a direction back
        further than 14 days has 900 s instead (``_extend_budget``). The
        time each unit's import takes, waiting for the recorder to save and
        confirm it, is not counted; ``time_limit``, the setup attempt's own
        ``asyncio.timeout``, moves on with the budget (``_async_uncounted``).
        ``progress`` is called with the fraction of the planned measurement
        windows done, after each one.

        Until a first sync has succeeded, one that Meridian failed, or whose
        statistics Home Assistant did not confirm (``statistics_write``), is
        tried again after 10 minutes, then 30, then hourly, and one that
        found the recorder still busy after 10 minutes; one whose replay was
        refused (``statistics_refused``) waits for the hourly update, since
        no retry clears it. A rate limit makes polling wait twice as long
        each time it comes again in a row, up to a day; so does one the
        lanes' coverage met in an update that succeeded, whose next update
        then waits as long, never less than the hourly interval, and whose
        coverage does not run until then (``_async_cover_lanes``).
        """
        first_sync = not self._initial_refresh_complete
        # A wait after a rate limit holds for the update after it alone.
        self.update_interval = UPDATE_INTERVAL
        try:
            data = await self._async_fetch_and_import_data(
                setup_import=setup_import,
                time_budget=time_budget,
                time_limit=time_limit,
                progress=progress,
            )
        except _RecorderBehindError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="recorder_busy",
                retry_after=FIRST_SYNC_RETRY_SECONDS if first_sync else None,
            ) from err
        except MeridianAuthenticationError as err:
            raise ConfigEntryAuthFailed(
                translation_domain=DOMAIN,
                translation_key="invalid_auth",
            ) from err
        except MeridianRateLimitError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="rate_limited",
                retry_after=self._rate_limit_wait(err, setup_import=setup_import),
            ) from err
        except RunBudgetError as err:
            if setup_import:
                raise
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="upstream_timeout",
                translation_placeholders={"detail": _BUDGET_DETAIL},
                retry_after=self._first_sync_retry(first_sync),
            ) from err
        except _UnitsDeferredError as err:
            reason = err.failure.reason if err.failure is not None else ""
            detail = err.failure.detail if err.failure is not None else None
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key=_DEFERRED_KEYS.get(reason, "cannot_connect"),
                translation_placeholders={"detail": detail or _BUDGET_DETAIL},
            ) from err
        except MeridianUpstreamError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key=_upstream_key(err),
                translation_placeholders={"detail": err.detail},
                retry_after=self._first_sync_retry(first_sync),
            ) from err
        except MeridianConnectionError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="cannot_connect",
                retry_after=self._first_sync_retry(first_sync),
            ) from err
        except StatisticsAnchorError as err:
            # A retry refuses again until the stored statistics change, so a
            # first sync waits for the hourly update, not its short retries.
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="statistics_refused",
            ) from err
        except StatisticsWriteError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="statistics_write",
                retry_after=self._first_sync_retry(first_sync),
            ) from err
        except (MeridianError, ValueError) as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="invalid_data",
                retry_after=self._first_sync_retry(first_sync),
            ) from err
        else:
            self._honour_coverage_rate_limit(data)
            return data

    def _honour_coverage_rate_limit(self, data: MeridianSyncData) -> None:
        """
        Make the update after one that succeeded wait out a coverage rate limit.

        Meridian's wait, doubled for each rate limit in a row as polling
        doubles it (``_rate_limit_wait``): no coverage runs before it has
        passed, and the next update starts no sooner, nor ever sooner than
        the hourly update, however short the wait: an update after it asks
        Meridian for its own walks as well. An update that met no rate limit
        starts the doubling again.
        """
        limit = self._coverage_rate_limit
        if limit is None:
            self._rate_limits = 0
            return
        wait = timedelta(seconds=self._rate_limit_wait(limit, setup_import=False))
        self._coverage_not_before = data.synced_at + wait
        self.update_interval = max(UPDATE_INTERVAL, wait)

    def _first_sync_retry(self, first_sync: bool) -> float | None:  # noqa: FBT001
        """
        Return when a failed first sync is tried again, and count the failure.

        Until one sync has succeeded, retry sooner than the hourly poll: the
        first sync runs after setup, so setup's own retry no longer covers it.
        Each failure in a row waits longer, so a failure that keeps coming is
        not repeated every 10 minutes.
        """
        if not first_sync:
            return None
        wait = FIRST_SYNC_RETRY_SCHEDULE[
            min(self._first_sync_failures, len(FIRST_SYNC_RETRY_SCHEDULE) - 1)
        ]
        self._first_sync_failures += 1
        return wait

    def _rate_limit_wait(
        self, err: MeridianRateLimitError, *, setup_import: bool
    ) -> float:
        """
        Return how long to wait after a rate limit.

        The setup flow shows Meridian's own wait. Polling waits that long
        the first time, and twice as long for each rate limit in a row, up
        to a day: Kraken's in-band rate limit, which gives no wait of its
        own, so waits 1 h, then 2 h, 4 h and on.
        """
        if setup_import:
            return err.retry_after
        self._rate_limits += 1
        doubled = err.retry_after * 2 ** min(5, self._rate_limits - 1)
        return float(min(MAX_RETRY_AFTER_SECONDS, doubled))

    async def _async_fetch_and_import_data(
        self,
        *,
        setup_import: bool,
        time_budget: float | None,
        time_limit: asyncio.Timeout | None,
        progress: Callable[[float], None] | None,
    ) -> MeridianSyncData:
        """Complete one sync and return its privacy-safe operational summary."""
        self._begin_calls()
        self._time_limit = time_limit
        self._imported_newest = {}
        self._setup_import = setup_import
        self._progress = None if progress is None else _Progress(progress)
        if not (setup_import or self._initial_refresh_complete):
            await self._async_wait_for_recorder()
        # Timed from here: a wait for a busy recorder is Home Assistant's,
        # and must not read as a slow Meridian sync in diagnostics.
        started_at = self._started_at = monotonic()
        self._deadline = None if time_budget is None else started_at + time_budget
        # Only the transport knows how many requests a call sends, so count
        # them there, for this sync's task alone.
        with count_requests() as requests:
            self._requests = requests
            now = _utcnow()
            accounts, topology_refreshed = await self._async_get_topology(now)
            self._topology_fresh = topology_refreshed
            mode = (
                await self._async_startup_mode(accounts, now)
                if not self._initial_refresh_complete
                else self._select_sync_mode(now)
            )
            try:
                results = await self._async_sync_accounts(accounts, mode, now)
            except MeridianGraphQLError as err:
                if topology_refreshed or not _is_topology_error(err):
                    raise
                accounts, topology_refreshed = await self._async_get_topology(
                    now, force=True
                )
                self._topology_fresh = topology_refreshed
                results = await self._async_sync_accounts(accounts, mode, now)
            self._settle_run(setup_import=setup_import)

            self._initial_refresh_complete = True
            self._record_sync_cadence(mode, now)
            account_results = (
                () if setup_import else await self._async_account_results(accounts, now)
            )
            outcomes = Counter(unit.outcome for unit in self._run_units)

            data = MeridianSyncData(
                account_count=len(accounts),
                property_count=sum(len(account.properties) for account in accounts),
                results=results,
                account_results=account_results,
                synced_at=now,
                sync_mode=mode,
                topology_refreshed=topology_refreshed,
                topology_cache_age_seconds=self._topology_cache_age(now),
                sync_duration_seconds=monotonic() - started_at,
                request_count=requests.sent,
                # A fetched direction makes at least one request.
                direction_count=sum(
                    1 + (result.generation_pages > 0) for result in results
                ),
                median_request_seconds=requests.median_seconds(),
                median_measurements_seconds=requests.median_seconds("measurements"),
                windows_ok=self._windows_ok,
                windows_empty=self._windows_empty,
                windows_failed=self._windows_failed,
                coverage_windows=self._coverage_windows,
                revision_windows=self._revision_windows,
                retries=self._retries,
                splits=self._splits,
                history_start=self._history_start,
                units_failed=outcomes[STATUS_FAILED],
                units_degraded=outcomes[STATUS_DEGRADED],
                units_deferred=outcomes[STATUS_DEFERRED],
            )
        self._log_sync_summary(data)
        return data

    def _settle_run(self, *, setup_import: bool) -> None:
        """
        Decide whether a sync succeeded, and warn about units that started failing.

        A sync succeeds when at least one unit fetched and imported, in full
        or its newer windows only; the setup import, when every unit did, or
        when at least one property did so for every direction it has and
        found readings for one of them, each with its newest window in full
        at least: the entry's updates import the rest. A property with no
        readings in its 90 days, as one whose supply ended, imported no
        history: counted, it would create the entry with none while the
        active property failed. Otherwise the first unit's failure fails
        it, as any failure failed every sync before 0.3.0, or, when every
        unit waited after an earlier failure, that failure. In setup it is
        the failure of the first unit that did not import its newest
        window, which is what kept setup from succeeding: a unit degraded
        after its newest window did not, and its failure would hide that
        one, such as a service error after which the form offers to
        continue without history. ``last_request`` names the failed request
        of the unit whose failure is raised, not a later unit's. A refused
        replay (``StatisticsAnchorError``) fails it only when no unit failed
        otherwise: no retry clears a refusal, so it
        would hold a failed first sync to the hourly update
        (``async_fetch_and_import``) while another unit's failure, one
        Meridian may soon recover from, is tried again after 10 minutes.
        A sync with no units at all, an account without properties say,
        succeeds. A setup import that fails because its time budget ran out
        after its size ladder went on past a gateway timeout fails with
        that timeout: it is what cost the time, so the failure form and its
        warning describe that timeout rather than the budget. In setup, each
        unit that started failing logs its one WARNING here, once the import
        is known to go on, since a setup attempt that fails logs its own; an
        hourly update has logged it already (``_report_failing``).
        """
        units = self._run_units
        imported = [
            unit for unit in units if unit.outcome in {STATUS_OK, STATUS_DEGRADED}
        ]
        if setup_import:
            properties: dict[str, list[_Unit]] = {}
            for unit in units:
                properties.setdefault(unit.cache_key[0], []).append(unit)
            succeeded = all(unit.imported_newest for unit in units) or any(
                all(unit.imported_newest for unit in group)
                and any(unit.found_readings for unit in group)
                for group in properties.values()
            )
        else:
            succeeded = not units or bool(imported)
        if succeeded:
            for unit in units:
                if unit.cache_key in self._unreported:
                    self._warn_unit_failing(unit, setup_import=True)
            self._unreported.clear()
        if not succeeded:
            failed = _failed_unit(units, setup_import=setup_import)
            if failed is not None and failed.error is not None:
                if failed.walk is not None and failed.walk.call is not None:
                    # Other units' requests may have come after it: the
                    # failure raised names its own.
                    self.last_request = failed.walk.call
                timeout = next(
                    (
                        unit.walk.gateway_timeout
                        for unit in units
                        if unit.walk is not None
                        and unit.walk.gateway_timeout is not None
                    ),
                    None,
                )
                if isinstance(failed.error, RunBudgetError) and timeout is not None:
                    # Raised as it came, with its own cause, as the failure
                    # warning names: not from the budget it left too short.
                    raise timeout
                raise failed.error
            raise _UnitsDeferredError(
                next(
                    (
                        unit.state.last_failure
                        for unit in units
                        if unit.state.last_failure is not None
                    ),
                    None,
                )
            )

    async def _async_wait_for_recorder(self) -> None:
        """
        Wait until the recorder has saved the work queued before this first sync.

        The setup import's rows, or those of the entry's coordinator before a
        reload, may still be queued: setup waits for them a minute at most.
        Read before they are saved, this sync would find no history, start
        the running sums from zero again and import over rows still to be
        saved, which breaks the sums at the first hour they differ, for
        good. It runs in the background, so it can wait longer; when the
        recorder is still behind, it stops before any request or statistics
        read, and Home Assistant tries again later. It waits in two passes,
        so rows the recorder had to queue again behind the first are saved
        too; only rows retried twice in a row can still come after it.
        """
        if not await async_wait_for_recorder(
            self.hass, attempts=FIRST_SYNC_RECORDER_WAITS, passes=2
        ):
            raise _RecorderBehindError

    def _begin_calls(self) -> None:
        """Start counting API calls for setup or a sync, forgetting the last one."""
        self._calls_started = 0
        self._requests = RequestTally()
        self.last_request = None
        self._deadline = None
        self._uncounted = 0.0
        self._time_limit = None
        self._pause = None
        self._measurement_requests = 0
        self._last_measurements_start = self._last_measurements_end = None
        self._resend_not_before = 0.0
        self._cooldown_until = {}
        self._coverage_rate_limit = None
        self._retries = 0
        self._splits = 0
        self._windows_ok = 0
        self._windows_empty = 0
        self._windows_failed = 0
        self._coverage_windows = 0
        self._revision_windows = 0
        self._history_start = False
        self._run_units = []
        self._unreported = set()

    def _note_call(
        self,
        operation: str,
        *,
        position: _Position | None = None,
        direction: str | None = None,
        page: int | None = None,
        bound: str | None = None,
        window: MeasurementWindow | None = None,
    ) -> RequestContext:
        """Record the API call about to start, so a failure can name it."""
        self._calls_started += 1
        self.last_request = call = RequestContext(
            operation=operation,
            calls=self._calls_started,
            started_at=monotonic(),
            direction=direction,
            account_index=position.account_index if position else None,
            account_count=position.account_count if position else None,
            property_index=position.property_index if position else None,
            property_count=position.property_count if position else None,
            page=page,
            bound=bound,
            window=window.index if window else None,
            window_count=window.count if window else None,
        )
        return call

    def _end_call_in_flight(self) -> None:
        """Mark the call in flight as finished: its failure has been handled."""
        if self.last_request is not None and self.last_request.finished_at is None:
            self._note_call_done(self.last_request)

    def _note_call_done(self, call: RequestContext) -> float:
        """
        Mark a call as finished, so a later failure is not blamed on it.

        Return how long it took, all the requests it sent included.
        """
        finished_at = monotonic()
        self.last_request = replace(call, finished_at=finished_at)
        return max(0.0, finished_at - call.started_at)

    def _record_sync_cadence(self, mode: SyncMode, now: datetime) -> None:
        """Record a completed measurement sync before optional billing work."""
        if mode in {SyncMode.INITIAL, SyncMode.RESTART}:
            self._last_targeted_reconciliation = now
            self._last_full_reconciliation = now
        elif mode is SyncMode.TARGETED_RECONCILIATION:
            self._last_targeted_reconciliation = now
        elif mode is SyncMode.FULL_RECONCILIATION:
            self._last_targeted_reconciliation = now
            self._last_full_reconciliation = now

    @staticmethod
    def _log_sync_summary(data: MeridianSyncData) -> None:
        """Write one privacy-safe summary when debug logging is enabled."""
        _LOGGER.debug(
            "Meridian sync completed: mode=%s duration=%.2fs accounts=%d "
            "properties=%d directions=%d requests=%d median_request=%s "
            "median_measurements=%s pages=%d rows_received=%d rows_retained=%d "
            "rows_imported=%d provisional=%d windows_ok=%d windows_empty=%d "
            "windows_failed=%d coverage_windows=%d revision_windows=%d retries=%d "
            "splits=%d history_start=%s units_failed=%d units_degraded=%d "
            "units_deferred=%d topology_refreshed=%s",
            data.sync_mode,
            data.sync_duration_seconds,
            data.account_count,
            data.property_count,
            data.direction_count,
            data.request_count,
            format_seconds(data.median_request_seconds),
            format_seconds(data.median_measurements_seconds),
            sum(
                result.consumption_pages + result.generation_pages
                for result in data.results
            ),
            sum(
                result.consumption_received_rows + result.generation_received_rows
                for result in data.results
            ),
            sum(
                result.consumption_retained_rows + result.generation_retained_rows
                for result in data.results
            ),
            sum(
                result.consumption_rows + result.generation_rows
                for result in data.results
            ),
            sum(result.estimated_rows for result in data.results),
            data.windows_ok,
            data.windows_empty,
            data.windows_failed,
            data.coverage_windows,
            data.revision_windows,
            data.retries,
            data.splits,
            "yes" if data.history_start else "no",
            data.units_failed,
            data.units_degraded,
            data.units_deferred,
            data.topology_refreshed,
        )

    def _select_sync_mode(self, now: datetime) -> SyncMode:
        if not self._initial_refresh_complete:
            return SyncMode.INITIAL
        return self._cadence_mode(now)

    def _cadence_mode(self, now: datetime) -> SyncMode:
        """Return the mode the reconciliation cadence asks for at ``now``."""
        if (
            self._last_full_reconciliation is None
            or now - self._last_full_reconciliation >= FULL_RECONCILIATION_INTERVAL
        ):
            return SyncMode.FULL_RECONCILIATION
        if (
            self._last_targeted_reconciliation is None
            or now - self._last_targeted_reconciliation
            >= TARGETED_RECONCILIATION_INTERVAL
        ):
            return SyncMode.TARGETED_RECONCILIATION
        return SyncMode.TIP

    async def _async_get_topology(
        self, now: datetime, *, force: bool = False
    ) -> tuple[tuple[MeridianAccount, ...], bool]:
        expired = (
            self._topology_cached_at is None
            or now - self._topology_cached_at >= TOPOLOGY_CACHE_INTERVAL
        )
        if self._topology_seeded and not force and not expired:
            # The accounts setup found: filtered and pruned as fetched ones
            # are, but not refreshed, so a topology error still refreshes them.
            self._topology_seeded = False
            self._include_new_accounts()
            accounts = self._filtered_topology()
            self._prune_topology_state(accounts)
            return accounts, False
        if force or self._topology is None or expired:
            self._topology_seeded = False
            call = self._note_call("accounts")
            self._topology = await self.client.async_get_accounts()
            self._note_call_done(call)
            self._topology_cached_at = now
            self._include_new_accounts()
            accounts = self._filtered_topology()
            self._prune_topology_state(accounts)
            return accounts, True
        return self._filtered_topology(), False

    def _prune_topology_state(self, accounts: tuple[MeridianAccount, ...]) -> None:
        """Discard runtime caches for accounts and directions no longer selected."""
        account_numbers = {account.number for account in accounts}
        property_directions: set[CacheKey] = set()
        statistic_ids: set[str] = set()
        for account in accounts:
            for property_data in account.properties:
                key = property_key(account.number, property_data.id)
                property_directions.add((key, READING_CONSUMPTION))
                statistic_ids.update(consumption_ids(key))
                if any(meter.has_feed_in for meter in property_data.meter_points):
                    property_directions.add((key, READING_GENERATION))
                    statistic_ids.update(generation_ids(key))

        self._billing_periods = {
            key: value
            for key, value in self._billing_periods.items()
            if key in account_numbers
        }
        self._billing_cached_at = {
            key: value
            for key, value in self._billing_cached_at.items()
            if key in account_numbers
        }
        self._billing_unavailable.intersection_update(account_numbers)
        self._measurement_cache = {
            key: value
            for key, value in self._measurement_cache.items()
            if key in property_directions
        }
        self._row_density = {
            key: value
            for key, value in self._row_density.items()
            if key in property_directions
        }
        self._known_statistics = {
            key: value
            for key, value in self._known_statistics.items()
            if key in statistic_ids
        }
        self._latest_statistics = {
            key: value
            for key, value in self._latest_statistics.items()
            if key in statistic_ids
        }
        self._frontier = {
            key: value for key, value in self._frontier.items() if key in statistic_ids
        }
        self._unit_states = {
            key: value
            for key, value in self._unit_states.items()
            if key in property_directions
        }
        self._lanes = {
            key: value
            for key, value in self._lanes.items()
            if key in property_directions
        }
        # In place: the mappings are their owner's (``unconfirmed``,
        # ``unsaved``).
        for stale in self._unconfirmed.keys() - property_directions:
            del self._unconfirmed[stale]
        for stale in self._unsaved_coverage.keys() - property_directions:
            del self._unsaved_coverage[stale]
        reappeared_directions = (
            property_directions - self._active_directions
        ) & self._seen_directions
        self._pending_backfill.update(reappeared_directions)
        self._backfill_attempted.intersection_update(property_directions)
        self._pending_backfill.intersection_update(property_directions)
        self._active_directions = property_directions
        self._seen_directions.update(property_directions)

    def _filtered_topology(self) -> tuple[MeridianAccount, ...]:
        """Return only accounts explicitly selected for this config entry."""
        if self._topology is None:
            return ()
        if self._selected_accounts is None:
            return self._topology
        selected = tuple(
            account
            for account in self._topology
            if account.number in self._selected_accounts
        )
        if not selected:
            raise ValueError("None of the selected Meridian accounts are available")
        return selected

    def _include_new_accounts(self) -> None:
        """Include newly discovered accounts when the user selected all accounts."""
        if not self._auto_add_accounts or self._topology is None:
            return
        selected_accounts = frozenset(account.number for account in self._topology)
        if selected_accounts == self._selected_accounts:
            return
        self._selected_accounts = selected_accounts
        if self._config_entry is not None:
            self.hass.config_entries.async_update_entry(
                self._config_entry,
                data={
                    **self._config_entry.data,
                    CONF_SELECTED_ACCOUNTS: sorted(selected_accounts),
                    CONF_AUTO_ADD_ACCOUNTS: True,
                },
            )

    async def _async_startup_mode(
        self, accounts: tuple[MeridianAccount, ...], now: datetime | None = None
    ) -> SyncMode:
        """
        Distinguish a new install from a restart without changing IDs.

        The first sync after setup continues what setup imported: with every
        statistic present, or searched in full by setup and found empty
        (``_searched_by_setup``), it is in the mode the cadence asks for,
        counted from the setup import's sync, rather than a 14-day restart.
        """
        has_all_statistics = True
        for account in accounts:
            for property_data in account.properties:
                key = property_key(account.number, property_data.id)
                for direction, (energy_id, _) in _directions(key, property_data):
                    await self._async_cache_statistic(energy_id)
                    has_all_statistics &= self._known_statistics[
                        energy_id
                    ] or self._searched_by_setup((key, direction))
        if not has_all_statistics:
            return SyncMode.INITIAL
        if self._reconciled_at is not None:
            return self._cadence_mode(now or _utcnow())
        return SyncMode.RESTART

    def _topology_cache_age(self, now: datetime) -> float:
        if self._topology_cached_at is None:
            return 0.0
        return max(0.0, (now - self._topology_cached_at).total_seconds())

    async def _async_sync_accounts(
        self,
        accounts: tuple[MeridianAccount, ...],
        mode: SyncMode,
        now: datetime,
    ) -> tuple[PropertySyncResult, ...]:
        """
        Sync every unit, newest windows first, each on its own.

        Every unit's newest window is fetched before any unit's older ones,
        so each unit's first request starts early, and the start reserve can
        only hold back older windows; only a unit whose property is cooling
        down after a gateway timeout lets the others go on meanwhile, and
        fetches its newest window once the cooldown has passed, before its
        property's older windows (``_async_run_units``). Each unit then
        walks on and imports, what it fetched before a failure or the end
        of the time budget included. A failure stops that unit's walk alone
        (``_async_walk_unit``); a rejected session, a rate limit and a
        missing account or property not yet refreshed stop the sync. An
        update that walks a direction back further than 14 days has 900 s
        (``_extend_budget``). Then, in an update after the coordinator's
        first sync, each unit whose walk went through asks for a few windows
        more of the older history still missing, or of its monthly revision
        (``_async_cover_lanes``).
        """
        disambiguate = sum(len(account.properties) for account in accounts) > 1
        units: list[_Unit] = []
        self._run_units = units
        for account_index, account in enumerate(accounts, start=1):
            for property_index, property_data in enumerate(account.properties, start=1):
                position = _Position(
                    account_index,
                    len(accounts),
                    property_index,
                    len(account.properties),
                )
                key = property_key(account.number, property_data.id)
                for direction, statistic_ids in _directions(key, property_data):
                    units.append(
                        await self._async_plan_unit(
                            account.number,
                            property_data,
                            (key, direction),
                            statistic_ids,
                            mode=mode,
                            now=now,
                            position=position,
                            disambiguate=disambiguate,
                        )
                    )
        if self._progress is not None:
            self._progress.planned = sum(
                unit.walk.planned for unit in units if unit.walk is not None
            )
            self._progress.done = 0
        if any(unit.walk is not None and unit.deep(now) for unit in units):
            self._extend_budget()
        try:
            await self._async_run_units(units, now)
            if not self._setup_import:
                await self._async_cover_lanes(units, now)
        finally:
            self._keep_unsaved(units, now)
        return tuple(
            self._property_result(account.number, property_data, units, mode)
            for account in accounts
            for property_data in account.properties
        )

    def _extend_budget(self) -> None:
        """
        Give an update that walks a direction back over 14 days 900 s.

        Such a walk, a first import or a catch-up after a long outage, can
        take more requests than 4 minutes allow at slow request times. It
        imports what it fetched all the same, and the days it did not reach
        are filled in later, but a search that never completes never shows
        that a supply has ended (``_quiet``): it would walk back again at
        every update. So it has 900 s instead. It is known before the first
        request (``_Unit.deep``), except for a quiet direction, whose walk
        goes back only once its newest window has readings again
        (``_async_walk_step``). The setup import has its own budget. The
        time imports took before is still not counted (``_async_uncounted``).
        """
        if self._deadline is not None and not self._setup_import:
            self._deadline = max(
                self._deadline,
                self._started_at + DEEP_WALK_BUDGET_SECONDS + self._uncounted,
            )

    async def _async_run_units(self, units: list[_Unit], now: datetime) -> None:
        """
        Fetch every unit's newest window, then walk each on and import it.

        The newest windows go first, in the order ``_next_newest`` gives:
        those of units the last syncs' time budget did not reach before the
        others (``UnitState.not_reached``), so no unit waits behind the same
        others every time, and a property with nothing new, as one whose
        supply ended, costs the others one request before their newest
        windows. A unit whose property is cooling down after a gateway
        timeout waits behind the units that can send at once, in both
        passes, rather than hold up their requests for the cooldown. The
        walks then go on in the order ``_next_walk`` gives, and before each
        window a walk lets a unit that comes before it go first
        (``_comes_first``): so a newest window that waited for a cooldown is
        fetched once it has passed, before the older windows of its
        property, and a property with nothing new goes last.
        """
        walking = [unit for unit in units if unit.walk is not None]
        newest = sorted(walking, key=lambda unit: -unit.state.not_reached)
        while ready := self._next_newest(walking, newest):
            newest.remove(ready)
            assert ready.walk is not None  # noqa: S101 - only walking units
            await self._async_walk_unit(ready.walk, newest_only=True)
        while walking:
            unit = self._next_walk(walking)
            walk = unit.walk
            assert walk is not None  # noqa: S101 - only walking units
            await self._async_walk_unit(
                walk, pause=partial(self._comes_first, unit, walking)
            )
            if walk.done:
                walking.remove(unit)
                await self._async_finish_unit(unit, now)

    def _next_newest(self, walking: list[_Unit], newest: list[_Unit]) -> _Unit | None:
        """
        Return the unit whose newest window is fetched next, if one can send.

        Of the units in ``newest`` that can send at once, those the time
        budget did not reach in the most syncs in a row first
        (``UnitState.not_reached``); among them, the units of a property
        whose newest windows returned readings in this sync, then those of a
        property none of whose newest windows was fetched yet, then the
        rest, each in the order ``newest`` holds. So a property whose newest
        window came back empty, as one whose supply ended, lets the other
        properties' newest windows go before its other direction's. That
        holds in setup too, where every count is 0, as each attempt builds a
        new coordinator: there the time budget can allow only a few
        requests, and a property counts only once all its directions have
        their newest windows (``_settle_run``).
        """
        at = monotonic()
        ready = [unit for unit in newest if not self._cooldown_left(unit, at)]
        with_readings = self._properties_with_readings()
        fetched = {unit.cache_key[0] for unit in walking if unit not in newest}

        def order(unit: _Unit) -> tuple[int, int]:
            key = unit.cache_key[0]
            rank = 0 if key in with_readings else 1 if key not in fetched else 2
            return (-unit.state.not_reached, rank)

        return min(ready, key=order, default=None)

    def _properties_with_readings(self) -> set[str]:
        """Return the properties whose walks returned readings in this sync."""
        return {
            unit.cache_key[0]
            for unit in self._run_units
            if unit.walk is not None and unit.walk.walk.oldest_seen is not None
        }

    def _next_walk(self, walking: list[_Unit]) -> _Unit:
        """
        Return the unit whose walk goes on next.

        Those that can send at once first, then the one whose property's
        cooldown ends soonest. Among those, the units of a property whose
        walks returned readings in this sync, so in setup an active
        property's weeks go before those of one that has none, as one whose
        supply ended. Then a unit whose newest window is not yet fetched in
        full, as one that waited for a cooldown or is asked for by day,
        before those on older windows, and one that has fetched none of it
        first, so each unit's first window comes early: the size ladder of
        one direction's newest week does not hold up the other's. Then the
        units whose walk returned readings, then topology order.
        """
        # One reading of the clock for every unit: those of one property
        # then wait exactly as long.
        at = monotonic()
        with_readings = self._properties_with_readings()

        def order(unit: _Unit) -> tuple[float, bool, bool, bool, bool, int]:
            walk = unit.walk
            assert walk is not None  # noqa: S101 - only walking units
            return (
                self._cooldown_left(unit, at),
                unit.cache_key[0] not in with_readings,
                walk.newest_done,
                walk.completed_on is not None,
                walk.walk.oldest_seen is None,
                walking.index(unit),
            )

        return min(walking, key=order)

    def _cooldown_left(self, unit: _Unit, at: float | None = None) -> float:
        """Return how long a unit must wait for its property's cooldown to send."""
        walk = unit.walk
        if walk is None or walk.done:
            return 0.0
        until = self._cooldown_until.get(unit.cache_key[0], 0.0)
        return max(0.0, until - (monotonic() if at is None else at))

    def _comes_first(self, unit: _Unit, walking: list[_Unit]) -> bool:
        """
        Return whether a walk should let another unit go first (``_next_walk``).

        As one cooling down after a gateway timeout lets a unit that can
        send go on, and one on its older windows lets a newest window go
        whose property's cooldown has passed.
        """
        return self._next_walk(walking) is not unit

    async def _async_plan_unit(
        self,
        account_number: str,
        property_data: MeridianProperty,
        cache_key: CacheKey,
        statistic_ids: tuple[str, str],
        *,
        mode: SyncMode,
        now: datetime,
        position: _Position,
        disambiguate: bool,
    ) -> _Unit:
        """
        Plan one unit's sync: its mode, cutoff, stored hours and windows.

        After the first sync has succeeded, a unit that failed waits out its
        backoff, fetching nothing (``deferred``); a degraded one runs the
        hourly update until the reconciliation it owes is due, then that
        reconciliation, or a larger mode if the sync asks for one. So does a
        degraded unit whose backfill is still pending, as after a backfill
        that imported its newer windows only: its 90 days wait too. Before
        that, every unit is tried at each attempt: the first sync's own
        retries are spaced out instead. A unit that waits owes the
        reconciliation the sync would have run for it, as the cadence counts
        it done: it runs it once it is tried again.

        A unit with statistics reads its stored hours once, from its lane's
        target on (``lanes.coverage_target``), which reaches back to any date
        of the lane whose import was not confirmed: its walk goes by those from
        its cutoff on, its daily reconciliation reaches back to an hour
        missing between them in the last 14 days (``_mode_since``), and its
        coverage asks for the older days missing (``_async_cover_lanes``).
        """
        state = self._unit_states.setdefault(cache_key, UnitState())
        lane = self._lanes.get(cache_key)
        if lane is None:
            # A new coordinator's lane, as after a reload, takes over the
            # dates an earlier one left unconfirmed (``unsaved``).
            lane = self._lanes[cache_key] = LaneState()
            lane.restore_unsaved(self._unsaved_coverage.get(cache_key, {}))
        unit_mode = await self._direction_mode(cache_key, statistic_ids, mode, now=now)
        waiting = self._initial_refresh_complete
        deferred = waiting and state.deferred(now)
        if waiting and state.reconciliation_waits(now):
            state.owe(unit_mode)
            unit_mode = SyncMode.TIP
        else:
            unit_mode = larger_mode(unit_mode, state.owed)
        energy_id, money_id = statistic_ids
        target = coverage_target(
            now, self._billing_start(account_number, now), unsaved=lane.unsaved
        )
        lane_hours = StoredHours()
        if not deferred:
            # An unconfirmed deep replay's walk may reach back further.
            read_from = min(target, self._unconfirmed.get(cache_key, target))
            lane_hours = await self._async_stored_hours(
                energy_id, money_id, read_from, now
            )
        lane.missing_recent = missing_recent(lane_hours.energy, now)
        since = self._requested_since(cache_key, unit_mode, now)
        frontier = self._frontier.get(energy_id)
        unit = _Unit(
            account_number=account_number,
            property_data=property_data,
            cache_key=cache_key,
            statistic_ids=statistic_ids,
            position=position,
            suffix=(
                f" — {_one_line_address(property_data.address)}" if disambiguate else ""
            ),
            mode=unit_mode,
            since=since,
            state=state,
            frontier=frontier,
            has_statistics=self._known_statistics.get(energy_id, False),
            quiet=self._quiet(cache_key, unit_mode, frontier, now),
            target=target,
            lane_hours=lane_hours,
        )
        if deferred:
            state.owe(unit_mode)
            unit.outcome = STATUS_DEFERRED
            return unit
        unit.stored = _hours_from(lane_hours, since - _HOUR)
        unit.walk = self._unit_walk(
            account_number=account_number,
            property_id=property_data.id,
            direction=cache_key[1],
            since=since,
            mode=unit_mode,
            position=position,
            stored_energy=unit.stored.energy,
            has_statistics=unit.has_statistics,
            now=now,
            state=state,
            quiet=unit.quiet,
        )
        return unit

    def _quiet(
        self,
        cache_key: CacheKey,
        mode: SyncMode,
        frontier: datetime | None,
        now: datetime,
    ) -> bool:
        """
        Return whether a unit's walk may stop at an empty newest window.

        So it may in the hourly and daily updates of a direction whose last
        walk that ended without a failure found no reading newer than the
        newest stored hour, which is still the newest, and older than the
        mode's own span: a supply that ended, or a meter that went quiet.
        The frontier rule would walk it back to that hour at every update,
        up to 13 requests an hour for nothing. While its newest window
        stays empty, nothing newer has come; once readings come back there,
        the walk goes on to the frontier as before. The weekly
        reconciliation, a restart and an initial import walk back in full,
        for readings that come back for older days only.
        """
        state = self._unit_states.get(cache_key)
        return (
            mode in {SyncMode.TIP, SyncMode.TARGETED_RECONCILIATION}
            and state is not None
            and frontier is not None
            and state.quiet_frontier == frontier
            and frontier < self._mode_since(cache_key, mode, now)
        )

    async def _async_finish_unit(self, unit: _Unit, now: datetime) -> None:
        """
        Import what a unit's walk fetched, if it may be, and record how it went.

        A walk that ended without a failure is imported as before. One that
        stopped early (a failure, the size ladder or the time budget)
        imports the windows it fetched, which, newest first, are one block
        back from today, with statistics or without, whether or not they
        reach the frontier, the newest hour stored: the replay writes older
        history behind newer rows consistently, so the days between are
        filled in later, by the reconciliation the unit then owes for the
        last 14 days, and by its lane's coverage for older ones
        (``_async_cover_lanes``). Its failed window's dates wait before the
        coverage asks for them (``lanes.LaneState.fail``). The unit is then
        degraded: the reconciliation it owes waits with a backoff while the
        hourly update keeps running. A walk that fetched no window imports
        nothing. A degraded unit whose reconciliation imports nothing stays
        degraded: the reconciliation waits longer, and the hourly update
        goes on.

        A unit the time budget stopped before its first request did not
        fail: it owes the reconciliation this sync would have run, and the
        next sync starts with it. An hourly update while a degraded unit's
        reconciliation waits leaves it degraded and owing that.
        """
        walk = unit.walk
        assert walk is not None  # noqa: S101 - only fetched units finish
        if self._progress is not None:
            self._progress.add(walk.remaining())
        state = unit.state
        if isinstance(walk.error, RunBudgetError) and not walk.attempted:
            unit.outcome = STATUS_DEFERRED
            unit.error = walk.error
            state.owe(unit.mode)
            state.not_reached += 1
            self._log_once(
                "not_reached",
                "Meridian answered too slowly for this update to reach every grid "
                "import and export within its time limit; the next update starts "
                "with those it did not reach (direction=%s account=%s property=%s)",
                unit.cache_key[1],
                _place(*unit.position[:2]),
                _place(*unit.position[2:]),
            )
            return
        state.not_reached = 0
        fetch = self._walk_result(walk)
        unit.fetch = fetch
        self._remember_density(unit.cache_key, fetch)
        imports = walk.complete or walk.completed_on is not None
        if imports:
            async with self._async_uncounted():
                try:
                    unit.fetch, unit.rows = await self._async_import_unit(
                        unit, fetch, now
                    )
                except (ValueError, StatisticsWriteError) as err:
                    unit.error = err
                    imports = False
        if imports:
            self._note_lane_walk(unit, now)
        if walk.error is not None and not isinstance(walk.error, RunBudgetError):
            self._note_lane_failure(unit, now)
        unit.error = unit.error or walk.error
        failure = None if unit.error is None else self._unit_failure(unit.error, now)
        self._note_quiet(unit, clean=imports and failure is None)
        if imports and failure is None:
            unit.outcome = STATUS_OK
            self._note_success(unit)
        else:
            assert failure is not None  # noqa: S101 - a walk that imports nothing failed
            self._note_failure(unit, failure, now, imported=imports)

    async def _async_cover_lanes(self, units: list[_Unit], now: datetime) -> None:
        """
        Ask for a few windows more of each lane's older history: its coverage.

        After every unit's walk, each unit whose walk went through in this
        update, and that has stored hours from its target on to go by, asks
        for at most 6 windows (``lanes.COVERAGE_WINDOWS``), but in the
        coordinator's first sync and for one whose walk was or is about to
        be its initial import (``_covers``): first
        the older dates still missing, newest first, then, during its
        monthly revision pass, the stored dates, for readings Meridian has
        revised since (``_coverage_spans``). The units take turns, a window
        each, so each gets some of the time budget, and the requests start
        2 to 5 s apart.
        A failure stops that unit's coverage for this update, and its
        window's dates wait before they are asked for again, each alone
        (``lanes.LaneState.fail``); after a gateway timeout, its property's
        other units stop too, as the property cools down. A rate limit and
        the end of the time budget stop every unit's coverage, and fail
        nothing: the update's own walks went through. A rate limit is not
        retried early all the same: no coverage runs until Meridian's wait
        has passed, doubled for each rate limit in a row, the next update
        waits as long, never less than its hour (``async_fetch_and_import``),
        and this one asks for no billing period (``_async_billing_period``),
        whose rate limit would fail it. A rejected
        session still fails the update. Each window is imported once
        fetched, so what came back is kept whatever comes next.

        A unit whose walk went through with nothing stored from its target
        on, as a grid export that never exported or a supply that ended
        before it, has nothing to complete: its lane reaches its target.
        """
        lanes = self._lanes_to_cover(units, now)
        if not lanes:
            return
        planned = {unit.cache_key: self._plan_lane(unit, now) for unit in lanes}
        asked: Counter[CacheKey] = Counter()
        stopped: set[str] = set()
        active = [unit for unit in lanes if planned[unit.cache_key]]
        self._pause = COVERAGE_PAUSE_SECONDS
        try:
            while active:
                for unit in list(active):
                    key = unit.cache_key
                    spans = (
                        self._coverage_spans(unit, now)
                        if key[0] not in stopped and asked[key] < COVERAGE_WINDOWS
                        else []
                    )
                    if not spans:
                        active.remove(unit)
                        continue
                    first, last, kind = spans[0]
                    asked[key] += 1
                    window = MeasurementWindow(
                        first, last, asked[key], max(planned[key], asked[key])
                    )
                    ended = await self._async_cover_window(unit, window, kind, now)
                    if ended == _STOP_ALL:
                        return
                    if ended == _STOP_PROPERTY:
                        stopped.add(key[0])
                    if ended != _GO_ON:
                        active.remove(unit)
        finally:
            self._pause = None
            for unit in lanes:
                self._settle_lane(unit, now, asked=asked[unit.cache_key])

    def _lanes_to_cover(self, units: list[_Unit], now: datetime) -> list[_Unit]:
        """
        Return the units whose lanes ask for older history in this update.

        None while a rate limit's wait lasts. A unit whose walk went through
        with nothing stored from its target on reaches its target.
        """
        for unit in units:
            if (
                unit.walk is not None
                and unit.outcome == STATUS_OK
                and not unit.lane_energy
            ):
                self._lanes[unit.cache_key].reaches_target = True
        if self._coverage_not_before is not None and now < self._coverage_not_before:
            return []
        return [unit for unit in units if self._covers(unit)]

    def _plan_lane(self, unit: _Unit, now: datetime) -> int:
        """
        Prepare a unit's lane for its coverage; return the windows it plans.

        Its dates before its target are forgotten, and its revision pass
        starts on its day of the month (``lanes.LaneState.start_revision``).
        """
        lane = self._lanes[unit.cache_key]
        lane.forget_before(unit.target.astimezone(_NZ).date())
        if lane.start_revision(_lane_key(unit.cache_key), now.astimezone(_NZ).date()):
            self._log_once(
                "revision_started",
                "Meridian Energy asks Meridian again for older usage history "
                "this month, a few weeks at each update, for readings it has "
                "revised since (direction=%s)",
                unit.cache_key[1],
            )
        return min(COVERAGE_WINDOWS, len(self._coverage_spans(unit, now)))

    def _covers(self, unit: _Unit) -> bool:
        """
        Return whether a unit's lane asks for older history in this update.

        Not in a coordinator's first sync, after setup, a restart or a
        reload: the entities have no state until it returns, and it has no
        billing period yet, whose start may be the lane's target; the next
        update asks. Then only a unit whose walk went through, imported or
        with nothing to import: a failed or degraded one has its own wait,
        and a deferred one has none. It needs stored hours from its target
        on, to tell what is missing; a unit without statistics has its
        initial import instead. Nor one whose walk was its initial import,
        which walked 90 days in this update, nor one whose initial import is
        queued for the next update (``_record_direction_fetch``), which
        would walk again the weeks the coverage asked for: their lanes ask
        at the update after it. Nor while its property cools down after a
        gateway timeout.
        """
        return (
            self._initial_refresh_complete
            and unit.walk is not None
            and unit.outcome == STATUS_OK
            and bool(unit.lane_energy)
            and unit.mode is not SyncMode.INITIAL
            and unit.cache_key not in self._pending_backfill
            and self._cooldown_until.get(unit.cache_key[0], 0.0) <= monotonic()
        )

    def _coverage_spans(
        self, unit: _Unit, now: datetime
    ) -> list[tuple[date, date, str]]:
        """
        Return the windows a unit's lane still asks for, newest first.

        As (first date, last date, kind): the missing dates, then those of
        a running revision pass (``lanes.plan_spans``), from the lane's
        target on, up to the date 14 days back, which the reconciliations
        revise from their cutoff on, the newest stored hour's date and the
        dates the unit's walk fetched in full in this update
        (``lanes.coverage_end``).
        """
        lane = self._lanes[unit.cache_key]
        frontier = self._frontier.get(unit.statistic_ids[0])
        end = self._coverage_end(unit, now)
        return plan_spans(
            lane,
            missing_days(
                lane,
                unit.lane_energy,
                target=unit.target,
                end=end,
                frontier=frontier,
                now=now,
            ),
            revision_days(lane, unit.lane_energy, target=unit.target, end=end),
            now=now,
        )

    def _coverage_end(self, unit: _Unit, now: datetime) -> date:
        """
        Return the first date a unit's lane does not ask for in this update.

        The earliest of the date after the one 14 days back, the date after
        the newest stored hour's and the first date the unit's walk fetched
        in full in this update (``lanes.coverage_end``).
        """
        walk = unit.walk
        fetched_from = None if walk is None else walk.completed_on
        if fetched_from is not None and local_midnight(fetched_from) < unit.since:
            # The walk kept nothing before its cutoff on its oldest date.
            fetched_from += _DAY
        if fetched_from is not None and unit.left_out_through is not None:
            # Nor did its import keep the rows of the dates it left out.
            fetched_from = max(fetched_from, unit.left_out_through + _DAY)
        return coverage_end(
            now, self._frontier.get(unit.statistic_ids[0]), fetched_from
        )

    async def _async_cover_window(
        self, unit: _Unit, window: MeasurementWindow, kind: str, now: datetime
    ) -> str:
        """
        Fetch and import one window of a unit's coverage; return how it ended.

        It is fetched as a walk's window is, its rows from the lane's target
        on, and a refusal of a window older than every stored hour marks the
        start of the history as a walk's does (``_async_fetch_or_refused``),
        that of a date asked for alone once the date before confirms it
        (``_async_confirm_refused_day``). A window that came back without
        its last date, or its last hour, while the history goes on after it,
        is checked with that date and the next (``_async_check_cover_edge``),
        and one without its first date, or its first hour, while the history
        may go on before it, with the date before and that date
        (``_async_check_cover_start``): a misread window fails.
        The oldest stored date counts as seen in full: the history may start
        on it after midnight, as a supply that started during the day, and
        Meridian may refuse a window that starts before the supply did, so
        a window ending on that date refused before any reading is asked for
        by its dates, which find the start (``_async_fetch_refused_days``).
        A window older than every stored reading that returns nothing logs
        no page line, which would give that date away. A rate limit keeps
        Meridian's wait for the update to honour (``_coverage_rate_limit``).

        Its dates are then fetched, but for those whose rows the stored-aware
        rules left out, or that hold stored hours Meridian returned no
        reading for, or whose import was not confirmed and that the replay
        did not write again in full (``_async_import_coverage``): those with
        an hour still missing, or that waited after a failure, wait again,
        so a hole Meridian no longer returns is not asked across at every
        update, and a date whose import was not confirmed stays so until a
        replay writes it again. An update cancelled during the import, as
        when its entry is unloaded, leaves its dates as not confirmed.
        """
        key = unit.cache_key
        direction = key[1]
        lane = self._lanes[key]
        days = _window_days(window)
        walk = _Walk(
            self._row_density.get(key, 1.0),
            frozenset(unit.lane_energy),
            has_statistics=True,
        )
        oldest = oldest_day(unit.lane_energy)
        assert oldest is not None  # noqa: S101 - only lanes with stored hours
        walk.oldest_seen = local_midnight(oldest + _DAY)
        request = _WalkRequest(
            account_number=unit.account_number,
            property_id=unit.property_data.id,
            direction=direction,
            position=unit.position,
            since=unit.target,
            now=now,
            log=_PageLog(
                mode=kind, direction=direction, position=unit.position, now=now
            ),
        )
        rows = WindowRows(window, since=unit.target, now=now)
        if kind == COVERAGE:
            self._log_once(
                "coverage_started",
                "Some older Meridian usage history may be missing, or where it "
                "starts is not known yet; each update asks for a few weeks of it, "
                "newest first, until it is complete back to 90 days, to the start "
                "of the billing period, or to where the history starts "
                "(direction=%s)",
                direction,
            )
        try:
            refused = await self._async_fetch_or_refused(walk, rows, request)
            if not refused:
                await self._async_check_cover_edge(unit, walk, rows, request)
                await self._async_check_cover_start(unit, walk, rows, request)
        except MeridianAuthenticationError:
            raise
        except RunBudgetError:
            return _STOP_ALL
        except MeridianRateLimitError as err:
            self._end_call_in_flight()
            self._coverage_rate_limit = err
            self._log_once(
                "coverage_rate_limited",
                "Meridian asked Home Assistant to wait; older usage history is "
                "asked for again by a later update",
            )
            return _STOP_ALL
        except (MeridianError, ValueError) as err:
            self._end_call_in_flight()
            self._count_cover_window(kind)
            lane.fail(days, now)
            self._log_once(
                "coverage_failed",
                "Meridian did not return some older usage history (%s); it is "
                "asked for again in about %d h, a day at a time (direction=%s)",
                _failure_reason(err),
                _hours_until(lane.bad[days[0]], now),
                direction,
            )
            return _STOP_PROPERTY if _is_gateway(err) else _STOP_LANE
        self._count_cover_window(kind)
        if refused:
            lane.refused_through = walk.start_after
            self._history_start = True
        try:
            async with self._async_uncounted():
                left_out = await self._async_import_coverage(
                    unit, rows, whole=not lane.unsaved.isdisjoint(days)
                )
        except asyncio.CancelledError:
            # As when an entry is unloaded while the replay waits to be
            # confirmed: its writes may be queued, and the shift after them
            # lost, so the entry keeps the dates for the next coordinator
            # (``_settle_lane``), which replays them in full.
            lane.fail(days, now, unsaved=True)
            raise
        except (ValueError, StatisticsWriteError) as err:
            lane.fail(days, now, unsaved=isinstance(err, StatisticsWriteError))
            self._log_once(
                "coverage_unsaved",
                "Older Meridian usage history could not be imported (%s); it is "
                "asked for again in about %d h (direction=%s)",
                _failure_reason(err),
                _hours_until(lane.bad[days[0]], now),
                direction,
            )
            return _STOP_LANE
        self._note_cover_window(unit, rows, left_out, kind, now)
        return _GO_ON

    def _note_cover_window(
        self,
        unit: _Unit,
        rows: WindowRows,
        left_out: set[date],
        kind: str,
        now: datetime,
    ) -> None:
        """
        Note what an imported coverage window showed, for its lane.

        Its dates count as fetched, and those with no reading as empty, but
        for those ``left_out`` (``_async_import_coverage``): of them, a date
        with an hour still missing, or that waited after a failure, waits
        again, and the others count as fetched, never as empty.
        """
        lane = self._lanes[unit.cache_key]
        days = _window_days(rows.window)
        readings = _days_of(start for start, _channel in rows.rows)
        frontier = self._frontier.get(unit.statistic_ids[0])
        retry = [
            day
            for day in days
            if day in left_out
            and (
                day in lane.bad
                or has_missing_hour(day, unit.lane_energy, unit.target, frontier)
            )
        ]
        lane.fail(retry, now)
        kept = [day for day in days if day not in retry]
        lane.took(
            kept,
            empty=[day for day in kept if day not in readings and day not in left_out],
            revised=kind != COVERAGE,
        )

    async def _async_check_cover_edge(
        self, unit: _Unit, walk: _Walk, rows: WindowRows, request: _WalkRequest
    ) -> None:
        """
        Check that a coverage window was not read without its last date.

        A walk checks its windows against the newer ones (``_Walk.check``);
        a coverage window has no newer window of its own, but the stored
        history after it shows what its end should hold. When its last hour
        came back with no reading although the hour after it is stored, or
        its last date with none at all although the history goes on after
        it, up to the frontier, and the date after is not known to have
        none, Meridian is asked for that date and the next together.
        Readings for the last date that the window did not return show that
        Meridian read its ``endOn`` as excluded, or its dates a day early:
        the window fails, so none of its dates counts as fetched or as
        empty, and they wait, then are asked for again. None confirm a gap,
        as a meter that sent nothing then, and the window is imported. So
        does a refusal of that request, as Meridian may refuse one that
        starts before the supply did (``_is_refusal``): it shows nothing of
        how Meridian read the window.
        """
        window = rows.window
        after = window.end
        frontier = self._frontier.get(unit.statistic_ids[0])
        if rows.last_hour or frontier is None or after > frontier:
            return
        if after not in unit.lane_energy and (
            rows.last_day or window.end_on + _DAY in self._lanes[unit.cache_key].empty
        ):
            return
        edge_rows = await self._async_fetch_cover_edge(
            walk, rows, request, window.end_on
        )
        if edge_rows is None:
            return
        last_date = local_midnight(window.end_on)
        returned = {key for key in rows.rows if key[0] >= last_date}
        if any(key[0] < after and key not in returned for key in edge_rows.rows):
            raise ValueError(
                "Meridian returned readings for the last date of a window only "
                "when asked for the date after it too"
            )

    async def _async_check_cover_start(
        self, unit: _Unit, walk: _Walk, rows: WindowRows, request: _WalkRequest
    ) -> None:
        """
        Check that a coverage window was not read without its first date.

        The walks check each boundary between two of their windows from both
        sides (``_Walk.check``); a coverage window's start has no older
        window of its own, so its first date is checked against what
        Meridian returns for it with the date before. When its first hour
        came back with no reading, though the window's rows reach it from
        the target on, or its first date with none at all, while the window
        returned readings on another date or holds that date alone, and
        the history may go on before it, Meridian is asked for the date
        before and that date together. The history may go on before it when
        the hour before it is stored, or when the date before is neither
        before the history's start, once known (``LaneState.history_start``),
        nor known to have no readings (``empty``): the window may start on
        the target's date, whose first hour a billing period's totals need.
        Readings for the first date that the window did not return show
        that Meridian read its ``startOn``, or the first hour of it, as
        excluded: the window fails, so none of its dates counts as fetched
        or as empty, and they wait, then are asked for again. None confirm a
        gap, or where the history starts, as a supply that started during
        that date, and the window is imported. So does a refusal of that
        request, as Meridian may refuse one that starts before the supply
        did (``_is_refusal``). An empty window of more than a day is not
        checked: reading its first date or hour as excluded would leave its
        other dates' readings, and it is what the weeks before a history's
        start return.
        """
        window = rows.window
        first_end = local_midnight(window.start_on + _DAY)
        kept_from = ceil_hour(max(request.since, window.start))
        if kept_from >= first_end or (not rows.inside and window.days > 1):
            return
        missed_hour = kept_from == window.start and not rows.first_hour
        if not missed_hour and any(key[0] < first_end for key in rows.rows):
            return
        lane = self._lanes[unit.cache_key]
        before = window.start_on - _DAY
        start = lane.history_start(oldest_day(unit.lane_energy))
        if window.start - _HOUR not in unit.lane_energy and (
            (start is not None and before < start) or before in lane.empty
        ):
            return
        edge_rows = await self._async_fetch_cover_edge(walk, rows, request, before)
        if edge_rows is None:
            return
        returned = {key for key in rows.rows if key[0] < first_end}
        if any(
            window.start <= key[0] < first_end and key not in returned
            for key in edge_rows.rows
        ):
            raise ValueError(
                "Meridian returned readings for the first date of a window only "
                "when asked for the date before it too"
            )

    async def _async_fetch_cover_edge(
        self, walk: _Walk, rows: WindowRows, request: _WalkRequest, first: date
    ) -> WindowRows | None:
        """
        Fetch a date of a coverage window's edge with the date after it.

        Return its rows, or None when Meridian refused the request as one
        that may start before the supply did (``_is_refusal``), which shows
        nothing of how it read the window. Any other failure is raised.
        """
        window = rows.window
        edge = MeasurementWindow(
            first, first + _DAY, index=window.index, count=window.count
        )
        edge_rows = WindowRows(edge, since=request.since, now=request.now)
        try:
            await self._async_walk_window(walk, edge_rows, request, walk.oldest_seen)
        except (MeridianGraphQLError, MeridianConnectionError) as err:
            if not _is_refusal(err, edge_rows, walk.oldest_seen):
                raise
            self._end_call_in_flight()
            return None
        return edge_rows

    def _count_cover_window(self, kind: str) -> None:
        """Count a window a lane's coverage asked for, by what it was for."""
        if kind == COVERAGE:
            self._coverage_windows += 1
        else:
            self._revision_windows += 1

    async def _async_import_coverage(
        self, unit: _Unit, rows: WindowRows, *, whole: bool = False
    ) -> set[date]:
        """
        Import the hours of one coverage window that are not stored as fetched.

        The stored-aware rules apply within the window: rows up to a stored
        hour Meridian did not return are left out, and nothing is imported
        when stored hours are newer than every row left (``importable``);
        nor the cost of hours Meridian returned without one where one is
        stored (``cost_plan``). Of the rest, only the hours whose usage or
        cost is not stored as fetched are given to the replay, which writes
        from the first of them to the last, behind newer stored rows, and
        shifts those to follow on (``statistics.async_replay``): a window
        that changed nothing writes nothing, and a manual correction outside
        its changed hours is kept. A window holding a date whose import Home
        Assistant did not confirm (``whole``) is replayed in full instead:
        the recorder may have saved its rows but not the shift of the rows
        after them, which no stored state shows, and a replay of the same
        hours heals both joins.

        Return the window's dates whose rows the rules left out, with those
        holding stored hours Meridian returned no reading for, as a walk's
        empty window over stored hours ends the walk: nothing shows they
        were fetched in full. With ``whole``, the dates whose stored hours
        the replay did not write again in full are among them too
        (``_replayed_days``), as one Meridian returned in part: a wrong step
        after any of their stored hours would be left as it is.
        """
        measurements = tuple(
            sorted(rows.rows.values(), key=lambda item: (item.start, item.channel_id))
        )
        window = rows.window
        days = _window_days(window)
        energy_id, money_id = unit.statistic_ids
        stored = StoredHours(
            energy=frozenset(
                hour for hour in unit.lane_energy if window.start <= hour < window.end
            ),
            cost=frozenset(
                hour for hour in unit.lane_cost if window.start <= hour < window.end
            ),
        )
        returned = {item.start.astimezone(_NZ).date() for item in measurements}
        plan = importable(measurements, stored)
        left_out = _left_out_days(plan, days) | (_days_of(stored.energy) - returned)
        if left_out:
            self._log_once(
                "coverage_left_out",
                "Meridian no longer returned some older readings Home Assistant has "
                "statistics for; the days they are on, and those before them, are "
                "asked for again later (direction=%s)",
                unit.cache_key[1],
            )
        if plan.tail_not_returned or not plan.measurements:
            return left_out
        changed: Collection[datetime]
        if whole:
            changes = plan.measurements
            changed = {item.start.astimezone(UTC) for item in changes}
        else:
            states = await async_stored_states(
                self.hass, {energy_id, money_id}, window.start, window.end
            )
            changed = changed_hours(
                plan.measurements, states.get(energy_id, {}), states.get(money_id, {})
            )
            changes = tuple(
                item
                for item in plan.measurements
                if item.start.astimezone(UTC) in changed
            )
        if not changes:
            return left_out
        # Whether Meridian still returns the stored costs is told by every
        # row it returned, not by the changed ones alone.
        cost = cost_plan(plan.measurements, stored.cost)
        energy_name, cost_name = _statistic_names(unit)
        await async_replay(
            self.hass,
            stat_energy_id=energy_id,
            stat_cost_id=money_id,
            energy_name=energy_name,
            cost_name=cost_name,
            measurements=changes,
            import_cost=not cost.skipped,
            cost_after=cost.after,
        )
        unit.lane_energy.update(changed)
        if whole:
            left_out |= set(days) - _replayed_days(days, changes, cost, stored)
        return left_out

    def _settle_lane(self, unit: _Unit, now: datetime, *, asked: int) -> None:
        """
        Note what a unit's lane covers after its coverage in this update.

        It reaches its target when no date is missing an hour Meridian may
        still have, and none waits after a failure with an hour missing or
        an import not confirmed (``lanes.reaches_target``): a revision pass
        still running, or a stored date whose revision failed, does not
        change that. Its history's start is found when Meridian showed
        where it is, which a revision pass asking again does not change
        either, and the dates before the start it asks from no longer wait,
        as none of them is asked for. A revision pass with no stored date
        left to ask for has ended. A lane that asked for windows and now
        reaches its target says so once.
        """
        lane = self._lanes[unit.cache_key]
        lane.forget_bad_before(first_day(lane, unit.lane_energy, unit.target))
        spans = self._coverage_spans(unit, now)
        if not any(kind != COVERAGE for _first, _last, kind in spans):
            lane.revising = False
        reached = lane.reaches_target
        lane.reaches_target = reaches_target(
            lane,
            unit.lane_energy,
            target=unit.target,
            end=self._coverage_end(unit, now),
            frontier=self._frontier.get(unit.statistic_ids[0]),
        )
        lane.history_start_found = (
            lane.history_start(oldest_day(unit.lane_energy), known=True) is not None
        )
        if asked and lane.reaches_target and not reached:
            self._log_once(
                "coverage_complete",
                "Meridian usage history for one direction is now complete back "
                "to 90 days, to the start of the billing period, or to where the "
                "history starts (direction=%s)",
                unit.cache_key[1],
            )

    def _keep_unsaved(self, units: list[_Unit], now: datetime) -> None:
        """
        Hand each lane's dates whose import was not confirmed to the owner.

        At the end of every sync, however it ended, a cancellation during a
        coverage import included: the coordinator after a reload takes them
        over (``unsaved``) and replays them in full too, and one that a walk
        or a window has confirmed since is no longer handed over.
        """
        for unit in units:
            lane = self._lanes[unit.cache_key]
            if lane.unsaved:
                self._unsaved_coverage[unit.cache_key] = lane.unsaved_retries(now)
            else:
                self._unsaved_coverage.pop(unit.cache_key, None)

    def _note_lane_walk(self, unit: _Unit, now: datetime) -> None:
        """
        Remember what a unit's imported walk showed of its history.

        Each date of a window it fetched in full that is older than the
        newest stored reading's: whether it returned no reading, for the
        start of the history (``lanes.LaneState.history_start``), and, for
        one older than the 14 days the reconciliations revise, that it was
        fetched, so the lane's coverage does not ask for an hour Meridian
        does not have again, and that it no longer waits after a failure,
        nor after an import Home Assistant did not confirm
        (``lanes.LaneState.settle``): only a walk that reached back more
        than 14 days fetches such a date. That last only when its replay,
        confirmed, wrote every stored hour of the date again
        (``_replayed_days``): one older than the replay's first hour, as
        before a gap the walk stopped at, keeps its wait, and a wrong step
        an unconfirmed import may have left after it is still written again.
        A newer date may still be published, and the walk kept nothing
        before its cutoff on the date it starts on, nor its import the rows
        of the dates the stored-aware rules left out
        (``_Unit.left_out_through``), which the coverage asks for again;
        nor does a date holding stored hours that returned no reading count
        as fetched or empty, nor any date of the empty window over stored
        hours that ended the walk: nothing shows Meridian read them in full.
        And a refusal of dates before the supply started, where the walk
        found the history's start that way.
        """
        walk = unit.walk
        assert walk is not None  # noqa: S101 - only fetched units finish
        lane = self._lanes.setdefault(unit.cache_key, LaneState())
        if walk.walk.start_after is not None:
            lane.refused_through = walk.walk.start_after
        frontier = self._frontier.get(unit.statistic_ids[0])
        if frontier is None:
            return
        revised_from = (now - REVISION_OVERLAP).astimezone(_NZ).date()
        noted = _walk_days(unit, frontier.astimezone(_NZ).date())
        stored_days = _days_of(unit.stored.energy)
        measurements, cost = unit.replayed or ((), CostPlan())
        replayed = _replayed_days(
            [day for day, _readings in noted if day < revised_from],
            measurements,
            cost,
            unit.stored,
        )
        for day, readings in noted:
            if readings:
                lane.empty.discard(day)
                lane.known_empty.discard(day)
            elif day in stored_days:
                continue
            else:
                lane.empty.add(day)
            if day < revised_from:
                lane.fetched.add(day)
                if day in replayed:
                    lane.settle([day])

    def _note_lane_failure(self, unit: _Unit, now: datetime) -> None:
        """
        Make the dates of the window a unit's walk failed at wait, for its lane.

        The walk's window, not the last request: a refused window's dates are
        asked for alone, and the date before a refused one may come back,
        failing the window (``_async_fetch_refused_days``). Only those older
        than the 14 days the reconciliations revise: the lane's coverage asks
        for no newer one. They wait 1 h, then longer, before it asks for
        them, each alone.
        """
        walk = unit.walk
        assert walk is not None  # noqa: S101 - only fetched units finish
        failing = walk.walk.failing
        if failing is None:
            return
        revised_from = (now - REVISION_OVERLAP).astimezone(_NZ).date()
        days = [day for day in _window_days(failing.window) if day < revised_from]
        if days:
            self._lanes.setdefault(unit.cache_key, LaneState()).fail(days, now)

    @asynccontextmanager
    async def _async_uncounted(self) -> AsyncIterator[None]:
        """
        Leave the time a unit's import takes out of the sync's time budget.

        That time is Home Assistant's: the replay waits for the recorder to
        save and confirm its rows, up to 90 s behind a busy recorder (a
        backup, say), and up to 60 s before that when the last replay of
        its statistics was not confirmed. Counted, it would leave too little
        of the budget for the windows other units still have to fetch, which
        would then fail as if Meridian had been slow (``time_budget``). The
        setup attempt's own time limit (``time_limit``) moves on with the
        budget: while the import runs, it leaves at least
        ``REPLAY_ALLOWANCE_SECONDS``, so it does not cut short a replay that
        started near its end, and then it is as far off as it was before the
        import. That limit runs on the event loop's clock, so the import is
        timed by that clock for it.
        """
        started = monotonic()
        loop = asyncio.get_running_loop()
        loop_started = loop.time()
        limit = self._time_limit
        ends = None if limit is None or limit.expired() else limit.when()
        if limit is not None and ends is not None:
            limit.reschedule(max(ends, loop_started + REPLAY_ALLOWANCE_SECONDS))
        try:
            yield
        finally:
            took = monotonic() - started
            self._uncounted += took
            if self._deadline is not None:
                self._deadline += took
            if limit is not None and ends is not None and not limit.expired():
                limit.reschedule(ends + loop.time() - loop_started)

    def _note_failure(
        self, unit: _Unit, failure: UnitFailure, now: datetime, *, imported: bool
    ) -> None:
        """
        Record a unit that failed: degraded when it imported its newer windows.

        A degraded unit whose owed reconciliation imported nothing stays
        degraded: the reconciliation waits longer, and the hourly update,
        which has kept working, goes on meanwhile. When that hourly update
        fails, the unit fails and waits 1 h, then longer, counting its
        failed hourly updates alone (``UnitState.fail_hourly``): the
        reconciliation keeps its own count and wait. Any other unit that
        imported nothing has failed, and waits whole. Until a first sync has
        succeeded, its own retries are spaced out and try every unit, so no
        failure counts towards a unit's wait: that starts at an hour once a
        first sync has succeeded. A failed unit that imported its newer
        windows works again, degraded, and says so as a recovered one does.
        """
        state = unit.state
        if not self._initial_refresh_complete:
            state.failures = state.hourly_failures = 0
        if imported:
            unit.outcome = STATUS_DEGRADED
            recovered = state.status == STATUS_FAILED
            state.degrade(now, failure, unit.mode)
            assert state.retry_at is not None  # noqa: S101 - just set
            self._log_once(
                "degraded",
                "Meridian's older usage history did not come back (%s); this update "
                "imported the newer days it fetched, and asks for the rest again in "
                "about %d h, while later updates fill in older days still missing "
                "(direction=%s)",
                failure.reason,
                _hours_until(state.retry_at, now),
                unit.cache_key[1],
            )
            self._note_recovered(unit, recovered=recovered)
            return
        unit.outcome = STATUS_FAILED
        if state.hourly_goes_on and unit.mode is SyncMode.TIP:
            if state.fail_hourly(now, failure):
                self._report_failing(unit)
        elif state.hourly_goes_on:
            state.defer_reconciliation(now, failure, unit.mode)
            assert state.retry_at is not None  # noqa: S101 - just set
            unsaved = _RECONCILIATION_UNSAVED.get(failure.reason)
            if unsaved is None:
                self._log_once(
                    "reconciliation_failed",
                    "Meridian's usage history did not come back for a "
                    "reconciliation (%s); this update imported nothing for this "
                    "direction, its hourly updates go on, and the reconciliation "
                    "is asked for again in about %d h (direction=%s)",
                    failure.reason,
                    _hours_until(state.retry_at, now),
                    unit.cache_key[1],
                )
            else:
                # Meridian's history came back: Home Assistant did not confirm
                # the replay, or it refused to write.
                self._log_once(
                    f"reconciliation_{failure.reason}",
                    "%s (%s); this update imported nothing for this direction, "
                    "its hourly updates go on, and the reconciliation is tried "
                    "again in about %d h (direction=%s)",
                    unsaved,
                    failure.reason,
                    _hours_until(state.retry_at, now),
                    unit.cache_key[1],
                )
        elif state.fail(now, failure, unit.mode):
            self._report_failing(unit)

    def _note_success(self, unit: _Unit) -> None:
        """
        Record a unit that fetched and imported in full; say if it recovered.

        An hourly update while a degraded unit's reconciliation waits leaves
        it degraded, owing that reconciliation (``UnitState.covers``); one
        whose hourly update had failed since is degraded again.
        """
        state = unit.state
        recovered = (
            state.succeed() if state.covers(unit.mode) else state.recover_hourly()
        )
        self._note_recovered(unit, recovered=recovered)

    def _note_recovered(self, unit: _Unit, *, recovered: bool) -> None:
        """
        Say once that a unit that failed or was degraded works again.

        Not for one that failed earlier in this setup import, before the
        accounts were refreshed: that failure never warned (``_unreported``).
        """
        if unit.cache_key in self._unreported:
            self._unreported.discard(unit.cache_key)
        elif recovered:
            _LOGGER.info(
                "Meridian usage history updates for one direction are working "
                "again (direction=%s account=%s property=%s)",
                unit.cache_key[1],
                _place(*unit.position[:2]),
                _place(*unit.position[2:]),
            )

    def _report_failing(self, unit: _Unit) -> None:
        """
        Warn about a unit that started failing, at once or once setup goes on.

        An hourly update warns at once, so a rate limit or a refresh of the
        accounts that ends or restarts the sync later cannot lose it. The
        setup import warns only once the import is known to go on, since a
        setup attempt that fails logs its own (``_settle_run``); until then
        the unit is remembered for the whole sync, a refresh of the
        accounts included.
        """
        if self._setup_import:
            self._unreported.add(unit.cache_key)
        else:
            self._warn_unit_failing(unit, setup_import=False)

    def _note_quiet(self, unit: _Unit, *, clean: bool) -> None:
        """
        Remember whether a unit's walk found a reading newer than its frontier.

        A walk that ended without a failure and found none newer than the
        frontier it started from makes the unit quiet at the frontier it
        leaves (``_quiet``); one that found any, imported or not, ends that,
        so the next walk reaches back to the frontier again (``_Unit.found_new``).
        Readings Meridian publishes more than a week late, for days older
        than the newest window, so keep coming at every update.
        """
        frontier = self._frontier.get(unit.statistic_ids[0])
        if frontier is None or unit.found_new(unit.fetch.measurements):
            unit.state.quiet_frontier = None
        elif clean:
            unit.state.quiet_frontier = frontier

    def _warn_unit_failing(self, unit: _Unit, *, setup_import: bool) -> None:
        """
        Log one WARNING for a unit that started failing, without identifiers.

        After a setup import, the entry's first update tries the unit again,
        which then logs no second warning while it still fails: setup hands
        it the unit's state (``SyncMemory``). Until a first sync after a
        restart or a reload has succeeded, each of its attempts tries every
        unit again, 10 minutes after the first failed, or with the hourly
        update once one succeeded: the next update, either way.
        """
        failure = unit.state.last_failure
        retry_at = unit.state.next_try
        assert failure is not None  # noqa: S101 - set when the unit failed
        assert retry_at is not None  # noqa: S101 - set when the unit failed
        if setup_import:
            when = "by the first update after setup"
        elif not self._initial_refresh_complete:
            when = "by the next update"
        else:
            when = f"in about {_hours_until(retry_at, failure.at)} h"
        _LOGGER.warning(
            "Meridian usage history updates are failing for one direction "
            "(direction=%s account=%s property=%s reason=%s status=%s); the other "
            "directions go on, and this one is tried again %s",
            unit.cache_key[1],
            _place(*unit.position[:2]),
            _place(*unit.position[2:]),
            failure.reason,
            "-" if failure.http_status is None else failure.http_status,
            when,
        )

    @staticmethod
    def _unit_failure(err: BaseException, now: datetime) -> UnitFailure:
        """Describe why a unit failed, for diagnostics and a waiting sync."""
        return UnitFailure(
            reason=_failure_reason(err),
            cause=type(err).__name__,
            http_status=http_status(err),
            at=now,
            detail=err.detail if isinstance(err, MeridianUpstreamError) else None,
        )

    def _property_result(
        self,
        account_number: str,
        property_data: MeridianProperty,
        units: list[_Unit],
        mode: SyncMode,
    ) -> PropertySyncResult:
        """Return one property's sync summary, from its units and its cache."""
        key = property_key(account_number, property_data.id)
        synced = {
            unit.cache_key[1]: _DirectionSync(unit.fetch, unit.rows, unit.since)
            for unit in units
            if unit.cache_key[0] == key
        }
        consumption = synced[READING_CONSUMPTION]
        generation = synced.get(READING_GENERATION, _DirectionSync.empty(consumption))

        consumption_cache = self._measurement_cache.get((key, READING_CONSUMPTION), {})
        qualities = Counter(item.quality for item in consumption_cache.values())
        provisional = sorted(
            item.start.astimezone(UTC)
            for item in consumption_cache.values()
            if item.quality != _ACTUAL
        )
        observed_density = max(
            consumption.fetch.observed_rows_per_hour,
            generation.fetch.observed_rows_per_hour,
        )
        return PropertySyncResult(
            property_key=key,
            account_key=account_key(account_number),
            consumption_rows=consumption.rows,
            generation_rows=generation.rows,
            latest_reading=max(
                ((item.end or item.start) for item in consumption_cache.values()),
                default=None,
            ),
            estimated_rows=len(provisional),
            sync_mode=mode,
            requested_since=min(consumption.since, generation.since),
            consumption_pages=consumption.fetch.pages,
            generation_pages=generation.fetch.pages,
            consumption_received_rows=consumption.fetch.received_rows,
            generation_received_rows=generation.fetch.received_rows,
            consumption_retained_rows=len(consumption.fetch.measurements),
            generation_retained_rows=len(generation.fetch.measurements),
            oldest_estimated=provisional[0] if provisional else None,
            newest_estimated=provisional[-1] if provisional else None,
            quality_counts=tuple(sorted(qualities.items())),
            observed_rows_per_hour=observed_density,
            consumption_walk=consumption.fetch.walk,
            generation_walk=generation.fetch.walk,
        )

    async def _async_account_results(
        self, accounts: tuple[MeridianAccount, ...], now: datetime
    ) -> tuple[AccountSyncResult, ...]:
        """Build account-scoped billing totals from imported HA statistics."""
        results: list[AccountSyncResult] = []
        for account_index, account in enumerate(accounts, start=1):
            billing_period = await self._async_billing_period(
                account.number, now, position=_Position(account_index, len(accounts))
            )
            has_feed_in = any(
                meter.has_feed_in
                for property_data in account.properties
                for meter in property_data.meter_points
            )
            usage = cost = exported = credit = None
            usage_complete = False
            cost_complete = False
            export_complete = False
            credit_complete = False
            if (
                self.hass.state is CoreState.running
                and billing_period is not None
                and billing_period.start is not None
                and billing_period.end is not None
            ):
                start = _local_day_start(billing_period.start)
                end = min(_local_day_start(billing_period.end + timedelta(days=1)), now)
                if start < end:
                    totals = await async_account_period_totals(
                        self.hass,
                        property_keys=tuple(
                            property_key(account.number, item.id)
                            for item in account.properties
                        ),
                        feed_in_keys=tuple(
                            property_key(account.number, item.id)
                            for item in account.properties
                            if any(meter.has_feed_in for meter in item.meter_points)
                        ),
                        start=start,
                        end=end,
                    )
                    usage = totals.usage
                    cost = totals.cost
                    exported = totals.export
                    credit = totals.credit
                    usage_complete = totals.usage_complete
                    cost_complete = totals.cost_complete
                    export_complete = totals.export_complete
                    credit_complete = totals.credit_complete
            results.append(
                AccountSyncResult(
                    account_key=account_key(account.number),
                    billing_period=billing_period,
                    current_bill_usage=usage,
                    current_bill_cost=cost,
                    current_bill_export=exported,
                    current_bill_credit=credit,
                    has_feed_in=has_feed_in,
                    usage_complete=usage_complete,
                    cost_complete=cost_complete,
                    export_complete=export_complete,
                    credit_complete=credit_complete,
                )
            )
        return tuple(results)

    async def _async_billing_period(
        self,
        account_number: str,
        now: datetime,
        *,
        position: _Position | None = None,
    ) -> MeridianBillingPeriod | None:
        """
        Return cached current billing metadata without blocking energy syncs.

        A cached period is asked for again once a day old, or once it has
        ended, but not in an update whose coverage met a rate limit: that
        update keeps what is cached, and withholds a period that has ended,
        as when the request fails, rather than ask Meridian again inside
        the wait it has just set (``_async_cover_lanes``).
        """
        cached_at = self._billing_cached_at.get(account_number)
        cached = self._billing_periods.get(account_number)
        local_today = now.astimezone(_NZ).date()
        expired = cached_at is None or now - cached_at >= BILLING_CACHE_INTERVAL
        period_ended = (
            cached is not None and cached.end is not None and (local_today > cached.end)
        )
        if cached is not None and not expired and not period_ended:
            return cached
        if self._coverage_rate_limit is not None:
            # Meridian has just asked to wait, in this update's coverage:
            # the update after the wait asks for the period again.
            return None if period_ended else cached
        call = self._note_call("billing", position=position)
        try:
            billing = await self.client.async_get_billing_period(account_number)
        except MeridianRateLimitError:
            raise
        except (MeridianConnectionError, MeridianGraphQLError, ValueError) as err:
            self._note_call_done(call)
            was_available = not self._billing_unavailable
            self._billing_unavailable.add(account_number)
            if was_available:
                _LOGGER.info(
                    "Meridian billing metadata is unavailable; %s (%s)",
                    (
                        "withholding expired cached data"
                        if period_ended
                        else "retaining current cached data"
                    ),
                    type(err).__name__,
                )
            return None if period_ended else cached
        self._note_call_done(call)
        if account_number in self._billing_unavailable:
            self._billing_unavailable.discard(account_number)
            if not self._billing_unavailable:
                _LOGGER.info("Meridian billing metadata is available again")
        self._billing_periods[account_number] = billing
        self._billing_cached_at[account_number] = now
        return billing

    async def _async_import_unit(
        self, unit: _Unit, fetch: MeasurementFetchResult, now: datetime
    ) -> tuple[MeasurementFetchResult, int]:
        """
        Import what a unit fetched, as far as it can be imported safely.

        Every import is one replay (``statistics.async_replay``), from the
        earliest hour that changed to the newest: hours it was not given
        keep their stored state, and stored rows after it are shifted to
        follow on. A direction whose energy statistic has stored rows reads
        those rows, and its cost statistic's, once: the walk stops at an
        empty window over stored hours, and the stored-aware rules leave out
        rows Meridian no longer returns as before (``windows.importable``),
        or their cost alone (``windows.cost_plan``). A cutoff more than 14
        days back, as the frontier rule gives after a long outage, is beyond
        what the measurement cache compares: the fetched block is then
        replayed whole, from its oldest hour, and so is the weekly
        reconciliation's window. The merge works on a copy of the cache,
        which replaces it only once the replay has confirmed its writes, so
        a replay that raises leaves the rows to be offered again: the next
        sync replays them from the same hour, which heals what the recorder
        saved in part. A replay that reached back more than 14 days starts
        at its walk's cutoff instead, which the next sync works out later:
        its oldest hour is remembered, and the next walks reach back to it
        (``_note_unconfirmed``), as it is when the sync is cancelled while
        the replay runs, as by an unload of its entry, which leaves its
        writes queued. Nothing else that records what was imported
        (the frontier, the newest hours imported, a finished initial import)
        moves on before that either.

        Return the fetch, marked when it was cut at a gap, and the rows
        written.
        """
        cache_key = unit.cache_key
        direction = cache_key[1]
        energy_id, money_id = unit.statistic_ids
        stored = unit.stored
        plan = importable(fetch.measurements, stored)
        unit.left_out_through = _left_out_through(plan, now)
        if plan.cut_at_hole:
            fetch = replace(fetch, walk=replace(fetch.walk, stopped_at_gap=True))
            self._log_once(
                "stored_hole",
                "Meridian's history has a gap where Home Assistant has statistics; "
                "this update imports only the readings after it (direction=%s)",
                direction,
            )
        rows = 0
        if plan.tail_not_returned:
            self._log_once(
                "tail_not_returned",
                "Meridian no longer returned the newest readings Home Assistant has "
                "statistics for; this update imports none for this direction "
                "(direction=%s)",
                direction,
            )
        else:
            staged = dict(self._measurement_cache.get(cache_key, {}))
            deep = unit.mode is SyncMode.INITIAL or unit.since < now - REVISION_OVERLAP
            merged = self._merge_measurements(
                cache_key,
                plan.measurements,
                now,
                initial_import=deep,
                whole_window=unit.mode is SyncMode.FULL_RECONCILIATION,
                cache=staged,
            )
            if merged:
                cost = cost_plan(merged, stored.cost)
                if cost.skipped:
                    self._log_once(
                        "cost_skipped",
                        "Meridian no longer returned the cost of the newest hours "
                        "Home Assistant has statistics for; this update imports "
                        "usage only for this direction (direction=%s)",
                        direction,
                    )
                elif cost.after is not None:
                    self._log_once(
                        "cost_cut",
                        "Meridian returned readings without a cost Home Assistant "
                        "already has statistics for; this update imports their "
                        "usage, and the cost only of the hours after them "
                        "(direction=%s)",
                        direction,
                    )
                self._uncache_costs(staged, merged, cost)
                energy_name, cost_name = _statistic_names(unit)
                try:
                    written = await async_replay(
                        self.hass,
                        stat_energy_id=energy_id,
                        stat_cost_id=money_id,
                        energy_name=energy_name,
                        cost_name=cost_name,
                        measurements=merged,
                        import_cost=not cost.skipped,
                        cost_after=cost.after,
                    )
                except StatisticsAnchorError:
                    raise
                except StatisticsWriteError, asyncio.CancelledError:
                    # Cancelled too, as when the entry is unloaded while the
                    # replay waits to be confirmed: its writes may be queued.
                    if deep:
                        self._note_unconfirmed(cache_key, merged)
                    raise
                rows = written[0]
                unit.replayed = (merged, cost)
                self._note_imported(unit.statistic_ids, merged, written, cost)
                self._note_confirmed(unit)
                # Its lane's coverage goes by these hours too.
                unit.lane_energy.update(item.start.astimezone(UTC) for item in merged)
            self._measurement_cache[cache_key] = staged
        self._record_direction_fetch(
            cache_key,
            unit.mode,
            fetch.measurements,
            energy_id,
            money_id,
            complete=unit.walk is not None and unit.walk.complete,
        )
        if (
            unit.mode is SyncMode.INITIAL
            and not fetch.measurements
            and not fetch.walk.stopped_at_gap
            and unit.walk is not None
            and unit.walk.complete
        ):
            self._log_no_history(
                fetch.walk, direction, round((now - unit.since) / _DAY)
            )
        return fetch, rows

    def _log_no_history(self, walk: WindowWalk, direction: str, days: int) -> None:
        """
        Say that an initial import found no readings.

        At INFO, once a direction for each coordinator, when its walk
        searched every window to the cutoff. One that stopped at a window
        Meridian refused has said so in its own INFO line: it logs at DEBUG.
        """
        if walk.history_start:
            _LOGGER.debug(
                "No Meridian usage history was found in the %d windows requested "
                "(direction=%s)",
                walk.windows_requested,
                direction,
            )
            return
        self._log_once(
            f"no_history_{direction}",
            "No Meridian usage history was found in the last %d days (direction=%s)",
            days,
            direction,
        )

    async def _async_stored_hours(
        self, energy_id: str, money_id: str, since: datetime, now: datetime
    ) -> StoredHours:
        """Read the hours a direction's statistics have stored, once per sync."""
        if not self._known_statistics.get(energy_id, False):
            return StoredHours()
        stored = await async_stored_hours(
            self.hass, {energy_id, money_id}, since - _HOUR, now
        )
        return StoredHours(
            energy=stored.get(energy_id, frozenset()),
            cost=stored.get(money_id, frozenset()),
        )

    def _billing_start(self, account_number: str, now: datetime) -> date | None:
        """
        Return the start of an account's current billing period, if known.

        The period cached by an earlier update: the first sync after a
        start-up has none yet, and asks for no coverage (``_covers``), so the
        lanes reach back 90 days only while the request for it fails. A
        period that has ended is no longer current.
        """
        period = self._billing_periods.get(account_number)
        if period is None or (
            period.end is not None and now.astimezone(_NZ).date() > period.end
        ):
            return None
        return period.start

    def _log_once(self, cause: str, message: str, *args: object) -> None:
        """Log a cause at INFO the first time this coordinator meets it, then DEBUG."""
        level = logging.DEBUG if cause in self._reported else logging.INFO
        self._reported.add(cause)
        _LOGGER.log(level, message, *args)

    def _note_imported(
        self,
        statistic_ids: tuple[str, str],
        measurements: tuple[MeridianMeasurement, ...],
        written: tuple[int, int],
        cost: CostPlan,
    ) -> None:
        """
        Remember the newest hour written to an energy statistic, and to its cost.

        Each moves its statistic's frontier on too, once the import has
        returned: the frontier is the newest hour stored. The importer
        writes an hour's cost only when every row of the hour has one, and
        only for the hours the cost plan writes.
        """
        energy_id, money_id = statistic_ids
        rows, cost_rows = written
        if rows:
            newest = max(item.start.astimezone(UTC) for item in measurements)
            self._imported_newest[energy_id] = newest
            self._advance_frontier(energy_id, newest)
        if cost_rows:
            without_cost = {
                item.start.astimezone(UTC)
                for item in measurements
                if item.cost_cents is None
            }
            costed = {
                start
                for item in measurements
                if (start := item.start.astimezone(UTC)) not in without_cost
                and cost.writes(start)
            }
            if costed:
                self._advance_frontier(money_id, max(costed))

    def _advance_frontier(self, statistic_id: str, newest: datetime) -> None:
        """Move a statistic's frontier on to an hour just written, never back."""
        frontier = self._frontier.get(statistic_id)
        self._frontier[statistic_id] = (
            newest if frontier is None else max(frontier, newest)
        )

    def _note_unconfirmed(
        self, cache_key: CacheKey, measurements: tuple[MeridianMeasurement, ...]
    ) -> None:
        """
        Remember the oldest hour a deep replay that was not confirmed wrote.

        A replay that reaches back more than 14 days, as an initial import
        or one after a long outage, starts at its walk's cutoff, which the
        next run works out from its own time, later. The recorder may have
        saved one statistic of the replay and not the other, as when it
        drops an import for good: had the retry started later, the hours
        between would keep the usage with no cost, or the cost with no
        usage, since nothing later reaches back that far. So the retry, and
        every walk until a replay from that hour is confirmed, reaches back
        to it (``_requested_since``), in this coordinator and in those its
        owner builds next with the same mapping (``unconfirmed``): the
        setup flow's next attempt, or the entry's after a reload. A restart
        of Home Assistant forgets it. A replay within the last 14 days needs
        none of this: the cache it compares with has not moved on, so the
        next run offers the same rows from the same hour.
        """
        first = min(item.start.astimezone(UTC) for item in measurements)
        self._unconfirmed[cache_key] = min(
            first, self._unconfirmed.get(cache_key, first)
        )

    def _note_confirmed(self, unit: _Unit) -> None:
        """
        Forget a unit's unconfirmed hour once a replay from it is confirmed.

        Only a walk that reached back to that hour and fetched every window
        does: one that stopped early imported its newer windows only.
        """
        unconfirmed = self._unconfirmed.get(unit.cache_key)
        if (
            unconfirmed is not None
            and unit.since <= unconfirmed
            and unit.walk is not None
            and unit.walk.complete
        ):
            del self._unconfirmed[unit.cache_key]

    async def _direction_mode(
        self,
        cache_key: CacheKey,
        statistic_ids: tuple[str, str],
        requested_mode: SyncMode,
        *,
        now: datetime | None = None,
    ) -> SyncMode:
        """
        Return the mode a direction syncs in this sync.

        A sync passes its ``now``, so a direction's mode, and so its windows,
        are the same when its progress is planned and when it is fetched.
        """
        for statistic_id in statistic_ids:
            await self._async_cache_statistic(statistic_id)
        energy_statistic_id = statistic_ids[0]
        if cache_key in self._pending_backfill:
            return SyncMode.INITIAL
        if (
            not self._known_statistics[energy_statistic_id]
            and cache_key not in self._backfill_attempted
        ):
            return SyncMode.INITIAL
        energy_latest = self._latest_statistics[energy_statistic_id]
        if self._initial_refresh_complete:
            return requested_mode
        if (
            energy_latest is not None and self._reconciled_at is not None
        ) or self._searched_by_setup(cache_key):
            # After setup, a direction with statistics continues with the
            # mode the cadence asks for (``_async_startup_mode``), even one
            # whose readings ended more than 14 days ago: the frontier rule
            # reaches back to its newest stored hour, so a new 90-day import
            # would only repeat what setup just did. So does one setup
            # searched in full and found empty: readings that come later
            # queue its initial import (``_record_direction_fetch``).
            return self._cadence_mode(now or _utcnow())
        if energy_latest is None or energy_latest < (now or _utcnow()) - (
            REVISION_OVERLAP
        ):
            return SyncMode.INITIAL
        return SyncMode.RESTART

    def _searched_by_setup(self, cache_key: CacheKey) -> bool:
        """
        Return whether the setup import just searched a direction in full.

        Setup hands over the directions whose initial import searched every
        window it planned (``SyncMemory.searched``), with when it ran
        (``reconciled_at``). A restart or a reload has no such record, so it
        searches a direction without statistics again, as it cannot tell it
        from one no setup searched, such as an account added later.
        """
        return self._reconciled_at is not None and cache_key in self._backfill_attempted

    async def _async_cache_statistic(self, statistic_id: str) -> None:
        """Cache whether Recorder has current numeric history for a statistic."""
        if statistic_id in self._known_statistics:
            return
        latest = await async_latest_numeric_statistic_start(self.hass, statistic_id)
        self._latest_statistics[statistic_id] = latest
        self._known_statistics[statistic_id] = latest is not None
        if latest is not None:
            self._frontier.setdefault(statistic_id, latest)

    def _record_direction_fetch(
        self,
        cache_key: CacheKey,
        mode: SyncMode,
        measurements: tuple[MeridianMeasurement, ...],
        energy_statistic_id: str,
        money_statistic_id: str,
        *,
        complete: bool = True,
    ) -> None:
        """
        Track attempted backfills separately from observable numeric history.

        An initial import whose walk is ``complete`` is done, and so is one
        that stopped early but imported its newer windows, as a degraded
        unit does: its lane's coverage asks for the older days, a few weeks
        at each update, rather than the 90 days being walked again. One that
        stopped before any reading leaves the backfill pending.
        """
        energy_was_known = self._known_statistics.get(energy_statistic_id, False)
        money_was_known = self._known_statistics.get(money_statistic_id, False)
        energy_observed = bool(measurements)
        money_observed = any(item.cost_cents is not None for item in measurements)

        if mode is SyncMode.INITIAL and (complete or energy_observed):
            self._backfill_attempted.add(cache_key)
            self._pending_backfill.discard(cache_key)
        elif (energy_observed and not energy_was_known) or (
            money_observed and not money_was_known
        ):
            self._pending_backfill.add(cache_key)

        if energy_observed:
            self._known_statistics[energy_statistic_id] = True
            self._latest_statistics[energy_statistic_id] = max(
                item.start.astimezone(UTC) for item in measurements
            )
        if money_observed:
            self._known_statistics[money_statistic_id] = True
            self._latest_statistics[money_statistic_id] = max(
                item.start.astimezone(UTC)
                for item in measurements
                if item.cost_cents is not None
            )

    def _requested_since(
        self, cache_key: CacheKey, mode: SyncMode, now: datetime
    ) -> datetime:
        """
        Return the cutoff a unit's walk reaches back to.

        Every mode reaches back to the frontier too, the newest stored
        energy hour, up to the 90 days of an initial import: after an outage
        longer than the mode's own span, the walk still joins the new rows to
        the stored history, with no hours missing between. Meridian may
        publish an hour's cost after its usage, so the newest stored cost
        hour can be older than that frontier: a walk the rule moves back
        reaches back to it as well, at most 48 h before the energy frontier,
        or the hours between would keep no cost for good, as no later
        update reaches back that far. After a replay that reached back more
        than 14 days and was not confirmed, every walk of the unit, whatever
        its mode, reaches back to the oldest hour it wrote, even more than
        90 days back (``_note_unconfirmed``): a new coordinator, as after a
        failed setup attempt or a reload, reads the frontier from the
        recorder, which usage saved without its cost has moved on.
        """
        since = self._mode_since(cache_key, mode, now)
        energy_id, money_id = _statistic_ids(cache_key)
        frontier = self._frontier.get(energy_id)
        if frontier is not None and frontier < since:
            cost_frontier = self._frontier.get(money_id)
            if cost_frontier is not None and cost_frontier < frontier:
                frontier = max(cost_frontier, frontier - COST_LAG_MARGIN)
            since = max(now - INITIAL_BACKFILL, frontier)
        if (unconfirmed := self._unconfirmed.get(cache_key)) is not None:
            since = min(since, unconfirmed)
        return since

    def _mode_since(
        self, cache_key: CacheKey, mode: SyncMode, now: datetime
    ) -> datetime:
        """
        Return the cutoff of a mode's own span.

        The daily reconciliation reaches back from 48 hours to 6 hours
        before the oldest estimate it imported, and before the oldest hour
        missing between stored hours of the last 14 days, as when Meridian
        publishes a day's readings late with no estimates for them first
        (``lanes.missing_recent``), never more than 14 days back.
        """
        if mode is SyncMode.INITIAL:
            return now - INITIAL_BACKFILL
        if mode in {SyncMode.RESTART, SyncMode.FULL_RECONCILIATION}:
            return now - REVISION_OVERLAP
        if mode is SyncMode.TIP:
            return now - TIP_WINDOW

        minimum = now - TARGETED_RECONCILIATION_MINIMUM
        oldest_provisional = min(
            (
                item.start.astimezone(UTC)
                for item in self._measurement_cache.get(cache_key, {}).values()
                if item.quality != _ACTUAL
            ),
            default=None,
        )
        lane = self._lanes.get(cache_key)
        oldest = min(
            (
                hour
                for hour in (
                    oldest_provisional,
                    None if lane is None else lane.missing_recent,
                )
                if hour is not None
            ),
            default=None,
        )
        if oldest is None:
            return minimum
        widened = oldest - RECONCILIATION_SAFETY_MARGIN
        return max(now - REVISION_OVERLAP, min(minimum, widened))

    @staticmethod
    def _uncache_costs(
        cache: dict[MeasurementKey, MeridianMeasurement],
        measurements: tuple[MeridianMeasurement, ...],
        cost: CostPlan,
    ) -> None:
        """
        Cache the rows of an import whose cost was not written without it.

        The cache holds what was last imported, to find what changed. These
        rows' costs were not written, so the next update that fetches them
        finds them changed and offers them again, rather than taking their
        readings for old ones.
        """
        for item in measurements:
            start = item.start.astimezone(UTC)
            key = (start, item.channel_id)
            if (
                not cost.writes(start)
                and item.cost_cents is not None
                and cache.get(key) == item
            ):
                cache[key] = replace(item, cost_cents=None)

    def _merge_measurements(
        self,
        cache_key: CacheKey,
        incoming: tuple[MeridianMeasurement, ...],
        now: datetime,
        *,
        initial_import: bool = False,
        whole_window: bool = False,
        cache: dict[MeasurementKey, MeridianMeasurement] | None = None,
    ) -> tuple[MeridianMeasurement, ...]:
        """
        Merge fetched rows into a direction's cache; return the rows to import.

        The rows fetched for an hour are all of that hour's: a window joins
        the direction's rows only once every page of it has come back. So
        they replace the hour's cached rows, those of channels no longer
        returned included, rather than join them: an hour is never counted
        from the channels of two fetches, as when a meter's registers
        change. A cached ACTUAL row is kept over an estimate of it. The
        rows to import run from the earliest hour that changed; with
        ``whole_window``, as the weekly reconciliation, from the earliest
        hour fetched, so its replay rewrites its whole window; with
        ``initial_import``, they are every row fetched.

        ``cache`` is the copy to merge into, which the caller commits once
        the import has returned; without it, the coordinator's own cache.
        """
        if cache is None:
            cache = self._measurement_cache.setdefault(cache_key, {})
        cutoff = now - REVISION_OVERLAP
        for key in tuple(cache):
            if key[0] < cutoff:
                del cache[key]

        hours = _fetched_hours(incoming, cutoff)
        earliest_changed = _merge_hours(cache, hours)

        if initial_import:
            return tuple(
                sorted(incoming, key=lambda item: (item.start, item.channel_id))
            )
        if whole_window and hours:
            earliest_changed = min(hours)
        if earliest_changed is None:
            return ()
        return tuple(
            sorted(
                (
                    item
                    for item in cache.values()
                    if item.start.astimezone(UTC) >= earliest_changed
                ),
                key=lambda item: (item.start, item.channel_id),
            )
        )

    def _unit_walk(
        self,
        *,
        account_number: str,
        property_id: str,
        direction: str,
        since: datetime,
        mode: SyncMode | None,
        position: _Position | None,
        stored_energy: frozenset[datetime],
        has_statistics: bool,
        now: datetime,
        state: UnitState | None = None,
        quiet: bool = False,
    ) -> _UnitWalk:
        """
        Plan one direction's walk: its windows, its page log and its ladder.

        A sync passes the ``now`` its cutoff and its progress were planned
        from, so the windows sent are those counted, even after New Zealand
        midnight has passed since it started.
        """
        cache_key = (property_key(account_number, property_id), direction)
        walk = _Walk(
            self._row_density.get(cache_key, 1.0),
            stored_energy,
            has_statistics=has_statistics,
            quiet=quiet,
        )
        request = _WalkRequest(
            account_number=account_number,
            property_id=property_id,
            direction=direction,
            position=position,
            since=since,
            now=now,
            log=_PageLog(mode=mode, direction=direction, position=position, now=now),
        )
        return _UnitWalk(
            walk,
            request,
            plan_windows(since, now),
            setup=self._setup_import,
            state=state,
        )

    def _walk_result(self, walk: _UnitWalk) -> MeasurementFetchResult:
        """Return what a walk fetched, noting the history start and older rows."""
        self._history_start |= walk.walk.history_start
        result = walk.walk.result()
        if result.walk.rows_before_window:
            self._log_once(
                "rows_before_window",
                "Meridian returned readings from before the requested window, "
                "which were left out (%d rows, direction=%s)",
                result.walk.rows_before_window,
                walk.request.direction,
            )
        return result

    async def _async_walk_unit(
        self,
        walk: _UnitWalk,
        *,
        newest_only: bool = False,
        pause: Callable[[], bool] | None = None,
    ) -> None:
        """
        Fetch a direction's windows until its walk ends, or only its next one.

        Newest first, back to the cutoff. ``pause`` is asked after each
        window: when it says so, the walk stops where it is, to go on later
        in the same sync. A failure stops this walk alone, and is recorded
        as its error, as is the end of the sync's time budget: the sync goes
        on with its other units. A rejected session and a rate limit stop
        the whole sync, and so does a missing account or property while the
        topology has not been refreshed in this sync, which refreshes it and
        tries again.

        Each window is fetched whole into its own rows before they join the
        direction's, so a failure leaves nothing half-read; each complete
        window is checked against the rows the newer windows returned from
        before their own start (``_Walk.check``). The page size of a window
        follows the rows an hour holds, learned from the first window that
        returns readings. A window with no readings stops the walk when Home
        Assistant has stored hours inside it (Meridian no longer returns the
        readings stored there, which the import leaves as stored); otherwise
        two in a row after a reading mark the start of the history. Empty
        windows mark no such start while hours are stored older than them,
        nor before any reading: the walk goes on to the stored hours, or to
        the cutoff, as one request for the whole span would, since a supply
        that ended weeks ago has empty newest weeks, and a meter may send
        nothing for weeks. Rows still to come are no reading. A window older
        than every reading returned so far that Meridian refuses before it
        returned any reading of its own can mark the start too
        (``_async_fetch_or_refused``), or show that there is no history yet,
        for a direction with no statistics (``_Walk.has_statistics``:
        whether its energy statistic has stored rows, inside the span or
        not).

        Each page logs one debug line, and a failed page one line naming the
        failure, with times as offsets from now and no identifiers.
        """
        try:
            while not walk.done:
                await self._async_walk_step(walk)
                if newest_only or (pause is not None and pause()):
                    return
        except RunBudgetError as err:
            walk.error = err
        except MeridianAuthenticationError, MeridianRateLimitError:
            raise
        except MeridianGraphQLError as err:
            if _is_topology_error(err) and not self._topology_fresh:
                raise
            self._fail_walk(walk, err)
        except (MeridianError, ValueError) as err:
            self._fail_walk(walk, err)

    def _fail_walk(self, walk: _UnitWalk, err: Exception) -> None:
        """
        Record the failure that stopped a walk, and the request it failed at.

        The sync goes on with other units' requests, so the call is kept as
        it was, for the failure the sync may raise to name (``_settle_run``),
        and then marked finished: a later failure, or a cancellation, must not
        name it as still waiting for an answer.
        """
        walk.error = err
        walk.call = self.last_request
        self._end_call_in_flight()

    async def _async_walk_step(self, walk: _UnitWalk) -> None:
        """
        Fetch one window of a walk, or plan its days after a failure that allows.

        After a gateway timeout of a window longer than a day, the setup
        import waits out the cooldown and asks for its newest day alone, the
        probe (``_ladder``). The probe answered, the window's other days
        follow one at a time, and the walk goes back to weeks; a second week
        that needs the ladder keeps days for the rest of the walk. A probe
        that fails as well shows that the window's size is not what fails,
        and the walk stops. A query over Kraken's limits splits the walk into
        days once. Polling stops at a unit's first gateway timeout, and its
        next update probes the window holding that week's newest date; a
        second window that times out after a probe came back keeps days for
        the updates after it (``_defer_ladder``).
        """
        splits = walk.splits
        window, weight, probe = walk.next_window()
        self._splits += walk.splits - splits
        rows = WindowRows(window, since=walk.request.since, now=walk.request.now)
        walk.walk.requested.append(rows)
        walk.walk.failing = rows
        sent = self._measurement_requests
        try:
            refused = await self._async_fetch_or_refused(walk.walk, rows, walk.request)
        except (MeridianUpstreamError, MeridianGraphQLError) as err:
            if self._ladder(walk, window, weight, err, probe=probe):
                # The failed request has ended: during the cooldown before
                # the probe, a failure or a cancellation must not name it as
                # still waiting for an answer.
                self._end_call_in_flight()
                walk.walk.failing = None
                return
            raise
        finally:
            attempted = self._measurement_requests > sent
            walk.attempted |= attempted
            if probe and attempted and walk.probed and walk.state is not None:
                # The probe an earlier update left went out: later updates
                # no longer owe it. One the time budget stopped before it
                # went out is still owed.
                walk.state.probe_on = None
        if probe and walk.state is not None and not walk.setup:
            # The newest day of a window an earlier update could not fetch
            # came back: size was the lever (``_defer_ladder``).
            walk.state.ladder(window.end_on, walk.today)
        if self._progress is not None:
            self._progress.add(weight)
        goes_on = not refused and self._take_window(
            walk.walk, rows, walk.request.direction, weight
        )
        if walk.walk.quiet and rows.inside and walk.deep:
            # A quiet direction's readings came back: its walk now goes on
            # to the newest stored hour, more than 14 days back.
            self._extend_budget()
        # Only now has the window joined the walk: one that failed its
        # checks was dropped, so the import may not count it as fetched.
        walk.completed_on = window.start_on
        walk.walk.failing = None
        if not goes_on:
            walk.ended = True

    def _ladder(
        self,
        walk: _UnitWalk,
        window: MeasurementWindow,
        weight: float,
        err: MeridianError,
        *,
        probe: bool,
    ) -> bool:
        """
        Plan a failed window's days, when that may help; return whether it did.

        See ``_async_walk_step``. The failed window's own rows are dropped
        with it, so its days never meet them.
        """
        direction = walk.request.direction
        if _is_gateway(err):
            if window.days == 1:
                if probe:
                    # In polling no probe that came back is remembered here:
                    # an update probes a window only when none is
                    # (``_defer_ladder``), and only that probe can come back.
                    self._log_once(
                        "size_not_the_lever",
                        "Meridian did not answer a request for one day of usage "
                        "history in time either, so smaller requests do not help; "
                        "this direction stops for this update (direction=%s)",
                        direction,
                    )
                return False
            if not self._setup_import:
                self._defer_ladder(walk, window)
                return False
            walk.gateway_timeout = err
            walk.laddered += 1
            if walk.laddered > 1 and not walk.days_only:
                walk.days_only = True
                self._log_once(
                    "days_kept",
                    "A second week of usage history needed requests by day; the "
                    "rest of this import asks for one day at a time (direction=%s)",
                    direction,
                )
            self._log_once(
                "probe_after_cooldown",
                "Meridian did not answer a request for a week of usage history in "
                "time; after a pause, it is asked for the week's newest day alone "
                "(direction=%s window=%d/%d)",
                direction,
                window.index,
                window.count,
            )
            walk.split(window, weight, probe=True)
            self._splits += 1
            return True
        if _is_query_limit(err) and window.days > 1 and not walk.query_split:
            walk.query_split = walk.days_only = True
            self._log_once(
                "query_split",
                "Meridian refused a request for a week of usage history as too "
                "large (%s); it is asked for one day at a time (direction=%s)",
                _rejection(err),
                direction,
            )
            walk.split(window, weight, probe=False)
            self._splits += 1
            return True
        return False

    def _defer_ladder(self, walk: _UnitWalk, window: MeasurementWindow) -> None:
        """
        Leave a window an hourly update could not fetch to the next update.

        A unit's first gateway timeout ends its walk for this update. The
        next one asks for the window by day, its newest day first, the probe
        (``UnitState.probe_on``). When a probe came back in the last week
        (``UnitState.recent_ladder``), as earlier in this walk, or for this
        same window at an earlier update, this is the second window that
        needs requests by day: later updates ask for every window starting
        on or before the newer of the two dates by day
        (``UnitState.days_kept``), as setup keeps days after a second such
        week. Asking for those windows whole again would time out again,
        and a walk would never get past the first. Both are remembered for
        a week from the update that learned them, whichever window's dates
        they are: a probe of the second week or an older one is remembered
        as long as one of the newest.
        """
        state = walk.state
        if state is None:
            return
        today = walk.today
        first = state.recent_ladder(today)
        if first is None:
            state.probe_on = window.end_on
            state.probe_days = window.days
            return
        state.keep_days(max(first, window.end_on), today)
        self._log_once(
            "days_kept_updates",
            "A second window of usage history needed requests by day; the next "
            "updates ask for those windows one day at a time (direction=%s)",
            walk.request.direction,
        )

    async def _async_walk_window(
        self,
        walk: _Walk,
        rows: WindowRows,
        request: _WalkRequest,
        oldest_seen: datetime | None,
    ) -> None:
        """Fetch every page of one window of a walk, or of one of its dates."""
        await self._async_fetch_window(
            account_number=request.account_number,
            property_id=request.property_id,
            direction=request.direction,
            rows=rows,
            last=page_size(rows.window.hours, walk.density),
            log=request.log,
            position=request.position,
            oldest_seen=oldest_seen,
            actions=request.actions,
        )

    async def _async_fetch_or_refused(
        self, walk: _Walk, rows: WindowRows, request: _WalkRequest
    ) -> bool:
        """
        Fetch one window of a walk; return whether a refusal of it ends the walk.

        Meridian's web app never asks for dates before the supply started, so
        Kraken may refuse them instead of returning nothing, perhaps for any
        window that starts before the supply did. Only a refusal of a window
        older than every reading returned so far, before any reading of its
        own, can mark the start of the history (``_is_refusal``). The newer
        windows do not show where the history starts: readings that stop
        short of a window's first hour may start inside it, or have a gap at
        00:00 on its first date. So Meridian is asked for the refused
        window's dates alone (``_async_fetch_refused_days``). A refused window
        of one day is not asked for again: it would be the same request, and
        a GraphQL error is never resent. It is the start, once the date
        before it confirms it when newer readings came back
        (``_async_confirm_refused_day``).

        Refused before any reading with a GraphQL error that has a code, a
        window of one day, or the newest date of a longer one asked for
        alone, is taken for a property whose supply has not started yet,
        when the direction has no statistics (``_Walk.may_precede_supply``):
        there is no history yet, and nothing to import. Should the refusal be
        of another kind, nothing is lost: with no statistics, the direction
        is imported as new again by the next coordinator, and as soon as
        readings come back. Refused so with a client error, or for a
        direction with statistics, inside the span or older, it fails the
        sync: a client error is what a request Meridian cannot take at all
        gets, for every property, and statistics show there is a history.

        The start must leave no stored hour before it: stored hours there
        show the history goes on, and the refusal fails the sync, as every
        other failure does.
        """
        try:
            await self._async_walk_window(walk, rows, request, walk.oldest_seen)
        except (MeridianGraphQLError, MeridianConnectionError) as err:
            if not _is_refusal(err, rows, walk.oldest_seen):
                raise
            rejected: MeridianError = err
        else:
            return False
        window = rows.window
        if window.days == 1:
            if walk.oldest_seen is None and not walk.may_precede_supply(rejected):
                raise rejected
            if walk.oldest_seen is not None:
                await self._async_confirm_refused_day(walk, rows, request, rejected)
            start = window, rejected
        else:
            found = await self._async_fetch_refused_days(walk, rows, request, rejected)
            if found is None:
                return False
            start = found
        start_after, refusal = start
        if any(hour < start_after.end for hour in walk.stored_energy):
            raise refusal
        walk.start_after = start_after.end_on
        self._take_rejected_window(walk, rows, refusal, request.direction)
        return True

    async def _async_fetch_refused_days(
        self,
        walk: _Walk,
        rows: WindowRows,
        request: _WalkRequest,
        rejected: MeridianError,
    ) -> tuple[MeasurementWindow, MeridianError] | None:
        """
        Ask for a refused window's dates alone, newest first.

        Return the window or date the history starts after, with the refusal
        that showed it, or None when every date came back: the window's rows
        are then those of its dates, and the walk goes on, the refusal having
        been about something else. Each date that comes back joins the
        window's rows. A refusal of the newest date makes the whole window
        the start: after a newer reading, where no reading for that date does
        too, and before any reading, as for a supply not started yet, when
        the refusal is a GraphQL error and the direction has no statistics;
        with no newer reading, a client error, or statistics, fail the sync.
        A date refused after a newer one came back may be the day before the
        supply started: the date before it is asked for too, and only a
        refusal of that date as well, or no readings for it, confirms the
        start;
        readings mean the refusal was of another kind, and the sync fails,
        for the setup retry or the next update to fetch the window again,
        rather than keep the newer dates alone for good. A refusal of the
        walk's oldest date needs no confirmation: it leaves out that one date
        at most. Each date counts as one more window requested in
        diagnostics, and one older than every reading logs no page line when
        it is empty.
        """
        window = rows.window
        first = first_date(request.since)
        newer = walk.oldest_seen is not None
        pending: tuple[MeasurementWindow, MeridianError] | None = None
        day_on = window.end_on
        while day_on >= window.start_on or pending is not None:
            if day_on < first:
                return pending
            day_rows, refusal = await self._async_fetch_day(walk, rows, request, day_on)
            if pending is not None:
                if refusal is None and day_rows.inside:
                    raise pending[1]
                return pending
            newest = day_on == window.end_on
            if newest and newer and (refusal is not None or not day_rows.inside):
                return window, refusal or rejected
            if refusal is None:
                rows.join(day_rows)
            elif newest:
                if not walk.may_precede_supply(refusal):
                    raise refusal
                return window, refusal
            else:
                pending = day_rows.window, refusal
            day_on -= timedelta(days=1)
        return None

    async def _async_confirm_refused_day(
        self,
        walk: _Walk,
        rows: WindowRows,
        request: _WalkRequest,
        rejected: MeridianError,
    ) -> None:
        """
        Confirm that a date refused alone, after newer readings, starts the history.

        A lane's coverage asks for a date alone when it is tried again after
        a failure, and for a lone missing date, and a walk asked for by day
        asks for each of its dates so. Refused after newer readings,
        that date may be the day before the supply started, or refused for
        another reason. As for a date of a refused window
        (``_async_fetch_refused_days``), the date before it is asked for too:
        a refusal of that date as well, or no readings for it, confirms the
        start. Readings show the history goes on, and the refusal is raised:
        the date waits, then is asked for again, rather than the history
        being cut off at it for good. The oldest date asked for, a walk's
        cutoff or a lane's target, needs no confirmation: it leaves out that
        one date at most.
        """
        day_on = rows.window.start_on - timedelta(days=1)
        if day_on < first_date(request.since):
            return
        day_rows, refusal = await self._async_fetch_day(walk, rows, request, day_on)
        if refusal is None and day_rows.inside:
            raise rejected

    async def _async_fetch_day(
        self, walk: _Walk, rows: WindowRows, request: _WalkRequest, day_on: date
    ) -> tuple[WindowRows, MeridianError | None]:
        """
        Fetch one date of a refused window alone.

        Return its rows, and its refusal when Meridian refused it as it may
        refuse dates before the supply (``_is_refusal``); any other failure
        is raised.
        """
        window = rows.window
        day = MeasurementWindow(day_on, day_on, index=window.index, count=window.count)
        day_rows = WindowRows(day, since=request.since, now=request.now)
        walk.requested.append(day_rows)
        oldest = min(
            (hour for hour in (walk.oldest_seen, rows.oldest) if hour is not None),
            default=None,
        )
        try:
            await self._async_walk_window(walk, day_rows, request, oldest)
        except (MeridianGraphQLError, MeridianConnectionError) as err:
            if not _is_refusal(err, day_rows, oldest):
                raise
            return day_rows, err
        return day_rows, None

    def _take_rejected_window(
        self, walk: _Walk, rows: WindowRows, err: MeridianError, direction: str
    ) -> None:
        """
        End a walk at a window Meridian refused, as the start of the history.

        The window's request failed in a way the sync handled, so it counts
        as finished, and the window as done. Its rows are those of the dates
        after the start that were asked for alone, if any. Refused before any
        reading, it says there is no history yet.
        """
        self._end_call_in_flight()
        self._windows_failed += 1
        if rows.inside:
            walk.add(rows)
        walk.history_start = True
        if walk.oldest_seen is None:
            self._log_once(
                f"no_history_yet_{direction}",
                "Meridian rejected a window before returning any reading (%s); "
                "this is taken as no usage history yet (direction=%s)",
                _rejection(err),
                direction,
            )
            return
        self._log_once(
            "history_start_error",
            "Meridian rejected a window older than every reading it "
            "returned (%s); this is taken as the start of the history "
            "(direction=%s)",
            _rejection(err),
            direction,
        )

    def _take_window(
        self, walk: _Walk, rows: WindowRows, direction: str, weight: float = 1.0
    ) -> bool:
        """
        Add a complete window to its walk; return whether the walk goes on.

        An empty window holding stored hours ends the walk. One with stored
        hours before it does not count towards the start of the history:
        they show the history goes on, so the walk goes on to them. Nor does
        one before any reading: the walk goes on to the cutoff, unless the
        direction is quiet (``_quiet``), when nothing newer than the newest
        stored hour has come, and the walk ends. Any other counts, and two
        in a row are the start of the history. A day of a window fetched by
        its days counts as its share of that window (``weight``), so it
        takes two weeks of empty days, not two days.
        """
        window = rows.window
        walk.check(rows)
        walk.kept.append(rows)
        if rows.inside:
            self._windows_ok += 1
            walk.add(rows)
            return True
        self._windows_empty += 1
        if any(window.start <= hour < window.end for hour in walk.stored_energy):
            walk.stopped_at_gap = True
            self._log_once(
                "stored_empty_window",
                "Meridian returned no readings for a window Home Assistant has "
                "statistics for; this update imports only the newer windows "
                "(direction=%s window=%d/%d)",
                direction,
                window.index,
                window.count,
            )
            return False
        if walk.oldest_seen is None and walk.quiet:
            return False
        if walk.oldest_seen is None or any(
            hour < window.start for hour in walk.stored_energy
        ):
            return True
        walk.empty_streak += weight
        # The shares of a week's 7 days add up to a hair under one.
        if walk.empty_streak >= EMPTY_WINDOWS_AT_HISTORY_START - _SHARE_ROUNDING:
            walk.history_start = True
            return False
        return True

    async def _async_fetch_window(
        self,
        *,
        account_number: str,
        property_id: str,
        direction: str,
        rows: WindowRows,
        last: int,
        log: _PageLog,
        position: _Position | None,
        oldest_seen: datetime | None,
        actions: _Actions | None = None,
    ) -> None:
        """
        Fetch every page of one window, with loop guards.

        Every page sends the window's dates and ``last`` unchanged; ``before``
        is only ever the previous page of this window's start cursor. An empty
        page ends the window, unless it says more pages exist; each page must
        reach older readings than the one before, or add the rest of the hour
        it ended at (``_check_page``); a window has 16 pages of 744 rows at
        most, or as many more pages as a smaller size needs (``page_limit``).
        A page with rows older than the window ends it too: Meridian ignored
        its start, and later pages would be older still.
        """
        window = rows.window
        before: str | None = None
        previous_oldest: datetime | None = None
        for page_number in range(1, page_limit(last) + 1):
            page, elapsed = await self._async_request_page(
                account_number=account_number,
                property_id=property_id,
                direction=direction,
                window=window,
                last=last,
                before=before,
                page_number=page_number,
                position=position,
                log=log,
                actions=actions,
            )
            try:
                kept, before_window = _check_page(page, rows, previous_oldest)
            except ValueError as err:
                log.failure(
                    window,
                    page_number,
                    before=before is not None,
                    last=last,
                    err=err,
                    elapsed=elapsed,
                    action=_STOP,
                )
                raise
            # An empty window older than every reading seen marks where the
            # history starts: its line would give that date away.
            if page.measurements or oldest_seen is None or window.end > oldest_seen:
                log.page(
                    rows,
                    page_number,
                    before=before is not None,
                    last=last,
                    page=page,
                    kept=kept,
                    before_window=before_window,
                    elapsed=elapsed,
                )
            if not page.measurements:
                return
            oldest = min(item.start.astimezone(UTC) for item in page.measurements)
            if oldest < window.start or not page.has_previous_page:
                return
            if not page.start_cursor or page.start_cursor == before:
                raise ValueError("Meridian pagination cursor did not advance")
            before = page.start_cursor
            previous_oldest = oldest
        raise ValueError("Meridian pagination exceeded the per-window page limit")

    async def _async_request_page(
        self,
        *,
        account_number: str,
        property_id: str,
        direction: str,
        window: MeasurementWindow,
        last: int,
        before: str | None,
        page_number: int,
        position: _Position | None,
        log: _PageLog,
        actions: _Actions | None = None,
    ) -> tuple[MeasurementPage, float]:
        """
        Send one measurements request, and resend it once if that may help.

        Return the page and how long it took. Each try is one call to the
        client, which renews the session itself when it is rejected: so a
        resend never forces a session refresh, and a rejected resend still
        gets its own. See ``_resend_action`` for what is resent, and when;
        ``actions`` names what a failure not resent leads to in the walk,
        for the failed page's debug line.
        """
        key = property_key(account_number, property_id)
        resent = False
        while True:
            await self._async_before_request(key)
            call = self._note_call(
                "measurements",
                position=position,
                direction=direction,
                page=page_number,
                bound="dates" if before is None else "before",
                window=window,
            )
            try:
                page = await self.client.async_get_measurements(
                    account_number=account_number,
                    property_id=property_id,
                    direction=direction,
                    start_on=window.start_on,
                    end_on=window.end_on,
                    last=last,
                    before=before,
                )
            except Exception as err:
                self._last_measurements_end = ended = monotonic()
                elapsed = ended - call.started_at
                action = self._resend_action(err, key, elapsed, resent=resent)
                log.failure(
                    window,
                    page_number,
                    before=before is not None,
                    last=last,
                    err=err,
                    elapsed=elapsed,
                    action=(
                        actions(err, window)
                        if action == _STOP and actions is not None
                        else action
                    ),
                )
                if action == _STOP:
                    raise
                # The failed request has ended: during the wait before the
                # resend, a failure or a cancellation must not name it as
                # still waiting for an answer.
                self._note_call_done(call)
                resent = True
                self._retries += 1
                continue
            elapsed = self._note_call_done(call)
            self._last_measurements_end = call.started_at + elapsed
            return page, elapsed

    def _resend_action(
        self, err: Exception, key: str, elapsed: float, *, resent: bool
    ) -> str:
        """
        Decide whether a failed measurements request is sent once more.

        After a gateway timeout (HTTP 504 or 524, or no answer within the
        time limit), no request goes to the property for a cooldown of 75 s,
        or as long as the failed request took: the service may still be
        working on it. So it is after a resend that timed out. It is never
        resent as it was: the setup import asks for the window's newest day
        alone after the cooldown (``_async_walk_step``), and polling leaves
        it to the next poll, so a struggling service is not asked twice
        every hour. Another passing failure (HTTP 408, 502, 503 or 520-523,
        or a network error) is resent after 2-5 s, when 80 s of the budget
        remain, unless it was a resend (``resent``). A rate limit, a GraphQL
        error, a client error or data that cannot be used is never resent.
        """
        if isinstance(err, MeridianRateLimitError) or not isinstance(
            err, MeridianConnectionError
        ):
            return _STOP
        now = monotonic()
        if _is_gateway(err):
            cooldown = max(float(GATEWAY_COOLDOWN_SECONDS), elapsed)
            self._cooldown_until[key] = now + cooldown
            return _STOP
        if resent or not _is_transient(err):
            return _STOP
        delay = random.uniform(*TRANSIENT_RESEND_DELAY_SECONDS)  # noqa: S311 - jitter
        if not self._time_left_after(delay):
            return _STOP
        self._resend_not_before = now + delay
        return _RESEND

    def _time_left_after(self, wait: float) -> bool:
        """Return whether a request could still start after a wait, by the budget."""
        return (
            self._deadline is None
            or self._deadline - monotonic() - wait >= REQUEST_START_RESERVE_SECONDS
        )

    async def _async_before_request(self, key: str) -> None:
        """
        Wait until the next measurements request may start, or stop the sync.

        Requests start at least 1 s apart, and, for the lanes' coverage
        (``_pause``), 2 to 5 s at random after the last one ended, so a slow
        Meridian gets its pause too; each after any cooldown for the property
        and any pause before a resend. Once the time budget has less than
        80 s left by then, no request starts except the sync's first, and
        ``RunBudgetError`` is raised instead.
        """
        now = monotonic()
        ready = max(self._cooldown_until.get(key, now), self._resend_not_before)
        if self._last_measurements_start is not None:
            ready = max(
                ready, self._last_measurements_start + MEASUREMENTS_PACING_SECONDS
            )
        if self._pause is not None and self._last_measurements_end is not None:
            pause = random.uniform(*self._pause)  # noqa: S311 - jitter
            ready = max(ready, self._last_measurements_end + pause)
        wait = max(0.0, ready - now)
        if (
            self._deadline is not None
            and self._measurement_requests
            and self._deadline - now - wait < REQUEST_START_RESERVE_SECONDS
        ):
            raise RunBudgetError
        if wait:
            await _async_wait(wait)
        self._last_measurements_start = monotonic()
        self._measurement_requests += 1

    def _remember_density(
        self, cache_key: CacheKey, result: MeasurementFetchResult
    ) -> None:
        if result.observed_rows_per_hour <= 0:
            return
        previous = self._row_density.get(cache_key)
        if previous is None:
            self._row_density[cache_key] = result.observed_rows_per_hour
            return
        smoothed = previous * 0.75 + result.observed_rows_per_hour * 0.25
        self._row_density[cache_key] = max(result.observed_rows_per_hour, smoothed)


# What a failed request that is not resent leads to in its walk, for the
# failed page's debug line: _PROBE, _SPLIT or _STOP.
type _Actions = Callable[[Exception, MeasurementWindow], str]


class _WalkRequest(NamedTuple):
    """What every measurements request of one direction's walk shares."""

    account_number: str
    property_id: str
    direction: str
    position: _Position | None
    since: datetime
    now: datetime
    log: _PageLog
    actions: _Actions | None = None


class _UnitWalk:
    """
    One direction's walk in a sync: the windows still to fetch, and its ladder.

    Windows are fetched newest first, each with a share of the sync's
    progress: one for a window, a fraction of it for each day of a window
    fetched day by day. ``completed_on`` is the oldest date of the windows
    fetched and kept so far, which, newest first and stopping at the first
    failure, are one block back from today; a window whose rows failed
    their checks is not kept. ``probe`` says the next window is a probe:
    the newest day of a longer window that timed out. ``call`` is the
    request the walk failed at, as it was when it failed, for the failure a
    sync raises to name.
    """

    def __init__(
        self,
        walk: _Walk,
        request: _WalkRequest,
        windows: tuple[MeasurementWindow, ...],
        *,
        setup: bool,
        state: UnitState | None = None,
    ) -> None:
        self.walk = walk
        self.request = request._replace(actions=self.action)
        self.planned = len(windows)
        self.queue: deque[tuple[MeasurementWindow, float]] = deque(
            (window, 1.0) for window in windows
        )
        self.newest = windows[0] if windows else None
        self.setup = setup
        self.state = state
        self.probe = False
        # Whether the walk has planned the probe an earlier update left it.
        self.probed = False
        self.laddered = 0
        # Windows asked for by their days because of what an earlier update
        # left: the probe it owed, or the days it kept.
        self.splits = 0
        self.days_only = False
        self.query_split = False
        self.completed_on: date | None = None
        self.ended = False
        self.error: Exception | None = None
        # A gateway timeout the setup import's ladder went on after, which
        # cost the walk the time of its cooldown.
        self.gateway_timeout: MeridianError | None = None
        self.call: RequestContext | None = None
        # Whether any request of the walk was sent.
        self.attempted = False

    @property
    def done(self) -> bool:
        """Return whether the walk has nothing more to fetch."""
        return self.ended or self.error is not None or not self.queue

    @property
    def complete(self) -> bool:
        """Return whether the walk ended without a failure."""
        return self.error is None and (self.ended or not self.queue)

    @property
    def newest_done(self) -> bool:
        """Return whether the walk fetched every date of its newest window."""
        return (
            self.newest is not None
            and self.completed_on is not None
            and self.completed_on <= self.newest.start_on
        )

    @property
    def deep(self) -> bool:
        """Return whether the walk's cutoff is more than 14 days back."""
        return self.request.since < self.request.now - REVISION_OVERLAP

    @property
    def today(self) -> date:
        """Return the New Zealand date the walk's windows end on."""
        return self.request.now.astimezone(_NZ).date()

    def remaining(self) -> float:
        """Return the progress the windows still to fetch would add."""
        return sum(weight for _window, weight in self.queue)

    def next_window(self) -> tuple[MeasurementWindow, float, bool]:
        """
        Take the next window to fetch, its share of the progress, and its probe flag.

        A window longer than a day is fetched day by day when the walk keeps
        days, when an earlier hourly update kept days for it
        (``UnitState.days_kept``), and when it reaches the date of a window
        the last hourly update could not fetch and is as long, whose newest
        day is then the probe. A shorter window, as the hourly update's two days
        after a week timed out, is asked for whole, and leaves the probe to
        the next window as long: it shows nothing about the week's size.
        Only the first such window of a walk is due; the update forgets the
        probe once it has gone out (``_async_walk_step``). A window split
        because of what an earlier update left counts in ``splits``: it is
        asked for again by its days. One split only because this walk keeps
        days does not.
        """
        window, weight = self.queue.popleft()
        if window.days > 1:
            state = self.state
            due = (
                not self.probed
                and state is not None
                and state.probe_on is not None
                and window.start_on <= state.probe_on
                and window.days >= state.probe_days
            )
            kept = None if state is None else state.days_kept(self.today)
            remembered = due or (kept is not None and window.start_on <= kept)
            if remembered or self.days_only:
                self.probed |= due
                if remembered:
                    self.splits += 1
                self.split(window, weight, probe=due or self.probe)
                window, weight = self.queue.popleft()
        probe = self.probe
        self.probe = False
        return window, weight, probe

    def split(self, window: MeasurementWindow, weight: float, *, probe: bool) -> None:
        """Put a window's dates, newest first, at the front of the walk."""
        days = [
            MeasurementWindow(
                window.end_on - timedelta(days=offset),
                window.end_on - timedelta(days=offset),
                index=window.index,
                count=window.count,
            )
            for offset in range(window.days)
        ]
        self.queue.extendleft((day, weight / window.days) for day in reversed(days))
        self.probe = probe

    def action(self, err: Exception, window: MeasurementWindow) -> str:
        """Return what a failure not resent leads to, for its debug line."""
        if window.days > 1 and (
            (_is_gateway(err) and self.setup)
            or (_is_query_limit(err) and not self.query_split)
        ):
            return _PROBE if _is_gateway(err) else _SPLIT
        return _STOP


def _failed_unit(units: list[_Unit], *, setup_import: bool) -> _Unit | None:
    """
    Return the unit whose failure a failed sync raises (``_settle_run``).

    The first unit that failed, a refused replay (``StatisticsAnchorError``)
    only when no other did. In setup, only a unit that did not import its
    newest window counts, if any failed: one degraded after its newest
    window did not keep setup from succeeding, and its failure would hide
    the one that did.
    """
    failures = [unit for unit in units if unit.walk is not None and unit.error]
    if setup_import:
        failures = [unit for unit in failures if not unit.imported_newest] or failures
    return next(
        (
            unit
            for unit in failures
            if not isinstance(unit.error, StatisticsAnchorError)
        ),
        next(iter(failures), None),
    )


class _Unit:
    """
    One unit of a sync: where it is, its mode and cutoff, its walk, how it ended.

    A unit waiting out a backoff has no walk; its outcome is ``deferred``,
    and so it is for one the time budget stopped before its first request.
    """

    def __init__(
        self,
        *,
        account_number: str,
        property_data: MeridianProperty,
        cache_key: CacheKey,
        statistic_ids: tuple[str, str],
        position: _Position,
        suffix: str,
        mode: SyncMode,
        since: datetime,
        state: UnitState,
        frontier: datetime | None,
        has_statistics: bool,
        quiet: bool = False,
        target: datetime | None = None,
        lane_hours: StoredHours | None = None,
    ) -> None:
        self.account_number = account_number
        self.property_data = property_data
        self.cache_key = cache_key
        self.statistic_ids = statistic_ids
        self.position = position
        self.suffix = suffix
        self.mode = mode
        self.since = since
        self.state = state
        self.frontier = frontier
        self.has_statistics = has_statistics
        self.quiet = quiet
        self.stored = StoredHours()
        # The unit's lane: its target, and the hours stored from it on, read
        # once when the sync was planned; the coverage adds what it imports.
        self.target = since if target is None else target
        lane = StoredHours() if lane_hours is None else lane_hours
        self.lane_energy: set[datetime] = set(lane.energy)
        self.lane_cost: set[datetime] = set(lane.cost)
        self.walk: _UnitWalk | None = None
        self.outcome: str | None = None
        self.error: Exception | None = None
        self.rows = 0
        self.fetch = MeasurementFetchResult((), 0, 0, 0.0)
        # The newest date whose fetched rows the import left out, every date
        # when it imported none: neither the lane nor the coverage counts
        # them as fetched (``windows.importable``).
        self.left_out_through: date | None = None
        # The rows its walk's confirmed replay was given, with the cost plan
        # it wrote by, for the dates that replay wrote again in full.
        self.replayed: tuple[tuple[MeridianMeasurement, ...], CostPlan] | None = None

    def deep(self, now: datetime) -> bool:
        """
        Return whether the walk reaches back further than 14 days.

        A quiet one (``_quiet``) stops at its empty newest window.
        """
        return not self.quiet and (
            self.mode is SyncMode.INITIAL or self.since < now - REVISION_OVERLAP
        )

    @property
    def imported_newest(self) -> bool:
        """
        Return whether the unit imported at least its newest window in full.

        In full, or with what it could fetch of it: a walk that ended
        without a failure, as at the start of the history, or whose newest
        window came back whole before it stopped.
        """
        return self.outcome == STATUS_OK or (
            self.outcome == STATUS_DEGRADED
            and self.walk is not None
            and self.walk.newest_done
        )

    @property
    def found_readings(self) -> bool:
        """
        Return whether the unit's walk returned readings, or it wrote rows.

        Estimates for hours still to come are no readings (``WindowRows``).
        """
        return self.rows > 0 or (
            self.walk is not None and self.walk.walk.oldest_seen is not None
        )

    def found_new(self, measurements: tuple[MeridianMeasurement, ...]) -> bool:
        """
        Return whether a walk's readings hold any newer than its frontier.

        The frontier is the one the walk started from: an import that
        returned has moved it on since. A first import has none, so its
        readings are new when its newest window has some: one whose newest
        window is empty, as for a supply that ended, found nothing newer
        than its older readings, and the entry's first update after setup
        asks for that window alone.
        """
        if self.frontier is not None:
            frontier = self.frontier
            return any(item.start.astimezone(UTC) > frontier for item in measurements)
        newest = None if self.walk is None else self.walk.newest
        return newest is None or any(
            item.start >= local_midnight(newest.start_on) for item in measurements
        )


class _Walk:
    """One direction's walk through its windows: what it kept and where it is."""

    def __init__(
        self,
        density: float,
        stored_energy: frozenset[datetime],
        *,
        has_statistics: bool = False,
        quiet: bool = False,
    ) -> None:
        self.density = density
        self._learned = False
        self.stored_energy = stored_energy
        self.has_statistics = has_statistics
        self.quiet = quiet
        self.measurements: dict[MeasurementKey, MeridianMeasurement] = {}
        self.requested: list[WindowRows] = []
        # The window of the walk in progress, until it has joined the walk:
        # the one a failure stopped it at. Its dates asked for alone, as
        # after a refusal, are among ``requested`` too.
        self.failing: WindowRows | None = None
        # The windows fetched in full and checked, readings or none.
        self.kept: list[WindowRows] = []
        self.oldest_seen: datetime | None = None
        self.empty_streak = 0.0
        self.stopped_at_gap = False
        self.history_start = False
        # The newest date of a window Meridian refused as before the supply,
        # where the walk found the start of the history that way.
        self.start_after: date | None = None
        # Hours and channels newer windows returned from before their start,
        # not yet checked against the window they fall in.
        self._before: set[MeasurementKey] = set()
        self._newer: WindowRows | None = None
        self._missing_last_days = 0
        self._one_sided_edges = 0

    def may_precede_supply(self, err: MeridianError) -> bool:
        """
        Return whether a refusal before any reading may be of a supply to come.

        Kraken refuses a request it can read with a GraphQL error that has a
        code, as it may for dates before a supply starts. A client error
        (HTTP 4xx) is what a request Meridian cannot take at all gets, for
        every property: that is no sign of a supply still to start. Nor is
        any refusal for a direction with statistics: it has a history, even
        when none of its stored hours falls inside the sync's span, as in an
        hourly update while Meridian publishes readings more than a day late.
        """
        return not self.has_statistics and isinstance(err, MeridianGraphQLError)

    def check(self, rows: WindowRows) -> None:
        """
        Check a complete window against the newer windows, for dates read shifted.

        A newer window's rows from before its start, as when Meridian ignores
        the start, must be rows this window returned too: when they are not,
        as when Meridian reads the dates a day early, this window lacks them,
        and the hours of its last date would be left out of every window.
        And a window with readings but none for its last date, after a newer
        window that returned its first hour, is what reading ``endOn`` as
        excluded gives, in every window. Once is a gap a meter may have, and
        so is a gap on the last dates of windows apart, which depends on the
        day the walk runs; two windows in a row fail the sync too. So do two
        boundaries in a row where, of two windows with readings, one returned
        its edge hour and the other did not: what reading the first hour of
        ``startOn``, or the last of ``endOn``, as excluded gives at every
        boundary, an hour no other window returns. An outage across a
        boundary leaves out the hours on both sides, and fails nothing. Both
        checks apply to windows next to each other: with the one-day overlap
        held in reserve switched on, the older window returns the newer
        one's first date.
        """
        window = rows.window
        inside = {key for key in self._before if window.start <= key[0] < window.end}
        if not rows.returned(inside):
            raise ValueError(
                "Meridian returned readings before a window that the older "
                "window did not return"
            )
        self._before = {
            key for key in self._before | rows.before_keys if key[0] < window.start
        }
        newer = self._newer
        self._newer = rows
        if newer is None or newer.window.start != window.end:
            self._missing_last_days = self._one_sided_edges = 0
            return
        if rows.inside and not rows.last_day and newer.first_hour:
            self._missing_last_days += 1
            if self._missing_last_days > 1:
                raise ValueError(
                    "Meridian returned no readings for the last date of two "
                    "windows in a row"
                )
        else:
            self._missing_last_days = 0
        if rows.inside and newer.inside and rows.last_hour != newer.first_hour:
            self._one_sided_edges += 1
            if self._one_sided_edges > 1:
                raise ValueError(
                    "Meridian returned only one window's edge hour at two window "
                    "boundaries in a row"
                )
        else:
            self._one_sided_edges = 0

    def add(self, rows: WindowRows) -> None:
        """
        Add a complete window that returned readings.

        The first such window sets the rows an hour holds for the windows
        after it: a whole fetch's rows over its span read too low while
        readings are published late.
        """
        self.empty_streak = 0.0
        seen = [hour for hour in (self.oldest_seen, rows.oldest) if hour is not None]
        self.oldest_seen = min(seen, default=None)
        window_rows = tuple(rows.rows.values())
        if not self._learned and window_rows:
            self.density = max(1.0, rows_per_hour(window_rows))
            self._learned = True
        _join_window(self.measurements, rows)

    def result(self) -> MeasurementFetchResult:
        """Return the walk's rows, sorted, with what it asked for and found."""
        measurements = tuple(
            sorted(
                self.measurements.values(),
                key=lambda item: (item.start, item.channel_id),
            )
        )
        pages = sum(rows.pages for rows in self.requested)
        return MeasurementFetchResult(
            measurements=measurements,
            pages=pages,
            received_rows=sum(rows.received for rows in self.requested),
            observed_rows_per_hour=rows_per_hour(measurements),
            walk=WindowWalk(
                requested_days=sum(rows.window.days for rows in self.requested),
                windows_requested=len(self.requested),
                continuation_pages=pages
                - sum(bool(rows.pages) for rows in self.requested),
                rows_before_window=sum(rows.before_window for rows in self.requested),
                windows_empty=sum(
                    1 for rows in self.requested if rows.pages and not rows.inside
                ),
                stopped_at_gap=self.stopped_at_gap,
                history_start=self.history_start,
            ),
        )


class _DirectionSync(NamedTuple):
    """What one direction's sync fetched, how many rows it imported, its cutoff."""

    fetch: MeasurementFetchResult
    rows: int
    since: datetime

    @classmethod
    def empty(cls, other: _DirectionSync) -> _DirectionSync:
        """Return the result for a direction not synced, with another's cutoff."""
        return cls(MeasurementFetchResult((), 0, 0, 0.0), 0, other.since)


def _statistic_names(unit: _Unit) -> tuple[str, str]:
    """Return the names of a unit's energy and money statistics."""
    consumption = unit.cache_key[1] == READING_CONSUMPTION
    return (
        f"Meridian grid {'import' if consumption else 'export'}{unit.suffix}",
        ("Meridian grid import cost" if consumption else "Meridian grid export credit")
        + unit.suffix,
    )


def _lane_key(cache_key: CacheKey) -> str:
    """Return a lane's key, as its revision day is spread by: no identifier."""
    return f"{cache_key[0]}:{cache_key[1]}"


def _left_out_through(plan: ImportPlan, now: datetime) -> date | None:
    """Return the newest date whose rows an import plan left out, if any."""
    if plan.tail_not_returned:
        return now.astimezone(_NZ).date()
    if plan.newest_hole is None:
        return None
    return plan.newest_hole.astimezone(_NZ).date()


def _left_out_days(plan: ImportPlan, days: list[date]) -> set[date]:
    """Return the dates of a window whose rows an import plan left out."""
    if plan.tail_not_returned:
        return set(days)
    if plan.newest_hole is None:
        return set()
    through = plan.newest_hole.astimezone(_NZ).date()
    return {day for day in days if day <= through}


def _walk_days(unit: _Unit, newest: date) -> list[tuple[date, bool]]:
    """
    Return the dates an imported walk shows, each with whether it had readings.

    Those of the windows it fetched in full, older than ``newest``, the
    newest stored reading's date, whose midnight its cutoff reached and
    whose rows its import did not leave out (``_Unit.left_out_through``);
    none of an empty window over stored hours, which ends a walk.
    """
    walk = unit.walk
    assert walk is not None  # noqa: S101 - only fetched units are noted
    days: list[tuple[date, bool]] = []
    for rows in walk.walk.kept:
        window = rows.window
        if not rows.inside and any(
            window.start <= hour < window.end for hour in unit.stored.energy
        ):
            continue
        readings = _days_of(start for start, _channel in rows.rows)
        days.extend(
            (day, day in readings)
            for day in _window_days(window)
            if day < newest
            and local_midnight(day) >= unit.since
            and (unit.left_out_through is None or day > unit.left_out_through)
        )
    return days


def _days_of(hours: Iterable[datetime]) -> set[date]:
    """Return the New Zealand dates of some hours."""
    return {hour.astimezone(_NZ).date() for hour in hours}


def _replayed_days(
    days: Collection[date],
    measurements: Collection[MeridianMeasurement],
    cost: CostPlan,
    stored: StoredHours,
) -> set[date]:
    """
    Return the dates whose stored hours a replay of these rows wrote again.

    A replay writes each statistic from the first hour it is given to the
    last, the stored hours between them included, and shifts the rows
    after them to follow on (``statistics.async_replay``); the cost only
    for the hours whose every row has one and that the cost plan writes.
    So a date counts when every hour of it stored for usage lies within
    the usage's span, and every one stored for cost within the cost's: an
    import Home Assistant did not confirm may have left a wrong step after
    any of them, which only such a replay writes again.
    """
    energy = {item.start.astimezone(UTC) for item in measurements}
    without_cost = {
        item.start.astimezone(UTC) for item in measurements if item.cost_cents is None
    }
    costed = {hour for hour in energy - without_cost if cost.writes(hour)}

    def within(hours: Collection[datetime], span: Collection[datetime]) -> bool:
        return not hours or (
            bool(span) and min(span) <= min(hours) and max(hours) <= max(span)
        )

    energy_by_day = _hours_by_day(stored.energy)
    cost_by_day = _hours_by_day(stored.cost)
    return {
        day
        for day in days
        if within(energy_by_day.get(day, ()), energy)
        and within(cost_by_day.get(day, ()), costed)
    }


def _hours_by_day(hours: Collection[datetime]) -> dict[date, list[datetime]]:
    """Return some hours grouped by their New Zealand date."""
    grouped: dict[date, list[datetime]] = {}
    for hour in hours:
        grouped.setdefault(hour.astimezone(_NZ).date(), []).append(hour)
    return grouped


def _window_days(window: MeasurementWindow) -> list[date]:
    """Return a window's New Zealand dates, newest first."""
    return [window.end_on - offset * _DAY for offset in range(window.days)]


def _hours_from(stored: StoredHours, start: datetime) -> StoredHours:
    """Return the stored hours from ``start`` on."""
    return StoredHours(
        energy=frozenset(hour for hour in stored.energy if hour >= start),
        cost=frozenset(hour for hour in stored.cost if hour >= start),
    )


def _statistic_ids(cache_key: CacheKey) -> tuple[str, str]:
    """Return the energy and money statistic IDs of a (property, direction) pair."""
    key, direction = cache_key
    return (
        consumption_ids(key)
        if direction == READING_CONSUMPTION
        else generation_ids(key)
    )


def _hours_until(when: datetime, now: datetime) -> int:
    """Return the whole hours from now until a time, for a log line, at least 1."""
    return max(1, round((when - now) / _HOUR))


def _directions(
    key: str, property_data: MeridianProperty
) -> list[tuple[str, tuple[str, str]]]:
    """Return a property's directions, with their energy and money statistic IDs."""
    directions = [(READING_CONSUMPTION, consumption_ids(key))]
    if any(meter.has_feed_in for meter in property_data.meter_points):
        directions.append((READING_GENERATION, generation_ids(key)))
    return directions


def _check_page(
    page: MeasurementPage, rows: WindowRows, previous_oldest: datetime | None
) -> tuple[int, int]:
    """
    Check that a page moved the window on, and keep its rows.

    A page must reach older readings than the one before, or, when a page
    boundary fell inside an hour, end at that same hour with readings no
    earlier page held. A page returned again, as for an ignored cursor,
    does neither. Return how many hours and channels it added, and how many
    of its rows were older than the window.
    """
    if not page.measurements:
        if page.has_previous_page:
            raise ValueError("Meridian returned an empty page with more to come")
        rows.pages += 1
        return 0, 0
    oldest = min(item.start.astimezone(UTC) for item in page.measurements)
    if previous_oldest is not None and (
        oldest > previous_oldest
        or (oldest == previous_oldest and not rows.holds_new_rows(page.measurements))
    ):
        raise ValueError("Meridian returned a page no older than the one before")
    return rows.add_page(page.measurements)


def _join_window(
    measurements: dict[MeasurementKey, MeridianMeasurement], rows: WindowRows
) -> None:
    """
    Add a complete window's rows to its direction's.

    Windows do not overlap, so an hour comes from one window only. With the
    one-day overlap held in reserve switched on, an hour both windows
    returned counts once when its readings are identical.
    """
    for key, measurement in rows.rows.items():
        existing = measurements.setdefault(key, measurement)
        if existing != measurement:
            raise ValueError(
                "Meridian returned different readings for one hour in two windows"
            )


def _is_transient(err: MeridianConnectionError) -> bool:
    """
    Return whether a failure may pass on its own: a resend may then succeed.

    That is a service error with a status a gateway or an overloaded service
    sends (408, 502, 503, 520-523), or a network failure, which has no status.
    """
    if isinstance(err, MeridianUpstreamError):
        return err.status in TRANSIENT_STATUSES
    return http_status(err) is None


def _is_refusal(
    err: MeridianError, rows: WindowRows, oldest_seen: datetime | None
) -> bool:
    """
    Return whether a failed window may be a refusal of dates before the supply.

    The window must be older than every reading returned so far, and
    Meridian must have refused it before it returned any reading of its own.
    The refusal must be the measurements request's own: a GraphQL error with
    a code that is not a query over Kraken's limits or a missing account or
    property, or Meridian's service refusing the request with a client error
    (HTTP 4xx). A GraphQL error without a code, as for a failure inside
    Meridian's service, a rate limit, a service failure and a failure to
    renew the session with Google's service are not.
    """
    if rows.inside or (oldest_seen is not None and rows.window.end > oldest_seen):
        return False
    if isinstance(err, MeridianGraphQLError):
        return bool(set(err.codes) - {UNKNOWN_ERROR_CODE}) and not (
            QUERY_LIMIT_CODES.intersection(err.codes) or _is_topology_error(err)
        )
    return isinstance(err, MeridianRejectedError)


def _rejection(err: MeridianError) -> str:
    """Name how Meridian rejected a window: its error codes, or its HTTP status."""
    if isinstance(err, MeridianGraphQLError):
        return ", ".join(err.codes) or "GraphQL error"
    return f"HTTP {http_status(err)}"


def _is_topology_error(err: MeridianGraphQLError) -> bool:
    return bool(_TOPOLOGY_ERROR_CODES.intersection(err.codes))


def _local_day_start(value: date) -> datetime:
    """Return a New Zealand local date boundary as UTC."""
    return datetime.combine(value, time.min, tzinfo=_NZ).astimezone(UTC)


def _one_line_address(value: str) -> str:
    """Normalize a local-only property label for display."""
    return " ".join(value.split())
