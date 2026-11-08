"""
Data coordinator for Meridian Energy: one sync of every unit, each hour.

A unit is one direction of one property: grid import always, and grid
export when the property has an active feed-in register. Each sync takes
the units one after another. For each it asks for its recent dates, then
fills in the dates still missing back to its target, 90 days back, and
writes what it fetched (``statistics.async_write_hours``). Then each
account's other figures are asked for, each kind at its own interval: the
billing period, the bill's day totals, the balance and the prices. What it
learns is kept in memory only, and built again after a start.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections import Counter
from collections.abc import Awaitable, Callable, Collection, Iterable
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from functools import partial
from operator import attrgetter
from time import monotonic
from typing import Any, NamedTuple

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from sqlalchemy.exc import SQLAlchemyError

from .api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianError,
    MeridianGraphQLError,
    MeridianRateLimitError,
    MeridianRejectedError,
    MeridianUpstreamError,
    exception_chain,
    http_status,
)
from .const import (
    BALANCE_INTERVAL,
    BILL_DAYS_INTERVAL,
    BILLING_CACHE_INTERVAL,
    DAILY_DATES,
    DAILY_INTERVAL,
    DOMAIN,
    FAILED_UPDATES_BEFORE_ISSUE,
    FILL_COOLDOWN,
    FILL_WINDOWS,
    FIRST_SYNC_RETRY_SCHEDULE,
    HISTORY_DAYS,
    MAX_FILL_COOLDOWN,
    MAX_RETRY_AFTER_SECONDS,
    MEASUREMENTS_PACING_SECONDS,
    NAME,
    PRICES_INTERVAL,
    READING_CONSUMPTION,
    READING_FREQUENCY_DAY,
    READING_GENERATION,
    RECENT_DATES,
    TOPOLOGY_CACHE_INTERVAL,
    TRANSIENT_RESEND_DELAY_SECONDS,
    TRANSIENT_STATUSES,
    UPDATE_INTERVAL,
)
from .graphql import QUERY_LIMIT_CODES, is_service_failure, shown_codes
from .models import (
    AccountSyncResult,
    BillFigures,
    MeridianAccount,
    MeridianBalance,
    MeridianBillingPeriod,
    MeridianMeasurement,
    MeridianPrices,
    MeridianProperty,
    MeridianSyncData,
    PropertySyncResult,
)
from .statistics import (
    account_key,
    async_stored_hours,
    async_write_hours,
    consumption_ids,
    cost_metadata,
    energy_metadata,
    generation_ids,
    property_key,
)
from .transport import MeridianUnreadableResponseError
from .windows import (
    HOUR,
    MeasurementWindow,
    Reading,
    date_hours,
    day_readings,
    hourly_readings,
    local_midnight,
    nz_date,
    windows_for,
)

_LOGGER = logging.getLogger(__name__)
_DAY = timedelta(days=1)
_ACTUAL = "ACTUAL"
_CENTS_PER_DOLLAR = Decimal(100)
# A request that failed in a way that may pass is sent once more.
_SENDS = 2
_LABELS = {READING_CONSUMPTION: "grid import", READING_GENERATION: "grid export"}
# How often each kind of an account's other figures is asked for. A failed
# one is asked again after as long, and its value is kept for two of these.
_INTERVALS = {
    "billing": BILLING_CACHE_INTERVAL,
    "days": BILL_DAYS_INTERVAL,
    "balance": BALANCE_INTERVAL,
    "prices": PRICES_INTERVAL,
}
_NAMES = {
    "billing": "billing period",
    "days": "bill's day totals",
    "balance": "balance",
    "prices": "prices",
}
# The INFO lines when a kind of figure is lost, by what it leaves unknown,
# and when it is back.
_EXTRA_LOST = {
    "accounts": "Meridian Energy couldn't refresh the list of accounts; it keeps "
    "using the one it has",
    "billing": "Meridian Energy couldn't get the billing period; the billing "
    "period sensors show unknown until it can",
    "billing_bill": "Meridian Energy couldn't get the billing period; the billing "
    "period and bill sensors show unknown until it can",
    "days": "Meridian Energy couldn't get the bill's day totals; the 'so far' "
    "sensors show unknown until it can",
    "days_all": "Meridian Energy couldn't get the bill's day totals; the bill "
    "sensors show unknown until it can",
    "balance": "Meridian Energy couldn't get the balance; Total due, Last bill "
    "and Last payment show unknown until it can",
    "prices": "Meridian Energy couldn't get the prices; Daily charge and Anytime "
    "price show unknown until it can",
    "prices_unanswered": "Meridian Energy couldn't get the prices; it asks again "
    "in a day",
}
_EXTRA_BACK = {
    "accounts": "Meridian Energy has refreshed the list of accounts again",
    "billing": "Meridian Energy is getting the billing period again",
    "days": "Meridian Energy is getting the bill's day totals again",
    "balance": "Meridian Energy is getting the balance again",
    "prices": "Meridian Energy is getting the prices again",
}
# How much each lost line leaves unknown: across accounts, the wider is said.
_LOST_RANK = {
    "billing": 0,
    "billing_bill": 1,
    "days": 0,
    "days_all": 1,
    "balance": 0,
    "prices_unanswered": 0,
    "prices": 1,
}

type UnitKey = tuple[str, str]


async def _async_wait(seconds: float) -> None:
    """Wait between requests; a seam tests patch."""
    await asyncio.sleep(seconds)


def _utcnow() -> datetime:
    """Return the current UTC time through a patchable seam."""
    return datetime.now(UTC)


class _NoAccountsError(Exception):
    """Meridian listed no active or dormant account, or no property, to follow."""


@dataclass(slots=True)
class _Cooldown:
    """When a fill date may be asked for again, its wait, and its first failure."""

    until: datetime
    wait: timedelta
    since: datetime


@dataclass(slots=True)
class _Memory:
    """
    What the coordinator keeps about a unit between syncs, in memory only.

    ``unreturned_*`` hold, for each New Zealand date asked for, the ended
    hours Meridian's last answer for it did not return. ``quality`` maps
    the hours of the last 21 days to their reading's quality.
    """

    daily_at: datetime | None = None
    unreturned_energy: dict[date, frozenset[datetime]] = field(default_factory=dict)
    unreturned_cost: dict[date, frozenset[datetime]] = field(default_factory=dict)
    cooldowns: dict[date, _Cooldown] = field(default_factory=dict)
    fill_stopped: bool = False
    fill_noted: bool = False
    fill_failure: str | None = None
    history_start: date | None = None
    quality: dict[datetime, str] = field(default_factory=dict)
    failure: str | None = None
    warned: bool = False
    quiet: bool = False
    complete: bool = False
    newest: datetime | None = None
    oldest: datetime | None = None


@dataclass(slots=True)
class _Unit:
    """One direction of one property, as the sync in progress sees it."""

    account_number: str
    property_id: str
    direction: str
    statistic_ids: tuple[str, str]
    names: tuple[str, str]
    place: str
    memory: _Memory
    # Whether it has readings, fetched in the sync in progress or stored,
    # and its recent dates came back, or readings came before a rate limit.
    imported: bool = False

    @property
    def kind(self) -> str:
        """Return the unit's direction as logs name it, as ``grid import``."""
        return _LABELS[self.direction]

    @property
    def label(self) -> str:
        """Return what logs call the unit, as ``grid import for property 2 of 3``."""
        return f"{self.kind}{self.place}"


@dataclass(slots=True)
class _Fetched:
    """A unit's hours fetched in this sync: kWh and NZD, by UTC hour."""

    energy: dict[datetime, Decimal] = field(default_factory=dict)
    cost: dict[datetime, Decimal] = field(default_factory=dict)
    windows: int = 0


@dataclass(slots=True)
class _Extra:
    """
    One kind of an account's other figures: the last value got, and when.

    ``due`` is when it is next asked for, None for the next update;
    ``failing`` says whether the last time it was asked for failed.
    """

    value: Any = None
    at: datetime | None = None
    due: datetime | None = None
    failing: bool = False

    def kept(self, kind: str, now: datetime) -> Any:
        """Return the value while it is kept, two intervals after it was got."""
        if self.at is None or now - self.at >= 2 * _INTERVALS[kind]:
            return None
        return self.value


class _Bill(NamedTuple):
    """A bill's figures from the day totals, and the period they are for."""

    start: date
    end: date
    figures: BillFigures


class _Outcome(NamedTuple):
    """How a sync went: the units that imported, and those that failed."""

    ok: list[_Unit]
    failed: list[tuple[_Unit, Exception]]


class MeridianDataCoordinator(DataUpdateCoordinator[MeridianSyncData]):
    """Fetch Meridian data and import it into the recorder."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: MeridianApiClient,
        *,
        config_entry: ConfigEntry | None = None,
    ) -> None:
        """Create a coordinator for every active or dormant account of a login."""
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=NAME,
            update_interval=UPDATE_INTERVAL,
        )
        self.client = client
        self._topology: tuple[MeridianAccount, ...] | None = None
        self._topology_cached_at: datetime | None = None
        self._extras: dict[tuple[str, str], _Extra] = {}
        self._memory: dict[UnitKey, _Memory] = {}
        # Each kind lost, and the key of the INFO line that said so.
        self._extras_lost: dict[str, str] = {}
        self._synced = False
        self._first_sync_failures = self._rate_limits = self._failed_updates = 0
        self._rate_limit: MeridianRateLimitError | None = None
        self._requests = 0
        self._last_request: float | None = None

    @property
    def accounts(self) -> tuple[MeridianAccount, ...]:
        """Return the cached accounts, none before they were first fetched."""
        return self._topology or ()

    def extras_diagnostics(self) -> dict[str, dict[str, Any]]:
        """Return, for each kind of other figure, how many fail and the oldest age."""
        now = _utcnow()
        kinds: dict[str, dict[str, Any]] = {}
        for kind in _INTERVALS:
            extras = [
                extra for (name, _number), extra in self._extras.items() if name == kind
            ]
            ages = [
                max(0.0, (now - extra.at).total_seconds())
                for extra in extras
                if extra.at is not None
            ]
            kinds[kind] = {
                "failing": sum(extra.failing for extra in extras),
                "oldest_age_seconds": round(max(ages), 3) if ages else None,
            }
        return kinds

    def unit_diagnostics(self) -> list[dict[str, Any]]:
        """Return each unit's state, by position, never by identifier."""
        accounts = self.accounts
        target = local_midnight(nz_date(_utcnow()) - _DAY * HISTORY_DAYS)
        units: list[dict[str, Any]] = []
        for account_index, account in enumerate(accounts, start=1):
            for property_index, property_data in enumerate(account.properties, start=1):
                key = property_key(account.number, property_data.id)
                for direction, _ids in _directions(key, property_data):
                    memory = self._memory.get((key, direction), _Memory())
                    units.append(
                        {
                            "account": f"{account_index}/{len(accounts)}",
                            "property": (f"{property_index}/{len(account.properties)}"),
                            "direction": direction.lower(),
                            "newest_stored_hour": memory.newest,
                            "oldest_stored_hour": memory.oldest,
                            "target": target,
                            "history_complete": memory.complete,
                            "fill_stopped": memory.fill_stopped,
                            "fill_failure": memory.fill_failure,
                            "quiet": memory.quiet,
                            "last_failure": memory.failure,
                        }
                    )
        return units

    async def async_prepare_topology(self) -> None:
        """
        Renew the stored session and cache every active or dormant account.

        This is the only Meridian work done during config-entry setup. Errors
        are raised as config-entry exceptions, so Home Assistant starts
        reauthentication or retries setup with its own backoff.
        """
        try:
            await self._async_get_topology(_utcnow())
        except MeridianAuthenticationError as err:
            raise ConfigEntryAuthFailed(
                translation_domain=DOMAIN, translation_key="invalid_auth"
            ) from err
        except (_NoAccountsError, MeridianError, ValueError) as err:
            raise ConfigEntryNotReady(
                translation_domain=DOMAIN,
                translation_key=_error_key(err),
                translation_placeholders={"detail": _error_detail(err)},
            ) from err

    async def _async_update_data(self) -> MeridianSyncData:
        """
        Run one sync; raise ``UpdateFailed`` when no unit imported.

        Until a first sync has succeeded, a failed one is tried again after
        10, 30 and 60 minutes, then hourly; a rate limit waits as Meridian
        asked, twice as long for each in a row, up to a day. After three
        failed updates in a row, or at once when there is no account, a
        repair notice says updates are failing; the next success removes it.
        """
        first_sync = not self._synced
        # A wait after a rate limit holds for the update after it alone.
        self.update_interval = UPDATE_INTERVAL
        try:
            data = await self._async_sync(_utcnow())
        except MeridianAuthenticationError as err:
            raise ConfigEntryAuthFailed(
                translation_domain=DOMAIN, translation_key="invalid_auth"
            ) from err
        except (_NoAccountsError, MeridianError, ValueError) as err:
            failure = UpdateFailed(
                translation_domain=DOMAIN,
                translation_key=_error_key(err),
                translation_placeholders={"detail": _error_detail(err)},
                retry_after=self._failure_wait(err, first_sync=first_sync),
            )
            self._note_failed_update(
                failure, no_accounts=_error_key(err) == "no_accounts"
            )
            raise failure from err
        self._synced = True
        self._failed_updates = 0
        if self.config_entry is not None:
            ir.async_delete_issue(self.hass, DOMAIN, self._issue_id)
        return data

    @property
    def _issue_id(self) -> str:
        entry_id = self.config_entry.entry_id if self.config_entry else ""
        return f"sync_failing_{entry_id}"

    def _note_failed_update(self, failure: UpdateFailed, *, no_accounts: bool) -> None:
        """
        Count a failed update, and raise the repair notice when it is due.

        Counted here: Home Assistant does not call listeners for a failure
        that follows a failure.
        """
        self._failed_updates += 1
        if self.config_entry is None or not (
            no_accounts or self._failed_updates >= FAILED_UPDATES_BEFORE_ISSUE
        ):
            return
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            self._issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.WARNING,
            translation_key="sync_failing",
            translation_placeholders={"reason": str(failure)},
        )

    def _failure_wait(self, err: Exception, *, first_sync: bool) -> float | None:
        """Return when a failed update is tried again, None for the usual hour."""
        if isinstance(err, MeridianRateLimitError):
            return self._rate_limit_wait(err)
        if not first_sync:
            return None
        wait = FIRST_SYNC_RETRY_SCHEDULE[
            min(self._first_sync_failures, len(FIRST_SYNC_RETRY_SCHEDULE) - 1)
        ]
        self._first_sync_failures += 1
        return wait

    def _rate_limit_wait(self, err: MeridianRateLimitError) -> float:
        """Return Meridian's wait, doubled for each rate limit in a row, up to a day."""
        self._rate_limits += 1
        doubled = err.retry_after * 2 ** min(5, self._rate_limits - 1)
        return float(min(MAX_RETRY_AFTER_SECONDS, doubled))

    async def _async_sync(self, now: datetime) -> MeridianSyncData:
        """Sync every unit, then the accounts' other figures, and summarise."""
        started = monotonic()
        self._requests = 0
        self._last_request = None
        self._rate_limit = None
        accounts = await self._async_get_topology(now)
        units = self._units(accounts)
        if not units:
            raise _NoAccountsError
        outcome = _Outcome([], [])
        for unit in units:
            try:
                error = await self._async_sync_unit(unit, now)
            except MeridianRateLimitError as err:
                self._rate_limit = err
                if unit.imported:
                    outcome.ok.append(unit)
                break
            if error is not None:
                outcome.failed.append((unit, error))
            elif unit.imported:
                outcome.ok.append(unit)
            else:
                # A unit without readings, quiet or not, counts as neither.
                unit.memory.failure = None
        self._settle(outcome)
        failed = {
            (property_key(unit.account_number, unit.property_id), unit.direction)
            for unit, _err in outcome.failed
        }
        account_results = await self._async_account_results(accounts, failed, now)
        if self._rate_limit is None:
            self._rate_limits = 0
        else:
            wait = timedelta(seconds=self._rate_limit_wait(self._rate_limit))
            self.update_interval = max(UPDATE_INTERVAL, wait)
        return MeridianSyncData(
            account_count=len(accounts),
            property_count=sum(len(account.properties) for account in accounts),
            results=tuple(
                self._property_result(account, property_data)
                for account in accounts
                for property_data in account.properties
            ),
            account_results=account_results,
            synced_at=now,
            sync_duration_seconds=monotonic() - started,
            requests=self._requests,
        )

    def _settle(self, outcome: _Outcome) -> None:
        """
        Decide whether a sync succeeded, and log what changed for its units.

        It succeeds when a unit with readings imported its recent dates, or
        none failed, as when every unit is quiet: a unit without readings
        never counts as imported, and counts as failed only when its recent
        dates fail. Then a unit that failed logs one WARNING, until
        it works again, which logs one INFO. Otherwise it raises the rate
        limit that stopped it, or its first failure: Home Assistant logs
        that one.
        """
        for unit, err in outcome.failed:
            unit.memory.failure = _error_key(err)
        if not outcome.ok:
            if self._rate_limit is not None:
                raise self._rate_limit
            if outcome.failed:
                raise outcome.failed[0][1]
        for unit, err in outcome.failed:
            if not unit.memory.warned:
                unit.memory.warned = True
                _LOGGER.warning(
                    "Meridian Energy couldn't update %s (%s); the rest keeps updating",
                    unit.label,
                    _error_phrase(err),
                )
        for unit in outcome.ok:
            unit.memory.failure = None
            if unit.memory.warned:
                unit.memory.warned = False
                _LOGGER.info("Meridian Energy is updating %s again", unit.label)

    def _units(self, accounts: tuple[MeridianAccount, ...]) -> list[_Unit]:
        """Return every unit of the accounts, in order, with its memory."""
        count = sum(len(account.properties) for account in accounts)
        units: list[_Unit] = []
        number = 0
        for account in accounts:
            for property_data in account.properties:
                number += 1
                key = property_key(account.number, property_data.id)
                suffix = place = ""
                if count > 1:
                    suffix = f" — {_one_line_address(property_data.address)}"
                    place = f" for property {number} of {count}"
                for direction, ids in _directions(key, property_data):
                    units.append(
                        _Unit(
                            account_number=account.number,
                            property_id=property_data.id,
                            direction=direction,
                            statistic_ids=ids,
                            names=_statistic_names(direction, suffix),
                            place=place,
                            memory=self._memory.setdefault((key, direction), _Memory()),
                        )
                    )
        return units

    async def _async_sync_unit(self, unit: _Unit, now: datetime) -> Exception | None:
        """
        Fetch a unit's recent dates and the missing ones, then write them.

        Return the failure that stopped its recent dates, else None; a quiet
        unit sends no request. What was fetched is written whatever
        happened, a rate limit or an ended sign-in included, which then go
        on up and stop the sync.
        """
        memory = unit.memory
        if memory.quiet:
            return None
        fetched = _Fetched()
        error: Exception | None = None
        try:
            await self._async_fetch_unit(unit, fetched, now)
        except (MeridianError, ValueError) as err:
            error = err
        await self._async_write(unit, fetched)
        unit.imported = bool(fetched.energy) or (
            unit.imported and memory.newest is not None
        )
        today = nz_date(now)
        oldest_quality = local_midnight(today - _DAY * (DAILY_DATES - 1))
        memory.quality = {
            hour: quality
            for hour, quality in memory.quality.items()
            if hour >= oldest_quality
        }
        target = today - _DAY * HISTORY_DAYS
        for kept in (
            memory.unreturned_energy,
            memory.unreturned_cost,
            memory.cooldowns,
        ):
            for day in [day for day in kept if day < target]:
                del kept[day]
        _LOGGER.debug(
            "Synced %s: %d windows, %d hours fetched%s",
            unit.label,
            fetched.windows,
            len(fetched.energy),
            "" if error is None else f", stopped by {type(error).__name__}",
        )
        if isinstance(error, MeridianRateLimitError | MeridianAuthenticationError):
            raise error
        return error

    async def _async_fetch_unit(
        self, unit: _Unit, fetched: _Fetched, now: datetime
    ) -> None:
        """
        Fetch a unit's recent dates, newest first, then fill in older ones.

        The last 21 dates when the daily request has not succeeded for a
        day, as after a start, else the last 3. A recent window that fails
        after its resend raises, but a refused one after readings, with no
        older hour stored, marks where the history starts. A unit whose
        search to its target found nothing at all is quiet until the
        accounts are next refreshed.
        """
        memory = unit.memory
        today = nz_date(now)
        daily = memory.daily_at is None or now - memory.daily_at >= DAILY_INTERVAL
        dates = DAILY_DATES if daily else RECENT_DATES
        recent_start = today - _DAY * (dates - 1)
        for window in windows_for(today - _DAY * offset for offset in range(dates)):
            try:
                await self._async_fetch(unit, window, fetched, now)
            except MeridianRateLimitError, MeridianAuthenticationError:
                raise
            except (MeridianError, ValueError) as err:
                if not (
                    fetched.energy
                    and _is_refusal(err)
                    and not await self._async_stored_before(unit, window, today)
                ):
                    raise
                self._stop_fill(unit)
                break
        unit.imported = True
        if daily:
            memory.daily_at = now
        searched = await self._async_fill(unit, recent_start, fetched, now)
        if searched and not fetched.energy and memory.newest is None:
            memory.quiet = True
            _LOGGER.debug(
                "Found no readings for %s; it waits for the accounts", unit.label
            )

    async def _async_fill(
        self, unit: _Unit, recent_start: date, fetched: _Fetched, now: datetime
    ) -> bool:
        """
        Fetch the dates missing from the target to the recent ones, newest first.

        A date is missing when one of its hours is absent from either
        statistic and Meridian's last answer for that date did not leave it
        out. Dates waiting after a failure are skipped, and a wait ends once
        its date is no longer missing. Return whether the fill searched the
        whole way to the target without a failure; the history is complete
        when it did and was not stopped.
        """
        memory = unit.memory
        memory.complete = False
        if memory.fill_stopped:
            return True
        target = nz_date(now) - _DAY * HISTORY_DAYS
        energy_id, cost_id = unit.statistic_ids
        try:
            stored = await async_stored_hours(
                self.hass,
                {energy_id, cost_id},
                local_midnight(target),
                local_midnight(recent_start),
            )
        except SQLAlchemyError:
            _LOGGER.debug(
                "Couldn't read the stored %s hours%s; no fill", unit.kind, unit.place
            )
            return False
        energy, cost = stored[energy_id], stored[cost_id]
        _note_stored(memory, energy)
        missing = [
            day
            for day in _dates(max(target, memory.history_start or target), recent_start)
            if _missing(memory, day, energy, cost)
        ]
        waiting = set(missing)
        memory.cooldowns = {
            day: cooldown
            for day, cooldown in memory.cooldowns.items()
            if day in waiting
        }
        if not memory.cooldowns:
            memory.fill_noted = False
            memory.fill_failure = None
        ready = [
            day
            for day in missing
            if (cooldown := memory.cooldowns.get(day)) is None or cooldown.until <= now
        ]
        windows = windows_for(ready)
        oldest = min((*energy, *fetched.energy), default=None)
        searched = (
            await self._async_fill_windows(
                unit, windows[:FILL_WINDOWS], oldest, fetched, now
            )
            and len(windows) <= FILL_WINDOWS
            and ready == missing
        )
        memory.complete = searched and not memory.fill_stopped
        return searched

    async def _async_fill_windows(
        self,
        unit: _Unit,
        windows: Iterable[MeasurementWindow],
        oldest: datetime | None,
        fetched: _Fetched,
        now: datetime,
    ) -> bool:
        """
        Fetch fill windows until one fails or the history's start shows.

        A refusal of a window older than every reading stops the fill until
        a restart; any other failure makes the window wait and ends the fill
        for this sync (False). Two answered windows in a row with no
        readings, older than every reading found, mark where the history
        starts: older dates are not missing for the rest of this run.
        """
        memory = unit.memory
        newer_empty: MeasurementWindow | None = None
        for window in windows:
            try:
                readings = await self._async_fetch(unit, window, fetched, now)
            except MeridianRateLimitError, MeridianAuthenticationError:
                raise
            except (MeridianError, ValueError) as err:
                if _is_refusal(err) and (oldest is None or window.end <= oldest):
                    self._stop_fill(unit)
                    return True
                self._cool_down(unit, window, err, now)
                return False
            for day in window.dates():
                memory.cooldowns.pop(day, None)
            if not memory.cooldowns:
                memory.fill_noted = False
                memory.fill_failure = None
            if readings:
                newer_empty = None
                oldest = min(readings) if oldest is None else min(oldest, *readings)
            elif oldest is not None and window.end <= oldest:
                if newer_empty is not None:
                    memory.history_start = newer_empty.end_on + _DAY
                    return True
                newer_empty = window
        return True

    def _stop_fill(self, unit: _Unit) -> None:
        """Stop filling in a unit's history until a restart: Meridian refused it."""
        if unit.memory.fill_stopped:
            return
        unit.memory.fill_stopped = True
        _LOGGER.info(
            "Meridian Energy stopped filling in older %s history%s until Home "
            "Assistant restarts: Meridian refused a request for older dates",
            unit.kind,
            unit.place,
        )

    async def _async_stored_before(
        self, unit: _Unit, window: MeasurementWindow, today: date
    ) -> bool:
        """Return whether a unit has energy stored from before a window, or may."""
        energy_id = unit.statistic_ids[0]
        target = local_midnight(today - _DAY * HISTORY_DAYS)
        try:
            stored = await async_stored_hours(
                self.hass, {energy_id}, target, window.start
            )
        except SQLAlchemyError:
            return True
        return bool(stored[energy_id])

    def _cool_down(
        self, unit: _Unit, window: MeasurementWindow, err: Exception, now: datetime
    ) -> None:
        """Make a failed fill window's dates wait, longer each time, up to a day."""
        memory = unit.memory
        wait = FILL_COOLDOWN
        since = now
        for day in window.dates():
            if (cooldown := memory.cooldowns.get(day)) is not None:
                wait = max(wait, min(MAX_FILL_COOLDOWN, cooldown.wait * 2))
                since = min(since, cooldown.since)
        for day in window.dates():
            memory.cooldowns[day] = _Cooldown(now + wait, wait, since)
        memory.fill_failure = f"{_error_key(err)} ({_error_detail(err)})"
        _LOGGER.debug(
            "Older %s history%s failed (%s); asking again in %d min",
            unit.kind,
            unit.place,
            _error_phrase(err),
            wait / timedelta(minutes=1),
        )
        if now - since >= _DAY and not memory.fill_noted:
            memory.fill_noted = True
            _LOGGER.info(
                "Meridian Energy has been unable to fetch some older %s history%s "
                "for a day; it keeps trying",
                unit.kind,
                unit.place,
            )

    async def _async_fetch(
        self, unit: _Unit, window: MeasurementWindow, fetched: _Fetched, now: datetime
    ) -> dict[datetime, Reading]:
        """
        Fetch one window, keep its readings, and note the hours it left out.

        For each of its dates, the ended hours the answer did not return,
        for energy and for cost, replace what was noted for that date.
        """
        rows = await self._async_request(unit, window, now)
        readings = hourly_readings(rows, window, now)
        fetched.windows += 1
        memory = unit.memory
        for hour, reading in readings.items():
            fetched.energy[hour] = reading.energy
            if reading.cost_cents is not None:
                fetched.cost[hour] = reading.cost_cents / _CENTS_PER_DOLLAR
            memory.quality[hour] = reading.quality
        for day in window.dates():
            ended = [hour for hour in date_hours(day) if hour + HOUR <= now]
            memory.unreturned_energy[day] = frozenset(
                hour for hour in ended if hour not in readings
            )
            memory.unreturned_cost[day] = frozenset(
                hour
                for hour in ended
                if hour not in readings or readings[hour].cost_cents is None
            )
        return readings

    async def _async_request(
        self, unit: _Unit, window: MeasurementWindow, now: datetime
    ) -> tuple[MeridianMeasurement, ...]:
        """Ask for one window, resending once after a failure that may pass."""
        today = nz_date(now)
        context = (
            f"{unit.kind.replace(' ', '_')} "
            f"window={(window.start_on - today).days}..{(window.end_on - today).days}d"
        )
        for attempt in range(_SENDS):
            try:
                return await self.client.async_get_window(
                    account_number=unit.account_number,
                    property_id=unit.property_id,
                    direction=unit.direction,
                    start_on=window.start_on,
                    end_on=window.end_on,
                    last=window.last,
                    pace=self._async_pace,
                    context=context,
                )
            except MeridianConnectionError as err:
                if attempt == _SENDS - 1 or not _is_transient(err):
                    raise
                _LOGGER.debug(
                    "Resending a request that failed (%s)", _error_detail(err)
                )
                await _async_wait(random.uniform(*TRANSIENT_RESEND_DELAY_SECONDS))  # noqa: S311
        raise AssertionError  # pragma: no cover - the loop returns or raises

    async def _async_pace(self) -> None:
        """Keep the starts of measurements requests at least 1 s apart."""
        if self._last_request is not None:
            wait = self._last_request + MEASUREMENTS_PACING_SECONDS - monotonic()
            if wait > 0:
                await _async_wait(wait)
        self._last_request = monotonic()
        self._requests += 1

    async def _async_write(self, unit: _Unit, fetched: _Fetched) -> None:
        """Write a unit's fetched hours, energy and cost in one call."""
        if not fetched.energy and not fetched.cost:
            return
        energy_id, cost_id = unit.statistic_ids
        energy_name, cost_name = unit.names
        try:
            written = await async_write_hours(
                self.hass,
                [
                    (energy_metadata(energy_id, energy_name), fetched.energy),
                    (cost_metadata(cost_id, cost_name), fetched.cost),
                ],
            )
        except SQLAlchemyError:
            _LOGGER.debug(
                "Couldn't read the stored %s statistics%s; none written",
                unit.kind,
                unit.place,
            )
            written = False
        if written:
            _note_stored(unit.memory, fetched.energy)
        else:
            unit.memory.complete = False

    async def _async_get_topology(self, now: datetime) -> tuple[MeridianAccount, ...]:
        """
        Return every account to follow, asking Meridian once they are a day old.

        When asking fails with a cached list, the cached one is kept and
        asked for again at the next update; a rate limit or an ended sign-in
        still stops the sync. An empty list is never cached. Fresh accounts
        wake quiet units.
        """
        cached_at = self._topology_cached_at
        if (
            self._topology is not None
            and cached_at is not None
            and now - cached_at < TOPOLOGY_CACHE_INTERVAL
        ):
            return self._topology
        try:
            accounts = await self.client.async_get_accounts()
        except MeridianRateLimitError, MeridianAuthenticationError:
            raise
        except MeridianError, ValueError:
            if self._topology is None:
                raise
            self._note_extra("accounts", "accounts")
            return self._topology
        self._note_extra("accounts", None)
        if not accounts:
            raise _NoAccountsError
        self._topology = accounts
        self._topology_cached_at = now
        self._prune(accounts)
        return accounts

    def _prune(self, accounts: tuple[MeridianAccount, ...]) -> None:
        """Forget accounts and units Meridian no longer lists; wake the rest."""
        numbers = {account.number for account in accounts}
        units = {
            (property_key(account.number, property_data.id), direction)
            for account in accounts
            for property_data in account.properties
            for direction, _ids in _directions(
                property_key(account.number, property_data.id), property_data
            )
        }
        self._memory = {
            key: value for key, value in self._memory.items() if key in units
        }
        for memory in self._memory.values():
            memory.quiet = False
        self._extras = {
            key: extra for key, extra in self._extras.items() if key[1] in numbers
        }

    def _note_extra(self, kind: str, lost: str | None) -> None:
        """
        Log at INFO when a kind is lost, and once when it is back.

        ``lost`` is the key of the line that says what is unknown, None when
        nothing is; a kind already lost logs again only when that changes.
        """
        if lost is None:
            if self._extras_lost.pop(kind, None) is not None:
                _LOGGER.info(_EXTRA_BACK[kind])
        elif self._extras_lost.get(kind) != lost:
            self._extras_lost[kind] = lost
            _LOGGER.info(_EXTRA_LOST[lost])

    def _property_result(
        self, account: MeridianAccount, property_data: MeridianProperty
    ) -> PropertySyncResult:
        """Return a property's summary, from its grid import's reading qualities."""
        key = property_key(account.number, property_data.id)
        memory = self._memory.get((key, READING_CONSUMPTION), _Memory())
        estimated = sorted(
            hour for hour, quality in memory.quality.items() if quality != _ACTUAL
        )
        return PropertySyncResult(
            property_key=key,
            account_key=account_key(account.number),
            latest_reading=max(memory.quality) + HOUR if memory.quality else None,
            estimated_rows=len(estimated),
            oldest_estimated=estimated[0] if estimated else None,
            newest_estimated=estimated[-1] if estimated else None,
            quality_counts=tuple(sorted(Counter(memory.quality.values()).items())),
        )

    async def _async_extra(
        self,
        kind: str,
        account_number: str,
        now: datetime,
        fetch: Callable[[], Awaitable[Any]],
        *,
        due: bool = False,
    ) -> _Extra:
        """
        Ask for one kind of an account's figures when it is due; return it.

        A failure keeps what was got and asks again after the kind's
        interval. A rate limit fails nothing: it makes the next update wait,
        and no more requests go out in this one. An ended sign-in stops the
        sync.
        """
        extra = self._extras.setdefault((kind, account_number), _Extra())
        if self._rate_limit is not None or not (
            due or extra.due is None or now >= extra.due
        ):
            return extra
        try:
            extra.value = await fetch()
        except MeridianAuthenticationError:
            raise
        except MeridianRateLimitError as err:
            self._rate_limit = err
            return extra
        except (MeridianError, ValueError) as err:
            extra.failing = True
            _LOGGER.debug("Couldn't get the %s (%s)", _NAMES[kind], _error_detail(err))
        else:
            extra.at = now
            extra.failing = False
        extra.due = now + _INTERVALS[kind]
        return extra

    async def _async_account_results(
        self,
        accounts: tuple[MeridianAccount, ...],
        failed: set[UnitKey],
        now: datetime,
    ) -> tuple[AccountSyncResult, ...]:
        """Return every account's figures, then log each kind lost or back."""
        lost: dict[str, str] = {}
        results = [
            await self._async_account_result(account, failed, now, lost)
            for account in accounts
        ]
        for kind in _INTERVALS:
            if kind in lost:
                self._note_extra(kind, lost[kind])
            elif not any(
                extra.failing
                for (name, _number), extra in self._extras.items()
                if name == kind
            ):
                self._note_extra(kind, None)
        return tuple(results)

    async def _async_account_result(
        self,
        account: MeridianAccount,
        failed: set[UnitKey],
        now: datetime,
        lost: dict[str, str],
    ) -> AccountSyncResult:
        """
        Return an account's figures, asking for those that are due.

        A billing period is withheld once it has ended. It is asked for again
        at the first update after its end, and every 6 hours until a new one
        comes, both decided from the last period Meridian gave; until a new
        one shows, the billing period is not reported back. ``lost`` gathers
        the kinds that failed and show unknown, each with the key of the line
        that says what is unknown.
        """
        number = account.number
        today = nz_date(now)
        billing = await self._async_extra(
            "billing",
            number,
            now,
            partial(self.client.async_get_billing_period, number),
        )
        last: MeridianBillingPeriod | None = billing.value
        ended = last is not None and last.end is not None and today > last.end
        period: MeridianBillingPeriod | None = (
            None if ended else billing.kept("billing", now)
        )
        if last is not None and last.end is not None and billing.due is not None:
            billing.due = min(
                billing.due,
                now + BILL_DAYS_INTERVAL if ended else local_midnight(last.end + _DAY),
            )
        bill = await self._async_bill(account, period, failed, now, lost)
        if period is None and (
            billing.failing or (ended and "billing" in self._extras_lost)
        ):
            _lose(
                lost, "billing", "billing_bill" if bill == BillFigures() else "billing"
            )
        extra = await self._async_extra(
            "balance", number, now, partial(self.client.async_get_balance, number)
        )
        balance: MeridianBalance | None = extra.kept("balance", now)
        if extra.failing and balance is None:
            _lose(lost, "balance", "balance")
        price_sensors, prices = await self._async_prices(account, now, lost)
        result = AccountSyncResult(
            account_key=account_key(number),
            billing_period=period,
            has_feed_in=_has_feed_in(account),
            bill=bill,
            price_sensors=price_sensors,
            prices=prices,
        )
        if balance is None:
            return result
        cents = balance.balance_cents
        return replace(
            result,
            total_due=Decimal(max(0, -cents)) / _CENTS_PER_DOLLAR,
            credit=Decimal(cents) / _CENTS_PER_DOLLAR if cents > 0 else None,
            last_bill=max(balance.bills, key=attrgetter("issued"), default=None),
            last_payment=max(
                (payment for payment in balance.payments if payment.paid_on <= today),
                key=attrgetter("paid_on"),
                default=None,
            ),
        )

    async def _async_bill(
        self,
        account: MeridianAccount,
        period: MeridianBillingPeriod | None,
        failed: set[UnitKey],
        now: datetime,
        lost: dict[str, str],
    ) -> BillFigures:
        """
        Return the bill's figures, asking for its day totals when due.

        They need every property's, so they are not asked for while one of
        the account's properties failed its recent grid import dates in this
        sync, and are asked for at once when the period changes. Once their period has
        ended they are withheld; until then the 'so far' figures are kept
        for two intervals, and the estimate throughout.
        """
        number = account.number
        extra = self._extras.get(("days", number))
        if (
            period is not None
            and period.start is not None
            and period.end is not None
            and not any(
                (property_key(number, item.id), READING_CONSUMPTION) in failed
                for item in account.properties
            )
        ):
            known: _Bill | None = extra.value if extra is not None else None
            extra = await self._async_extra(
                "days",
                number,
                now,
                partial(
                    self._async_bill_days, account, period.start, period.end, failed
                ),
                due=known is not None
                and (known.start, known.end) != (period.start, period.end)
                and not (extra is not None and extra.failing),
            )
        if extra is None:
            return BillFigures()
        bill: _Bill | None = extra.value
        current = bill is not None and nz_date(now) <= bill.end
        kept = current and extra.kept("days", now) is not None
        if extra.failing and not kept:
            _lose(lost, "days", "days" if current else "days_all")
        if bill is None or not current:
            return BillFigures()
        return bill.figures if kept else BillFigures(estimate=bill.figures.estimate)

    async def _async_bill_days(
        self,
        account: MeridianAccount,
        start_on: date,
        end_on: date,
        failed: set[UnitKey],
    ) -> _Bill:
        """
        Return a bill's figures from every property's day totals.

        'So far' counts the actual days only; the estimate adds every day's
        cost, future estimates included. The export and its credit are asked
        for after the rest, and are unknown when a property's grid export
        failed in this sync or its day totals fail.
        """
        number = account.number
        days = await self._async_days(account, start_on, end_on, READING_CONSUMPTION)
        used = [day for day in days if day.quality == _ACTUAL]
        figures = BillFigures(
            usage=sum((day.energy for day in used), Decimal(0)),
            cost=_dollars(used),
            estimate=_dollars(days),
        )
        if not _has_feed_in(account):
            return _Bill(start_on, end_on, figures)
        if any(
            (property_key(number, item.id), READING_GENERATION) in failed
            for item in account.properties
        ):
            _LOGGER.debug("Not asking for the bill's grid export: it failed")
            return _Bill(start_on, end_on, figures)
        try:
            days = await self._async_days(account, start_on, end_on, READING_GENERATION)
        except MeridianAuthenticationError, MeridianRateLimitError:
            raise
        except (MeridianError, ValueError) as err:
            _LOGGER.debug(
                "Couldn't get the bill's grid export (%s)", _error_detail(err)
            )
            return _Bill(start_on, end_on, figures)
        sent = [day for day in days if day.quality == _ACTUAL]
        figures = replace(
            figures,
            export=sum((day.energy for day in sent), Decimal(0)),
            credit=_dollars(sent),
        )
        return _Bill(start_on, end_on, figures)

    async def _async_days(
        self, account: MeridianAccount, start_on: date, end_on: date, direction: str
    ) -> list[Reading]:
        """Return one direction's day totals from each property that has it."""
        days: list[Reading] = []
        for property_data in account.properties:
            key = property_key(account.number, property_data.id)
            if direction not in dict(_directions(key, property_data)):
                continue
            rows = await self.client.async_get_window(
                account_number=account.number,
                property_id=property_data.id,
                direction=direction,
                start_on=start_on,
                end_on=end_on,
                last=(end_on - start_on).days + 1,
                frequency=READING_FREQUENCY_DAY,
                pace=self._async_pace,
                context=f"{_LABELS[direction].replace(' ', '_')} bill_days",
            )
            days.extend(day_readings(rows, start_on, end_on).values())
        return days

    async def _async_prices(
        self, account: MeridianAccount, now: datetime, lost: dict[str, str]
    ) -> tuple[bool | None, MeridianPrices | None]:
        """
        Return whether an account has price sensors, and their prices.

        Only an account with one property and one meter point is asked; the
        answer says whether its plan is all-day. Until an answer has come,
        there is no saying (None): sensors already registered stay, showing
        unknown.
        """
        properties = account.properties
        if len(properties) != 1 or len(properties[0].meter_points) != 1:
            return False, None
        extra = await self._async_extra(
            "prices",
            account.number,
            now,
            partial(self.client.async_get_prices, account.number, properties[0].id),
        )
        if extra.at is None:
            if extra.failing:
                _lose(lost, "prices", "prices_unanswered")
            return None, None
        if extra.value is None:
            return False, None
        prices: MeridianPrices | None = extra.kept("prices", now)
        if extra.failing and prices is None:
            _lose(lost, "prices", "prices")
        return True, prices


def _lose(lost: dict[str, str], kind: str, key: str) -> None:
    """Note a kind lost, keeping the line that leaves the most unknown."""
    lost[kind] = max(lost.get(kind, key), key, key=_LOST_RANK.__getitem__)


def _note_stored(memory: _Memory, hours: Collection[datetime]) -> None:
    """Move a unit's oldest and newest stored hours out to cover some hours."""
    if not hours:
        return
    oldest, newest = min(hours), max(hours)
    memory.oldest = oldest if memory.oldest is None else min(memory.oldest, oldest)
    memory.newest = newest if memory.newest is None else max(memory.newest, newest)


def _dates(first: date, end: date) -> Iterable[date]:
    """Return the dates from ``first`` until ``end``, which is left out."""
    return (first + _DAY * offset for offset in range((end - first).days))


def _missing(
    memory: _Memory, day: date, energy: frozenset[datetime], cost: frozenset[datetime]
) -> bool:
    """Return whether either statistic lacks an hour of a date Meridian may return."""
    no_energy = memory.unreturned_energy.get(day, frozenset())
    no_cost = memory.unreturned_cost.get(day, frozenset())
    return any(
        (hour not in energy and hour not in no_energy)
        or (hour not in cost and hour not in no_cost)
        for hour in date_hours(day)
    )


def _is_transient(err: MeridianConnectionError) -> bool:
    """Return whether a failure may pass on its own, so a resend may succeed."""
    if isinstance(err, MeridianRateLimitError | MeridianRejectedError):
        return False
    if isinstance(err, MeridianUpstreamError):
        return err.status is None or err.status in TRANSIENT_STATUSES
    return http_status(err) is None


def _is_refusal(err: Exception) -> bool:
    """Return whether Meridian refused a request: a coded error or a client error."""
    if isinstance(err, MeridianGraphQLError):
        return not is_service_failure(err.codes) and not QUERY_LIMIT_CODES.intersection(
            err.codes
        )
    return isinstance(err, MeridianRejectedError)


def _error_key(err: BaseException) -> str:
    """Return the translation key of a failure's category."""
    if isinstance(err, _NoAccountsError):
        return "no_accounts"
    if isinstance(err, MeridianRateLimitError):
        return "rate_limited"
    if isinstance(err, MeridianRejectedError) or _unreadable(err):
        return "invalid_data"
    if isinstance(err, MeridianConnectionError):
        return "cannot_connect"
    if isinstance(err, MeridianGraphQLError) and is_service_failure(err.codes):
        return "cannot_connect"
    return "invalid_data"


def _error_detail(err: BaseException) -> str:
    """Describe a failure without identifiers, as in ``HTTP 504``."""
    if isinstance(err, MeridianUpstreamError | MeridianRejectedError):
        return err.detail
    if isinstance(err, MeridianGraphQLError):
        return ", ".join(shown_codes(err.codes)) or "a Meridian service error"
    if _unreadable(err):
        return "not JSON"
    if isinstance(err, MeridianConnectionError):
        status = http_status(err)
        return "network error" if status is None else f"HTTP {status}"
    if isinstance(err, _NoAccountsError):
        return "no account"
    return "unexpected data"


def _unreadable(err: BaseException) -> bool:
    """Return whether a failure is an answer that is not JSON, as a web page."""
    return any(
        isinstance(item, MeridianUnreadableResponseError)
        for item in exception_chain(err)
    )


def _error_phrase(err: Exception) -> str:
    """Describe a unit's failure for its WARNING, as ``no answer: HTTP 504``."""
    if _error_key(err) == "cannot_connect":
        return f"no answer from Meridian: {_error_detail(err)}"
    return (
        "Meridian refused the request or sent an answer Meridian Energy can't "
        f"read: {_error_detail(err)}"
    )


def _dollars(days: Collection[Reading]) -> Decimal | None:
    """Return days' cost in NZD, None when one of them has none."""
    cents = [day.cost_cents for day in days]
    if None in cents:
        return None
    return sum((cost for cost in cents if cost is not None), Decimal(0)) / (
        _CENTS_PER_DOLLAR
    )


def _has_feed_in(account: MeridianAccount) -> bool:
    """Return whether any of an account's meters has an active feed-in register."""
    return any(
        meter.has_feed_in
        for property_data in account.properties
        for meter in property_data.meter_points
    )


def _directions(
    key: str, property_data: MeridianProperty
) -> list[tuple[str, tuple[str, str]]]:
    """Return a property's directions, with their energy and money statistic IDs."""
    directions = [(READING_CONSUMPTION, consumption_ids(key))]
    if any(meter.has_feed_in for meter in property_data.meter_points):
        directions.append((READING_GENERATION, generation_ids(key)))
    return directions


def _statistic_names(direction: str, suffix: str) -> tuple[str, str]:
    """Return the names of a unit's energy and money statistics."""
    if direction == READING_CONSUMPTION:
        return f"Meridian grid import{suffix}", f"Meridian grid import cost{suffix}"
    return f"Meridian grid export{suffix}", f"Meridian grid export credit{suffix}"


def _one_line_address(value: str) -> str:
    """Normalize a local-only property label for display."""
    return " ".join(value.split())
