"""Data coordinator for Meridian Energy statistics."""

from __future__ import annotations

import logging
import math
from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from time import monotonic
from zoneinfo import ZoneInfo

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CoreState, HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianError,
    MeridianGraphQLError,
    MeridianRateLimitError,
)
from .const import (
    BILLING_CACHE_INTERVAL,
    CONF_AUTO_ADD_ACCOUNTS,
    CONF_SELECTED_ACCOUNTS,
    DOMAIN,
    FULL_RECONCILIATION_INTERVAL,
    INITIAL_BACKFILL,
    MAX_MEASUREMENT_PAGES,
    MIN_PAGE_SIZE,
    NAME,
    PAGE_SIZE,
    READING_CONSUMPTION,
    READING_GENERATION,
    RECONCILIATION_SAFETY_MARGIN,
    REVISION_OVERLAP,
    TARGETED_RECONCILIATION_INTERVAL,
    TARGETED_RECONCILIATION_MINIMUM,
    TIP_WINDOW,
    TOPOLOGY_CACHE_INTERVAL,
    UPDATE_INTERVAL,
)
from .models import (
    AccountSyncResult,
    MeasurementFetchResult,
    MeridianAccount,
    MeridianBillingPeriod,
    MeridianMeasurement,
    MeridianProperty,
    MeridianSyncData,
    PropertySyncResult,
    SyncMode,
)
from .statistics import (
    account_key,
    async_account_period_totals,
    async_import_measurements,
    async_latest_numeric_statistic_start,
    consumption_ids,
    generation_ids,
    property_key,
)

_LOGGER = logging.getLogger(__name__)
_NZ = ZoneInfo("Pacific/Auckland")
_ACTUAL = "ACTUAL"
_TOPOLOGY_ERROR_CODES = frozenset(
    {"ACCOUNT_NOT_FOUND", "METER_NOT_FOUND", "PROPERTY_NOT_FOUND", "NOT_FOUND"}
)

MeasurementKey = tuple[datetime, str]
CacheKey = tuple[str, str]


def _utcnow() -> datetime:
    """Return the current UTC time through a patchable seam."""
    return datetime.now(UTC)


class MeridianDataCoordinator(DataUpdateCoordinator[MeridianSyncData]):
    """Fetch Meridian data and import it into the recorder."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: MeridianApiClient,
        *,
        config_entry: ConfigEntry | None = None,
        selected_accounts: frozenset[str] | None = None,
        auto_add_accounts: bool = False,
    ) -> None:
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
        self._topology: tuple[MeridianAccount, ...] | None = None
        self._topology_cached_at: datetime | None = None
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
        self._active_directions: set[CacheKey] = set()
        self._seen_directions: set[CacheKey] = set()
        self._initial_refresh_complete = False
        self._last_targeted_reconciliation: datetime | None = None
        self._last_full_reconciliation: datetime | None = None

    @property
    def accounts(self) -> tuple[MeridianAccount, ...]:
        """Return the selected, cached account topology."""
        return self._filtered_topology()

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

    async def _async_update_data(self) -> MeridianSyncData:
        return await self.async_fetch_and_import()

    async def async_fetch_and_import(self) -> MeridianSyncData:
        """Fetch Meridian data and import statistics for setup or polling."""
        try:
            data = await self._async_fetch_and_import_data()
        except MeridianAuthenticationError as err:
            raise ConfigEntryAuthFailed(
                translation_domain=DOMAIN,
                translation_key="invalid_auth",
            ) from err
        except MeridianRateLimitError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="rate_limited",
                retry_after=err.retry_after,
            ) from err
        except MeridianConnectionError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="cannot_connect",
            ) from err
        except (MeridianError, ValueError) as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="invalid_data",
            ) from err
        else:
            return data

    async def _async_fetch_and_import_data(self) -> MeridianSyncData:
        """Complete one sync and return its privacy-safe operational summary."""
        now = _utcnow()
        started_at = monotonic()
        accounts, topology_refreshed = await self._async_get_topology(now)
        mode = (
            await self._async_startup_mode(accounts)
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
            results = await self._async_sync_accounts(accounts, mode, now)

        self._initial_refresh_complete = True
        self._record_sync_cadence(mode, now)
        account_results = await self._async_account_results(accounts, now)

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
        )
        self._log_sync_summary(data)
        return data

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
            "properties=%d pages=%d rows_received=%d rows_retained=%d "
            "rows_imported=%d provisional=%d topology_refreshed=%s",
            data.sync_mode,
            data.sync_duration_seconds,
            data.account_count,
            data.property_count,
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
            data.topology_refreshed,
        )

    async def async_refresh_billing_totals(self) -> None:
        """Refresh Recorder-derived billing totals without polling Meridian usage."""
        if self.data is None or self.hass.state is not CoreState.running:
            return
        account_results = await self._async_account_results(self.accounts, _utcnow())
        self.async_set_updated_data(replace(self.data, account_results=account_results))

    def _select_sync_mode(self, now: datetime) -> SyncMode:
        if not self._initial_refresh_complete:
            return SyncMode.INITIAL
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
        if force or self._topology is None or expired:
            self._topology = await self.client.async_get_accounts()
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
        self, accounts: tuple[MeridianAccount, ...]
    ) -> SyncMode:
        """Distinguish a new install from a restart without changing IDs."""
        has_all_statistics = True
        for account in accounts:
            for property_data in account.properties:
                key = property_key(account.number, property_data.id)
                consumption_energy_id, _ = consumption_ids(key)
                required_ids = [consumption_energy_id]
                if any(meter.has_feed_in for meter in property_data.meter_points):
                    generation_energy_id, _ = generation_ids(key)
                    required_ids.append(generation_energy_id)
                for statistic_id in required_ids:
                    await self._async_cache_statistic(statistic_id)
                    has_all_statistics &= self._known_statistics[statistic_id]
        return SyncMode.RESTART if has_all_statistics else SyncMode.INITIAL

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
        results: list[PropertySyncResult] = []
        disambiguate = sum(len(account.properties) for account in accounts) > 1
        for account in accounts:
            for property_data in account.properties:
                results.append(
                    await self._async_sync_property(
                        account.number,
                        property_data,
                        mode,
                        now,
                        disambiguate=disambiguate,
                    )
                )
        return tuple(results)

    async def _async_account_results(
        self, accounts: tuple[MeridianAccount, ...], now: datetime
    ) -> tuple[AccountSyncResult, ...]:
        """Build account-scoped billing totals from imported HA statistics."""
        results: list[AccountSyncResult] = []
        for account in accounts:
            billing_period = await self._async_billing_period(account.number, now)
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
                        start=start,
                        end=end,
                        include_generation=has_feed_in,
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
        self, account_number: str, now: datetime
    ) -> MeridianBillingPeriod | None:
        """Return cached current billing metadata without blocking energy syncs."""
        cached_at = self._billing_cached_at.get(account_number)
        cached = self._billing_periods.get(account_number)
        local_today = now.astimezone(_NZ).date()
        expired = cached_at is None or now - cached_at >= BILLING_CACHE_INTERVAL
        period_ended = (
            cached is not None and cached.end is not None and (local_today > cached.end)
        )
        if cached is not None and not expired and not period_ended:
            return cached
        try:
            billing = await self.client.async_get_billing_period(account_number)
        except MeridianRateLimitError:
            raise
        except (MeridianConnectionError, MeridianGraphQLError, ValueError) as err:
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
        if account_number in self._billing_unavailable:
            self._billing_unavailable.discard(account_number)
            if not self._billing_unavailable:
                _LOGGER.info("Meridian billing metadata is available again")
        self._billing_periods[account_number] = billing
        self._billing_cached_at[account_number] = now
        return billing

    async def _async_sync_property(
        self,
        account_number: str,
        property_data: MeridianProperty,
        mode: SyncMode,
        now: datetime,
        *,
        disambiguate: bool = False,
    ) -> PropertySyncResult:
        key = property_key(account_number, property_data.id)
        consumption_cache_key = (key, READING_CONSUMPTION)
        consumption_energy_id, consumption_cost_id = consumption_ids(key)
        consumption_mode = await self._direction_mode(
            consumption_cache_key,
            (consumption_energy_id, consumption_cost_id),
            mode,
        )
        consumption_since = self._requested_since(
            consumption_cache_key, consumption_mode, now
        )
        consumption = await self._async_fetch_since(
            account_number=account_number,
            property_id=property_data.id,
            direction=READING_CONSUMPTION,
            since=consumption_since,
            page_size=self._page_size(consumption_cache_key, consumption_since, now),
        )
        self._remember_density(consumption_cache_key, consumption)
        consumption_import = self._merge_measurements(
            consumption_cache_key,
            consumption.measurements,
            now,
            initial_import=consumption_mode is SyncMode.INITIAL,
        )
        consumption_rows = 0
        if consumption_import:
            suffix = (
                f" — {_one_line_address(property_data.address)}" if disambiguate else ""
            )
            consumption_rows, _ = await async_import_measurements(
                self.hass,
                stat_energy_id=consumption_energy_id,
                stat_cost_id=consumption_cost_id,
                energy_name=f"Meridian grid import{suffix}",
                cost_name=f"Meridian grid import cost{suffix}",
                measurements=consumption_import,
            )
        self._record_direction_fetch(
            consumption_cache_key,
            consumption_mode,
            consumption.measurements,
            consumption_energy_id,
            consumption_cost_id,
        )

        generation_rows = 0
        generation = _empty_fetch()
        generation_since = consumption_since
        if any(meter.has_feed_in for meter in property_data.meter_points):
            generation_cache_key = (key, READING_GENERATION)
            generation_energy_id, generation_credit_id = generation_ids(key)
            generation_mode = await self._direction_mode(
                generation_cache_key,
                (generation_energy_id, generation_credit_id),
                mode,
            )
            generation_since = self._requested_since(
                generation_cache_key, generation_mode, now
            )
            generation = await self._async_fetch_since(
                account_number=account_number,
                property_id=property_data.id,
                direction=READING_GENERATION,
                since=generation_since,
                page_size=self._page_size(generation_cache_key, generation_since, now),
            )
            self._remember_density(generation_cache_key, generation)
            generation_import = self._merge_measurements(
                generation_cache_key,
                generation.measurements,
                now,
                initial_import=generation_mode is SyncMode.INITIAL,
            )
            if generation_import:
                suffix = (
                    f" — {_one_line_address(property_data.address)}"
                    if disambiguate
                    else ""
                )
                generation_rows, _ = await async_import_measurements(
                    self.hass,
                    stat_energy_id=generation_energy_id,
                    stat_cost_id=generation_credit_id,
                    energy_name=f"Meridian grid export{suffix}",
                    cost_name=f"Meridian grid export credit{suffix}",
                    measurements=generation_import,
                )
            self._record_direction_fetch(
                generation_cache_key,
                generation_mode,
                generation.measurements,
                generation_energy_id,
                generation_credit_id,
            )

        consumption_cache = self._measurement_cache.get((key, READING_CONSUMPTION), {})
        qualities = Counter(item.quality for item in consumption_cache.values())
        provisional = sorted(
            item.start.astimezone(UTC)
            for item in consumption_cache.values()
            if item.quality != _ACTUAL
        )
        observed_density = max(
            consumption.observed_rows_per_hour,
            generation.observed_rows_per_hour,
        )
        return PropertySyncResult(
            property_key=key,
            account_key=account_key(account_number),
            consumption_rows=consumption_rows,
            generation_rows=generation_rows,
            latest_reading=max(
                ((item.end or item.start) for item in consumption_cache.values()),
                default=None,
            ),
            estimated_rows=len(provisional),
            sync_mode=mode,
            requested_since=min(consumption_since, generation_since),
            consumption_pages=consumption.pages,
            generation_pages=generation.pages,
            consumption_received_rows=consumption.received_rows,
            generation_received_rows=generation.received_rows,
            consumption_retained_rows=len(consumption.measurements),
            generation_retained_rows=len(generation.measurements),
            oldest_estimated=provisional[0] if provisional else None,
            newest_estimated=provisional[-1] if provisional else None,
            quality_counts=tuple(sorted(qualities.items())),
            observed_rows_per_hour=observed_density,
        )

    async def _direction_mode(
        self,
        cache_key: CacheKey,
        statistic_ids: tuple[str, str],
        requested_mode: SyncMode,
    ) -> SyncMode:
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
        if not self._initial_refresh_complete and (
            energy_latest is None or energy_latest < _utcnow() - REVISION_OVERLAP
        ):
            return SyncMode.INITIAL
        if not self._initial_refresh_complete:
            return SyncMode.RESTART
        return requested_mode

    async def _async_cache_statistic(self, statistic_id: str) -> None:
        """Cache whether Recorder has current numeric history for a statistic."""
        if statistic_id in self._known_statistics:
            return
        latest = await async_latest_numeric_statistic_start(self.hass, statistic_id)
        self._latest_statistics[statistic_id] = latest
        self._known_statistics[statistic_id] = latest is not None

    def _record_direction_fetch(
        self,
        cache_key: CacheKey,
        mode: SyncMode,
        measurements: tuple[MeridianMeasurement, ...],
        energy_statistic_id: str,
        money_statistic_id: str,
    ) -> None:
        """Track attempted backfills separately from observable numeric history."""
        energy_was_known = self._known_statistics.get(energy_statistic_id, False)
        money_was_known = self._known_statistics.get(money_statistic_id, False)
        energy_observed = bool(measurements)
        money_observed = any(item.cost_cents is not None for item in measurements)

        if mode is SyncMode.INITIAL:
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
        if oldest_provisional is None:
            return minimum
        widened = oldest_provisional - RECONCILIATION_SAFETY_MARGIN
        return max(now - REVISION_OVERLAP, min(minimum, widened))

    def _page_size(self, cache_key: CacheKey, since: datetime, now: datetime) -> int:
        hours = max(1, math.ceil((now - since).total_seconds() / 3600))
        density = max(1.0, self._row_density.get(cache_key, 1.0))
        requested = math.ceil(hours * density * 1.25) + 4
        return min(PAGE_SIZE, max(MIN_PAGE_SIZE, requested))

    def _merge_measurements(
        self,
        cache_key: CacheKey,
        incoming: tuple[MeridianMeasurement, ...],
        now: datetime,
        *,
        initial_import: bool = False,
    ) -> tuple[MeridianMeasurement, ...]:
        cache = self._measurement_cache.setdefault(cache_key, {})
        cutoff = now - REVISION_OVERLAP
        for key in tuple(cache):
            if key[0] < cutoff:
                del cache[key]

        earliest_changed: datetime | None = None
        for measurement in incoming:
            start = measurement.start.astimezone(UTC)
            if start < cutoff:
                continue
            key = (start, measurement.channel_id)
            existing = cache.get(key)
            if (
                existing is not None
                and existing.quality == _ACTUAL
                and measurement.quality != _ACTUAL
            ):
                continue
            if existing != measurement:
                cache[key] = measurement
                earliest_changed = (
                    start if earliest_changed is None else min(earliest_changed, start)
                )

        if initial_import:
            return tuple(
                sorted(incoming, key=lambda item: (item.start, item.channel_id))
            )
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

    async def _async_fetch_since(
        self,
        *,
        account_number: str,
        property_id: str,
        direction: str,
        since: datetime,
        page_size: int | None = None,
    ) -> MeasurementFetchResult:
        """Fetch backwards until the requested UTC cutoff, with loop guards."""
        before: str | None = None
        measurements: dict[MeasurementKey, MeridianMeasurement] = {}
        now = _utcnow()
        end_on = now.astimezone(_NZ).date().isoformat()
        received_rows = 0
        pages = 0
        resolved_page_size = page_size or PAGE_SIZE

        for _page_number in range(MAX_MEASUREMENT_PAGES):
            page = await self.client.async_get_measurements(
                account_number=account_number,
                property_id=property_id,
                direction=direction,
                end_on=end_on,
                before=before,
                page_size=resolved_page_size,
            )
            pages += 1
            received_rows += len(page.measurements)
            if not page.measurements:
                break
            for measurement in page.measurements:
                start = measurement.start.astimezone(UTC)
                interval_end = (measurement.end or measurement.start).astimezone(UTC)
                if start >= since and interval_end <= now:
                    key = (start, measurement.channel_id)
                    existing = measurements.get(key)
                    if existing is None or (
                        existing.quality != _ACTUAL and measurement.quality == _ACTUAL
                    ):
                        measurements[key] = measurement
            oldest = min(item.start.astimezone(UTC) for item in page.measurements)
            if oldest < since or not page.has_previous_page:
                break
            if not page.start_cursor or page.start_cursor == before:
                raise ValueError("Meridian pagination did not advance")
            before = page.start_cursor
        else:
            raise ValueError("Meridian pagination exceeded the safety limit")

        hours = max(1.0, (now - since).total_seconds() / 3600)
        observed_density = len(measurements) / hours
        result = tuple(
            sorted(
                measurements.values(), key=lambda item: (item.start, item.channel_id)
            )
        )
        return MeasurementFetchResult(
            measurements=result,
            pages=pages,
            received_rows=received_rows,
            observed_rows_per_hour=observed_density,
        )

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


def _empty_fetch() -> MeasurementFetchResult:
    return MeasurementFetchResult((), 0, 0, 0.0)


def _is_topology_error(err: MeridianGraphQLError) -> bool:
    return bool(_TOPOLOGY_ERROR_CODES.intersection(err.codes))


def _local_day_start(value: date) -> datetime:
    """Return a New Zealand local date boundary as UTC."""
    return datetime.combine(value, time.min, tzinfo=_NZ).astimezone(UTC)


def _one_line_address(value: str) -> str:
    """Normalize a local-only property label for display."""
    return " ".join(value.split())
