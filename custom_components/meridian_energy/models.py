"""Typed models for Meridian Energy API data."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .units import SyncMemory


class SyncMode(StrEnum):
    """A Meridian statistics retrieval mode."""

    INITIAL = "initial"
    RESTART = "restart"
    TIP = "tip"
    TARGETED_RECONCILIATION = "targeted_reconciliation"
    FULL_RECONCILIATION = "full_reconciliation"


@dataclass(frozen=True, slots=True)
class MeridianTokenSet:
    """
    A renewable Firebase authentication session.

    The tokens and the Firebase user ID are left out of ``repr`` so that a
    token set reaching a log line or a traceback never shows them.
    """

    id_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expires_at: datetime
    user_id: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class MeridianMeterPoint:
    """An electricity meter point."""

    id: str
    market_identifier: str
    has_feed_in: bool


@dataclass(frozen=True, slots=True)
class MeridianProperty:
    """A Meridian property."""

    id: str
    address: str
    meter_points: tuple[MeridianMeterPoint, ...]


@dataclass(frozen=True, slots=True)
class MeridianAccount:
    """A Meridian customer account."""

    number: str
    status: str
    properties: tuple[MeridianProperty, ...]


@dataclass(frozen=True, slots=True)
class SetupHandoff:
    """
    What a finished setup hands its new entry, in memory only.

    The accounts the flow found, and when its import's sync started: the
    entry's setup then makes no request, and its first sync continues the
    history setup imported instead of reconciling 14 days of it again. And
    what the import's coordinator remembers of each direction (``memory``):
    how its sync went, and the rows it imported in the last 14 days. After
    setup went on without history (``imported`` false), only the accounts,
    as of ``imported_at``: the first sync imports the history as new.
    """

    accounts: tuple[MeridianAccount, ...]
    imported_at: datetime
    memory: SyncMemory | None = None
    imported: bool = True


@dataclass(frozen=True, slots=True)
class MeridianBillingPeriod:
    """Billing-cycle metadata for one Meridian account."""

    period_length: str | None
    period_length_multiplier: int | None
    is_fixed: bool
    start: date | None
    end: date | None
    next_billing_date: date | None
    period_start_day: int | None


@dataclass(frozen=True, slots=True)
class MeridianMeasurement:
    """One interval measurement returned by Meridian."""

    start: datetime
    end: datetime | None
    value_kwh: Decimal
    quality: str
    direction: str
    channel_id: str
    cost_cents: Decimal | None


@dataclass(frozen=True, slots=True)
class MeasurementPage:
    """A page of interval measurements."""

    measurements: tuple[MeridianMeasurement, ...]
    has_previous_page: bool
    start_cursor: str | None


@dataclass(frozen=True, slots=True)
class WindowWalk:
    """
    What one direction's walk through its windows asked for and found.

    ``requested_days`` counts the New Zealand dates the windows it sent
    covered, and ``windows_requested`` those windows; ``continuation_pages``
    counts the pages after a window's first. ``rows_before_window`` counts
    rows older than their window's start, which are dropped. ``windows_empty``
    counts windows that returned no readings. ``stopped_at_gap`` says the
    walk stopped, or its rows were cut, at a gap over stored rows;
    ``history_start`` that it stopped where the history begins.
    """

    requested_days: int = 0
    windows_requested: int = 0
    continuation_pages: int = 0
    rows_before_window: int = 0
    windows_empty: int = 0
    stopped_at_gap: bool = False
    history_start: bool = False


@dataclass(frozen=True, slots=True)
class MeasurementFetchResult:
    """Measurements and non-sensitive request metrics for one direction."""

    measurements: tuple[MeridianMeasurement, ...]
    pages: int
    received_rows: int
    observed_rows_per_hour: float
    walk: WindowWalk = WindowWalk()


@dataclass(frozen=True, slots=True)
class RequestContext:
    """
    The Meridian call a sync made last, described without identifiers.

    The coordinator records one before every API call, so a failure, a
    timeout or a cancellation can still say which call was in flight.
    ``finished_at`` is set once the call has returned, or failed in a way
    the sync handled, so a later failure (writing statistics, say) is not
    blamed on it. Positions are 1-based; ``calls`` counts the API calls the
    sync has started, this one included. A measurements request names its
    window (``window`` of ``window_count``, newest first) and its bound:
    ``dates`` for a window's first page, ``before`` for a later one.
    """

    operation: str
    calls: int
    started_at: float
    direction: str | None = None
    account_index: int | None = None
    account_count: int | None = None
    property_index: int | None = None
    property_count: int | None = None
    page: int | None = None
    bound: str | None = None
    finished_at: float | None = None
    window: int | None = None
    window_count: int | None = None


@dataclass(frozen=True, slots=True)
class PropertySyncResult:
    """Non-sensitive summary of a property sync."""

    property_key: str
    account_key: str
    consumption_rows: int
    generation_rows: int
    latest_reading: datetime | None
    estimated_rows: int
    sync_mode: SyncMode
    requested_since: datetime
    consumption_pages: int
    generation_pages: int
    consumption_received_rows: int
    generation_received_rows: int
    consumption_retained_rows: int
    generation_retained_rows: int
    oldest_estimated: datetime | None
    newest_estimated: datetime | None
    quality_counts: tuple[tuple[str, int], ...]
    observed_rows_per_hour: float
    consumption_walk: WindowWalk = WindowWalk()
    generation_walk: WindowWalk = WindowWalk()


@dataclass(frozen=True, slots=True)
class AccountSyncResult:
    """Non-sensitive billing and sync summary for one selected account."""

    account_key: str
    billing_period: MeridianBillingPeriod | None
    current_bill_usage: Decimal | None
    current_bill_cost: Decimal | None
    current_bill_export: Decimal | None
    current_bill_credit: Decimal | None
    has_feed_in: bool
    usage_complete: bool
    cost_complete: bool
    export_complete: bool
    credit_complete: bool


@dataclass(frozen=True, slots=True)
class BillingPeriodTotals:
    """Account totals derived from timestamped Home Assistant statistics."""

    usage: Decimal | None
    cost: Decimal | None
    export: Decimal | None
    credit: Decimal | None
    usage_complete: bool
    cost_complete: bool
    export_complete: bool
    credit_complete: bool


@dataclass(frozen=True, slots=True)
class MeridianSyncData:
    """
    Non-sensitive coordinator result.

    ``request_count`` counts the HTTP requests the sync sent, session
    refreshes and retries included, and ``median_request_seconds`` is the
    median time of those that ended; ``median_measurements_seconds`` is that
    of the measurements requests alone. ``direction_count`` counts the
    (property, direction) pairs it fetched. ``windows_ok``,
    ``windows_empty`` and ``windows_failed`` count the measurement windows
    that returned readings, returned none, and failed in a way taken as the
    start of the history; ``coverage_windows`` and ``revision_windows``
    count those the lanes asked for after the walks, for older history
    still missing and for their monthly revision; ``retries`` counts
    resent requests, ``splits`` windows asked for again by their days,
    after a failure in this sync or one an earlier update left to it (the
    probe it owed, or the days it kept), and ``history_start`` says
    whether any walk found where the history begins.
    ``units_failed``, ``units_degraded`` and ``units_deferred`` count the
    (property, direction) pairs that imported nothing after a failure,
    imported their newer windows only, and waited out a backoff unfetched.
    """

    account_count: int
    property_count: int
    results: tuple[PropertySyncResult, ...]
    account_results: tuple[AccountSyncResult, ...]
    synced_at: datetime
    sync_mode: SyncMode
    topology_refreshed: bool
    topology_cache_age_seconds: float
    sync_duration_seconds: float = 0.0
    request_count: int = 0
    direction_count: int = 0
    median_request_seconds: float | None = None
    median_measurements_seconds: float | None = None
    windows_ok: int = 0
    windows_empty: int = 0
    windows_failed: int = 0
    coverage_windows: int = 0
    revision_windows: int = 0
    retries: int = 0
    splits: int = 0
    history_start: bool = False
    units_failed: int = 0
    units_degraded: int = 0
    units_deferred: int = 0


def require_mapping(value: Any, context: str) -> dict[str, Any]:
    """Validate that an API value is a mapping."""
    if not isinstance(value, dict):
        raise ValueError(f"Expected an object for {context}")
    return value


def require_list(value: Any, context: str) -> list[Any]:
    """Validate that an API value is a list."""
    if not isinstance(value, list):
        raise ValueError(f"Expected a list for {context}")
    return value
