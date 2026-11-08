"""Typed models for Meridian Energy API data."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any


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
class PropertySyncResult:
    """
    Non-sensitive summary of a property sync, from its usage readings.

    The estimated rows are the hours of the last 21 days whose reading is
    not ``ACTUAL``, oldest and newest included.
    """

    property_key: str
    account_key: str
    latest_reading: datetime | None
    estimated_rows: int
    oldest_estimated: datetime | None
    newest_estimated: datetime | None
    quality_counts: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class AccountSyncResult:
    """Non-sensitive billing and sync summary for one account."""

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
    """Non-sensitive coordinator result; ``requests`` counts measurement pages."""

    account_count: int
    property_count: int
    results: tuple[PropertySyncResult, ...]
    account_results: tuple[AccountSyncResult, ...]
    synced_at: datetime
    sync_duration_seconds: float = 0.0
    requests: int = 0


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
