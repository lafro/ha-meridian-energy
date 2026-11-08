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
class MeridianBill:
    """A bill: when it was issued, and the period printed on it."""

    issued: date
    start: date | None
    end: date | None


@dataclass(frozen=True, slots=True)
class MeridianPayment:
    """A payment: its amount in cents, its date and its status."""

    amount_cents: int
    paid_on: date
    status: str | None


@dataclass(frozen=True, slots=True)
class MeridianBalance:
    """An account's balance in cents, negative when owed, its bills and payments."""

    balance_cents: int
    bills: tuple[MeridianBill, ...]
    payments: tuple[MeridianPayment, ...]


@dataclass(frozen=True, slots=True)
class MeridianPrices:
    """An all-day plan's name and prices, in NZD; None where unreadable."""

    plan: str | None
    daily_charge: Decimal | None
    anytime: Decimal | None


@dataclass(frozen=True, slots=True)
class BillFigures:
    """The current bill from Meridian's day totals, in kWh and NZD."""

    usage: Decimal | None = None
    cost: Decimal | None = None
    export: Decimal | None = None
    credit: Decimal | None = None
    estimate: Decimal | None = None


@dataclass(frozen=True, slots=True)
class AccountSyncResult:
    """
    Non-sensitive figures for one account; None where unknown.

    ``price_sensors`` says whether the account has price sensors, which
    exist only for one property and one meter point on an all-day plan:
    None until Meridian's answer has said.
    """

    account_key: str
    billing_period: MeridianBillingPeriod | None
    has_feed_in: bool
    bill: BillFigures = BillFigures()
    total_due: Decimal | None = None
    credit: Decimal | None = None
    last_bill: MeridianBill | None = None
    last_payment: MeridianPayment | None = None
    price_sensors: bool | None = False
    prices: MeridianPrices | None = None


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
