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
    token set reaching a log line or a traceback never shows them. The user
    ID is stored but never sent, so an answer without one is still a session.
    """

    id_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expires_at: datetime
    user_id: str | None = field(repr=False)


@dataclass(frozen=True, slots=True)
class MeridianMeterPoint:
    """An electricity meter point; its ID and ICP are kept but never used."""

    id: str | None
    market_identifier: str | None
    has_feed_in: bool


@dataclass(frozen=True, slots=True)
class MeridianProperty:
    """
    A Meridian property; its address only labels it, None when not given.

    ``feed_in_unread`` says that a register's feed-in flag couldn't be read
    and no meter point was read as exporting.
    """

    id: str
    address: str | None
    meter_points: tuple[MeridianMeterPoint, ...]
    feed_in_unread: bool = False


@dataclass(frozen=True, slots=True)
class MeridianAccount:
    """
    A Meridian customer account.

    ``skipped_properties`` counts the properties Meridian listed on it that
    couldn't be read, and are not among ``properties``.
    """

    number: str
    status: str
    properties: tuple[MeridianProperty, ...]
    skipped_properties: int = 0


@dataclass(frozen=True, slots=True)
class MeridianBillingPeriod:
    """
    Billing-cycle metadata for one Meridian account.

    The start and end are read strictly and set the bill's period. The next
    billing date feeds the Next billing date sensor; the period length and
    whether it is fixed appear only in diagnostics; the multiplier and start
    day are kept but unused. Every field but the start and end is None when
    Meridian gave none, or a value that can't be read.
    """

    period_length: str | None
    period_length_multiplier: int | None
    is_fixed: bool | None
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

    The latest reading is the end of the newest ``ACTUAL`` hour of the last
    21 days. The estimated rows are the hours of the last 21 days whose
    reading is not ``ACTUAL``, oldest and newest included.
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
class UnreadablePrice:
    """
    A price whose text couldn't be read: its sensor's key, field and shape.

    The shape is the text masked, numbers as N and letters as A, after
    compatibility normalisation, and cut short, so it says how the text
    looked without what it said; or, with no text to mask, a word for why:
    ``missing``, ``not text``, ``empty`` or ``not true or false``.
    """

    key: str
    field: str
    shape: str


@dataclass(frozen=True, slots=True)
class MeridianPrices:
    """
    An all-day plan's name and prices, in NZD; None where unreadable.

    ``unreadable`` holds each price that couldn't be read.
    """

    plan: str | None
    daily_charge: Decimal | None
    anytime: Decimal | None
    unreadable: tuple[UnreadablePrice, ...] = ()


@dataclass(frozen=True, slots=True)
class MeridianNoAgreement:
    """A property with no agreement in force today, so no plan and no prices."""


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
    None until Meridian's answer has said. ``has_feed_in`` is None while
    one of the account's properties can't be read and no readable one
    sends power to the grid.
    """

    account_key: str
    billing_period: MeridianBillingPeriod | None
    has_feed_in: bool | None
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
