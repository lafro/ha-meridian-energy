"""Typed models for Meridian Energy API data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class SyncMode(StrEnum):
    """A Meridian statistics retrieval mode."""

    INITIAL = "initial"
    RESTART = "restart"
    TIP = "tip"
    TARGETED_RECONCILIATION = "targeted_reconciliation"
    FULL_RECONCILIATION = "full_reconciliation"


@dataclass(frozen=True, slots=True)
class MeridianTokenSet:
    """A renewable Firebase authentication session."""

    id_token: str
    refresh_token: str
    expires_at: datetime
    user_id: str


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
class MeridianMeasurement:
    """One interval measurement returned by Meridian."""

    start: datetime
    end: datetime | None
    value_kwh: Decimal
    quality: str
    direction: str
    channel_id: str
    cost_cents: Decimal


@dataclass(frozen=True, slots=True)
class MeasurementPage:
    """A page of interval measurements."""

    measurements: tuple[MeridianMeasurement, ...]
    has_previous_page: bool
    start_cursor: str | None


@dataclass(frozen=True, slots=True)
class MeasurementFetchResult:
    """Measurements and non-sensitive request metrics for one direction."""

    measurements: tuple[MeridianMeasurement, ...]
    pages: int
    received_rows: int
    observed_rows_per_hour: float


@dataclass(frozen=True, slots=True)
class PropertySyncResult:
    """Non-sensitive summary of a property sync."""

    property_key: str
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


@dataclass(frozen=True, slots=True)
class MeridianSyncData:
    """Non-sensitive coordinator result."""

    account_count: int
    property_count: int
    results: tuple[PropertySyncResult, ...]
    synced_at: datetime
    sync_mode: SyncMode
    topology_refreshed: bool
    topology_cache_age_seconds: float


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
