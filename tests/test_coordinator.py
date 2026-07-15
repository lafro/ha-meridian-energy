"""Tests for Meridian statistics coordination and pagination."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.config_entries import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.meridian_energy.api import (
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianGraphQLError,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeasurementFetchResult,
    MeasurementPage,
    MeridianAccount,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianProperty,
)


def _measurement(
    start: datetime,
    *,
    quality: str = "ACTUAL",
    channel: str = "meter:register",
    direction: str = "CONSUMPTION",
) -> MeridianMeasurement:
    return MeridianMeasurement(
        start=start,
        end=start + timedelta(hours=1),
        value_kwh=Decimal("1.0"),
        quality=quality,
        direction=direction,
        channel_id=channel,
        cost_cents=Decimal(30),
    )


def _account(*, feed_in: bool = False) -> MeridianAccount:
    return MeridianAccount(
        number="A-SYNTHETIC",
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id="property",
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint(
                        id="meter",
                        market_identifier="SYNTHETIC-ICP",
                        has_feed_in=feed_in,
                    ),
                ),
            ),
        ),
    )


def _fetch(*measurements: MeridianMeasurement) -> MeasurementFetchResult:
    return MeasurementFetchResult(
        measurements=measurements,
        pages=1,
        received_rows=len(measurements),
        observed_rows_per_hour=float(len(measurements)),
    )


@pytest.mark.asyncio
async def test_update_imports_consumption(hass) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(),))
    client.async_get_billing_period = AsyncMock(return_value=None)
    coordinator = MeridianDataCoordinator(hass, client)
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    coordinator._async_fetch_since = AsyncMock(return_value=_fetch(_measurement(now)))

    with (
        patch(
            "custom_components.meridian_energy.coordinator.async_has_statistics",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "custom_components.meridian_energy.coordinator.async_import_measurements",
            new=AsyncMock(return_value=(1, 1)),
        ) as importer,
    ):
        result = await coordinator._async_update_data()

    assert result.account_count == 1
    assert result.property_count == 1
    assert result.results[0].consumption_rows == 1
    assert result.results[0].generation_rows == 0
    importer.assert_awaited_once()
    assert importer.await_args.kwargs["energy_name"] == (
        "Meridian electricity consumption — 1 Synthetic Street"
    )
    assert importer.await_args.kwargs["cost_name"] == (
        "Meridian electricity cost — 1 Synthetic Street"
    )


@pytest.mark.asyncio
async def test_update_imports_generation_for_feed_in(hass) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(feed_in=True),))
    client.async_get_billing_period = AsyncMock(return_value=None)
    coordinator = MeridianDataCoordinator(hass, client)
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    coordinator._async_fetch_since = AsyncMock(
        side_effect=[
            _fetch(_measurement(now)),
            _fetch(_measurement(now, direction="GENERATION")),
        ]
    )
    with (
        patch(
            "custom_components.meridian_energy.coordinator.async_has_statistics",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "custom_components.meridian_energy.coordinator.async_import_measurements",
            new=AsyncMock(return_value=(1, 1)),
        ) as importer,
    ):
        result = await coordinator._async_update_data()

    assert result.results[0].generation_rows == 1
    assert importer.await_count == 2
    assert coordinator._async_fetch_since.await_count == 2
    assert importer.await_args_list[1].kwargs["energy_name"] == (
        "Meridian solar export — 1 Synthetic Street"
    )
    assert importer.await_args_list[1].kwargs["cost_name"] == (
        "Meridian solar export credit — 1 Synthetic Street"
    )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (MeridianAuthenticationError(), ConfigEntryAuthFailed),
        (MeridianConnectionError(), UpdateFailed),
        (MeridianGraphQLError("accounts", ("CODE",)), UpdateFailed),
        (ValueError("bad data"), UpdateFailed),
    ],
)
@pytest.mark.asyncio
async def test_update_maps_errors(
    hass, error: Exception, expected: type[Exception]
) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=error)
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(expected):
        await coordinator._async_update_data()


@pytest.mark.asyncio
async def test_fetch_since_prefers_actual_for_same_channel(hass) -> None:
    client = MagicMock()
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=2
    )
    estimate = _measurement(start, quality="ESTIMATE")
    actual = _measurement(start, quality="ACTUAL")
    other_channel = _measurement(start, channel="meter:controlled")
    client.async_get_measurements = AsyncMock(
        return_value=MeasurementPage(
            measurements=(estimate, actual, other_channel),
            has_previous_page=False,
            start_cursor=None,
        )
    )
    coordinator = MeridianDataCoordinator(hass, client)

    result = await coordinator._async_fetch_since(
        account_number="A-SYNTHETIC",
        property_id="property",
        direction="CONSUMPTION",
        since=start - timedelta(hours=1),
    )

    assert len(result.measurements) == 2
    assert {item.quality for item in result.measurements} == {"ACTUAL"}


@pytest.mark.asyncio
async def test_fetch_since_paginates_backwards(hass) -> None:
    client = MagicMock()
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=2
    )
    client.async_get_measurements = AsyncMock(
        side_effect=[
            MeasurementPage((_measurement(now),), True, "cursor-1"),
            MeasurementPage((_measurement(now - timedelta(days=2)),), False, None),
        ]
    )
    coordinator = MeridianDataCoordinator(hass, client)

    result = await coordinator._async_fetch_since(
        account_number="A-SYNTHETIC",
        property_id="property",
        direction="CONSUMPTION",
        since=now - timedelta(days=3),
    )

    assert len(result.measurements) == 2
    assert (
        client.async_get_measurements.await_args_list[1].kwargs["before"] == "cursor-1"
    )


@pytest.mark.asyncio
async def test_fetch_since_rejects_stalled_cursor(hass) -> None:
    client = MagicMock()
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=2
    )
    client.async_get_measurements = AsyncMock(
        side_effect=[
            MeasurementPage((_measurement(now),), True, "cursor-1"),
            MeasurementPage((_measurement(now),), True, "cursor-1"),
        ]
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(ValueError, match="did not advance"):
        await coordinator._async_fetch_since(
            account_number="A-SYNTHETIC",
            property_id="property",
            direction="CONSUMPTION",
            since=now - timedelta(days=3),
        )


@pytest.mark.asyncio
async def test_fetch_since_stops_on_empty_or_older_page(hass) -> None:
    client = MagicMock()
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    client.async_get_measurements = AsyncMock(
        side_effect=[
            MeasurementPage((), False, None),
            MeasurementPage((_measurement(now - timedelta(days=5)),), True, "old"),
        ]
    )
    coordinator = MeridianDataCoordinator(hass, client)
    empty = await coordinator._async_fetch_since(
        account_number="A-SYNTHETIC",
        property_id="property",
        direction="CONSUMPTION",
        since=now - timedelta(days=1),
    )
    older = await coordinator._async_fetch_since(
        account_number="A-SYNTHETIC",
        property_id="property",
        direction="CONSUMPTION",
        since=now - timedelta(days=1),
    )
    assert empty.measurements == ()
    assert older.measurements == ()


@pytest.mark.asyncio
async def test_fetch_since_has_hard_page_limit(hass) -> None:
    client = MagicMock()
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=2
    )
    client.async_get_measurements = AsyncMock(
        side_effect=[
            MeasurementPage((_measurement(now),), True, f"cursor-{index}")
            for index in range(2)
        ]
    )
    coordinator = MeridianDataCoordinator(hass, client)
    with (
        patch("custom_components.meridian_energy.coordinator.MAX_MEASUREMENT_PAGES", 2),
        pytest.raises(ValueError, match="safety limit"),
    ):
        await coordinator._async_fetch_since(
            account_number="A-SYNTHETIC",
            property_id="property",
            direction="CONSUMPTION",
            since=now - timedelta(days=365),
        )


@pytest.mark.asyncio
async def test_fetch_since_ignores_incomplete_and_future_intervals(hass) -> None:
    client = MagicMock()
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    completed = _measurement(now - timedelta(hours=2), quality="ESTIMATE")
    current = _measurement(now, quality="ESTIMATE")
    future = _measurement(now + timedelta(hours=1), quality="ESTIMATE")
    client.async_get_measurements = AsyncMock(
        return_value=MeasurementPage(
            measurements=(completed, current, future),
            has_previous_page=False,
            start_cursor=None,
        )
    )
    coordinator = MeridianDataCoordinator(hass, client)

    result = await coordinator._async_fetch_since(
        account_number="A-SYNTHETIC",
        property_id="property",
        direction="CONSUMPTION",
        since=now - timedelta(days=1),
    )

    assert result.measurements == (completed,)
