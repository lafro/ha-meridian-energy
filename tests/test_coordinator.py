"""Tests for Meridian statistics coordination and pagination."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from time import monotonic
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.config_entries import ConfigEntryAuthFailed
from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.meridian_energy.api import (
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianGraphQLError,
    MeridianRateLimitError,
)
from custom_components.meridian_energy.const import (
    FIRST_SYNC_RETRY_SECONDS,
    MAX_WINDOW_PAGES,
    PAGE_SIZE,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeasurementPage,
    MeridianAccount,
    MeridianBillingPeriod,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianProperty,
    RequestContext,
)

from .kraken_fake import FakeKraken

TEST_HOUR = datetime(2026, 7, 16, 0, 0, tzinfo=UTC)


def _measurement(
    start: datetime,
    *,
    quality: str = "ACTUAL",
    channel: str = "meter:register",
    direction: str = "CONSUMPTION",
    value: str = "1.0",
    cost: str = "30",
) -> MeridianMeasurement:
    return MeridianMeasurement(
        start=start,
        end=start + timedelta(hours=1),
        value_kwh=Decimal(value),
        quality=quality,
        direction=direction,
        channel_id=channel,
        cost_cents=Decimal(cost),
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


def _short_history_client(account: MeridianAccount) -> MagicMock:
    """Return a client whose properties have a day of history each, any direction."""
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(account,))
    client.async_get_billing_period = AsyncMock(return_value=None)
    client.async_get_measurements = AsyncMock(
        side_effect=FakeKraken(TEST_HOUR, days=1).async_get_measurements
    )
    return client


def _patched_import(importer: AsyncMock, latest: datetime | None = None) -> Any:
    """Fix the clock and the stored statistics, and stand in for the importer."""
    return (
        patch(
            "custom_components.meridian_energy.coordinator._utcnow",
            return_value=TEST_HOUR,
        ),
        patch(
            "custom_components.meridian_energy.coordinator.async_latest_numeric_statistic_start",
            new=AsyncMock(return_value=latest),
        ),
        patch(
            "custom_components.meridian_energy.coordinator.async_replay",
            new=importer,
        ),
    )


@pytest.mark.asyncio
async def test_update_imports_consumption(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, _short_history_client(_account()))
    importer = AsyncMock(return_value=(1, 1))

    clock, latest, imported = _patched_import(importer)
    with clock, latest, imported:
        result = await coordinator._async_update_data()

    assert result.account_count == 1
    assert result.property_count == 1
    assert result.results[0].consumption_rows == 1
    assert result.results[0].generation_rows == 0
    importer.assert_awaited_once()
    assert importer.await_args.kwargs["energy_name"] == "Meridian grid import"
    assert importer.await_args.kwargs["cost_name"] == "Meridian grid import cost"


@pytest.mark.asyncio
async def test_update_imports_generation_for_feed_in(hass) -> None:
    client = _short_history_client(_account(feed_in=True))
    coordinator = MeridianDataCoordinator(hass, client)
    importer = AsyncMock(return_value=(1, 1))

    clock, latest, imported = _patched_import(importer, TEST_HOUR - timedelta(hours=1))
    with clock, latest, imported:
        result = await coordinator._async_update_data()

    assert result.results[0].generation_rows == 1
    assert importer.await_count == 2
    assert {
        call.kwargs["direction"]
        for call in client.async_get_measurements.await_args_list
    } == {"CONSUMPTION", "GENERATION"}
    assert importer.await_args_list[1].kwargs["energy_name"] == ("Meridian grid export")
    assert importer.await_args_list[1].kwargs["cost_name"] == (
        "Meridian grid export credit"
    )


@pytest.mark.asyncio
async def test_multiple_properties_get_disambiguated_one_line_names(hass) -> None:
    account = _account()
    second_property = MeridianProperty(
        id="property-2",
        address="  2 Synthetic\nStreet  ",
        meter_points=account.properties[0].meter_points,
    )
    account = MeridianAccount(
        number=account.number,
        status=account.status,
        properties=(account.properties[0], second_property),
    )
    coordinator = MeridianDataCoordinator(hass, _short_history_client(account))
    importer = AsyncMock(return_value=(1, 1))

    clock, latest, imported = _patched_import(importer)
    with clock, latest, imported:
        await coordinator._async_update_data()

    assert importer.await_args_list[0].kwargs["energy_name"] == (
        "Meridian grid import — 1 Synthetic Street"
    )
    assert importer.await_args_list[1].kwargs["energy_name"] == (
        "Meridian grid import — 2 Synthetic Street"
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


@pytest.mark.parametrize(
    ("error", "expected", "translation_key"),
    [
        (MeridianAuthenticationError(), ConfigEntryAuthFailed, "invalid_auth"),
        (MeridianRateLimitError(120), ConfigEntryNotReady, "rate_limited"),
        (MeridianConnectionError(), ConfigEntryNotReady, "cannot_connect"),
        (
            MeridianGraphQLError("accounts", ("CODE",)),
            ConfigEntryNotReady,
            "invalid_data",
        ),
        (ValueError("bad data"), ConfigEntryNotReady, "invalid_data"),
    ],
)
@pytest.mark.asyncio
async def test_prepare_topology_maps_errors_for_setup(
    hass, error: Exception, expected: type[Exception], translation_key: str
) -> None:
    """Setup's only Meridian call raises config-entry exceptions HA acts on."""
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=error)
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(expected) as raised:
        await coordinator.async_prepare_topology()

    assert raised.value.translation_key == translation_key


@pytest.mark.asyncio
async def test_prepare_topology_counts_its_request_from_one(hass) -> None:
    """
    Setup's account request starts its own count, as each sync does.

    So the request context never describes a call from an earlier round of
    requests, here the account request of a setup that failed.
    """
    client = MagicMock()
    client.async_get_accounts = AsyncMock(
        side_effect=[MeridianConnectionError(), (_account(),)]
    )
    coordinator = MeridianDataCoordinator(hass, client)
    with pytest.raises(ConfigEntryNotReady):
        await coordinator.async_prepare_topology()
    failed = coordinator.last_request
    assert failed is not None
    assert (failed.operation, failed.calls, failed.finished_at) == ("accounts", 1, None)

    await coordinator.async_prepare_topology()

    last = coordinator.last_request
    assert last is not None
    assert (last.operation, last.calls) == ("accounts", 1)
    assert last.finished_at is not None


@pytest.mark.asyncio
async def test_prepare_topology_rejects_missing_selected_accounts(hass) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(),))
    coordinator = MeridianDataCoordinator(
        hass, client, selected_accounts=frozenset({"A-OTHER"})
    )

    with pytest.raises(ConfigEntryNotReady):
        await coordinator.async_prepare_topology()


@pytest.mark.asyncio
async def test_prepare_topology_caches_accounts_for_the_first_sync(hass) -> None:
    """The first sync reuses the topology fetched during setup."""
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(),))
    coordinator = MeridianDataCoordinator(hass, client)

    await coordinator.async_prepare_topology()

    assert coordinator.accounts == (_account(),)
    accounts, refreshed = await coordinator._async_get_topology(
        coordinator._topology_cached_at
    )
    assert accounts == (_account(),)
    assert refreshed is False
    client.async_get_accounts.assert_awaited_once()


@pytest.mark.parametrize(
    "error",
    [MeridianConnectionError(), MeridianGraphQLError("accounts", ("CODE",))],
)
@pytest.mark.asyncio
async def test_failed_first_sync_retries_sooner_than_the_hourly_poll(
    hass, error: Exception
) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=error)
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(UpdateFailed) as first:
        await coordinator._async_update_data()
    assert first.value.retry_after == FIRST_SYNC_RETRY_SECONDS

    coordinator._initial_refresh_complete = True
    with pytest.raises(UpdateFailed) as later:
        await coordinator._async_update_data()
    assert later.value.retry_after is None


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
async def test_fetch_since_rejects_conflicting_anonymous_rows(hass) -> None:
    client = MagicMock()
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=2
    )
    client.async_get_measurements = AsyncMock(
        return_value=MeasurementPage(
            measurements=(
                _measurement(start, value="1", channel="aggregate"),
                _measurement(start, value="2", channel="aggregate"),
            ),
            has_previous_page=False,
            start_cursor=None,
        )
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(ValueError, match="ambiguous"):
        await coordinator._async_fetch_since(
            account_number="A-SYNTHETIC",
            property_id="property",
            direction="CONSUMPTION",
            since=start - timedelta(hours=1),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "measurements",
    [
        (
            _measurement(TEST_HOUR, value="1", cost="30", channel="aggregate"),
            _measurement(TEST_HOUR, value="1", cost="40", channel="aggregate"),
        ),
        (
            _measurement(TEST_HOUR, value="1", cost="30", channel="aggregate"),
            _measurement(TEST_HOUR, value="1", cost="30", channel="aggregate"),
        ),
    ],
    ids=("equal-energy-different-cost", "fully-identical"),
)
async def test_fetch_since_rejects_indistinguishable_anonymous_rows(
    hass, measurements: tuple[MeridianMeasurement, MeridianMeasurement]
) -> None:
    client = MagicMock()
    client.async_get_measurements = AsyncMock(
        return_value=MeasurementPage(
            measurements=measurements,
            has_previous_page=False,
            start_cursor=None,
        )
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with (
        patch(
            "custom_components.meridian_energy.coordinator._utcnow",
            return_value=TEST_HOUR + timedelta(hours=3),
        ),
        pytest.raises(ValueError, match="ambiguous"),
    ):
        await coordinator._async_fetch_since(
            account_number="A-SYNTHETIC",
            property_id="property",
            direction="CONSUMPTION",
            since=TEST_HOUR - timedelta(hours=1),
        )


@pytest.mark.asyncio
async def test_fetch_since_rejects_mixed_anonymous_and_identified_rows(hass) -> None:
    client = MagicMock()
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=2
    )
    client.async_get_measurements = AsyncMock(
        return_value=MeasurementPage(
            measurements=(
                _measurement(start, channel="aggregate"),
                _measurement(start, channel="identified-channel"),
            ),
            has_previous_page=False,
            start_cursor=None,
        )
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(ValueError, match="ambiguous"):
        await coordinator._async_fetch_since(
            account_number="A-SYNTHETIC",
            property_id="property",
            direction="CONSUMPTION",
            since=start - timedelta(hours=1),
        )


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
            MeasurementPage(
                (_measurement(now - timedelta(hours=1)),), True, "cursor-1"
            ),
        ]
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(ValueError, match="cursor did not advance"):
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


@pytest.mark.parametrize("days", [2, 6], ids=["three-dates", "a-week"])
@pytest.mark.asyncio
async def test_a_window_holds_at_most_16_pages_of_744_rows(hass, days: int) -> None:
    """
    The page cap is per window, by rows: 16 pages of 744, or more smaller ones.

    Each page here moves the window on, with another channel's reading for
    the same hour, until the window has taken as many pages as 11,904 rows
    need at its page size. Then the window stops.
    """
    client = MagicMock()
    newest = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=2
    )
    sizes: list[int] = []

    async def page(**kwargs: Any) -> MeasurementPage:
        sizes.append(kwargs["last"])
        channel = len(sizes)
        return MeasurementPage(
            (_measurement(newest, channel=f"meter:register-{channel}"),),
            True,
            f"cursor-{channel}",
        )

    client.async_get_measurements = AsyncMock(side_effect=page)
    coordinator = MeridianDataCoordinator(hass, client)
    with pytest.raises(ValueError, match="per-window page limit"):
        await coordinator._async_fetch_since(
            account_number="A-SYNTHETIC",
            property_id="property",
            direction="CONSUMPTION",
            since=newest - timedelta(days=days),
        )

    (last,) = set(sizes)
    assert len(sizes) == math.ceil(MAX_WINDOW_PAGES * PAGE_SIZE / last)
    assert len(sizes) > MAX_WINDOW_PAGES


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


def _two_account_topology() -> tuple[MeridianAccount, ...]:
    first = _account(feed_in=True)
    second = MeridianAccount(
        number="A-SYNTHETIC-2",
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id="property-2a",
                address="2 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("meter-2a", "SYNTHETIC-ICP-2A", False),
                ),
            ),
            MeridianProperty(
                id="property-2b",
                address="3 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("meter-2b", "SYNTHETIC-ICP-2B", False),
                ),
            ),
        ),
    )
    return (first, second)


@pytest.mark.asyncio
async def test_sync_records_each_call_before_making_it(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The in-flight context says which call failed, and carries no identifiers.

    Each measurements call names its window (k of n, newest first), its page
    and its bound: ``dates`` for a window's first page, ``before`` after.
    Every direction's newest window comes first, then each walks on: a walk
    with no stored rows stops after two empty windows, at the start of the
    history. A network failure is resent once; when the resend fails too,
    that direction alone stops, with one warning, and the others import.
    """
    seen: list[RequestContext | None] = []
    client = MagicMock()
    client.async_get_billing_period = AsyncMock(return_value=None)
    coordinator = MeridianDataCoordinator(hass, client)
    short = FakeKraken(TEST_HOUR, days=3)
    # Two channels over a full week: the newest window needs two pages.
    long = FakeKraken(TEST_HOUR, days=7, channels=2)

    async def get_accounts() -> tuple[MeridianAccount, ...]:
        seen.append(coordinator.last_request)
        return _two_account_topology()

    async def get_measurements(**kwargs: Any) -> MeasurementPage:
        seen.append(coordinator.last_request)
        if kwargs["property_id"] != "property-2b":
            return await short.async_get_measurements(**kwargs)
        if kwargs["before"] is not None:
            raise MeridianConnectionError("Unable to reach Meridian (synthetic)")
        return await long.async_get_measurements(**kwargs)

    client.async_get_accounts = AsyncMock(side_effect=get_accounts)
    client.async_get_measurements = AsyncMock(side_effect=get_measurements)
    with (
        patch(
            "custom_components.meridian_energy.coordinator._utcnow",
            return_value=TEST_HOUR,
        ),
        patch(
            "custom_components.meridian_energy.coordinator.async_latest_numeric_statistic_start",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "custom_components.meridian_energy.coordinator.async_replay",
            new=AsyncMock(return_value=(1, 1)),
        ),
    ):
        data = await coordinator.async_fetch_and_import()

    accounts_call, *measurement_calls = seen
    assert accounts_call is not None
    assert accounts_call.operation == "accounts"
    assert accounts_call.calls == 1
    assert accounts_call.direction is None
    assert accounts_call.window is None
    newest = (1, 1, "dates")
    older = [(window, 1, "dates") for window in (2, 3)]
    expected = [
        ("CONSUMPTION", 1, 2, 1, 1, *newest),
        ("GENERATION", 1, 2, 1, 1, *newest),
        ("CONSUMPTION", 2, 2, 1, 2, *newest),
        ("CONSUMPTION", 2, 2, 2, 2, 1, 1, "dates"),
        ("CONSUMPTION", 2, 2, 2, 2, 1, 2, "before"),
        ("CONSUMPTION", 2, 2, 2, 2, 1, 2, "before"),
        *(("CONSUMPTION", 1, 2, 1, 1, *call) for call in older),
        *(("GENERATION", 1, 2, 1, 1, *call) for call in older),
        *(("CONSUMPTION", 2, 2, 1, 2, *call) for call in older),
    ]
    assert [
        (
            item.direction,
            item.account_index,
            item.account_count,
            item.property_index,
            item.property_count,
            item.window,
            item.page,
            item.bound,
            item.calls,
        )
        for item in measurement_calls
        if item is not None
    ] == [(*call, number) for number, call in enumerate(expected, start=2)]
    assert {item.window_count for item in measurement_calls if item} == {13}
    # Each call is recorded before it starts.
    assert all(item is not None and item.finished_at is None for item in seen)
    failed = measurement_calls[5]
    assert failed is not None
    assert failed.operation == "measurements"
    assert failed.started_at <= monotonic()
    assert data.units_failed == 1
    assert [result.consumption_rows for result in data.results] == [1, 1, 0]
    (warning,) = [record for record in caplog.records if record.levelname == "WARNING"]
    assert warning.getMessage().startswith(
        "Meridian usage history updates are failing for one direction "
        "(direction=CONSUMPTION account=2/2 property=2/2 reason=cannot_connect "
        "status=-)"
    )
    for item in measurement_calls:
        text = repr(item)
        for identifier in ("A-SYNTHETIC", "property-2b", "meter", "ICP", "cursor"):
            assert identifier not in text


@pytest.mark.asyncio
async def test_a_call_that_returned_is_not_left_in_flight(hass) -> None:
    """A failure after the last call returned must not be blamed on that call."""
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(),))
    client.async_get_measurements = FakeKraken(TEST_HOUR, days=3).async_get_measurements
    coordinator = MeridianDataCoordinator(hass, client)
    with (
        patch(
            "custom_components.meridian_energy.coordinator._utcnow",
            return_value=TEST_HOUR,
        ),
        patch(
            "custom_components.meridian_energy.coordinator.async_latest_numeric_statistic_start",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "custom_components.meridian_energy.coordinator.async_replay",
            new=AsyncMock(side_effect=HomeAssistantError("synthetic recorder failure")),
        ),
        pytest.raises(HomeAssistantError),
    ):
        await coordinator.async_fetch_and_import()

    last = coordinator.last_request
    assert last is not None
    # Windows 2 and 3 are empty: the history started 3 days ago.
    assert (last.operation, last.direction, last.window, last.page, last.calls) == (
        "measurements",
        "CONSUMPTION",
        3,
        1,
        4,
    )
    assert last.finished_at is not None
    assert last.finished_at >= last.started_at


@pytest.mark.asyncio
async def test_a_billing_failure_the_sync_handles_is_not_left_in_flight(
    hass,
) -> None:
    client = MagicMock()
    client.async_get_billing_period = AsyncMock(
        side_effect=MeridianConnectionError("synthetic")
    )
    coordinator = MeridianDataCoordinator(hass, client)

    assert await coordinator._async_billing_period("A-TEST-1", TEST_HOUR) is None

    last = coordinator.last_request
    assert last is not None
    assert last.operation == "billing"
    assert last.finished_at is not None


@pytest.mark.asyncio
async def test_billing_call_is_recorded_and_each_sync_counts_from_one(hass) -> None:
    client = _short_history_client(_account())
    coordinator = MeridianDataCoordinator(hass, client)
    seen: list[RequestContext | None] = []
    fetch = client.async_get_measurements.side_effect

    async def get_billing_period(account_number: str) -> MeridianBillingPeriod:
        del account_number
        seen.append(coordinator.last_request)
        return MeridianBillingPeriod(
            period_length="MONTHLY",
            period_length_multiplier=1,
            is_fixed=False,
            start=None,
            end=None,
            next_billing_date=None,
            period_start_day=1,
        )

    async def get_measurements(**kwargs: Any) -> MeasurementPage:
        seen.append(coordinator.last_request)
        return await fetch(**kwargs)

    client.async_get_billing_period = AsyncMock(side_effect=get_billing_period)
    client.async_get_measurements = AsyncMock(side_effect=get_measurements)
    clock, latest, imported = _patched_import(AsyncMock(return_value=(1, 1)))
    with clock, latest, imported:
        await coordinator.async_fetch_and_import()
        first_sync = list(seen)
        finished = coordinator.last_request
        seen.clear()
        await coordinator.async_fetch_and_import()

    billing = first_sync[-1]
    assert billing is not None
    assert billing.operation == "billing"
    assert (billing.account_index, billing.account_count) == (1, 1)
    assert billing.direction is None
    assert billing.page is None
    # The account list, then a day of history and two empty weeks.
    assert billing.calls == 5
    assert billing.finished_at is None
    # Once the call has returned, the context says so.
    assert finished is not None
    assert finished.operation == "billing"
    assert finished.finished_at is not None
    assert finished.finished_at >= finished.started_at
    # The second sync reuses the cached topology and billing period: its
    # only call is the hourly update's, counted from one, with nothing left
    # over from the first sync before it.
    assert [(item.operation, item.calls) for item in seen if item] == [
        ("measurements", 1)
    ]
    assert seen[0] is not None
    assert seen[0].finished_at is None
