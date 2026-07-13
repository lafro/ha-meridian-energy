"""Tests for adaptive scheduling and rolling quality reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.meridian_energy.api import (
    MeridianGraphQLError,
    MeridianRateLimitError,
)
from custom_components.meridian_energy.const import (
    READING_CONSUMPTION,
    REVISION_OVERLAP,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeasurementFetchResult,
    MeridianAccount,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianProperty,
    SyncMode,
)

NOW = datetime(2026, 7, 14, 0, 0, tzinfo=UTC)
CACHE_KEY = ("property-key", READING_CONSUMPTION)


def _measurement(
    start: datetime,
    *,
    quality: str = "ESTIMATE",
    value: str = "1",
    channel: str = "meter:register",
) -> MeridianMeasurement:
    return MeridianMeasurement(
        start=start,
        end=start + timedelta(hours=1),
        value_kwh=Decimal(value),
        quality=quality,
        direction=READING_CONSUMPTION,
        channel_id=channel,
        cost_cents=Decimal(25),
    )


def _account(*, feed_in: bool = False) -> MeridianAccount:
    return MeridianAccount(
        number="synthetic-account",
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id="synthetic-property",
                address="Synthetic address",
                meter_points=(
                    MeridianMeterPoint(
                        id="meter",
                        market_identifier="synthetic-meter",
                        has_feed_in=feed_in,
                    ),
                ),
            ),
        ),
    )


def _two_property_account() -> MeridianAccount:
    first = _account().properties[0]
    second = MeridianProperty(
        id="second-property",
        address="Second synthetic address",
        meter_points=first.meter_points,
    )
    return MeridianAccount(
        number="synthetic-account",
        status="ACTIVE",
        properties=(first, second),
    )


def _fetch(density: float) -> MeasurementFetchResult:
    return MeasurementFetchResult((), 1, 0, density)


def test_sync_mode_schedule_replaces_hourly_request(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    assert coordinator._select_sync_mode(NOW) is SyncMode.INITIAL

    coordinator._initial_refresh_complete = True
    assert coordinator._select_sync_mode(NOW) is SyncMode.FULL_RECONCILIATION

    coordinator._last_full_reconciliation = NOW
    assert coordinator._select_sync_mode(NOW) is SyncMode.TARGETED_RECONCILIATION

    coordinator._last_targeted_reconciliation = NOW
    assert coordinator._select_sync_mode(NOW) is SyncMode.TIP
    assert (
        coordinator._select_sync_mode(NOW + timedelta(days=1))
        is SyncMode.TARGETED_RECONCILIATION
    )
    assert (
        coordinator._select_sync_mode(NOW + timedelta(days=7))
        is SyncMode.FULL_RECONCILIATION
    )


@pytest.mark.asyncio
async def test_topology_is_cached_for_24_hours_and_can_be_forced(hass) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(),))
    coordinator = MeridianDataCoordinator(hass, client)

    first, refreshed = await coordinator._async_get_topology(NOW)
    cached, cached_refreshed = await coordinator._async_get_topology(
        NOW + timedelta(hours=23)
    )
    expired, expired_refreshed = await coordinator._async_get_topology(
        NOW + timedelta(days=1)
    )
    forced, forced_refreshed = await coordinator._async_get_topology(
        NOW + timedelta(days=1, minutes=1), force=True
    )

    assert first == cached == expired == forced
    assert (refreshed, cached_refreshed, expired_refreshed, forced_refreshed) == (
        True,
        False,
        True,
        True,
    )
    assert client.async_get_accounts.await_count == 3
    assert coordinator._topology_cache_age(NOW) == 0


@pytest.mark.asyncio
async def test_startup_distinguishes_install_restart_and_solar(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    with patch(
        "custom_components.meridian_energy.coordinator.async_has_statistics",
        new=AsyncMock(side_effect=[True, True]),
    ) as checker:
        assert (
            await coordinator._async_startup_mode((_account(feed_in=True),))
            is SyncMode.RESTART
        )
        assert checker.await_count == 2

    coordinator = MeridianDataCoordinator(hass, MagicMock())
    with patch(
        "custom_components.meridian_energy.coordinator.async_has_statistics",
        new=AsyncMock(return_value=False),
    ):
        assert await coordinator._async_startup_mode((_account(),)) is SyncMode.INITIAL


@pytest.mark.asyncio
async def test_multiple_properties_are_processed_serially(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._async_sync_property = AsyncMock(side_effect=[MagicMock(), MagicMock()])

    results = await coordinator._async_sync_accounts(
        (_two_property_account(),), SyncMode.TIP, NOW
    )

    assert len(results) == 2
    assert [
        call.args[1].id for call in coordinator._async_sync_property.await_args_list
    ] == [
        "synthetic-property",
        "second-property",
    ]


def test_requested_windows_include_provisional_safety_and_cap(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    assert coordinator._requested_since(CACHE_KEY, SyncMode.INITIAL, NOW) == NOW - (
        timedelta(days=365)
    )
    assert coordinator._requested_since(CACHE_KEY, SyncMode.RESTART, NOW) == NOW - (
        REVISION_OVERLAP
    )
    assert coordinator._requested_since(CACHE_KEY, SyncMode.TIP, NOW) == NOW - (
        timedelta(hours=24)
    )
    assert coordinator._requested_since(
        CACHE_KEY, SyncMode.TARGETED_RECONCILIATION, NOW
    ) == NOW - timedelta(hours=48)

    provisional = _measurement(NOW - timedelta(days=5))
    coordinator._measurement_cache[CACHE_KEY] = {
        (provisional.start, provisional.channel_id): provisional
    }
    assert coordinator._requested_since(
        CACHE_KEY, SyncMode.TARGETED_RECONCILIATION, NOW
    ) == provisional.start - timedelta(hours=6)

    old = _measurement(NOW - timedelta(days=20))
    coordinator._measurement_cache[CACHE_KEY] = {(old.start, old.channel_id): old}
    assert (
        coordinator._requested_since(CACHE_KEY, SyncMode.TARGETED_RECONCILIATION, NOW)
        == NOW - REVISION_OVERLAP
    )


def test_page_size_uses_density_headroom_and_bounds(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    assert coordinator._page_size(CACHE_KEY, NOW - timedelta(hours=1), NOW) == 24
    assert coordinator._page_size(CACHE_KEY, NOW - timedelta(days=365), NOW) == 744

    coordinator._row_density[CACHE_KEY] = 2.0
    assert coordinator._page_size(CACHE_KEY, NOW - timedelta(hours=24), NOW) == 64


def test_merge_replays_from_change_and_protects_actual(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    expired = _measurement(NOW - REVISION_OVERLAP - timedelta(hours=1))
    recent = tuple(_measurement(NOW - timedelta(hours=offset)) for offset in (3, 2, 1))

    initial = coordinator._merge_measurements(
        CACHE_KEY, (expired, *recent), NOW, initial_import=True
    )
    assert initial == (expired, *recent)
    assert expired not in coordinator._measurement_cache[CACHE_KEY].values()

    actual = _measurement(recent[1].start, quality="ACTUAL", value="1.1")
    replay = coordinator._merge_measurements(CACHE_KEY, (actual,), NOW)
    assert replay == (actual, recent[2])

    regression = _measurement(recent[1].start, quality="ESTIMATE", value="9")
    assert coordinator._merge_measurements(CACHE_KEY, (regression,), NOW) == ()
    assert (
        coordinator._measurement_cache[CACHE_KEY][(actual.start, actual.channel_id)]
        == actual
    )


def test_density_observation_is_smoothed_and_ignores_empty(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._remember_density(CACHE_KEY, _fetch(0))
    assert CACHE_KEY not in coordinator._row_density
    coordinator._remember_density(CACHE_KEY, _fetch(2))
    coordinator._remember_density(CACHE_KEY, _fetch(1))
    assert coordinator._row_density[CACHE_KEY] == 1.75


@pytest.mark.asyncio
async def test_daily_and_weekly_modes_update_only_their_cadence(hass) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(),))
    coordinator = MeridianDataCoordinator(hass, client)
    coordinator._initial_refresh_complete = True
    coordinator._async_sync_accounts = AsyncMock(return_value=())

    coordinator._last_full_reconciliation = NOW
    coordinator._last_targeted_reconciliation = NOW - timedelta(days=1)
    with patch(
        "custom_components.meridian_energy.coordinator._utcnow", return_value=NOW
    ):
        daily = await coordinator.async_fetch_and_import()
    assert daily.sync_mode is SyncMode.TARGETED_RECONCILIATION
    assert coordinator._last_targeted_reconciliation == NOW
    assert coordinator._last_full_reconciliation == NOW

    coordinator._last_full_reconciliation = NOW - timedelta(days=7)
    with patch(
        "custom_components.meridian_energy.coordinator._utcnow",
        return_value=NOW + timedelta(hours=1),
    ):
        weekly = await coordinator.async_fetch_and_import()
    assert weekly.sync_mode is SyncMode.FULL_RECONCILIATION
    assert coordinator._last_full_reconciliation == NOW + timedelta(hours=1)


@pytest.mark.asyncio
async def test_topology_error_forces_one_refresh_and_retry(hass) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(),))
    coordinator = MeridianDataCoordinator(hass, client)
    coordinator._topology = (_account(),)
    coordinator._topology_cached_at = NOW
    coordinator._initial_refresh_complete = True
    coordinator._last_full_reconciliation = NOW
    coordinator._last_targeted_reconciliation = NOW
    coordinator._async_sync_accounts = AsyncMock(
        side_effect=[MeridianGraphQLError("measurements", ("PROPERTY_NOT_FOUND",)), ()]
    )

    with patch(
        "custom_components.meridian_energy.coordinator._utcnow", return_value=NOW
    ):
        result = await coordinator.async_fetch_and_import()

    assert result.topology_refreshed is True
    client.async_get_accounts.assert_awaited_once()
    assert coordinator._async_sync_accounts.await_count == 2


@pytest.mark.asyncio
async def test_rate_limit_delay_is_passed_to_home_assistant(hass) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=MeridianRateLimitError(321))
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(UpdateFailed) as raised:
        await coordinator.async_fetch_and_import()

    assert raised.value.retry_after == 321


@pytest.mark.parametrize(
    "now",
    [
        datetime(2026, 4, 4, 13, 30, tzinfo=UTC),
        datetime(2026, 9, 26, 14, 30, tzinfo=UTC),
    ],
)
@pytest.mark.asyncio
async def test_api_end_date_uses_new_zealand_date_across_dst(hass, now) -> None:
    client = MagicMock()
    client.async_get_measurements = AsyncMock(
        return_value=MagicMock(
            measurements=(), has_previous_page=False, start_cursor=None
        )
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with patch(
        "custom_components.meridian_energy.coordinator._utcnow", return_value=now
    ):
        await coordinator._async_fetch_since(
            account_number="synthetic-account",
            property_id="synthetic-property",
            direction=READING_CONSUMPTION,
            since=now - timedelta(hours=24),
        )

    assert client.async_get_measurements.await_args.kwargs["end_on"] == (
        now.astimezone(ZoneInfo("Pacific/Auckland")).date().isoformat()
    )
