"""Tests for cumulative Home Assistant statistics generation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.meridian_energy.models import MeridianMeasurement
from custom_components.meridian_energy.statistics import (
    _aggregate_measurements,
    _async_baseline_sum,
    _timestamp,
    async_has_statistics,
    async_import_measurements,
    consumption_ids,
    generation_ids,
    property_key,
    statistic_id,
)


def _measurement(
    start: datetime,
    value: str,
    cost_cents: str,
    *,
    quality: str = "ACTUAL",
) -> MeridianMeasurement:
    return MeridianMeasurement(
        start=start,
        end=start + timedelta(hours=1),
        value_kwh=Decimal(value),
        quality=quality,
        direction="CONSUMPTION",
        channel_id="meter:register",
        cost_cents=Decimal(cost_cents),
    )


def test_property_key_is_stable_and_redacted() -> None:
    result = property_key("A-SECRET", "property-secret")

    assert result == property_key("A-SECRET", "property-secret")
    assert len(result) == 12
    assert "SECRET" not in result


def test_aggregate_multiple_registers_at_same_utc_hour() -> None:
    start_a = datetime(2026, 7, 13, 1, tzinfo=timezone(timedelta(hours=12)))
    start_b = datetime(2026, 7, 12, 13, tzinfo=UTC)

    result = _aggregate_measurements(
        [_measurement(start_a, "1.2", "30"), _measurement(start_b, "0.8", "20")]
    )

    assert result == {
        datetime(2026, 7, 12, 13, tzinfo=UTC): (Decimal("2.0"), Decimal(50))
    }


def test_aggregate_rejects_non_hour_timestamp() -> None:
    with pytest.raises(ValueError, match="aligned"):
        _aggregate_measurements(
            [_measurement(datetime(2026, 7, 13, 1, 30, tzinfo=UTC), "1", "1")]
        )


@pytest.mark.asyncio
async def test_import_builds_monotonic_energy_and_dollar_sums() -> None:
    measurements = [
        _measurement(datetime(2026, 7, 13, 0, tzinfo=UTC), "1.25", "50"),
        _measurement(datetime(2026, 7, 13, 1, tzinfo=UTC), "0.75", "25"),
    ]
    with (
        patch(
            "custom_components.meridian_energy.statistics._async_baseline_sum",
            new=AsyncMock(side_effect=[100.0, 20.0]),
        ),
        patch(
            "custom_components.meridian_energy.statistics.async_add_external_statistics"
        ) as add_statistics,
    ):
        result = await async_import_measurements(
            AsyncMock(),
            stat_energy_id="meridian_energy:consumption_test",
            stat_cost_id="meridian_energy:cost_test",
            energy_name="Consumption",
            cost_name="Cost",
            measurements=measurements,
        )

    assert result == (2, 2)
    energy_rows = add_statistics.call_args_list[0].args[2]
    cost_rows = add_statistics.call_args_list[1].args[2]
    assert [row["sum"] for row in energy_rows] == [101.25, 102.0]
    assert [row["sum"] for row in cost_rows] == [20.5, 20.75]
    assert add_statistics.call_args_list[0].args[1]["unit_class"] == "energy"
    assert add_statistics.call_args_list[1].args[1]["unit_class"] is None


@pytest.mark.asyncio
async def test_import_empty_measurements_does_nothing() -> None:
    with patch(
        "custom_components.meridian_energy.statistics.async_add_external_statistics"
    ) as add_statistics:
        result = await async_import_measurements(
            AsyncMock(),
            stat_energy_id="meridian_energy:consumption_test",
            stat_cost_id="meridian_energy:cost_test",
            energy_name="Consumption",
            cost_name="Cost",
            measurements=[],
        )

    assert result == (0, 0)
    add_statistics.assert_not_called()


@pytest.mark.parametrize(("rows", "expected"), [([], False), ([{"sum": 1}], True)])
@pytest.mark.asyncio
async def test_has_statistics(rows, expected: bool) -> None:
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={"meridian_energy:test": rows}
    )
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        assert (
            await async_has_statistics(AsyncMock(), "meridian_energy:test") is expected
        )


@pytest.mark.asyncio
async def test_baseline_selects_latest_row_before_overlap() -> None:
    first = datetime(2026, 7, 13, 2, tzinfo=UTC)
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={
            "meridian_energy:test": [
                {"start": first.timestamp() - 7200, "sum": 10},
                {"start": first - timedelta(hours=1), "sum": 12},
                {"start": first.timestamp(), "sum": 15},
            ]
        }
    )
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        assert (
            await _async_baseline_sum(AsyncMock(), "meridian_energy:test", first) == 12
        )


@pytest.mark.asyncio
async def test_baseline_defaults_to_zero_without_prior_row() -> None:
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(return_value={})
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        assert (
            await _async_baseline_sum(
                AsyncMock(), "meridian_energy:test", datetime.now(UTC)
            )
            == 0
        )


def test_statistic_identifier_helpers() -> None:
    assert statistic_id("kind", "key") == "meridian_energy:kind_key"
    assert consumption_ids("key") == (
        "meridian_energy:consumption_key",
        "meridian_energy:consumption_cost_key",
    )
    assert generation_ids("key") == (
        "meridian_energy:generation_key",
        "meridian_energy:generation_credit_key",
    )
    now = datetime.now(UTC)
    assert _timestamp(now) == now.timestamp()
    assert _timestamp(1.5) == 1.5
