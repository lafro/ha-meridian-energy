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
    _change_rows,
    _timestamp,
    async_account_period_totals,
    async_clear_statistics,
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
async def test_clear_statistics_handles_empty_and_sorted_ids() -> None:
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock()
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        await async_clear_statistics(AsyncMock(), set())
        instance.async_add_executor_job.assert_not_awaited()
        await async_clear_statistics(AsyncMock(), {"b", "a"})

    assert instance.async_add_executor_job.await_args.args[2] == ["a", "b"]


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


@pytest.mark.asyncio
async def test_account_period_totals_combines_properties_and_generation() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=2)
    first_consumption, first_cost = consumption_ids("first")
    second_consumption, second_cost = consumption_ids("second")
    first_generation, first_credit = generation_ids("first")
    instance = MagicMock()
    instance.async_block_till_done = AsyncMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={
            first_consumption: [
                {"start": start.timestamp(), "change": 1.0},
                {"start": (start + timedelta(hours=1)).timestamp(), "change": 2.0},
            ],
            first_cost: [
                {"start": start.timestamp(), "change": 0.3},
                {"start": (start + timedelta(hours=1)).timestamp(), "change": 0.6},
            ],
            second_consumption: [{"start": start.timestamp(), "change": 4.0}],
            second_cost: [{"start": start.timestamp(), "change": 1.2}],
            first_generation: [{"start": start.timestamp(), "change": 0.5}],
            first_credit: [{"start": start.timestamp(), "change": 0.1}],
        }
    )
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        result = await async_account_period_totals(
            AsyncMock(),
            property_keys=("first", "second"),
            start=start,
            end=end,
            include_generation=True,
        )

    assert result.usage == Decimal("7.0")
    assert result.cost == Decimal("2.1")
    assert result.export == Decimal("0.5")
    assert result.credit == Decimal("0.1")
    assert result.complete is True
    instance.async_block_till_done.assert_awaited_once()


@pytest.mark.asyncio
async def test_account_period_totals_withholds_incomplete_cost_and_history() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=2)
    energy_id, cost_id = consumption_ids("first")
    instance = MagicMock()
    instance.async_block_till_done = AsyncMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={
            energy_id: [
                {
                    "start": (start + timedelta(hours=1)).timestamp(),
                    "change": 2.0,
                }
            ],
            cost_id: [],
        }
    )
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        result = await async_account_period_totals(
            AsyncMock(),
            property_keys=("first",),
            start=start,
            end=end,
            include_generation=False,
        )

    assert result.usage is None
    assert result.cost is None
    assert result.complete is False


def test_aggregate_marks_hourly_cost_incomplete() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    missing = _measurement(start, "1", "1")
    missing = MeridianMeasurement(
        start=missing.start,
        end=missing.end,
        value_kwh=missing.value_kwh,
        quality=missing.quality,
        direction=missing.direction,
        channel_id=missing.channel_id,
        cost_cents=None,
    )
    assert _aggregate_measurements([missing])[start] == (Decimal(1), None)


@pytest.mark.asyncio
async def test_import_with_missing_cost_writes_only_energy() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    measurement = _measurement(start, "1", "1")
    measurement = MeridianMeasurement(
        start=measurement.start,
        end=measurement.end,
        value_kwh=measurement.value_kwh,
        quality=measurement.quality,
        direction=measurement.direction,
        channel_id=measurement.channel_id,
        cost_cents=None,
    )
    with (
        patch(
            "custom_components.meridian_energy.statistics._async_baseline_sum",
            new=AsyncMock(side_effect=[0, 0]),
        ),
        patch(
            "custom_components.meridian_energy.statistics.async_add_external_statistics"
        ) as add_statistics,
    ):
        result = await async_import_measurements(
            AsyncMock(),
            stat_energy_id="meridian_energy:energy",
            stat_cost_id="meridian_energy:cost",
            energy_name="Energy",
            cost_name="Cost",
            measurements=[measurement],
        )

    assert result == (1, 0)
    add_statistics.assert_called_once()


def test_change_rows_filters_boundaries_and_missing_values() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=1)
    rows = [
        {"start": start - timedelta(hours=1), "change": 1},
        {"start": start, "change": None},
        {"start": start + timedelta(minutes=30), "change": 2},
        {"start": end, "change": 3},
    ]
    assert _change_rows(rows, start, end) == [
        ((start + timedelta(minutes=30)).timestamp(), Decimal(2))
    ]
