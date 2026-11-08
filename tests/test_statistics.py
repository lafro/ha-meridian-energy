"""Tests for the statistics helpers: identifiers and the bill totals."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy.statistics import (
    _change_rows,
    _statistic_kind,
    _steps_follow_states,
    _timestamp,
    async_account_period_totals,
    consumption_ids,
    generation_ids,
    property_key,
    statistic_id,
)


@pytest.fixture
def no_recorder_wait() -> Generator[AsyncMock]:
    """Stand in for the barrier before a totals read, where no recorder runs."""
    with patch.object(
        statistics_module, "_async_run_removal", new=AsyncMock(return_value=True)
    ) as wait:
        yield wait


def test_property_key_is_stable_and_redacted() -> None:
    result = property_key("A-SECRET", "property-secret")

    assert result == property_key("A-SECRET", "property-secret")
    assert len(result) == 12
    assert "SECRET" not in result


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


@pytest.mark.usefixtures("no_recorder_wait")
@pytest.mark.asyncio
async def test_account_period_totals_combines_properties_and_generation() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=2)
    first_consumption, first_cost = consumption_ids("first")
    second_consumption, second_cost = consumption_ids("second")
    first_generation, first_credit = generation_ids("first")
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={
            first_consumption: [
                {"start": start.timestamp(), "change": 1.0, "state": 1.0},
                {
                    "start": (start + timedelta(hours=1)).timestamp(),
                    "change": 2.0,
                    "state": 2.0,
                },
            ],
            first_cost: [
                {"start": start.timestamp(), "change": 0.3, "state": 0.3},
                {
                    "start": (start + timedelta(hours=1)).timestamp(),
                    "change": 0.6,
                    "state": 0.6,
                },
            ],
            second_consumption: [
                {"start": start.timestamp(), "change": 4.0, "state": 4.0},
                {
                    "start": (start + timedelta(hours=1)).timestamp(),
                    "change": 5.0,
                    "state": 5.0,
                },
            ],
            second_cost: [
                {"start": start.timestamp(), "change": 1.2, "state": 1.2},
                {
                    "start": (start + timedelta(hours=1)).timestamp(),
                    "change": 1.5,
                    "state": 1.5,
                },
            ],
            first_generation: [
                {"start": start.timestamp(), "change": 0.5, "state": 0.5}
            ],
            first_credit: [{"start": start.timestamp(), "change": 0.1, "state": 0.1}],
        }
    )
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        result = await async_account_period_totals(
            AsyncMock(),
            property_keys=("first", "second"),
            feed_in_keys=("first",),
            start=start,
            end=end,
        )

    assert result.usage == Decimal("12.0")
    assert result.cost == Decimal("3.6")
    assert result.export == Decimal("0.5")
    assert result.credit == Decimal("0.1")
    assert result.usage_complete is True
    assert result.cost_complete is True
    assert result.export_complete is True
    assert result.credit_complete is True


@pytest.mark.usefixtures("no_recorder_wait")
@pytest.mark.asyncio
async def test_account_period_totals_withholds_incomplete_cost_and_history() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=2)
    energy_id, cost_id = consumption_ids("first")
    instance = MagicMock()
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
            feed_in_keys=(),
        )

    assert result.usage is None
    assert result.cost is None
    assert result.usage_complete is False
    assert result.cost_complete is False


@pytest.mark.parametrize(
    "start",
    [
        datetime(2026, 7, 1, tzinfo=UTC),
        datetime(2026, 9, 26, 12, tzinfo=UTC),
    ],
)
@pytest.mark.usefixtures("no_recorder_wait")
@pytest.mark.asyncio
async def test_account_period_totals_withholds_matched_internal_hourly_gap(
    start: datetime,
) -> None:
    end = start + timedelta(hours=3)
    energy_id, cost_id = consumption_ids("first")
    rows = [start.timestamp(), (start + timedelta(hours=2)).timestamp()]
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={
            energy_id: [
                {"start": value, "change": 1.0, "state": 1.0} for value in rows
            ],
            cost_id: [{"start": value, "change": 0.3, "state": 0.3} for value in rows],
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
            feed_in_keys=(),
        )

    assert result.usage is None
    assert result.cost is None
    assert result.usage_complete is False
    assert result.cost_complete is False


@pytest.mark.usefixtures("no_recorder_wait")
@pytest.mark.asyncio
async def test_account_period_totals_reports_per_metric_completeness() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=2)
    energy_id, cost_id = consumption_ids("first")
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={
            energy_id: [
                {"start": start.timestamp(), "change": 1.0, "state": 1.0},
                {
                    "start": (start + timedelta(hours=1)).timestamp(),
                    "change": 1.0,
                    "state": 1.0,
                },
            ],
            cost_id: [{"start": start.timestamp(), "change": 0.3, "state": 0.3}],
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
            feed_in_keys=(),
        )

    assert result.usage == Decimal("2.0")
    assert result.cost is None
    assert result.usage_complete is True
    assert result.cost_complete is False
    assert result.export_complete is False
    assert result.credit_complete is False


@pytest.mark.usefixtures("no_recorder_wait")
@pytest.mark.asyncio
async def test_multi_property_totals_share_latest_published_horizon() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=3)
    first_energy, first_cost = consumption_ids("first")
    second_energy, second_cost = consumption_ids("second")
    full_starts = [start + timedelta(hours=offset) for offset in range(3)]
    stale_starts = [start]
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={
            first_energy: [
                {"start": value.timestamp(), "change": 1.0, "state": 1.0}
                for value in full_starts
            ],
            first_cost: [
                {"start": value.timestamp(), "change": 0.3, "state": 0.3}
                for value in full_starts
            ],
            second_energy: [
                {"start": value.timestamp(), "change": 1.0, "state": 1.0}
                for value in stale_starts
            ],
            second_cost: [
                {"start": value.timestamp(), "change": 0.3, "state": 0.3}
                for value in stale_starts
            ],
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
            feed_in_keys=(),
        )

    assert result.usage is None
    assert result.cost is None
    assert result.usage_complete is False
    assert result.cost_complete is False


@pytest.mark.parametrize("generation", [False, True])
@pytest.mark.usefixtures("no_recorder_wait")
@pytest.mark.asyncio
async def test_account_period_totals_withhold_money_whose_steps_miss_its_states(
    generation: bool,
) -> None:
    """A cost or credit row whose running total does not step by its state."""
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=2)
    energy_id, money_id = (generation_ids if generation else consumption_ids)("first")
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={
            energy_id: [
                {"start": start.timestamp(), "change": 1.0, "state": 1.0},
                {
                    "start": (start + timedelta(hours=1)).timestamp(),
                    "change": 1.0,
                    "state": 1.0,
                },
            ],
            money_id: [
                {"start": start.timestamp(), "change": 0.3, "state": 0.3},
                {
                    "start": (start + timedelta(hours=1)).timestamp(),
                    "change": 0.1,
                    "state": 0.3,
                },
            ],
        }
    )
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        result = await async_account_period_totals(
            AsyncMock(),
            property_keys=("first",),
            feed_in_keys=("first",) if generation else (),
            start=start,
            end=end,
        )

    if generation:
        assert result.export == Decimal("2.0")
        assert result.credit is None
        assert result.credit_complete is False
    else:
        assert result.usage == Decimal("2.0")
        assert result.cost is None
        assert result.cost_complete is False


@pytest.mark.asyncio
async def test_account_period_totals_read_on_after_a_barrier_timed_out(
    no_recorder_wait: AsyncMock,
) -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    energy_id, cost_id = consumption_ids("first")
    no_recorder_wait.return_value = False
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={
            energy_id: [{"start": start.timestamp(), "change": 1.0, "state": 1.0}],
            cost_id: [{"start": start.timestamp(), "change": 0.3, "state": 0.3}],
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
            end=start + timedelta(hours=1),
            feed_in_keys=(),
        )

    assert result.usage == Decimal("1.0")
    assert result.cost == Decimal("0.3")


def test_steps_follow_states_only_inside_the_range() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=1)
    rows = [
        {"start": start - timedelta(hours=1), "change": 5.0, "state": 1.0},
        {"start": start, "change": 1.0, "state": 1.0},
        {"start": end, "change": 9.0, "state": 1.0},
    ]
    assert _steps_follow_states(rows, start, end) is True
    rows[1]["state"] = None
    assert _steps_follow_states(rows, start, end) is False


def test_statistic_kind_leaves_out_the_key() -> None:
    assert _statistic_kind(consumption_ids("abc123")[1]) == "consumption_cost"
    assert _statistic_kind(generation_ids("abc123")[0]) == "generation"


def test_change_rows_filters_boundaries_and_missing_values() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(hours=1)
    rows = [
        {"start": start - timedelta(hours=1), "change": 1, "state": 1},
        {"start": start, "change": None, "state": None},
        {"start": start + timedelta(minutes=30), "change": 2, "state": 2},
        {"start": end, "change": 3, "state": 3},
    ]
    assert _change_rows(rows, start, end) == [
        ((start + timedelta(minutes=30)).timestamp(), Decimal(2))
    ]
