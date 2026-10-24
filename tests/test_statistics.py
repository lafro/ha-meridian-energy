"""Tests for cumulative Home Assistant statistics generation."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy.models import MeridianMeasurement
from custom_components.meridian_energy.statistics import (
    _aggregate_measurements,
    _change_rows,
    _statistic_kind,
    _steps_follow_states,
    _timestamp,
    async_account_period_totals,
    async_clear_statistics,
    async_has_numeric_statistics,
    async_has_statistics,
    async_wait_for_recorder,
    changed_hours,
    consumption_ids,
    generation_ids,
    property_key,
    statistic_id,
)


@pytest.fixture
def no_recorder_wait() -> Generator[AsyncMock]:
    """Stand in for the barrier before a totals read, where no recorder runs."""
    with patch.object(
        statistics_module, "async_wait_for_recorder", new=AsyncMock(return_value=True)
    ) as wait:
        yield wait


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


def test_changed_hours_are_those_not_stored_as_fetched() -> None:
    """
    An hour whose usage or cost differs from what is stored has changed.

    So has one not stored. An hour's usage is the sum of its registers, and
    its cost too, in dollars once stored; an hour whose cost Meridian did
    not return for every register is compared by its usage alone, as the
    replay keeps its stored cost. A backfill or revision window gives the
    replay only the changed hours, so a correction on another is kept.
    """
    hour = datetime(2026, 7, 12, 13, tzinfo=UTC)
    same, usage, cost, without_cost, registers, new = (
        hour + offset * timedelta(hours=1) for offset in range(6)
    )
    measurements = [
        _measurement(same, "0.5", "15"),
        _measurement(usage, "0.7", "15"),
        _measurement(cost, "0.5", "45"),
        replace(_measurement(without_cost, "0.5", "15"), cost_cents=None),
        _measurement(registers, "0.2", "6"),
        replace(_measurement(registers, "0.3", "9"), channel_id="meter:second"),
        _measurement(new, "0.5", "15"),
    ]
    energy = {same: 0.5, usage: 0.5, cost: 0.5, without_cost: 0.5, registers: 0.5}
    money = {same: 0.15, usage: 0.15, cost: 0.15, without_cost: 0.99, registers: 0.15}

    assert changed_hours(measurements, energy, money) == {usage, cost, new}
    assert changed_hours(measurements[:1], {}, {}) == {same}


def test_aggregate_rejects_non_hour_timestamp() -> None:
    with pytest.raises(ValueError, match="aligned"):
        _aggregate_measurements(
            [_measurement(datetime(2026, 7, 13, 1, 30, tzinfo=UTC), "1", "1")]
        )


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_aggregate_rejects_non_finite_energy(value: str) -> None:
    with pytest.raises(ValueError, match="finite"):
        _aggregate_measurements(
            [_measurement(datetime(2026, 7, 13, 1, tzinfo=UTC), value, "1")]
        )


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_aggregate_rejects_non_finite_cost(value: str) -> None:
    with pytest.raises(ValueError, match="finite"):
        _aggregate_measurements(
            [_measurement(datetime(2026, 7, 13, 1, tzinfo=UTC), "1", value)]
        )


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


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([], False),
        ([{"start": datetime(2026, 7, 1, tzinfo=UTC), "sum": 1}], True),
    ],
)
@pytest.mark.asyncio
async def test_has_numeric_statistics(rows, expected: bool) -> None:
    instance = MagicMock()
    instance.async_add_executor_job = AsyncMock(
        return_value={"meridian_energy:test": rows}
    )
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        assert (
            await async_has_numeric_statistics(AsyncMock(), "meridian_energy:test")
            is expected
        )


@pytest.mark.asyncio
async def test_clear_statistics_handles_empty_and_sorted_ids(hass) -> None:
    instance = MagicMock()
    instance.async_clear_statistics.side_effect = lambda _ids, *, on_done: on_done()
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        assert await async_clear_statistics(hass, set()) is True
        instance.async_clear_statistics.assert_not_called()
        assert await async_clear_statistics(hass, {"b", "a"}) is True

    assert instance.async_clear_statistics.call_args.args == (["a", "b"],)


@pytest.mark.asyncio
async def test_clear_statistics_timeout_keeps_caller_error_path(hass, caplog) -> None:
    """A slow recorder stops the wait without raising over the original error."""
    instance = MagicMock()
    with (
        patch(
            "custom_components.meridian_energy.statistics.get_instance",
            return_value=instance,
        ),
        patch(
            "custom_components.meridian_energy.statistics."
            "CLEAR_STATISTICS_TIMEOUT_SECONDS",
            0,
        ),
    ):
        removed = await async_clear_statistics(hass, {"a"})

    assert removed is False
    instance.async_clear_statistics.assert_called_once()
    # The caller reports it, in the one warning or form for that setup.
    assert caplog.records == []


@pytest.mark.asyncio
async def test_wait_for_recorder_queues_a_removal_of_nothing_and_waits_for_it(
    hass,
) -> None:
    """
    The wait is a task queued behind the recorder's work, not its commit future.

    The commit future is None while the recorder runs a task it has already
    taken off its queue, so it cannot wait for a removal in progress.
    """
    instance = MagicMock()
    finished: list[list[str]] = []

    def clear(statistic_ids: list[str], *, on_done) -> None:
        # The recorder thread calls on_done once the task has run.
        finished.append(statistic_ids)
        on_done()

    instance.async_clear_statistics.side_effect = clear
    with patch(
        "custom_components.meridian_energy.statistics.get_instance",
        return_value=instance,
    ):
        assert await async_wait_for_recorder(hass) is True

    assert finished == [[]]
    instance.async_get_commit_future.assert_not_called()


@pytest.mark.asyncio
async def test_wait_for_recorder_times_out_with_work_still_queued(hass, caplog) -> None:
    instance = MagicMock()
    with (
        patch(
            "custom_components.meridian_energy.statistics.get_instance",
            return_value=instance,
        ),
        patch(
            "custom_components.meridian_energy.statistics."
            "CLEAR_STATISTICS_TIMEOUT_SECONDS",
            0,
        ),
    ):
        assert await async_wait_for_recorder(hass) is False

    instance.async_clear_statistics.assert_called_once()
    # The setup form says so.
    assert caplog.records == []


@pytest.mark.parametrize(
    ("outcomes", "attempts", "expected"),
    [
        # The second pass is queued only once the first has run.
        ([True, True], 2, True),
        # A timed-out wait counts against the attempts, whichever pass it was.
        ([False, True, True], 3, True),
        ([True, False, True], 3, True),
        # Once too few attempts are left for the passes still to run, no
        # further wait could succeed: the next is not queued, so the setup
        # dialog does not wait 30 s more for a cleanup_pending already certain.
        ([False], 2, False),
        ([True, False], 2, False),
        ([False, True, False], 3, False),
        ([False] * 9, 10, False),
        ([False] * 8 + [True, True], 10, True),
    ],
)
@pytest.mark.asyncio
async def test_wait_for_recorder_in_two_passes_shares_its_attempts(
    hass, outcomes: list[bool], attempts: int, expected: bool
) -> None:
    removals = AsyncMock(side_effect=outcomes)
    with patch(
        "custom_components.meridian_energy.statistics._async_run_removal", removals
    ):
        assert (
            await async_wait_for_recorder(hass, attempts=attempts, passes=2) is expected
        )

    assert removals.await_count == len(outcomes)
    assert all(call.args == (hass, []) for call in removals.await_args_list)


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
            start=start,
            end=end,
            include_generation=True,
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
            include_generation=False,
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
            include_generation=False,
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
            include_generation=False,
        )

    assert result.usage == Decimal("2.0")
    assert result.cost is None
    assert result.usage_complete is True
    assert result.cost_complete is False
    assert result.export_complete is False
    assert result.credit_complete is False


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
            include_generation=False,
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
            start=start,
            end=end,
            include_generation=generation,
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
            include_generation=False,
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
