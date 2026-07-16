"""Home Assistant external-statistics import for Meridian Energy."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from hashlib import sha256
from itertools import pairwise
from typing import cast

from homeassistant.components.recorder import get_instance  # type: ignore[attr-defined]
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    StatisticsRow,
    async_add_external_statistics,
    clear_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.util.unit_conversion import EnergyConverter

from .const import (
    DOMAIN,
    INITIAL_BACKFILL,
    STAT_CONSUMPTION,
    STAT_CONSUMPTION_COST,
    STAT_GENERATION,
    STAT_GENERATION_CREDIT,
)
from .models import BillingPeriodTotals, MeridianMeasurement

_CENTS_PER_DOLLAR = Decimal(100)
_SECONDS_PER_HOUR = 3600


def property_key(account_number: str, property_id: str) -> str:
    """Return a stable non-sensitive key for a Meridian property."""
    value = f"{account_number}:{property_id}".encode()
    return sha256(value).hexdigest()[:12]


def account_key(account_number: str) -> str:
    """Return a stable non-sensitive key for a Meridian account."""
    return sha256(account_number.encode()).hexdigest()[:12]


def statistic_id(kind: str, key: str) -> str:
    """Return a valid external statistic ID."""
    return f"{DOMAIN}:{kind}_{key}"


async def async_has_statistics(hass: HomeAssistant, stat_id: str) -> bool:
    """Return whether a statistic already has at least one row."""
    instance = get_instance(hass)
    result = await instance.async_add_executor_job(
        get_last_statistics,
        hass,
        1,
        stat_id,
        False,
        {"sum"},
    )
    return bool(result.get(stat_id))


async def async_has_numeric_statistics(hass: HomeAssistant, stat_id: str) -> bool:
    """Return whether a statistic contains at least one numeric cumulative row."""
    return await async_latest_numeric_statistic_start(hass, stat_id) is not None


async def async_latest_numeric_statistic_start(
    hass: HomeAssistant, stat_id: str
) -> datetime | None:
    """Return the newest trustworthy cumulative row timestamp, if one exists."""
    instance = get_instance(hass)
    result = await instance.async_add_executor_job(
        get_last_statistics,
        hass,
        int(INITIAL_BACKFILL.total_seconds() // 3600) + 72,
        stat_id,
        False,
        {"sum"},
    )
    numeric_rows = [
        row for row in result.get(stat_id, []) if row.get("sum") is not None
    ]
    if not numeric_rows:
        return None
    latest = max(numeric_rows, key=lambda row: _timestamp(row["start"]))
    return datetime.fromtimestamp(_timestamp(latest["start"]), tz=UTC)


async def async_clear_statistics(hass: HomeAssistant, statistic_ids: set[str]) -> None:
    """Remove statistics created by an incomplete first-install import."""
    if not statistic_ids:
        return
    instance = get_instance(hass)
    await instance.async_add_executor_job(
        clear_statistics, instance, sorted(statistic_ids)
    )


async def async_import_measurements(
    hass: HomeAssistant,
    *,
    stat_energy_id: str,
    stat_cost_id: str,
    energy_name: str,
    cost_name: str,
    measurements: Iterable[MeridianMeasurement],
) -> tuple[int, int]:
    """Aggregate interval data and upsert cumulative energy and cost statistics."""
    aggregated = _aggregate_measurements(measurements)
    if not aggregated:
        return (0, 0)

    first_start = min(aggregated)
    energy_baseline = await _async_baseline_sum(hass, stat_energy_id, first_start)
    cost_baseline = await _async_baseline_sum(hass, stat_cost_id, first_start)
    if energy_baseline is None:
        return (0, 0)

    energy_total = Decimal(str(energy_baseline))
    cost_total = Decimal(str(cost_baseline or 0.0))
    energy_rows: list[StatisticData] = []
    cost_rows: list[StatisticData] = []
    for start in sorted(aggregated):
        energy, cost = aggregated[start]
        energy_total += energy
        energy_value = float(energy_total)
        energy_rows.append({"start": start, "state": energy_value, "sum": energy_value})
        if cost is not None and cost_baseline is not None:
            cost_total += cost / _CENTS_PER_DOLLAR
            cost_value = float(cost_total)
            cost_rows.append({"start": start, "state": cost_value, "sum": cost_value})

    async_add_external_statistics(
        hass,
        _energy_metadata(stat_energy_id, energy_name),
        energy_rows,
    )
    if cost_rows:
        async_add_external_statistics(
            hass,
            _cost_metadata(stat_cost_id, cost_name),
            cost_rows,
        )
    return (len(energy_rows), len(cost_rows))


async def _async_baseline_sum(
    hass: HomeAssistant, stat_id: str, first_start: datetime
) -> float | None:
    """Return the cumulative sum immediately before an overlap import."""
    instance = get_instance(hass)
    # A direction recovery can replay the complete supported backfill window.
    # Request enough rows to retain a trustworthy predecessor even when the
    # replay begins 90 days in the past and newer rows already exist.
    number_of_stats = int(INITIAL_BACKFILL.total_seconds() // 3600) + 72
    result = await instance.async_add_executor_job(
        partial(
            get_last_statistics,
            hass,
            number_of_stats,
            stat_id,
            False,
            {"sum"},
        )
    )
    candidates = [
        row
        for row in result.get(stat_id, [])
        if _timestamp(row["start"]) < first_start.timestamp()
    ]
    if not candidates:
        return 0.0
    numeric_candidates = [row for row in candidates if row.get("sum") is not None]
    if not numeric_candidates:
        return None
    latest = max(numeric_candidates, key=lambda row: _timestamp(row["start"]))
    return float(cast("float", latest["sum"]))


def _aggregate_measurements(
    measurements: Iterable[MeridianMeasurement],
) -> dict[datetime, tuple[Decimal, Decimal | None]]:
    """Aggregate multiple registers into one row per UTC hour."""
    energy: defaultdict[datetime, Decimal] = defaultdict(lambda: Decimal(0))
    cost: defaultdict[datetime, Decimal] = defaultdict(lambda: Decimal(0))
    cost_complete: defaultdict[datetime, bool] = defaultdict(lambda: True)
    for measurement in measurements:
        start = measurement.start.astimezone(UTC)
        if start.minute or start.second or start.microsecond:
            raise ValueError("Meridian measurement is not aligned to an hour")
        if not measurement.value_kwh.is_finite():
            raise ValueError("Meridian measurement value is not finite")
        energy[start] += measurement.value_kwh
        if measurement.cost_cents is None:
            cost_complete[start] = False
        else:
            if not measurement.cost_cents.is_finite():
                raise ValueError("Meridian measurement cost is not finite")
            cost[start] += measurement.cost_cents
    return {
        start: (energy[start], cost[start] if cost_complete[start] else None)
        for start in energy
    }


async def async_account_period_totals(
    hass: HomeAssistant,
    *,
    property_keys: tuple[str, ...],
    start: datetime,
    end: datetime,
    include_generation: bool,
) -> BillingPeriodTotals:
    """Return complete account totals for an exact UTC billing-period range."""
    instance = get_instance(hass)
    await instance.async_block_till_done()
    statistic_ids: set[str] = set()
    pairs: list[tuple[str, str, bool]] = []
    for key in property_keys:
        consumption_energy, consumption_cost = consumption_ids(key)
        pairs.append((consumption_energy, consumption_cost, False))
        statistic_ids.update((consumption_energy, consumption_cost))
        if include_generation:
            generation_energy, generation_credit = generation_ids(key)
            pairs.append((generation_energy, generation_credit, True))
            statistic_ids.update((generation_energy, generation_credit))

    result = await instance.async_add_executor_job(
        partial(
            statistics_during_period,
            hass,
            start,
            end,
            statistic_ids,
            "hour",
            None,
            {"change"},
        )
    )

    usage = Decimal(0)
    cost = Decimal(0)
    exported = Decimal(0)
    credit = Decimal(0)
    consumption_complete = True
    cost_complete = True
    generation_complete = True
    credit_complete = True
    saw_consumption = False
    saw_generation = False

    pair_rows = [
        (
            _change_rows(result.get(energy_id, []), start, end),
            _change_rows(result.get(money_id, []), start, end),
            generation,
        )
        for energy_id, money_id, generation in pairs
    ]
    consumption_horizon = max(
        (
            row[0]
            for energy_rows, _money_rows, generation in pair_rows
            if not generation
            for row in energy_rows
        ),
        default=None,
    )
    generation_horizon = max(
        (
            row[0]
            for energy_rows, _money_rows, generation in pair_rows
            if generation
            for row in energy_rows
        ),
        default=None,
    )

    for energy_rows, money_rows, generation in pair_rows:
        energy_starts = {row[0] for row in energy_rows}
        money_starts = {row[0] for row in money_rows}
        energy_contiguous = _hourly_rows_are_contiguous(
            energy_starts,
            start,
            generation_horizon if generation else consumption_horizon,
        )
        if generation:
            if not energy_rows:
                continue
            saw_generation = True
            generation_complete &= energy_contiguous
            credit_complete &= energy_starts == money_starts
            exported += sum((row[1] for row in energy_rows), Decimal(0))
            credit += sum((row[1] for row in money_rows), Decimal(0))
        else:
            saw_consumption |= bool(energy_rows)
            consumption_complete &= energy_contiguous
            cost_complete &= energy_starts == money_starts
            usage += sum((row[1] for row in energy_rows), Decimal(0))
            cost += sum((row[1] for row in money_rows), Decimal(0))

    complete = saw_consumption and consumption_complete
    return BillingPeriodTotals(
        usage=usage if complete else None,
        cost=cost if complete and cost_complete else None,
        export=(exported if saw_generation and generation_complete else None),
        credit=(
            credit
            if saw_generation and generation_complete and credit_complete
            else None
        ),
        complete=complete,
    )


def _hourly_rows_are_contiguous(
    starts: set[float], period_start: datetime, expected_horizon: float | None
) -> bool:
    """Return whether published rows cover every UTC hour from period start."""
    if (
        not starts
        or expected_horizon is None
        or min(starts) != period_start.timestamp()
        or max(starts) != expected_horizon
    ):
        return False
    ordered = sorted(starts)
    return all(
        later - earlier == _SECONDS_PER_HOUR for earlier, later in pairwise(ordered)
    )


def _change_rows(
    rows: list[StatisticsRow], start: datetime, end: datetime
) -> list[tuple[float, Decimal]]:
    """Return timestamp/change pairs constrained to the requested range."""
    result: list[tuple[float, Decimal]] = []
    for row in rows:
        timestamp = _timestamp(cast("float | datetime", row["start"]))
        change = row.get("change")
        if start.timestamp() <= timestamp < end.timestamp() and change is not None:
            result.append((timestamp, Decimal(str(change))))
    return result


def _energy_metadata(stat_id: str, name: str) -> StatisticMetaData:
    return {
        "has_sum": True,
        "mean_type": StatisticMeanType.NONE,
        "name": name,
        "source": DOMAIN,
        "statistic_id": stat_id,
        "unit_class": EnergyConverter.UNIT_CLASS,
        "unit_of_measurement": UnitOfEnergy.KILO_WATT_HOUR,
    }


def _cost_metadata(stat_id: str, name: str) -> StatisticMetaData:
    return {
        "has_sum": True,
        "mean_type": StatisticMeanType.NONE,
        "name": name,
        "source": DOMAIN,
        "statistic_id": stat_id,
        "unit_class": None,
        "unit_of_measurement": "NZD",
    }


def _timestamp(value: float | datetime) -> float:
    return value.timestamp() if isinstance(value, datetime) else float(value)


def consumption_ids(key: str) -> tuple[str, str]:
    """Return consumption and consumption-cost statistic IDs."""
    return (
        statistic_id(STAT_CONSUMPTION, key),
        statistic_id(STAT_CONSUMPTION_COST, key),
    )


def generation_ids(key: str) -> tuple[str, str]:
    """Return generation and generation-credit statistic IDs."""
    return (
        statistic_id(STAT_GENERATION, key),
        statistic_id(STAT_GENERATION_CREDIT, key),
    )
