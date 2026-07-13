"""Home Assistant external-statistics import for Meridian Energy."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from hashlib import sha256
from typing import cast

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
)
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.util.unit_conversion import EnergyConverter

from .const import (
    DOMAIN,
    REVISION_OVERLAP,
    STAT_CONSUMPTION,
    STAT_CONSUMPTION_COST,
    STAT_GENERATION,
    STAT_GENERATION_CREDIT,
)
from .models import MeridianMeasurement

_CENTS_PER_DOLLAR = Decimal(100)


def property_key(account_number: str, property_id: str) -> str:
    """Return a stable non-sensitive key for a Meridian property."""
    value = f"{account_number}:{property_id}".encode()
    return sha256(value).hexdigest()[:12]


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

    energy_total = Decimal(str(energy_baseline))
    cost_total = Decimal(str(cost_baseline))
    energy_rows: list[StatisticData] = []
    cost_rows: list[StatisticData] = []
    for start in sorted(aggregated):
        energy, cost = aggregated[start]
        energy_total += energy
        cost_total += cost / _CENTS_PER_DOLLAR
        energy_rows.append({"start": start, "sum": float(energy_total)})
        cost_rows.append({"start": start, "sum": float(cost_total)})

    async_add_external_statistics(
        hass,
        _energy_metadata(stat_energy_id, energy_name),
        energy_rows,
    )
    async_add_external_statistics(
        hass,
        _cost_metadata(stat_cost_id, cost_name),
        cost_rows,
    )
    return (len(energy_rows), len(cost_rows))


async def _async_baseline_sum(
    hass: HomeAssistant, stat_id: str, first_start: datetime
) -> float:
    """Return the cumulative sum immediately before an overlap import."""
    instance = get_instance(hass)
    number_of_stats = int(REVISION_OVERLAP.total_seconds() // 3600) + 72
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
        and row.get("sum") is not None
    ]
    if not candidates:
        return 0.0
    latest = max(candidates, key=lambda row: _timestamp(row["start"]))
    return float(cast("float", latest["sum"]))


def _aggregate_measurements(
    measurements: Iterable[MeridianMeasurement],
) -> dict[datetime, tuple[Decimal, Decimal]]:
    """Aggregate multiple registers into one row per UTC hour."""
    energy: defaultdict[datetime, Decimal] = defaultdict(lambda: Decimal(0))
    cost: defaultdict[datetime, Decimal] = defaultdict(lambda: Decimal(0))
    for measurement in measurements:
        start = measurement.start.astimezone(UTC)
        if start.minute or start.second or start.microsecond:
            raise ValueError("Meridian measurement is not aligned to an hour")
        energy[start] += measurement.value_kwh
        cost[start] += measurement.cost_cents
    return {start: (energy[start], cost[start]) for start in energy}


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
