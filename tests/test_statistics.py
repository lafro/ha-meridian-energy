"""Tests for the statistics helpers: identifiers."""

from __future__ import annotations

from datetime import UTC, datetime

from custom_components.meridian_energy.statistics import (
    _statistic_kind,
    _timestamp,
    consumption_ids,
    generation_ids,
    property_key,
    statistic_id,
)


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


def test_statistic_kind_leaves_out_the_key() -> None:
    assert _statistic_kind(consumption_ids("abc123")[1]) == "consumption_cost"
    assert _statistic_kind(generation_ids("abc123")[0]) == "generation"
