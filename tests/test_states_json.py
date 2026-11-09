"""
The states the integration writes, through Home Assistant's real set-up.

Each test signs in through Home Assistant's real flow, then runs the first
sync with the real recorder, client and transport in front of a synthetic
Meridian service at the HTTP level. Every state must be JSON: one Home
Assistant can't serialise drops out of the frontend and the recorder, and its
WARNING names the value. A sensor that stays registered must be written by
the integration, not left as the unavailable state Home Assistant restores.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.history import get_last_state_changes
from homeassistant.config_entries import SOURCE_USER
from homeassistant.const import CONF_EMAIL
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

import custom_components.meridian_energy as integration_module
from custom_components.meridian_energy import config_flow as config_flow_module
from custom_components.meridian_energy.const import DOMAIN

from .meridian_http import ACCOUNT_NUMBER, EMAIL, FakeMeridianSession, json_response

_NOW = datetime(2026, 7, 16, 12, 30, tzinfo=UTC)
_TODAY = (_NOW + timedelta(hours=12)).date()
_TOTAL_DUE = "sensor.meridian_energy_total_due"
_PRICE_SENSORS = (
    "sensor.meridian_energy_daily_charge",
    "sensor.meridian_energy_anytime_price",
)


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(
    recorder_mock: object, enable_custom_integrations: None
) -> None:
    """Set up the recorder before enabling this recorder-dependent integration."""


@pytest.fixture
def meridian() -> Iterator[FakeMeridianSession]:
    session = FakeMeridianSession(_NOW)
    session.billing_period = (_TODAY - timedelta(days=15), _TODAY + timedelta(days=15))
    with (
        patch.object(
            config_flow_module, "async_get_clientsession", return_value=session
        ),
        patch.object(
            integration_module, "async_get_clientsession", return_value=session
        ),
        patch(
            "custom_components.meridian_energy.coordinator._utcnow", return_value=_NOW
        ),
    ):
        yield session


async def _set_up(hass: HomeAssistant, caplog: pytest.LogCaptureFixture) -> str:
    """Sign in and run the first sync; return the entry's id."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    # Loading the integration warns that it is a custom one: not this test's.
    caplog.clear()
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: EMAIL}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"otp": "123456"}
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    entry = result["result"]
    await hass.async_block_till_done(wait_background_tasks=True)
    await async_wait_recording_done(hass)
    assert entry.runtime_data.coordinator.last_update_success
    return str(entry.entry_id)


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        f"{record.name}: {record.getMessage()}"
        for record in caplog.records
        if record.levelno >= logging.WARNING
    ]


async def test_a_credit_is_shown_and_recorded(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """In credit, Total due is 0 and its credit a number, in every state written."""
    caplog.set_level(logging.WARNING)
    meridian.balance = {**meridian.balance, "balance": 1250}

    await _set_up(hass, caplog)

    state = hass.states.get(_TOTAL_DUE)
    assert state is not None
    assert state.state == "0"
    assert state.attributes["credit"] == 12.5
    assert state.as_dict_json
    assert state.as_compressed_state_json
    recorded = await get_instance(hass).async_add_executor_job(
        get_last_state_changes, hass, 1, _TOTAL_DUE
    )
    assert recorded[_TOTAL_DUE][0].attributes["credit"] == 12.5
    assert _warnings(caplog) == []


@pytest.mark.parametrize("variant", ["owed", "credit", "feed_in"])
async def test_every_state_of_the_entry_is_json(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    variant: str,
) -> None:
    caplog.set_level(logging.WARNING)
    if variant == "credit":
        meridian.balance = {**meridian.balance, "balance": 1250}
    if variant == "feed_in":
        meridian.feed_in = frozenset({ACCOUNT_NUMBER})

    entry_id = await _set_up(hass, caplog)

    entities = er.async_entries_for_config_entry(er.async_get(hass), entry_id)
    states: list[Any] = [hass.states.get(item.entity_id) for item in entities]
    shown = [state for state in states if state is not None]
    # The three billing dates are disabled at first, so have no state.
    assert len(shown) == (16 if variant == "feed_in" else 14) - 3
    for state in shown:
        assert state.as_dict_json, state.entity_id
        assert state.as_compressed_state_json, state.entity_id
    assert _warnings(caplog) == []


async def test_a_fresh_install_with_no_plan_in_force_names_no_price_sensor(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No price sensor is made, and the one line about it names none."""
    caplog.set_level(logging.INFO)
    meridian.prices = {"meterPoints": [{"activeAgreement": None}]}

    entry_id = await _set_up(hass, caplog)

    registry = er.async_get(hass)
    assert not [
        item.entity_id
        for item in er.async_entries_for_config_entry(registry, entry_id)
        if item.unique_id.endswith(("_daily_charge", "_anytime_price"))
    ]
    for entity_id in _PRICE_SENSORS:
        assert hass.states.get(entity_id) is None
    lines = [
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("custom_components.meridian_energy")
        and "plan" in record.getMessage()
    ]
    assert lines == [
        (
            "Meridian lists no plan in force for the meter; Meridian Energy asks "
            "again later"
        )
    ]


async def test_price_sensors_stay_unknown_when_prices_fail_after_a_reload(
    hass: HomeAssistant,
    meridian: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Not yet asked again, so no saying whether they apply: they show unknown."""
    caplog.set_level(logging.WARNING)
    entry_id = await _set_up(hass, caplog)
    for entity_id in _PRICE_SENSORS:
        state = hass.states.get(entity_id)
        assert state is not None
        assert state.state not in {"unknown", "unavailable"}, entity_id

    meridian.hook = lambda operation, _variables: (
        json_response({}, status=503) if operation == "prices" else None
    )
    assert await hass.config_entries.async_reload(entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)

    entry = hass.config_entries.async_get_entry(entry_id)
    assert entry is not None
    assert entry.runtime_data.coordinator.last_update_success
    for entity_id in _PRICE_SENSORS:
        state = hass.states.get(entity_id)
        assert state is not None
        assert state.state == "unknown", entity_id
        assert "restored" not in state.attributes, entity_id
    assert _warnings(caplog) == []
