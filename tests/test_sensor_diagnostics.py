"""Tests for safe diagnostic entities and downloads."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from homeassistant.components.sensor import SensorDeviceClass, SensorStateClass
from homeassistant.const import EntityCategory, UnitOfEnergy
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.meridian_energy.api import MeridianUpstreamError
from custom_components.meridian_energy.const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    DOMAIN,
)
from custom_components.meridian_energy.coordinator import (
    MeridianDataCoordinator,
    _Extra,
    _Memory,
)
from custom_components.meridian_energy.diagnostics import (
    async_get_config_entry_diagnostics,
)
from custom_components.meridian_energy.models import (
    AccountSyncResult,
    BillFigures,
    MeridianAccount,
    MeridianBill,
    MeridianBillingPeriod,
    MeridianMeterPoint,
    MeridianPayment,
    MeridianPrices,
    MeridianProperty,
    MeridianSyncData,
    PropertySyncResult,
)
from custom_components.meridian_energy.sensor import (
    DESCRIPTIONS,
    _remove_stale_devices,
    _remove_stale_entities,
    async_setup_entry,
)
from custom_components.meridian_energy.statistics import account_key, property_key

ACCOUNT_NUMBER = "synthetic-account"
ACCOUNT_KEY = account_key(ACCOUNT_NUMBER)
_ALWAYS = (
    "last_sync",
    "latest_meter_data",
    "estimated_readings",
    "bill_usage",
    "bill_cost",
    "bill_estimate",
    "total_due",
    "last_bill",
    "last_payment",
    "billing_period_start",
    "billing_period_end",
    "next_billing_date",
)
_FEED_IN = ("bill_export", "bill_credit")
_PRICES = ("daily_charge", "anytime_price")


def test_sensor_descriptions_follow_home_assistant_semantics() -> None:
    """Lock the audited entity contract to Home Assistant sensor semantics."""
    descriptions = {description.key: description for description in DESCRIPTIONS}

    assert set(descriptions) == {*_ALWAYS, *_FEED_IN, *_PRICES}

    for key in ("last_sync", "latest_meter_data"):
        description = descriptions[key]
        assert description.device_class is SensorDeviceClass.TIMESTAMP
        assert description.entity_category is EntityCategory.DIAGNOSTIC

    provisional = descriptions["estimated_readings"]
    assert provisional.device_class is None
    assert provisional.native_unit_of_measurement is None
    assert provisional.state_class is SensorStateClass.MEASUREMENT
    assert provisional.suggested_display_precision == 0
    assert provisional.entity_category is EntityCategory.DIAGNOSTIC
    # No other sensor has a state class: none is a statistic.
    assert [key for key, item in descriptions.items() if item.state_class] == [
        "estimated_readings"
    ]

    for key in ("bill_usage", "bill_export"):
        description = descriptions[key]
        assert description.device_class is SensorDeviceClass.ENERGY
        assert description.native_unit_of_measurement is UnitOfEnergy.KILO_WATT_HOUR
        assert description.suggested_display_precision == 1

    for key in ("bill_cost", "bill_credit", "bill_estimate", "total_due"):
        description = descriptions[key]
        assert description.device_class is SensorDeviceClass.MONETARY
        assert description.native_unit_of_measurement == "NZD"
        assert description.suggested_display_precision == 2
    assert descriptions["last_payment"].device_class is SensorDeviceClass.MONETARY
    assert descriptions["last_bill"].device_class is SensorDeviceClass.DATE

    # Home Assistant's monetary class takes a currency alone.
    assert descriptions["daily_charge"].device_class is None
    assert descriptions["daily_charge"].native_unit_of_measurement == "NZD/day"
    assert descriptions["anytime_price"].device_class is None
    assert descriptions["anytime_price"].native_unit_of_measurement == "NZD/kWh"

    for key in ("billing_period_start", "billing_period_end", "next_billing_date"):
        description = descriptions[key]
        assert description.device_class is SensorDeviceClass.DATE
        assert description.entity_category is EntityCategory.DIAGNOSTIC
        assert description.entity_registry_enabled_default is False

    assert {key: item.condition for key, item in descriptions.items()} == {
        **dict.fromkeys(_ALWAYS),
        **dict.fromkeys(_FEED_IN, "feed_in"),
        **dict.fromkeys(_PRICES, "prices"),
    }


def _account() -> MeridianAccount:
    return MeridianAccount(
        number=ACCOUNT_NUMBER,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id="synthetic-property",
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint(
                        id="synthetic-meter",
                        market_identifier="synthetic-icp",
                        has_feed_in=False,
                    ),
                ),
            ),
        ),
    )


def _result(**changes: object) -> AccountSyncResult:
    result = AccountSyncResult(
        account_key=ACCOUNT_KEY,
        billing_period=MeridianBillingPeriod(
            period_length="MONTHLY",
            period_length_multiplier=1,
            is_fixed=True,
            start=date(2026, 7, 1),
            end=date(2026, 7, 31),
            next_billing_date=date(2026, 8, 1),
            period_start_day=1,
        ),
        has_feed_in=False,
        bill=BillFigures(
            usage=Decimal("123.4"),
            cost=Decimal("45.67"),
            estimate=Decimal("98.76"),
        ),
        total_due=Decimal("54.32"),
        last_bill=MeridianBill(
            issued=date(2026, 7, 3), start=date(2026, 6, 1), end=date(2026, 6, 30)
        ),
        last_payment=MeridianPayment(
            amount_cents=8765, paid_on=date(2026, 7, 10), status="CLEARED"
        ),
        price_sensors=True,
        prices=MeridianPrices(
            plan="Synthetic Plan",
            daily_charge=Decimal("2.11"),
            anytime=Decimal("0.3141"),
        ),
    )
    return replace(result, **changes)


def _data(*results: AccountSyncResult) -> MeridianSyncData:
    reading = datetime.now(UTC) - timedelta(days=1)
    return MeridianSyncData(
        account_count=1,
        property_count=1,
        results=(
            PropertySyncResult(
                property_key="hashed-key",
                account_key=ACCOUNT_KEY,
                latest_reading=reading,
                estimated_rows=2,
                oldest_estimated=reading - timedelta(hours=1),
                newest_estimated=reading,
                quality_counts=(("ACTUAL", 22), ("ESTIMATE", 2)),
            ),
        ),
        account_results=results or (_result(),),
        synced_at=datetime.now(UTC),
        sync_duration_seconds=12.3456,
        requests=5,
    )


def _entry(coordinator) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        entry_id="entry",
        title="private@example.com",
        data={
            "email": "private@example.com",
            CONF_REFRESH_TOKEN: "private-refresh",
            CONF_FIREBASE_USER_ID: "private-user",
        },
        version=1,
        minor_version=1,
    )
    entry.runtime_data = SimpleNamespace(coordinator=coordinator, client=MagicMock())
    return entry


def _by_key(entities: list) -> dict:
    return {entity.entity_description.key: entity for entity in entities}


@pytest.mark.asyncio
async def test_sensor_values_and_device_identifier_are_redacted(hass) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._topology = (_account(),)
    coordinator.data = _data()
    entry = _entry(coordinator)
    entities = []

    await async_setup_entry(hass, entry, entities.extend)

    sensors = _by_key(entities)
    assert set(sensors) == {*_ALWAYS, *_PRICES}
    assert {key: sensor.native_value for key, sensor in sensors.items()} == {
        "last_sync": coordinator.data.synced_at,
        "latest_meter_data": coordinator.data.results[0].latest_reading,
        "estimated_readings": 2,
        "bill_usage": Decimal("123.4"),
        "bill_cost": Decimal("45.67"),
        "bill_estimate": Decimal("98.76"),
        "total_due": Decimal("54.32"),
        "last_bill": date(2026, 7, 3),
        "last_payment": Decimal("87.65"),
        "daily_charge": Decimal("2.11"),
        "anytime_price": Decimal("0.3141"),
        "billing_period_start": date(2026, 7, 1),
        "billing_period_end": date(2026, 7, 31),
        "next_billing_date": date(2026, 8, 1),
    }
    assert {
        key: sensor.extra_state_attributes
        for key, sensor in sensors.items()
        if sensor.extra_state_attributes is not None
    } == {
        "estimated_readings": {
            "oldest_provisional_interval": coordinator.data.results[0].oldest_estimated,
            "newest_provisional_interval": coordinator.data.results[0].newest_estimated,
            "upstream_quality_counts": {"ACTUAL": 22, "ESTIMATE": 2},
        },
        "total_due": {"credit": None},
        "last_bill": {
            "period_start": date(2026, 6, 1),
            "period_end": date(2026, 6, 30),
        },
        "last_payment": {"date": date(2026, 7, 10), "status": "CLEARED"},
        "daily_charge": {"plan": "Synthetic Plan"},
        "anytime_price": {"plan": "Synthetic Plan"},
    }
    assert all(sensor.last_reset is None for sensor in entities)

    # A credit: nothing due, the credit as an attribute; and nothing known.
    coordinator.data = _data(
        _result(
            total_due=Decimal(0),
            credit=Decimal("12.5"),
            billing_period=None,
            bill=BillFigures(),
            last_bill=None,
            last_payment=None,
            prices=None,
        )
    )
    assert sensors["total_due"].native_value == Decimal(0)
    credit = sensors["total_due"].extra_state_attributes
    assert credit == {"credit": 12.5}
    assert isinstance(credit["credit"], float)
    for key in (*_PRICES, "bill_usage", "bill_estimate", "last_bill", "last_payment"):
        assert sensors[key].native_value is None, key
    assert sensors["billing_period_start"].native_value is None
    assert sensors["last_bill"].extra_state_attributes == {
        "period_start": None,
        "period_end": None,
    }
    assert sensors["last_payment"].extra_state_attributes == {
        "date": None,
        "status": None,
    }
    assert sensors["daily_charge"].extra_state_attributes == {"plan": None}

    identifiers = entities[0].device_info["identifiers"]
    assert "private-user" not in str(identifiers)
    assert entities[0].device_info["name"] == "Meridian Energy"
    assert entities[0].device_info["model"] == "MyMeridian"
    assert all(not hasattr(entity, "_attr_name") for entity in entities)
    await entry._async_process_on_unload(hass)


@pytest.mark.asyncio
async def test_every_device_is_named_meridian_energy(hass) -> None:
    """No address or count in a device name, whatever the accounts hold."""
    account = _account()
    solar_meter = replace(account.properties[0].meter_points[0], has_feed_in=True)
    account = replace(
        account,
        properties=(
            replace(account.properties[0], meter_points=(solar_meter,)),
            replace(account.properties[0], id="second-property"),
        ),
    )
    second = replace(_account(), number="second-account", properties=())
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._topology = (account, second)
    coordinator.data = _data(
        _result(
            has_feed_in=True,
            bill=BillFigures(export=Decimal(10), credit=Decimal(2)),
            price_sensors=False,
            prices=None,
        ),
        _result(account_key=account_key("second-account")),
    )
    entities = []
    entry = _entry(coordinator)

    await async_setup_entry(hass, entry, entities.extend)

    assert len(entities) == 2 * len(_ALWAYS) + len(_FEED_IN) + len(_PRICES)
    first = _by_key(entities[: len(_ALWAYS) + len(_FEED_IN)])
    assert first["bill_export"].native_value == Decimal(10)
    assert first["bill_credit"].native_value == Decimal(2)
    assert {entity.device_info["name"] for entity in entities} == {"Meridian Energy"}
    await entry._async_process_on_unload(hass)


def test_stale_entity_cleanup_skips_entities_owned_by_another_entry(hass) -> None:
    """Never remove a registry entity belonging to a different config entry."""
    entry = _entry(MagicMock())
    entity_registry = MagicMock()
    entity_registry.async_get_entity_id.side_effect = [
        "sensor.shared",
        "sensor.owned",
    ]
    entity_registry.async_get.side_effect = [
        SimpleNamespace(config_entry_id="other-entry"),
        SimpleNamespace(config_entry_id=entry.entry_id),
    ]

    with patch(
        "custom_components.meridian_energy.sensor.er.async_get",
        return_value=entity_registry,
    ):
        _remove_stale_entities(
            hass,
            entry,
            {(ACCOUNT_KEY, "shared"), (ACCOUNT_KEY, "owned")},
        )

    entity_registry.async_remove.assert_called_once_with("sensor.owned")


def _removed(entity_registry: MagicMock) -> set[str]:
    return {call.args[0] for call in entity_registry.async_remove.call_args_list}


@pytest.fixture
def entity_registry() -> MagicMock:
    """A registry that holds every sensor of this entry, to see what is removed."""
    registry = MagicMock()
    registry.async_get_entity_id.side_effect = lambda _domain, _platform, unique: (
        f"sensor.{unique.removeprefix(f'{ACCOUNT_KEY}_')}"
    )
    registry.async_get.return_value = SimpleNamespace(config_entry_id="entry")
    return registry


@pytest.mark.asyncio
async def test_conditional_entities_follow_the_account(hass, entity_registry) -> None:
    """Feed-in and price sensors come and go with the account; unknown keeps them."""
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._topology = (_account(),)
    coordinator.data = _data()
    entry = _entry(coordinator)
    entities = []

    with patch(
        "custom_components.meridian_energy.sensor.er.async_get",
        return_value=entity_registry,
    ):
        await async_setup_entry(hass, entry, entities.extend)
        assert set(_by_key(entities)) == {*_ALWAYS, *_PRICES}
        # Feed-in sensors that are not wanted are removed even after a reload.
        assert _removed(entity_registry) == {"sensor.bill_export", "sensor.bill_credit"}
        entity_registry.async_remove.reset_mock()

        feed_in = _result(has_feed_in=True, bill=BillFigures(export=Decimal(2)))
        coordinator.async_set_updated_data(_data(feed_in))
        assert len(entities) == len(_ALWAYS) + len(_PRICES) + len(_FEED_IN)
        assert entities[-2].native_value == Decimal(2)

        # Not yet known, as when the prices were never answered: kept.
        coordinator.async_set_updated_data(_data(replace(feed_in, price_sensors=None)))
        assert not entity_registry.async_remove.called

        # A plan that is not all-day, or a second property: removed.
        coordinator.async_set_updated_data(
            _data(replace(feed_in, has_feed_in=False, price_sensors=False))
        )
        assert _removed(entity_registry) == {
            "sensor.bill_export",
            "sensor.bill_credit",
            "sensor.daily_charge",
            "sensor.anytime_price",
        }

        coordinator.async_set_updated_data(_data(feed_in))
        assert len(entities) == len(_ALWAYS) + 2 * (len(_PRICES) + len(_FEED_IN))

    coordinator._topology = ()
    coordinator.async_set_updated_data(
        replace(coordinator.data, results=(), account_results=())
    )
    await entry._async_process_on_unload(hass)


@pytest.mark.asyncio
async def test_entities_tolerate_transient_missing_account_result(
    hass, caplog, entity_registry
) -> None:
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._topology = (_account(),)
    original_data = _data()
    coordinator.data = original_data
    coordinator.last_update_success = True
    entry = _entry(coordinator)
    entities = []

    with patch(
        "custom_components.meridian_energy.sensor.er.async_get",
        return_value=entity_registry,
    ):
        await async_setup_entry(hass, entry, entities.extend)
        entity_registry.async_remove.reset_mock()
        caplog.set_level(logging.ERROR)

        coordinator.async_set_updated_data(replace(original_data, account_results=()))

        assert not entity_registry.async_remove.called
    sensors = _by_key(entities)
    assert "Unexpected error updating listener" not in caplog.text
    assert sensors["bill_usage"].available is False
    assert sensors["bill_usage"].native_value is None
    assert sensors["bill_usage"].extra_state_attributes is None
    assert sensors["daily_charge"].native_value is None

    coordinator.async_set_updated_data(original_data)
    assert sensors["bill_usage"].available is True
    assert sensors["bill_usage"].native_value == Decimal("123.4")
    await entry._async_process_on_unload(hass)


def test_stale_device_cleanup_removes_only_owned_stale_entities(hass) -> None:
    entry = _entry(MagicMock())
    unrelated = SimpleNamespace(id="unrelated", identifiers={("other", "id")})
    active = SimpleNamespace(id="active", identifiers={(DOMAIN, ACCOUNT_KEY)})
    stale = SimpleNamespace(id="stale", identifiers={(DOMAIN, "stale-key")})
    owned = SimpleNamespace(entity_id="sensor.owned", config_entry_id=entry.entry_id)
    shared = SimpleNamespace(entity_id="sensor.shared", config_entry_id="other")
    device_registry = MagicMock()
    entity_registry = MagicMock()

    with (
        patch(
            "custom_components.meridian_energy.sensor.dr.async_get",
            return_value=device_registry,
        ),
        patch(
            "custom_components.meridian_energy.sensor.dr.async_entries_for_config_entry",
            return_value=[unrelated, active, stale],
        ),
        patch(
            "custom_components.meridian_energy.sensor.er.async_get",
            return_value=entity_registry,
        ),
        patch(
            "custom_components.meridian_energy.sensor.er.async_entries_for_device",
            return_value=[owned, shared],
        ),
    ):
        _remove_stale_devices(hass, entry, {ACCOUNT_KEY})

    entity_registry.async_remove.assert_called_once_with("sensor.owned")
    device_registry.async_update_device.assert_called_once_with(
        "stale", remove_config_entry_id=entry.entry_id
    )


_EXTRAS = {
    "billing": {"failing": 0, "oldest_age_seconds": 3600.125},
    "days": {"failing": 1, "oldest_age_seconds": None},
    "balance": {"failing": 0, "oldest_age_seconds": None},
    "prices": {"failing": 0, "oldest_age_seconds": None},
}


def _coordinator_mock(data: MeridianSyncData | None) -> MagicMock:
    coordinator = MagicMock()
    coordinator.data = data
    coordinator.accounts = (_account(),)
    coordinator.last_update_success = True
    coordinator.last_exception = None
    coordinator.extras_diagnostics.return_value = _EXTRAS
    coordinator.unit_diagnostics = list
    coordinator.client.skipped_accounts = 1
    coordinator.rate_limits_met = 2
    return coordinator


@pytest.mark.asyncio
async def test_diagnostics_exclude_all_sensitive_fields(hass) -> None:
    """No identifier, email, token or amount: only whether each figure is known."""
    coordinator = _coordinator_mock(
        _data(_result(credit=Decimal("12.5"), bill=BillFigures(usage=Decimal(1))))
    )
    entry = _entry(coordinator)

    diagnostics = await async_get_config_entry_diagnostics(hass, entry)
    serialized = str(diagnostics)

    assert diagnostics["coordinator"]["account_count"] == 1
    assert diagnostics["coordinator"]["last_failure"] is None
    assert diagnostics["coordinator"]["sync_duration_seconds"] == 12.346
    assert diagnostics["coordinator"]["requests"] == 5
    assert diagnostics["coordinator"]["extras"] == _EXTRAS
    assert diagnostics["coordinator"]["account_results"][0]["known"] == {
        "bill_usage": True,
        "bill_cost": False,
        "bill_export": False,
        "bill_credit": False,
        "bill_estimate": False,
        "total_due": True,
        "last_bill": True,
        "last_payment": True,
        "daily_charge": True,
        "anytime_price": True,
    }
    for text in (
        "private@example.com",
        "private-refresh",
        "private-user",
        "hashed-key",
        ACCOUNT_KEY,
        "Synthetic Plan",
        "CLEARED",
        "Decimal",
        "54.32",
        "12.5",
        "8765",
        "2.11",
        "0.3141",
        "2026, 7, 3",
        "2026, 7, 10",
    ):
        assert text not in serialized, text


@pytest.mark.asyncio
async def test_diagnostics_name_the_last_failure_by_key_and_cause(hass) -> None:
    """The failure's category and its cause's class; never its text."""
    coordinator = _coordinator_mock(_data())
    failure = UpdateFailed(
        translation_domain=DOMAIN,
        translation_key="cannot_connect",
        translation_placeholders={"detail": "HTTP 504"},
    )
    failure.__cause__ = MeridianUpstreamError(504)
    coordinator.last_update_success = False
    coordinator.last_exception = failure

    diagnostics = await async_get_config_entry_diagnostics(hass, _entry(coordinator))

    assert diagnostics["coordinator"]["last_failure"] == {
        "key": "cannot_connect",
        "cause": "MeridianUpstreamError",
    }


def test_extras_diagnostics_count_failures_and_the_oldest_age(hass, freezer) -> None:
    """Empty, then each kind's failures and oldest age; a future time reads 0."""
    freezer.move_to("2026-07-15T01:00:00+00:00")
    now = datetime(2026, 7, 15, 1, tzinfo=UTC)
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    assert coordinator.extras_diagnostics() == {
        kind: {"failing": 0, "oldest_age_seconds": None}
        for kind in ("billing", "days", "balance", "prices")
    }

    coordinator._extras = {
        ("billing", "one"): _Extra(at=now - timedelta(hours=2), failing=True),
        ("billing", "two"): _Extra(at=now - timedelta(minutes=30)),
        ("days", "one"): _Extra(failing=True),
        ("balance", "one"): _Extra(at=now + timedelta(minutes=5)),
    }

    assert coordinator.extras_diagnostics() == {
        "billing": {"failing": 1, "oldest_age_seconds": 7200.0},
        "days": {"failing": 1, "oldest_age_seconds": None},
        "balance": {"failing": 0, "oldest_age_seconds": 0.0},
        "prices": {"failing": 0, "oldest_age_seconds": None},
    }


def _units(hass) -> MeridianDataCoordinator:
    """
    Return a coordinator whose grid export failed and whose import is whole.

    Its grid import's history reaches back to its target; its grid export
    failed with a timeout, and an older window of it with a gateway timeout.
    """
    solar_meter = replace(_account().properties[0].meter_points[0], has_feed_in=True)
    account = replace(
        _account(),
        properties=(replace(_account().properties[0], meter_points=(solar_meter,)),),
    )
    units = MeridianDataCoordinator(hass, MagicMock())
    units._topology = (account,)
    key = property_key(account.number, account.properties[0].id)
    units._memory[(key, "CONSUMPTION")] = _Memory(
        complete=True,
        newest=datetime(2026, 7, 14, 11, tzinfo=UTC),
        oldest=datetime(2026, 4, 15, 12, tzinfo=UTC),
    )
    units._memory[(key, "GENERATION")] = _Memory(
        failure="cannot_connect", fill_failure="cannot_connect (HTTP 504)"
    )
    return units


@pytest.mark.asyncio
async def test_diagnostics_snapshot(hass, snapshot, freezer) -> None:
    """The full diagnostics payload, so any new field is reviewed for privacy."""
    freezer.move_to("2026-07-15T01:00:00+00:00")
    coordinator = _coordinator_mock(_data())
    coordinator.unit_diagnostics = _units(hass).unit_diagnostics
    entry = _entry(coordinator)

    diagnostics = await async_get_config_entry_diagnostics(hass, entry)

    assert diagnostics == snapshot
    assert [unit["last_failure"] for unit in diagnostics["coordinator"]["units"]] == [
        None,
        "cannot_connect",
    ]
    serialized = str(diagnostics)
    assert ACCOUNT_NUMBER not in serialized
    assert "1 Synthetic Street" not in serialized


@pytest.mark.asyncio
async def test_an_account_with_no_property_has_no_estimates_to_count(hass) -> None:
    """Unknown, not 0: there is no property whose hours could be estimates."""
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._topology = (replace(_account(), properties=()),)
    coordinator.data = replace(_data(), results=())
    entry = _entry(coordinator)
    entities = []

    await async_setup_entry(hass, entry, entities.extend)

    estimated = _by_key(entities)["estimated_readings"]
    assert estimated.native_value is None
    await entry._async_process_on_unload(hass)


@pytest.mark.asyncio
async def test_entities_before_first_sync_come_from_topology(hass) -> None:
    """Before the first sync: the feed-in sensors from the accounts, no prices."""
    account = _account()
    solar_meter = replace(account.properties[0].meter_points[0], has_feed_in=True)
    account = replace(
        account,
        properties=(replace(account.properties[0], meter_points=(solar_meter,)),),
    )
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._topology = (account,)
    assert coordinator.data is None
    entities = []
    entry = _entry(coordinator)

    await async_setup_entry(hass, entry, entities.extend)

    assert set(_by_key(entities)) == {*_ALWAYS, *_FEED_IN}
    for entity in entities:
        assert entity.available is False
        assert entity.native_value is None
        assert entity.last_reset is None
        assert entity.extra_state_attributes is None

    coordinator.async_set_updated_data(_data(_result(has_feed_in=True)))
    assert all(entity.available for entity in entities)
    assert set(_by_key(entities)) == {*_ALWAYS, *_FEED_IN, *_PRICES}
    await entry._async_process_on_unload(hass)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_feed_in", [False, True])
async def test_registered_sensors_before_first_sync(
    hass, entity_registry, has_feed_in: bool
) -> None:
    """Price entries stay in the registry until Meridian has answered."""
    account = _account()
    meter = replace(account.properties[0].meter_points[0], has_feed_in=has_feed_in)
    account = replace(
        account, properties=(replace(account.properties[0], meter_points=(meter,)),)
    )
    coordinator = MeridianDataCoordinator(hass, MagicMock())
    coordinator._topology = (account,)
    assert coordinator.data is None
    entry = _entry(coordinator)
    entities = []

    with patch(
        "custom_components.meridian_energy.sensor.er.async_get",
        return_value=entity_registry,
    ):
        await async_setup_entry(hass, entry, entities.extend)

    feed_in = _FEED_IN if has_feed_in else ()
    assert set(_by_key(entities)) == {*_ALWAYS, *feed_in, *_PRICES}
    assert _removed(entity_registry) == (
        set() if has_feed_in else {"sensor.bill_export", "sensor.bill_credit"}
    )
    await entry._async_process_on_unload(hass)


@pytest.mark.asyncio
async def test_diagnostics_before_first_sync(hass, freezer) -> None:
    """Before a first sync, each unit shows its target and nothing stored."""
    freezer.move_to("2026-07-15T01:00:00+00:00")
    coordinator = _coordinator_mock(None)
    units = MeridianDataCoordinator(hass, MagicMock())
    units._topology = (_account(),)
    coordinator.unit_diagnostics = units.unit_diagnostics
    entry = _entry(coordinator)

    diagnostics = await async_get_config_entry_diagnostics(hass, entry)

    assert diagnostics["coordinator"] == {
        "last_update_success": True,
        "last_failure": None,
        "first_sync_pending": True,
        "account_count": 1,
        "skipped_account_count": 1,
        "rate_limits": 2,
        "extras": _EXTRAS,
        "units": [
            {
                "account": "1/1",
                "property": "1/1",
                "direction": "consumption",
                "newest_stored_hour": None,
                "oldest_stored_hour": None,
                # New Zealand midnight at the start of 16 April 2026.
                "target": datetime(2026, 4, 15, 12, tzinfo=UTC),
                "history_complete": False,
                "fill_stopped": False,
                "fill_failure": None,
                "quiet": False,
                "last_failure": None,
            }
        ],
    }
    serialized = str(diagnostics)
    assert ACCOUNT_NUMBER not in serialized
    assert "private@example.com" not in serialized
