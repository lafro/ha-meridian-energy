"""
Each account's other figures: billing period, bill days, balance and prices.

Each test runs the real coordinator, client and transport in front of a
synthetic Meridian service at the HTTP level (``meridian_http``), with the
statistics kept in memory. Every value is synthetic: an hour holds 0.5 kWh
and 15 cents, so a day of 24 hours 12 kWh and $3.60.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed

from custom_components.meridian_energy.const import UPDATE_INTERVAL
from custom_components.meridian_energy.models import (
    BillFigures,
    MeridianBill,
    MeridianPayment,
    MeridianPrices,
)
from custom_components.meridian_energy.windows import local_midnight

from .meridian_http import (
    ACCOUNT_NUMBER,
    FakeMeridianSession,
    Response,
    account_data,
    json_response,
    rate,
)
from .syncs import DAY, HOUR, NOW, TODAY, coordinator_for, sync

_INTEGRATION = "custom_components.meridian_energy"
# The current period: 1 to 31 July, today the 16th.
_PERIOD = (TODAY - 15 * DAY, TODAY + 15 * DAY)
_FAILED = json_response({}, status=503)


@pytest.fixture
def session() -> FakeMeridianSession:
    """A synthetic Meridian with a current billing period."""
    meridian = FakeMeridianSession(NOW)
    meridian.billing_period = _PERIOD
    return meridian


def _own(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name.startswith(_INTEGRATION) and record.levelno == level
    ]


def _day_requests(session: FakeMeridianSession) -> list[dict[str, Any]]:
    return [item for item in session.kraken.requests if "frequency" in item]


def _failing(operation: str, answer: Response = _FAILED) -> Any:
    return lambda name, _variables: answer if name == operation else None


async def test_a_first_sync_gets_every_figure(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """The bill from Meridian's day totals; the balance, last bill and payment."""
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    (result,) = data.account_results
    assert result.billing_period is not None
    assert (result.billing_period.start, result.billing_period.end) == _PERIOD
    # 15 days read so far; the estimate adds the 16 to come, today's included.
    assert result.bill == BillFigures(
        usage=Decimal(180), cost=Decimal(54), estimate=Decimal("111.6")
    )
    assert result.total_due == Decimal(40)
    assert result.credit is None
    assert result.last_bill == MeridianBill(
        issued=TODAY - 12 * DAY, start=TODAY - 45 * DAY, end=TODAY - 14 * DAY
    )
    # The payment requested for a later date is not the last one.
    assert result.last_payment == MeridianPayment(
        amount_cents=5000, paid_on=TODAY - 10 * DAY, status="CLEARED"
    )
    assert result.price_sensors is True
    assert result.prices == MeridianPrices(
        plan="Synthetic Plan", daily_charge=Decimal(2), anytime=Decimal("0.3")
    )
    # One request for the period's days, paced and counted with the hours.
    assert _day_requests(session) == [
        {
            "startOn": _PERIOD[0].isoformat(),
            "endOn": _PERIOD[1].isoformat(),
            "last": 31,
            "direction": "CONSUMPTION",
            "frequency": "DAY_INTERVAL",
        }
    ]
    assert data.requests == 13 + 1


async def test_the_last_payment_is_the_newest_made_by_today(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """Meridian's order of payments is not by date: the newest by today is chosen."""

    def payment(cents: int, days_ago: int, status: str) -> dict[str, Any]:
        paid_on = (TODAY - days_ago * DAY).isoformat()
        return {"node": {"amount": cents, "paymentDate": paid_on, "status": status}}

    session.balance = {
        **session.balance,
        "payments": {
            "edges": [
                payment(4000, 40, "CLEARED"),
                payment(6000, -5, "REQUESTED"),
                payment(8000, 0, "CLEARED"),
                payment(7000, 70, "CLEARED"),
                payment(5000, 10, "CLEARED"),
            ]
        },
    }
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].last_payment == MeridianPayment(
        amount_cents=8000, paid_on=TODAY, status="CLEARED"
    )


async def test_a_meter_with_two_registers_sums_both_over_two_pages(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    session.kraken.channels = 2
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].bill == BillFigures(
        usage=Decimal(360), cost=Decimal(108), estimate=Decimal("223.2")
    )
    pages = _day_requests(session)
    assert [page["last"] for page in pages] == [31, 31]
    assert "before" in pages[1]


async def test_a_day_without_its_cost_leaves_the_bills_cost_unknown(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """The usage so far stays known; the cost and the estimate do not."""
    without_cost = local_midnight(TODAY - 3 * DAY)
    session.kraken.cost = lambda hour, _channel: (
        None if without_cost <= hour < without_cost + DAY else "15"
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].bill == BillFigures(usage=Decimal(180))


async def test_a_feed_in_register_adds_export_and_credit_so_far(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    # The export credit never enters the estimate.
    assert data.account_results[0].bill == BillFigures(
        usage=Decimal(180),
        cost=Decimal(54),
        export=Decimal(180),
        credit=Decimal(54),
        estimate=Decimal("111.6"),
    )
    assert [item["direction"] for item in _day_requests(session)] == [
        "CONSUMPTION",
        "GENERATION",
    ]


async def test_each_figure_is_asked_for_at_its_own_interval(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """In a day of hourly updates: the period and prices once, the rest 4 times."""
    coordinator = coordinator_for(hass, session)

    for update in range(24):
        await sync(coordinator, NOW + update * HOUR, session)

    assert session.operations.count("billingPeriods") == 1
    assert session.operations.count("prices") == 1
    assert session.operations.count("balance") == 4
    assert len(_day_requests(session)) == 4

    await sync(coordinator, NOW + DAY, session)
    assert session.operations.count("billingPeriods") == 2
    assert session.operations.count("prices") == 2


async def test_an_ended_period_is_asked_for_at_the_next_update(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """And the new period's days are asked for at once, not at their interval."""
    start = NOW - 2 * HOUR
    session.billing_period = (TODAY - 30 * DAY, TODAY)
    coordinator = coordinator_for(hass, session)
    for update in range(14):
        await sync(coordinator, start + update * HOUR, session)
    # Days at 0, 6 and 12 hours in; New Zealand midnight was 12.5 hours in.
    assert session.operations.count("billingPeriods") == 1
    assert len(_day_requests(session)) == 3

    session.billing_period = (TODAY + DAY, TODAY + 31 * DAY)
    data = await sync(coordinator, start + 14 * HOUR, session)

    assert session.operations.count("billingPeriods") == 2
    assert _day_requests(session)[-1]["startOn"] == (TODAY + DAY).isoformat()
    period = data.account_results[0].billing_period
    assert period is not None
    assert period.start == TODAY + DAY


async def test_an_ended_period_is_withheld_when_its_refresh_fails(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    session.billing_period = (TODAY - 30 * DAY, TODAY)
    coordinator = coordinator_for(hass, session)
    data = await sync(coordinator, NOW, session)
    assert data.account_results[0].bill.estimate is not None

    session.hook = _failing("billingPeriods")
    # The first update after New Zealand midnight asks again, and fails.
    data = await sync(coordinator, NOW + 12 * HOUR, session)

    (result,) = data.account_results
    assert result.billing_period is None
    assert result.bill == BillFigures()
    assert _own(caplog, logging.INFO) == [
        (
            "Meridian Energy couldn't get the billing period; the billing period "
            "sensors show unknown until it can"
        )
    ]
    # Asked again a day later, not at every update.
    await sync(coordinator, NOW + 13 * HOUR, session)
    assert session.operations.count("billingPeriods") == 2

    session.hook = lambda _operation, _variables: None
    session.billing_period = (TODAY + DAY, TODAY + 31 * DAY)
    data = await sync(coordinator, NOW + 36 * HOUR, session)
    assert data.account_results[0].billing_period is not None
    assert _own(caplog, logging.INFO)[1:] == [
        "Meridian Energy is getting the billing period again"
    ]


async def test_a_failed_figure_is_kept_for_two_intervals_then_shows_unknown(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Asked again after its interval; one INFO when lost, one when back."""
    caplog.set_level(logging.INFO)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)

    session.hook = lambda operation, variables: (
        _FAILED
        if operation == "balance"
        or variables.get("readingFrequencyType") == "DAY_INTERVAL"
        else None
    )
    for update in range(6, 12):
        data = await sync(coordinator, NOW + update * HOUR, session)
        (result,) = data.account_results
        assert result.total_due == Decimal(40)
        assert result.bill.usage == Decimal(180)
    assert session.operations.count("balance") == 2
    assert not _own(caplog, logging.INFO)

    data = await sync(coordinator, NOW + 12 * HOUR, session)

    (result,) = data.account_results
    assert (result.total_due, result.last_bill, result.last_payment) == (
        None,
        None,
        None,
    )
    # The estimate is kept until its period ends.
    assert result.bill == BillFigures(estimate=Decimal("111.6"))
    assert session.operations.count("balance") == 3
    assert _own(caplog, logging.INFO) == [
        (
            "Meridian Energy couldn't get the bill's day totals; the 'so far' sensors "
            "show unknown until it can"
        ),
        (
            "Meridian Energy couldn't get the balance; Total due, Last bill and Last "
            "payment show unknown until it can"
        ),
    ]

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, NOW + 18 * HOUR, session)
    assert data.account_results[0].total_due == Decimal(40)
    assert _own(caplog, logging.INFO)[2:] == [
        "Meridian Energy is getting the bill's day totals again",
        "Meridian Energy is getting the balance again",
    ]


async def test_prices_that_fail_show_unknown_after_two_days(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    session.hook = _failing("prices")

    data = await sync(coordinator, NOW + DAY, session)
    assert data.account_results[0].prices is not None
    data = await sync(coordinator, NOW + 2 * DAY, session)

    (result,) = data.account_results
    assert result.price_sensors is True
    assert result.prices is None
    assert _own(caplog, logging.INFO)[-1] == (
        "Meridian Energy couldn't get the prices; Daily charge and Anytime price show "
        "unknown until it can"
    )


async def test_prices_never_answered_leave_the_price_sensors_as_they_are(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No saying yet whether the plan is all-day: no INFO about sensors."""
    caplog.set_level(logging.INFO)
    session.hook = _failing("prices")
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].price_sensors is None
    assert not _own(caplog, logging.INFO)


async def test_a_rate_limit_on_a_figure_fails_nothing_and_waits(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """No more requests in that update; the next waits, and asks again."""
    session.hook = _failing(
        "balance", Response(b"", status=429, headers={"Retry-After": "10800"})
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].total_due is None
    assert "prices" not in session.operations
    assert coordinator.update_interval == timedelta(hours=3)

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, NOW + 3 * HOUR, session)
    assert data.account_results[0].total_due == Decimal(40)
    assert coordinator.update_interval == UPDATE_INTERVAL


async def test_no_figure_is_asked_for_after_a_measurements_rate_limit(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    calls: list[str] = []

    def hook(operation: str, _variables: dict[str, Any]) -> Response | None:
        calls.append(operation)
        if operation == "measurements" and calls.count("measurements") == 2:
            return Response(b"", status=429, headers={"Retry-After": "120"})
        return None

    session.hook = hook
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.results[0].latest_reading is not None
    assert not {"billingPeriods", "balance", "prices"} & set(calls)
    assert _day_requests(session) == []
    assert data.account_results[0].bill == BillFigures()


@pytest.mark.parametrize(
    ("answer", "failure"),
    [
        (_FAILED, None),
        (
            json_response({"errors": [{"extensions": {"errorCode": "KT-CT-4321"}}]}),
            None,
        ),
        (json_response({"data": {"account": {"balance": "lots"}}}), None),
        (json_response({}, status=401), ConfigEntryAuthFailed),
    ],
)
async def test_failed_figures_never_fail_an_update_but_an_ended_sign_in_does(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    answer: Response,
    failure: type[Exception] | None,
) -> None:
    session.hook = lambda operation, _variables: (
        answer if operation in {"billingPeriods", "balance", "prices"} else None
    )
    coordinator = coordinator_for(hass, session)

    if failure is not None:
        with pytest.raises(failure):
            await sync(coordinator, NOW, session)
        return
    data = await sync(coordinator, NOW, session)
    assert data.account_results[0].total_due is None


async def test_a_property_whose_recent_dates_failed_skips_its_day_request(
    hass: HomeAssistant,
) -> None:
    """The account's bill needs every property's days: none are asked for."""
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    session.hook = lambda operation, variables: (
        _FAILED
        if operation == "measurements"
        and variables["readingDirectionType"] == "GENERATION"
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert _day_requests(session) == []
    (result,) = data.account_results
    assert result.bill == BillFigures()
    assert result.total_due == Decimal(40)


def _account_with(**changes: Any) -> Response:
    account = account_data()
    account["properties"][0].update(changes)
    return json_response({"data": {"account": account}})


@pytest.mark.parametrize(
    ("account", "prices"),
    [
        # A time-of-use plan: the price sensors are not offered.
        (
            None,
            {
                "meterPoints": [
                    {
                        "activeAgreement": {
                            "displayName": "Synthetic Plan",
                            "rates": [
                                rate("STANDING_CHARGE", "$2.000000 /day"),
                                rate("CONSUMPTION_CHARGE", "$0.300000 /kWh"),
                                rate(
                                    "CONSUMPTION_CHARGE",
                                    "$0.200000 /kWh",
                                    bucket="Night",
                                ),
                            ],
                        }
                    }
                ]
            },
        ),
        # Two meter points: not asked for at all.
        (
            _account_with(
                meterPoints=[
                    {"id": f"synthetic-meter-{n}", "marketIdentifier": f"SYN-{n}"}
                    for n in range(2)
                ]
            ),
            None,
        ),
    ],
)
async def test_price_sensors_only_for_an_all_day_plan_on_one_meter(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    account: Response | None,
    prices: dict[str, Any] | None,
) -> None:
    if account is not None:
        session.hook = lambda operation, _variables: (
            account if operation == "account" else None
        )
    if prices is not None:
        session.prices = prices
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    (result,) = data.account_results
    assert result.price_sensors is False
    assert result.prices is None
    assert session.operations.count("prices") == (0 if account is not None else 1)


async def test_two_properties_get_no_price_sensors_and_one_bill(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """Each property's days add up to the account's bill."""
    account = account_data()
    second = {**account["properties"][0], "id": "synthetic-property-2"}
    account["properties"].append(second)
    session.hook = lambda operation, _variables: (
        json_response({"data": {"account": account}})
        if operation == "account"
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    (result,) = data.account_results
    assert result.price_sensors is False
    assert "prices" not in session.operations
    assert result.bill == BillFigures(
        usage=Decimal(360), cost=Decimal(108), estimate=Decimal("223.2")
    )


@pytest.mark.parametrize(("cents", "credit"), [(1250, Decimal("12.5")), (0, None)])
async def test_a_credit_shows_nothing_due_and_the_credit(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    cents: int,
    credit: Decimal | None,
) -> None:
    """A balance of nothing is no credit."""
    session.balance = {**session.balance, "balance": cents}
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    (result,) = data.account_results
    assert result.total_due == Decimal(0)
    assert result.credit == credit


async def test_a_period_without_dates_asks_for_no_days(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    session.billing_period = None
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert _day_requests(session) == []
    assert data.account_results[0].bill == BillFigures()
    assert data.account_results[0].total_due == Decimal(40)


async def test_figures_of_an_account_no_longer_listed_are_forgotten(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, "A-TEST-2"))
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    assert {number for _kind, number in coordinator._extras} == {
        ACCOUNT_NUMBER,
        "A-TEST-2",
    }

    session.accounts = (ACCOUNT_NUMBER,)
    await sync(coordinator, NOW + DAY, session)

    assert {number for _kind, number in coordinator._extras} == {ACCOUNT_NUMBER}
