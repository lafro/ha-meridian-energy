"""
Each account's other figures: billing period, bill days, balance and prices.

Each test runs the real coordinator, client and transport in front of a
synthetic Meridian service at the HTTP level (``meridian_http``), with the
statistics kept in memory. Every value is synthetic: an hour holds 0.5 kWh
and 15 cents, so a day of 24 hours 12 kWh and $3.60.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
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
    balance_data,
    billing_data,
    json_response,
    prices_data,
    rate,
)
from .syncs import DAY, HOUR, NOW, TODAY, coordinator_for, refresh, sync

_INTEGRATION = "custom_components.meridian_energy"
# The current period: 1 to 31 July, today the 16th.
_PERIOD = (TODAY - 15 * DAY, TODAY + 15 * DAY)
_FAILED = json_response({}, status=503)
_FIGURES = frozenset({"billingPeriods", "balance", "prices"})
_BILLING_AND_BILL_LOST = (
    "Meridian Energy couldn't get the billing period; the billing period and bill "
    "sensors show unknown until it can"
)
_BILLING_LOST = (
    "Meridian Energy couldn't get the billing period; the billing period sensors "
    "show unknown until it can"
)
_BILLING_BACK = "Meridian Energy is getting the billing period again"
_DAYS_ALL_LOST = (
    "Meridian Energy couldn't get the bill's day totals; the bill sensors show "
    "unknown until it can"
)
_DAYS_BACK = "Meridian Energy is getting the bill's day totals again"
_PRICES_LOST = (
    "Meridian Energy couldn't get the prices; Daily charge and Anytime price show "
    "unknown until it can"
)
_PRICES_UNANSWERED = "Meridian Energy couldn't get the prices; it asks again in a day"
_HUGE = "1E+1000000"


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


def _failing_days(_operation: str, variables: dict[str, Any]) -> Response | None:
    return _FAILED if variables.get("readingFrequencyType") == "DAY_INTERVAL" else None


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
    assert _own(caplog, logging.INFO) == [_BILLING_AND_BILL_LOST]
    # Asked again 6 hours later, not at every update.
    for update in range(13, 18):
        await sync(coordinator, NOW + update * HOUR, session)
    assert session.operations.count("billingPeriods") == 2

    session.hook = lambda _operation, _variables: None
    session.billing_period = (TODAY + DAY, TODAY + 31 * DAY)
    data = await sync(coordinator, NOW + 18 * HOUR, session)
    assert session.operations.count("billingPeriods") == 3
    assert data.account_results[0].billing_period is not None
    assert _own(caplog, logging.INFO)[1:] == [
        "Meridian Energy is getting the billing period again"
    ]


async def test_an_ended_period_still_answered_is_asked_again_within_6_hours(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """Meridian may answer the ended period for a while after midnight."""
    session.billing_period = (TODAY - 30 * DAY, TODAY)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)

    data = await sync(coordinator, NOW + 12 * HOUR, session)
    assert session.operations.count("billingPeriods") == 2
    assert data.account_results[0].billing_period is None
    assert data.account_results[0].bill == BillFigures()

    await sync(coordinator, NOW + 13 * HOUR, session)
    session.billing_period = (TODAY + DAY, TODAY + 31 * DAY)
    for update in range(14, 19):
        data = await sync(coordinator, NOW + update * HOUR, session)

    assert session.operations.count("billingPeriods") == 3
    (result,) = data.account_results
    assert result.billing_period is not None
    assert result.billing_period.start == TODAY + DAY
    # The new period's first date has not ended: nothing read so far.
    assert result.bill == BillFigures(
        usage=Decimal(0), cost=Decimal(0), estimate=Decimal("111.6")
    )


async def test_a_period_that_never_rolls_is_asked_for_4_times_a_day(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    session.billing_period = (TODAY - 30 * DAY, TODAY)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)

    for update in range(12, 36):
        await sync(coordinator, NOW + update * HOUR, session)

    assert session.operations.count("billingPeriods") == 1 + 4


async def test_an_ended_period_answered_after_a_failure_is_not_back(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The billing period is back when a new one shows, not when Meridian answers."""
    caplog.set_level(logging.INFO)
    session.billing_period = (TODAY - 30 * DAY, TODAY)
    coordinator = coordinator_for(hass, session)
    for update in range(12):
        await sync(coordinator, NOW + update * HOUR, session)

    session.hook = _failing("billingPeriods")
    await sync(coordinator, NOW + 12 * HOUR, session)
    assert _own(caplog, logging.INFO) == [_BILLING_AND_BILL_LOST]
    for update in range(13, 24):
        await sync(coordinator, NOW + update * HOUR, session)
    assert session.operations.count("billingPeriods") == 3

    # Meridian answers again, still with the ended period.
    session.hook = lambda _operation, _variables: None
    for update in range(24, 36):
        data = await sync(coordinator, NOW + update * HOUR, session)
        assert data.account_results[0].billing_period is None
        assert data.account_results[0].bill == BillFigures()
    assert session.operations.count("billingPeriods") == 5
    assert _own(caplog, logging.INFO) == [_BILLING_AND_BILL_LOST]

    session.billing_period = (TODAY + DAY, TODAY + 31 * DAY)
    data = await sync(coordinator, NOW + 36 * HOUR, session)
    assert data.account_results[0].billing_period is not None
    assert _own(caplog, logging.INFO) == [_BILLING_AND_BILL_LOST, _BILLING_BACK]


def _billing_asks(session: FakeMeridianSession, failing: range) -> list[int]:
    """Answer the billing period with a 503 in some hours from NOW; note each ask."""
    asked: list[int] = []

    def answer(operation: str, _variables: dict[str, Any]) -> Response | None:
        if operation != "billingPeriods":
            return None
        hours = round((session.kraken.now - NOW) / HOUR)
        asked.append(hours)
        return _FAILED if hours in failing else None

    session.hook = answer
    return asked


async def _first_shown(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    updates: int,
) -> tuple[int | None, list[tuple[int, str]]]:
    """
    Update hourly, the next period offered from 36 hours on.

    Return when it first shows, and each INFO line with the update it came at.
    """
    caplog.set_level(logging.INFO)
    new = (TODAY + 2 * DAY, TODAY + 32 * DAY)
    coordinator = coordinator_for(hass, session)
    shown = None
    said: list[tuple[int, str]] = []
    for update in range(updates):
        if update == 36:
            session.billing_period = new
        data = await sync(coordinator, NOW + update * HOUR, session)
        period = data.account_results[0].billing_period
        if shown is None and period is not None and period.start == new[0]:
            shown = update
        said.extend((update, line) for line in _own(caplog, logging.INFO)[len(said) :])
    return shown, said


async def test_a_failed_ask_before_a_periods_end_still_asks_after_it(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """At the first update after its end, then every 6 hours: 4 a day at most."""
    # The period ends tomorrow; New Zealand midnight after it is 35.5 hours on.
    session.billing_period = (TODAY + DAY - 30 * DAY, TODAY + DAY)
    asked = _billing_asks(session, range(24, 61))

    shown, said = await _first_shown(hass, session, caplog, updates=73)

    assert asked == [0, 24, 36, 42, 48, 54, 60, 66]
    assert shown == 66
    in_a_day = [
        len([hours for hours in asked if start <= hours < start + 24])
        for start in range(73)
    ]
    assert max(in_a_day) == 4
    assert said == [(36, _BILLING_AND_BILL_LOST), (66, _BILLING_BACK)]


async def test_an_ended_period_is_asked_for_every_6_hours_past_its_keep(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The last period got decides, however long ago it came."""
    session.billing_period = (TODAY + DAY - 30 * DAY, TODAY + DAY)
    asked = _billing_asks(session, range(36, 100))

    shown, said = await _first_shown(hass, session, caplog, updates=103)

    assert asked == [0, 24, *range(36, 103, 6)]
    assert shown == 102
    assert said == [(36, _BILLING_AND_BILL_LOST), (102, _BILLING_BACK)]


async def test_a_current_period_is_kept_two_days_when_its_refresh_fails(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    coordinator = coordinator_for(hass, session)
    data = await sync(coordinator, NOW, session)
    first = data.account_results[0].billing_period
    session.hook = _failing("billingPeriods")

    data = await sync(coordinator, NOW + DAY, session)
    assert data.account_results[0].billing_period == first
    assert not _own(caplog, logging.INFO)
    assert session.operations.count("billingPeriods") == 2
    await sync(coordinator, NOW + DAY + HOUR, session)
    assert session.operations.count("billingPeriods") == 2

    data = await sync(coordinator, NOW + 2 * DAY, session)
    (result,) = data.account_results
    assert result.billing_period is None
    # The day totals got a day ago are past their keep; the estimate is kept
    # until the period ends.
    assert result.bill == BillFigures(estimate=Decimal("111.6"))
    assert _own(caplog, logging.INFO) == [
        (
            "Meridian Energy couldn't get the billing period; the billing period "
            "sensors show unknown until it can"
        )
    ]

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, NOW + 3 * DAY, session)
    assert data.account_results[0].billing_period == first
    assert _own(caplog, logging.INFO)[1:] == [
        "Meridian Energy is getting the billing period again"
    ]


async def test_day_totals_failing_at_the_first_sync_leave_the_bill_unknown(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    session.hook = _failing_days
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].bill == BillFigures()
    assert _own(caplog, logging.INFO) == [_DAYS_ALL_LOST]


async def test_a_new_periods_failing_day_totals_say_so_at_once(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The old period's figures are withheld at once, so the line comes at once."""
    caplog.set_level(logging.INFO)
    session.billing_period = (TODAY - 30 * DAY, TODAY)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    # The old period's day totals, still kept 6 hours on.
    await sync(coordinator, NOW + 6 * HOUR, session)

    session.billing_period = (TODAY + DAY, TODAY + 31 * DAY)
    session.hook = _failing_days
    data = await sync(coordinator, NOW + 12 * HOUR, session)

    assert data.account_results[0].billing_period is not None
    assert data.account_results[0].bill == BillFigures()
    assert _own(caplog, logging.INFO) == [_DAYS_ALL_LOST]


async def test_a_lost_line_comes_again_when_more_shows_unknown(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The 'so far' figures go first; at the period's end, the estimate too."""
    caplog.set_level(logging.INFO)
    session.billing_period = (TODAY - 30 * DAY, TODAY + DAY)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    session.hook = _failing_days

    for update in (6, 12, 36):
        await sync(coordinator, NOW + update * HOUR, session)

    assert _own(caplog, logging.INFO) == [
        (
            "Meridian Energy couldn't get the bill's day totals; the 'so far' "
            "sensors show unknown until it can"
        ),
        _DAYS_ALL_LOST,
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
    """No saying yet whether the plan is all-day: the line says only when it asks."""
    caplog.set_level(logging.INFO)
    session.hook = _failing("prices")
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].price_sensors is None
    assert _own(caplog, logging.INFO) == [
        "Meridian Energy couldn't get the prices; it asks again in a day"
    ]
    await sync(coordinator, NOW + 23 * HOUR, session)
    assert session.operations.count("prices") == 1

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, NOW + DAY, session)
    assert session.operations.count("prices") == 2
    assert data.account_results[0].price_sensors is True
    assert _own(caplog, logging.INFO)[1:] == [
        "Meridian Energy is getting the prices again"
    ]


async def test_a_rate_limit_on_a_figure_fails_nothing_and_waits(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No more requests in that update; the next waits, and asks again."""
    caplog.set_level(logging.INFO)
    session.hook = _failing(
        "balance", Response(b"", status=429, headers={"Retry-After": "10800"})
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].total_due is None
    assert "prices" not in session.operations
    assert coordinator.update_interval == timedelta(hours=3)
    assert not _own(caplog, logging.INFO)
    assert coordinator.extras_diagnostics()["balance"]["failing"] == 0

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, NOW + 3 * HOUR, session)
    assert data.account_results[0].total_due == Decimal(40)
    assert coordinator.update_interval == UPDATE_INTERVAL
    assert not _own(caplog, logging.INFO)


async def test_a_kind_is_back_only_when_no_account_fails_it(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Another account still failing, though it shows its kept value, is not back."""
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, "A-TEST-2"))
    failing = {ACCOUNT_NUMBER}
    session.hook = lambda operation, variables: (
        _FAILED
        if operation == "balance" and variables["accountNumber"] in failing
        else None
    )
    coordinator = coordinator_for(hass, session)
    lost = (
        "Meridian Energy couldn't get the balance; Total due, Last bill and Last "
        "payment show unknown until it can"
    )

    data = await sync(coordinator, NOW, session)
    assert [result.total_due for result in data.account_results] == [
        None,
        Decimal(40),
    ]
    assert _own(caplog, logging.INFO) == [lost]

    failing = {"A-TEST-2"}
    data = await sync(coordinator, NOW + 6 * HOUR, session)
    assert [result.total_due for result in data.account_results] == [
        Decimal(40),
        Decimal(40),
    ]
    assert _own(caplog, logging.INFO) == [lost]

    failing = set()
    await sync(coordinator, NOW + 12 * HOUR, session)
    assert _own(caplog, logging.INFO) == [
        lost,
        "Meridian Energy is getting the balance again",
    ]


@pytest.mark.parametrize(
    "accounts", [(ACCOUNT_NUMBER, "A-TEST-2"), ("A-TEST-2", ACCOUNT_NUMBER)]
)
async def test_with_two_accounts_the_days_line_says_the_wider_loss(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture, accounts: tuple[str, str]
) -> None:
    """One account's bill unknown, the other's 'so far' only: one line, either way."""
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW, accounts=accounts)
    session.billing_period = _PERIOD
    outages = {ACCOUNT_NUMBER: range(41), "A-TEST-2": range(6, 30)}

    def answer(_operation: str, variables: dict[str, Any]) -> Response | None:
        if variables.get("readingFrequencyType") != "DAY_INTERVAL":
            return None
        hours = round((session.kraken.now - NOW) / HOUR)
        return _FAILED if hours in outages[variables["accountNumber"]] else None

    session.hook = answer
    coordinator = coordinator_for(hass, session)

    for update in range(42):
        await sync(coordinator, NOW + update * HOUR, session)
    assert _own(caplog, logging.INFO) == [_DAYS_ALL_LOST]
    await sync(coordinator, NOW + 42 * HOUR, session)

    assert _own(caplog, logging.INFO) == [_DAYS_ALL_LOST, _DAYS_BACK]


async def test_with_two_accounts_lost_prices_are_said_over_unanswered_ones(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, "A-TEST-2"))
    session.hook = lambda operation, variables: (
        _FAILED
        if operation == "prices"
        and (variables["accountNumber"] == "A-TEST-2" or session.kraken.now > NOW)
        else None
    )
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    await sync(coordinator, NOW + DAY, session)
    assert _own(caplog, logging.INFO) == [_PRICES_UNANSWERED]

    data = await sync(coordinator, NOW + 2 * DAY, session)

    assert [result.price_sensors for result in data.account_results] == [True, None]
    assert data.account_results[0].prices is None
    assert _own(caplog, logging.INFO) == [_PRICES_UNANSWERED, _PRICES_LOST]


async def test_with_two_accounts_a_narrower_billing_line_waits_for_the_wider(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Said once the account whose bill sensors are unknown too has its period."""
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, "A-TEST-2"))
    session.billing_period = _PERIOD
    failing = {ACCOUNT_NUMBER}
    session.hook = lambda operation, variables: (
        _FAILED
        if operation == "billingPeriods" and variables["accountNumber"] in failing
        else None
    )
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)

    failing.add("A-TEST-2")
    for day in (1, 2):
        await sync(coordinator, NOW + day * DAY, session)
    assert _own(caplog, logging.INFO) == [_BILLING_AND_BILL_LOST]

    failing.discard(ACCOUNT_NUMBER)
    for day in (3, 4):
        data = await sync(coordinator, NOW + day * DAY, session)

    assert [result.billing_period is None for result in data.account_results] == [
        False,
        True,
    ]
    assert _own(caplog, logging.INFO) == [_BILLING_AND_BILL_LOST, _BILLING_LOST]


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


def _billing_with(**changes: Any) -> Response:
    account = billing_data(_PERIOD)
    account["billingOptions"].update(changes)
    return json_response({"data": {"account": account}})


def _balance_with(connection: str, **changes: Any) -> Response:
    account = balance_data(TODAY)
    account[connection]["edges"][0]["node"].update(changes)
    return json_response({"data": {"account": account}})


def _prices_with(**changes: Any) -> Response:
    prices = prices_data()
    agreement = prices["meterPoints"][0]["activeAgreement"]
    if "rates" in changes:
        agreement["rates"] = changes["rates"]
    else:
        agreement["rates"][0].update(changes)
    return json_response({"data": {"account": {"property": prices}}})


@pytest.mark.parametrize(
    ("operations", "answer", "failure"),
    [
        (_FIGURES, _FAILED, None),
        (
            _FIGURES,
            json_response({"errors": [{"extensions": {"errorCode": "KT-CT-4321"}}]}),
            None,
        ),
        (_FIGURES, json_response({"data": {"account": {"balance": "lots"}}}), None),
        # A field of the wrong JSON type is as unreadable as a wrong value,
        # except a time-of-use bucket: any but none ties a rate to a time.
        ({"prices"}, _prices_with(rates=5), None),
        ({"prices"}, _prices_with(touBucketName=["x"]), None),
        ({"balance"}, _balance_with("bills", issuedDate={}), None),
        ({"balance"}, _balance_with("payments", paymentDate=[]), None),
        ({"billingPeriods"}, _billing_with(currentBillingPeriodEndDate=["x"]), None),
        (_FIGURES, json_response({}, status=401), ConfigEntryAuthFailed),
    ],
)
async def test_failed_figures_never_fail_an_update_but_an_ended_sign_in_does(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    operations: frozenset[str],
    answer: Response,
    failure: type[Exception] | None,
) -> None:
    """A failed figure shows unknown, and is asked for again only when due."""
    session.billing_period = _PERIOD
    session.hook = lambda operation, _variables: (
        answer if operation in operations else None
    )
    coordinator = coordinator_for(hass, session)

    if failure is not None:
        with pytest.raises(failure):
            await sync(coordinator, NOW, session)
        return
    await refresh(coordinator, NOW, session)

    assert coordinator.last_update_success
    (result,) = coordinator.data.account_results
    unknown = {
        "billingPeriods": result.billing_period is None,
        "balance": result.total_due is None,
        "prices": result.prices is None,
    }
    assert all(unknown[operation] for operation in operations)
    asked = [name for name in session.operations if name in operations]
    assert len(asked) == len(operations)
    await refresh(coordinator, NOW + HOUR, session)
    assert coordinator.last_update_success
    assert [name for name in session.operations if name in operations] == asked


@pytest.mark.parametrize("field", ["value", "cost"])
async def test_a_day_total_too_large_to_add_fails_only_the_bill(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    """A day's value or cost out of range is unreadable, like any other."""
    day_node = session.kraken.day_node

    def huge(day: date, channel: int, direction: str) -> dict[str, Any]:
        node = day_node(day, channel, direction)
        if field == "value":
            node["value"] = _HUGE
        else:
            node["metaData"]["statistics"][0]["costInclTax"] = {
                "estimatedAmount": _HUGE
            }
        return node

    monkeypatch.setattr(session.kraken, "day_node", huge)
    coordinator = coordinator_for(hass, session)

    await refresh(coordinator, NOW, session)

    assert coordinator.last_update_success
    assert coordinator.data.account_results[0].bill == BillFigures()
    await refresh(coordinator, NOW + HOUR, session)
    assert coordinator.last_update_success
    assert len(_day_requests(session)) == 1


async def test_a_property_whose_grid_import_failed_skips_its_day_request(
    hass: HomeAssistant,
) -> None:
    """The account's bill needs every property's days: none are asked for."""
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    session.hook = lambda operation, variables: (
        _FAILED
        if operation == "measurements"
        and variables["readingDirectionType"] == "CONSUMPTION"
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert _day_requests(session) == []
    (result,) = data.account_results
    assert result.bill == BillFigures()
    assert result.total_due == Decimal(40)


@pytest.mark.parametrize("failing", ["hours", "days"])
async def test_a_failing_grid_export_leaves_the_rest_of_the_bill(
    hass: HomeAssistant, failing: str
) -> None:
    """Its hours failing skip its day request; its day request may fail alone."""
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    asked: list[str] = []

    def answer(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation != "measurements":
            return None
        direction = variables["readingDirectionType"]
        by_day = variables["readingFrequencyType"] == "DAY_INTERVAL"
        if by_day:
            asked.append(direction)
        if direction == "GENERATION" and (failing == "days") == by_day:
            return json_response({}, status=400)
        return None

    session.hook = answer
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert asked == (
        ["CONSUMPTION"] if failing == "hours" else ["CONSUMPTION", "GENERATION"]
    )
    assert data.account_results[0].bill == BillFigures(
        usage=Decimal(180), cost=Decimal(54), estimate=Decimal("111.6")
    )


async def test_a_rate_limit_on_the_grid_export_days_waits_like_any_other(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Nothing fails and nothing is said; no more requests, and the next waits."""
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    session.hook = lambda operation, variables: (
        Response(b"", status=429, headers={"Retry-After": "10800"})
        if operation == "measurements"
        and variables["readingDirectionType"] == "GENERATION"
        and variables["readingFrequencyType"] == "DAY_INTERVAL"
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert coordinator.update_interval == timedelta(hours=3)
    assert "balance" not in session.operations
    assert data.account_results[0].bill == BillFigures()
    assert not _own(caplog, logging.INFO)


async def test_an_ended_sign_in_on_the_grid_export_days_stops_the_sync(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    session.hook = lambda operation, variables: (
        json_response({}, status=401)
        if operation == "measurements"
        and variables["readingDirectionType"] == "GENERATION"
        and variables["readingFrequencyType"] == "DAY_INTERVAL"
        else None
    )
    coordinator = coordinator_for(hass, session)

    with pytest.raises(ConfigEntryAuthFailed):
        await sync(coordinator, NOW, session)

    assert "balance" not in session.operations


async def test_only_a_property_with_feed_in_is_asked_for_its_export_days(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    account = account_data()
    second = {
        **account_data(feed_in=True)["properties"][0],
        "id": "synthetic-property-2",
    }
    account["properties"].append(second)
    session.billing_period = _PERIOD
    asked: list[tuple[str, str]] = []

    def answer(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation == "account":
            return json_response({"data": {"account": account}})
        if variables.get("readingFrequencyType") == "DAY_INTERVAL":
            asked.append((variables["propertyId"], variables["readingDirectionType"]))
        return None

    session.hook = answer
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert asked == [
        (account["properties"][0]["id"], "CONSUMPTION"),
        ("synthetic-property-2", "CONSUMPTION"),
        ("synthetic-property-2", "GENERATION"),
    ]
    bill = data.account_results[0].bill
    assert bill.usage == Decimal(360)
    assert bill.export is not None


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
