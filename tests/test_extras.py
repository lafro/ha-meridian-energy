"""
Each account's other figures: billing period, bill days, balance and prices.

Each test runs the real coordinator, client and transport in front of a
synthetic Meridian service at the HTTP level (``meridian_http``), with the
statistics kept in memory. Every value is synthetic: an hour holds 0.5 kWh
and 15 cents, so a day of 24 hours 12 kWh and $3.60.
"""

from __future__ import annotations

import copy
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed

from custom_components.meridian_energy.const import (
    BILL_DAYS_INTERVAL,
    UPDATE_INTERVAL,
)
from custom_components.meridian_energy.models import (
    BillFigures,
    MeridianBill,
    MeridianPayment,
    MeridianPrices,
)
from custom_components.meridian_energy.windows import local_midnight

from .kraken_fake import FakeKraken
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
_FAILED = json_response({}, status=500)
_FIGURES = frozenset({"billingPeriods", "balance", "prices"})
_BILLING_AND_BILL_LOST = (
    "Meridian Energy couldn't get the billing period; the billing period and bill "
    "sensors show unknown until it can"
)
_BILLING_LOST = (
    "Meridian Energy couldn't get the billing period; the billing period sensors "
    "show unknown until it can"
)
_BILLING_SO_FAR_LOST = (
    "Meridian Energy couldn't get the billing period; the billing period and "
    "'so far' sensors show unknown until it can"
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
_PRICES_UNANSWERED = "Meridian Energy couldn't get the prices; it asks again later"
_PRICES_BACK = "Meridian Energy is getting the prices again"
_NO_PLAN = (
    "Meridian lists no plan in force for the meter; Meridian Energy asks again later"
)
_DAYS_SKIPPED = (
    "Meridian Energy can't read one of the properties Meridian lists on an "
    "account; that account's bill sensors show unknown until it can"
)
_ESTIMATE_LOST = (
    "Meridian's day totals leave out a date of the billing period; Bill estimate "
    "shows unknown until they don't"
)
_ESTIMATE_BACK = "Meridian's day totals cover the billing period again"
_RATE_LIMITED = (
    "Meridian asked Meridian Energy to wait (HTTP 429, no code, with "
    "Retry-After); it waits before its next update"
)
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
    """Whatever order Meridian lists payments in, the newest by today is chosen."""

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


@pytest.mark.parametrize("status", ["FAILED", "PROMISE_BROKEN", "DELETED", "PROMISED"])
async def test_a_payment_not_made_is_never_the_last_payment(
    hass: HomeAssistant, session: FakeMeridianSession, status: str
) -> None:
    """
    A failed payment is not one made, though newer than the last that cleared.

    Meridian's app shows a FAILED one as a failed auto-pay; the others are
    left out to be safe.
    """

    def payment(cents: int, days_ago: int, status: str) -> dict[str, Any]:
        paid_on = (TODAY - days_ago * DAY).isoformat()
        return {"node": {"amount": cents, "paymentDate": paid_on, "status": status}}

    session.balance = {
        **session.balance,
        "payments": {
            "edges": [
                payment(9000, 2, status),
                payment(5000, 10, "CLEARED"),
                payment(3000, 20, status),
            ]
        },
    }
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].last_payment == MeridianPayment(
        amount_cents=5000, paid_on=TODAY - 10 * DAY, status="CLEARED"
    )


@pytest.mark.parametrize(
    "status",
    [
        "SCHEDULED",
        "PENDING",
        "CLEARED",
        "FULFILLED",
        "HISTORIC",
        "THIRD_PARTY",
        "REQUESTED",
    ],
)
async def test_a_payment_made_by_today_is_the_last_payment_whatever_its_status(
    hass: HomeAssistant, session: FakeMeridianSession, status: str
) -> None:
    """Every status Meridian sends but those not made counts, REQUESTED as before."""
    session.balance = {
        **session.balance,
        "payments": {
            "edges": [
                {
                    "node": {
                        "amount": 9000,
                        "paymentDate": (TODAY - 2 * DAY).isoformat(),
                        "status": status,
                    }
                },
                {
                    "node": {
                        "amount": 5000,
                        "paymentDate": (TODAY - 10 * DAY).isoformat(),
                        "status": "CLEARED",
                    }
                },
            ]
        },
    }
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].last_payment == MeridianPayment(
        amount_cents=9000, paid_on=TODAY - 2 * DAY, status=status
    )


@pytest.mark.parametrize(
    "status",
    [None, "missing", "", "REVERSED"],
    ids=["null", "missing", "empty", "not one Meridian sends"],
)
async def test_a_payment_of_unknown_status_is_never_the_last_payment(
    hass: HomeAssistant, session: FakeMeridianSession, status: str | None
) -> None:
    """A status missing or not known says nothing of a payment made: it is left out."""
    newer: dict[str, Any] = {
        "amount": 9000,
        "paymentDate": (TODAY - 2 * DAY).isoformat(),
    }
    if status != "missing":
        newer["status"] = status
    cleared = {
        "amount": 5000,
        "paymentDate": (TODAY - 10 * DAY).isoformat(),
        "status": "CLEARED",
    }
    session.balance = {
        **session.balance,
        "payments": {"edges": [{"node": newer}, {"node": cleared}]},
    }
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].last_payment == MeridianPayment(
        amount_cents=5000, paid_on=TODAY - 10 * DAY, status="CLEARED"
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
    """Answer the billing period with a 500 in some hours from NOW; note each ask."""
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
    # until the period ends, and the line says the 'so far' sensors are unknown.
    assert result.bill == BillFigures(estimate=Decimal("111.6"))
    assert _own(caplog, logging.INFO) == [_BILLING_SO_FAR_LOST]

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, NOW + 3 * DAY, session)
    assert data.account_results[0].billing_period == first
    assert _own(caplog, logging.INFO)[1:] == [
        "Meridian Energy is getting the billing period again"
    ]


async def test_the_lost_billing_line_widens_when_the_so_far_figures_go(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Hourly, with the billing period failing from the first hour on.

    The period is kept two days, then shows unknown; the day totals, no longer
    asked for, are kept until six hours later, when the 'so far' figures show
    unknown too and the line says so. Once Meridian answers again, at 72
    hours, one line says the period is back.
    """
    caplog.set_level(logging.INFO)
    _billing_asks(session, range(1, 72))
    coordinator = coordinator_for(hass, session)
    said: list[tuple[int, str]] = []
    shown: list[int] = []
    for update in range(76):
        data = await sync(coordinator, NOW + update * HOUR, session)
        (result,) = data.account_results
        if result.billing_period is not None:
            shown.append(update)
        said.extend((update, line) for line in _own(caplog, logging.INFO)[len(said) :])

    assert shown == [*range(48), *range(72, 76)]
    assert said == [
        (48, _BILLING_LOST),
        (54, _BILLING_SO_FAR_LOST),
        (72, _BILLING_BACK),
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


@pytest.mark.parametrize("status", [500, 503])
async def test_prices_never_answered_leave_the_price_sensors_as_they_are(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    status: int,
) -> None:
    """
    No saying yet whether the plan is all-day; the line says it asks again.

    When is the failure's to say: one that may pass (503) is asked again at
    the next update, any other a day later.
    """
    caplog.set_level(logging.INFO)
    session.hook = _failing("prices", json_response({}, status=status))
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].price_sensors is None
    assert _own(caplog, logging.INFO) == [_PRICES_UNANSWERED]
    sent = session.operations.count("prices")
    if status == 503:
        answered_at = NOW + HOUR
    else:
        await sync(coordinator, NOW + 23 * HOUR, session)
        assert session.operations.count("prices") == sent
        answered_at = NOW + DAY

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, answered_at, session)
    assert session.operations.count("prices") == sent + 1
    assert data.account_results[0].price_sensors is True
    assert _own(caplog, logging.INFO) == [_PRICES_UNANSWERED, _PRICES_BACK]
    assert [line for line in _own(caplog, logging.INFO) if "in a day" in line] == []


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
    # One line for the limit, which names no figure; nothing failed.
    assert _own(caplog, logging.INFO) == [_RATE_LIMITED]
    assert coordinator.extras_diagnostics()["balance"]["failing"] == 0

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, NOW + 3 * HOUR, session)
    assert data.account_results[0].total_due == Decimal(40)
    assert coordinator.update_interval == UPDATE_INTERVAL
    assert _own(caplog, logging.INFO) == [_RATE_LIMITED]


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
    # The other's day totals are past their keep: its 'so far' figures are
    # unknown too.
    assert _own(caplog, logging.INFO) == [_BILLING_AND_BILL_LOST, _BILLING_SO_FAR_LOST]


_TWO_ORDERS = [(ACCOUNT_NUMBER, "A-TEST-2"), ("A-TEST-2", ACCOUNT_NUMBER)]


async def _two_billing_accounts(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
    accounts: tuple[str, str],
    *,
    periods: Any,
    failing: Any,
    updates: list[int],
) -> tuple[list[tuple[int, str]], int | None]:
    """
    Update at these hours, each account's period from ``periods(number, hours)``.

    Return each INFO line with the update it came at, and the first update
    from 36 hours on at which ACCOUNT_NUMBER shows a period.
    """
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW, accounts=accounts)

    def answer(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation != "billingPeriods":
            return None
        hours = round((session.kraken.now - NOW) / HOUR)
        number = variables["accountNumber"]
        if failing(number, hours):
            return _FAILED
        period = billing_data(periods(number, hours))
        return json_response({"data": {"account": period}})

    session.hook = answer
    coordinator = coordinator_for(hass, session)
    said: list[tuple[int, str]] = []
    shown = None
    for update in updates:
        data = await sync(coordinator, NOW + update * HOUR, session)
        said.extend((update, line) for line in _own(caplog, logging.INFO)[len(said) :])
        own = data.account_results[accounts.index(ACCOUNT_NUMBER)].billing_period
        if shown is None and update >= 36 and own is not None:
            shown = update
    return said, shown


@pytest.mark.parametrize("accounts", _TWO_ORDERS)
async def test_an_ended_period_never_failed_joins_no_other_accounts_loss(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture, accounts: tuple[str, str]
) -> None:
    """One account's period ended and is answered as it is; the other's fails."""
    ended = (TODAY - 30 * DAY, TODAY)

    said, _shown = await _two_billing_accounts(
        hass,
        caplog,
        accounts,
        periods=lambda number, _hours: _PERIOD if number == ACCOUNT_NUMBER else ended,
        failing=lambda number, hours: (
            number == ACCOUNT_NUMBER and (1 <= hours <= 70 or 73 <= hours <= 150)
        ),
        # Every ask and keep's end falls on a 6-hour grid; hourly around 48 and 72.
        updates=[
            *range(0, 46, 6),
            *range(46, 51),
            *range(54, 72, 6),
            *range(72, 75),
            *range(78, 171, 6),
        ],
    )

    # Kept two days after it was got: lost at 48 and 120 hours, the 'so far'
    # figures 6 hours later, back when asked.
    assert said == [
        (48, _BILLING_LOST),
        (54, _BILLING_SO_FAR_LOST),
        (72, _BILLING_BACK),
        (120, _BILLING_LOST),
        (126, _BILLING_SO_FAR_LOST),
        (168, _BILLING_BACK),
    ]


@pytest.mark.parametrize("accounts", _TWO_ORDERS)
async def test_a_period_back_after_a_failure_waits_for_no_other_account(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture, accounts: tuple[str, str]
) -> None:
    """The other account's ended period, answered as it is, rolls only later."""

    def periods(number: str, hours: int) -> tuple[date, date]:
        if number == ACCOUNT_NUMBER:
            if hours >= 36:
                return (TODAY + 2 * DAY, TODAY + 32 * DAY)
            return (TODAY + DAY - 30 * DAY, TODAY + DAY)
        if hours >= 80:
            return (TODAY + DAY, TODAY + 31 * DAY)
        return (TODAY - 30 * DAY, TODAY)

    said, shown = await _two_billing_accounts(
        hass,
        caplog,
        accounts,
        periods=periods,
        failing=lambda number, hours: number == ACCOUNT_NUMBER and 24 <= hours <= 45,
        # Every ask falls on a 6-hour grid; hourly from the end to the back line.
        updates=[*range(0, 36, 6), *range(36, 51), *range(54, 91, 6)],
    )

    assert shown == 48
    assert said == [(36, _BILLING_AND_BILL_LOST), (48, _BILLING_BACK)]


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
        # A refused request is not sent again, nor ends the sign-in.
        (_FIGURES, json_response({}, status=403), None),
        (
            _FIGURES,
            json_response(
                {"errors": [{"extensions": {"errorCode": "KT-CT-1124"}}]}, status=401
            ),
            ConfigEntryAuthFailed,
        ),
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


_TOO_LARGE_DOLLARS = "1" + "0" * 1_000_000
_PAST_A_FLOAT = "9" * 400


@pytest.mark.parametrize(
    ("daily", "anytime"),
    [
        pytest.param(f"${_TOO_LARGE_DOLLARS}", "$0.300000", id="daily"),
        pytest.param("$2.000000", f"${_PAST_A_FLOAT}", id="anytime"),
    ],
)
async def test_a_price_too_large_shows_unknown_and_nothing_fails(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    daily: str,
    anytime: str,
) -> None:
    """Only that price is unknown; its number is never logged."""
    caplog.set_level(logging.DEBUG)
    session.prices = prices_data(daily, anytime)
    coordinator = coordinator_for(hass, session)

    for update in (0, 1):
        await refresh(coordinator, NOW + update * HOUR, session)
        assert coordinator.last_update_success

    prices = coordinator.data.account_results[0].prices
    assert prices is not None
    assert (prices.daily_charge, prices.anytime) == (
        (None, Decimal("0.3")) if daily != "$2.000000" else (Decimal(2), None)
    )
    assert session.operations.count("prices") == 1
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert "0" * 13 not in caplog.text
    assert "9" * 13 not in caplog.text


@pytest.mark.parametrize("field", ["balance", "payment"])
async def test_a_balance_or_payment_too_large_shows_unknown_and_nothing_fails(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    field: str,
) -> None:
    """Past what a sensor could show: the balance's figures are unknown, no more."""
    caplog.set_level(logging.DEBUG)
    too_large = 10**400
    if field == "balance":
        session.balance = {**session.balance, "balance": too_large}
    else:
        session.balance["payments"]["edges"][0]["node"]["amount"] = too_large
    coordinator = coordinator_for(hass, session)

    for update in (0, 1):
        await refresh(coordinator, NOW + update * HOUR, session)
        assert coordinator.last_update_success

    (result,) = coordinator.data.account_results
    assert (result.total_due, result.last_bill, result.last_payment) == (
        None,
        None,
        None,
    )
    assert result.prices is not None
    assert session.operations.count("balance") == 1
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert "0" * 13 not in caplog.text


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


async def test_a_grid_export_day_failure_that_may_pass_is_asked_again_next(
    hass: HomeAssistant,
) -> None:
    """
    As the bill's other figures are: at the next update, not in 6 hours.

    Until then, only the export and credit so far show unknown.
    """
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    # Sent, then resent once, as a 503 is.
    failures = iter([json_response({}, status=503)] * 2)
    session.hook = lambda operation, variables: (
        next(failures, None)
        if operation == "measurements"
        and variables["readingDirectionType"] == "GENERATION"
        and variables["readingFrequencyType"] == "DAY_INTERVAL"
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)
    bill = data.account_results[0].bill
    assert (bill.usage, bill.export, bill.credit) == (Decimal(180), None, None)

    data = await sync(coordinator, NOW + HOUR, session)
    bill = data.account_results[0].bill
    assert (bill.export, bill.credit) == (Decimal(180), Decimal(54))


def _failing_export_hours(answer: Response, count: int) -> Any:
    """Fail the grid export's hours so many times, then answer them."""
    failures = iter([answer] * count)
    return lambda operation, variables: (
        next(failures, None)
        if operation == "measurements"
        and variables["readingDirectionType"] == "GENERATION"
        and variables["readingFrequencyType"] != "DAY_INTERVAL"
        else None
    )


async def test_a_grid_export_unit_failure_that_may_pass_asks_for_its_days_next(
    hass: HomeAssistant,
) -> None:
    """
    The export's days wait for its hours; a failure that may pass, one update.

    Until then, only the export and credit so far show unknown.
    """
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    # Sent, then resent once, as a 503 is.
    session.hook = _failing_export_hours(json_response({}, status=503), 2)
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)
    bill = data.account_results[0].bill
    assert (bill.usage, bill.export, bill.credit) == (Decimal(180), None, None)

    data = await sync(coordinator, NOW + HOUR, session)
    bill = data.account_results[0].bill
    assert (bill.export, bill.credit) == (Decimal(180), Decimal(54))


async def test_a_grid_export_unit_refused_waits_for_its_days_interval(
    hass: HomeAssistant,
) -> None:
    """A refusal won't pass by itself: its days are asked for when next due."""
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    session.hook = _failing_export_hours(json_response({}, status=400), 1)
    coordinator = coordinator_for(hass, session)

    for update in range(2):
        data = await sync(coordinator, NOW + update * HOUR, session)
        bill = data.account_results[0].bill
        assert (bill.usage, bill.export, bill.credit) == (Decimal(180), None, None)

    data = await sync(coordinator, NOW + BILL_DAYS_INTERVAL, session)
    assert data.account_results[0].bill.export is not None


async def test_a_rate_limit_on_the_grid_export_days_waits_like_any_other(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Nothing fails and only the limit is noted; no more requests, the next waits."""
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
    assert _own(caplog, logging.INFO) == [_RATE_LIMITED]


async def test_an_ended_sign_in_on_the_grid_export_days_stops_the_sync(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    session.hook = lambda operation, variables: (
        json_response(
            {"errors": [{"extensions": {"errorCode": "KT-CT-1124"}}]}, status=401
        )
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


_UNREADABLE_ANYTIME = (
    "Meridian Energy can't read the anytime price in formattedRateIncludingTax, "
    "which looks like '$N.NN /AAA' (N a digit, A a letter); Anytime price shows "
    "unknown"
)
_UNREADABLE_DISCOUNT = (
    "Meridian Energy can't read the daily charge in "
    "formattedRateIncludingTaxIncludingDiscount, which looks like '$N.NN AAA' "
    "(N a digit, A a letter); Daily charge shows unknown"
)


async def test_an_unreadable_price_is_warned_once_by_its_shape(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Only that price is unknown; one WARNING names its field and its shape.

    Each price's field is warned of once for the entry, however often it
    is asked for or read in between; its text is never logged.
    """
    caplog.set_level(logging.DEBUG)
    session.prices = prices_data(anytime="$0.30 /kWh")
    coordinator = coordinator_for(hass, session)

    for day in range(3):
        data = await sync(coordinator, NOW + day * DAY, session)
        prices = data.account_results[0].prices
        assert prices is not None
        assert (prices.daily_charge, prices.anytime) == (Decimal(2), None)
    session.prices = prices_data()
    await sync(coordinator, NOW + 3 * DAY, session)
    session.prices = prices_data(anytime="$0.30 /kWh")
    await sync(coordinator, NOW + 4 * DAY, session)

    assert session.operations.count("prices") == 5
    assert _own(caplog, logging.WARNING) == [_UNREADABLE_ANYTIME]
    assert "$0.30" not in caplog.text

    # A discounted price is read from its own field, warned of apart.
    session.prices = _plan_of(
        rate("STANDING_CHARGE", "$2.000000", discount="$1.80 off"),
        rate("CONSUMPTION_CHARGE", "$0.300000"),
    )
    data = await sync(coordinator, NOW + 5 * DAY, session)
    prices = data.account_results[0].prices
    assert prices is not None
    assert (prices.daily_charge, prices.anytime) == (None, Decimal("0.3"))
    assert _own(caplog, logging.WARNING) == [_UNREADABLE_ANYTIME, _UNREADABLE_DISCOUNT]
    assert "$1.80" not in caplog.text


@pytest.mark.parametrize(
    ("text", "words"),
    [(None, "is missing"), ("", "is empty"), (2, "is not text")],
    ids=["null", "empty", "a number"],
)
async def test_a_price_with_no_text_to_mask_is_warned_of_in_words(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    text: object,
    words: str,
) -> None:
    """With nothing to mask, the WARNING says why, never as if it were a shape."""
    caplog.set_level(logging.WARNING)
    standing = rate("STANDING_CHARGE", "$2.000000")
    standing["formattedRateIncludingTax"] = text
    session.prices = _plan_of(standing, rate("CONSUMPTION_CHARGE", "$0.300000"))

    await sync(coordinator_for(hass, session), NOW, session)

    assert _own(caplog, logging.WARNING) == [
        (
            "Meridian Energy can't read the daily charge in formattedRateIncludingTax, "
            f"which {words}; Daily charge shows unknown"
        )
    ]
    assert "looks like" not in caplog.text


async def test_a_discount_flag_not_true_or_false_is_warned_of_in_words(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    standing = rate("STANDING_CHARGE", "$2.000000")
    standing["hasDiscount"] = None
    session.prices = _plan_of(standing, rate("CONSUMPTION_CHARGE", "$0.300000"))

    data = await sync(coordinator_for(hass, session), NOW, session)

    prices = data.account_results[0].prices
    assert prices is not None
    assert (prices.daily_charge, prices.anytime) == (None, Decimal("0.3"))
    assert _own(caplog, logging.WARNING) == [
        (
            "Meridian Energy can't read the daily charge in hasDiscount, which is "
            "not true or false; Daily charge shows unknown"
        )
    ]


def _plan_of(*rates: dict[str, Any]) -> dict[str, Any]:
    plan = prices_data()
    plan["meterPoints"][0]["activeAgreement"]["rates"] = list(rates)
    return plan


# A time-of-use plan: no price sensors.
_TIME_OF_USE = _plan_of(
    rate("STANDING_CHARGE", "$2.000000"),
    rate("CONSUMPTION_CHARGE", "$0.300000"),
    rate("CONSUMPTION_CHARGE", "$0.200000", bucket="Night"),
)


def _account_with(**changes: Any) -> Response:
    account = account_data()
    account["properties"][0].update(changes)
    return json_response({"data": {"account": account}})


@pytest.mark.parametrize(
    ("account", "prices"),
    [
        # A time-of-use plan: the price sensors are not offered.
        (None, _TIME_OF_USE),
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


def _two_properties(*, second_id: str | None) -> dict[str, Any]:
    """Return the account with a second property, whose ID may be missing."""
    account = copy.deepcopy(account_data())
    second = copy.deepcopy(account["properties"][0])
    second["id"] = second_id
    account["properties"].append(second)
    return account


async def test_a_property_that_cannot_be_read_at_a_refresh_keeps_list_and_bill(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """
    The list Meridian answered before is kept, both properties in it.

    So the bill still adds up both, and no price sensor comes and goes.
    """
    answer = {"account": _two_properties(second_id="synthetic-property-2")}
    session.hook = lambda operation, _variables: (
        json_response({"data": answer}) if operation == "account" else None
    )
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)

    answer["account"] = _two_properties(second_id=None)
    data = await sync(coordinator, NOW + DAY, session)

    (result,) = data.account_results
    assert data.property_count == 2
    assert result.price_sensors is False
    assert "prices" not in session.operations
    # 16 dates read now, both properties.
    assert result.bill == BillFigures(
        usage=Decimal(384), cost=Decimal("115.2"), estimate=Decimal("223.2")
    )


async def test_a_property_that_cannot_be_read_at_a_first_sync_leaves_the_bill_unknown(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    With no list before it, the one property read is followed.

    The bill and prices would leave the other out, so neither is asked for,
    and the list is asked for again at the next update. One line says the
    bill shows unknown, and one when it is back.
    """
    caplog.set_level(logging.INFO)
    answer = {"account": _two_properties(second_id=None)}
    session.hook = lambda operation, _variables: (
        json_response({"data": answer}) if operation == "account" else None
    )
    coordinator = coordinator_for(hass, session)

    for update in range(2):
        data = await sync(coordinator, NOW + update * HOUR, session)
        (result,) = data.account_results
        assert data.property_count == 1
        assert result.bill == BillFigures()
        assert result.price_sensors is None
    assert _day_requests(session) == []
    assert "prices" not in session.operations
    # Said once, and the bill's own line, not the billing period's.
    assert [line for line in _own(caplog, logging.INFO) if "bill" in line] == [
        _DAYS_SKIPPED
    ]

    answer["account"] = _two_properties(second_id="synthetic-property-2")
    data = await sync(coordinator, NOW + 2 * HOUR, session)
    assert session.operations.count("accountsList") == 3
    assert data.property_count == 2
    assert data.account_results[0].bill == BillFigures(
        usage=Decimal(360), cost=Decimal(108), estimate=Decimal("223.2")
    )
    assert [line for line in _own(caplog, logging.INFO) if "bill" in line] == [
        _DAYS_SKIPPED,
        _DAYS_BACK,
    ]


async def test_a_property_that_cannot_be_read_never_blames_the_billing_period(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """With the billing period failing too, each line says only its own part."""
    caplog.set_level(logging.INFO)
    answer = json_response({"data": {"account": _two_properties(second_id=None)}})
    session.hook = lambda operation, _variables: (
        answer
        if operation == "account"
        else _FAILED
        if operation == "billingPeriods"
        else None
    )
    coordinator = coordinator_for(hass, session)

    await sync(coordinator, NOW, session)

    assert sorted(_own(caplog, logging.INFO)) == sorted([_BILLING_LOST, _DAYS_SKIPPED])


def _exporter(*, readable: bool) -> dict[str, Any]:
    """Return a property with a feed-in register, its ID missing if unreadable."""
    exporter = copy.deepcopy(account_data(feed_in=True)["properties"][0])
    exporter["id"] = "synthetic-property-2" if readable else None
    return exporter


@pytest.mark.parametrize(
    ("first_feed_in", "expected"),
    [
        # The property that can't be read may be the one that exports.
        (False, None),
        # A readable one exports: that decides it.
        (True, True),
    ],
)
async def test_feed_in_is_undecided_only_while_an_exporter_may_be_unreadable(
    hass: HomeAssistant, first_feed_in: bool, expected: bool | None
) -> None:
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    account = copy.deepcopy(account_data(feed_in=first_feed_in))
    account["properties"].append(_exporter(readable=False))
    session.hook = lambda operation, _variables: (
        json_response({"data": {"account": account}})
        if operation == "account"
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].has_feed_in is expected


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


async def test_a_figure_that_fails_once_in_a_way_that_may_pass_is_resent_at_once(
    hass: HomeAssistant, session: FakeMeridianSession
) -> None:
    """
    Like Meridian's web app, which resends on 502 to 504.

    A 503 is sent once more, 2 to 5 s later.
    """
    failures = iter([json_response({}, status=503)])
    session.hook = lambda operation, _variables: (
        next(failures, None) if operation == "balance" else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert session.operations.count("balance") == 2
    assert data.account_results[0].total_due == Decimal(40)
    assert coordinator.extras_diagnostics()["balance"]["failing"] == 0


@pytest.mark.parametrize(
    ("status", "may_pass"),
    [(503, True), (502, True), (500, False), (524, False)],
)
async def test_a_figure_still_failing_after_its_resend_waits_as_its_failure_says(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    status: int,
    may_pass: bool,
) -> None:
    """
    A failure that may pass is asked again at the next update; others wait.

    Only HTTP 502 to 504, which Meridian's web app resends on, a timeout or
    a network error may pass. Any other failure waits the figure's own
    interval, 6 hours for the balance.
    """
    session.hook = _failing("balance", json_response({}, status=status))
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)
    assert data.account_results[0].total_due is None
    sends = 2 if may_pass else 1
    assert session.operations.count("balance") == sends

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, NOW + HOUR, session)
    assert session.operations.count("balance") == sends + (1 if may_pass else 0)
    assert (data.account_results[0].total_due is not None) is may_pass

    data = await sync(coordinator, NOW + 6 * HOUR, session)
    assert data.account_results[0].total_due == Decimal(40)


@pytest.mark.parametrize("missing", [1, 4])
async def test_the_estimate_is_unknown_when_the_day_totals_leave_out_a_date(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture, missing: int
) -> None:
    """
    So far counts the dates read; the estimate needs every date of the period.

    An answer short of a date would leave that date's cost out of the
    estimate, so it shows unknown rather than too little. One INFO line
    says so, however many answers leave a date out, and one when an answer
    covers the period again.
    """
    caplog.set_level(logging.INFO)
    kraken = FakeKraken(NOW, history_start=local_midnight(_PERIOD[0] + missing * DAY))
    session = FakeMeridianSession(NOW, kraken=kraken)
    session.billing_period = _PERIOD
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    # From the first date read to 15 July, of 12 kWh and $3.60 each.
    read = 15 - missing
    assert data.account_results[0].bill == BillFigures(
        usage=Decimal(12 * read), cost=Decimal("3.6") * read, estimate=None
    )
    await sync(coordinator, NOW + 6 * HOUR, session)
    assert len(_day_requests(session)) == 2
    assert _own(caplog, logging.INFO) == [_ESTIMATE_LOST]

    kraken.history_start = local_midnight(_PERIOD[0])
    data = await sync(coordinator, NOW + 12 * HOUR, session)
    assert data.account_results[0].bill.estimate == Decimal("111.6")
    assert _own(caplog, logging.INFO) == [_ESTIMATE_LOST, _ESTIMATE_BACK]


async def test_a_billing_period_that_ends_before_it_starts_is_refused(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    session.billing_period = (_PERIOD[1], _PERIOD[0])
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    (result,) = data.account_results
    assert result.billing_period is None
    assert result.bill == BillFigures()
    assert _day_requests(session) == []
    assert (
        "Couldn't get the billing period (a period that ends before it starts)"
        in _own(caplog, logging.DEBUG)
    )


async def test_an_account_with_no_property_has_no_bill_to_show(
    hass: HomeAssistant,
) -> None:
    """Unknown, not 0: there is no property whose dates could be summed."""
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, "A-TEST-2"))
    session.billing_period = _PERIOD
    session.hook = lambda operation, variables: (
        json_response(
            {"data": {"account": {**account_data("A-TEST-2"), "properties": []}}}
        )
        if operation == "account" and variables["accountNumber"] == "A-TEST-2"
        else None
    )
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    first, second = data.account_results
    assert first.bill.usage == Decimal(180)
    assert second.bill == BillFigures()
    # The first account's dates alone were asked for.
    assert len(_day_requests(session)) == 1


async def test_a_day_with_no_agreement_in_force_keeps_the_price_sensors_unknown(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Meridian lists no agreement in force for the meter (when that happens is not known).

    That says nothing of the plan, so the price sensors stay, showing
    unknown, until a later answer says. One INFO line says why, and one
    when the prices are back.
    """
    caplog.set_level(logging.INFO)
    coordinator = coordinator_for(hass, session)
    data = await sync(coordinator, NOW, session)
    assert data.account_results[0].price_sensors is True

    session.prices = {"meterPoints": [{"activeAgreement": None}]}
    for day in (1, 2):
        data = await sync(coordinator, NOW + day * DAY, session)
        (result,) = data.account_results
        assert (result.price_sensors, result.prices) == (None, None)
        # Said once, at INFO: a state Meridian's app expects.
        assert _own(caplog, logging.INFO) == [_NO_PLAN]

    session.prices = prices_data()
    data = await sync(coordinator, NOW + 3 * DAY, session)
    assert data.account_results[0].price_sensors is True
    assert data.account_results[0].prices is not None
    assert _own(caplog, logging.INFO) == [_NO_PLAN, _PRICES_BACK]


async def test_prices_that_failed_then_name_no_plan_are_not_back(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An answer with no plan in force leaves them unknown: the line says why."""
    caplog.set_level(logging.INFO)
    session.hook = _failing("prices")
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)

    session.hook = lambda _operation, _variables: None
    session.prices = {"meterPoints": [{"activeAgreement": None}]}
    data = await sync(coordinator, NOW + DAY, session)

    assert data.account_results[0].price_sensors is None
    assert _own(caplog, logging.INFO) == [_PRICES_UNANSWERED, _NO_PLAN]


async def test_no_plan_in_force_on_a_time_of_use_account_names_no_price_sensor(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The account has no price sensors, before or after: the line names none."""
    caplog.set_level(logging.INFO)
    session.prices = _TIME_OF_USE
    coordinator = coordinator_for(hass, session)
    data = await sync(coordinator, NOW, session)
    assert data.account_results[0].price_sensors is False

    session.prices = {"meterPoints": [{"activeAgreement": None}]}
    await sync(coordinator, NOW + DAY, session)
    session.prices = _TIME_OF_USE
    data = await sync(coordinator, NOW + 2 * DAY, session)

    assert data.account_results[0].price_sensors is False
    lines = _own(caplog, logging.INFO)
    assert _NO_PLAN in lines
    for line in lines:
        assert "Daily charge" not in line
        assert "Anytime price" not in line


@pytest.mark.parametrize("options", ["null", "missing"])
async def test_an_account_without_billing_options_has_no_period_and_nothing_fails(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
    options: str,
) -> None:
    """Meridian may leave them out: no period, so no bill, and no failure."""
    caplog.set_level(logging.INFO)
    account = {"billingOptions": None} if options == "null" else {}
    session.hook = lambda operation, _variables: (
        json_response({"data": {"account": account}})
        if operation == "billingPeriods"
        else None
    )
    coordinator = coordinator_for(hass, session)

    for update in range(2):
        data = await sync(coordinator, NOW + update * HOUR, session)

    (result,) = data.account_results
    assert result.billing_period is None
    assert result.bill == BillFigures()
    assert result.total_due == Decimal(40)
    assert _day_requests(session) == []
    assert session.operations.count("billingPeriods") == 1
    assert coordinator.extras_diagnostics()["billing"]["failing"] == 0
    assert _own(caplog, logging.INFO) == []


async def test_billing_options_gone_in_a_period_take_its_bill_with_them(
    hass: HomeAssistant,
    session: FakeMeridianSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Meridian answers with no options: no period, so no bill, and nothing lost.

    The day totals got for the period before are dropped, not kept as its
    estimate until that period would have ended; they are asked for at once
    when a period comes back.
    """
    caplog.set_level(logging.INFO)
    coordinator = coordinator_for(hass, session)
    await sync(coordinator, NOW, session)
    caplog.clear()
    session.kraken.requests.clear()
    session.hook = lambda operation, _variables: (
        json_response({"data": {"account": {"billingOptions": None}}})
        if operation == "billingPeriods"
        else None
    )

    data = await sync(coordinator, NOW + DAY + HOUR, session)

    (result,) = data.account_results
    assert result.billing_period is None
    assert result.bill == BillFigures()
    assert _day_requests(session) == []
    assert _own(caplog, logging.INFO) == []

    session.hook = lambda _operation, _variables: None
    data = await sync(coordinator, NOW + 2 * DAY + 2 * HOUR, session)
    (result,) = data.account_results
    assert result.billing_period is not None
    assert len(_day_requests(session)) == 1
    assert result.bill.estimate is not None


@pytest.mark.parametrize(
    ("kwh", "cents", "expected"),
    [
        # 15 dates read: 180.225 kWh and $54.225; all 31 dates: $112.065.
        (
            "0.500625",
            "15.0625",
            BillFigures(
                usage=Decimal("180.23"),
                cost=Decimal("54.23"),
                export=Decimal("180.23"),
                credit=Decimal("54.23"),
                estimate=Decimal("112.07"),
            ),
        ),
        # 15 dates read: 180.0045 kWh and $54.0045; all 31 dates: $111.6093.
        (
            "0.5000125",
            "15.00125",
            BillFigures(
                usage=Decimal(180),
                cost=Decimal(54),
                export=Decimal(180),
                credit=Decimal(54),
                estimate=Decimal("111.61"),
            ),
        ),
    ],
    ids=["a half", "under a half"],
)
async def test_bill_figures_are_summed_then_rounded_to_the_cent(
    hass: HomeAssistant, kwh: str, cents: str, expected: BillFigures
) -> None:
    """
    The rule: each figure is summed in full, then rounded to 0.01.

    A half is rounded away from zero, as money is, and less than a half
    down. Each date here is 12.015 kWh and $3.615, both ways, so rounding
    each date first would give other totals ($54.30 so far), and rounding a
    half to even 180.22 kWh, $54.22 and $112.06; at 12.0003 kWh and
    $3.6003, rounding up would give 180.01 kWh and $54.01.
    """
    kraken = FakeKraken(
        NOW,
        days=100,
        value=lambda _start, _channel: kwh,
        cost=lambda _start, _channel: cents,
    )
    session = FakeMeridianSession(NOW, kraken=kraken, feed_in=(ACCOUNT_NUMBER,))
    session.billing_period = _PERIOD
    coordinator = coordinator_for(hass, session)

    data = await sync(coordinator, NOW, session)

    assert data.account_results[0].bill == expected
