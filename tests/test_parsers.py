"""The day-total, balance and price parsers, on synthetic answers."""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from custom_components.meridian_energy import parsers as parsers_module
from custom_components.meridian_energy.models import (
    MeridianBill,
    MeridianPayment,
    MeridianPrices,
)
from custom_components.meridian_energy.parsers import (
    parse_balance,
    parse_measurement,
    parse_prices,
)
from custom_components.meridian_energy.windows import day_readings

from .meridian_http import prices_data, rate

_DAY = "DAY_INTERVAL"


def _day_node(start: str, end: str, *, frequency: str = _DAY) -> dict[str, Any]:
    return {
        "value": "12.5",
        "unit": "kWh",
        "startAt": start,
        "endAt": end,
        "readAt": end,
        "metaData": {
            "utilityFilters": {
                "readingFrequencyType": frequency,
                "readingDirection": "CONSUMPTION",
                "readingQuality": "ESTIMATE",
                "marketSupplyPointId": "synthetic-market-point",
                "registerId": "synthetic-register",
            },
            "statistics": [
                {"type": "CONSUMPTION_COST", "costInclTax": {"estimatedAmount": "300"}},
                {
                    "type": "STANDING_CHARGE_COST",
                    "costInclTax": {"estimatedAmount": "200"},
                },
            ],
        },
    }


@pytest.mark.parametrize(
    ("start", "end", "hours"),
    [
        # Daylight saving starts: 23 hours.
        ("2026-09-27T00:00:00+12:00", "2026-09-28T00:00:00+13:00", 23),
        ("2026-07-13T00:00:00+12:00", "2026-07-14T00:00:00+12:00", 24),
        # Daylight saving ends: 25 hours.
        ("2026-04-05T00:00:00+13:00", "2026-04-06T00:00:00+12:00", 25),
    ],
)
def test_a_day_node_runs_from_one_new_zealand_midnight_to_the_next(
    start: str, end: str, hours: int
) -> None:
    measurement = parse_measurement(_day_node(start, end), "CONSUMPTION", _DAY)

    assert measurement.end is not None
    assert (measurement.end - measurement.start).total_seconds() == hours * 3600
    assert measurement.value_kwh == Decimal("12.5")
    assert measurement.cost_cents == Decimal(500)
    assert measurement.quality == "ESTIMATE"


@pytest.mark.parametrize(
    ("start", "end"),
    [
        # A UTC day, an hour, a day shifted off midnight, and one that starts
        # an hour late.
        ("2026-07-13T00:00:00+00:00", "2026-07-14T00:00:00+00:00"),
        ("2026-07-13T00:00:00+12:00", "2026-07-13T01:00:00+12:00"),
        ("2026-07-13T01:00:00+12:00", "2026-07-14T01:00:00+12:00"),
        ("2026-07-13T01:00:00+12:00", "2026-07-14T00:00:00+12:00"),
        # 24 hours across the day daylight saving starts: not one date.
        ("2026-09-27T00:00:00+12:00", "2026-09-28T01:00:00+13:00"),
    ],
)
def test_a_day_node_off_new_zealand_midnights_is_refused(start: str, end: str) -> None:
    with pytest.raises(ValueError, match="New Zealand date"):
        parse_measurement(_day_node(start, end), "CONSUMPTION", _DAY)


@pytest.mark.parametrize("field", ["value", "cost"])
def test_a_number_past_1e12_is_refused(field: str) -> None:
    """No kWh, nor any cost in cents, comes near: the sum of a few would overflow."""

    def node(number: str) -> dict[str, Any]:
        day = _day_node("2026-07-13T00:00:00+12:00", "2026-07-14T00:00:00+12:00")
        if field == "value":
            day["value"] = number
        else:
            day["metaData"]["statistics"][0]["costInclTax"]["estimatedAmount"] = number
        return day

    for number in ("123456.789", "9E+12"):
        row = parse_measurement(node(number), "CONSUMPTION", _DAY)
        if field == "value":
            assert row.value_kwh == Decimal(number)
        else:
            assert row.cost_cents == Decimal(number) + 200
    for number in ("1E+13", "1E+1000000"):
        with pytest.raises(ValueError, match="out of range"):
            parse_measurement(node(number), "CONSUMPTION", _DAY)


def test_an_hour_node_in_a_day_answer_is_refused() -> None:
    node = _day_node(
        "2026-07-13T00:00:00+12:00",
        "2026-07-14T00:00:00+12:00",
        frequency="HOUR_INTERVAL",
    )
    with pytest.raises(ValueError, match="frequency"):
        parse_measurement(node, "CONSUMPTION", _DAY)


def test_day_readings_sum_channels_and_keep_dates_in_range() -> None:
    """A date is actual only when every channel is; dates outside are left out."""
    nodes = [
        _day_node("2026-07-13T00:00:00+12:00", "2026-07-14T00:00:00+12:00"),
        _day_node("2026-07-14T00:00:00+12:00", "2026-07-15T00:00:00+12:00"),
        _day_node("2026-07-15T00:00:00+12:00", "2026-07-16T00:00:00+12:00"),
    ]
    second = _day_node("2026-07-13T00:00:00+12:00", "2026-07-14T00:00:00+12:00")
    second["metaData"]["utilityFilters"]["registerId"] = "synthetic-register-2"
    nodes[0]["metaData"]["utilityFilters"]["readingQuality"] = "ACTUAL"
    rows = [parse_measurement(node, "CONSUMPTION", _DAY) for node in (*nodes, second)]

    readings = day_readings(rows, date(2026, 7, 13), date(2026, 7, 14))

    assert set(readings) == {date(2026, 7, 13), date(2026, 7, 14)}
    assert readings[date(2026, 7, 13)].energy == Decimal(25)
    assert readings[date(2026, 7, 13)].cost_cents == Decimal(1000)
    assert readings[date(2026, 7, 13)].quality == "ESTIMATE"


def _edges(*nodes: dict[str, Any]) -> dict[str, Any]:
    return {"edges": [{"node": node} for node in nodes]}


def test_the_balance_keeps_dated_bills_and_payments_in_answer_order() -> None:
    """
    The parser keeps Meridian's order, leaving out nodes without a date.

    The coordinator chooses the last bill and payment.
    """
    balance = parse_balance(
        {
            "balance": -12345,
            "bills": _edges(
                {"fromDate": "2026-05-01", "toDate": "2026-05-31", "issuedDate": None},
                {
                    "fromDate": "2026-06-01",
                    "toDate": "2026-06-30",
                    "issuedDate": "2026-07-02",
                },
            ),
            "payments": _edges(
                {"amount": 1000, "paymentDate": "2026-06-10", "status": "CLEARED"},
                {"amount": 3000, "paymentDate": "2026-07-20", "status": "REQUESTED"},
                {"amount": 2000, "paymentDate": None, "status": "CLEARED"},
                {"amount": 4000, "paymentDate": "2026-07-10", "status": None},
            ),
        }
    )

    assert balance.balance_cents == -12345
    assert balance.bills == (
        MeridianBill(
            issued=date(2026, 7, 2), start=date(2026, 6, 1), end=date(2026, 6, 30)
        ),
    )
    assert balance.payments == (
        MeridianPayment(amount_cents=1000, paid_on=date(2026, 6, 10), status="CLEARED"),
        MeridianPayment(
            amount_cents=3000, paid_on=date(2026, 7, 20), status="REQUESTED"
        ),
        MeridianPayment(amount_cents=4000, paid_on=date(2026, 7, 10), status=None),
    )


@pytest.mark.parametrize(
    "account",
    [
        {"balance": "-100", "bills": _edges(), "payments": _edges()},
        {"balance": True, "bills": _edges(), "payments": _edges()},
        {"balance": -100, "bills": None, "payments": _edges()},
        {"balance": -100, "bills": _edges(), "payments": {"edges": [None]}},
        {
            "balance": -100,
            "bills": _edges(),
            "payments": _edges({"amount": 1.5, "paymentDate": "2026-07-10"}),
        },
    ],
)
def test_a_balance_that_does_not_read_is_refused(account: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=r"Invalid Meridian|Expected"):
        parse_balance(account)


@pytest.mark.parametrize(
    "cents", [10**13, -(10**13), 10**400], ids=["1E+13", "-1E+13", "1E+400"]
)
@pytest.mark.parametrize("field", ["balance", "payment"])
def test_a_balance_or_payment_past_1e12_cents_is_refused(
    field: str, cents: int
) -> None:
    """Far past any account's, and past what a sensor can show at 10**400."""

    def account(amount: int) -> dict[str, Any]:
        payment = {"amount": amount, "paymentDate": "2026-07-10", "status": "CLEARED"}
        return {
            "balance": amount if field == "balance" else -100,
            "bills": _edges(),
            "payments": _edges(payment if field == "payment" else {}),
        }

    limit = 10**13 - 1 if cents > 0 else -(10**13 - 1)
    within = parse_balance(account(limit))
    read = (
        within.balance_cents if field == "balance" else within.payments[0].amount_cents
    )
    assert read == limit
    with pytest.raises(ValueError, match="out of range"):
        parse_balance(account(cents))


@pytest.fixture(autouse=True)
def fresh_rate_log() -> None:
    """Each test starts with no unreadable price field logged yet."""
    parsers_module._logged_rates.clear()


@pytest.mark.parametrize(
    ("daily", "anytime", "expected"),
    [
        ("$2.000000 /day", "$0.300000 /kWh", (Decimal(2), Decimal("0.3"))),
        ("$2.12/day", "$0.3141/kWh", (Decimal("2.12"), Decimal("0.3141"))),
        ("212.500 c/day", "31.41 c/kWh", (Decimal("2.125"), Decimal("0.3141"))),
        # A wrong unit for its band, or a currency marker it can't read.
        ("$0.300000 /kWh", "$2.000000 /day", (None, None)),
        ("€2.00 /day", "NZD 0.30 /kWh", (None, None)),
        ("2.00 /day", "", (None, None)),
        # More than a price: the whole text must read as one.
        ("$2.00 /day + GST", "from $0.30 /kWh", (None, None)),
        ("$2.00 /day $0.30 /kWh", "$0.30 /kWh, $0.25 /kWh off-peak", (None, None)),
        ("212.500 c/day extra", "31.41 c/kWh more", (None, None)),
        # Up to 1E+12 in either unit; past it, too large to divide or to show.
        pytest.param(
            "$1" + "0" * 12 + " /day",
            "1" + "0" * 12 + " c/kWh",
            (Decimal("1E12"), Decimal("1E10")),
            id="at-the-limit",
        ),
        pytest.param(
            "$1" + "0" * 13 + " /day",
            "1" + "0" * 13 + " c/kWh",
            (None, None),
            id="past-the-limit",
        ),
        pytest.param(
            "$" + "9" * 400 + " /day",
            "9" * 400 + " c/kWh",
            (None, None),
            id="past-a-float",
        ),
        pytest.param(
            "$1" + "0" * 1_000_000 + " /day",
            "1" + "0" * 1_000_003 + " c/kWh",
            (None, None),
            id="too-large-to-divide",
        ),
    ],
)
def test_prices_read_dollars_or_cents_per_their_band_unit(
    daily: str, anytime: str, expected: tuple[Decimal | None, Decimal | None]
) -> None:
    prices = parse_prices(prices_data(daily, anytime))

    assert prices == MeridianPrices("Synthetic Plan", *expected)


_DAILY_UNREAD = (
    "Meridian Energy can't read the daily charge in formattedRateIncludingTax; "
    "it shows unknown"
)
_ANYTIME_UNREAD = (
    "Meridian Energy can't read the anytime price in formattedRateIncludingTax; "
    "it shows unknown"
)


def test_an_unreadable_price_logs_its_field_name_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Each price's field is named once, though both prices read the same field."""
    caplog.set_level(logging.INFO)
    for _answer in range(2):
        parse_prices(prices_data("2 dollars", "$0.30 /day"))

    assert [record.getMessage() for record in caplog.records] == [
        _DAILY_UNREAD,
        _ANYTIME_UNREAD,
    ]
    assert "2 dollars" not in caplog.text
    assert "$0.30 /day" not in caplog.text


def test_the_other_price_unreadable_later_is_named_too(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    parse_prices(prices_data("2 dollars", "$0.30 /kWh"))
    parse_prices(prices_data("$2.00 /day", "30 cents"))

    assert [record.getMessage() for record in caplog.records] == [
        _DAILY_UNREAD,
        _ANYTIME_UNREAD,
    ]


def test_a_discount_uses_the_discounted_price() -> None:
    plan = prices_data()
    plan["meterPoints"][0]["activeAgreement"]["rates"] = [
        rate("STANDING_CHARGE", "$2.000000 /day", discount="$1.800000 /day"),
        rate("CONSUMPTION_CHARGE", "$0.300000 /kWh"),
    ]
    prices = parse_prices(plan)

    assert prices is not None
    assert prices.daily_charge == Decimal("1.8")
    assert prices.anytime == Decimal("0.3")


def _plan(*rates: dict[str, Any]) -> dict[str, Any]:
    plan = prices_data()
    plan["meterPoints"][0]["activeAgreement"]["rates"] = list(rates)
    return plan


@pytest.mark.parametrize(
    "plan",
    [
        # A time of use, two daily charges, no price per kWh.
        _plan(
            rate("STANDING_CHARGE", "$2 /day"),
            rate("CONSUMPTION_CHARGE", "$0.30 /kWh", bucket="Day"),
        ),
        _plan(
            rate("STANDING_CHARGE", "$2 /day"),
            rate("STANDING_CHARGE", "$1 /day"),
            rate("CONSUMPTION_CHARGE", "$0.30 /kWh"),
        ),
        _plan(rate("STANDING_CHARGE", "$2 /day")),
        # Two meter points, or none with a current agreement.
        {"meterPoints": prices_data()["meterPoints"] * 2},
        {"meterPoints": [{"activeAgreement": None}]},
    ],
)
def test_any_plan_but_one_all_day_agreement_has_no_prices(plan: dict[str, Any]) -> None:
    assert parse_prices(plan) is None
