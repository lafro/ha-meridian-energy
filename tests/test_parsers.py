"""The day-total, balance and price parsers, on synthetic answers."""

from __future__ import annotations

import logging
from datetime import date, time
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from custom_components.meridian_energy.models import (
    MeridianBill,
    MeridianNoAgreement,
    MeridianPayment,
    MeridianPrices,
    UnreadablePrice,
)
from custom_components.meridian_energy.parsers import (
    parse_balance,
    parse_measurement,
    parse_prices,
    price_shape,
)
from custom_components.meridian_energy.windows import day_readings

from .meridian_http import prices_data, rate

_DAY = "DAY_INTERVAL"
_NZ = ZoneInfo("Pacific/Auckland")


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
    "end",
    [
        # An hour, 24 hours across the day daylight saving starts, a UTC
        # midnight, and no end at all.
        "2026-09-27T01:00:00+12:00",
        "2026-09-28T01:00:00+13:00",
        "2026-09-28T00:00:00+00:00",
        None,
    ],
)
def test_a_day_total_is_filed_under_the_date_it_starts_whatever_its_end(
    end: str | None,
) -> None:
    """A total that starts at a New Zealand midnight is that whole date's."""
    node = _day_node("2026-09-27T00:00:00+12:00", end or "")
    if end is None:
        del node["endAt"]
    measurement = parse_measurement(node, "CONSUMPTION", _DAY)

    assert measurement.start.astimezone(_NZ).date() == date(2026, 9, 27)
    assert measurement.end is not None
    # Daylight saving starts that date: 23 hours, to the next midnight.
    assert (measurement.end - measurement.start).total_seconds() == 23 * 3600
    assert measurement.end.astimezone(_NZ).time() == time(0)


@pytest.mark.parametrize(
    ("start", "end"),
    [
        # A UTC day, a day shifted off midnight, and one that starts an hour
        # late.
        ("2026-07-13T00:00:00+00:00", "2026-07-14T00:00:00+00:00"),
        ("2026-07-13T01:00:00+12:00", "2026-07-14T01:00:00+12:00"),
        ("2026-07-13T01:00:00+12:00", "2026-07-14T00:00:00+12:00"),
    ],
)
def test_a_day_node_that_does_not_start_at_a_new_zealand_midnight_is_refused(
    start: str, end: str
) -> None:
    with pytest.raises(ValueError, match="New Zealand midnight"):
        parse_measurement(_day_node(start, end), "CONSUMPTION", _DAY)


@pytest.mark.parametrize("field", ["value", "cost"])
def test_a_number_from_1e13_is_refused(field: str) -> None:
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
def test_a_balance_or_payment_from_1e13_cents_is_refused(
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


_TAX = "formattedRateIncludingTax"
_DISCOUNTED = "formattedRateIncludingTaxIncludingDiscount"


@pytest.mark.parametrize(
    ("daily", "anytime", "expected"),
    [
        ("$2.000000", "$0.300000", (Decimal(2), Decimal("0.3"))),
        ("$2.12", "$0.3141", (Decimal("2.12"), Decimal("0.3141"))),
        # Space around it is no part of it; a whole number of dollars reads.
        (" $2 ", "\t$0.30\n", (Decimal(2), Decimal("0.3"))),
        ("$0", "$0.0", (Decimal(0), Decimal(0))),
        # Below 1E+13, as written; from 1E+13 up, too large to show.
        pytest.param(
            "$1" + "0" * 12,
            "$" + "9" * 12 + ".5",
            (Decimal("1E12"), Decimal("999999999999.5")),
            id="at-the-limit",
        ),
    ],
)
def test_a_price_is_its_dollar_amount_as_written(
    daily: str, anytime: str, expected: tuple[Decimal, Decimal]
) -> None:
    """The amount is the price, its unit given by its band, with no arithmetic."""
    prices = parse_prices(prices_data(daily, anytime))

    assert prices == MeridianPrices("Synthetic Plan", *expected)


@pytest.mark.parametrize(
    ("text", "shape"),
    [
        # A unit, cents, a currency in letters, and words around the amount.
        ("$0.300000 /kWh", "$N.NNNNNN /AAA"),
        ("31.41c", "NN.NNA"),
        ("NZD 0.30", "AAA N.NN"),
        ("from $0.30", "AAAA $N.NN"),
        ("$0.30 + GST", "$N.NN + AAA"),
        # A negative amount, and amounts written otherwise.
        ("-$0.30", "-$N.NN"),
        ("$-0.30", "$-N.NN"),
        ("$1,000.00", "$N,NNN.NN"),
        ("$.30", "$.NN"),
        ("$0.", "$N."),
        ("€0.30", "€N.NN"),
        # Digits of another script, or full width, are digits all the same.
        ("$\u0662", "$N"),
        ("$\uff12", "$N"),
        # Nothing to read.
        ("", "empty"),
        ("   ", "empty"),
        (None, "missing"),
        (0.3, "not text"),
        # Space as one space, and what can't be printed as "?".
        ("$0.30\u00a0\u200b", "$N.NN ?"),
        # A long text is cut short.
        (
            "$0.30 per kWh including GST and the levy",
            "$N.NN AAA AAA AAAAAAAAA…",
        ),
    ],
)
def test_any_other_price_text_is_unknown_and_listed_by_its_shape(
    text: object, shape: str
) -> None:
    """Only a dollar amount alone reads; anything else is listed, never quoted."""
    plan = prices_data()
    plan["meterPoints"][0]["activeAgreement"]["rates"][1][_TAX] = text
    prices = parse_prices(plan)

    assert prices == MeridianPrices(
        "Synthetic Plan",
        Decimal(2),
        None,
        (UnreadablePrice("anytime_price", _TAX, shape),),
    )
    assert len(shape) <= 24


@pytest.mark.parametrize(
    "text",
    [
        "$1" + "0" * 13,
        "$" + "9" * 400,
        "$1" + "0" * 1_000_000,
    ],
    ids=["past-the-limit", "past-a-float", "a-million-digits"],
)
def test_a_price_from_1e13_is_unknown(text: str) -> None:
    prices = parse_prices(prices_data(daily=text))

    assert prices is not None
    assert prices.daily_charge is None
    assert prices.anytime == Decimal("0.3")
    assert [(item.key, item.field) for item in prices.unreadable] == [
        ("daily_charge", _TAX)
    ]
    assert len(prices.unreadable[0].shape) <= 24


def test_both_prices_unreadable_are_both_listed() -> None:
    prices = parse_prices(prices_data("$2.00 /day", "30 cents"))

    assert prices is not None
    assert prices.unreadable == (
        UnreadablePrice("daily_charge", _TAX, "$N.NN /AAA"),
        UnreadablePrice("anytime_price", _TAX, "NN AAAAA"),
    )


def test_a_discount_uses_the_discounted_price() -> None:
    plan = _plan(
        rate("STANDING_CHARGE", "$2.000000", discount="$1.800000"),
        rate("CONSUMPTION_CHARGE", "$0.300000"),
    )
    prices = parse_prices(plan)

    assert prices == MeridianPrices("Synthetic Plan", Decimal("1.8"), Decimal("0.3"))


def test_a_discounted_price_that_cannot_be_read_names_its_field() -> None:
    plan = _plan(
        rate("STANDING_CHARGE", "$2.000000", discount="$1.80 off"),
        rate("CONSUMPTION_CHARGE", "$0.300000"),
    )
    prices = parse_prices(plan)

    assert prices is not None
    assert prices.daily_charge is None
    assert prices.unreadable == (
        UnreadablePrice("daily_charge", _DISCOUNTED, "$N.NN AAA"),
    )


@pytest.mark.parametrize(
    ("has_discount", "daily"),
    [(True, Decimal("1.8")), (False, Decimal(2))],
    ids=repr,
)
def test_a_discount_counts_only_when_it_is_true_or_false(
    has_discount: object, daily: Decimal
) -> None:
    """hasDiscount true picks the discounted price, false the one including tax."""
    standing = rate("STANDING_CHARGE", "$2.000000", discount="$1.800000")
    standing["hasDiscount"] = has_discount
    prices = parse_prices(_plan(standing, rate("CONSUMPTION_CHARGE", "$0.300000")))

    assert prices == MeridianPrices("Synthetic Plan", daily, Decimal("0.3"))


@pytest.mark.parametrize("has_discount", ["missing", None, "true", 1, 0], ids=repr)
def test_a_discount_flag_not_true_or_false_leaves_the_price_unknown(
    has_discount: object,
) -> None:
    """Neither text can be said to be the price: it is unknown, and listed."""
    standing = rate("STANDING_CHARGE", "$2.000000", discount="$1.800000")
    if has_discount == "missing":
        del standing["hasDiscount"]
    else:
        standing["hasDiscount"] = has_discount
    prices = parse_prices(_plan(standing, rate("CONSUMPTION_CHARGE", "$0.300000")))

    assert prices == MeridianPrices(
        "Synthetic Plan",
        None,
        Decimal("0.3"),
        (UnreadablePrice("daily_charge", "hasDiscount", "not true or false"),),
    )


@pytest.mark.parametrize(
    ("discounted", "shape"),
    [("delete", "missing"), (None, "missing"), ("", "empty")],
    ids=["missing", "null", "empty"],
)
def test_a_discount_without_its_price_never_falls_back_to_the_full_one(
    discounted: str | None, shape: str
) -> None:
    """A discount's price is its discounted text: without one, it is unknown."""
    standing = rate("STANDING_CHARGE", "$2.000000", discount="$1.800000")
    if discounted == "delete":
        del standing[_DISCOUNTED]
    else:
        standing[_DISCOUNTED] = discounted
    prices = parse_prices(_plan(standing, rate("CONSUMPTION_CHARGE", "$0.300000")))

    assert prices == MeridianPrices(
        "Synthetic Plan",
        None,
        Decimal("0.3"),
        (UnreadablePrice("daily_charge", _DISCOUNTED, shape),),
    )


def test_the_parser_logs_no_price(caplog: pytest.LogCaptureFixture) -> None:
    """The parser only lists what it can't read: the coordinator logs it."""
    caplog.set_level(logging.DEBUG)
    parse_prices(prices_data("2 dollars", "$0.30 /day"))

    assert not caplog.records


def test_a_price_shape_keeps_no_letter_or_digit() -> None:
    assert price_shape("Synthetic 0123 ABC-9") == "AAAAAAAAA NNNN AAA-N"
    assert price_shape("x" * 24) == "A" * 24
    assert price_shape("x" * 25) == "A" * 23 + "…"
    # Numbers and letters of any kind, after compatibility normalisation:
    # Roman numerals, fractions and circled ones; an accent is a letter's.
    assert price_shape("\u216b\u00bd\u2469 \u00e9 \u24b6") == "AAAN\u2044NNN A A"
    assert price_shape("\u00bd\u2460\u3007") == "N\u2044NNN"
    # A combining mark that is no letter's is no character it can keep.
    assert price_shape("e\u0301 \u0301") == "A ?"


def _plan(*rates: dict[str, Any]) -> dict[str, Any]:
    plan = prices_data()
    plan["meterPoints"][0]["activeAgreement"]["rates"] = list(rates)
    return plan


@pytest.mark.parametrize(
    "plan",
    [
        # A time of use, two daily charges, no price per kWh.
        _plan(
            rate("STANDING_CHARGE", "$2"),
            rate("CONSUMPTION_CHARGE", "$0.30", bucket="Day"),
        ),
        _plan(
            rate("STANDING_CHARGE", "$2"),
            rate("STANDING_CHARGE", "$1"),
            rate("CONSUMPTION_CHARGE", "$0.30"),
        ),
        _plan(rate("STANDING_CHARGE", "$2")),
        # Two meter points.
        {"meterPoints": prices_data()["meterPoints"] * 2},
    ],
)
def test_any_plan_but_one_all_day_agreement_has_no_prices(plan: dict[str, Any]) -> None:
    assert parse_prices(plan) is None


def test_a_meter_point_with_no_agreement_in_force_says_nothing_of_the_plan() -> None:
    """
    No agreement in force is told apart from a plan with no all-day prices.

    No agreement in force says nothing of the plan, so the price sensors
    stay and show unknown, where a plan they cannot show has none.
    """
    assert parse_prices({"meterPoints": [{"activeAgreement": None}]}) == (
        MeridianNoAgreement()
    )
