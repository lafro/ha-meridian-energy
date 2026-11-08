"""Strict parsers for Meridian authentication and measurement payloads."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from .const import (
    AGGREGATE_CHANNEL_ID,
    COST_STATISTIC_TYPES,
    GENERATION_CREDIT_TYPES,
    READING_FREQUENCY_HOUR,
)
from .models import (
    MeridianBalance,
    MeridianBill,
    MeridianMeasurement,
    MeridianPayment,
    MeridianPrices,
    MeridianTokenSet,
    require_list,
    require_mapping,
)

_LOGGER = logging.getLogger(__name__)


class TokenParseError(ValueError):
    """A Firebase token response could not be parsed safely."""


_MAX_TOKEN_LIFETIME_SECONDS = 86400
# The largest power of ten a measurement's value or cost may reach: no
# hour's or day's kWh, nor any cost in cents, comes near 1E+13, and a number
# far past it would overflow when added up.
_LARGEST_EXPONENT = 12
_KNOWN_COST_TYPES = COST_STATISTIC_TYPES | GENERATION_CREDIT_TYPES
_COST_TYPE_PATTERN = re.compile(r"[A-Z][A-Z0-9_]{0,63}")
# What a type is named in the log when it is not a plain upper-case name.
_UNNAMED_COST_TYPE = "UNNAMED"
# The unknown types already logged since Home Assistant started: every hour
# of every window would carry one, so each is logged once.
_logged_cost_types: set[str] = set()
_NZ = ZoneInfo("Pacific/Auckland")
# A price as the web app formats it, in dollars or in cents, per day or kWh.
_DOLLAR_RATE = re.compile(r"\$\s?(\d+(?:\.\d+)?)\s?/\s?(day|kWh)")
_CENT_RATE = re.compile(r"(\d+(?:\.\d+)?)\s?c\s?/\s?(day|kWh)")
_STANDING, _CONSUMPTION = "STANDING_CHARGE", "CONSUMPTION_CHARGE"
_PRICE_NAMES = {_STANDING: "daily charge", _CONSUMPTION: "anytime price"}
# Each price's field already logged as unreadable, by band and field: each
# price's field is logged once.
_logged_rates: set[tuple[str, str]] = set()


def parse_firebase_tokens(payload: dict[str, Any]) -> MeridianTokenSet:
    """Parse a renewable Firebase token response."""
    id_token = required_string(payload, "idToken")
    refresh_token = required_string(payload, "refreshToken")
    user_id = firebase_user_id(payload, id_token)
    raw_expires = payload.get("expiresIn")
    try:
        if not isinstance(raw_expires, (str, int)):
            raise TypeError
        expires_in = int(raw_expires)
    except (TypeError, ValueError) as err:
        raise TokenParseError("Firebase returned an invalid expiry") from err
    if expires_in <= 0 or expires_in > _MAX_TOKEN_LIFETIME_SECONDS:
        raise TokenParseError("Firebase returned an invalid expiry")
    return MeridianTokenSet(
        id_token=id_token,
        refresh_token=refresh_token,
        expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
        user_id=user_id,
    )


def firebase_user_id(payload: dict[str, Any], id_token: str) -> str:
    """Return the Firebase UID from a response field or signed token claims."""
    local_id = payload.get("localId")
    if isinstance(local_id, str) and local_id:
        return local_id
    try:
        encoded_claims = id_token.split(".")[1]
        padding = "=" * (-len(encoded_claims) % 4)
        claims = json.loads(
            base64.urlsafe_b64decode(encoded_claims + padding).decode("utf-8")
        )
        if not isinstance(claims, dict):
            raise TypeError
        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject:
            raise TypeError
    except (IndexError, TypeError, ValueError) as err:
        raise TokenParseError(
            "Firebase returned no authenticated user identifier"
        ) from err
    return subject


def parse_measurement(
    node: dict[str, Any],
    expected_direction: str,
    frequency: str = READING_FREQUENCY_HOUR,
) -> MeridianMeasurement:
    """
    Parse one electricity measurement: an hour, or a New Zealand date.

    A date runs from its midnight to the next, so it has 23, 24 or 25 hours.
    """
    start = parse_datetime(node.get("startAt"), "measurement start")
    end = parse_datetime(node.get("endAt"), "measurement end")
    if frequency == READING_FREQUENCY_HOUR:
        if end.astimezone(UTC) - start.astimezone(UTC) != timedelta(hours=1):
            raise ValueError("Measurement interval is not exactly one hour")
    else:
        day = start.astimezone(_NZ).date()
        if (start, end) != (_nz_midnight(day), _nz_midnight(day + timedelta(days=1))):
            raise ValueError("Measurement interval is not one New Zealand date")
    if required_string(node, "unit").casefold() != "kwh":
        raise ValueError("Unexpected measurement unit")
    value = _finite_decimal(node.get("value"), "measurement value")
    if value < 0:
        raise ValueError("Negative electricity measurement")

    metadata = require_mapping(node.get("metaData"), "measurement metadata")
    filters = require_mapping(metadata.get("utilityFilters"), "measurement filters")
    direction = required_string(filters, "readingDirection")
    if direction != expected_direction:
        raise ValueError("Unexpected measurement direction")
    if required_string(filters, "readingFrequencyType") != frequency:
        raise ValueError("Unexpected measurement frequency")
    quality = required_string(filters, "readingQuality")
    channel_id = _measurement_channel_id(filters)

    return MeridianMeasurement(
        start=start,
        end=end,
        value_kwh=value,
        quality=quality,
        direction=direction,
        channel_id=channel_id,
        cost_cents=_measurement_cost_cents(metadata, direction),
    )


def _nz_midnight(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=_NZ)


def parse_balance(account: dict[str, Any]) -> MeridianBalance:
    """Parse an account's balance, its bills and its payments; amounts in cents."""
    bills: list[MeridianBill] = []
    for node in _nodes(account.get("bills"), "bills"):
        issued = optional_date(node.get("issuedDate"))
        if issued is not None:
            bills.append(
                MeridianBill(
                    issued=issued,
                    start=optional_date(node.get("fromDate")),
                    end=optional_date(node.get("toDate")),
                )
            )
    payments: list[MeridianPayment] = []
    for node in _nodes(account.get("payments"), "payments"):
        paid_on = optional_date(node.get("paymentDate"))
        if paid_on is not None:
            payments.append(
                MeridianPayment(
                    amount_cents=_cents(node.get("amount"), "payment amount"),
                    paid_on=paid_on,
                    status=optional_string(node.get("status")),
                )
            )
    return MeridianBalance(
        balance_cents=_cents(account.get("balance"), "balance"),
        bills=tuple(bills),
        payments=tuple(payments),
    )


def _nodes(connection: Any, context: str) -> list[dict[str, Any]]:
    """Return the nodes of a connection's edges."""
    edges = require_list(require_mapping(connection, context).get("edges"), context)
    return [
        require_mapping(require_mapping(edge, context).get("node"), context)
        for edge in edges
    ]


def _cents(value: Any, context: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"Invalid Meridian {context}")
    return value


def parse_prices(property_data: dict[str, Any]) -> MeridianPrices | None:
    """
    Parse an all-day plan's prices, or return None for any other plan.

    All-day: one meter point, whose current agreement has one daily charge
    and one price per kWh, and no rate tied to a time of day. Each price is
    read strictly, its discounted form when it has a discount; one that does
    not read as dollars or cents per day or kWh, as its band says, is None,
    and the name of each price's field is logged once.
    """
    meters = require_list(property_data.get("meterPoints"), "meter points")
    if len(meters) != 1:
        return None
    agreement = require_mapping(meters[0], "meter point").get("activeAgreement")
    if agreement is None:
        return None
    agreement = require_mapping(agreement, "agreement")
    rates = [
        require_mapping(rate, "rate")
        for rate in require_list(agreement.get("rates") or [], "rates")
    ]
    bands = {
        band: [rate for rate in rates if rate.get("bandCategory") == band]
        for band in _PRICE_NAMES
    }
    if any(len(found) != 1 for found in bands.values()) or any(
        rate.get("touBucketName") not in (None, "") for rate in rates
    ):
        return None
    return MeridianPrices(
        plan=optional_string(agreement.get("displayName")),
        daily_charge=_price(bands[_STANDING][0], _STANDING, "day"),
        anytime=_price(bands[_CONSUMPTION][0], _CONSUMPTION, "kWh"),
    )


def _price(rate: dict[str, Any], band: str, unit: str) -> Decimal | None:
    """Return a rate's price in NZD, or None when it does not read as its band's."""
    field = (
        "formattedRateIncludingTaxIncludingDiscount"
        if rate.get("hasDiscount") is True
        else "formattedRateIncludingTax"
    )
    text = rate.get(field)
    text = text.strip() if isinstance(text, str) else ""
    for pattern, dollars in ((_DOLLAR_RATE, Decimal(1)), (_CENT_RATE, Decimal(100))):
        match = pattern.fullmatch(text)
        if match is not None and match[2] == unit:
            return Decimal(match[1]) / dollars
    if (band, field) not in _logged_rates:
        _logged_rates.add((band, field))
        _LOGGER.info(
            "Meridian Energy can't read the %s in %s; it shows unknown",
            _PRICE_NAMES[band],
            field,
        )
    return None


def _measurement_channel_id(filters: dict[str, Any]) -> str:
    """Return a stable, privacy-safe identity for one physical meter channel."""
    identifiers: dict[str, str] = {}
    for key in ("marketSupplyPointId", "deviceId", "registerId"):
        value = filters.get(key)
        if value is None or value == "":
            continue
        if not isinstance(value, str):
            raise ValueError(f"Invalid {key}")
        identifiers[key] = value

    if register_id := identifiers.get("registerId"):
        identity = ("registerId", register_id)
    elif device_id := identifiers.get("deviceId"):
        identity = ("deviceId", device_id)
    else:
        return AGGREGATE_CHANNEL_ID

    canonical = json.dumps(identity, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _measurement_cost_cents(metadata: dict[str, Any], direction: str) -> Decimal | None:
    """
    Return a complete interval cost or credit, in cents.

    A cost adds up every cost type Meridian's own app counts, each with its
    sign, so a credit lowers it, below zero even. An export credit counts
    as positive, whatever its sign. A type the integration does not know is
    left out, and its name logged once.
    """
    generation = direction == "GENERATION"
    allowed_cost_types = GENERATION_CREDIT_TYPES if generation else COST_STATISTIC_TYPES
    cost_cents = Decimal(0)
    found_cost = False
    incomplete_cost = False
    for statistic_value in require_list(
        metadata.get("statistics") or [], "measurement statistics"
    ):
        statistic = require_mapping(statistic_value, "measurement statistic")
        cost_type = statistic.get("type")
        if not isinstance(cost_type, str) or cost_type not in allowed_cost_types:
            _log_unknown_cost_type(cost_type)
            continue
        found_cost = True
        raw_cost = statistic.get("costInclTax")
        if not isinstance(raw_cost, dict):
            incomplete_cost = True
            continue
        cost = require_mapping(raw_cost, "measurement cost")
        amount = cost.get("estimatedAmount")
        if amount is None or amount == "":
            incomplete_cost = True
            continue
        cents = _finite_decimal(amount, "measurement cost")
        cost_cents += abs(cents) if generation else cents
    return None if not found_cost or incomplete_cost else cost_cents


def _log_unknown_cost_type(cost_type: object) -> None:
    """Log, once and by name only, a type that is neither a cost nor a credit."""
    if isinstance(cost_type, str) and cost_type in _KNOWN_COST_TYPES:
        return
    name = (
        cost_type
        if isinstance(cost_type, str) and _COST_TYPE_PATTERN.fullmatch(cost_type)
        else _UNNAMED_COST_TYPE
    )
    if name in _logged_cost_types:
        return
    _logged_cost_types.add(name)
    _LOGGER.debug("Left out a measurement statistic of an unknown type: %s", name)


def _finite_decimal(value: Any, context: str) -> Decimal:
    """Parse one finite upstream decimal value, smaller than 1E+13 either way."""
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError) as err:
        raise ValueError(f"Invalid {context}") from err
    if not parsed.is_finite():
        raise ValueError(f"{context.capitalize()} is not finite")
    if parsed.adjusted() > _LARGEST_EXPONENT:
        raise ValueError(f"{context.capitalize()} is out of range")
    return parsed


def parse_datetime(value: Any, context: str) -> datetime:
    """Parse a timezone-aware upstream timestamp."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing {context}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        # The standard message quotes the string, or names its year and month,
        # and a failed account discovery logs the traceback: keep the value out.
        raise ValueError(f"Invalid timestamp for {context}") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Naive timestamp for {context}")
    return parsed


def required_string(payload: dict[str, Any], key: str) -> str:
    """Return a required non-empty string."""
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing {key}")
    return value


def optional_string(value: Any) -> str | None:
    """Return a non-empty string or None."""
    return value if isinstance(value, str) and value else None


def optional_date(value: Any) -> date | None:
    """Parse an optional ISO local date."""
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("Invalid Meridian billing date")
    try:
        return date.fromisoformat(value)
    except ValueError:
        # Not chained: the standard message would put the date in a traceback.
        raise ValueError("Invalid Meridian billing date") from None


def optional_local_date(value: Any, zone: tzinfo) -> date | None:
    """
    Parse an optional plain date, or an aware instant as its date in ``zone``.

    Meridian sends register dates as UTC instants of New Zealand midnight
    (``2021-06-30T12:00:00+00:00`` is 1 July there), not as plain dates.
    """
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("Invalid Meridian register date")
    try:
        return date.fromisoformat(value)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        # Not chained: the standard message would put the date in a traceback.
        raise ValueError("Invalid Meridian register date") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Naive Meridian register date")
    return parsed.astimezone(zone).date()
