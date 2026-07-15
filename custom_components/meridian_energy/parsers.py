"""Strict parsers for Meridian authentication and measurement payloads."""

from __future__ import annotations

import base64
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from .const import COST_STATISTIC_TYPES, GENERATION_CREDIT_TYPES
from .models import MeridianMeasurement, MeridianTokenSet, require_list, require_mapping


class TokenParseError(ValueError):
    """A Firebase token response could not be parsed safely."""


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
    if expires_in <= 0:
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
    node: dict[str, Any], expected_direction: str
) -> MeridianMeasurement:
    """Parse one hourly electricity measurement."""
    start_value = node.get("startAt") or node.get("readAt")
    start = parse_datetime(start_value, "measurement start")
    end_value = node.get("endAt")
    end = parse_datetime(end_value, "measurement end") if end_value else None
    try:
        value = Decimal(str(node.get("value")))
    except (InvalidOperation, TypeError) as err:
        raise ValueError("Invalid measurement value") from err
    if value < 0:
        raise ValueError("Negative electricity measurement")

    metadata = require_mapping(node.get("metaData"), "measurement metadata")
    filters = require_mapping(metadata.get("utilityFilters"), "measurement filters")
    direction = required_string(filters, "readingDirection")
    if direction != expected_direction:
        raise ValueError("Unexpected measurement direction")
    quality = required_string(filters, "readingQuality")
    channel_parts = [
        str(filters[key])
        for key in ("marketSupplyPointId", "deviceId", "registerId")
        if filters.get(key) not in {None, ""}
    ]
    allowed_cost_types = (
        GENERATION_CREDIT_TYPES if direction == "GENERATION" else COST_STATISTIC_TYPES
    )
    cost_cents = Decimal(0)
    found_cost = False
    incomplete_cost = False
    for statistic_value in require_list(
        metadata.get("statistics") or [], "measurement statistics"
    ):
        statistic = require_mapping(statistic_value, "measurement statistic")
        if statistic.get("type") not in allowed_cost_types:
            continue
        found_cost = True
        raw_cost = statistic.get("costInclTax")
        if not isinstance(raw_cost, dict):
            incomplete_cost = True
            continue
        cost = require_mapping(raw_cost, "measurement cost")
        amount = cost.get("estimatedAmount")
        if amount in {None, ""}:
            incomplete_cost = True
            continue
        try:
            cost_cents += abs(Decimal(str(amount)))
        except InvalidOperation as err:
            raise ValueError("Invalid measurement cost") from err

    return MeridianMeasurement(
        start=start,
        end=end,
        value_kwh=value,
        quality=quality,
        direction=direction,
        channel_id=":".join(channel_parts) or "aggregate",
        cost_cents=None if not found_cost or incomplete_cost else cost_cents,
    )


def parse_datetime(value: Any, context: str) -> datetime:
    """Parse a timezone-aware upstream timestamp."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing {context}")
    parsed = datetime.fromisoformat(value)
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
    if value in {None, ""}:
        return None
    if not isinstance(value, str):
        raise ValueError("Invalid Meridian billing date")
    try:
        return date.fromisoformat(value)
    except ValueError as err:
        raise ValueError("Invalid Meridian billing date") from err
