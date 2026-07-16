"""Strict parsers for Meridian authentication and measurement payloads."""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from .const import (
    COST_STATISTIC_TYPES,
    GENERATION_CREDIT_TYPES,
    READING_FREQUENCY_HOUR,
)
from .models import MeridianMeasurement, MeridianTokenSet, require_list, require_mapping


class TokenParseError(ValueError):
    """A Firebase token response could not be parsed safely."""


_MAX_TOKEN_LIFETIME_SECONDS = 86400


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
    node: dict[str, Any], expected_direction: str
) -> MeridianMeasurement:
    """Parse one hourly electricity measurement."""
    start = parse_datetime(node.get("startAt"), "measurement start")
    end = parse_datetime(node.get("endAt"), "measurement end")
    if end.astimezone(UTC) - start.astimezone(UTC) != timedelta(hours=1):
        raise ValueError("Measurement interval is not exactly one hour")
    if required_string(node, "unit") != "kWh":
        raise ValueError("Unexpected measurement unit")
    value = _finite_decimal(node.get("value"), "measurement value")
    if value < 0:
        raise ValueError("Negative electricity measurement")

    metadata = require_mapping(node.get("metaData"), "measurement metadata")
    filters = require_mapping(metadata.get("utilityFilters"), "measurement filters")
    direction = required_string(filters, "readingDirection")
    if direction != expected_direction:
        raise ValueError("Unexpected measurement direction")
    if required_string(filters, "readingFrequencyType") != READING_FREQUENCY_HOUR:
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


def _measurement_channel_id(filters: dict[str, Any]) -> str:
    """Return a stable, privacy-safe identity for one physical meter channel."""
    parts = [
        required_string(filters, key)
        for key in ("marketSupplyPointId", "deviceId", "registerId")
    ]
    canonical = json.dumps(parts, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _measurement_cost_cents(metadata: dict[str, Any], direction: str) -> Decimal | None:
    """Return a complete interval cost or credit, in cents."""
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
        cost_cents += abs(_finite_decimal(amount, "measurement cost"))
    return None if not found_cost or incomplete_cost else cost_cents


def _finite_decimal(value: Any, context: str) -> Decimal:
    """Parse one finite upstream decimal value."""
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError) as err:
        raise ValueError(f"Invalid {context}") from err
    if not parsed.is_finite():
        raise ValueError(f"{context.capitalize()} is not finite")
    return parsed


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
