"""Tests for the Meridian API client."""

from __future__ import annotations

import base64
import json
import logging
import re
import traceback
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import ClientError

from custom_components.meridian_energy import parsers as parsers_module
from custom_components.meridian_energy.api import (
    InvertedPeriodError,
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianGraphQLError,
    MeridianOtpError,
    MeridianRateLimitError,
    MeridianRejectedError,
    _feed_in_state,
    _graphql_error_code,
    _MeridianHttpError,
    _optional_string,
    _parse_datetime,
    _parse_measurement,
    _parse_retry_after,
    _required_string,
    exception_chain,
)
from custom_components.meridian_energy.const import GRAPHQL_URL
from custom_components.meridian_energy.graphql import (
    BALANCE_NAME,
    BALANCE_QUERY,
    PRICES_NAME,
    PRICES_QUERY,
)
from custom_components.meridian_energy.models import (
    MeasurementPage,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianTokenSet,
    require_list,
    require_mapping,
)
from custom_components.meridian_energy.parsers import (
    TokenParseError,
    optional_date,
    optional_local_date,
    parse_firebase_tokens,
)

from .kraken_fake import NZ
from .meridian_http import FakeMeridianSession, balance_data, prices_data


def _tokens() -> MeridianTokenSet:
    return MeridianTokenSet(
        id_token="id-token",
        refresh_token="refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        user_id="user-id",
    )


def test_token_set_repr_hides_credentials() -> None:
    """A token set that reaches a log line or traceback shows no credentials."""
    tokens = MeridianTokenSet(
        id_token="synthetic-id-token-value",
        refresh_token="synthetic-refresh-token-value",
        expires_at=datetime(2026, 7, 13, tzinfo=UTC),
        user_id="synthetic-user-value",
    )

    text = repr(tokens)

    assert "synthetic-id-token-value" not in text
    assert "synthetic-refresh-token-value" not in text
    assert "synthetic-user-value" not in text
    assert text.startswith("MeridianTokenSet(")
    assert tokens == MeridianTokenSet(
        id_token="synthetic-id-token-value",
        refresh_token="synthetic-refresh-token-value",
        expires_at=datetime(2026, 7, 13, tzinfo=UTC),
        user_id="synthetic-user-value",
    )


def _measurement_node(
    *,
    value: str = "1",
    direction: str = "CONSUMPTION",
    quality: str = "ACTUAL",
    statistics: list[object] | None = None,
) -> dict[str, object]:
    """Return one complete synthetic hourly measurement payload."""
    return {
        "value": value,
        "unit": "kWh",
        "startAt": "2026-07-13T01:00:00+12:00",
        "endAt": "2026-07-13T02:00:00+12:00",
        "readAt": "2026-07-13T01:00:00+12:00",
        "metaData": {
            "utilityFilters": {
                "readingFrequencyType": "HOUR_INTERVAL",
                "readingDirection": direction,
                "readingQuality": quality,
                "marketSupplyPointId": "synthetic-market-point",
                "deviceId": "synthetic-device",
                "registerId": "synthetic-register",
            },
            "statistics": statistics or [],
        },
    }


class _FakeResponse:
    def __init__(
        self,
        payload,
        *,
        status: int = 200,
        body: bytes | None = None,
        headers=None,
    ):
        self.body = json.dumps(payload).encode() if body is None else body
        self.status = status
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def read(self):
        return self.body


class _FakeSession:
    def __init__(self, response=None, *, error: Exception | None = None):
        self.response = response
        self.error = error

    def post(self, *_args, **_kwargs):
        if self.error:
            raise self.error
        return self.response


@pytest.mark.asyncio
async def test_send_otp() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(return_value={"success": True})

    await client.async_send_otp("person@example.com", "journey")

    payload = client._async_json_request.await_args.kwargs["json"]
    assert payload["email"] == "person@example.com"
    assert payload["otpEnabled"] is True
    assert "password" not in payload
    assert client.tokens is None


@pytest.mark.parametrize("answer", [{"success": False}, {}, {"sent": "yes"}])
@pytest.mark.asyncio
async def test_any_success_status_counts_as_a_sent_code(answer: dict) -> None:
    """The body of a sent code's answer is not read, as in Meridian's app."""
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(return_value=answer)

    await client.async_send_otp("person@example.com", "journey")


@pytest.mark.asyncio
async def test_send_otp_maps_server_error_without_leaking_payload() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(503, {"secret": "must-not-leak"}, json_body=True)
    )

    with pytest.raises(MeridianConnectionError) as raised:
        await client.async_send_otp("person@example.com", "journey")

    assert "must-not-leak" not in str(raised.value)


@pytest.mark.asyncio
async def test_send_otp_maps_request_timeout_to_connection_failure() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(408, {"error": "redacted"}, json_body=True)
    )

    with pytest.raises(MeridianConnectionError):
        await client.async_send_otp("person@example.com", "journey")


@pytest.mark.parametrize(
    ("status", "html_body", "error_type"),
    [
        (404, False, MeridianAuthenticationError),
        (404, True, MeridianAuthenticationError),
        (403, False, MeridianAuthenticationError),
        (403, True, MeridianRejectedError),
        (400, False, MeridianRejectedError),
        (401, False, MeridianRejectedError),
        (422, False, MeridianRejectedError),
        (429, False, MeridianRateLimitError),
    ],
)
@pytest.mark.asyncio
async def test_send_otp_maps_client_errors(
    status: int, *, html_body: bool, error_type: type[Exception]
) -> None:
    """Only 404, or 403 unless a web page, refuses the email."""
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(
            status,
            {"secret": "must-not-leak"},
            123,
            json_body=not html_body,
            html_body=html_body,
        )
    )

    with pytest.raises(error_type) as raised:
        await client.async_send_otp("person@example.com", "journey")

    assert "must-not-leak" not in str(raised.value)


@pytest.mark.asyncio
async def test_validate_otp_exchanges_custom_token() -> None:
    callback = AsyncMock()
    client = MeridianApiClient(MagicMock(), token_update_callback=callback)
    client._async_json_request = AsyncMock(
        side_effect=[
            {"customToken": "custom"},
            {
                "idToken": "id",
                "refreshToken": "refresh",
                "expiresIn": "3600",
                "localId": "uid",
            },
        ]
    )

    tokens = await client.async_validate_otp("person@example.com", "123456", "journey")

    assert tokens.refresh_token == "refresh"
    assert tokens.user_id == "uid"
    callback.assert_awaited_once_with(tokens)


@pytest.mark.asyncio
async def test_validate_otp_maps_error_without_leaking_payload() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(
            404, {"code": "OTP_NOT_FOUND", "error": "OTP not found"}
        )
    )

    with pytest.raises(MeridianOtpError) as raised:
        await client.async_validate_otp("person@example.com", "123456", "journey")

    assert raised.value.code == "OTP_NOT_FOUND"
    assert str(raised.value) == "Meridian rejected the login code"


@pytest.mark.asyncio
async def test_validate_otp_maps_server_error_to_connection_failure() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(500, {"error": "redacted"}, json_body=True)
    )

    with pytest.raises(MeridianConnectionError):
        await client.async_validate_otp("person@example.com", "123456", "journey")


@pytest.mark.asyncio
async def test_validate_otp_maps_request_timeout_to_connection_failure() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(408, {"error": "redacted"}, json_body=True)
    )

    with pytest.raises(MeridianConnectionError):
        await client.async_validate_otp("person@example.com", "123456", "journey")


@pytest.mark.asyncio
async def test_validate_otp_maps_rate_limit() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(429, {}, retry_after=123)
    )

    with pytest.raises(MeridianRateLimitError) as raised:
        await client.async_validate_otp("person@example.com", "123456", "journey")

    assert raised.value.retry_after == 123


@pytest.mark.parametrize("status", [400, 404, 429, 503])
@pytest.mark.asyncio
async def test_validate_otp_reads_the_refused_codes_name_before_its_status(
    status: int,
) -> None:
    """
    A refused code names itself whatever the status, HTTP 429 included.

    Too many tries may come with 429: read as a rate limit, the form would
    tell the user to wait, when only a new code helps.
    """
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(
            status, {"code": "OTP_TOO_MANY_ATTEMPTS"}, retry_after=60
        )
    )

    with pytest.raises(MeridianOtpError) as raised:
        await client.async_validate_otp("person@example.com", "123456", "journey")

    assert raised.value.code == "OTP_TOO_MANY_ATTEMPTS"


@pytest.mark.parametrize(
    ("payload", "error_type"),
    [
        ({"code": "SOMETHING_ELSE"}, MeridianRateLimitError),
        ({"code": 7}, MeridianRateLimitError),
        ({}, MeridianRateLimitError),
    ],
)
@pytest.mark.asyncio
async def test_validate_otp_takes_a_429_without_a_code_name_as_a_rate_limit(
    payload: dict[str, object], error_type: type[Exception]
) -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(429, payload, retry_after=60)
    )

    with pytest.raises(error_type):
        await client.async_validate_otp("person@example.com", "123456", "journey")


@pytest.mark.asyncio
async def test_validate_otp_names_an_unknown_refusal_by_its_code() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(400, {"code": "SOMETHING_ELSE"}, json_body=True)
    )

    with pytest.raises(MeridianOtpError) as raised:
        await client.async_validate_otp("person@example.com", "123456", "journey")

    assert raised.value.code == "SOMETHING_ELSE"


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (400, MeridianAuthenticationError),
        (408, MeridianConnectionError),
        (429, MeridianRateLimitError),
        (500, MeridianConnectionError),
        (503, MeridianConnectionError),
    ],
)
@pytest.mark.asyncio
async def test_validate_otp_maps_custom_token_exchange_error(
    status: int, error_type: type[Exception]
) -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=[
            {"customToken": "custom"},
            _MeridianHttpError(status, {"secret": "must-not-leak"}, json_body=True),
        ]
    )

    with pytest.raises(error_type) as raised:
        await client.async_validate_otp("person@example.com", "123456", "journey")

    assert "must-not-leak" not in str(raised.value)


@pytest.mark.asyncio
async def test_validate_otp_requires_custom_token() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(return_value={})

    with pytest.raises(MeridianAuthenticationError):
        await client.async_validate_otp("person@example.com", "123456", "journey")


@pytest.mark.asyncio
async def test_refresh_tokens_rotates_refresh_token() -> None:
    expired = MeridianTokenSet(
        id_token="old-id",
        refresh_token="old-refresh",
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
        user_id="uid",
    )
    callback = AsyncMock()
    client = MeridianApiClient(
        MagicMock(), tokens=expired, token_update_callback=callback
    )
    client._async_json_request = AsyncMock(
        return_value={
            "id_token": "new-id",
            "refresh_token": "new-refresh",
            "expires_in": "3600",
            "user_id": "uid",
        }
    )

    result = await client.async_refresh_tokens()

    assert result.id_token == "new-id"
    assert result.refresh_token == "new-refresh"
    callback.assert_awaited_once()


@pytest.mark.asyncio
async def test_refresh_tokens_reuses_valid_token() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_json_request = AsyncMock()

    result = await client.async_refresh_tokens()

    assert result.id_token == "id-token"
    client._async_json_request.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_requires_existing_session() -> None:
    client = MeridianApiClient(MagicMock())
    with pytest.raises(MeridianAuthenticationError):
        await client.async_refresh_tokens()


@pytest.mark.parametrize("status", [400, 401, 403])
@pytest.mark.asyncio
async def test_refresh_rejected_requires_reauth(status: int) -> None:
    expired = MeridianTokenSet(
        "id", "refresh", datetime.now(UTC) - timedelta(seconds=1), "uid"
    )
    client = MeridianApiClient(MagicMock(), tokens=expired)
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(status, {"error": "redacted"}, json_body=True)
    )
    with pytest.raises(MeridianAuthenticationError):
        await client.async_refresh_tokens()


@pytest.mark.asyncio
async def test_refresh_maps_server_error_to_connection_failure() -> None:
    expired = MeridianTokenSet(
        "id", "refresh", datetime.now(UTC) - timedelta(seconds=1), "uid"
    )
    client = MeridianApiClient(MagicMock(), tokens=expired)
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(500, {"error": "redacted"})
    )
    with pytest.raises(MeridianConnectionError):
        await client.async_refresh_tokens()


@pytest.mark.asyncio
async def test_refresh_maps_request_timeout_to_connection_failure() -> None:
    expired = MeridianTokenSet(
        "id", "refresh", datetime.now(UTC) - timedelta(seconds=1), "uid"
    )
    client = MeridianApiClient(MagicMock(), tokens=expired)
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(408, {"error": "redacted"})
    )

    with pytest.raises(MeridianConnectionError):
        await client.async_refresh_tokens()


@pytest.mark.asyncio
async def test_refresh_propagates_rate_limit_delay() -> None:
    expired = MeridianTokenSet(
        "id", "refresh", datetime.now(UTC) - timedelta(seconds=1), "uid"
    )
    client = MeridianApiClient(MagicMock(), tokens=expired)
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(429, {}, retry_after=123)
    )
    with pytest.raises(MeridianRateLimitError) as raised:
        await client.async_refresh_tokens()
    assert raised.value.retry_after == 123


@pytest.mark.asyncio
async def test_get_accounts_parses_feed_in_register() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        side_effect=[
            {
                "viewer": {
                    "accounts": [
                        {"number": "A-TEST", "status": "ACTIVE", "id": "account"},
                        {"number": "A-PENDING", "status": "PENDING", "id": "pending"},
                    ]
                }
            },
            {
                "account": {
                    "number": "A-TEST",
                    "status": "ACTIVE",
                    "properties": [
                        {
                            "id": "property",
                            "address": "1 Example Street",
                            "meterPoints": [
                                {
                                    "id": "meter",
                                    "marketIdentifier": "0000000000TEST",
                                    "registers": [
                                        {
                                            # Meridian's form: the UTC instant
                                            # of New Zealand midnight.
                                            "identifier": "EXPORT",
                                            "activeFrom": "2025-12-31T11:00:00+00:00",
                                            "activeTo": None,
                                            "isFeedIn": True,
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            },
        ]
    )

    accounts = await client.async_get_accounts()

    assert len(accounts) == 1
    assert accounts[0].properties[0].meter_points[0].has_feed_in is True


@pytest.mark.asyncio
async def test_get_accounts_ignores_inactive_feed_in_registers() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        side_effect=[
            {
                "viewer": {
                    "accounts": [
                        {"number": "A-TEST", "status": "ACTIVE", "id": "account"}
                    ]
                }
            },
            {
                "account": {
                    "number": "A-TEST",
                    "status": "ACTIVE",
                    "properties": [
                        {
                            "id": "property",
                            "address": "Synthetic address",
                            "meterPoints": [
                                {
                                    "id": "meter",
                                    "marketIdentifier": "synthetic-market-point",
                                    "registers": [
                                        {
                                            "identifier": "EXPIRED",
                                            "activeFrom": "2020-01-01",
                                            "activeTo": "2020-12-31",
                                            "isFeedIn": True,
                                        },
                                        {
                                            "identifier": "FUTURE",
                                            "activeFrom": "2999-01-01",
                                            "activeTo": None,
                                            "isFeedIn": True,
                                        },
                                    ],
                                }
                            ],
                        }
                    ],
                }
            },
        ]
    )

    accounts = await client.async_get_accounts()

    assert accounts[0].properties[0].meter_points[0].has_feed_in is False


@pytest.mark.parametrize(
    ("register", "today", "expected"),
    [
        ({"isFeedIn": False}, date(2026, 7, 16), False),
        ({"isFeedIn": True}, date(2026, 7, 16), True),
        # Plain dates: activeFrom is inclusive, activeTo exclusive.
        (
            {"isFeedIn": True, "activeFrom": "2026-07-16", "activeTo": None},
            date(2026, 7, 16),
            True,
        ),
        (
            {"isFeedIn": True, "activeFrom": "2026-07-17", "activeTo": None},
            date(2026, 7, 16),
            False,
        ),
        (
            {"isFeedIn": True, "activeFrom": None, "activeTo": "2026-07-17"},
            date(2026, 7, 16),
            True,
        ),
        # A register ending today is no longer active.
        (
            {"isFeedIn": True, "activeFrom": "2026-01-01", "activeTo": "2026-07-16"},
            date(2026, 7, 16),
            False,
        ),
        # Meridian's form: UTC instants of New Zealand midnight, read as New
        # Zealand dates (12:00 UTC on 15 July is midnight on 16 July there).
        (
            {"isFeedIn": True, "activeFrom": "2026-07-15T12:00:00+00:00"},
            date(2026, 7, 16),
            True,
        ),
        (
            {"isFeedIn": True, "activeFrom": "2026-07-15T12:00:00+00:00"},
            date(2026, 7, 15),
            False,
        ),
        (
            {"isFeedIn": True, "activeFrom": "2026-07-16T00:00:00+12:00"},
            date(2026, 7, 16),
            True,
        ),
        (
            {"isFeedIn": True, "activeFrom": "2021-06-30T12:00:00Z"},
            date(2026, 7, 16),
            True,
        ),
        (
            {
                "isFeedIn": True,
                "activeFrom": "2021-06-30T12:00:00+00:00",
                "activeTo": "2026-07-15T12:00:00+00:00",
            },
            date(2026, 7, 16),
            False,
        ),
    ],
)
def test_active_feed_in_register_contract(
    register: dict[str, object], today: date, expected: bool
) -> None:
    assert _feed_in_state(register, today) is expected


@pytest.mark.parametrize(
    ("today", "expected"),
    [(date(2026, 7, 15), (True, False)), (date(2026, 7, 16), (False, True))],
)
def test_back_to_back_feed_in_registers_are_active_one_at_a_time(
    today: date, expected: tuple[bool, bool]
) -> None:
    # One register's activeTo is the next one's activeFrom: New Zealand
    # midnight starting 16 July.
    handover = "2026-07-15T12:00:00+00:00"
    earlier = {
        "isFeedIn": True,
        "activeFrom": "2021-06-30T12:00:00+00:00",
        "activeTo": handover,
    }
    later = {"isFeedIn": True, "activeFrom": handover, "activeTo": None}
    assert (
        _feed_in_state(earlier, today),
        _feed_in_state(later, today),
    ) == expected


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("2026-07-16T00:00:00", "Naive"),
        ("2026-13-45T00:00:00+00:00 for A-TEST-1", "Invalid"),
        (20260716, "Invalid"),
    ],
)
def test_feed_in_register_dates_are_strict_and_quote_nothing(
    value: object, message: str
) -> None:
    """A feed-in register's unreadable date is no feed-in, read strictly."""
    with pytest.raises(ValueError, match=message) as raised:
        optional_local_date(value, NZ)
    assert "A-TEST-1" not in "".join(traceback.format_exception(raised.value))
    assert "2026" not in str(raised.value)
    register = {"isFeedIn": True, "activeFrom": value}
    assert _feed_in_state(register, date(2026, 7, 16)) is None


@pytest.mark.parametrize(
    "register",
    [{"isFeedIn": "yes"}, {"isFeedIn": None}, {}, None, "register"],
    ids=["text", "null", "absent", "null register", "not an object"],
)
def test_a_feed_in_flag_that_is_not_true_or_false_cannot_be_read(
    register: object,
) -> None:
    assert _feed_in_state(register, date(2026, 7, 16)) is None


@pytest.mark.asyncio
async def test_get_account_skips_null_meter_point() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        return_value={
            "account": {
                "number": "A-TEST",
                "status": "DORMANT",
                "properties": [
                    {
                        "id": "property",
                        "address": "Synthetic address",
                        "meterPoints": [None],
                    }
                ],
            }
        }
    )
    account = await client._async_get_account("A-TEST", "DORMANT", 1)
    assert account.properties[0].meter_points == ()


def _register(*, is_feed_in: object = True) -> dict[str, object]:
    return {
        "identifier": "synthetic-register",
        "activeFrom": None,
        "activeTo": None,
        "isFeedIn": is_feed_in,
    }


def _property(**changes: object) -> dict[str, object]:
    return {
        "id": "synthetic-property",
        "address": "1 Synthetic Street",
        "meterPoints": [
            {
                "id": "synthetic-meter",
                "marketIdentifier": "0000000000TEST",
                "registers": [_register()],
            }
        ],
        **changes,
    }


def _answering(
    accounts: list[dict[str, object]], properties: list[object]
) -> AsyncMock:
    """Answer the account list, then each account with these properties."""

    async def answer(
        operation: str, _query: str, variables: dict[str, Any], **_kwargs: Any
    ) -> dict[str, Any]:
        if operation == "accountsList":
            return {"viewer": {"accounts": accounts}}
        return {
            "account": {
                "number": variables["accountNumber"],
                "status": None,
                "properties": properties,
            }
        }

    return AsyncMock(side_effect=answer)


@pytest.mark.asyncio
async def test_an_account_needs_no_status_address_meter_id_or_icp() -> None:
    """
    Only a property's ID and its registers are needed to follow it.

    The status comes from the account list; the rest is kept when given.
    """
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    bare_meter = {"id": None, "marketIdentifier": None, "registers": [_register()]}
    client._async_graphql = _answering(
        [{"number": "A-TEST", "status": "DORMANT", "id": "account"}],
        [_property(address=None, meterPoints=[bare_meter])],
    )

    (account,) = await client.async_get_accounts()

    assert account.status == "DORMANT"
    (prop,) = account.properties
    assert prop.address is None
    assert prop.meter_points == (
        MeridianMeterPoint(id=None, market_identifier=None, has_feed_in=True),
    )


@pytest.mark.asyncio
async def test_a_property_that_cannot_be_read_is_skipped_and_logged_by_place(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The others are followed; the line names its place, never its contents."""
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = _answering(
        [{"number": "A-TEST-1", "status": "ACTIVE", "id": "account"}],
        [
            _property(),
            _property(id=None, address="2 Synthetic Street"),
            "synthetic-property-3",
            _property(meterPoints=[{"id": "synthetic-meter", "registers": "none"}]),
        ],
    )

    for _update in range(2):
        (account,) = await client.async_get_accounts()
        assert [prop.id for prop in account.properties] == ["synthetic-property"]
        assert account.skipped_properties == client.skipped_properties == 3

    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.WARNING,
            (
                "Meridian Energy can't read Meridian's answer for property "
                f"{number} of 4 on account 1, as Meridian lists them"
            ),
        )
        for number in (2, 3, 4)
    ]
    for text in ("A-TEST-1", "Synthetic Street", "synthetic-property"):
        assert text not in caplog.text


@pytest.mark.parametrize(
    "unreadable",
    [_property(id=None), _property(meterPoints="synthetic-meter")],
    ids=["no id", "meter points not a list"],
)
@pytest.mark.asyncio
async def test_a_login_whose_only_property_cannot_be_read_is_unreadable_data(
    unreadable: dict[str, object],
) -> None:
    """
    Not a login with no property: nothing says Meridian lists none.

    So the flow asks again, and a sync keeps the list it has.
    """
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = _answering(
        [{"number": "A-TEST-1", "status": "ACTIVE", "id": "account"}], [unreadable]
    )

    with pytest.raises(ValueError, match="can't read any property"):
        await client.async_get_accounts()


@pytest.mark.asyncio
async def test_an_account_whose_only_property_cannot_be_read_is_still_listed() -> None:
    """Another account's property is followed; the count says one was skipped."""
    client = MeridianApiClient(MagicMock(), tokens=_tokens())

    async def answer(
        operation: str, _query: str, variables: dict[str, Any], **_kwargs: Any
    ) -> dict[str, Any]:
        if operation == "accountsList":
            return {
                "viewer": {
                    "accounts": [
                        {"number": "A-TEST-1", "status": "ACTIVE", "id": "1"},
                        {"number": "A-TEST-2", "status": "ACTIVE", "id": "2"},
                    ]
                }
            }
        number = variables["accountNumber"]
        prop = _property() if number == "A-TEST-1" else _property(id=None)
        return {"account": {"number": number, "properties": [prop]}}

    client._async_graphql = AsyncMock(side_effect=answer)

    first, second = await client.async_get_accounts()

    assert (first.number, len(first.properties), first.skipped_properties) == (
        "A-TEST-1",
        1,
        0,
    )
    assert (second.number, second.properties, second.skipped_properties) == (
        "A-TEST-2",
        (),
        1,
    )
    assert client.skipped_properties == 1


@pytest.mark.parametrize("flag", ["yes", None, 1], ids=["text", "null", "a number"])
@pytest.mark.asyncio
async def test_a_feed_in_flag_that_cannot_be_read_counts_as_none_and_is_noted(
    caplog: pytest.LogCaptureFixture, flag: object
) -> None:
    """
    The property says so, and the client logs nothing.

    The coordinator logs it, as it alone knows what was read before.
    """
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    meter = {"id": "synthetic-meter", "registers": [_register(is_feed_in=flag)]}
    client._async_graphql = _answering(
        [
            {"number": "A-TEST-1", "status": "ACTIVE", "id": "1"},
            {"number": "A-TEST-2", "status": "ACTIVE", "id": "2"},
        ],
        [_property(meterPoints=[meter])],
    )

    for _update in range(2):
        accounts = await client.async_get_accounts()
        assert [
            account.properties[0].meter_points[0].has_feed_in for account in accounts
        ] == [False, False]
        assert [account.properties[0].feed_in_unread for account in accounts] == [
            True,
            True,
        ]

    assert caplog.messages == []


@pytest.mark.asyncio
async def test_a_readable_feed_in_register_outweighs_an_unreadable_one(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Nothing is taken as not sending power: nothing is logged."""
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    meter = {
        "id": "synthetic-meter",
        "registers": [_register(is_feed_in="yes"), _register()],
    }
    client._async_graphql = _answering(
        [{"number": "A-TEST-1", "status": "ACTIVE", "id": "1"}],
        [_property(meterPoints=[meter])],
    )

    (account,) = await client.async_get_accounts()

    assert account.properties[0].meter_points[0].has_feed_in is True
    assert account.properties[0].feed_in_unread is False
    assert caplog.messages == []


@pytest.mark.asyncio
async def test_accounts_with_other_statuses_are_skipped_counted_and_logged_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Active and dormant accounts are followed; any other is counted.

    Each status is logged once, named only when it looks like an enum name,
    with no digits, so a status that carries text or a number can't put it
    in the log.
    """
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = _answering(
        [
            {"number": "A-TEST-1", "status": "ACTIVE", "id": "1"},
            {"number": "A-TEST-2", "status": "CLOSED", "id": "2"},
            {"number": "A-TEST-3", "status": "CLOSED", "id": "3"},
            {"number": "A-TEST-4", "status": "on hold for A-TEST-4", "id": "4"},
            {"number": "A-TEST-5", "status": "DORMANT", "id": "5"},
            {"number": "A-TEST-6", "status": "CLOSED for A-TEST-6", "id": "6"},
            {"number": "A-TEST-7", "status": "A12345678", "id": "7"},
        ],
        [_property()],
    )

    for _update in range(2):
        accounts = await client.async_get_accounts()
        assert [account.number for account in accounts] == ["A-TEST-1", "A-TEST-5"]
        assert client.skipped_accounts == 5

    follows = "; it follows active and dormant accounts"
    assert caplog.messages == [
        f"Meridian Energy doesn't follow an account whose status is CLOSED{follows}",
        (
            "Meridian Energy doesn't follow an account whose status is one it "
            f"doesn't show{follows}"
        ),
    ]
    for text in ("A-TEST-4", "A-TEST-6", "A12345678"):
        assert text not in caplog.text


def _billing_options(**changes: object) -> dict[str, object]:
    """Return billing options in the shape Meridian answers: a fixed monthly cycle."""
    return {
        "periodLength": "MONTHLY",
        "periodLengthMultiplier": None,
        "isFixed": True,
        "currentBillingPeriodStartDate": "2026-07-20",
        "currentBillingPeriodEndDate": "2026-08-19",
        "nextBillingDate": "2026-08-20",
        "periodStartDay": 20,
        **changes,
    }


@pytest.mark.asyncio
async def test_get_billing_period_parses_supported_metadata() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        return_value={"account": {"billingOptions": _billing_options()}}
    )

    result = await client.async_get_billing_period("A-TEST")

    assert result.start == date(2026, 7, 20)
    assert result.end == date(2026, 8, 19)
    assert result.next_billing_date == date(2026, 8, 20)
    assert result.period_length == "MONTHLY"
    assert result.period_length_multiplier is None
    assert result.is_fixed is True
    assert result.period_start_day == 20
    assert client._async_graphql.await_args.args[0] == "billingPeriods"
    assert client._async_graphql.await_args.args[2] == {"accountNumber": "A-TEST"}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("periodLength", "WEEKLY"),
        ("periodLength", {"name": "MONTHLY"}),
        ("periodLengthMultiplier", 0),
        ("periodLengthMultiplier", True),
        ("periodStartDay", 0),
        ("periodStartDay", 32),
        ("periodStartDay", True),
        ("isFixed", "yes"),
        ("nextBillingDate", "2026-13-45"),
        ("nextBillingDate", 20260820),
    ],
)
@pytest.mark.asyncio
async def test_unused_billing_fields_are_read_leniently(
    caplog: pytest.LogCaptureFixture, field: str, value: object
) -> None:
    """
    A field the bill figures don't use never refuses the period.

    It reads as None, and its name, never its value, is logged once at DEBUG.
    """
    caplog.set_level(logging.DEBUG)
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        return_value={"account": {"billingOptions": _billing_options(**{field: value})}}
    )

    for _ in range(2):
        result = await client.async_get_billing_period("A-TEST")

    assert (result.start, result.end) == (date(2026, 7, 20), date(2026, 8, 19))
    read = {
        "periodLength": result.period_length,
        "periodLengthMultiplier": result.period_length_multiplier,
        "periodStartDay": result.period_start_day,
        "isFixed": result.is_fixed,
        "nextBillingDate": result.next_billing_date,
    }
    assert read[field] is None
    assert [record.getMessage() for record in caplog.records] == [
        f"Meridian's billing period has an unreadable {field}"
    ]


@pytest.mark.parametrize(
    "field", ["currentBillingPeriodStartDate", "currentBillingPeriodEndDate"]
)
@pytest.mark.asyncio
async def test_the_billing_period_dates_are_read_strictly(field: str) -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        return_value={
            "account": {"billingOptions": _billing_options(**{field: "not-a-date"})}
        }
    )

    with pytest.raises(ValueError, match="date"):
        await client.async_get_billing_period("A-TEST")


@pytest.mark.asyncio
async def test_a_period_that_ends_before_it_starts_is_refused() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        return_value={
            "account": {
                "billingOptions": _billing_options(
                    currentBillingPeriodStartDate="2026-08-20",
                    currentBillingPeriodEndDate="2026-08-19",
                )
            }
        }
    )

    with pytest.raises(InvertedPeriodError) as refused:
        await client.async_get_billing_period("A-TEST")

    assert str(refused.value) == (
        "Meridian gave a billing period that ends before it starts"
    )


@pytest.mark.asyncio
async def test_every_measurements_request_has_both_bounds() -> None:
    """
    Every page sends the window's dates and size; a continuation adds only before.

    Until 0.3.0 a continuation page dropped ``endOn``, and no page had a start
    bound, so each request could make Meridian resolve the whole history.
    """
    node = _measurement_node()
    response = {
        "account": {
            "property": {
                "measurements": {
                    "edges": [{"node": node}],
                    "pageInfo": {
                        "hasPreviousPage": True,
                        "startCursor": "cursor",
                    },
                }
            }
        }
    }
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(return_value=response)
    window = {
        "account_number": "A-TEST",
        "property_id": "property",
        "direction": "CONSUMPTION",
        "start_on": date(2026, 7, 7),
        "end_on": date(2026, 7, 13),
        "last": 214,
    }

    first = await client.async_get_measurements(**window)
    await client.async_get_measurements(**window, before="cursor")

    assert len(first.measurements[0].channel_id) == 64
    assert "synthetic" not in first.measurements[0].channel_id
    assert first.has_previous_page is True
    assert first.start_cursor == "cursor"
    page_one, continuation = (
        call.args[2] for call in client._async_graphql.await_args_list
    )
    for variables in (page_one, continuation):
        assert variables["startOn"] == "2026-07-07"
        assert variables["endOn"] == "2026-07-13"
        assert variables["last"] == 214
    assert "before" not in page_one
    assert continuation == {**page_one, "before": "cursor"}
    for call in client._async_graphql.await_args_list:
        assert call.args[0] == "measurements"
        assert call.kwargs["operation_name"] == "GetMeasurements"
        assert call.kwargs["url"].endswith("?opName=GetMeasurements")


def test_parse_consumption_measurement_filters_cost_types() -> None:
    measurement = _parse_measurement(
        _measurement_node(
            value="1.25",
            statistics=[
                {
                    "type": "CONSUMPTION_COST",
                    "costInclTax": {"estimatedAmount": "37.5"},
                },
                {
                    "type": "STANDING_CHARGE_COST",
                    "costInclTax": {"estimatedAmount": "8.0"},
                },
                {
                    "type": "UNRELATED",
                    "costInclTax": {"estimatedAmount": "999"},
                },
            ],
        ),
        "CONSUMPTION",
    )

    assert str(measurement.value_kwh) == "1.25"
    assert str(measurement.cost_cents) == "45.5"
    assert measurement.start.utcoffset() == timedelta(hours=12)


def _cost(cost_type: object, amount: str) -> dict[str, object]:
    return {"type": cost_type, "costInclTax": {"estimatedAmount": amount}}


def test_parse_consumption_measurement_adds_every_charge_meridian_counts() -> None:
    """The charges Meridian's app adds to usage and the daily charge count too."""
    measurement = _parse_measurement(
        _measurement_node(
            statistics=[
                _cost("CONSUMPTION_COST", "37.5"),
                _cost("STANDING_CHARGE_COST", "8.0"),
                _cost("CAPACITY_CHARGE_COST", "1.25"),
                _cost("DEMAND_CHARGE_COST", "2.5"),
                _cost("DISTANCE_CHARGE_COST", "0.75"),
                _cost("ADDITIONAL_CHARGE_COST", "3"),
            ]
        ),
        "CONSUMPTION",
    )

    assert measurement.cost_cents == Decimal("53.0")


@pytest.mark.parametrize(
    ("credit", "expected"),
    [("-20", "25.5"), ("-60", "-14.5")],
    ids=["lowers_the_cost", "below_zero"],
)
def test_parse_consumption_measurement_subtracts_a_credit(
    credit: str, expected: str
) -> None:
    """A credit keeps its sign, as in Meridian's app, so it lowers the hour's cost."""
    measurement = _parse_measurement(
        _measurement_node(
            statistics=[
                _cost("CONSUMPTION_COST", "37.5"),
                _cost("STANDING_CHARGE_COST", "8.0"),
                _cost("ADDITIONAL_CREDIT_COST", credit),
            ]
        ),
        "CONSUMPTION",
    )

    assert measurement.cost_cents == Decimal(expected)


def test_parse_measurement_leaves_out_an_unknown_type_and_logs_only_its_name(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A type Meridian may add later is left out, and named once in the log.

    The name is logged only when it is a plain upper-case name, and no
    amount ever is. The export credit's type is known, so a usage reading
    that carries it leaves it out without a word.
    """
    monkeypatch.setattr(parsers_module, "_logged_cost_types", set())
    caplog.set_level(logging.DEBUG, logger=parsers_module.__name__)
    node = _measurement_node(
        statistics=[
            _cost("CONSUMPTION_COST", "37.5"),
            _cost("SYNTHETIC_NEW_COST", "123.45"),
            _cost("synthetic-label-0f3a", "678.9"),
            _cost({"nested": "synthetic"}, "246.8"),
            _cost("GENERATION_VALUE", "-5"),
        ]
    )

    first = _parse_measurement(node, "CONSUMPTION")
    second = _parse_measurement(node, "CONSUMPTION")

    assert first.cost_cents == second.cost_cents == Decimal("37.5")
    records = [r for r in caplog.records if r.name == parsers_module.__name__]
    assert [r.levelno for r in records] == [logging.DEBUG, logging.DEBUG]
    assert [r.getMessage().rsplit(": ", 1)[1] for r in records] == [
        "SYNTHETIC_NEW_COST",
        "UNNAMED",
    ]
    for leaked in ("123.45", "678.9", "246.8", "synthetic-label", "nested"):
        assert leaked not in caplog.text


def test_parse_generation_measurement_keeps_the_export_credit_positive(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Export credit counts as before; the usage cost types are left out of it."""
    monkeypatch.setattr(parsers_module, "_logged_cost_types", set())
    caplog.set_level(logging.DEBUG, logger=parsers_module.__name__)
    measurement = _parse_measurement(
        _measurement_node(
            direction="GENERATION",
            statistics=[
                _cost("GENERATION_VALUE", "-12.5"),
                _cost("CONSUMPTION_COST", "40"),
                _cost("ADDITIONAL_CREDIT_COST", "-3"),
            ],
        ),
        "GENERATION",
    )

    assert measurement.cost_cents == Decimal("12.5")
    assert not [r for r in caplog.records if r.name == parsers_module.__name__]


def test_parse_measurement_rejects_negative_energy() -> None:
    with pytest.raises(ValueError, match="Negative"):
        _parse_measurement(_measurement_node(value="-1"), "CONSUMPTION")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_parse_measurement_rejects_non_finite_energy(value: str) -> None:
    with pytest.raises(ValueError, match="finite"):
        _parse_measurement(_measurement_node(value=value), "CONSUMPTION")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_parse_measurement_rejects_non_finite_cost(value: str) -> None:
    node = _measurement_node(
        statistics=[
            {
                "type": "CONSUMPTION_COST",
                "costInclTax": {"estimatedAmount": value},
            }
        ]
    )
    with pytest.raises(ValueError, match="finite"):
        _parse_measurement(node, "CONSUMPTION")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("unit", "Wh", "unit"),
        ("endAt", None, "end"),
        ("endAt", "2026-07-13T02:30:00+12:00", "one hour"),
    ],
)
def test_parse_measurement_rejects_non_hourly_contract(
    field: str, value: object, message: str
) -> None:
    node = _measurement_node()
    node[field] = value
    with pytest.raises(ValueError, match=message):
        _parse_measurement(node, "CONSUMPTION")


@pytest.mark.parametrize("unit", ["kwh", "kWh", "KWH"])
def test_parse_measurement_accepts_kilowatt_hour_unit_case_variants(
    unit: str,
) -> None:
    """Kraken emits lower-case kwh while HA uses the canonical kWh symbol."""
    node = _measurement_node()
    node["unit"] = unit

    assert _parse_measurement(node, "CONSUMPTION").value_kwh == Decimal(1)


def test_parse_measurement_rejects_non_hourly_frequency() -> None:
    node = _measurement_node()
    metadata = node["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    filters["readingFrequencyType"] = "DAY_INTERVAL"
    with pytest.raises(ValueError, match="frequency"):
        _parse_measurement(node, "CONSUMPTION")


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2026-09-27T01:00:00+12:00", "2026-09-27T03:00:00+13:00"),
        ("2026-04-05T02:00:00+13:00", "2026-04-05T02:00:00+12:00"),
    ],
)
def test_parse_measurement_accepts_nz_dst_hour(start: str, end: str) -> None:
    node = _measurement_node()
    node["startAt"] = start
    node["readAt"] = start
    node["endAt"] = end

    measurement = _parse_measurement(node, "CONSUMPTION")

    assert measurement.end is not None
    assert measurement.end.astimezone(UTC) - measurement.start.astimezone(
        UTC
    ) == timedelta(hours=1)


def test_parse_measurement_classifies_supply_point_only_as_aggregate() -> None:
    node = _measurement_node()
    metadata = node["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    filters["deviceId"] = None
    filters["registerId"] = None

    measurement = _parse_measurement(node, "CONSUMPTION")

    assert measurement.channel_id == "aggregate"


def test_parse_measurement_accepts_register_identity_without_device() -> None:
    node = _measurement_node()
    metadata = node["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    filters["deviceId"] = None

    measurement = _parse_measurement(node, "CONSUMPTION")

    assert len(measurement.channel_id) == 64


def test_parse_measurement_distinguishes_devices_without_registers() -> None:
    first = _measurement_node()
    second = _measurement_node()
    first_metadata = first["metaData"]
    assert isinstance(first_metadata, dict)
    first_filters = first_metadata["utilityFilters"]
    assert isinstance(first_filters, dict)
    first_filters["registerId"] = None
    metadata = second["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    filters["registerId"] = None
    filters["deviceId"] = "other-synthetic-device"

    first_channel = _parse_measurement(first, "CONSUMPTION").channel_id
    second_channel = _parse_measurement(second, "CONSUMPTION").channel_id

    assert first_channel != second_channel
    assert "synthetic" not in first_channel
    assert "synthetic" not in second_channel


def test_parse_measurement_register_identity_ignores_optional_parent_fields() -> None:
    first = _measurement_node()
    second = _measurement_node()
    metadata = second["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    filters["marketSupplyPointId"] = None
    filters["deviceId"] = None

    first_measurement = _parse_measurement(first, "CONSUMPTION")
    second_measurement = _parse_measurement(second, "CONSUMPTION")

    assert first_measurement.channel_id == second_measurement.channel_id
    assert (
        len(
            {
                (measurement.start, measurement.channel_id)
                for measurement in (first_measurement, second_measurement)
            }
        )
        == 1
    )


def test_parse_measurement_device_identity_ignores_optional_supply_point() -> None:
    first = _measurement_node()
    second = _measurement_node()
    for node in (first, second):
        metadata = node["metaData"]
        assert isinstance(metadata, dict)
        filters = metadata["utilityFilters"]
        assert isinstance(filters, dict)
        filters["registerId"] = None
    second_metadata = second["metaData"]
    assert isinstance(second_metadata, dict)
    second_filters = second_metadata["utilityFilters"]
    assert isinstance(second_filters, dict)
    second_filters["marketSupplyPointId"] = None

    assert (
        _parse_measurement(first, "CONSUMPTION").channel_id
        == _parse_measurement(second, "CONSUMPTION").channel_id
    )


def test_parse_measurement_accepts_aggregate_channel_identity() -> None:
    node = _measurement_node()
    metadata = node["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    for field in ("marketSupplyPointId", "deviceId", "registerId"):
        filters[field] = None

    measurement = _parse_measurement(node, "CONSUMPTION")

    assert measurement.channel_id == "aggregate"


@pytest.mark.parametrize("field", ["marketSupplyPointId", "deviceId", "registerId"])
@pytest.mark.parametrize("value", [123, []])
def test_parse_measurement_rejects_malformed_channel_identity(
    field: str, value: object
) -> None:
    node = _measurement_node()
    metadata = node["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    filters[field] = value
    with pytest.raises(ValueError, match=field):
        _parse_measurement(node, "CONSUMPTION")


@pytest.mark.asyncio
async def test_graphql_error_exposes_only_operation_and_codes() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_json_request = AsyncMock(
        return_value={
            "errors": [
                {
                    "message": "Internal details",
                    "extensions": {"errorCode": "SAFE_CODE"},
                }
            ]
        }
    )

    with pytest.raises(MeridianGraphQLError) as raised:
        await client._async_graphql("accountsList", "query", {})

    assert raised.value.codes == ("SAFE_CODE",)
    assert "Internal details" not in str(raised.value)


@pytest.mark.asyncio
async def test_graphql_retries_once_after_unauthorized() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    refreshed = MeridianTokenSet(
        "new-id", "refresh", datetime.now(UTC) + timedelta(hours=1), "uid"
    )
    client.async_refresh_tokens = AsyncMock(side_effect=[_tokens(), refreshed])
    client._async_json_request = AsyncMock(
        side_effect=[_MeridianHttpError(401, {}), {"data": {"ok": True}}]
    )

    result = await client._async_graphql("operation", "query", {})

    assert result == {"ok": True}
    assert client.async_refresh_tokens.await_args_list[1].kwargs == {"force": True}
    assert client._async_json_request.await_args_list[1].kwargs["headers"] == {
        "Authorization": "new-id"
    }


def _coded(code: str) -> dict[str, Any]:
    return {"errors": [{"message": "A-TEST-1", "extensions": {"errorCode": code}}]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "json_body", "error"),
    [
        # A code that says the session has ended, after a renewal.
        (_coded("KT-CT-1111"), True, MeridianAuthenticationError),
        (_coded("KT-CT-1120"), True, MeridianAuthenticationError),
        (_coded("KT-CT-1124"), True, MeridianAuthenticationError),
        (_coded("KT-CT-1143"), True, MeridianAuthenticationError),
        # Any other 401 after renewal is Meridian refusing the request.
        ({}, True, MeridianRejectedError),
        (_coded("KT-CT-1112"), True, MeridianRejectedError),
        (_coded("KT-CT-4321"), True, MeridianRejectedError),
        ({}, False, MeridianRejectedError),
    ],
    ids=[
        "KT-CT-1111",
        "KT-CT-1120",
        "KT-CT-1124",
        "KT-CT-1143",
        "no code",
        "KT-CT-1112",
        "another code",
        "a page",
    ],
)
async def test_a_401_after_renewal_ends_the_sign_in_only_with_a_session_code(
    payload: dict[str, Any], *, json_body: bool, error: type[Exception]
) -> None:
    """A 401 is sent once more with a renewed session, as Meridian's app does."""
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client.async_refresh_tokens = AsyncMock(return_value=_tokens())
    client._async_json_request = AsyncMock(
        side_effect=[
            _MeridianHttpError(401, {}, json_body=True),
            _MeridianHttpError(401, payload, json_body=json_body),
        ]
    )
    with pytest.raises(error) as raised:
        await client._async_graphql("operation", "query", {})

    assert client.async_refresh_tokens.await_args_list[1].kwargs == {"force": True}
    assert "A-TEST-1" not in str(raised.value)


@pytest.mark.parametrize(
    "payload",
    [{}, _coded("KT-CT-1124")],
    ids=["no code", "a session code"],
)
@pytest.mark.asyncio
async def test_a_403_is_a_refused_request_never_sent_again(
    payload: dict[str, Any],
) -> None:
    """Meridian's app sends no request again after a 403, nor ends the sign-in."""
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client.async_refresh_tokens = AsyncMock(return_value=_tokens())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(403, payload, json_body=True)
    )
    with pytest.raises(MeridianRejectedError) as raised:
        await client._async_graphql("operation", "query", {})

    assert raised.value.status == 403
    client._async_json_request.assert_awaited_once()
    client.async_refresh_tokens.assert_awaited_once_with(force=False)


@pytest.mark.asyncio
async def test_a_missing_authorization_header_code_is_a_refused_request() -> None:
    """KT-CT-1112 does not end the sign-in: it is not sent again, nor renewed."""
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client.async_refresh_tokens = AsyncMock(return_value=_tokens())
    client._async_json_request = AsyncMock(return_value=_coded("KT-CT-1112"))

    with pytest.raises(MeridianGraphQLError) as raised:
        await client._async_graphql("operation", "query", {})

    assert raised.value.codes == ("KT-CT-1112",)
    client._async_json_request.assert_awaited_once()
    client.async_refresh_tokens.assert_awaited_once_with(force=False)


@pytest.mark.asyncio
async def test_graphql_maps_non_auth_http_error_to_connection_failure() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_json_request = AsyncMock(side_effect=_MeridianHttpError(500, {}))
    with pytest.raises(MeridianConnectionError):
        await client._async_graphql("operation", "query", {})


@pytest.mark.asyncio
async def test_graphql_propagates_rate_limit_delay() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(429, {}, retry_after=456)
    )
    with pytest.raises(MeridianRateLimitError) as raised:
        await client._async_graphql("operation", "query", {})
    assert raised.value.retry_after == 456


@pytest.mark.asyncio
async def test_kraken_rate_limit_code_is_a_rate_limit_of_an_hour() -> None:
    """KT-CT-1199 arrives in an HTTP 200 body; it stops the sync like a 429."""
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_json_request = AsyncMock(
        return_value={
            "errors": [
                {"message": "synthetic", "extensions": {"errorCode": "KT-CT-1199"}}
            ]
        }
    )

    with pytest.raises(MeridianRateLimitError) as raised:
        await client._async_graphql("measurements", "query", {})

    assert raised.value.retry_after == 3600
    client._async_json_request.assert_awaited_once()


@pytest.mark.parametrize("code", ["KT-CT-1188", "KT-CT-1189"])
@pytest.mark.asyncio
async def test_query_limit_codes_stay_graphql_errors_naming_the_code(
    code: str,
) -> None:
    """A query over Kraken's complexity or node limit says so, and is not resent."""
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_json_request = AsyncMock(
        return_value={
            "errors": [{"message": "A-TEST-1", "extensions": {"errorCode": code}}]
        }
    )

    with pytest.raises(MeridianGraphQLError) as raised:
        await client._async_graphql("measurements", "query", {})

    assert raised.value.codes == (code,)
    assert (
        str(raised.value) == f"Meridian GraphQL operation measurements failed ({code})"
    )
    assert "A-TEST-1" not in str(raised.value)
    client._async_json_request.assert_awaited_once()


@pytest.mark.parametrize(
    "code", ["KT-CT-1111", "KT-CT-1120", "KT-CT-1124", "KT-CT-1143"]
)
@pytest.mark.asyncio
async def test_graphql_auth_code_requires_reauth(code: str) -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client.async_refresh_tokens = AsyncMock(return_value=_tokens())
    client._async_json_request = AsyncMock(
        return_value={"errors": [{"extensions": {"errorCode": code}}]}
    )
    with pytest.raises(MeridianAuthenticationError):
        await client._async_graphql("operation", "query", {})

    assert client.async_refresh_tokens.await_args_list[1].kwargs == {"force": True}


@pytest.mark.asyncio
async def test_graphql_auth_code_recovers_after_forced_refresh() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client.async_refresh_tokens = AsyncMock(return_value=_tokens())
    client._async_json_request = AsyncMock(
        side_effect=[
            {"errors": [{"extensions": {"errorCode": "KT-CT-1111"}}]},
            {"data": {"ok": True}},
        ]
    )

    result = await client._async_graphql("operation", "query", {})

    assert result == {"ok": True}
    assert client.async_refresh_tokens.await_args_list[1].kwargs == {"force": True}


@pytest.mark.asyncio
async def test_json_request_success_and_http_error() -> None:
    client = MeridianApiClient(_FakeSession(_FakeResponse({"ok": True})))
    assert await client._async_json_request(
        "https://example.test", operation="test", authenticated=False
    ) == {"ok": True}

    client = MeridianApiClient(
        _FakeSession(_FakeResponse({"error": "safe"}, status=500))
    )
    with pytest.raises(_MeridianHttpError):
        await client._async_json_request(
            "https://example.test", operation="test", authenticated=False
        )


@pytest.mark.asyncio
async def test_json_request_preserves_rate_limit_header_on_non_json_error() -> None:
    client = MeridianApiClient(
        _FakeSession(
            _FakeResponse(
                None,
                status=429,
                body=b"<html>Too many requests</html>",
                headers={"Retry-After": "120"},
            )
        )
    )
    with pytest.raises(_MeridianHttpError) as raised:
        await client._async_json_request(
            "https://example.test", operation="test", authenticated=False
        )
    assert raised.value.status == 429
    assert raised.value.retry_after == 120


@pytest.mark.asyncio
async def test_json_request_preserves_rate_limit_header_on_non_object_error() -> None:
    client = MeridianApiClient(
        _FakeSession(
            _FakeResponse(
                ["unexpected"],
                status=429,
                headers={"Retry-After": "120"},
            )
        )
    )
    with pytest.raises(_MeridianHttpError) as raised:
        await client._async_json_request(
            "https://example.test", operation="test", authenticated=False
        )

    assert raised.value.status == 429
    assert raised.value.payload == {}
    assert raised.value.retry_after == 120


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("30", 60),
        ("120", 120),
        ("999999", 86400),
        ("invalid", 3600),
        (None, 3600),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 60),
        ("Wed, 21 Oct 2015 07:28:00", 3600),
    ],
)
def test_retry_after_is_parsed_and_clamped(value, expected: float) -> None:
    assert _parse_retry_after(value) == expected


@pytest.mark.parametrize(
    "body", [b"<html>maintenance</html>", b'{"data": ', b"\xff\xfe\xfd"]
)
@pytest.mark.asyncio
async def test_json_request_rejects_unreadable_response(body: bytes) -> None:
    client = MeridianApiClient(_FakeSession(_FakeResponse(None, body=body)))
    with pytest.raises(Exception, match="unreadable"):
        await client._async_json_request(
            "https://example.test", operation="test", authenticated=False
        )


@pytest.mark.asyncio
async def test_json_request_maps_network_error() -> None:
    client = MeridianApiClient(_FakeSession(error=ClientError()))
    with pytest.raises(Exception, match="Unable to reach"):
        await client._async_json_request(
            "https://example.test", operation="test", authenticated=False
        )


@pytest.mark.parametrize(
    "expires", [None, "not-a-number", "0", -1, "999999999999999999999999"]
)
def test_parse_firebase_tokens_rejects_invalid_expiry(expires) -> None:
    with pytest.raises(TokenParseError):
        parse_firebase_tokens(
            {
                "idToken": "id",
                "refreshToken": "refresh",
                "localId": "uid",
                "expiresIn": expires,
            }
        )


def test_parse_firebase_tokens_derives_current_user_id_from_jwt() -> None:
    claims = base64.urlsafe_b64encode(json.dumps({"sub": "uid"}).encode()).decode()
    tokens = parse_firebase_tokens(
        {
            "idToken": f"header.{claims.rstrip('=')}.signature",
            "refreshToken": "refresh",
            "expiresIn": "3600",
        }
    )
    assert tokens.user_id == "uid"


_NOT_CLAIMS = base64.urlsafe_b64encode(json.dumps(["not", "claims"]).encode()).decode()


@pytest.mark.parametrize(
    "id_token",
    ["invalid", "a.b.c", f"header.{_NOT_CLAIMS.rstrip('=')}.signature"],
    ids=["one part", "unreadable claims", "claims not a mapping"],
)
def test_a_session_without_a_readable_user_id_is_still_a_session(
    id_token: str,
) -> None:
    """
    The user ID is kept when it can be read, and is never needed.

    Nothing sends it to Meridian or Google, so a session whose token does
    not name its user is still a session.
    """
    tokens = parse_firebase_tokens(
        {"idToken": id_token, "refreshToken": "refresh", "expiresIn": "3600"}
    )

    assert tokens.id_token == id_token
    assert tokens.user_id is None


def test_parse_measurement_defensive_validation() -> None:
    base = _measurement_node(value="invalid")
    with pytest.raises(ValueError, match="value"):
        _parse_measurement(base, "CONSUMPTION")

    wrong_direction = {**base, "value": "1"}
    with pytest.raises(ValueError, match="direction"):
        _parse_measurement(wrong_direction, "GENERATION")

    invalid_cost = {
        **wrong_direction,
        "metaData": {
            **wrong_direction["metaData"],
            "statistics": [
                {
                    "type": "CONSUMPTION_COST",
                    "costInclTax": {"estimatedAmount": "invalid"},
                }
            ],
        },
    }
    with pytest.raises(ValueError, match="cost"):
        _parse_measurement(invalid_cost, "CONSUMPTION")

    missing_cost = {
        **wrong_direction,
        "metaData": {
            **wrong_direction["metaData"],
            "statistics": [
                {
                    "type": "CONSUMPTION_COST",
                    "costInclTax": {"estimatedAmount": None},
                }
            ],
        },
    }
    assert _parse_measurement(missing_cost, "CONSUMPTION").cost_cents is None

    non_mapping_cost = {
        **wrong_direction,
        "metaData": {
            **wrong_direction["metaData"],
            "statistics": [
                {"type": "CONSUMPTION_COST", "costInclTax": "not-an-object"}
            ],
        },
    }
    assert _parse_measurement(non_mapping_cost, "CONSUMPTION").cost_cents is None


@pytest.mark.parametrize(
    ("parse", "malformed"),
    [
        # Partials, not lambdas: a lambda's frame would show its source line,
        # and with it the literal, in the formatted traceback.
        (partial(_parse_datetime, context="measurement start"), "2026-07-16 noon"),
        (partial(_parse_datetime, context="measurement end"), "2026-02-30T01:00Z"),
        (optional_date, "2026-07-32"),
    ],
)
def test_malformed_dates_stay_out_of_tracebacks(
    parse: Callable[[str], object], malformed: str
) -> None:
    """
    A malformed date from Meridian fails parsing without repeating any of it.

    A failed setup import logs its traceback, and the standard library's own
    messages quote an unparseable string, or name the year and month of an
    impossible date.
    """
    with pytest.raises(ValueError, match="Invalid") as caught:
        parse(malformed)

    formatted = "".join(traceback.format_exception(caught.value))
    assert malformed not in formatted
    assert "2026" not in formatted


def test_parsing_helpers_reject_invalid_values() -> None:
    with pytest.raises(ValueError, match="Missing"):
        _parse_datetime(None, "time")
    with pytest.raises(ValueError, match="Naive"):
        _parse_datetime("2026-07-13T01:00:00", "time")
    with pytest.raises(ValueError, match="Missing"):
        _required_string({}, "key")
    assert _optional_string("value") == "value"
    assert _optional_string(1) is None
    assert _graphql_error_code({}) == "UNKNOWN"
    assert (
        _graphql_error_code({"extensions": {"errorCode": "KT-CT-1111"}}) == "KT-CT-1111"
    )
    assert (
        _graphql_error_code({"extensions": {"errorCode": "secret\nvalue"}}) == "UNKNOWN"
    )
    assert _graphql_error_code({"extensions": {"errorCode": "A" * 65}}) == "UNKNOWN"
    assert optional_date(None) is None
    with pytest.raises(ValueError, match="billing date"):
        optional_date(1)
    with pytest.raises(ValueError, match="billing date"):
        optional_date("not-a-date")
    with pytest.raises(ValueError, match="object"):
        require_mapping([], "test")
    with pytest.raises(ValueError, match="list"):
        require_list({}, "test")


def _raised(build: Callable[[], None]) -> BaseException:
    try:
        build()
    except Exception as err:  # noqa: BLE001 - the test inspects any error
        return err
    raise AssertionError("nothing was raised")


def test_exception_chain_follows_what_a_traceback_shows() -> None:
    """
    A context the code suppressed is not part of the chain, as in a traceback.

    json raises its decode error ``from None`` over an internal StopIteration,
    so following every context ended the chain there.
    """

    def unreadable() -> None:
        try:
            json.loads(b"<html>maintenance</html>")
        except ValueError as err:
            raise MeridianConnectionError("unreadable") from err

    def replaced() -> None:
        try:
            raise ValueError("internal")
        except ValueError:
            raise MeridianConnectionError("replaced") from None

    def during_handling() -> None:
        try:
            raise ValueError("first")
        except ValueError:
            raise MeridianConnectionError("second")  # noqa: B904 - the case tested

    chain = exception_chain(_raised(unreadable))
    assert [type(item) for item in chain] == [
        MeridianConnectionError,
        json.JSONDecodeError,
    ]
    assert [type(item) for item in exception_chain(_raised(replaced))] == [
        MeridianConnectionError
    ]
    assert [type(item) for item in exception_chain(_raised(during_handling))] == [
        MeridianConnectionError,
        ValueError,
    ]


def _page(rows: int, *, more: bool, cursor: str | None) -> MeasurementPage:
    start = datetime(2026, 7, 10, tzinfo=UTC)
    return MeasurementPage(
        measurements=tuple(
            MeridianMeasurement(
                start=start + timedelta(hours=offset),
                end=start + timedelta(hours=offset + 1),
                value_kwh=Decimal("0.5"),
                quality="ACTUAL",
                direction="CONSUMPTION",
                channel_id="synthetic-meter:synthetic-register",
                cost_cents=Decimal(15),
            )
            for offset in range(rows)
        ),
        has_previous_page=more,
        start_cursor=cursor,
    )


@pytest.mark.parametrize(
    ("pages", "message"),
    [
        ([_page(0, more=True, cursor="c1")], "an empty page with more to come"),
        ([_page(1, more=True, cursor=None)], "a page that does not move on"),
        (
            [_page(1, more=True, cursor=f"c{index}") for index in range(8)],
            "more pages than a window can hold",
        ),
    ],
)
@pytest.mark.asyncio
async def test_a_window_whose_pages_cannot_end_is_refused(
    pages: list[MeasurementPage], message: str
) -> None:
    """A window's pages must move on, end with an empty one, and number 8 at most."""
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client.async_get_measurements = AsyncMock(side_effect=pages)  # type: ignore[method-assign]
    pace = AsyncMock()

    with pytest.raises(ValueError, match=message):
        await client.async_get_window(
            account_number="A-TEST-1",
            property_id="synthetic-property",
            direction="CONSUMPTION",
            start_on=date(2026, 7, 10),
            end_on=date(2026, 7, 16),
            last=214,
            pace=pace,
            context="grid_import window=-6..0d",
        )

    calls = client.async_get_measurements.await_args_list
    # Each page's request is paced, a resend of it included.
    assert [call.kwargs["pace"] for call in calls] == [pace] * len(pages)
    contexts = [call.kwargs["context"] for call in calls]
    assert contexts == [
        f"grid_import window=-6..0d page={number}"
        for number in range(1, len(pages) + 1)
    ]


@pytest.mark.asyncio
async def test_a_window_without_pacing_or_context_reads_its_pages() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client.async_get_measurements = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            _page(2, more=True, cursor="c1"),
            _page(1, more=False, cursor=None),
        ]
    )

    rows = await client.async_get_window(
        account_number="A-TEST-1",
        property_id="synthetic-property",
        direction="CONSUMPTION",
        start_on=date(2026, 7, 10),
        end_on=date(2026, 7, 16),
        last=214,
    )

    assert len(rows) == 3
    assert [
        call.kwargs["before"] for call in client.async_get_measurements.await_args_list
    ] == [
        None,
        "c1",
    ]
    assert (
        client.async_get_measurements.await_args_list[0].kwargs["context"] == "page=1"
    )


@pytest.mark.asyncio
async def test_balance_and_prices_are_asked_as_the_web_app_asks() -> None:
    """Each goes to its own operation's URL, with no address and no file link."""
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        side_effect=[
            {"account": balance_data(date(2026, 7, 13))},
            {"account": {"property": prices_data()}},
        ]
    )

    balance = await client.async_get_balance("A-TEST-1")
    prices = await client.async_get_prices("A-TEST-1", "synthetic-property")

    assert balance.balance_cents == -4000
    assert prices is not None
    assert prices.daily_charge == Decimal(2)
    balance_call, prices_call = client._async_graphql.await_args_list
    assert balance_call.args == (
        "balance",
        BALANCE_QUERY,
        {"accountNumber": "A-TEST-1"},
    )
    assert prices_call.args == (
        "prices",
        PRICES_QUERY,
        {"accountNumber": "A-TEST-1", "propertyId": "synthetic-property"},
    )
    for call, name in ((balance_call, BALANCE_NAME), (prices_call, PRICES_NAME)):
        assert call.kwargs == {
            "operation_name": name,
            "url": f"{GRAPHQL_URL}?opName={name}",
        }
    for query in (BALANCE_QUERY, PRICES_QUERY):
        assert "temporaryUrl" not in query
        assert "address" not in query.lower()


def _names(query: str) -> Counter[str]:
    return Counter(re.findall(r"[A-Za-z_]+", query))


def test_the_balance_and_prices_documents_ask_only_for_the_fields_used() -> None:
    """Counted, not a set: an added field may share a name already there."""
    asked = {"query": 1, "accountNumber": 3, "String": 1, "account": 1}
    assert _names(BALANCE_QUERY) == Counter(
        {
            **asked,
            "GetAccountBalance": 1,
            "payments": 1,
            "first": 2,
            "edges": 2,
            "node": 2,
            "amount": 1,
            "paymentDate": 1,
            "status": 1,
            "bills": 1,
            "fromDate": 1,
            "toDate": 1,
            "issuedDate": 1,
            "balance": 1,
            "includeAllLedgers": 1,
            "true": 1,
        }
    )
    assert _names(PRICES_QUERY) == Counter(
        {
            **asked,
            "GetAccountPropertyAgreements": 1,
            "propertyId": 2,
            "ID": 1,
            "property": 1,
            "id": 1,
            "meterPoints": 1,
            "activeAgreement": 1,
            "displayName": 1,
            "rates": 1,
            "hasDiscount": 1,
            "formattedRateIncludingTax": 1,
            "formattedRateIncludingTaxIncludingDiscount": 1,
            "bandCategory": 1,
            "touBucketName": 1,
        }
    )


def test_codes_not_in_krakens_form_are_never_shown() -> None:
    """A code that could carry an identifier reads "an error code", once."""
    codes = ("A-TEST-1", "KT-CT-1234", "UNKNOWN", "synthetic-account", "KT-CT-1234")

    graphql = MeridianGraphQLError("balance", codes)
    rejected = MeridianRejectedError(400, codes)

    assert str(graphql) == (
        "Meridian GraphQL operation balance failed (an error code, KT-CT-1234)"
    )
    assert rejected.detail == "HTTP 400, an error code, KT-CT-1234"
    assert str(MeridianGraphQLError("prices", ("UNKNOWN",))) == (
        "Meridian GraphQL operation prices failed"
    )
    for text in (str(graphql), rejected.detail):
        assert "A-TEST-1" not in text
        assert "synthetic" not in text


@pytest.mark.asyncio
async def test_a_graphql_failure_logs_only_codes_in_krakens_form(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_json_request = AsyncMock(
        return_value={
            "errors": [
                {"message": "x", "extensions": {"errorCode": "A-TEST-1"}},
                {"message": "y", "extensions": {"errorCode": "KT-CT-4321"}},
            ]
        }
    )

    with pytest.raises(MeridianGraphQLError):
        await client._async_graphql("balance", "query", {})

    assert (
        "Meridian GraphQL operation balance failed with codes an error code,KT-CT-4321"
        in caplog.text
    )
    assert "A-TEST-1" not in caplog.text


@pytest.mark.asyncio
async def test_the_client_sends_only_these_operation_names_and_urls() -> None:
    """
    Pin every (URL, operation name) the client sends, across all its queries.

    The web app's names go to ``?opName=<name>``. Discovery and billing keep
    the integration's own names on the plain endpoint, as since 0.2.x; a new
    name, or a name moved to another URL, fails here.
    """
    session = FakeMeridianSession(datetime(2026, 7, 16, 0, 30, tzinfo=UTC))
    client = MeridianApiClient(session, tokens=_tokens())  # type: ignore[arg-type]

    (account,) = await client.async_get_accounts()
    (prop,) = account.properties
    await client.async_get_window(
        account_number=account.number,
        property_id=prop.id,
        direction="CONSUMPTION",
        start_on=date(2026, 7, 9),
        end_on=date(2026, 7, 15),
        last=214,
    )
    await client.async_get_billing_period(account.number)
    await client.async_get_balance(account.number)
    await client.async_get_prices(account.number, prop.id)

    plain = "https://api.meridianenergy.nz/v1/graphql/"
    assert plain == GRAPHQL_URL
    assert set(session.posts) == {
        (plain, "accountsList"),
        (plain, "account"),
        (plain, "billingPeriods"),
        (f"{plain}?opName=GetMeasurements", "GetMeasurements"),
        (f"{plain}?opName=GetAccountBalance", "GetAccountBalance"),
        (
            f"{plain}?opName=GetAccountPropertyAgreements",
            "GetAccountPropertyAgreements",
        ),
    }
