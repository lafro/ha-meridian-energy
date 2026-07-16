"""Tests for the Meridian API client."""

from __future__ import annotations

import base64
import json
from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import ClientError

from custom_components.meridian_energy.api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianGraphQLError,
    MeridianOtpError,
    MeridianRateLimitError,
    _graphql_error_code,
    _is_active_feed_in_register,
    _MeridianHttpError,
    _optional_string,
    _parse_datetime,
    _parse_firebase_tokens,
    _parse_measurement,
    _parse_retry_after,
    _required_string,
)
from custom_components.meridian_energy.models import (
    MeridianTokenSet,
    require_list,
    require_mapping,
)
from custom_components.meridian_energy.parsers import optional_date


def _tokens() -> MeridianTokenSet:
    return MeridianTokenSet(
        id_token="id-token",
        refresh_token="refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        user_id="user-id",
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
        error: Exception | None = None,
        headers=None,
    ):
        self.payload = payload
        self.status = status
        self.error = error
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self, *, content_type=None):
        del content_type
        if self.error:
            raise self.error
        return self.payload


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


@pytest.mark.asyncio
async def test_send_otp_rejected() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(return_value={"success": False})

    with pytest.raises(MeridianAuthenticationError):
        await client.async_send_otp("person@example.com", "journey")


@pytest.mark.asyncio
async def test_send_otp_maps_server_error_without_leaking_payload() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(503, {"secret": "must-not-leak"})
    )

    with pytest.raises(MeridianConnectionError) as raised:
        await client.async_send_otp("person@example.com", "journey")

    assert "must-not-leak" not in str(raised.value)


@pytest.mark.asyncio
async def test_send_otp_maps_request_timeout_to_connection_failure() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(408, {"error": "redacted"})
    )

    with pytest.raises(MeridianConnectionError):
        await client.async_send_otp("person@example.com", "journey")


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (400, MeridianAuthenticationError),
        (429, MeridianRateLimitError),
    ],
)
@pytest.mark.asyncio
async def test_send_otp_maps_client_errors(
    status: int, error_type: type[Exception]
) -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(status, {"secret": "must-not-leak"}, 123)
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
        side_effect=_MeridianHttpError(500, {"error": "redacted"})
    )

    with pytest.raises(MeridianConnectionError):
        await client.async_validate_otp("person@example.com", "123456", "journey")


@pytest.mark.asyncio
async def test_validate_otp_maps_request_timeout_to_connection_failure() -> None:
    client = MeridianApiClient(MagicMock())
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(408, {"error": "redacted"})
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


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (400, MeridianAuthenticationError),
        (408, MeridianConnectionError),
        (429, MeridianRateLimitError),
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
            _MeridianHttpError(status, {"secret": "must-not-leak"}),
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
        side_effect=_MeridianHttpError(status, {"error": "redacted"})
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
                                            "identifier": "EXPORT",
                                            "activeFrom": "2026-01-01",
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
        (
            {
                "isFeedIn": True,
                "activeFrom": "2026-07-16",
                "activeTo": "2026-07-16",
            },
            date(2026, 7, 16),
            True,
        ),
        (
            {"isFeedIn": True, "activeFrom": None, "activeTo": "2026-07-15"},
            date(2026, 7, 16),
            False,
        ),
    ],
)
def test_active_feed_in_register_contract(
    register: dict[str, object], today: date, expected: bool
) -> None:
    assert _is_active_feed_in_register(register, today) is expected


def test_active_feed_in_register_rejects_invalid_flag() -> None:
    with pytest.raises(ValueError, match="flag"):
        _is_active_feed_in_register({"isFeedIn": "yes"}, date(2026, 7, 16))


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
    account = await client._async_get_account("A-TEST")
    assert account.properties[0].meter_points == ()


@pytest.mark.asyncio
async def test_get_billing_period_parses_supported_metadata() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        return_value={
            "account": {
                "billingOptions": {
                    "periodLength": "MONTHLY",
                    "periodLengthMultiplier": 1,
                    "isFixed": True,
                    "currentBillingPeriodStartDate": "2026-07-20",
                    "currentBillingPeriodEndDate": "2026-08-19",
                    "nextBillingDate": "2026-08-20",
                    "periodStartDay": 20,
                }
            }
        }
    )

    result = await client.async_get_billing_period("A-TEST")

    assert result.start == date(2026, 7, 20)
    assert result.end == date(2026, 8, 19)
    assert result.next_billing_date == date(2026, 8, 20)
    assert result.period_length == "MONTHLY"
    assert result.period_length_multiplier == 1
    assert result.is_fixed is True
    assert result.period_start_day == 20
    assert client._async_graphql.await_args.args[0] == "billingPeriods"
    assert client._async_graphql.await_args.args[2] == {"accountNumber": "A-TEST"}


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("periodLength", "WEEKLY", "period length"),
        ("periodLengthMultiplier", 0, "multiplier"),
        ("periodLengthMultiplier", True, "multiplier"),
        ("periodStartDay", 0, "start day"),
        ("periodStartDay", True, "start day"),
        ("isFixed", "yes", "fixed"),
        ("currentBillingPeriodStartDate", "not-a-date", "date"),
    ],
)
@pytest.mark.asyncio
async def test_get_billing_period_rejects_invalid_metadata(
    field: str, value: object, message: str
) -> None:
    options = {
        "periodLength": "MONTHLY",
        "periodLengthMultiplier": 1,
        "isFixed": True,
        "currentBillingPeriodStartDate": "2026-07-20",
        "currentBillingPeriodEndDate": "2026-08-19",
        "nextBillingDate": None,
        "periodStartDay": 20,
    }
    options[field] = value
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_graphql = AsyncMock(
        return_value={"account": {"billingOptions": options}}
    )

    with pytest.raises(ValueError, match=message):
        await client.async_get_billing_period("A-TEST")


@pytest.mark.asyncio
async def test_get_measurements_builds_first_and_cursor_queries() -> None:
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

    first = await client.async_get_measurements(
        account_number="A-TEST",
        property_id="property",
        direction="CONSUMPTION",
        end_on="2026-07-13",
    )
    await client.async_get_measurements(
        account_number="A-TEST",
        property_id="property",
        direction="CONSUMPTION",
        end_on="ignored",
        before="cursor",
    )

    assert len(first.measurements[0].channel_id) == 64
    assert "synthetic" not in first.measurements[0].channel_id
    assert first.has_previous_page is True
    assert first.start_cursor == "cursor"
    assert client._async_graphql.await_args_list[0].args[2]["endOn"] == "2026-07-13"
    assert client._async_graphql.await_args_list[1].args[2]["before"] == "cursor"
    assert "endOn" not in client._async_graphql.await_args_list[1].args[2]


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


def test_parse_measurement_requires_register_identity() -> None:
    node = _measurement_node()
    metadata = node["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    filters["registerId"] = None
    with pytest.raises(ValueError, match="registerId"):
        _parse_measurement(node, "CONSUMPTION")


def test_parse_measurement_channel_identity_distinguishes_devices() -> None:
    first = _measurement_node()
    second = _measurement_node()
    metadata = second["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    filters["deviceId"] = "other-synthetic-device"

    first_channel = _parse_measurement(first, "CONSUMPTION").channel_id
    second_channel = _parse_measurement(second, "CONSUMPTION").channel_id

    assert first_channel != second_channel
    assert "synthetic" not in first_channel
    assert "synthetic" not in second_channel


@pytest.mark.parametrize("field", ["marketSupplyPointId", "deviceId", "registerId"])
def test_parse_measurement_requires_complete_channel_identity(field: str) -> None:
    node = _measurement_node()
    metadata = node["metaData"]
    assert isinstance(metadata, dict)
    filters = metadata["utilityFilters"]
    assert isinstance(filters, dict)
    filters[field] = None
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


@pytest.mark.asyncio
async def test_graphql_second_unauthorized_requires_reauth() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client.async_refresh_tokens = AsyncMock(return_value=_tokens())
    client._async_json_request = AsyncMock(
        side_effect=[_MeridianHttpError(401, {}), _MeridianHttpError(403, {})]
    )
    with pytest.raises(MeridianAuthenticationError):
        await client._async_graphql("operation", "query", {})


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
async def test_graphql_auth_code_requires_reauth() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client.async_refresh_tokens = AsyncMock(return_value=_tokens())
    client._async_json_request = AsyncMock(
        return_value={"errors": [{"extensions": {"errorCode": "KT-CT-1111"}}]}
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
        "https://example.test", authenticated=False
    ) == {"ok": True}

    client = MeridianApiClient(
        _FakeSession(_FakeResponse({"error": "safe"}, status=500))
    )
    with pytest.raises(_MeridianHttpError):
        await client._async_json_request("https://example.test", authenticated=False)


@pytest.mark.asyncio
async def test_json_request_preserves_rate_limit_header_on_non_json_error() -> None:
    client = MeridianApiClient(
        _FakeSession(
            _FakeResponse(
                None,
                status=429,
                error=ValueError(),
                headers={"Retry-After": "120"},
            )
        )
    )
    with pytest.raises(_MeridianHttpError) as raised:
        await client._async_json_request("https://example.test", authenticated=False)
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
        await client._async_json_request("https://example.test", authenticated=False)

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


@pytest.mark.parametrize("error", [ValueError(), TypeError()])
@pytest.mark.asyncio
async def test_json_request_rejects_unreadable_response(error: Exception) -> None:
    client = MeridianApiClient(_FakeSession(_FakeResponse(None, error=error)))
    with pytest.raises(Exception, match="unreadable"):
        await client._async_json_request("https://example.test", authenticated=False)


@pytest.mark.asyncio
async def test_json_request_maps_network_error() -> None:
    client = MeridianApiClient(_FakeSession(error=ClientError()))
    with pytest.raises(Exception, match="Unable to reach"):
        await client._async_json_request("https://example.test", authenticated=False)


@pytest.mark.parametrize(
    "expires", [None, "not-a-number", "0", -1, "999999999999999999999999"]
)
def test_parse_firebase_tokens_rejects_invalid_expiry(expires) -> None:
    with pytest.raises(MeridianAuthenticationError):
        _parse_firebase_tokens(
            {
                "idToken": "id",
                "refreshToken": "refresh",
                "localId": "uid",
                "expiresIn": expires,
            }
        )


def test_parse_firebase_tokens_derives_current_user_id_from_jwt() -> None:
    claims = base64.urlsafe_b64encode(json.dumps({"sub": "uid"}).encode()).decode()
    tokens = _parse_firebase_tokens(
        {
            "idToken": f"header.{claims.rstrip('=')}.signature",
            "refreshToken": "refresh",
            "expiresIn": "3600",
        }
    )
    assert tokens.user_id == "uid"


@pytest.mark.parametrize("id_token", ["invalid", "a.b.c"])
def test_parse_firebase_tokens_rejects_missing_user_id(id_token: str) -> None:
    with pytest.raises(MeridianAuthenticationError, match="user identifier"):
        _parse_firebase_tokens(
            {
                "idToken": id_token,
                "refreshToken": "refresh",
                "expiresIn": "3600",
            }
        )


def test_parse_firebase_tokens_rejects_non_mapping_claims() -> None:
    claims = base64.urlsafe_b64encode(json.dumps(["not", "claims"]).encode()).decode()
    with pytest.raises(MeridianAuthenticationError, match="user identifier"):
        _parse_firebase_tokens(
            {
                "idToken": f"header.{claims.rstrip('=')}.signature",
                "refreshToken": "refresh",
                "expiresIn": "3600",
            }
        )


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
