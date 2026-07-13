"""Tests for the Meridian API client."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import ClientError

from custom_components.meridian_energy.api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianGraphQLError,
    MeridianOtpError,
    _graphql_error_code,
    _MeridianHttpError,
    _optional_string,
    _parse_datetime,
    _parse_firebase_tokens,
    _parse_measurement,
    _required_string,
)
from custom_components.meridian_energy.models import (
    MeridianTokenSet,
    require_list,
    require_mapping,
)


def _tokens() -> MeridianTokenSet:
    return MeridianTokenSet(
        id_token="id-token",
        refresh_token="refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        user_id="user-id",
    )


class _FakeResponse:
    def __init__(self, payload, *, status: int = 200, error: Exception | None = None):
        self.payload = payload
        self.status = status
        self.error = error

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
async def test_refresh_propagates_server_error() -> None:
    expired = MeridianTokenSet(
        "id", "refresh", datetime.now(UTC) - timedelta(seconds=1), "uid"
    )
    client = MeridianApiClient(MagicMock(), tokens=expired)
    client._async_json_request = AsyncMock(
        side_effect=_MeridianHttpError(500, {"error": "redacted"})
    )
    with pytest.raises(_MeridianHttpError):
        await client.async_refresh_tokens()


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
async def test_get_measurements_builds_first_and_cursor_queries() -> None:
    node = {
        "value": "1",
        "startAt": "2026-07-13T01:00:00+12:00",
        "readAt": "2026-07-13T01:00:00+12:00",
        "metaData": {
            "utilityFilters": {
                "readingDirection": "CONSUMPTION",
                "readingQuality": "ACTUAL",
                "marketSupplyPointId": "synthetic",
            },
            "statistics": [],
        },
    }
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

    assert first.measurements[0].channel_id == "synthetic"
    assert first.has_previous_page is True
    assert first.start_cursor == "cursor"
    assert client._async_graphql.await_args_list[0].args[2]["endOn"] == "2026-07-13"
    assert client._async_graphql.await_args_list[1].args[2]["before"] == "cursor"
    assert "endOn" not in client._async_graphql.await_args_list[1].args[2]


def test_parse_consumption_measurement_filters_cost_types() -> None:
    measurement = _parse_measurement(
        {
            "value": "1.25",
            "startAt": "2026-07-13T01:00:00+12:00",
            "endAt": "2026-07-13T02:00:00+12:00",
            "readAt": "2026-07-13T01:00:00+12:00",
            "metaData": {
                "utilityFilters": {
                    "readingDirection": "CONSUMPTION",
                    "readingQuality": "ACTUAL",
                },
                "statistics": [
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
            },
        },
        "CONSUMPTION",
    )

    assert str(measurement.value_kwh) == "1.25"
    assert str(measurement.cost_cents) == "45.5"
    assert measurement.start.utcoffset() == timedelta(hours=12)


def test_parse_measurement_rejects_negative_energy() -> None:
    with pytest.raises(ValueError, match="Negative"):
        _parse_measurement(
            {
                "value": "-1",
                "startAt": "2026-07-13T01:00:00+12:00",
                "readAt": "2026-07-13T01:00:00+12:00",
                "metaData": {
                    "utilityFilters": {
                        "readingDirection": "CONSUMPTION",
                        "readingQuality": "ACTUAL",
                    },
                    "statistics": [],
                },
            },
            "CONSUMPTION",
        )


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
async def test_graphql_propagates_non_auth_http_error() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_json_request = AsyncMock(side_effect=_MeridianHttpError(500, {}))
    with pytest.raises(_MeridianHttpError):
        await client._async_graphql("operation", "query", {})


@pytest.mark.asyncio
async def test_graphql_auth_code_requires_reauth() -> None:
    client = MeridianApiClient(MagicMock(), tokens=_tokens())
    client._async_json_request = AsyncMock(
        return_value={"errors": [{"extensions": {"errorCode": "KT-CT-1111"}}]}
    )
    with pytest.raises(MeridianAuthenticationError):
        await client._async_graphql("operation", "query", {})


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


@pytest.mark.parametrize("expires", [None, "not-a-number", "0", -1])
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


def test_parse_measurement_defensive_validation() -> None:
    base = {
        "value": "invalid",
        "startAt": "2026-07-13T01:00:00+12:00",
        "metaData": {
            "utilityFilters": {
                "readingDirection": "CONSUMPTION",
                "readingQuality": "ACTUAL",
            },
            "statistics": [],
        },
    }
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
    assert _parse_measurement(missing_cost, "CONSUMPTION").cost_cents == 0


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
    with pytest.raises(ValueError, match="object"):
        require_mapping([], "test")
    with pytest.raises(ValueError, match="list"):
        require_list({}, "test")
