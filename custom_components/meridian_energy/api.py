"""Async client for Meridian Energy's current customer application API."""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout

from .const import (
    AUTH_EMAIL_URL,
    AUTH_OTP_URL,
    BRAND,
    CLIENT_PLATFORM,
    COST_STATISTIC_TYPES,
    DEFAULT_RETRY_AFTER_SECONDS,
    FIREBASE_API_KEY,
    FIREBASE_CUSTOM_TOKEN_URL,
    FIREBASE_REFRESH_URL,
    GENERATION_CREDIT_TYPES,
    GRAPHQL_URL,
    MAX_RETRY_AFTER_SECONDS,
    MIN_RETRY_AFTER_SECONDS,
    PAGE_SIZE,
    READING_FREQUENCY_HOUR,
    READING_QUALITY_COMBINED,
    REDIRECT_URL,
    REQUEST_TIMEOUT_SECONDS,
)
from .models import (
    MeasurementPage,
    MeridianAccount,
    MeridianBillingPeriod,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianProperty,
    MeridianTokenSet,
    require_list,
    require_mapping,
)

_LOGGER = logging.getLogger(__name__)
_LAST_DAY_OF_LONG_MONTH = 31
_HTTP_BAD_REQUEST = 400
_HTTP_TOO_MANY_REQUESTS = 429

TokenUpdateCallback = Callable[[MeridianTokenSet], Awaitable[None]]


class MeridianError(Exception):
    """Base Meridian API error."""


class MeridianConnectionError(MeridianError):
    """The Meridian service could not be reached."""


class MeridianAuthenticationError(MeridianError):
    """The Meridian session is invalid and requires reauthentication."""


class MeridianRateLimitError(MeridianConnectionError):
    """Meridian asked the client to defer further requests."""

    def __init__(self, retry_after: float) -> None:
        super().__init__("Meridian rate limited the request")
        self.retry_after = retry_after


class MeridianOtpError(MeridianAuthenticationError):
    """Meridian rejected an emailed one-time code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class MeridianGraphQLError(MeridianError):
    """Meridian returned a GraphQL error."""

    def __init__(self, operation: str, codes: tuple[str, ...]) -> None:
        super().__init__(f"Meridian GraphQL operation {operation} failed")
        self.operation = operation
        self.codes = codes


class MeridianApiClient:
    """Client for Meridian's OTP, Firebase and GraphQL services."""

    def __init__(
        self,
        session: ClientSession,
        *,
        tokens: MeridianTokenSet | None = None,
        token_update_callback: TokenUpdateCallback | None = None,
    ) -> None:
        self._session = session
        self._tokens = tokens
        self._token_update_callback = token_update_callback
        self._timeout = ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

    @property
    def tokens(self) -> MeridianTokenSet | None:
        """Return the current in-memory token set."""
        return self._tokens

    async def async_send_otp(self, email: str, journey_id: str) -> None:
        """Ask Meridian to email a one-time login code."""
        payload = {
            "email": email,
            "brand": BRAND,
            "redirectUrl": REDIRECT_URL,
            "journeyId": journey_id,
            "otpEnabled": True,
        }
        response = await self._async_json_request(
            AUTH_EMAIL_URL,
            json=payload,
            headers={"X-Client-Platform": CLIENT_PLATFORM},
            authenticated=False,
        )
        if response.get("success") is not True:
            raise MeridianAuthenticationError(
                "Meridian did not accept the login request"
            )

    async def async_validate_otp(
        self, email: str, otp: str, journey_id: str
    ) -> MeridianTokenSet:
        """Exchange an emailed code for a renewable Firebase session."""
        try:
            response = await self._async_json_request(
                AUTH_OTP_URL,
                json={
                    "email": email,
                    "otp": otp,
                    "brand": BRAND,
                    "journeyId": journey_id,
                },
                headers={"X-Client-Platform": CLIENT_PLATFORM},
                authenticated=False,
            )
        except _MeridianHttpError as err:
            code = str(err.payload.get("code") or "OTP_INVALID")
            message = str(
                err.payload.get("error") or "Meridian rejected the login code"
            )
            raise MeridianOtpError(code, message) from err

        custom_token = response.get("customToken")
        if not isinstance(custom_token, str) or not custom_token:
            raise MeridianAuthenticationError(
                "Meridian returned no authentication token"
            )

        firebase = await self._async_json_request(
            f"{FIREBASE_CUSTOM_TOKEN_URL}?key={FIREBASE_API_KEY}",
            json={"token": custom_token, "returnSecureToken": True},
            authenticated=False,
        )
        tokens = _parse_firebase_tokens(firebase)
        await self._async_set_tokens(tokens)
        return tokens

    async def async_refresh_tokens(self, *, force: bool = False) -> MeridianTokenSet:
        """Refresh the Firebase ID token when it is close to expiry."""
        if self._tokens is None:
            raise MeridianAuthenticationError("No Meridian session is available")
        if not force and self._tokens.expires_at > datetime.now(UTC) + timedelta(
            minutes=5
        ):
            return self._tokens

        try:
            response = await self._async_json_request(
                f"{FIREBASE_REFRESH_URL}?key={FIREBASE_API_KEY}",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._tokens.refresh_token,
                },
                authenticated=False,
            )
        except _MeridianHttpError as err:
            if err.status == _HTTP_TOO_MANY_REQUESTS:
                raise MeridianRateLimitError(err.retry_after) from err
            if err.status in {400, 401, 403}:
                raise MeridianAuthenticationError(
                    "Meridian session refresh was rejected"
                ) from err
            raise

        normalized = {
            "idToken": response.get("id_token"),
            "refreshToken": response.get("refresh_token"),
            "expiresIn": response.get("expires_in"),
            "localId": response.get("user_id"),
        }
        tokens = _parse_firebase_tokens(normalized)
        await self._async_set_tokens(tokens)
        return tokens

    async def async_get_accounts(self) -> tuple[MeridianAccount, ...]:
        """Return active Meridian accounts and their electricity properties."""
        data = await self._async_graphql(
            "accountsList",
            _ACCOUNTS_QUERY,
            {"allowedBrandCodes": ["MERIDIAN_ENERGY"]},
        )
        viewer = require_mapping(data.get("viewer"), "viewer")
        raw_accounts = require_list(viewer.get("accounts"), "viewer.accounts")

        accounts: list[MeridianAccount] = []
        for raw_account_value in raw_accounts:
            raw_account = require_mapping(raw_account_value, "account")
            number = _required_string(raw_account, "number")
            status = _required_string(raw_account, "status")
            if status not in {"ACTIVE", "DORMANT"}:
                continue
            details = await self._async_get_account(number)
            accounts.append(details)
        return tuple(accounts)

    async def _async_get_account(self, account_number: str) -> MeridianAccount:
        data = await self._async_graphql(
            "account",
            _ACCOUNT_QUERY,
            {"accountNumber": account_number, "activeFrom": "1970-01-01T00:00:00Z"},
        )
        account = require_mapping(data.get("account"), "account")
        properties: list[MeridianProperty] = []
        for raw_property_value in require_list(
            account.get("properties"), "account.properties"
        ):
            raw_property = require_mapping(raw_property_value, "property")
            meter_points: list[MeridianMeterPoint] = []
            for raw_meter_value in require_list(
                raw_property.get("meterPoints") or [], "property.meterPoints"
            ):
                if raw_meter_value is None:
                    continue
                raw_meter = require_mapping(raw_meter_value, "meter point")
                registers = require_list(
                    raw_meter.get("registers") or [], "meter registers"
                )
                meter_points.append(
                    MeridianMeterPoint(
                        id=_required_string(raw_meter, "id"),
                        market_identifier=_required_string(
                            raw_meter, "marketIdentifier"
                        ),
                        has_feed_in=any(
                            bool(require_mapping(register, "register").get("isFeedIn"))
                            for register in registers
                        ),
                    )
                )
            properties.append(
                MeridianProperty(
                    id=_required_string(raw_property, "id"),
                    address=_required_string(raw_property, "address"),
                    meter_points=tuple(meter_points),
                )
            )
        return MeridianAccount(
            number=_required_string(account, "number"),
            status=_required_string(account, "status"),
            properties=tuple(properties),
        )

    async def async_get_measurements(
        self,
        *,
        account_number: str,
        property_id: str,
        direction: str,
        end_on: str,
        before: str | None = None,
        page_size: int = PAGE_SIZE,
    ) -> MeasurementPage:
        """Return a backwards page of hourly measurements."""
        variables: dict[str, Any] = {
            "accountNumber": account_number,
            "propertyId": property_id,
            "last": page_size,
            "readingFrequencyType": READING_FREQUENCY_HOUR,
            "readingDirectionType": direction,
            "readingQualityType": READING_QUALITY_COMBINED,
        }
        if before is None:
            variables["endOn"] = end_on
        else:
            variables["before"] = before

        data = await self._async_graphql("measurements", _MEASUREMENTS_QUERY, variables)
        account = require_mapping(data.get("account"), "measurement account")
        property_data = require_mapping(account.get("property"), "measurement property")
        connection = require_mapping(property_data.get("measurements"), "measurements")
        page_info = require_mapping(connection.get("pageInfo"), "measurements.pageInfo")

        measurements: list[MeridianMeasurement] = []
        for edge_value in require_list(connection.get("edges"), "measurements.edges"):
            edge = require_mapping(edge_value, "measurement edge")
            node = require_mapping(edge.get("node"), "measurement node")
            measurements.append(_parse_measurement(node, direction))

        return MeasurementPage(
            measurements=tuple(measurements),
            has_previous_page=bool(page_info.get("hasPreviousPage")),
            start_cursor=_optional_string(page_info.get("startCursor")),
        )

    async def async_get_billing_period(
        self, account_number: str
    ) -> MeridianBillingPeriod:
        """Return the retailer-defined current billing period for an account."""
        data = await self._async_graphql(
            "billingPeriods",
            _BILLING_PERIODS_QUERY,
            {"accountNumber": account_number},
        )
        account = require_mapping(data.get("account"), "billing account")
        options = require_mapping(account.get("billingOptions"), "billing options")
        period_length = _optional_string(options.get("periodLength"))
        if period_length not in {None, "MONTHLY", "QUARTERLY"}:
            raise ValueError("Unsupported Meridian billing period length")
        multiplier = options.get("periodLengthMultiplier")
        if multiplier is not None and (
            not isinstance(multiplier, int)
            or isinstance(multiplier, bool)
            or multiplier <= 0
        ):
            raise ValueError("Invalid Meridian billing period multiplier")
        start_day = options.get("periodStartDay")
        if start_day is not None and (
            not isinstance(start_day, int)
            or isinstance(start_day, bool)
            or not 1 <= start_day <= _LAST_DAY_OF_LONG_MONTH
        ):
            raise ValueError("Invalid Meridian billing period start day")
        is_fixed = options.get("isFixed")
        if not isinstance(is_fixed, bool):
            raise ValueError("Invalid Meridian fixed billing-period flag")
        return MeridianBillingPeriod(
            period_length=period_length,
            period_length_multiplier=multiplier,
            is_fixed=is_fixed,
            start=_optional_date(options.get("currentBillingPeriodStartDate")),
            end=_optional_date(options.get("currentBillingPeriodEndDate")),
            next_billing_date=_optional_date(options.get("nextBillingDate")),
            period_start_day=start_day,
        )

    async def _async_graphql(
        self, operation: str, query: str, variables: dict[str, Any]
    ) -> dict[str, Any]:
        response: dict[str, Any] | None = None
        for attempt in range(2):
            tokens = await self.async_refresh_tokens(force=attempt == 1)
            try:
                response = await self._async_json_request(
                    GRAPHQL_URL,
                    json={
                        "operationName": operation,
                        "query": query,
                        "variables": variables,
                    },
                    headers={"Authorization": tokens.id_token},
                    authenticated=True,
                )
                break
            except _MeridianHttpError as err:
                if err.status == _HTTP_TOO_MANY_REQUESTS:
                    raise MeridianRateLimitError(err.retry_after) from err
                if err.status in {401, 403} and attempt == 1:
                    raise MeridianAuthenticationError(
                        "Meridian rejected the authenticated session"
                    ) from err
                if err.status not in {401, 403}:
                    raise

        if response is None:
            raise MeridianAuthenticationError("Meridian authentication retry failed")

        errors = response.get("errors")
        if errors:
            error_list = require_list(errors, "GraphQL errors")
            codes = tuple(
                _graphql_error_code(require_mapping(error, "GraphQL error"))
                for error in error_list
            )
            if any(code in _AUTH_GRAPHQL_CODES for code in codes):
                raise MeridianAuthenticationError(
                    "Meridian rejected the authenticated session"
                )
            raise MeridianGraphQLError(operation, codes)
        return require_mapping(response.get("data"), f"GraphQL {operation} data")

    async def _async_set_tokens(self, tokens: MeridianTokenSet) -> None:
        self._tokens = tokens
        if self._token_update_callback is not None:
            await self._token_update_callback(tokens)

    async def _async_json_request(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        authenticated: bool,
    ) -> dict[str, Any]:
        del authenticated  # Documents intent at each call site; never log the request.
        try:
            async with self._session.post(
                url,
                json=json,
                data=data,
                headers=headers,
                timeout=self._timeout,
            ) as response:
                response_headers = getattr(response, "headers", {})
                retry_after = _parse_retry_after(response_headers.get("Retry-After"))
                try:
                    payload = await response.json(content_type=None)
                except (ValueError, TypeError) as err:
                    if response.status >= _HTTP_BAD_REQUEST:
                        raise _MeridianHttpError(
                            response.status, {}, retry_after
                        ) from err
                    raise MeridianConnectionError(
                        "Meridian returned an unreadable response"
                    ) from err
                parsed = require_mapping(payload, "HTTP response")
                if response.status >= _HTTP_BAD_REQUEST:
                    raise _MeridianHttpError(response.status, parsed, retry_after)
                return parsed
        except _MeridianHttpError:
            raise
        except (ClientError, TimeoutError) as err:
            _LOGGER.debug("Meridian request failed: %s", type(err).__name__)
            raise MeridianConnectionError("Unable to reach Meridian") from err


class _MeridianHttpError(MeridianError):
    def __init__(
        self,
        status: int,
        payload: dict[str, Any],
        retry_after: float = DEFAULT_RETRY_AFTER_SECONDS,
    ) -> None:
        super().__init__(f"Meridian returned HTTP {status}")
        self.status = status
        self.payload = payload
        self.retry_after = retry_after


def _parse_retry_after(value: str | None) -> float:
    """Return a bounded Retry-After delay from seconds or an HTTP date."""
    delay = float(DEFAULT_RETRY_AFTER_SECONDS)
    if value:
        try:
            delay = float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None or retry_at.utcoffset() is None:
                    raise ValueError
                delay = (retry_at.astimezone(UTC) - datetime.now(UTC)).total_seconds()
            except TypeError, ValueError, OverflowError:
                delay = float(DEFAULT_RETRY_AFTER_SECONDS)
    return min(
        float(MAX_RETRY_AFTER_SECONDS),
        max(float(MIN_RETRY_AFTER_SECONDS), delay),
    )


def _parse_firebase_tokens(payload: dict[str, Any]) -> MeridianTokenSet:
    id_token = _required_string(payload, "idToken")
    refresh_token = _required_string(payload, "refreshToken")
    user_id = _firebase_user_id(payload, id_token)
    raw_expires = payload.get("expiresIn")
    try:
        if not isinstance(raw_expires, (str, int)):
            raise TypeError
        expires_in = int(raw_expires)
    except (TypeError, ValueError) as err:
        raise MeridianAuthenticationError(
            "Firebase returned an invalid expiry"
        ) from err
    if expires_in <= 0:
        raise MeridianAuthenticationError("Firebase returned an invalid expiry")
    return MeridianTokenSet(
        id_token=id_token,
        refresh_token=refresh_token,
        expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
        user_id=user_id,
    )


def _firebase_user_id(payload: dict[str, Any], id_token: str) -> str:
    """Return the Firebase UID from a legacy field or the issued ID token."""
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
        raise MeridianAuthenticationError(
            "Firebase returned no authenticated user identifier"
        ) from err
    return subject


def _parse_measurement(
    node: dict[str, Any], expected_direction: str
) -> MeridianMeasurement:
    start_value = node.get("startAt") or node.get("readAt")
    start = _parse_datetime(start_value, "measurement start")
    end_value = node.get("endAt")
    end = _parse_datetime(end_value, "measurement end") if end_value else None
    try:
        value = Decimal(str(node.get("value")))
    except (InvalidOperation, TypeError) as err:
        raise ValueError("Invalid measurement value") from err
    if value < 0:
        raise ValueError("Negative electricity measurement")

    metadata = require_mapping(node.get("metaData"), "measurement metadata")
    filters = require_mapping(metadata.get("utilityFilters"), "measurement filters")
    direction = _required_string(filters, "readingDirection")
    if direction != expected_direction:
        raise ValueError("Unexpected measurement direction")
    quality = _required_string(filters, "readingQuality")
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


def _parse_datetime(value: Any, context: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing {context}")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Naive timestamp for {context}")
    return parsed


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing {key}")
    return value


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_date(value: Any) -> date | None:
    if value in {None, ""}:
        return None
    if not isinstance(value, str):
        raise ValueError("Invalid Meridian billing date")
    try:
        return date.fromisoformat(value)
    except ValueError as err:
        raise ValueError("Invalid Meridian billing date") from err


def _graphql_error_code(error: dict[str, Any]) -> str:
    extensions = error.get("extensions")
    if not isinstance(extensions, dict):
        return "UNKNOWN"
    return str(extensions.get("errorCode") or "UNKNOWN")


_AUTH_GRAPHQL_CODES = frozenset(
    {"KT-CT-1111", "KT-CT-1112", "KT-CT-1120", "KT-CT-1124", "KT-CT-1143"}
)

_ACCOUNTS_QUERY = """
query accountsList($allowedBrandCodes: [BrandChoices]) {
  viewer {
    accounts(allowedBrandCodes: $allowedBrandCodes) {
      number
      status
      ... on AccountType { id }
    }
  }
}
"""

_ACCOUNT_QUERY = """
query account($accountNumber: String!, $activeFrom: DateTime) {
  account(accountNumber: $accountNumber) {
    number
    status
    properties(activeFrom: $activeFrom) {
      id
      address
      meterPoints {
        id
        marketIdentifier
        registers { identifier activeFrom activeTo isFeedIn }
      }
    }
  }
}
"""

_MEASUREMENTS_QUERY = """
fragment MeasurementFields on MeasurementConnection {
  pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
  edges {
    node {
      source
      value
      unit
      readAt
      ... on IntervalMeasurementType { startAt endAt }
      metaData {
        utilityFilters {
          ... on ElectricityFiltersOutput {
            readingFrequencyType
            readingDirection
            registerId
            deviceId
            marketSupplyPointId
            readingQuality
          }
        }
        statistics { type costInclTax { estimatedAmount } }
      }
    }
  }
}
query measurements(
  $accountNumber: String!
  $propertyId: ID!
  $before: String
  $last: Int
  $endOn: Date
  $readingFrequencyType: ReadingFrequencyType!
  $readingDirectionType: ReadingDirectionType
  $readingQualityType: ReadingQualityType
) {
  account(accountNumber: $accountNumber) {
    id
    property(id: $propertyId) {
      id
      measurements(
        before: $before
        last: $last
        endOn: $endOn
        timezone: "Pacific/Auckland"
        utilityFilters: [{ electricityFilters: {
          readingDirection: $readingDirectionType
          readingQuality: $readingQualityType
          readingFrequencyType: $readingFrequencyType
        }}]
      ) { ... on MeasurementConnection { ...MeasurementFields } }
    }
  }
}
"""

_BILLING_PERIODS_QUERY = """
query billingPeriods($accountNumber: String!) {
  account(accountNumber: $accountNumber) {
    billingOptions {
      periodLength
      periodLengthMultiplier
      isFixed
      currentBillingPeriodStartDate
      currentBillingPeriodEndDate
      nextBillingDate
      periodStartDay
    }
  }
}
"""
