"""Async client for Meridian Energy's current customer application API."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from aiohttp import ClientSession

from .const import (
    AUTH_EMAIL_URL,
    AUTH_OTP_URL,
    BRAND,
    CLIENT_PLATFORM,
    FIREBASE_API_KEY,
    FIREBASE_CUSTOM_TOKEN_URL,
    FIREBASE_REFRESH_URL,
    GRAPHQL_URL,
    PAGE_SIZE,
    READING_FREQUENCY_HOUR,
    READING_QUALITY_COMBINED,
    REDIRECT_URL,
)
from .graphql import (
    ACCOUNT_QUERY as _ACCOUNT_QUERY,
)
from .graphql import (
    ACCOUNTS_QUERY as _ACCOUNTS_QUERY,
)
from .graphql import (
    AUTH_GRAPHQL_CODES as _AUTH_GRAPHQL_CODES,
)
from .graphql import (
    BILLING_PERIODS_QUERY as _BILLING_PERIODS_QUERY,
)
from .graphql import (
    MEASUREMENTS_QUERY as _MEASUREMENTS_QUERY,
)
from .graphql import (
    graphql_error_code as _graphql_error_code,
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
from .parsers import (
    TokenParseError,
    parse_firebase_tokens,
)
from .parsers import (
    optional_date as _optional_date,
)
from .parsers import (
    optional_string as _optional_string,
)
from .parsers import (
    parse_datetime as _parse_datetime,
)
from .parsers import (
    parse_measurement as _parse_measurement,
)
from .parsers import (
    required_string as _required_string,
)
from .transport import (
    MeridianHttpError as _MeridianHttpError,
)
from .transport import (
    MeridianTransport,
    MeridianTransportError,
)
from .transport import (
    parse_retry_after as _parse_retry_after,
)

__all__ = [
    "MeridianApiClient",
    "MeridianAuthenticationError",
    "MeridianConnectionError",
    "MeridianError",
    "MeridianGraphQLError",
    "MeridianOtpError",
    "MeridianRateLimitError",
    "_MeridianHttpError",
    "_graphql_error_code",
    "_optional_string",
    "_parse_datetime",
    "_parse_firebase_tokens",
    "_parse_measurement",
    "_parse_retry_after",
    "_required_string",
]

_LAST_DAY_OF_LONG_MONTH = 31
_HTTP_REQUEST_TIMEOUT = 408
_HTTP_TOO_MANY_REQUESTS = 429
_HTTP_SERVER_ERROR = 500
_LOGGER = logging.getLogger(__name__)
_NZ = ZoneInfo("Pacific/Auckland")

TokenUpdateCallback = Callable[[MeridianTokenSet], Awaitable[None]]


def _is_active_feed_in_register(register: dict[str, Any], today: date) -> bool:
    """Return whether a feed-in register is active on the supplied local date."""
    is_feed_in = register.get("isFeedIn")
    if not isinstance(is_feed_in, bool):
        raise ValueError("Invalid feed-in register flag")
    if not is_feed_in:
        return False
    active_from = _optional_date(register.get("activeFrom"))
    active_to = _optional_date(register.get("activeTo"))
    return (active_from is None or active_from <= today) and (
        active_to is None or active_to >= today
    )


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


def _parse_firebase_tokens(payload: dict[str, Any]) -> MeridianTokenSet:
    """Translate strict token parsing failures into an authentication error."""
    try:
        return parse_firebase_tokens(payload)
    except TokenParseError as err:
        raise MeridianAuthenticationError(str(err)) from err


class MeridianApiClient:
    """Client for Meridian's OTP, Firebase and GraphQL services."""

    def __init__(
        self,
        session: ClientSession,
        *,
        tokens: MeridianTokenSet | None = None,
        token_update_callback: TokenUpdateCallback | None = None,
    ) -> None:
        self._transport = MeridianTransport(session)
        self._tokens = tokens
        self._token_update_callback = token_update_callback

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
        try:
            response = await self._async_json_request(
                AUTH_EMAIL_URL,
                json=payload,
                headers={"X-Client-Platform": CLIENT_PLATFORM},
                authenticated=False,
            )
        except _MeridianHttpError as err:
            if err.status == _HTTP_TOO_MANY_REQUESTS:
                raise MeridianRateLimitError(err.retry_after) from err
            if err.status == _HTTP_REQUEST_TIMEOUT or err.status >= _HTTP_SERVER_ERROR:
                raise _connection_error(err) from err
            raise MeridianAuthenticationError(
                "Meridian did not accept the login request"
            ) from err
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
            if err.status == _HTTP_TOO_MANY_REQUESTS:
                raise MeridianRateLimitError(err.retry_after) from err
            if err.status == _HTTP_REQUEST_TIMEOUT or err.status >= _HTTP_SERVER_ERROR:
                raise _connection_error(err) from err
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

        try:
            firebase = await self._async_json_request(
                f"{FIREBASE_CUSTOM_TOKEN_URL}?key={FIREBASE_API_KEY}",
                json={"token": custom_token, "returnSecureToken": True},
                authenticated=False,
            )
        except _MeridianHttpError as err:
            if err.status == _HTTP_TOO_MANY_REQUESTS:
                raise MeridianRateLimitError(err.retry_after) from err
            if err.status == _HTTP_REQUEST_TIMEOUT or err.status >= _HTTP_SERVER_ERROR:
                raise _connection_error(err) from err
            raise MeridianAuthenticationError(
                "Meridian authentication exchange was rejected"
            ) from err
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
            raise _connection_error(err) from err

        normalized = {
            "idToken": response.get("id_token"),
            "refreshToken": response.get("refresh_token"),
            "expiresIn": response.get("expires_in"),
            "localId": response.get("user_id"),
        }
        tokens = _parse_firebase_tokens(normalized)
        await self._async_set_tokens(tokens)
        _LOGGER.debug("Meridian session refreshed")
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
        today = datetime.now(_NZ).date()
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
                            _is_active_feed_in_register(
                                require_mapping(register, "register"),
                                today,
                            )
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
            except _MeridianHttpError as err:
                if err.status == _HTTP_TOO_MANY_REQUESTS:
                    raise MeridianRateLimitError(err.retry_after) from err
                if err.status in {401, 403} and attempt == 1:
                    raise MeridianAuthenticationError(
                        "Meridian rejected the authenticated session"
                    ) from err
                if err.status not in {401, 403}:
                    raise _connection_error(err) from err
                continue

            errors = response.get("errors")
            if errors:
                error_list = require_list(errors, "GraphQL errors")
                codes = tuple(
                    _graphql_error_code(require_mapping(error, "GraphQL error"))
                    for error in error_list
                )
                if any(code in _AUTH_GRAPHQL_CODES for code in codes):
                    if attempt == 0:
                        continue
                    raise MeridianAuthenticationError(
                        "Meridian rejected the authenticated session"
                    )
                _LOGGER.debug(
                    "Meridian GraphQL operation %s failed with codes %s",
                    operation,
                    ",".join(codes),
                )
                raise MeridianGraphQLError(operation, codes)
            return require_mapping(response.get("data"), f"GraphQL {operation} data")

        raise MeridianAuthenticationError("Meridian authentication retry failed")

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
            return await self._transport.async_json_request(
                url,
                json=json,
                data=data,
                headers=headers,
            )
        except _MeridianHttpError:
            raise
        except MeridianTransportError as err:
            raise MeridianConnectionError(str(err)) from err


def _connection_error(err: _MeridianHttpError) -> MeridianConnectionError:
    """Return a privacy-safe connection error for an upstream HTTP failure."""
    return MeridianConnectionError(f"Meridian service returned HTTP {err.status}")
