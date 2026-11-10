"""Async client for Meridian Energy's current customer application API."""

from __future__ import annotations

import asyncio
import logging
import random
import re
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
    DEFAULT_RETRY_AFTER_SECONDS,
    FIREBASE_API_KEY,
    FIREBASE_CUSTOM_TOKEN_URL,
    FIREBASE_REFRESH_URL,
    GRAPHQL_URL,
    MAX_WINDOW_PAGES,
    MEASUREMENTS_TIMEOUT_SECONDS,
    READING_FREQUENCY_HOUR,
    READING_QUALITY_COMBINED,
    REDIRECT_URL,
    REQUEST_TIMEOUT_SECONDS,
    TRANSIENT_RESEND_DELAY_SECONDS,
    TRANSIENT_STATUSES,
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
    BALANCE_NAME,
    BALANCE_QUERY,
    PRICES_NAME,
    PRICES_QUERY,
    RATE_LIMIT_CODE,
    UNKNOWN_ERROR_CODE,
    measurements_operation,
    measurements_query,
    shown_codes,
    web_app_url,
)
from .graphql import (
    BILLING_PERIODS_QUERY as _BILLING_PERIODS_QUERY,
)
from .graphql import (
    graphql_error_code as _graphql_error_code,
)
from .models import (
    MeasurementPage,
    MeridianAccount,
    MeridianBalance,
    MeridianBillingPeriod,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianNoAgreement,
    MeridianPrices,
    MeridianProperty,
    MeridianTokenSet,
    require_list,
    require_mapping,
)
from .parsers import (
    TokenParseError,
    parse_balance,
    parse_firebase_tokens,
    parse_prices,
)
from .parsers import (
    optional_date as _optional_date,
)
from .parsers import (
    optional_local_date as _optional_local_date,
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
    AnswerNote,
    MeridianTimeoutError,
    MeridianTransport,
    MeridianTransportError,
    MeridianUnreadableResponseError,
)
from .transport import (
    MeridianHttpError as _MeridianHttpError,
)
from .transport import (
    parse_retry_after as _parse_retry_after,
)

__all__ = [
    "InvertedPeriodError",
    "MeridianApiClient",
    "MeridianAuthenticationError",
    "MeridianConnectionError",
    "MeridianError",
    "MeridianGraphQLError",
    "MeridianOtpError",
    "MeridianRateLimitError",
    "MeridianRejectedError",
    "MeridianSignInServiceError",
    "MeridianSignInServiceUnreadableError",
    "MeridianUpstreamError",
    "_MeridianHttpError",
    "_graphql_error_code",
    "_optional_string",
    "_parse_datetime",
    "_parse_measurement",
    "_parse_retry_after",
    "_required_string",
    "exception_chain",
    "http_status",
    "is_passing_failure",
    "is_unreadable",
    "log_rate_limit",
]

_LAST_DAY_OF_LONG_MONTH = 31
_MAX_CAUSE_DEPTH = 10
_HTTP_SUCCESS = range(200, 300)
_HTTP_CLIENT_ERROR = 400
_HTTP_UNAUTHORIZED = 401
_HTTP_FORBIDDEN = 403
_HTTP_NOT_FOUND = 404
_HTTP_REQUEST_TIMEOUT = 408
_HTTP_TOO_MANY_REQUESTS = 429
_HTTP_SERVER_ERROR = 500
# A query that failed in a way that may pass is sent once more.
_SENDS = 2
# The account statuses whose accounts are followed.
_FOLLOWED_STATUSES = frozenset({"ACTIVE", "DORMANT"})
# The billing period lengths known; any other is read as none.
_PERIOD_LENGTHS = frozenset({"MONTHLY", "QUARTERLY"})
# A plain upper-case name with no digits, as Kraken's enums are: only such
# text is logged.
_ENUM_NAME = re.compile(r"[A-Z][A-Z_]{0,31}")
# What a status is called in the log when it is not such a name.
_UNNAMED = "one it doesn't show"
# The service names a rate limit's INFO line gives.
_MERIDIAN = "Meridian"
_SIGN_IN_SERVICE = "The sign-in service"
# The codes Meridian gives a login code it refused.
_OTP_CODES = frozenset(
    {"OTP_EXPIRED", "OTP_INVALID", "OTP_NOT_FOUND", "OTP_TOO_MANY_ATTEMPTS"}
)
_LOGGER = logging.getLogger(__name__)
_NZ = ZoneInfo("Pacific/Auckland")
# The measurements query goes out under the web app's name and URL; logs and
# errors call it "measurements", the integration's own label for it.
_MEASUREMENTS = "measurements"
_MEASUREMENTS_NAME, _MEASUREMENTS_URL = measurements_operation(GRAPHQL_URL)
_MEASUREMENTS_QUERY = measurements_query(_MEASUREMENTS_NAME)

TokenUpdateCallback = Callable[[MeridianTokenSet], Awaitable[None]]


async def _async_wait(seconds: float) -> None:
    """Wait before a resend; a seam tests patch."""
    await asyncio.sleep(seconds)


def _feed_in_state(register: Any, today: date) -> bool | None:
    """
    Return whether a register is a feed-in register active on a local date.

    None when that can't be read: a register that is not an object, a flag
    that is not true or false, or a feed-in register's unreadable dates.
    ``activeTo`` is exclusive: a register's ``activeTo`` is the next one's
    ``activeFrom``.
    """
    if not isinstance(register, dict):
        return None
    is_feed_in = register.get("isFeedIn")
    if not isinstance(is_feed_in, bool):
        return None
    if not is_feed_in:
        return False
    try:
        active_from = _optional_local_date(register.get("activeFrom"), _NZ)
        active_to = _optional_local_date(register.get("activeTo"), _NZ)
    except ValueError:
        return None
    return (active_from is None or active_from <= today) and (
        active_to is None or today < active_to
    )


def _parse_property(raw: Any, today: date) -> MeridianProperty:
    """
    Return a property, noting when an unread register left it as not exporting.

    Only its ID, its meter points and their registers must be readable: the
    address, and each meter's ID and ICP, are kept when given but never
    needed. A ``ValueError`` when the property can't be followed.
    """
    raw_property = require_mapping(raw, "property")
    meter_points: list[MeridianMeterPoint] = []
    states: list[bool | None] = []
    for raw_meter_value in require_list(
        raw_property.get("meterPoints") or [], "property.meterPoints"
    ):
        if raw_meter_value is None:
            continue
        raw_meter = require_mapping(raw_meter_value, "meter point")
        meter_states = [
            _feed_in_state(register, today)
            for register in require_list(
                raw_meter.get("registers") or [], "meter registers"
            )
        ]
        states.extend(meter_states)
        meter_points.append(
            MeridianMeterPoint(
                id=_optional_string(raw_meter.get("id")),
                market_identifier=_optional_string(raw_meter.get("marketIdentifier")),
                has_feed_in=True in meter_states,
            )
        )
    return MeridianProperty(
        id=_required_string(raw_property, "id"),
        address=_optional_string(raw_property.get("address")),
        meter_points=tuple(meter_points),
        feed_in_unread=None in states and True not in states,
    )


def _whole(value: Any, low: int, high: int | None) -> bool:
    """Return whether a value is a whole number from ``low`` to ``high``, if any."""
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and low <= value
        and (high is None or value <= high)
    )


def _is_iso_date(value: Any) -> bool:
    """Return whether a value is a plain date, as ``2026-07-16``."""
    if not isinstance(value, str):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


class MeridianError(Exception):
    """Base Meridian API error."""


class MeridianConnectionError(MeridianError):
    """The Meridian service could not be reached."""


class MeridianUpstreamError(MeridianConnectionError):
    """
    Meridian's service, or a gateway in front of it, failed or did not answer.

    ``status`` is the HTTP status, or None when no answer came within the
    request's time limit.
    """

    def __init__(self, status: int | None) -> None:
        super().__init__(
            "Meridian did not answer in time"
            if status is None
            else f"Meridian service returned HTTP {status}"
        )
        self.status = status

    @property
    def detail(self) -> str:
        """Describe the failure without identifiers, as in ``HTTP 503``."""
        return "timed out" if self.status is None else f"HTTP {self.status}"


class MeridianSignInServiceError(MeridianConnectionError):
    """
    Google's sign-in service, which holds the session, failed or did not answer.

    ``status`` is its HTTP status, or None when no answer came: ``timed_out``
    then says whether the request ran out of time rather than failing on the
    way.
    """

    def __init__(self, status: int | None, *, timed_out: bool = False) -> None:
        super().__init__(
            "The sign-in service did not answer"
            if status is None
            else f"The sign-in service returned HTTP {status}"
        )
        self.status = status
        self.timed_out = timed_out

    @property
    def detail(self) -> str:
        """Describe the failure, as in ``sign-in service: HTTP 503``."""
        if self.status is not None:
            return f"sign-in service: HTTP {self.status}"
        return f"sign-in service: {'timed out' if self.timed_out else 'network error'}"


class MeridianSignInServiceUnreadableError(MeridianSignInServiceError):
    """
    Google's sign-in service sent an answer that can't be read.

    ``reason`` is ``not JSON`` or ``unexpected data``. It is raised from the
    error that read the answer, so a page that is not JSON stays
    ``is_unreadable``. Never an ended sign-in, and never sent again.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(None)
        self.args = (
            f"The sign-in service sent an answer that can't be read: {reason}",
        )
        self.reason = reason

    @property
    def detail(self) -> str:
        """Describe the answer, as in ``sign-in service: not JSON``."""
        return f"sign-in service: {self.reason}"


class MeridianRejectedError(MeridianConnectionError):
    """
    Meridian's service refused a request with a client error (HTTP 4xx).

    Not a timeout (408) or a rate limit (429), each handled on its own, and
    never a failure of Google's session service. ``codes`` are the validated
    Kraken error codes its body named, if any; never its messages.
    """

    def __init__(self, status: int, codes: tuple[str, ...] = ()) -> None:
        super().__init__(f"Meridian service returned HTTP {status}")
        self.status = status
        self.codes = codes

    @property
    def detail(self) -> str:
        """Describe the refusal without identifiers, as in ``HTTP 400, KT-CT-1234``."""
        return ", ".join((f"HTTP {self.status}", *shown_codes(self.codes)))


class MeridianAuthenticationError(MeridianError):
    """The Meridian session is invalid and requires reauthentication."""


class MeridianRateLimitError(MeridianConnectionError):
    """
    Meridian, or the sign-in service, asked the client to defer requests.

    Besides the wait, it says what the answer was, for a log line: its HTTP
    status, the code it named if any, whether it carried Retry-After, and
    whether the sign-in service sent it rather than Meridian.
    """

    def __init__(
        self,
        retry_after: float,
        *,
        answer_status: int | None = None,
        code: str | None = None,
        retry_after_given: bool = False,
        sign_in_service: bool = False,
    ) -> None:
        super().__init__("Meridian rate limited the request")
        self.retry_after = retry_after
        self.answer_status = answer_status
        self.code = code
        self.retry_after_given = retry_after_given
        self.sign_in_service = sign_in_service

    @property
    def service(self) -> str:
        """Return the service that asked to wait, as its log line names it."""
        return _SIGN_IN_SERVICE if self.sign_in_service else _MERIDIAN


class MeridianOtpError(MeridianAuthenticationError):
    """Meridian rejected an emailed one-time code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class MeridianGraphQLError(MeridianError):
    """
    Meridian returned a GraphQL error.

    The message names the operation and its upstream error codes as
    ``shown_codes`` shows them; the free-text messages that came with them
    are never kept.
    """

    def __init__(self, operation: str, codes: tuple[str, ...]) -> None:
        shown = shown_codes(codes)
        detail = f" ({', '.join(shown)})" if shown else ""
        super().__init__(f"Meridian GraphQL operation {operation} failed{detail}")
        self.operation = operation
        self.codes = codes


class InvertedPeriodError(ValueError):
    """Meridian gave a billing period that ends before it starts."""

    detail = "a period that ends before it starts"

    def __init__(self) -> None:
        super().__init__("Meridian gave a billing period that ends before it starts")


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
        # How many accounts the latest list skipped for their status, and
        # how many properties it skipped as unreadable.
        self.skipped_accounts = self.skipped_properties = 0
        # What was already logged once, so each is logged once per client.
        self._logged: set[tuple[Any, ...]] = set()

    @property
    def tokens(self) -> MeridianTokenSet | None:
        """Return the current in-memory token set."""
        return self._tokens

    async def async_send_otp(self, email: str, journey_id: str) -> None:
        """
        Ask Meridian to email a one-time login code.

        As in Meridian's app, the status decides: any 2xx answer counts as
        sent, whatever its body, and HTTP 404 or 403 refuses the email.
        Unlike the app, a 403 that is a web page is taken to come from
        something in between and, like any other status, is no answer.
        """
        payload = {
            "email": email,
            "brand": BRAND,
            "redirectUrl": REDIRECT_URL,
            "journeyId": journey_id,
            "otpEnabled": True,
        }
        note = AnswerNote()
        try:
            await self._async_json_request(
                AUTH_EMAIL_URL,
                operation="send_code",
                json=payload,
                headers={"X-Client-Platform": CLIENT_PLATFORM},
                authenticated=False,
                note=note,
            )
        except _MeridianHttpError as err:
            if err.status == _HTTP_TOO_MANY_REQUESTS:
                raise _rate_limit(err) from err
            if err.status == _HTTP_NOT_FOUND or (
                err.status == _HTTP_FORBIDDEN and not err.html_body
            ):
                raise MeridianAuthenticationError(
                    "Meridian did not accept the login request"
                ) from err
            raise _connection_error(err) from err
        except ValueError:
            # An answer whose body is empty, or JSON but not an object: the
            # body is not read, and the status decides below.
            pass
        except MeridianConnectionError as err:
            # An answer that is not JSON: a 2xx one is sent all the same.
            if not is_unreadable(err) or note.status not in _HTTP_SUCCESS:
                raise
        if note.status not in _HTTP_SUCCESS:
            raise MeridianConnectionError(
                f"Meridian service returned HTTP {note.status}"
            )

    async def async_validate_otp(
        self, email: str, otp: str, journey_id: str
    ) -> MeridianTokenSet:
        """
        Exchange an emailed code for a renewable Firebase session.

        Meridian's and Google's refusals are assumed to be JSON; a page from
        something in between, or an answer that can't be read, makes no
        claim about the email, code or session. A session answer that can't
        be read is a ``TokenParseError``, a ``ValueError``.
        """
        try:
            response = await self._async_json_request(
                AUTH_OTP_URL,
                operation="check_code",
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
            # A refused code names itself whatever the status, as too many
            # tries may come with HTTP 429; only an answer without one is a
            # rate limit or a failure of the service.
            code = err.payload.get("code")
            if isinstance(code, str) and code in _OTP_CODES:
                raise MeridianOtpError(
                    code, "Meridian rejected the login code"
                ) from err
            if err.status == _HTTP_TOO_MANY_REQUESTS:
                raise _rate_limit(err) from err
            if _no_refusal(err):
                raise _connection_error(err) from err
            raise MeridianOtpError(
                str(code or ""), "Meridian rejected the login code"
            ) from err

        custom_token = response.get("customToken")
        if not isinstance(custom_token, str) or not custom_token:
            raise MeridianAuthenticationError(
                "Meridian returned no authentication token"
            )

        try:
            firebase = await self._async_json_request(
                f"{FIREBASE_CUSTOM_TOKEN_URL}?key={FIREBASE_API_KEY}",
                operation="session_sign_in",
                json={"token": custom_token, "returnSecureToken": True},
                authenticated=False,
                meridian_service=False,
            )
        except _MeridianHttpError as err:
            if err.status == _HTTP_TOO_MANY_REQUESTS:
                raise _rate_limit(err, sign_in_service=True) from err
            if _no_refusal(err):
                raise _connection_error(err, meridian_service=False) from err
            raise MeridianAuthenticationError(
                "Meridian authentication exchange was rejected"
            ) from err
        tokens = parse_firebase_tokens(firebase)
        await self._async_set_tokens(tokens)
        return tokens

    async def async_refresh_tokens(self, *, force: bool = False) -> MeridianTokenSet:
        """
        Refresh the Firebase ID token when it is close to expiry.

        Google's refusals are assumed to be JSON, so only a refusal in JSON
        (HTTP 400, 401 or 403) ends the sign-in. An answer that can't be
        read is the sign-in service's
        (``MeridianSignInServiceUnreadableError``): invalid data, which a
        later update tries again.
        """
        if self._tokens is None:
            raise MeridianAuthenticationError("No Meridian session is available")
        if not force and self._tokens.expires_at > datetime.now(UTC) + timedelta(
            minutes=5
        ):
            return self._tokens

        try:
            response = await self._async_json_request(
                f"{FIREBASE_REFRESH_URL}?key={FIREBASE_API_KEY}",
                operation="session_refresh",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._tokens.refresh_token,
                },
                authenticated=False,
                meridian_service=False,
            )
        except _MeridianHttpError as err:
            if err.status == _HTTP_TOO_MANY_REQUESTS:
                raise _rate_limit(err, sign_in_service=True) from err
            if err.status in {400, 401, 403} and err.json_body:
                raise MeridianAuthenticationError(
                    "Meridian session refresh was rejected"
                ) from err
            raise _connection_error(err, meridian_service=False) from err

        normalized = {
            "idToken": response.get("id_token"),
            "refreshToken": response.get("refresh_token"),
            "expiresIn": response.get("expires_in"),
            "localId": response.get("user_id"),
        }
        try:
            tokens = parse_firebase_tokens(normalized)
        except TokenParseError as err:
            raise MeridianSignInServiceUnreadableError("unexpected data") from err
        await self._async_set_tokens(tokens)
        _LOGGER.debug("Meridian session refreshed")
        return tokens

    async def async_get_accounts(self) -> tuple[MeridianAccount, ...]:
        """
        Return the active and dormant accounts, with their properties.

        An account with any other status is skipped, and counted in
        ``skipped_accounts``; each such status is logged once, named only
        when it looks like an enum name, with no digits. A property that
        can't be read is skipped, and counted in ``skipped_properties`` and
        on its account; a ``ValueError`` when one was and no account has a
        property left.
        """
        data = await self._async_graphql(
            "accountsList",
            _ACCOUNTS_QUERY,
            {"allowedBrandCodes": ["MERIDIAN_ENERGY"]},
        )
        viewer = require_mapping(data.get("viewer"), "viewer")
        raw_accounts = require_list(viewer.get("accounts"), "viewer.accounts")

        accounts: list[MeridianAccount] = []
        skipped = 0
        for position, raw_account_value in enumerate(raw_accounts, start=1):
            raw_account = require_mapping(raw_account_value, "account")
            number = _required_string(raw_account, "number")
            status = _required_string(raw_account, "status")
            if status not in _FOLLOWED_STATUSES:
                skipped += 1
                self._log_skipped_status(status)
                continue
            accounts.append(await self._async_get_account(number, status, position))
        self.skipped_accounts = skipped
        self.skipped_properties = sum(item.skipped_properties for item in accounts)
        if self.skipped_properties and not any(item.properties for item in accounts):
            raise ValueError("Meridian Energy can't read any property Meridian lists")
        return tuple(accounts)

    def _log_skipped_status(self, status: str) -> None:
        """Log once that accounts with a status are not followed."""
        name = status if _ENUM_NAME.fullmatch(status) else _UNNAMED
        if ("status", name) in self._logged:
            return
        self._logged.add(("status", name))
        _LOGGER.info(
            "Meridian Energy doesn't follow an account whose status is %s; it "
            "follows active and dormant accounts",
            name,
        )

    async def _async_get_account(
        self, account_number: str, status: str, position: int
    ) -> MeridianAccount:
        """
        Return an account and the properties that can be followed.

        A property whose answer can't be read is skipped, counted, and
        logged by its place in Meridian's lists. A register whose feed-in
        flag can't be read counts as no feed-in, which the property notes
        (``feed_in_unread``) for the coordinator to log.
        """
        data = await self._async_graphql(
            "account",
            _ACCOUNT_QUERY,
            {"accountNumber": account_number, "activeFrom": "1970-01-01T00:00:00Z"},
        )
        account = require_mapping(data.get("account"), "account")
        today = datetime.now(_NZ).date()
        raw_properties = require_list(account.get("properties"), "account.properties")
        properties: list[MeridianProperty] = []
        for number, raw_property in enumerate(raw_properties, start=1):
            place = (number, len(raw_properties), position)
            try:
                properties.append(_parse_property(raw_property, today))
            except ValueError:
                self._log_once(
                    ("property", *place),
                    logging.WARNING,
                    "Meridian Energy can't read Meridian's answer for property %d "
                    "of %d on account %d, as Meridian lists them",
                    place,
                )
        return MeridianAccount(
            number=_required_string(account, "number"),
            status=status,
            properties=tuple(properties),
            skipped_properties=len(raw_properties) - len(properties),
        )

    def _log_once(
        self, key: tuple[Any, ...], level: int, message: str, args: tuple[int, ...]
    ) -> None:
        """Log a line once for this client, keyed by what it is about."""
        if key in self._logged:
            return
        self._logged.add(key)
        _LOGGER.log(level, message, *args)

    async def async_get_measurements(
        self,
        *,
        account_number: str,
        property_id: str,
        direction: str,
        start_on: date,
        end_on: date,
        last: int,
        before: str | None = None,
        frequency: str = READING_FREQUENCY_HOUR,
        pace: Callable[[], Awaitable[None]] | None = None,
        context: str = "",
    ) -> MeasurementPage:
        """
        Return one backwards page of measurements, hours or days, in a window.

        The window is ``start_on`` to ``end_on``, New Zealand dates, both
        included. Every page of a window sends both bounds and ``last``; a
        continuation page adds ``before``, the previous page's start cursor,
        and differs from the first page in nothing else. ``pace`` is awaited
        before each send, a resend included. ``context`` goes into the
        request's DEBUG line.
        """
        if start_on > end_on or last < 1:
            raise ValueError("Invalid Meridian measurements window")
        variables: dict[str, Any] = {
            "accountNumber": account_number,
            "propertyId": property_id,
            "last": last,
            "startOn": start_on.isoformat(),
            "endOn": end_on.isoformat(),
            "readingFrequencyType": frequency,
            "readingDirectionType": direction,
            "readingQualityType": READING_QUALITY_COMBINED,
        }
        if before is not None:
            variables["before"] = before

        data = await self._async_graphql(
            _MEASUREMENTS,
            _MEASUREMENTS_QUERY,
            variables,
            time_limit=MEASUREMENTS_TIMEOUT_SECONDS,
            operation_name=_MEASUREMENTS_NAME,
            url=_MEASUREMENTS_URL,
            pace=pace,
            context=context,
        )
        account = require_mapping(data.get("account"), "measurement account")
        property_data = require_mapping(account.get("property"), "measurement property")
        connection = require_mapping(property_data.get("measurements"), "measurements")
        page_info = require_mapping(connection.get("pageInfo"), "measurements.pageInfo")

        measurements: list[MeridianMeasurement] = []
        for edge_value in require_list(connection.get("edges"), "measurements.edges"):
            edge = require_mapping(edge_value, "measurement edge")
            node = require_mapping(edge.get("node"), "measurement node")
            measurements.append(_parse_measurement(node, direction, frequency))

        return MeasurementPage(
            measurements=tuple(measurements),
            has_previous_page=bool(page_info.get("hasPreviousPage")),
            start_cursor=_optional_string(page_info.get("startCursor")),
        )

    async def async_get_window(
        self,
        *,
        account_number: str,
        property_id: str,
        direction: str,
        start_on: date,
        end_on: date,
        last: int,
        frequency: str = READING_FREQUENCY_HOUR,
        pace: Callable[[], Awaitable[None]] | None = None,
        context: str = "",
    ) -> tuple[MeridianMeasurement, ...]:
        """
        Return every row of one window, following its pages backwards.

        ``pace`` is awaited before each request, a resend included. A window
        takes at most 8 pages; each continuation must carry a new cursor, and
        an empty page must be the last. Otherwise the answer is one this can't
        read (``ValueError``).
        """
        rows: list[MeridianMeasurement] = []
        before: str | None = None
        for page_number in range(1, MAX_WINDOW_PAGES + 1):
            page = await self.async_get_measurements(
                account_number=account_number,
                property_id=property_id,
                direction=direction,
                start_on=start_on,
                end_on=end_on,
                last=last,
                before=before,
                frequency=frequency,
                pace=pace,
                context=f"{context} page={page_number}".strip(),
            )
            rows.extend(page.measurements)
            if not page.has_previous_page:
                return tuple(rows)
            if not page.measurements:
                raise ValueError("Meridian returned an empty page with more to come")
            if page.start_cursor is None or page.start_cursor == before:
                raise ValueError("Meridian returned a page that does not move on")
            before = page.start_cursor
        raise ValueError("Meridian returned more pages than a window can hold")

    async def async_get_billing_period(
        self, account_number: str
    ) -> MeridianBillingPeriod | None:
        """
        Return the retailer-defined current billing period for an account.

        None when the account has no billing options, which Meridian may
        leave out: no period, and no failure. The start and end are read
        strictly, and a period that ends before it starts is refused
        (``InvertedPeriodError``). The other fields, the next billing date
        among them, are read leniently: each is None when absent or
        unreadable, logging the field's name once at DEBUG, never its value.
        """
        data = await self._async_graphql(
            "billingPeriods",
            _BILLING_PERIODS_QUERY,
            {"accountNumber": account_number},
        )
        account = require_mapping(data.get("account"), "billing account")
        if account.get("billingOptions") is None:
            return None
        options = require_mapping(account.get("billingOptions"), "billing options")
        start = _optional_date(options.get("currentBillingPeriodStartDate"))
        end = _optional_date(options.get("currentBillingPeriodEndDate"))
        if start is not None and end is not None and end < start:
            raise InvertedPeriodError
        length = self._lenient(
            options,
            "periodLength",
            lambda value: isinstance(value, str) and value in _PERIOD_LENGTHS,
        )
        multiplier = self._lenient(
            options, "periodLengthMultiplier", lambda value: _whole(value, 1, None)
        )
        start_day = self._lenient(
            options,
            "periodStartDay",
            lambda value: _whole(value, 1, _LAST_DAY_OF_LONG_MONTH),
        )
        is_fixed = self._lenient(
            options, "isFixed", lambda value: isinstance(value, bool)
        )
        next_billing = self._lenient(options, "nextBillingDate", _is_iso_date)
        return MeridianBillingPeriod(
            period_length=length,
            period_length_multiplier=multiplier,
            is_fixed=is_fixed,
            start=start,
            end=end,
            next_billing_date=(
                None if next_billing is None else date.fromisoformat(next_billing)
            ),
            period_start_day=start_day,
        )

    def _lenient(
        self, options: dict[str, Any], name: str, valid: Callable[[Any], bool]
    ) -> Any:
        """Return a field's value if valid, else None; an invalid one is logged once."""
        value = options.get(name)
        if value is None or valid(value):
            return value
        if ("billing", name) not in self._logged:
            self._logged.add(("billing", name))
            _LOGGER.debug("Meridian's billing period has an unreadable %s", name)
        return None

    async def async_get_balance(self, account_number: str) -> MeridianBalance:
        """Return an account's balance, last bill and recent payments."""
        data = await self._async_graphql(
            "balance",
            BALANCE_QUERY,
            {"accountNumber": account_number},
            operation_name=BALANCE_NAME,
            url=web_app_url(GRAPHQL_URL, BALANCE_NAME),
        )
        return parse_balance(require_mapping(data.get("account"), "balance account"))

    async def async_get_prices(
        self, account_number: str, property_id: str
    ) -> MeridianPrices | MeridianNoAgreement | None:
        """Return a property's all-day prices, None for any other plan."""
        data = await self._async_graphql(
            "prices",
            PRICES_QUERY,
            {"accountNumber": account_number, "propertyId": property_id},
            operation_name=PRICES_NAME,
            url=web_app_url(GRAPHQL_URL, PRICES_NAME),
        )
        account = require_mapping(data.get("account"), "prices account")
        return parse_prices(require_mapping(account.get("property"), "prices property"))

    async def _async_graphql(
        self,
        operation: str,
        query: str,
        variables: dict[str, Any],
        *,
        time_limit: float = REQUEST_TIMEOUT_SECONDS,
        operation_name: str | None = None,
        url: str = GRAPHQL_URL,
        pace: Callable[[], Awaitable[None]] | None = None,
        context: str = "",
    ) -> dict[str, Any]:
        """
        Send one GraphQL query, and once more after a failure that may pass.

        Meridian's web app resends a query that met HTTP 502, 503 or 504;
        this does once, 2 to 5 s later, and after a timeout or a network
        error, the session's renewal included (``is_passing_failure``). A
        rate limit is never resent. ``pace`` is awaited before each send.
        """
        for send in range(_SENDS):
            if pace is not None:
                await pace()
            try:
                return await self._async_graphql_once(
                    operation,
                    query,
                    variables,
                    time_limit=time_limit,
                    operation_name=operation_name,
                    url=url,
                    context=context,
                )
            except MeridianConnectionError as err:
                if send == _SENDS - 1 or not is_passing_failure(err):
                    raise
                _LOGGER.debug(
                    "Resending a request that failed (%s)",
                    getattr(err, "detail", "network error"),
                )
                await _async_wait(random.uniform(*TRANSIENT_RESEND_DELAY_SECONDS))  # noqa: S311
        raise AssertionError  # pragma: no cover - the loop returns or raises

    async def _async_graphql_once(  # noqa: C901 - one branch per kind of answer
        self,
        operation: str,
        query: str,
        variables: dict[str, Any],
        *,
        time_limit: float,
        operation_name: str | None,
        url: str,
        context: str,
    ) -> dict[str, Any]:
        """
        Send one GraphQL query, renewing the session once if it is rejected.

        ``operation`` labels the query in logs and errors; ``operation_name``,
        when given, is the name the document and the request carry instead.
        Kraken's rate limit (KT-CT-1199) arrives as a GraphQL error and is
        raised as a rate limit with an hour's delay. A 401 is sent once more
        with a renewed session, as Meridian's app does. The sign-in ends only
        when a code that says the session has ended (KT-CT-1111, 1120, 1124
        or 1143) comes back after that renewal, in a GraphQL answer or in a
        401's body. Any other 401 after renewal, and any 403, is Meridian
        refusing the request.
        """
        for attempt in range(2):
            tokens = await self.async_refresh_tokens(force=attempt == 1)
            note = AnswerNote()
            try:
                response = await self._async_json_request(
                    url,
                    operation=operation,
                    time_limit=time_limit,
                    json={
                        "operationName": operation_name or operation,
                        "query": query,
                        "variables": variables,
                    },
                    headers={"Authorization": tokens.id_token},
                    authenticated=True,
                    context=context,
                    note=note,
                )
            except _MeridianHttpError as err:
                if err.status == _HTTP_TOO_MANY_REQUESTS:
                    raise _rate_limit(err) from err
                if err.status == _HTTP_UNAUTHORIZED:
                    if attempt == 0:
                        continue
                    if any(
                        code in _AUTH_GRAPHQL_CODES
                        for code in _error_codes(err.payload)
                    ):
                        raise MeridianAuthenticationError(
                            "Meridian rejected the authenticated session"
                        ) from err
                raise _connection_error(err) from err

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
                    ",".join(shown_codes(codes)) or "none",
                )
                if RATE_LIMIT_CODE in codes:
                    raise MeridianRateLimitError(
                        DEFAULT_RETRY_AFTER_SECONDS,
                        answer_status=note.status,
                        code=RATE_LIMIT_CODE,
                        retry_after_given=note.retry_after_given,
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
        operation: str,
        time_limit: float = REQUEST_TIMEOUT_SECONDS,
        json: dict[str, Any] | None = None,
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        authenticated: bool,
        meridian_service: bool = True,
        context: str = "",
        note: AnswerNote | None = None,
    ) -> dict[str, Any]:
        """
        Send one request through the transport and translate its failures.

        ``meridian_service`` is False for Google's sign-in service (Firebase),
        which holds the session: its failures are its own
        (``MeridianSignInServiceError``), never Meridian's service failing.
        An answer that is not JSON is unreadable, whichever sent it.
        """
        del authenticated  # Documents intent at each call site; never log the request.
        try:
            return await self._transport.async_json_request(
                url,
                operation=operation,
                time_limit=time_limit,
                json=json,
                data=data,
                headers=headers,
                context=context,
                note=note,
            )
        except _MeridianHttpError:
            raise
        except MeridianTimeoutError as err:
            if not meridian_service:
                raise MeridianSignInServiceError(None, timed_out=True) from err
            raise MeridianUpstreamError(None) from err
        except MeridianUnreadableResponseError as err:
            if not meridian_service:
                raise MeridianSignInServiceUnreadableError("not JSON") from err
            raise MeridianConnectionError(str(err)) from err
        except MeridianTransportError as err:
            if not meridian_service:
                raise MeridianSignInServiceError(None) from err
            raise MeridianConnectionError(str(err)) from err


def exception_chain(err: BaseException | None) -> list[BaseException]:
    """
    Return an exception and the chain of causes behind it, outermost first.

    The chain is the one a traceback shows: an exception's context counts only
    when it was not suppressed (``raise ... from None``, or an explicit cause).
    """
    chain: list[BaseException] = []
    while err is not None and err not in chain and len(chain) < _MAX_CAUSE_DEPTH:
        chain.append(err)
        err = err.__cause__ or (None if err.__suppress_context__ else err.__context__)
    return chain


def http_status(err: BaseException | None) -> int | None:
    """Return the first HTTP status found in an exception chain."""
    for item in exception_chain(err):
        status = getattr(item, "status", None)
        if isinstance(status, int) and not isinstance(status, bool):
            return status
    return None


def is_unreadable(err: BaseException) -> bool:
    """Return whether a failure is an answer that is not JSON, as a web page."""
    return any(
        isinstance(item, MeridianUnreadableResponseError)
        for item in exception_chain(err)
    )


def is_passing_failure(err: BaseException) -> bool:
    """
    Return whether a failure may pass on its own, so asking again may succeed.

    HTTP 502, 503 or 504, a timeout or a network error, from Meridian or
    the sign-in service, as Meridian's web app resends a query after 502 to
    504. Never a rate limit, a refusal or an answer that can't be read.
    """
    if isinstance(
        err,
        MeridianRateLimitError
        | MeridianRejectedError
        | MeridianSignInServiceUnreadableError,
    ):
        return False
    if isinstance(err, MeridianUpstreamError | MeridianSignInServiceError):
        return err.status is None or err.status in TRANSIENT_STATUSES
    # A network error has no status; any answer, an unreadable one included,
    # has one.
    return isinstance(err, MeridianConnectionError) and http_status(err) is None


def _no_refusal(err: _MeridianHttpError) -> bool:
    """
    Return whether the code check's or the session sign-in's answer is no refusal.

    A timeout (408) or a server error is a failure of the service. Meridian's
    and Google's refusals are assumed to be JSON, so any other status is
    taken to refuse only in JSON; a page from something in between makes no
    claim about the code or session, and is no answer.
    """
    return (
        err.status == _HTTP_REQUEST_TIMEOUT
        or err.status >= _HTTP_SERVER_ERROR
        or not err.json_body
    )


def _rate_limit(
    err: _MeridianHttpError, *, sign_in_service: bool = False
) -> MeridianRateLimitError:
    """Return the rate limit an HTTP 429 answer is, with what it said."""
    return MeridianRateLimitError(
        err.retry_after,
        answer_status=err.status,
        code=", ".join(shown_codes(_error_codes(err.payload))) or None,
        retry_after_given=err.retry_after_given,
        sign_in_service=sign_in_service,
    )


def log_rate_limit(err: MeridianRateLimitError, then: str) -> None:
    """
    Log at INFO what a rate limit's answer said, and what is done about it.

    Its status, code and whether it gave Retry-After, never its body, so a
    real limit leaves a trace of its form.
    """
    _LOGGER.info(
        "%s asked Meridian Energy to wait (HTTP %s, %s, %s Retry-After); %s",
        err.service,
        "-" if err.answer_status is None else err.answer_status,
        err.code or "no code",
        "with" if err.retry_after_given else "without",
        then,
    )


def _connection_error(
    err: _MeridianHttpError, *, meridian_service: bool = True
) -> MeridianConnectionError:
    """
    Return a privacy-safe connection error for an unsuccessful HTTP status.

    A request timeout or a server error from Meridian's service, or a gateway
    in front of it, is that service failing; another client error from it is
    the request being refused (``MeridianRejectedError``). Any status from
    Google's sign-in service (``meridian_service=False``) is that service's
    failure.
    """
    if not meridian_service:
        return MeridianSignInServiceError(err.status)
    if err.status == _HTTP_REQUEST_TIMEOUT or err.status >= _HTTP_SERVER_ERROR:
        return MeridianUpstreamError(err.status)
    if err.status >= _HTTP_CLIENT_ERROR:
        return MeridianRejectedError(err.status, _error_codes(err.payload))
    return MeridianConnectionError(f"Meridian service returned HTTP {err.status}")


def _error_codes(payload: dict[str, Any]) -> tuple[str, ...]:
    """Return the validated Kraken error codes an error body names, in order."""
    errors = payload.get("errors")
    if not isinstance(errors, list):
        return ()
    codes = (_graphql_error_code(item) for item in errors if isinstance(item, dict))
    return tuple(dict.fromkeys(code for code in codes if code != UNKNOWN_ERROR_CODE))
