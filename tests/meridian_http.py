"""
A synthetic Meridian service at the HTTP level, for tests of the real client.

It answers the login, session and GraphQL requests the integration sends,
through an object that stands in for aiohttp's ClientSession, so the real
transport, API client, coordinator and config flow all run. Every value is
synthetic. Measurements come from ``kraken_fake.FakeKraken``, which builds
each request's rows from its own window and pages them with Kraken's
array-connection cursors (base64 of ``arrayconnection:<offset>``), so a test
can check that none reaches a log.

``stalled_session`` is a real aiohttp session whose connections are never
made, for the time limits only aiohttp itself can show: nothing it is asked
to send leaves the machine.
"""

from __future__ import annotations

import asyncio
import base64
import json
import socket
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Self

from aiohttp import ClientSession, TCPConnector
from aiohttp.abc import AbstractResolver, ResolveResult

from custom_components.meridian_energy.const import (
    AUTH_EMAIL_URL,
    AUTH_OTP_URL,
    FIREBASE_CUSTOM_TOKEN_URL,
    FIREBASE_REFRESH_URL,
    GRAPHQL_URL,
)
from custom_components.meridian_energy.graphql import measurements_operation

from .kraken_fake import FakeKraken

# The measurements query's name and URL, as the integration sends them. Hooks
# and the operation log name it "measurements", as the integration does.
MEASUREMENTS_NAME, MEASUREMENTS_URL = measurements_operation(GRAPHQL_URL)

ACCOUNT_NUMBER = "A-TEST-1"
PROPERTY_ID = "synthetic-property"
EMAIL = "person@example.com"
CUSTOM_TOKEN = "synthetic-custom-token-value"
REFRESH_TOKEN = "synthetic-refresh-token-value"
# A token-shaped value: base64 header, claims and signature.
ID_TOKEN = ".".join(
    base64.urlsafe_b64encode(part.encode()).decode().rstrip("=")
    for part in ('{"alg":"none"}', '{"sub":"synthetic-user-0123"}', "signature-0123")
)


# Where a request to a stalled session goes: a reserved name that never
# resolves, though the session never even tries to resolve it.
STALLED_URL = "https://meridian.invalid/graphql"


class _NeverResolves(AbstractResolver):
    """A resolver that never answers, so no connection is ever made."""

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[ResolveResult]:
        del host, port, family
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        return None


@asynccontextmanager
async def stalled_session() -> AsyncIterator[ClientSession]:
    """
    Yield a real aiohttp session whose connections are never made.

    Its name lookups never answer, as on a host whose DNS or network has
    stalled, so a request ends only by a time limit, the way aiohttp ends
    it. Addresses that need no lookup, such as 127.0.0.1, still connect.
    """
    session = ClientSession(connector=TCPConnector(resolver=_NeverResolves()))
    try:
        yield session
    finally:
        await session.close()


class StalledConnection:
    """
    An answer that never comes, because the connection is never made.

    ``FakeMeridianSession`` sends a request a hook answers with this through
    ``session``, a ``stalled_session``, to ``STALLED_URL``, with the time
    limits the transport set.
    """

    def __init__(self, session: ClientSession) -> None:
        self.session = session


class Response:
    """
    One canned response, read the way the transport reads it.

    ``error`` is raised before the status and headers arrive, as a request
    that got no answer; ``read_error`` once they have, as a body that
    stalled or broke off.
    """

    def __init__(
        self,
        body: bytes,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        gate: asyncio.Event | None = None,
        error: BaseException | None = None,
        read_error: BaseException | None = None,
    ) -> None:
        self.status = status
        self.headers = (
            {"Content-Type": "application/json"} if headers is None else headers
        )
        self._body = body
        self._gate = gate
        self._error = error
        self._read_error = read_error

    async def __aenter__(self) -> Self:
        await asyncio.sleep(0)
        if self._gate is not None:
            await self._gate.wait()
        if self._error is not None:
            raise self._error
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def read(self) -> bytes:
        if self._read_error is not None:
            raise self._read_error
        return self._body


def json_response(payload: object, *, status: int = 200) -> Response:
    """Return a JSON response."""
    return Response(json.dumps(payload).encode(), status=status)


# A hook may replace the answer to a GraphQL request: it gets the operation
# name and the request's variables, and returns a response or None.
Hook = Callable[[str, dict[str, Any]], Response | StalledConnection | None]
# An auth hook may replace the answer to a request that is not GraphQL: it
# gets the transport's name for it (``send_code``, ``check_code``,
# ``session_sign_in`` or ``session_refresh``) and returns a response or None.
AuthHook = Callable[[str], Response | None]


class FakeMeridianSession:
    """
    Answer the integration's requests from a synthetic hourly history.

    ``kraken`` answers the measurements requests; its options shape the
    history. ``posts`` records the URL and operation name of every GraphQL
    request, so a test can check the measurements query's name and URL.

    ``accounts`` are the login's account numbers, each with one property
    (``account_data``); those in ``feed_in`` also have an active feed-in
    register, so they have a generation direction. Every account's
    measurements come from the same ``kraken``. Both may be changed between
    requests. ``hook`` may replace a GraphQL answer, ``auth_hook`` the
    answer to a login or session request.
    """

    def __init__(
        self,
        now: datetime,
        *,
        days: int = 100,
        kraken: FakeKraken | None = None,
        accounts: Iterable[str] = (ACCOUNT_NUMBER,),
        feed_in: Iterable[str] = (),
    ) -> None:
        self.kraken = kraken if kraken is not None else FakeKraken(now, days=days)
        self.accounts = tuple(accounts)
        self.feed_in = frozenset(feed_in)
        self.hook: Hook = lambda _operation, _variables: None
        self.auth_hook: AuthHook = lambda _operation: None
        self.operations: list[str] = []
        self.posts: list[tuple[str, str]] = []

    @property
    def cursors(self) -> set[str]:
        """Return every cursor an answer carried, so a test can look for each."""
        return self.kraken.cursors

    def post(self, url: str, **kwargs: Any) -> Any:
        """Return the response to one POST."""
        body = kwargs.get("json") or {}
        if (operation := auth_operation(url)) is not None:
            if (override := self.auth_hook(operation)) is not None:
                return override
            return json_response(_AUTH_ANSWERS[operation])
        if url not in {GRAPHQL_URL, MEASUREMENTS_URL}:
            raise AssertionError("unexpected URL")
        return self._graphql(url, body, kwargs)

    def _graphql(self, url: str, body: dict[str, Any], kwargs: dict[str, Any]) -> Any:
        """Return the answer to one GraphQL request, or the hook's."""
        name = str(body["operationName"])
        self.posts.append((url, name))
        if f"query {name}(" not in str(body["query"]):
            raise AssertionError("operation name not in the query document")
        if (name == MEASUREMENTS_NAME) != (url == MEASUREMENTS_URL):
            raise AssertionError("measurements query name and URL do not match")
        operation = "measurements" if name == MEASUREMENTS_NAME else name
        variables = dict(body["variables"])
        self.operations.append(operation)
        if isinstance(override := self.hook(operation, variables), StalledConnection):
            return override.session.post(STALLED_URL, **kwargs)
        if override is not None:
            return override
        if operation == "measurements":
            return json_response(self.kraken.graphql(variables))
        return json_response({"data": self._data(operation, variables)})

    def _data(self, operation: str, variables: dict[str, Any]) -> dict[str, Any]:
        if operation == "accountsList":
            return {
                "viewer": {
                    "accounts": [
                        {"number": number, "status": "ACTIVE", "id": str(position)}
                        for position, number in enumerate(self.accounts, start=1)
                    ]
                }
            }
        if operation == "account":
            number = str(variables["accountNumber"])
            if number not in self.accounts:
                raise AssertionError("an account the login does not have")
            return {"account": account_data(number, feed_in=number in self.feed_in)}
        if operation == "billingPeriods":
            return {"account": billing_data()}
        raise AssertionError(f"unexpected operation {operation}")

    def payload_dates(self) -> set[str]:
        """Return every calendar date in the history or the requests' bounds."""
        return self.kraken.payload_dates()


def property_id(number: str) -> str:
    """Return the synthetic property ID of an account."""
    return PROPERTY_ID if number == ACCOUNT_NUMBER else f"{PROPERTY_ID}-{number}"


def property_address(number: str) -> str:
    """Return the synthetic street address of an account's property."""
    if number == ACCOUNT_NUMBER:
        return "1 Synthetic Street"
    return f"1 Synthetic Street, unit {number}"


def account_data(
    number: str = ACCOUNT_NUMBER, *, feed_in: bool = False
) -> dict[str, Any]:
    """
    Return a synthetic account, its property and its meter.

    With ``feed_in``, the meter also has an active feed-in register, so the
    property has a generation direction.
    """
    registers: list[dict[str, Any]] = [
        {
            "identifier": "synthetic-register",
            "activeFrom": "2020-01-01",
            "activeTo": None,
            "isFeedIn": False,
        }
    ]
    if feed_in:
        registers.append(
            {
                "identifier": "synthetic-feed-in-register",
                "activeFrom": "2020-01-01",
                "activeTo": None,
                "isFeedIn": True,
            }
        )
    return {
        "number": number,
        "status": "ACTIVE",
        "properties": [
            {
                "id": property_id(number),
                "address": property_address(number),
                "meterPoints": [
                    {
                        "id": "synthetic-meter",
                        "marketIdentifier": "0000000001SYN00",
                        "registers": registers,
                    }
                ],
            }
        ],
    }


# The answers to the login and session requests, by the transport's name.
_AUTH_ANSWERS: dict[str, dict[str, Any]] = {
    "send_code": {"success": True},
    "check_code": {"customToken": CUSTOM_TOKEN},
    "session_sign_in": {
        "idToken": ID_TOKEN,
        "refreshToken": REFRESH_TOKEN,
        "expiresIn": "3600",
        "localId": "synthetic-user-0123",
    },
    "session_refresh": {
        "id_token": ID_TOKEN,
        "refresh_token": REFRESH_TOKEN,
        "expires_in": "3600",
        "user_id": "synthetic-user-0123",
    },
}


def auth_operation(url: str) -> str | None:
    """Return the transport's name for a login or session request, or None."""
    if url == AUTH_EMAIL_URL:
        return "send_code"
    if url == AUTH_OTP_URL:
        return "check_code"
    if url.startswith(FIREBASE_CUSTOM_TOKEN_URL):
        return "session_sign_in"
    if url.startswith(FIREBASE_REFRESH_URL):
        return "session_refresh"
    return None


def billing_data() -> dict[str, Any]:
    """Return a synthetic monthly billing period."""
    return {
        "billingOptions": {
            "periodLength": "MONTHLY",
            "periodLengthMultiplier": 1,
            "isFixed": False,
            "currentBillingPeriodStartDate": None,
            "currentBillingPeriodEndDate": None,
            "nextBillingDate": None,
            "periodStartDay": 1,
        }
    }
