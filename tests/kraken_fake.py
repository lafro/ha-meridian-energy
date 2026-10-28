"""
A Kraken-like measurements service for tests, answering each request from its bounds.

Kraken builds a measurements connection's list for each request from that
request's own arguments, and pages through it with graphql-relay's offset
cursors (base64 of ``arrayconnection:<offset>``), silently ignoring a cursor
out of range. So does this fake: a cursor means something only under the
arguments that issued it. It checks every request the way the integration
must send it: ``startOn``, ``endOn`` and ``last`` present, a continuation
page's arguments identical to those of the page that issued its cursor, and
no cursor sent twice under the same arguments (unless ``repeat_cursors``,
for a fake that outlives one sync, as Kraken does: a later sync may page
through the same window again). A breach is recorded in ``violations`` and
raised as an ``AssertionError``.

``startOn`` and ``endOn`` are New Zealand dates, both included: a window's
rows run from New Zealand midnight at the start of ``startOn`` to midnight
after ``endOn``. Rows from the start of today are estimates, and rows run on
to the end of today (future estimates, as Meridian's COMBINED quality may
return), unless ``future_rows`` is off. The options model the ways Meridian
might differ: more channels, an ignored start, shifted dates, an excluded
``startOn`` or ``endOn``, a first or last hour left out (a bound read as
exclusive at the hour), a history start, an error before it (for windows
that end before it, or for any ``startOn`` before its date, as the web
app's clamping of ``startOn`` to the supply start suggests), gaps, rows
without a cost, withdrawn newest rows, empty windows and in-band error
codes, for any window starting on a date or only for one longer than a day
(``week_codes``). Every value is synthetic.

The fake answers at two levels: ``graphql`` returns the HTTP body for the
integration's real client (see ``meridian_http.FakeMeridianSession``), and
``async_get_measurements`` stands in for the client itself, parsing the same
body with the client's own code.
"""

from __future__ import annotations

import base64
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from custom_components.meridian_energy.api import (
    MeridianApiClient,
    MeridianGraphQLError,
    MeridianRateLimitError,
)
from custom_components.meridian_energy.const import DEFAULT_RETRY_AFTER_SECONDS
from custom_components.meridian_energy.graphql import RATE_LIMIT_CODE
from custom_components.meridian_energy.models import MeasurementPage, MeridianTokenSet

NZ = ZoneInfo("Pacific/Auckland")
HOUR = timedelta(hours=1)
PREFIX = "arrayconnection:"
PROPERTY_ID = "synthetic-property"
# The arguments a continuation page must repeat exactly.
_WINDOW_ARGUMENTS = (
    "accountNumber",
    "propertyId",
    "startOn",
    "endOn",
    "last",
    "readingFrequencyType",
    "readingDirectionType",
    "readingQualityType",
)


def cursor(offset: int) -> str:
    """Return the array-connection cursor for an offset."""
    return base64.b64encode(f"{PREFIX}{offset}".encode()).decode()


def cursor_offset(value: str) -> int | None:
    """Return a cursor's offset, or None for one that is not an offset cursor."""
    try:
        decoded = base64.b64decode(value).decode()
        return int(decoded.removeprefix(PREFIX)) if decoded.startswith(PREFIX) else None
    except ValueError:
        return None


def local_midnight(day: date) -> datetime:
    """Return midnight in New Zealand at the start of a date, in UTC."""
    return datetime.combine(day, time.min, tzinfo=NZ).astimezone(UTC)


def _ceil_hour(value: datetime) -> datetime:
    floored = value.replace(minute=0, second=0, microsecond=0)
    return floored if floored == value else floored + HOUR


class FakeKraken:
    """Answer measurements requests from a synthetic hourly history."""

    def __init__(
        self,
        now: datetime,
        *,
        history_start: datetime | None = None,
        days: int | None = None,
        channels: int = 1,
        aggregate: bool = False,
        value: Callable[[datetime, int], str] | None = None,
        cost: Callable[[datetime, int], str | None] | None = None,
        future_rows: bool = True,
        ignore_start_on: bool = False,
        shift_days: int = 0,
        exclusive_start_on: bool = False,
        exclusive_end_on: bool = False,
        exclusive_start_hour: bool = False,
        exclusive_end_hour: bool = False,
        gaps: Iterable[tuple[datetime, datetime]] = (),
        empty_start_on: Iterable[date] = (),
        withdrawn_from: datetime | None = None,
        pre_supply_error: str | None = None,
        pre_supply_start_on: bool = False,
        codes: dict[date, str] | None = None,
        week_codes: dict[date, str] | None = None,
        repeat_cursors: bool = False,
    ) -> None:
        self.now = now
        if history_start is None:
            history_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(
                days=400 if days is None else days
            )
        self.history_start = history_start
        self.channels = channels
        self.aggregate = aggregate
        self.value = value or (lambda _start, _channel: "0.5")
        self.cost = cost or (lambda _start, _channel: "15")
        self.future_rows = future_rows
        self.ignore_start_on = ignore_start_on
        self.shift_days = shift_days
        self.exclusive_start_on = exclusive_start_on
        self.exclusive_end_on = exclusive_end_on
        self.exclusive_start_hour = exclusive_start_hour
        self.exclusive_end_hour = exclusive_end_hour
        self.gaps = tuple(gaps)
        self.empty_start_on = set(empty_start_on)
        self.withdrawn_from = withdrawn_from
        self.pre_supply_error = pre_supply_error
        self.pre_supply_start_on = pre_supply_start_on
        self.codes = dict(codes or {})
        self.week_codes = dict(week_codes or {})
        self.repeat_cursors = repeat_cursors
        # Each request's arguments, without identifiers, and the rows its
        # list held: what Kraken had to build to answer it.
        self.requests: list[dict[str, Any]] = []
        self.materialized: list[int] = []
        self.violations: list[str] = []
        self.cursors: set[str] = set()
        self.sent_dates: set[str] = set()
        self._issued: set[tuple[str, tuple[Any, ...]]] = set()
        self._consumed: set[tuple[str, tuple[Any, ...]]] = set()

    # ------------------------------------------------------------------ rows

    @property
    def horizon(self) -> datetime:
        """Return where the rows end: the end of today in New Zealand, or now."""
        if self.future_rows:
            return local_midnight(self.now.astimezone(NZ).date() + timedelta(days=1))
        return self.now.replace(minute=0, second=0, microsecond=0)

    def history(self) -> list[tuple[datetime, int]]:
        """Return every row of the history, oldest first: an unbounded list."""
        return self._hours(self.history_start, self.horizon)

    def _hours(self, lower: datetime, upper: datetime) -> list[tuple[datetime, int]]:
        if self.withdrawn_from is not None:
            upper = min(upper, self.withdrawn_from)
        rows: list[tuple[datetime, int]] = []
        start = _ceil_hour(max(lower, self.history_start))
        while start < upper:
            if not any(lo <= start < hi for lo, hi in self.gaps):
                rows.extend((start, channel) for channel in range(self.channels))
            start += HOUR
        return rows

    def window_rows(self, start_on: date, end_on: date) -> list[tuple[datetime, int]]:
        """
        Return the rows a request for a window gets, as this fake reads it.

        With ``exclusive_start_on``, its list leaves out the date ``startOn``;
        with ``exclusive_start_hour``, the hour from midnight at the start of
        ``startOn``; with ``exclusive_end_hour``, the hour that ends at
        midnight after ``endOn``.
        """
        if start_on in self.empty_start_on:
            return []
        shift = timedelta(days=self.shift_days)
        first = start_on + shift + timedelta(days=1 if self.exclusive_start_on else 0)
        lower = (
            self.history_start
            if self.ignore_start_on
            else local_midnight(first)
            + (HOUR if self.exclusive_start_hour else timedelta(0))
        )
        last = end_on + shift + timedelta(days=0 if self.exclusive_end_on else 1)
        upper = min(
            self.horizon,
            local_midnight(last) - (HOUR if self.exclusive_end_hour else timedelta(0)),
        )
        return self._hours(lower, upper)

    def refuses(self, start_on: date, end_on: date) -> bool:
        """
        Return whether a request is refused as asking for dates before the supply.

        By default only a window that ends before the history starts is; with
        ``pre_supply_start_on``, any whose ``startOn`` is before its NZ date.
        """
        if self.pre_supply_error is None:
            return False
        if self.pre_supply_start_on:
            return start_on < self.history_start.astimezone(NZ).date()
        return local_midnight(end_on + timedelta(days=1)) <= self.history_start

    def node(self, start: datetime, channel: int, direction: str) -> dict[str, Any]:
        """Return one measurement node, as Kraken sends it."""
        end = start + HOUR
        amount = self.cost(start, channel)
        cost_type = (
            "GENERATION_VALUE" if direction == "GENERATION" else ("CONSUMPTION_COST")
        )
        return {
            "source": "SYNTHETIC",
            "value": self.value(start, channel),
            "unit": "kWh",
            "readAt": end.astimezone(NZ).isoformat(),
            "startAt": start.astimezone(NZ).isoformat(),
            "endAt": end.astimezone(NZ).isoformat(),
            "metaData": {
                "utilityFilters": {
                    "readingFrequencyType": "HOUR_INTERVAL",
                    "readingDirection": direction,
                    "registerId": (
                        None if self.aggregate else f"synthetic-register-{channel}"
                    ),
                    "deviceId": None if self.aggregate else "synthetic-device",
                    "marketSupplyPointId": "0000000001SYN00",
                    "readingQuality": (
                        "ACTUAL"
                        if end <= local_midnight(self.now.astimezone(NZ).date())
                        else "ESTIMATE"
                    ),
                },
                "statistics": [
                    {
                        "type": cost_type,
                        "costInclTax": (
                            None if amount is None else {"estimatedAmount": amount}
                        ),
                    }
                ],
            },
        }

    # -------------------------------------------------------------- requests

    def _violation(self, message: str) -> None:
        self.violations.append(message)
        raise AssertionError(message)

    def _check(self, variables: dict[str, Any]) -> tuple[Any, ...]:
        """Check one request's arguments; return those a continuation repeats."""
        for name in ("startOn", "endOn", "last"):
            if variables.get(name) is None:
                self._violation(f"a measurements request without {name}")
        arguments = tuple(variables.get(name) for name in _WINDOW_ARGUMENTS)
        before = variables.get("before")
        if before is not None:
            if (before, arguments) not in self._issued:
                self._violation(
                    "a continuation page whose arguments differ from those of "
                    "the page that issued its cursor"
                )
            if (before, arguments) in self._consumed and not self.repeat_cursors:
                self._violation("a cursor sent twice under the same arguments")
            self._consumed.add((before, arguments))
        return arguments

    def graphql(self, variables: dict[str, Any]) -> dict[str, Any]:
        """Return the HTTP body that answers one measurements request."""
        arguments = self._check(variables)
        start_on = date.fromisoformat(variables["startOn"])
        end_on = date.fromisoformat(variables["endOn"])
        self.sent_dates.update({variables["startOn"], variables["endOn"]})
        self.requests.append(
            {
                name: variables[name]
                for name in ("startOn", "endOn", "last", "before")
                if variables.get(name) is not None
            }
        )
        code = self.codes.get(start_on)
        if code is None and end_on > start_on:
            code = self.week_codes.get(start_on)
        if code is not None:
            self.materialized.append(0)
            return _errors(code)
        if self.refuses(start_on, end_on):
            self.materialized.append(0)
            return _errors(str(self.pre_supply_error))
        rows = self.window_rows(start_on, end_on)
        self.materialized.append(len(rows))
        end = len(rows)
        before = variables.get("before")
        if before is not None:
            offset = cursor_offset(before)
            if offset is not None and 0 <= offset < end:
                end = offset
        start = max(0, end - int(variables["last"]))
        edges = [
            {
                "cursor": cursor(offset),
                "node": self.node(
                    *rows[offset], str(variables["readingDirectionType"])
                ),
            }
            for offset in range(start, end)
        ]
        start_cursor = cursor(start) if edges else None
        end_cursor = cursor(end - 1) if edges else None
        for issued in (start_cursor, end_cursor):
            if issued is not None:
                self.cursors.add(issued)
                self._issued.add((issued, arguments))
        self.cursors.update(str(edge["cursor"]) for edge in edges)
        return {
            "data": {
                "account": {
                    "id": "1",
                    "property": {
                        "id": variables.get("propertyId", PROPERTY_ID),
                        "measurements": {
                            "pageInfo": {
                                "hasNextPage": False,
                                "hasPreviousPage": start > 0,
                                "startCursor": start_cursor,
                                "endCursor": end_cursor,
                            },
                            "edges": edges,
                        },
                    },
                }
            }
        }

    async def async_get_measurements(self, **kwargs: Any) -> MeasurementPage:
        """Stand in for the client's call, parsing the answer as the client does."""
        client = MeridianApiClient(
            MagicMock(),
            tokens=MeridianTokenSet(
                id_token="synthetic-id-token",
                refresh_token="synthetic-refresh-token",
                expires_at=datetime.now(UTC) + timedelta(days=1),
                user_id="synthetic-user",
            ),
        )

        async def answer(
            operation: str, _query: str, variables: dict[str, Any], **_options: Any
        ) -> dict[str, Any]:
            body = self.graphql(variables)
            if "errors" in body:
                codes = tuple(
                    error["extensions"]["errorCode"] for error in body["errors"]
                )
                if RATE_LIMIT_CODE in codes:
                    raise MeridianRateLimitError(DEFAULT_RETRY_AFTER_SECONDS)
                raise MeridianGraphQLError(operation, codes)
            data: dict[str, Any] = body["data"]
            return data

        client._async_graphql = answer  # type: ignore[method-assign,assignment]
        return await client.async_get_measurements(**kwargs)

    def payload_dates(self) -> set[str]:
        """Return every date the history or the requests held, in either zone."""
        dates = set(self.sent_dates)
        for start, _channel in self.history():
            dates.add(start.date().isoformat())
            dates.add(start.astimezone(NZ).date().isoformat())
        return dates


def _errors(code: str) -> dict[str, Any]:
    """Return a GraphQL error body with one code and a free-text message."""
    return {
        "data": None,
        "errors": [
            {
                "message": "Synthetic message for A-TEST-1 at 1 Synthetic Street",
                "extensions": {"errorCode": code},
            }
        ],
    }
