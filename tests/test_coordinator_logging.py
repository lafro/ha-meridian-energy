"""Tests for the coordinator's request logs, summaries and failure keys."""

from __future__ import annotations

import logging
import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientConnectionError
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.translation import async_get_translations
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy import transport as transport_module
from custom_components.meridian_energy.api import (
    MeridianApiClient,
    MeridianConnectionError,
    MeridianUpstreamError,
)
from custom_components.meridian_energy.const import DOMAIN, FIRST_SYNC_RETRY_SECONDS
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeasurementPage,
    MeridianAccount,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianProperty,
    MeridianTokenSet,
    SyncMode,
)
from custom_components.meridian_energy.statistics import (
    consumption_ids,
    generation_ids,
    property_key,
)
from custom_components.meridian_energy.transport import MeridianHttpError

from . import meridian_http
from .kraken_fake import FakeKraken
from .meridian_http import FakeMeridianSession, Response, json_response
from .walks import fetch_since

# 12:30 UTC is 00:30 on 17 July in New Zealand, so the NZ date and the UTC
# date differ, and a log that leaked either would show it.
_NOW = datetime(2026, 7, 16, 12, 30, tzinfo=UTC)
_ACCOUNT = "A-TEST-1"
_PROPERTY = "synthetic-property"
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_LOGGER_NAME = coordinator_module.__name__


def _account(*, feed_in: bool = False) -> MeridianAccount:
    return MeridianAccount(
        number=_ACCOUNT,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id=_PROPERTY,
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("synthetic-meter", "synthetic-icp", feed_in),
                ),
            ),
        ),
    )


def _row(start: datetime, *, direction: str = "CONSUMPTION") -> MeridianMeasurement:
    return MeridianMeasurement(
        start=start,
        end=start + timedelta(hours=1),
        value_kwh=Decimal("0.5"),
        quality="ACTUAL",
        direction=direction,
        channel_id="synthetic-channel",
        cost_cents=Decimal(15),
    )


class _Clock:
    """A monotonic clock each reading moves on by a fixed step."""

    def __init__(self, step: float) -> None:
        self.now = 1000.0
        self.step = step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


def _records(caplog: pytest.LogCaptureFixture, prefix: str) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == _LOGGER_NAME and record.getMessage().startswith(prefix)
    ]


@pytest.mark.parametrize(
    ("error", "key", "detail"),
    [
        (MeridianUpstreamError(503, gateway=False), "upstream_unavailable", "HTTP 503"),
        (MeridianUpstreamError(504, gateway=True), "upstream_timeout", "HTTP 504"),
        (
            MeridianUpstreamError(None, gateway=True),
            "upstream_timeout",
            "no answer within the time limit",
        ),
    ],
)
@pytest.mark.asyncio
async def test_upstream_failures_have_their_own_keys(
    hass, error: MeridianUpstreamError, key: str, detail: str
) -> None:
    """A gateway timeout or a service error is no longer a connection failure."""
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=error)
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(UpdateFailed) as polled:
        await coordinator.async_fetch_and_import()
    with pytest.raises(ConfigEntryNotReady) as prepared:
        await coordinator.async_prepare_topology()

    for raised in (polled.value, prepared.value):
        assert raised.translation_key == key
        assert raised.translation_placeholders == {"detail": detail}
        assert raised.__cause__ is error
    assert polled.value.retry_after == FIRST_SYNC_RETRY_SECONDS


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (
            MeridianUpstreamError(504, gateway=True),
            (
                "Meridian's service didn't answer in time (HTTP 504). "
                "Home Assistant will try again automatically"
            ),
        ),
        (
            MeridianUpstreamError(524, gateway=True),
            (
                "Meridian's service didn't answer in time (HTTP 524). "
                "Home Assistant will try again automatically"
            ),
        ),
        (
            MeridianUpstreamError(None, gateway=True),
            (
                "Meridian's service didn't answer in time (no answer within the time "
                "limit). Home Assistant will try again automatically"
            ),
        ),
        (
            MeridianUpstreamError(503, gateway=False),
            (
                "Meridian's service returned an error (HTTP 503). "
                "Home Assistant will try again automatically"
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_upstream_messages_give_what_failed(
    hass, error: MeridianUpstreamError, message: str
) -> None:
    """
    A gateway timeout, a request that got no answer and a service error differ.

    Both gateway timeouts, Meridian's 504 and Cloudflare's 524, give their
    status.
    """
    # Home Assistant renders messages of loaded integrations, without the
    # final full stop.
    hass.config.components.add(DOMAIN)
    await async_get_translations(hass, "en", "exceptions", {DOMAIN})
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=error)
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(UpdateFailed) as polled:
        await coordinator.async_fetch_and_import()

    assert str(polled.value) == message


@pytest.mark.asyncio
async def test_each_measurements_page_is_logged_with_relative_offsets(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    One debug line per page: what was asked, what came back, and how long.

    Rows are placed by hour offsets from now and the window's dates by NZ-day
    offsets from today, so the log carries no date, cursor or identifier.
    ``kept`` counts each hour and channel once, on the page that first had
    it, so the pages' counts add up to the rows the fetch kept: a second
    reading for an hour already kept, such as an estimate after the actual
    reading, is not counted again. ``before_window`` counts rows older than
    the window's first hour, and ``edge_hours`` whether the window's first
    and last hours have come back in any of its pages so far.
    """
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    newest = _NOW.replace(minute=0) - timedelta(hours=1)
    since = _NOW - timedelta(hours=48)

    def estimate(hour: int) -> MeridianMeasurement:
        return replace(_row(newest - timedelta(hours=hour)), quality="ESTIMATE")

    first_page = (
        *(_row(newest - timedelta(hours=hour)) for hour in range(30)),
        _row(newest + timedelta(hours=3)),  # a future estimate, not kept
        estimate(1),  # after the actual reading on the same page
    )
    second_page = (
        estimate(40),  # replaced by the actual reading below
        *(_row(newest - timedelta(hours=hour)) for hour in range(30, 50)),
        estimate(29),  # after the actual reading on the first page
    )
    client = MagicMock()
    client.async_get_measurements = AsyncMock(
        side_effect=[
            MeasurementPage(first_page, True, "YXJyYXljb25uZWN0aW9uOjMw"),
            MeasurementPage(second_page, True, "YXJyYXljb25uZWN0aW9uOjUw"),
        ]
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        patch.object(coordinator_module, "monotonic", side_effect=_Clock(1.5)),
    ):
        result = await fetch_since(
            coordinator,
            account_number=_ACCOUNT,
            property_id=_PROPERTY,
            direction="CONSUMPTION",
            since=since,
            mode=SyncMode.RESTART,
            position=coordinator_module._Position(1, 2, 2, 3),
        )

    assert len(result.measurements) == 47
    assert {item.quality for item in result.measurements} == {"ACTUAL"}
    first, second = _records(caplog, "Meridian measurements page:")
    assert first.levelno == logging.DEBUG
    # One window, from 2 days ago to today: 72 hours, so 94 rows a page.
    # Offsets count whole hours from the start of the current hour. The two
    # oldest rows came before the window, which ends it.
    assert first.getMessage() == (
        "Meridian measurements page: mode=restart direction=CONSUMPTION "
        "account=1/2 property=2/3 window=1/1 page=1 bounds=-2d..0d before=no "
        "last=94 rows=32 kept=30 before_window=0 edge_hours=first:no,last:no "
        "oldest=-30h newest=+2h more=yes elapsed=1.5s"
    )
    assert second.getMessage() == (
        "Meridian measurements page: mode=restart direction=CONSUMPTION "
        "account=1/2 property=2/3 window=1/1 page=2 bounds=-2d..0d before=yes "
        "last=94 rows=22 kept=17 before_window=2 edge_hours=first:yes,last:no "
        "oldest=-50h newest=-30h more=yes elapsed=1.5s"
    )
    assert client.async_get_measurements.await_count == 2
    for record in (first, second):
        message = record.getMessage()
        assert not _DATE.search(message)
        for private in (_ACCOUNT, _PROPERTY, "YXJyYXljb25uZWN0aW9u", "synthetic"):
            assert private not in message


@pytest.mark.asyncio
async def test_an_empty_page_is_logged_without_offsets(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    client = MagicMock()
    client.async_get_measurements = AsyncMock(
        return_value=MeasurementPage((), False, None)
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with patch.object(coordinator_module, "_utcnow", return_value=_NOW):
        await fetch_since(
            coordinator,
            account_number=_ACCOUNT,
            property_id=_PROPERTY,
            direction="GENERATION",
            since=_NOW - timedelta(days=1),
        )

    (record,) = _records(caplog, "Meridian measurements page:")
    assert (
        "mode=- direction=GENERATION account=- property=- window=1/1 page=1 "
        "bounds=-1d..0d before=no last=64 rows=0 kept=0 before_window=0 "
        "edge_hours=first:no,last:no oldest=- newest=- more=no elapsed="
    ) in record.getMessage()


@pytest.mark.parametrize(
    ("error", "kind", "status"),
    [
        (MeridianUpstreamError(504, gateway=True), "MeridianUpstreamError", "504"),
        (MeridianUpstreamError(None, gateway=True), "MeridianUpstreamError", "-"),
        (ValueError("Expected an object for measurements"), "ValueError", "-"),
    ],
)
@pytest.mark.asyncio
async def test_a_failed_page_is_logged_with_its_kind_and_status(
    hass,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    kind: str,
    status: str,
) -> None:
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    newest = _NOW.replace(minute=0) - timedelta(hours=1)
    client = MagicMock()
    client.async_get_measurements = AsyncMock(
        side_effect=[MeasurementPage((_row(newest),), True, "cursor-1"), error]
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        patch.object(coordinator_module, "monotonic", side_effect=_Clock(30.0)),
        pytest.raises(type(error)),
    ):
        await fetch_since(
            coordinator,
            account_number=_ACCOUNT,
            property_id=_PROPERTY,
            direction="CONSUMPTION",
            since=_NOW - timedelta(days=90),
            mode=SyncMode.INITIAL,
            position=coordinator_module._Position(1, 1, 1, 1),
        )

    (record,) = _records(caplog, "Meridian measurements page failed:")
    assert record.levelno == logging.DEBUG
    # Polling resends no gateway timeout: the next poll is the retry.
    assert record.getMessage() == (
        "Meridian measurements page failed: mode=initial direction=CONSUMPTION "
        "account=1/1 property=1/1 window=1/13 page=2 bounds=-6d..0d before=yes "
        f"last=214 kind={kind} status={status} action=stop elapsed=30.0s"
    )
    assert record.exc_info is None


@pytest.mark.asyncio
async def test_a_page_with_ambiguous_rows_is_logged_as_failed(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """A page whose rows fail the channel checks is a failed page too."""
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    hour = _NOW.replace(minute=0) - timedelta(hours=2)
    client = MagicMock()
    client.async_get_measurements = AsyncMock(
        return_value=MeasurementPage(
            (_row(hour), replace(_row(hour), value_kwh=Decimal("0.7"))), False, None
        )
    )
    coordinator = MeridianDataCoordinator(hass, client)

    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        pytest.raises(ValueError, match="ambiguous"),
    ):
        await fetch_since(
            coordinator,
            account_number=_ACCOUNT,
            property_id=_PROPERTY,
            direction="CONSUMPTION",
            since=_NOW - timedelta(days=1),
        )

    assert not _records(caplog, "Meridian measurements page:")
    (record,) = _records(caplog, "Meridian measurements page failed:")
    assert (
        " window=1/1 page=1 bounds=-1d..0d before=no last=64 kind=ValueError "
        "status=- action=stop "
    ) in record.getMessage()


@pytest.mark.asyncio
async def test_failed_page_status_is_read_from_the_cause(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """A rejected request names its HTTP status, found behind the API error."""
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    error = MeridianConnectionError("Meridian service returned HTTP 404")
    error.__cause__ = MeridianHttpError(404, {})
    client = MagicMock()
    client.async_get_measurements = AsyncMock(side_effect=error)
    coordinator = MeridianDataCoordinator(hass, client)

    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        pytest.raises(MeridianConnectionError),
    ):
        await fetch_since(
            coordinator,
            account_number=_ACCOUNT,
            property_id=_PROPERTY,
            direction="CONSUMPTION",
            since=_NOW - timedelta(days=1),
        )

    (record,) = _records(caplog, "Meridian measurements page failed:")
    assert " kind=MeridianConnectionError status=404 " in record.getMessage()


def _paged_client(*, feed_in: bool = False) -> MagicMock:
    """
    Return a client whose history began a day ago.

    So an initial walk gets readings from its newest window only, and stops
    after the two empty windows before it, at the start of the history.
    """
    kraken = FakeKraken(_NOW, days=1)
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(feed_in=feed_in),))
    client.async_get_billing_period = AsyncMock(side_effect=AssertionError)
    client.async_get_measurements = AsyncMock(side_effect=kraken.async_get_measurements)
    return client


class _RequestClock:
    """A clock for the transport, moved on by the synthetic service's answers."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _http_client(
    clock: _RequestClock, *, seconds: dict[str, float], expired: bool = False
) -> MeridianApiClient:
    """Return the real client in front of a synthetic service with a feed-in meter."""
    session = FakeMeridianSession(_NOW, days=2)
    account = meridian_http.account_data()
    meter = account["properties"][0]["meterPoints"][0]
    meter["registers"][0]["isFeedIn"] = True

    def answer(operation: str, variables: dict[str, object]) -> Response | None:
        clock.now += seconds.get(
            f"{operation}:{variables.get('readingDirectionType')}",
            seconds.get(operation, 0.0),
        )
        if operation == "account":
            return json_response({"data": {"account": account}})
        return None

    session.hook = answer
    tokens = MeridianTokenSet(
        id_token="synthetic-id-token",
        refresh_token="synthetic-refresh",
        # The client checks expiry against the real clock.
        expires_at=datetime.now(UTC) + timedelta(days=-1 if expired else 1),
        user_id="synthetic-user",
    )
    return MeridianApiClient(session, tokens=tokens)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_sync_summary_counts_http_requests_and_their_median_times(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Requests are counted as sent, not as calls the coordinator made.

    The account list and the account each send a request, and an expired
    session sends a refresh first. Measurements get a median of their own,
    which the setup time limit is tuned from.
    """
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    clock = _RequestClock()
    client = _http_client(
        clock,
        seconds={
            "accountsList": 1.0,
            "account": 1.0,
            "measurements:CONSUMPTION": 30.0,
            "measurements:GENERATION": 50.0,
        },
        expired=True,
    )
    coordinator = MeridianDataCoordinator(hass, client)
    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        patch.object(transport_module, "monotonic", side_effect=clock),
        patch.object(
            coordinator_module,
            "async_latest_numeric_statistic_start",
            new=AsyncMock(return_value=None),
        ),
        patch.object(
            coordinator_module,
            "async_replay",
            new=AsyncMock(return_value=(24, 24)),
        ),
    ):
        data = await coordinator.async_fetch_and_import(setup_import=True)

    # The refresh (0 s), the account list and the account (1 s each), and
    # three windows in each direction (30 s and 50 s each): the history began
    # 2 days ago, so the two windows before the newest are empty.
    assert data.request_count == 9
    assert data.direction_count == 2
    assert data.median_request_seconds == pytest.approx(30.0)
    assert data.median_measurements_seconds == pytest.approx(40.0)
    assert coordinator.request_count == 9
    assert coordinator.median_request_seconds == pytest.approx(30.0)
    (record,) = _records(caplog, "Meridian sync completed:")
    assert record.levelno == logging.DEBUG
    message = record.getMessage()
    assert "mode=initial " in message
    assert (
        " directions=2 requests=9 median_request=30.0s median_measurements=40.0s "
        "pages=6 "
    ) in message
    assert (
        " windows_ok=2 windows_empty=4 windows_failed=0 coverage_windows=0 "
        "revision_windows=0 retries=0 splits=0 history_start=yes units_failed=0 "
        "units_degraded=0 units_deferred=0 "
    ) in message
    assert not _DATE.search(message)


@pytest.mark.asyncio
async def test_a_failed_sync_counts_its_failed_request(hass) -> None:
    clock = _RequestClock()
    client = _http_client(clock, seconds={"accountsList": 2.0})
    session = client._transport._session  # type: ignore[attr-defined]
    answer = session.hook

    def fail_the_account_list(
        operation: str, variables: dict[str, object]
    ) -> Response | None:
        answer(operation, variables)
        if operation == "accountsList":
            return Response(b"", error=ClientConnectionError("synthetic reset"))
        return None

    session.hook = fail_the_account_list
    coordinator = MeridianDataCoordinator(hass, client)

    with (
        patch.object(transport_module, "monotonic", side_effect=clock),
        pytest.raises(UpdateFailed),
    ):
        await coordinator.async_fetch_and_import()

    assert coordinator.request_count == 1
    assert coordinator.median_request_seconds == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_a_sync_that_sent_no_request_has_no_median(hass) -> None:
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=MeridianConnectionError)
    coordinator = MeridianDataCoordinator(hass, client)

    with pytest.raises(UpdateFailed):
        await coordinator.async_fetch_and_import()

    assert coordinator.request_count == 0
    assert coordinator.median_request_seconds is None


@pytest.mark.asyncio
async def test_setup_import_can_leave_out_billing(hass) -> None:
    """
    The setup import fetches no billing metadata and computes no bill totals.

    Its coordinator is discarded once the entry exists, so that work only
    added a request and a recorder wait that could fail a finished import.
    """
    client = _paged_client()
    coordinator = MeridianDataCoordinator(hass, client)
    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        patch.object(
            coordinator_module,
            "async_latest_numeric_statistic_start",
            new=AsyncMock(return_value=None),
        ),
        patch.object(
            coordinator_module,
            "async_replay",
            new=AsyncMock(return_value=(24, 24)),
        ),
        patch.object(
            coordinator_module, "async_account_period_totals", new=AsyncMock()
        ) as totals,
    ):
        data = await coordinator.async_fetch_and_import(setup_import=True)

    assert data.account_results == ()
    client.async_get_billing_period.assert_not_awaited()
    totals.assert_not_awaited()
    assert coordinator.last_request is not None
    assert coordinator.last_request.operation == "measurements"


@pytest.mark.asyncio
async def test_coordinator_reports_the_newest_hour_it_imported(hass) -> None:
    """The setup checks these hours are readable before the entry exists."""
    client = _paged_client(feed_in=True)
    coordinator = MeridianDataCoordinator(hass, client)
    key = property_key(_ACCOUNT, _PROPERTY)
    importer = AsyncMock(side_effect=[(24, 24), (0, 0)])
    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        patch.object(
            coordinator_module,
            "async_latest_numeric_statistic_start",
            new=AsyncMock(return_value=None),
        ),
        patch.object(coordinator_module, "async_replay", importer),
    ):
        await coordinator.async_fetch_and_import(setup_import=True)

    # Generation wrote nothing (its running sum had no numeric baseline), so
    # there is nothing of it to look for.
    assert coordinator.imported_newest == {
        consumption_ids(key)[0]: _NOW.replace(minute=0) - timedelta(hours=1)
    }
    assert generation_ids(key)[0] not in coordinator.imported_newest


@pytest.mark.asyncio
async def test_first_sync_waits_for_the_recorder_before_reading_statistics(
    hass,
) -> None:
    """
    An entry's first sync reads no statistics until the recorder has caught up.

    Rows that setup, or the entry's earlier coordinator, queued may still be
    unsaved: read then, the sync would find no history and import over them
    with running sums from zero. It waits in two passes, so an import the
    recorder had to queue again behind the first is saved too.
    """
    client = _paged_client()
    client.async_get_billing_period = AsyncMock(return_value=None)
    coordinator = MeridianDataCoordinator(hass, client)
    order: list[str] = []

    async def wait(_hass: object, *, attempts: int, passes: int) -> bool:
        order.append(f"wait x{attempts} passes={passes}")
        return True

    async def latest(_hass: object, _statistic_id: str) -> None:
        order.append("read")

    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        patch.object(coordinator_module, "async_wait_for_recorder", wait),
        patch.object(
            coordinator_module, "async_latest_numeric_statistic_start", latest
        ),
        patch.object(
            coordinator_module,
            "async_replay",
            new=AsyncMock(return_value=(24, 24)),
        ),
    ):
        await coordinator.async_fetch_and_import()
        # Later syncs read what this coordinator itself wrote: no wait.
        await coordinator.async_fetch_and_import()

    assert order[0] == (
        f"wait x{coordinator_module.FIRST_SYNC_RECORDER_WAITS} passes=2"
    )
    assert order.count(order[0]) == 1
    assert "read" in order


@pytest.mark.asyncio
async def test_first_sync_duration_leaves_out_the_wait_for_the_recorder(
    hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A first sync that waited for a busy recorder does not look like a slow sync.

    Its duration, in diagnostics and in the summary line, times the requests
    and the import; the wait for the recorder, up to five minutes, is Home
    Assistant's, not Meridian's.
    """
    caplog.set_level(logging.DEBUG, logger="custom_components.meridian_energy")
    clock = _RequestClock()
    client = _paged_client()
    client.async_get_billing_period = AsyncMock(return_value=None)
    page = client.async_get_measurements.side_effect

    async def slow_page(**kwargs: object) -> MeasurementPage:
        clock.now += 10.0
        return await page(**kwargs)

    async def busy_recorder(_hass: object, *, attempts: int, passes: int) -> bool:
        del attempts, passes
        clock.now += 120.0
        return True

    client.async_get_measurements.side_effect = slow_page
    coordinator = MeridianDataCoordinator(hass, client)
    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(coordinator_module, "async_wait_for_recorder", busy_recorder),
        patch.object(
            coordinator_module,
            "async_latest_numeric_statistic_start",
            new=AsyncMock(return_value=None),
        ),
        patch.object(
            coordinator_module,
            "async_replay",
            new=AsyncMock(return_value=(24, 24)),
        ),
    ):
        data = await coordinator.async_fetch_and_import()

    # Three windows: the newest, and the two empty ones before it.
    assert data.sync_duration_seconds == pytest.approx(30.0)
    (record,) = _records(caplog, "Meridian sync completed:")
    assert " duration=30.00s " in record.getMessage()


@pytest.mark.asyncio
async def test_first_sync_stops_and_retries_while_the_recorder_is_behind(
    hass,
) -> None:
    client = _paged_client()
    coordinator = MeridianDataCoordinator(hass, client)

    with (
        patch.object(
            coordinator_module,
            "async_wait_for_recorder",
            new=AsyncMock(return_value=False),
        ),
        patch.object(
            coordinator_module, "async_latest_numeric_statistic_start", new=AsyncMock()
        ) as latest,
        pytest.raises(UpdateFailed) as raised,
    ):
        await coordinator.async_fetch_and_import()

    assert raised.value.translation_key == "recorder_busy"
    assert raised.value.retry_after == FIRST_SYNC_RETRY_SECONDS
    latest.assert_not_awaited()
    client.async_get_accounts.assert_not_awaited()
    client.async_get_measurements.assert_not_awaited()


@pytest.mark.asyncio
async def test_setup_import_leaves_the_recorder_wait_to_the_flow(hass) -> None:
    """The flow waits for the recorder itself just before it starts the import."""
    client = _paged_client()
    coordinator = MeridianDataCoordinator(hass, client)
    with (
        patch.object(coordinator_module, "_utcnow", return_value=_NOW),
        patch.object(
            coordinator_module, "async_wait_for_recorder", new=AsyncMock()
        ) as wait,
        patch.object(
            coordinator_module,
            "async_latest_numeric_statistic_start",
            new=AsyncMock(return_value=None),
        ),
        patch.object(
            coordinator_module,
            "async_replay",
            new=AsyncMock(return_value=(24, 24)),
        ),
    ):
        await coordinator.async_fetch_and_import(setup_import=True)

    wait.assert_not_awaited()
