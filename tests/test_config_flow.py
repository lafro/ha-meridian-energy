"""Tests for the Meridian Energy config flow."""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import re
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any, Self
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import probatio
import pytest
from homeassistant.components.recorder import Recorder, get_instance
from homeassistant.components.recorder import statistics as recorder_statistics
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    clear_statistics,
    statistics_during_period,
)
from homeassistant.components.recorder.tasks import ClearStatisticsTask, RecorderTask
from homeassistant.config_entries import (
    SOURCE_REAUTH,
    SOURCE_RECONFIGURE,
    SOURCE_USER,
    ConfigFlowResult,
)
from homeassistant.const import CONF_EMAIL, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.data_entry_flow import (
    EVENT_DATA_ENTRY_FLOW_PROGRESS_UPDATE,
    EVENT_DATA_ENTRY_FLOW_PROGRESSED,
    FlowResultType,
)
from homeassistant.exceptions import ConfigEntryAuthFailed, HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import selector
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_capture_events,
)
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)
from sqlalchemy.exc import OperationalError

import custom_components.meridian_energy as integration_module
from custom_components.meridian_energy import api as api_module
from custom_components.meridian_energy import config_flow as config_flow_module
from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy import transport as transport_module
from custom_components.meridian_energy.api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianGraphQLError,
    MeridianOtpError,
    MeridianRateLimitError,
    MeridianUpstreamError,
)
from custom_components.meridian_energy.config_flow import (
    MeridianEnergyConfigFlow,
    _account_label,
    _describe_call,
    _describe_failure,
    _root_name,
)
from custom_components.meridian_energy.const import (
    CONF_AUTO_ADD_ACCOUNTS,
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONF_SELECTED_ACCOUNTS,
    DOMAIN,
    NAME,
    SETUP_HANDOFF,
)
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeasurementPage,
    MeridianAccount,
    MeridianBillingPeriod,
    MeridianMeasurement,
    MeridianMeterPoint,
    MeridianProperty,
    MeridianSyncData,
    MeridianTokenSet,
    RequestContext,
    SyncMode,
)
from custom_components.meridian_energy.parsers import parse_datetime
from custom_components.meridian_energy.statistics import (
    StatisticsWriteError,
    _cost_metadata,
    _energy_metadata,
    async_has_statistics,
    async_replay,
    consumption_ids,
    generation_ids,
    property_key,
)
from custom_components.meridian_energy.transport import (
    MeridianHttpError,
    MeridianTimeoutError,
    MeridianTransportError,
    MeridianUnreadableResponseError,
)
from custom_components.meridian_energy.windows import NZ

from .kraken_fake import FakeKraken
from .meridian_http import (
    STALLED_URL,
    FakeMeridianSession,
    Response,
    StalledConnection,
    json_response,
    stalled_session,
)
from .recorder_hold import RecorderHold

# The integration's own entry setup, kept before any test patches it.
_REAL_SETUP_ENTRY = integration_module.async_setup_entry


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(
    recorder_mock: object, enable_custom_integrations: None
) -> None:
    """Set up recorder before enabling this recorder-dependent integration."""


def _tokens() -> MeridianTokenSet:
    return MeridianTokenSet(
        id_token="synthetic-id",
        refresh_token="synthetic-refresh",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        user_id="synthetic-user",
    )


def _account(number: str = "synthetic-account") -> MeridianAccount:
    return MeridianAccount(
        number=number,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id=f"property-{number}",
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint(
                        id=f"meter-{number}",
                        market_identifier=f"icp-{number}",
                        has_feed_in=False,
                    ),
                ),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_user_flow_success(hass) -> None:
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(return_value=_tokens())
    client.async_get_accounts = AsyncMock(return_value=(_account(),))

    import_started = asyncio.Event()
    finish_import = asyncio.Event()

    async def initial_import(
        tokens: MeridianTokenSet,
        selected_accounts: frozenset[str],
        time_limit: asyncio.Timeout | None,
    ) -> MeridianSyncData:
        assert tokens.refresh_token == "synthetic-refresh"
        assert selected_accounts == {"synthetic-account"}
        # The attempt's own time limit, which the import moves on by the
        # time it waits for the recorder.
        assert time_limit is not None
        import_started.set()
        await finish_import.wait()
        return MeridianSyncData(
            account_count=1,
            property_count=1,
            results=(),
            account_results=(),
            synced_at=datetime.now(UTC),
            sync_mode=SyncMode.INITIAL,
            topology_refreshed=True,
            topology_cache_age_seconds=0.0,
        )

    with (
        patch.object(
            MeridianEnergyConfigFlow,
            "_client",
            new_callable=PropertyMock,
            return_value=client,
        ),
        patch.object(
            MeridianEnergyConfigFlow,
            "_async_initial_import",
            side_effect=initial_import,
        ),
        patch.object(
            MeridianEnergyConfigFlow,
            "_authenticated_client",
            return_value=client,
        ),
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "user"
        email_field = next(iter(result["data_schema"].schema.values()))
        assert isinstance(email_field, selector.TextSelector)
        assert email_field.config["type"] == selector.TextSelectorType.EMAIL
        assert email_field.config["autocomplete"] == "email"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_EMAIL: " PERSON@Example.com "}
        )
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "otp"
        client.async_send_otp.assert_awaited_once()

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        assert result["step_id"] == "initial_import"
        assert result["progress_action"] == "initial_import"
        await import_started.wait()
        finish_import.set()
        await asyncio.sleep(0)
        result = await hass.config_entries.flow.async_configure(result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == NAME
    assert result["version"] == 3
    assert result["minor_version"] == 1
    assert result["data"][CONF_EMAIL] == "person@example.com"
    assert result["data"][CONF_REFRESH_TOKEN] == "synthetic-refresh"
    assert result["data"][CONF_FIREBASE_USER_ID] == "synthetic-user"
    assert result["data"][CONF_SELECTED_ACCOUNTS] == ["synthetic-account"]
    assert result["data"][CONF_AUTO_ADD_ACCOUNTS] is True
    assert "statistics_state_version" not in result["data"]
    assert "id_token" not in result["data"]
    assert "otp" not in result["data"]


_NOW = datetime(2026, 7, 16, 12, 30, tzinfo=UTC)
_HISTORY_START = (_NOW - timedelta(days=120)).replace(minute=0)
_TEST_ACCOUNT = "A-TEST-1"
_TEST_PROPERTY = "synthetic-property"
_TEST_EMAIL = "person@example.com"


def _test_account(*, feed_in: bool = False) -> MeridianAccount:
    return MeridianAccount(
        number=_TEST_ACCOUNT,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id=_TEST_PROPERTY,
                address="1 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint(
                        id="synthetic-meter",
                        market_identifier="synthetic-icp",
                        has_feed_in=feed_in,
                    ),
                ),
            ),
        ),
    )


class _Clock:
    """A monotonic clock the tests move forward by hand."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class _FakeMeridian:
    """
    A synthetic Meridian client with 120 days of hourly history.

    Measurements come from ``FakeKraken``, which builds each request's rows
    from its own window and checks the requests. Every call awaits once
    before answering or failing, because Home Assistant starts tasks eagerly
    and a real request never fails at once.
    """

    def __init__(self) -> None:
        self.account = _test_account()
        self.async_send_otp = AsyncMock()
        self.async_validate_otp = AsyncMock(return_value=_tokens())
        self.failure: Callable[[str, str | None], BaseException | None] = (
            lambda _direction, _before: None
        )
        self.blocked_direction: str | None = None
        # How many of the blocked direction's requests go through first:
        # every direction's newest window is asked for before any older one.
        self.blocked_after = 0
        self.release = asyncio.Event()
        self.measurement_calls: list[tuple[str, str | None]] = []
        self.billing_calls = 0
        self.kraken = FakeKraken(_NOW, history_start=_HISTORY_START, future_rows=False)

    async def async_get_accounts(self) -> tuple[MeridianAccount, ...]:
        await asyncio.sleep(0)
        return (self.account,)

    async def async_get_billing_period(
        self, account_number: str
    ) -> MeridianBillingPeriod:
        del account_number
        await asyncio.sleep(0)
        self.billing_calls += 1
        return MeridianBillingPeriod(
            period_length="MONTHLY",
            period_length_multiplier=1,
            is_fixed=False,
            start=None,
            end=None,
            next_billing_date=None,
            period_start_day=1,
        )

    async def async_get_measurements(self, **kwargs: Any) -> MeasurementPage:
        await asyncio.sleep(0)
        direction = str(kwargs["direction"])
        before = kwargs.get("before")
        self.measurement_calls.append((direction, before))
        if direction == self.blocked_direction and (
            self.calls(direction) > self.blocked_after
        ):
            await self.release.wait()
        if (error := self.failure(direction, before)) is not None:
            raise error
        return await self.kraken.async_get_measurements(**kwargs)

    def calls(self, direction: str) -> int:
        """Return how many measurements requests a direction has sent."""
        return sum(1 for sent, _before in self.measurement_calls if sent == direction)


def _gateway_timeout() -> MeridianUpstreamError:
    """Return the error the client raises for an HTTP 504, with its cause."""
    error = MeridianUpstreamError(504, gateway=True)
    error.__cause__ = MeridianHttpError(504, {})
    return error


@pytest.fixture
def clock() -> _Clock:
    return _Clock()


@pytest.fixture
def meridian(clock: _Clock) -> Iterator[_FakeMeridian]:
    """Route the flow's clients to the fake and fix both clocks."""
    fake = _FakeMeridian()
    with (
        patch.object(
            MeridianEnergyConfigFlow,
            "_client",
            new_callable=PropertyMock,
            return_value=fake,
        ),
        patch.object(
            MeridianEnergyConfigFlow, "_authenticated_client", return_value=fake
        ),
        patch(
            "custom_components.meridian_energy.coordinator._utcnow",
            return_value=_NOW,
        ),
        patch(
            "custom_components.meridian_energy.config_flow.monotonic",
            side_effect=clock,
        ),
        patch(
            "custom_components.meridian_energy.async_setup_entry",
            new=AsyncMock(return_value=True),
        ),
    ):
        yield fake


async def _submit_login(
    hass: HomeAssistant, email: str = _TEST_EMAIL
) -> ConfigFlowResult:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: email}
    )
    assert result["step_id"] == "otp"
    return await hass.config_entries.flow.async_configure(
        result["flow_id"], {"otp": "123456"}
    )


async def _finish_import(hass: HomeAssistant, flow_id: str) -> ConfigFlowResult:
    """Wait for the running import, then fetch the step the frontend would show."""
    await hass.async_block_till_done()
    return await hass.config_entries.flow.async_configure(flow_id)


async def _rows(hass: HomeAssistant, statistic_id: str) -> list[dict[str, Any]]:
    """Read a statistic's hourly rows once the recorder has written its queue."""
    # The recorder's own wait, not hass.async_block_till_done: that would also
    # wait for an import a test is holding open.
    await get_instance(hass).async_block_till_done()
    stats = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        _HISTORY_START - timedelta(days=1),
        None,
        {statistic_id},
        "hour",
        None,
        {"state", "sum"},
    )
    return list(stats.get(statistic_id, []))


def _import_failure_records(
    caplog: pytest.LogCaptureFixture,
) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.getMessage().startswith("Initial Meridian history import failed")
    ]


def _flow_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Return the WARNING records the config flow wrote."""
    return [
        record
        for record in caplog.records
        if record.name == config_flow_module.__name__
        and record.levelno == logging.WARNING
    ]


def _integration_warnings(
    caplog: pytest.LogCaptureFixture,
) -> list[logging.LogRecord]:
    """Return the WARNING records any module of the integration wrote."""
    return [
        record
        for record in caplog.records
        if record.name.startswith(f"custom_components.{DOMAIN}")
        and record.levelno >= logging.WARNING
    ]


class _SlowRemoval(RecorderTask):
    """A statistics removal that the recorder thread runs until released."""

    commit_before = False

    def __init__(self, statistic_ids: list[str]) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self._statistic_ids = statistic_ids

    def run(self, instance: Recorder) -> None:
        self.started.set()
        self.release.wait(30)
        clear_statistics(instance, self._statistic_ids)


async def _wait_until(condition: Callable[[], bool]) -> None:
    """Let other tasks run until a condition holds, as a request would."""
    for _ in range(1000):
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail("the condition never held")


async def _wait_for_progress_done(hass: HomeAssistant, flow_id: str) -> None:
    """
    Wait for the import to end without hass.async_block_till_done.

    That would also wait for the held recorder.
    """
    for _ in range(1000):
        step = hass.config_entries.flow._progress[flow_id].cur_step
        if step is not None and step["type"] is FlowResultType.SHOW_PROGRESS_DONE:
            return
        await asyncio.sleep(0.01)
    pytest.fail("the import did not end")


@pytest.mark.asyncio
async def test_import_failure_shows_retry_form_and_retry_creates_entry(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    A failed import reaches the user; a retry keeps the login and succeeds.

    In setup a gateway timeout is sent once more after its cooldown, so the
    first attempt fails only when the resend times out too.
    """
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    progressed = async_capture_events(hass, EVENT_DATA_ENTRY_FLOW_PROGRESSED)
    coordinators: list[MeridianDataCoordinator] = []
    real_coordinator = MeridianDataCoordinator

    def new_coordinator(*args: Any, **kwargs: Any) -> MeridianDataCoordinator:
        coordinator = real_coordinator(*args, **kwargs)
        coordinators.append(coordinator)
        return coordinator

    with patch(
        "custom_components.meridian_energy.config_flow.MeridianDataCoordinator",
        side_effect=new_coordinator,
    ):
        result = await _submit_login(hass)
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        flow_id = result["flow_id"]

        result = await _finish_import(hass, flow_id)
        assert [event.data["flow_id"] for event in progressed] == [flow_id]
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "import_failed"
        assert result["errors"] == {"base": "upstream_timeout"}
        detail = result["description_placeholders"]["detail"]
        assert detail == (
            "HTTP 504 while requesting grid import history, window 1 of 13, page 1 "
            "(account 1 of 1, property 1 of 1)"
        )
        assert len(meridian.measurement_calls) == 2

        meridian.failure = lambda _direction, _before: None
        clock.now += 121
        result = await hass.config_entries.flow.async_configure(flow_id, {})
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_SELECTED_ACCOUNTS] == [_TEST_ACCOUNT]
    meridian.async_send_otp.assert_awaited_once()
    meridian.async_validate_otp.assert_awaited_once()
    assert len(coordinators) == 2
    assert coordinators[0] is not coordinators[1]
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    assert len(await _rows(hass, energy_id)) == 90 * 24 - 1
    # The entry is handed what the attempt that succeeded remembered: the
    # direction working, and the rows of the last 14 days it imported.
    memory = hass.data[DOMAIN][SETUP_HANDOFF][_TEST_EMAIL].memory
    key = (property_key(_TEST_ACCOUNT, _TEST_PROPERTY), "CONSUMPTION")
    assert memory.unit_states[key].status == "ok"
    assert memory.unit_states[key].last_failure is None
    assert min(start for start, _channel in memory.measurements[key]) >= (
        _NOW - timedelta(days=14)
    )
    assert len(memory.measurements[key]) >= 13 * 24


@pytest.mark.asyncio
async def test_the_progress_ring_counts_the_weeks_imported(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """The setup dialog's progress moves on after each window of history."""
    meridian.account = _test_account(feed_in=True)
    updates = async_capture_events(hass, EVENT_DATA_ENTRY_FLOW_PROGRESS_UPDATE)

    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert {event.data["flow_id"] for event in updates} == {flow_id}
    assert {event.data["handler"] for event in updates} == {DOMAIN}
    # 13 windows for each direction.
    assert [event.data["progress"] for event in updates] == pytest.approx(
        [index / 26 for index in range(1, 27)]
    )


@pytest.mark.asyncio
async def test_one_transient_gateway_timeout_in_setup_still_completes(
    hass: HomeAssistant, meridian: _FakeMeridian, waits: AsyncMock
) -> None:
    """
    One 504 no longer fails setup: the week's days follow, 75 s later.

    The newest day alone is asked for after the cooldown, then the week's
    other days, and the walk goes back to weeks for the 12 others.
    """
    failures = {"remaining": 1}

    def time_out_once(_direction: str, _before: str | None) -> BaseException | None:
        if failures["remaining"]:
            failures["remaining"] -= 1
            return _gateway_timeout()
        return None

    meridian.failure = time_out_once
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert len(meridian.measurement_calls) == 1 + 7 + 12
    assert waits.await_args_list[0].args[0] == pytest.approx(75.0, abs=1.0)
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    assert len(await _rows(hass, energy_id)) == 90 * 24 - 1


@pytest.mark.asyncio
async def test_retry_waits_120_seconds_and_stops_after_five_attempts(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)
    assert result["errors"] == {"base": "upstream_timeout"}

    clock.now += 30
    result = await hass.config_entries.flow.async_configure(flow_id, {})
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "retry_too_soon"}
    assert result["description_placeholders"]["seconds"] == "90"
    # The gateway timeout, and its one resend after the cooldown.
    assert len(meridian.measurement_calls) == 2

    for attempt in range(2, 5):
        clock.now += 120
        result = await hass.config_entries.flow.async_configure(flow_id, {})
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        result = await _finish_import(hass, flow_id)
        assert result["errors"] == {"base": "upstream_timeout"}
        assert len(meridian.measurement_calls) == 2 * attempt

    clock.now += 120
    result = await hass.config_entries.flow.async_configure(flow_id, {})
    assert result["type"] is FlowResultType.SHOW_PROGRESS
    result = await _finish_import(hass, flow_id)

    # The fifth failure offers no retry, which could only abort: only to go
    # on without history, as Meridian's service did not answer in time. Its
    # error says no attempt is left, rather than to try again.
    detail = (
        "HTTP 504 while requesting grid import history, window 1 of 13, "
        "page 1 (account 1 of 1, property 1 of 1)"
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "import_failed_last"
    assert result["errors"] == {"base": "last_upstream_timeout"}
    assert _schema_fields(result) == ["continue_without_history"]
    assert result["description_placeholders"]["detail"] == detail

    # Submitted without it, setup ends, saying what failed.
    result = await hass.config_entries.flow.async_configure(flow_id, {})
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "too_many_attempts"
    assert result["description_placeholders"] == {"detail": detail}
    assert len(meridian.measurement_calls) == 10
    assert _flow_ids(hass) == set()
    meridian.async_validate_otp.assert_awaited_once()


@pytest.mark.asyncio
async def test_after_the_last_attempt_setup_can_still_go_on_without_history(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    Five gateway timeouts: no retry is left, but going on needs none.

    The choice waits as long as a retry would, and stays ticked meanwhile;
    setup then creates the entry with no history and no request more.
    """
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)
    for _attempt in range(4):
        clock.now += 120
        await hass.config_entries.flow.async_configure(flow_id, {})
        result = await _finish_import(hass, flow_id)
    assert result["step_id"] == "import_failed_last"
    sent = len(meridian.measurement_calls)

    result = await hass.config_entries.flow.async_configure(
        flow_id, {"continue_without_history": True}
    )
    assert result["step_id"] == "import_failed_last"
    assert result["errors"] == {"base": "continue_too_soon"}
    assert result["description_placeholders"]["seconds"] == "120"
    assert _schema_default(result, "continue_without_history") is True

    clock.now += 120
    result = await hass.config_entries.flow.async_configure(
        flow_id, {"continue_without_history": True}
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["description"] == "partial"
    assert len(meridian.measurement_calls) == sent
    handoff = hass.data[DOMAIN][SETUP_HANDOFF][_TEST_EMAIL]
    assert handoff.imported is False
    assert handoff.memory is None


@pytest.mark.asyncio
async def test_the_last_failure_form_says_no_attempt_is_left(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    Five service errors: the last form's error does not say to try again.

    Submitting without Continue without history ends setup there, so an
    error that said to wait and try again would end it for one who did.
    """
    meridian.failure = lambda _direction, _before: MeridianUpstreamError(
        503, gateway=False
    )
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)
    assert result["errors"] == {"base": "upstream_unavailable"}
    for _attempt in range(4):
        clock.now += 120
        await hass.config_entries.flow.async_configure(flow_id, {})
        result = await _finish_import(hass, flow_id)

    assert result["step_id"] == "import_failed_last"
    assert result["errors"] == {"base": "last_upstream_unavailable"}
    strings = json.loads(
        (Path(config_flow_module.__file__).parent / "strings.json").read_text()
    )["config"]["error"]
    for key in ("last_upstream_timeout", "last_upstream_unavailable"):
        assert "try again" not in strings[key]
        assert "no import attempts are left" in strings[key]
    assert "continuing" in strings["continue_too_soon"]


@pytest.mark.parametrize("last", [False, True], ids=["retry_left", "last_attempt"])
@pytest.mark.asyncio
async def test_going_on_without_history_pressed_twice_creates_one_entry(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock, last: bool
) -> None:
    """
    Enter pressed twice on Continue without history: one entry, no stale form.

    The first submission creates the entry, which Home Assistant sets up
    before the flow ends. The second waits for the first, as any does, and
    answers only once the flow has ended, with an abort, which reaches the
    dialog second and is dropped. Answered as soon as the first submission
    was handled, with the failure form, it reached the dialog first, which
    then stayed at a form whose Submit could only fail, the flow being gone.
    """
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)
    if last:
        for _attempt in range(4):
            clock.now += 120
            await hass.config_entries.flow.async_configure(flow_id, {})
            result = await _finish_import(hass, flow_id)
    assert result["step_id"] == ("import_failed_last" if last else "import_failed")
    clock.now += 121
    setting_up = asyncio.Event()
    finish_setup = asyncio.Event()

    async def slow_setup_entry(hass_: HomeAssistant, entry: object) -> bool:
        del hass_, entry
        setting_up.set()
        await finish_setup.wait()
        return True

    answers: list[ConfigFlowResult] = []

    async def press() -> None:
        answers.append(
            await hass.config_entries.flow.async_configure(
                flow_id, {"continue_without_history": True}
            )
        )

    with patch(
        "custom_components.meridian_energy.async_setup_entry", new=slow_setup_entry
    ):
        first = hass.async_create_task(press())
        second = hass.async_create_task(press())
        await setting_up.wait()
        await asyncio.sleep(0.05)
        # Neither answers while the entry is set up.
        assert answers == []
        finish_setup.set()
        await asyncio.gather(first, second)

    assert [answer["type"] for answer in answers] == [
        FlowResultType.CREATE_ENTRY,
        FlowResultType.ABORT,
    ]
    assert answers[1]["reason"] == "already_configured"
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    assert _flow_ids(hass) == set()


@pytest.mark.parametrize("last", [False, True], ids=["retry_left", "last_attempt"])
@pytest.mark.asyncio
async def test_a_press_while_the_new_entry_is_set_up_creates_nothing(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
    last: bool,
) -> None:
    """
    Continue without history pressed again once the first press has answered.

    The first press created the entry, which Home Assistant is setting up,
    so the flow is still at the failure form. A second press then, as
    Enter pressed again or a slow setup gives, repeats nothing: no
    rollback of the statistics the new entry's first update may already be
    writing, and no second entry, which would make Home Assistant replace
    the first. It answers once the flow has ended, with an abort the dialog
    drops.
    """
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)
    if last:
        for _attempt in range(4):
            clock.now += 120
            await hass.config_entries.flow.async_configure(flow_id, {})
            result = await _finish_import(hass, flow_id)
    assert result["step_id"] == ("import_failed_last" if last else "import_failed")
    clock.now += 121
    setting_up = asyncio.Event()
    finish_setup = asyncio.Event()
    setups: list[str] = []

    async def slow_setup_entry(hass_: HomeAssistant, entry: Any) -> bool:
        del hass_
        setups.append(entry.entry_id)
        setting_up.set()
        await finish_setup.wait()
        return True

    rollback = MeridianEnergyConfigFlow._async_rollback_complete
    rollbacks: list[object] = []

    async def counted_rollback(flow: MeridianEnergyConfigFlow) -> bool:
        rollbacks.append(flow)
        return await rollback(flow)

    answers: list[ConfigFlowResult] = []

    async def press() -> None:
        answers.append(
            await hass.config_entries.flow.async_configure(
                flow_id, {"continue_without_history": True}
            )
        )

    with (
        patch(
            "custom_components.meridian_energy.async_setup_entry",
            new=slow_setup_entry,
        ),
        patch.object(
            MeridianEnergyConfigFlow, "_async_rollback_complete", counted_rollback
        ),
    ):
        first = hass.async_create_task(press())
        await setting_up.wait()
        second = hass.async_create_task(press())
        await asyncio.sleep(0.05)
        assert answers == []
        finish_setup.set()
        await asyncio.gather(first, second)
        await hass.async_block_till_done()

    assert [answer["type"] for answer in answers] == [
        FlowResultType.CREATE_ENTRY,
        FlowResultType.ABORT,
    ]
    assert answers[1]["reason"] == "already_configured"
    assert len(setups) == 1
    assert len(rollbacks) == 1
    assert [entry.entry_id for entry in hass.config_entries.async_entries(DOMAIN)] == (
        setups
    )
    assert "same unique ID" not in caplog.text
    assert _flow_ids(hass) == set()


@pytest.mark.asyncio
async def test_the_finish_step_asked_again_during_the_entrys_setup_creates_nothing(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The import succeeded, and the finish step is asked for twice.

    The first answer creates the entry, which Home Assistant is setting up,
    so the flow is still at the import's end. The second, asked for then,
    creates no second entry, which would make Home Assistant replace the
    first: it answers once the flow has ended, with an abort.
    """
    del meridian
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    await hass.async_block_till_done()
    setting_up = asyncio.Event()
    finish_setup = asyncio.Event()
    setups: list[str] = []

    async def slow_setup_entry(hass_: HomeAssistant, entry: Any) -> bool:
        del hass_
        setups.append(entry.entry_id)
        setting_up.set()
        await finish_setup.wait()
        return True

    answers: list[ConfigFlowResult] = []

    async def finish() -> None:
        answers.append(await hass.config_entries.flow.async_configure(flow_id))

    with patch(
        "custom_components.meridian_energy.async_setup_entry", new=slow_setup_entry
    ):
        first = hass.async_create_task(finish())
        await setting_up.wait()
        second = hass.async_create_task(finish())
        await asyncio.sleep(0.05)
        assert answers == []
        finish_setup.set()
        await asyncio.gather(first, second)
        await hass.async_block_till_done()

    assert [answer["type"] for answer in answers] == [
        FlowResultType.CREATE_ENTRY,
        FlowResultType.ABORT,
    ]
    assert answers[1]["reason"] == "already_configured"
    assert [entry.entry_id for entry in hass.config_entries.async_entries(DOMAIN)] == (
        setups
    )
    assert "same unique ID" not in caplog.text


@pytest.mark.asyncio
async def test_after_the_last_attempt_other_failures_still_end_setup(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """Data Home Assistant cannot use: the updates would meet it too."""
    meridian.failure = lambda _direction, _before: MeridianGraphQLError(
        "measurements", ("UNKNOWN",)
    )
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)
    for _attempt in range(4):
        clock.now += 120
        await hass.config_entries.flow.async_configure(flow_id, {})
        result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "too_many_attempts"


@pytest.mark.asyncio
async def test_the_last_failure_form_needs_the_login_it_was_shown_for(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """A step reached without a failed import to go on from ends setup."""
    del meridian, clock
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    result = await flow.async_step_import_failed_last()
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "login_expired"


@pytest.mark.asyncio
async def test_rate_limited_import_waits_for_meridians_retry_after(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    calls = {"count": 0}

    def rate_limit_once(_direction: str, _before: str | None) -> BaseException | None:
        calls["count"] += 1
        return MeridianRateLimitError(600) if calls["count"] == 1 else None

    meridian.failure = rate_limit_once
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)
    assert result["errors"] == {"base": "import_rate_limited"}

    clock.now += 121
    result = await hass.config_entries.flow.async_configure(flow_id, {})
    assert result["errors"] == {"base": "retry_too_soon"}
    assert result["description_placeholders"]["seconds"] == "479"

    clock.now += 479
    result = await hass.config_entries.flow.async_configure(flow_id, {})
    assert result["type"] is FlowResultType.SHOW_PROGRESS
    result = await _finish_import(hass, flow_id)
    assert result["type"] is FlowResultType.CREATE_ENTRY


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ValueError("Expected an object for measurements"), "import_invalid_data"),
        # A query over Kraken's limits: a GraphQL error that is never taken
        # for dates before a supply.
        (MeridianGraphQLError("measurements", ("KT-CT-1188",)), "import_invalid_data"),
        (RuntimeError("unexpected"), "unknown"),
    ],
)
@pytest.mark.asyncio
async def test_import_failure_categories_reach_the_form(
    hass: HomeAssistant, meridian: _FakeMeridian, error: Exception, expected: str
) -> None:
    meridian.failure = lambda _direction, _before: error
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": expected}
    assert result["description_placeholders"]["detail"].startswith(
        f"{type(error).__name__} while requesting grid import history"
    )


class _TimedOutRequest:
    """A request that runs out of time, stopped as aiohttp's own limit stops it."""

    async def __aenter__(self) -> None:
        # The limit cancels the wait, then raises TimeoutError from that
        # cancellation, as aiohttp's timer does.
        async with asyncio.timeout(0):
            await asyncio.Event().wait()

    async def __aexit__(self, *_args: object) -> None:
        return None


class _UnreadableAnswer:
    """A successful answer whose body is not JSON, such as an HTML page."""

    def __init__(self) -> None:
        self.status = 200
        self.headers = {"Content-Type": "text/html"}

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def read(self) -> bytes:
        # The transport parses it with json, which raises its JSONDecodeError
        # "from None", over a StopIteration.
        return b"<html>synthetic page</html>"


async def _client_error(answer: object) -> MeridianConnectionError:
    """Return the error the real client and transport raise for an answer."""
    session = MagicMock()
    session.post.return_value = answer
    client = MeridianApiClient(session)
    with pytest.raises(MeridianConnectionError) as caught:
        await client._async_json_request(
            "https://example.test",
            operation="measurements",
            json={},
            authenticated=False,
        )
    return caught.value


@pytest.mark.parametrize(
    "case",
    [
        (
            _TimedOutRequest(),
            "upstream_timeout",
            "no answer within the time limit",
            "cause=MeridianUpstreamError root=TimeoutError status=-",
        ),
        (
            _UnreadableAnswer(),
            "import_cannot_connect",
            "an unreadable answer (HTTP 200, html)",
            "cause=MeridianConnectionError root=JSONDecodeError status=200",
        ),
    ],
    ids=["request-timeout", "unreadable-answer"],
)
@pytest.mark.asyncio
async def test_failure_names_the_error_behind_a_timeout_or_an_unreadable_answer(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    caplog: pytest.LogCaptureFixture,
    case: tuple[object, str, str, str],
) -> None:
    """
    The form and the warning name the error a traceback ends with.

    The time limit stops a request by cancelling it, and raises TimeoutError
    from that cancellation, so the chain ended with a CancelledError, which
    reads like a closed dialog. json hides the StopIteration behind its
    JSONDecodeError, as a traceback does too. The form describes each in
    words, and the warning's root names the error, never the cancellation or
    the StopIteration.
    """
    answer, error, detail, logged = case
    failure = await _client_error(answer)
    meridian.failure = lambda _direction, _before: failure
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["errors"] == {"base": error}
    assert result["description_placeholders"]["detail"] == (
        f"{detail} while requesting grid import history, window 1 of 13, page 1 "
        "(account 1 of 1, property 1 of 1)"
    )
    (record,) = _import_failure_records(caplog)
    assert logged in record.getMessage()


@pytest.mark.asyncio
async def test_synchronous_failure_is_not_retried_with_the_login_code_input(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    An import that fails before the progress step returns is not resubmitted.

    The flow manager passes the code step's input on to the step a finished
    progress step names, so without the own-form guard the code input would
    count as a retry and every retry would fail at once the same way.
    """
    del meridian
    calls = {"count": 0}

    async def fail_at_once(self: MeridianDataCoordinator, **kwargs: object) -> None:
        del self, kwargs
        calls["count"] += 1
        raise UpdateFailed(translation_domain=DOMAIN, translation_key="cannot_connect")

    with (
        patch.object(MeridianDataCoordinator, "async_fetch_and_import", fail_at_once),
        patch(
            "custom_components.meridian_energy.config_flow.async_clear_statistics",
            new=AsyncMock(return_value=True),
        ),
    ):
        result = await _submit_login(hass)

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "import_cannot_connect"}
    assert calls["count"] == 1
    # No Meridian call had started, so there is no call to name.
    assert result["description_placeholders"]["detail"] == "UpdateFailed"
    (record,) = _import_failure_records(caplog)
    assert "cause=- root=UpdateFailed status=- operation=-" in record.getMessage()


@pytest.mark.asyncio
async def test_import_authentication_failure_aborts_with_login_expired(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    meridian.failure = lambda _direction, _before: MeridianAuthenticationError()
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "login_expired"
    (record,) = _import_failure_records(caplog)
    assert "reason=login_expired" in record.getMessage()
    assert record.exc_info is not None
    assert record.exc_info[0] is ConfigEntryAuthFailed


@pytest.mark.asyncio
async def test_a_failed_import_names_the_request_of_the_failure_it_shows(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The form and the warning name the request whose failure they show.

    Grid import fails at its newest week with unusable data. Grid export,
    which walks after it, times out on its second week, and after the pause
    on that week's newest day too, the attempt's last request. The failure
    shown is grid import's, the first direction's, so its request is the
    one named, in flight when it failed, rather than grid export's last one.
    """
    meridian.account = _test_account(feed_in=True)

    def fail(direction: str, before: str | None) -> BaseException | None:
        del before
        if direction == "GENERATION":
            return _gateway_timeout() if meridian.calls(direction) > 1 else None
        if meridian.calls(direction) == 1:
            return MeridianGraphQLError("measurements", ("UNKNOWN",))
        return None

    meridian.failure = fail
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "import_invalid_data"}
    assert result["description_placeholders"]["detail"].startswith(
        "MeridianGraphQLError while requesting grid import history, window 1 of "
        "13, page 1"
    )
    assert meridian.measurement_calls[-1][0] == "GENERATION"
    (record,) = _import_failure_records(caplog)
    message = record.getMessage()
    assert "direction=CONSUMPTION" in message
    assert "window=1/13 page=1 bound=dates" in message
    assert "in_flight=yes" in message


@pytest.mark.asyncio
async def test_retry_after_partial_import_rebuilds_history_from_scratch(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    Attempt 1 imports consumption, then generation fails and is rolled back.

    The retry runs on a new coordinator. A reused one would remember the
    consumption history it imported and skip it, leaving no rows at all.
    """
    meridian.account = _test_account(feed_in=True)
    # A gateway timeout, and the resend after its cooldown.
    generation_failures = {"remaining": 2}

    def fail_generation_once(
        direction: str, before: str | None
    ) -> BaseException | None:
        del before
        if direction == "GENERATION" and generation_failures["remaining"]:
            generation_failures["remaining"] -= 1
            return _gateway_timeout()
        return None

    meridian.failure = fail_generation_once
    key = property_key(_TEST_ACCOUNT, _TEST_PROPERTY)
    energy_id, cost_id = consumption_ids(key)
    generation_id, credit_id = generation_ids(key)

    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)
    assert result["step_id"] == "import_failed"
    assert result["description_placeholders"]["detail"].startswith(
        "HTTP 504 while requesting grid export history, window 1 of 13, page 1"
    )
    for statistic_id in (energy_id, cost_id, generation_id, credit_id):
        assert await _rows(hass, statistic_id) == []

    clock.now += 120
    await hass.config_entries.flow.async_configure(flow_id, {})
    result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    for statistic_id in (energy_id, cost_id, generation_id, credit_id):
        rows = await _rows(hass, statistic_id)
        assert len(rows) == 90 * 24 - 1
        assert rows[0]["sum"] == pytest.approx(rows[0]["state"])
        for previous, current in pairwise(rows):
            assert current["sum"] - previous["sum"] == pytest.approx(current["state"])


@pytest.mark.parametrize(
    "removal_finished",
    [False, True],
    ids=["removal-still-queued", "rows-still-present"],
)
@pytest.mark.asyncio
async def test_retry_waits_while_the_rollback_is_unfinished(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    removal_finished: bool,
) -> None:
    """
    A retry waits for a removal that timed out, and for rows that remain.

    A removal still queued is pending even when no rows can be read: the
    recorder may not have committed the failed attempt's rows yet either.
    """
    meridian.account = _test_account(feed_in=True)
    meridian.failure = lambda direction, _before: (
        _gateway_timeout() if direction == "GENERATION" else None
    )
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    with patch(
        "custom_components.meridian_energy.config_flow.async_clear_statistics",
        new=AsyncMock(return_value=removal_finished),
    ) as clear:
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        await _finish_import(hass, flow_id)
        assert await _rows(hass, energy_id)

        clock.now += 120
        # A queued removal: the rows may not be committed yet, so none show.
        uncommitted: AbstractContextManager[object] = (
            nullcontext()
            if removal_finished
            else patch(
                "custom_components.meridian_energy.config_flow.async_has_statistics",
                new=AsyncMock(return_value=False),
            )
        )
        with uncommitted:
            result = await hass.config_entries.flow.async_configure(flow_id, {})

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cleanup_pending"}
    assert clear.await_count == 2
    # 13 windows of grid import, then grid export's first window and its resend.
    assert len(meridian.measurement_calls) == 15


@pytest.mark.asyncio
async def test_retry_waits_until_the_recorder_has_removed_the_failed_attempt(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    A removal queued behind other recorder work holds the retry back.

    Until the recorder catches up, the failed attempt's rows are not even
    committed, so only the removal's own result shows they are not gone. A
    retry started then could read them back as existing history while the
    recorder caught up, import two weeks instead of 90 days, and then lose
    those rows to the queued removal.
    """
    meridian.account = _test_account(feed_in=True)
    recorder = RecorderHold(hass)
    # A gateway timeout, and the resend after its cooldown.
    generation_failures = {"remaining": 2}

    def hold_then_fail_generation_once(
        direction: str, before: str | None
    ) -> BaseException | None:
        del before
        recorder.hold_once()
        if direction == "GENERATION" and generation_failures["remaining"]:
            generation_failures["remaining"] -= 1
            return _gateway_timeout()
        return None

    meridian.failure = hold_then_fail_generation_once
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    with (
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
        _unconfirmed_replays(),
    ):
        try:
            result = await _submit_login(hass)
            flow_id = result["flow_id"]
            await _wait_for_progress_done(hass, flow_id)
            result = await hass.config_entries.flow.async_configure(flow_id)
            assert result["step_id"] == "import_failed"
            # The failure's one warning says the removal is still queued.
            (record,) = _integration_warnings(caplog)
            assert record.getMessage().startswith(
                "Initial Meridian history import failed"
            )
            assert record.getMessage().endswith(" rollback=queued")

            clock.now += 120
            for _ in range(2):
                result = await hass.config_entries.flow.async_configure(flow_id, {})
                assert result["type"] is FlowResultType.FORM
                assert result["errors"] == {"base": "cleanup_pending"}
            # The form says so; each press does not log the removal again.
            assert _integration_warnings(caplog) == [record]
        finally:
            recorder.release()
        await get_instance(hass).async_block_till_done()
        statistics_module.CONFIRM_TIMEOUT_SECONDS = 30
        result = await hass.config_entries.flow.async_configure(flow_id, {})
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    rows = await _rows(hass, energy_id)
    assert len(rows) == 90 * 24 - 1
    assert rows[0]["sum"] == pytest.approx(rows[0]["state"])


@pytest.mark.asyncio
async def test_retry_waits_for_the_recorder_when_every_statistic_existed_before(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    A retry with nothing of its own to remove still waits for the recorder.

    When every statistic existed before setup, the rollback has nothing to
    remove, so it used to return without waiting. Rows the failed attempt
    added to those statistics could then still be queued behind other
    recorder work: the retry read the history without them and, once an hour
    had passed, started its window an hour after theirs. The recorder then
    wrote both, and that hour was missing from every later sum for good.
    """
    meridian.account = _test_account(feed_in=True)
    key = property_key(_TEST_ACCOUNT, _TEST_PROPERTY)
    energy_id, cost_id = consumption_ids(key)
    generation_id, credit_id = generation_ids(key)
    # Statistics kept from an earlier installation, last written 100 days ago.
    earlier = (_NOW - timedelta(days=100)).replace(minute=0)
    for metadata in (
        _energy_metadata(energy_id, "Meridian grid import"),
        _cost_metadata(cost_id, "Meridian grid import cost"),
        _energy_metadata(generation_id, "Meridian grid export"),
        _cost_metadata(credit_id, "Meridian grid export credit"),
    ):
        async_add_external_statistics(
            hass, metadata, [{"start": earlier, "state": 1.0, "sum": 1.0}]
        )
    await async_wait_recording_done(hass)
    recorder = RecorderHold(hass)
    # A gateway timeout, and the resend after its cooldown.
    generation_failures = {"remaining": 2}

    def hold_then_fail_generation_once(
        direction: str, before: str | None
    ) -> BaseException | None:
        del before
        recorder.hold_once()
        if direction == "GENERATION" and generation_failures["remaining"]:
            generation_failures["remaining"] -= 1
            return _gateway_timeout()
        return None

    meridian.failure = hold_then_fail_generation_once
    with (
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
        _unconfirmed_replays(),
    ):
        try:
            result = await _submit_login(hass)
            flow_id = result["flow_id"]
            await _wait_for_progress_done(hass, flow_id)
            result = await hass.config_entries.flow.async_configure(flow_id)
            assert result["step_id"] == "import_failed"

            clock.now += 120
            result = await hass.config_entries.flow.async_configure(flow_id, {})
            assert result["type"] is FlowResultType.FORM
            assert result["errors"] == {"base": "cleanup_pending"}
        finally:
            recorder.release()
        await get_instance(hass).async_block_till_done()
        statistics_module.CONFIRM_TIMEOUT_SECONDS = 30
        # The retry starts in the next hour, so its window starts an hour later.
        with patch(
            "custom_components.meridian_energy.coordinator._utcnow",
            return_value=_NOW + timedelta(hours=1),
        ):
            result = await hass.config_entries.flow.async_configure(flow_id, {})
            assert result["type"] is FlowResultType.SHOW_PROGRESS
            result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    for statistic_id in (energy_id, cost_id, generation_id, credit_id):
        rows = await _rows(hass, statistic_id)
        assert rows[0]["sum"] == pytest.approx(1.0)
        assert len(rows) > 89 * 24
        for previous, current in pairwise(rows):
            assert current["sum"] - previous["sum"] == pytest.approx(current["state"])


def _energy_import_retried_once(
    energy_id: str, writes: list[int], *, delay: float = 1.0
) -> AbstractContextManager[Any]:
    """
    Make the recorder queue a statistic's first import again, as on MySQL.

    The recorder's retryable database job returns False on a lock error (a
    lock-wait timeout or a deadlock, say), and the import task then queues
    itself again at the back of the queue. The retry is slow, so a reader
    not waiting behind it reads the history without its rows.
    """
    real_import = recorder_statistics.import_statistics

    def retried_once(
        instance: Recorder, metadata: Any, statistics: Any, table: Any
    ) -> bool:
        if metadata["statistic_id"] == energy_id:
            writes.append(len(writes))
            if len(writes) == 1:
                return False
            if len(writes) == 2:  # the retried write
                time.sleep(delay)
        return real_import(instance, metadata, statistics, table)

    return patch(
        "homeassistant.components.recorder.tasks.statistics.import_statistics",
        side_effect=retried_once,
    )


def _hold_after_replays(recorder: RecorderHold) -> AbstractContextManager[Any]:
    """
    Hold the recorder once the import's first replay has been confirmed.

    Each replay waits until the recorder has saved its writes before it
    returns, so a recorder held from then on stands for other work queued
    behind the import, as a backup's, which the flow's waits must wait for.
    """
    real_replay = coordinator_module.async_replay

    async def replay_then_hold(*args: Any, **kwargs: Any) -> tuple[int, int]:
        written = await real_replay(*args, **kwargs)
        recorder.hold_once()
        return written

    return patch.object(coordinator_module, "async_replay", replay_then_hold)


def _unconfirmed_replays() -> AbstractContextManager[Any]:
    """
    Make a replay give up on its confirmation at once, as a busy recorder would.

    Its rows then stay queued behind the held recorder, unsaved, and the
    replay fails the import with ``statistics_write``: what the flow's waits
    before a retry or a new setup must cover. Set the module's
    ``CONFIRM_TIMEOUT_SECONDS`` back to 30 before an attempt that must
    succeed.
    """
    return patch.object(statistics_module, "CONFIRM_TIMEOUT_SECONDS", 0.05)


async def _add_kept_statistics(hass: HomeAssistant) -> tuple[str, str, str, str]:
    """Add the four statistics as kept from an earlier installation, 100 days old."""
    key = property_key(_TEST_ACCOUNT, _TEST_PROPERTY)
    energy_id, cost_id = consumption_ids(key)
    generation_id, credit_id = generation_ids(key)
    earlier = (_NOW - timedelta(days=100)).replace(minute=0)
    for metadata in (
        _energy_metadata(energy_id, "Meridian grid import"),
        _cost_metadata(cost_id, "Meridian grid import cost"),
        _energy_metadata(generation_id, "Meridian grid export"),
        _cost_metadata(credit_id, "Meridian grid export credit"),
    ):
        async_add_external_statistics(
            hass, metadata, [{"start": earlier, "state": 1.0, "sum": 1.0}]
        )
    await async_wait_recording_done(hass)
    return energy_id, cost_id, generation_id, credit_id


@pytest.mark.parametrize("retried", [False, True], ids=["saved", "retried"])
@pytest.mark.asyncio
async def test_retry_waits_for_rows_the_recorder_queued_again(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock, retried: bool
) -> None:
    """
    A retry waits in two passes, behind rows the recorder queued again.

    Every statistic existed before setup, so the retry's wait is a barrier,
    not a removal. On MySQL or MariaDB the failed attempt's import into them
    can meet a lock error once the recorder catches up, and the recorder then
    queues it again, behind that barrier, which finished with the rows still
    unsaved. The retry read the history without them and, an hour later,
    started its window an hour after theirs, leaving that hour out of every
    later sum. A second barrier, queued once the first has run, is behind
    the import's second try.
    """
    meridian.account = _test_account(feed_in=True)
    statistic_ids = await _add_kept_statistics(hass)
    recorder = RecorderHold(hass)
    # A gateway timeout, and the resend after its cooldown.
    generation_failures = {"remaining": 2}
    energy_writes: list[int] = []

    def hold_then_fail_generation_once(
        direction: str, before: str | None
    ) -> BaseException | None:
        del before
        recorder.hold_once()
        if direction == "GENERATION" and generation_failures["remaining"]:
            generation_failures["remaining"] -= 1
            return _gateway_timeout()
        return None

    meridian.failure = hold_then_fail_generation_once
    with (
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 5),
        _unconfirmed_replays(),
        (
            _energy_import_retried_once(statistic_ids[0], energy_writes)
            if retried
            else nullcontext()
        ),
    ):
        try:
            result = await _submit_login(hass)
            flow_id = result["flow_id"]
            await _wait_for_progress_done(hass, flow_id)
            result = await hass.config_entries.flow.async_configure(flow_id)
            assert result["step_id"] == "import_failed"
            statistics_module.CONFIRM_TIMEOUT_SECONDS = 30
            clock.now += 120
            # The recorder catches up once the retry's first barrier is queued.
            recorder.release_after(0.3)
            with patch(
                "custom_components.meridian_energy.coordinator._utcnow",
                return_value=_NOW + timedelta(hours=1),
            ):
                result = await hass.config_entries.flow.async_configure(flow_id, {})
                assert result["type"] is FlowResultType.SHOW_PROGRESS
                result = await _finish_import(hass, flow_id)
        finally:
            recorder.release()
        await async_wait_recording_done(hass)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    if retried:
        # The failed attempt's import, its second try, and the retry's import.
        assert energy_writes == [0, 1, 2]
    for statistic_id in statistic_ids:
        rows = await _rows(hass, statistic_id)
        assert rows[0]["sum"] == pytest.approx(1.0)
        assert _running_sum_breaks(rows) == []


@pytest.mark.asyncio
async def test_retry_removes_rows_the_recorder_saved_after_removing_them(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    A retry checks for leftover rows only once a second barrier has run.

    The failed attempt's import met a lock error once the recorder caught up
    and was queued again, behind the retry's removal, which finished first.
    That import's second try then wrote the rows back. Checked straight
    after the removal, the statistic looked empty, so the retry started a
    new 90-day import from a sum of zero, an hour later, and the rows written
    back stayed: an hour of them before the retry's window, with a sum it
    did not continue. Checked after a second barrier, the rows are found,
    the form asks to wait, and the next press removes them. The failed
    attempt's replays were not confirmed, but nothing is left of what they
    wrote, so the retry walks its own 90 days rather than reach back to
    their oldest hour (``_note_unconfirmed``).
    """
    meridian.account = _test_account(feed_in=True)
    recorder = RecorderHold(hass)
    # A gateway timeout, and the resend after its cooldown.
    generation_failures = {"remaining": 2}
    energy_writes: list[int] = []
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))

    def hold_then_fail_generation_once(
        direction: str, before: str | None
    ) -> BaseException | None:
        del before
        recorder.hold_once()
        if direction == "GENERATION" and generation_failures["remaining"]:
            generation_failures["remaining"] -= 1
            return _gateway_timeout()
        return None

    meridian.failure = hold_then_fail_generation_once
    with (
        # The failed attempt's own removal stays queued behind the hold.
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
        _unconfirmed_replays(),
        _energy_import_retried_once(energy_id, energy_writes),
    ):
        try:
            result = await _submit_login(hass)
            flow_id = result["flow_id"]
            await _wait_for_progress_done(hass, flow_id)
            result = await hass.config_entries.flow.async_configure(flow_id)
            assert result["step_id"] == "import_failed"
            clock.now += 120
            statistics_module.CLEAR_STATISTICS_TIMEOUT_SECONDS = 5
            statistics_module.CONFIRM_TIMEOUT_SECONDS = 30
            # The recorder catches up once the retry's removal is queued.
            recorder.release_after(0.3)
            result = await hass.config_entries.flow.async_configure(flow_id, {})
            assert result["type"] is FlowResultType.FORM
            assert result["errors"] == {"base": "cleanup_pending"}
            assert energy_writes == [0, 1]
            with patch(
                "custom_components.meridian_energy.coordinator._utcnow",
                return_value=_NOW + timedelta(hours=1),
            ):
                result = await hass.config_entries.flow.async_configure(flow_id, {})
                assert result["type"] is FlowResultType.SHOW_PROGRESS
                result = await _finish_import(hass, flow_id)
        finally:
            recorder.release()
        await async_wait_recording_done(hass)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    rows = await _rows(hass, energy_id)
    # The rows start with the retry's window, an hour after the failed
    # attempt's: no row the recorder wrote back is left before it.
    retry_start = (_NOW + timedelta(hours=1) - timedelta(days=90)).replace(minute=0)
    assert rows[0]["start"] == (retry_start + timedelta(hours=1)).timestamp()
    assert rows[0]["sum"] == pytest.approx(rows[0]["state"])
    assert _running_sum_breaks(rows) == []


@pytest.mark.asyncio
async def test_setup_waits_for_rows_the_recorder_queued_again(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    The first attempt also waits in two passes before it reads the history.

    Rows queued before setup started, here added to a statistic kept from an
    earlier installation, met a lock error once the recorder caught up and
    were queued again, behind the barrier setup waited on. Setup then read
    the history without them and took the sum before its window from the
    older row, so the sum dropped back at the window's first hour once they
    were saved.
    """
    meridian.account = _test_account(feed_in=True)
    energy_id, *_ = await _add_kept_statistics(hass)
    energy_writes: list[int] = []
    recorder = RecorderHold(hass)
    with (
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 5),
        _energy_import_retried_once(energy_id, energy_writes),
    ):
        try:
            recorder.hold_once()
            later = (_NOW - timedelta(days=95)).replace(minute=0)
            async_add_external_statistics(
                hass,
                _energy_metadata(energy_id, "Meridian grid import"),
                [{"start": later, "state": 1.0, "sum": 2.0}],
            )
            # The recorder catches up once setup's first barrier is queued.
            recorder.release_after(0.3)
            result = await _submit_login(hass)
            assert result["type"] is FlowResultType.SHOW_PROGRESS
            result = await _finish_import(hass, result["flow_id"])
        finally:
            recorder.release()
        await async_wait_recording_done(hass)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert energy_writes == [0, 1, 2]
    rows = await _rows(hass, energy_id)
    assert [row["sum"] for row in rows[:2]] == pytest.approx([1.0, 2.0])
    assert _running_sum_breaks(rows) == []


@pytest.mark.asyncio
async def test_new_setup_waits_until_the_recorder_has_removed_an_earlier_attempt(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    A replaced setup's queued removal is not read as existing history.

    The new setup would otherwise count the earlier attempt's rows as
    history from before it, continue them with two weeks of data, and lose
    them to the earlier attempt's removal.
    """
    meridian.account = _test_account(feed_in=True)
    recorder = RecorderHold(hass)
    failing = {"generation": True}

    def hold_then_fail_generation(
        direction: str, before: str | None
    ) -> BaseException | None:
        del before
        recorder.hold_once()
        if failing["generation"] and direction == "GENERATION":
            return _gateway_timeout()
        return None

    meridian.failure = hold_then_fail_generation
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    with (
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
        _unconfirmed_replays(),
    ):
        try:
            first = await _submit_login(hass)
            await _wait_for_progress_done(hass, first["flow_id"])
            failing["generation"] = False
            statistics_module.CONFIRM_TIMEOUT_SECONDS = 30

            second = await _submit_login(hass)
            assert second["type"] is FlowResultType.FORM
            assert second["step_id"] == "discovery_retry"
            assert second["errors"] == {"base": "cleanup_pending"}
        finally:
            recorder.release()
        await get_instance(hass).async_block_till_done()
        result = await hass.config_entries.flow.async_configure(second["flow_id"], {})
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        result = await _finish_import(hass, second["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert _flow_ids(hass) == set()
    rows = await _rows(hass, energy_id)
    assert len(rows) == 90 * 24 - 1
    assert rows[0]["sum"] == pytest.approx(rows[0]["state"])


@pytest.mark.asyncio
async def test_new_setup_waits_for_a_removal_the_recorder_is_running(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    A removal the recorder thread has already started holds setup back too.

    The recorder's commit future does not count a task it has taken off its
    queue, so a wait on it returned at once, and the new setup read rows
    about to be deleted as existing history. It left them out of its own
    rollback and imported those statistics without a fresh baseline.
    """
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    # Rows an earlier setup of the account imported before it was replaced.
    async_add_external_statistics(
        hass,
        _energy_metadata(energy_id, "Meridian grid import"),
        [{"start": _HISTORY_START, "state": 1.0, "sum": 1.0}],
    )
    await async_wait_recording_done(hass)
    assert await async_has_statistics(hass, energy_id)
    removal = _SlowRemoval([energy_id])
    get_instance(hass).queue_task(removal)
    await _wait_until(removal.started.is_set)

    with patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2):
        try:
            result = await _submit_login(hass)
            assert result["type"] is FlowResultType.FORM
            assert result["step_id"] == "discovery_retry"
            assert result["errors"] == {"base": "cleanup_pending"}
            assert meridian.measurement_calls == []
        finally:
            removal.release.set()
        await get_instance(hass).async_block_till_done()
        assert not await async_has_statistics(hass, energy_id)
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    rows = await _rows(hass, energy_id)
    assert len(rows) == 90 * 24 - 1
    assert rows[0]["sum"] == pytest.approx(rows[0]["state"])


@pytest.mark.asyncio
async def test_waiting_for_the_recorder_asks_meridian_for_the_accounts_once(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    A retry of a wait on the local recorder sends Meridian no new requests.

    Each press repeated the account discovery first, so a rate limit or a
    connection error there could replace cleanup_pending.
    """
    meridian.async_get_accounts = AsyncMock(return_value=(_test_account(),))
    with patch.object(
        config_flow_module,
        "async_wait_for_recorder",
        new=AsyncMock(side_effect=[False, False, True]),
    ):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        assert result["step_id"] == "discovery_retry"
        assert result["errors"] == {"base": "cleanup_pending"}
        result = await hass.config_entries.flow.async_configure(flow_id, {})
        assert result["step_id"] == "discovery_retry"
        assert result["errors"] == {"base": "cleanup_pending"}
        meridian.async_get_accounts.assert_awaited_once()
        result = await hass.config_entries.flow.async_configure(flow_id, {})
        assert result["type"] is FlowResultType.SHOW_PROGRESS

    result = await _finish_import(hass, flow_id)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_SELECTED_ACCOUNTS] == [_TEST_ACCOUNT]
    # The flow's discovery alone: the import uses the accounts it found.
    meridian.async_get_accounts.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_first_barrier_that_times_out_shows_cleanup_pending_at_once(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    The setup dialog does not wait a second 30 s for an answer already known.

    Each wait before an attempt has two passes and one 30 s wait for each.
    Once the first has timed out, the second cannot complete the wait, so
    queueing it only kept the spinner up for 30 s more before
    cleanup_pending.
    """
    del meridian
    real_removal = statistics_module._async_run_removal
    barriers: list[list[str]] = []

    async def counted_removal(
        hass_: HomeAssistant, statistic_ids: list[str], **kwargs: Any
    ) -> bool:
        if "seconds" not in kwargs:
            # Not a replay confirming its own writes.
            barriers.append(statistic_ids)
        return await real_removal(hass_, statistic_ids, **kwargs)

    recorder = RecorderHold(hass)
    with (
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
        patch.object(statistics_module, "_async_run_removal", counted_removal),
    ):
        recorder.hold_once()
        try:
            result = await _submit_login(hass)
        finally:
            recorder.release()
        flow_id = result["flow_id"]
        assert result["step_id"] == "discovery_retry"
        assert result["errors"] == {"base": "cleanup_pending"}
        assert barriers == [[]]

        await get_instance(hass).async_block_till_done()
        result = await hass.config_entries.flow.async_configure(flow_id, {})
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        # Both passes now: the recorder has caught up.
        assert barriers == [[], [], []]
        result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.CREATE_ENTRY


@pytest.mark.asyncio
async def test_account_selection_waits_while_the_recorder_is_behind(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    meridian.async_get_accounts = AsyncMock(
        return_value=(_test_account(), _account("A-TEST-2"))
    )
    result = await _start_user_flow(hass)
    flow_id = result["flow_id"]
    result = await hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    assert result["step_id"] == "accounts"
    assert _schema_default(result, CONF_SELECTED_ACCOUNTS) == [
        _TEST_ACCOUNT,
        "A-TEST-2",
    ]

    with patch.object(
        config_flow_module,
        "async_wait_for_recorder",
        new=AsyncMock(return_value=False),
    ):
        result = await hass.config_entries.flow.async_configure(
            flow_id, {CONF_SELECTED_ACCOUNTS: [_TEST_ACCOUNT]}
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "accounts"
    assert result["errors"] == {"base": "cleanup_pending"}
    assert meridian.measurement_calls == []
    # The form keeps the selection: the frontend fills a form from its
    # defaults, so submitting it again must not import every account.
    selection = _schema_default(result, CONF_SELECTED_ACCOUNTS)
    assert selection == [_TEST_ACCOUNT]
    result = await hass.config_entries.flow.async_configure(
        flow_id, {CONF_SELECTED_ACCOUNTS: selection}
    )
    assert result["type"] is FlowResultType.SHOW_PROGRESS
    result = await _finish_import(hass, flow_id)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_SELECTED_ACCOUNTS] == [_TEST_ACCOUNT]
    assert result["data"][CONF_AUTO_ADD_ACCOUNTS] is False


def _schema_default(result: ConfigFlowResult, name: str) -> object:
    """Return the default a form's field starts with, as the frontend fills it."""
    schema = result["data_schema"]
    assert schema is not None
    for key in schema.schema:
        if str(key) == name:
            return key.default()
    pytest.fail(f"the form has no {name} field")


def _count_coordinators() -> tuple[list[MeridianDataCoordinator], Any]:
    """Patch the flow's coordinator class to record each one it builds."""
    built: list[MeridianDataCoordinator] = []
    real_coordinator = MeridianDataCoordinator

    def new_coordinator(*args: Any, **kwargs: Any) -> MeridianDataCoordinator:
        coordinator = real_coordinator(*args, **kwargs)
        built.append(coordinator)
        return coordinator

    return built, patch.object(
        config_flow_module, "MeridianDataCoordinator", side_effect=new_coordinator
    )


@pytest.mark.asyncio
async def test_two_retry_submissions_start_one_attempt(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    A second submission while the first still checks the rollback starts nothing.

    The frontend sends a submission for each press of Enter, even while one
    is still waiting. Two imports would write the same statistics, each
    failure's rollback would delete the other's rows, and closing the dialog
    would cancel only one of them. The second waits for the first and shows
    the attempt it started.
    """
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    coordinators, counting = _count_coordinators()
    with counting:
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        result = await _finish_import(hass, flow_id)
        assert result["step_id"] == "import_failed"
        meridian.failure = lambda _direction, _before: None
        clock.now += 121
        meridian.blocked_direction = "CONSUMPTION"

        first, second = await asyncio.gather(
            hass.config_entries.flow.async_configure(flow_id, {}),
            hass.config_entries.flow.async_configure(flow_id, {}),
        )

    assert first["type"] is FlowResultType.SHOW_PROGRESS
    assert second["type"] is FlowResultType.SHOW_PROGRESS
    assert second["step_id"] == "initial_import"
    assert len(coordinators) == 2
    flow = hass.config_entries.flow._progress[flow_id]
    assert isinstance(flow, MeridianEnergyConfigFlow)
    assert flow._import_attempts == 2
    assert flow.cur_step is not None
    assert flow.cur_step["type"] is FlowResultType.SHOW_PROGRESS
    task = flow.async_get_progress_task()
    assert task is not None
    hass.config_entries.flow.async_abort(flow_id)
    # hass.async_block_till_done skips tasks that are being cancelled.
    await asyncio.wait({task}, timeout=10)
    assert task.cancelled()
    meridian.release.set()
    await hass.async_block_till_done()
    assert await _rows(hass, energy_id) == []
    assert hass.config_entries.async_entries(DOMAIN) == []


@pytest.mark.asyncio
async def test_two_account_selections_start_one_attempt(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    meridian.async_get_accounts = AsyncMock(
        return_value=(_test_account(), _account("A-TEST-2"))
    )

    async def recorder_round_trip(hass_: HomeAssistant, **passes: int) -> bool:
        # The test recorder answers reads at once; a real one takes a moment,
        # and the second submission arrives during it.
        del hass_, passes
        await asyncio.sleep(0)
        return True

    coordinators, counting = _count_coordinators()
    with (
        counting,
        patch.object(
            config_flow_module, "async_wait_for_recorder", recorder_round_trip
        ),
    ):
        result = await _start_user_flow(hass)
        flow_id = result["flow_id"]
        result = await hass.config_entries.flow.async_configure(
            flow_id, {"otp": "123456"}
        )
        assert result["step_id"] == "accounts"

        first, second = await asyncio.gather(
            hass.config_entries.flow.async_configure(
                flow_id, {CONF_SELECTED_ACCOUNTS: [_TEST_ACCOUNT]}
            ),
            hass.config_entries.flow.async_configure(
                flow_id, {CONF_SELECTED_ACCOUNTS: [_TEST_ACCOUNT, "A-TEST-2"]}
            ),
        )
        assert first["type"] is FlowResultType.SHOW_PROGRESS
        assert second["type"] is FlowResultType.SHOW_PROGRESS
        assert second["step_id"] == "initial_import"
        result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_SELECTED_ACCOUNTS] == [_TEST_ACCOUNT]
    assert len(coordinators) == 1


def _discovery_answer(
    answer: str, account: MeridianAccount
) -> tuple[MeridianAccount, ...]:
    """Return, or raise, what an account discovery answers."""
    if answer == "cannot_connect":
        raise MeridianConnectionError
    if answer == "login_rejected":
        raise MeridianAuthenticationError
    if answer == "no_accounts":
        return ()
    return (account, _account("A-TEST-2"))


def _assert_import_still_running(hass: HomeAssistant, flow_id: str) -> None:
    flow = hass.config_entries.flow._progress[flow_id]
    assert flow.cur_step is not None
    assert flow.cur_step["type"] is FlowResultType.SHOW_PROGRESS
    task = flow.async_get_progress_task()
    assert task is not None
    assert not task.done()


@pytest.mark.parametrize(
    "second_answer",
    ["cannot_connect", "login_rejected", "no_accounts", "several_accounts"],
)
@pytest.mark.asyncio
async def test_a_second_discovery_submission_leaves_the_import_alone(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    caplog: pytest.LogCaptureFixture,
    second_answer: str,
) -> None:
    """
    A discovery retry sent again while the first still waits changes nothing.

    The frontend sends a submission for each press of Enter, even while one
    is still waiting. Handled side by side, the second answer arrived after
    the first had started the import, and any result other than its
    progress (a form, an abort or the account selection) made Home Assistant
    cancel the import. The flow then stayed at that form and never started
    another import. The second now makes no request and shows the import's
    progress, as the first does.
    """
    first_waiting = asyncio.Event()
    finish_first = asyncio.Event()
    import_running = asyncio.Event()
    answers: list[Callable[[], Awaitable[tuple[MeridianAccount, ...]]]] = []

    async def get_accounts() -> tuple[MeridianAccount, ...]:
        # Taken when the request is made; the import's own request finds none.
        answer = answers.pop(0) if answers else None
        await asyncio.sleep(0)
        return (meridian.account,) if answer is None else await answer()

    async def unreachable() -> tuple[MeridianAccount, ...]:
        raise MeridianConnectionError

    async def slow_success() -> tuple[MeridianAccount, ...]:
        first_waiting.set()
        await finish_first.wait()
        return (meridian.account,)

    async def late_answer() -> tuple[MeridianAccount, ...]:
        # A request for the second submission answers once the import runs.
        await import_running.wait()
        return _discovery_answer(second_answer, meridian.account)

    meridian.async_get_accounts = get_accounts
    meridian.blocked_direction = "CONSUMPTION"
    coordinators, counting = _count_coordinators()
    with counting:
        answers.append(unreachable)
        result = await _start_user_flow(hass)
        flow_id = result["flow_id"]
        result = await hass.config_entries.flow.async_configure(
            flow_id, {"otp": "123456"}
        )
        assert result["step_id"] == "discovery_retry"

        answers.append(slow_success)
        first = hass.async_create_task(
            hass.config_entries.flow.async_configure(flow_id, {})
        )
        await first_waiting.wait()
        answers.append(late_answer)
        second = hass.async_create_task(
            hass.config_entries.flow.async_configure(flow_id, {})
        )
        second_made_a_request = late_answer not in answers
        answers.clear()
        finish_first.set()
        first_result = await first
        assert first_result["type"] is FlowResultType.SHOW_PROGRESS
        await _wait_until(lambda: bool(meridian.measurement_calls))
        import_running.set()
        second_result = await second

        # The second makes no request and shows the step the first produced.
        assert second_result["type"] is FlowResultType.SHOW_PROGRESS
        assert second_result["step_id"] == "initial_import"
        assert not second_made_a_request
        _assert_import_still_running(hass, flow_id)
        meridian.release.set()
        result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_SELECTED_ACCOUNTS] == [_TEST_ACCOUNT]
    assert result["data"][CONF_AUTO_ADD_ACCOUNTS] is True
    assert len(coordinators) == 1
    # Nothing cancelled the import, so nothing logged a cancellation.
    assert _flow_warnings(caplog) == []


@pytest.mark.parametrize("second_accepted", [False, True], ids=["rejected", "accepted"])
@pytest.mark.asyncio
async def test_a_second_code_submission_leaves_the_import_alone(
    hass: HomeAssistant, meridian: _FakeMeridian, second_accepted: bool
) -> None:
    """
    A code sent again while the first is still being checked changes nothing.

    Checked side by side, the second answer arrived after the import had
    started. A rejected code (a code works once) returned the code form,
    which made Home Assistant cancel the import. An accepted one replaced
    the login data the import had filled in, so the entry was created
    without its selected accounts and could not be set up.
    """
    first_waiting = asyncio.Event()
    finish_first = asyncio.Event()
    import_running = asyncio.Event()
    calls = {"count": 0}

    async def validate(*_args: object) -> MeridianTokenSet:
        calls["count"] += 1
        await asyncio.sleep(0)
        if calls["count"] == 1:
            first_waiting.set()
            await finish_first.wait()
            return _tokens()
        await import_running.wait()
        if not second_accepted:
            raise MeridianOtpError("OTP_NOT_FOUND", "synthetic")
        return _tokens()

    meridian.async_validate_otp = AsyncMock(side_effect=validate)
    meridian.blocked_direction = "CONSUMPTION"
    result = await _start_user_flow(hass)
    flow_id = result["flow_id"]

    first = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    )
    await first_waiting.wait()
    second = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    )
    finish_first.set()
    first_result = await first
    assert first_result["type"] is FlowResultType.SHOW_PROGRESS
    await _wait_until(lambda: bool(meridian.measurement_calls))
    import_running.set()
    second_result = await second

    assert second_result["type"] is FlowResultType.SHOW_PROGRESS
    assert second_result["step_id"] == "initial_import"
    assert calls["count"] == 1
    _assert_import_still_running(hass, flow_id)
    meridian.release.set()
    result = await _finish_import(hass, flow_id)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_SELECTED_ACCOUNTS] == [_TEST_ACCOUNT]
    assert result["data"][CONF_AUTO_ADD_ACCOUNTS] is True


@pytest.mark.asyncio
async def test_a_second_email_submission_sends_no_second_code(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    An email sent again while the first code is being sent sends no second one.

    A second code would start a new login journey, which the code in the
    first email might not match. Both answers are the code form: the
    frontend shows whichever reaches the dialog first, and the email form
    shown again could not be submitted to a flow already at the code step.
    """
    sending = asyncio.Event()
    finish_sending = asyncio.Event()

    async def send(*_args: object) -> None:
        await asyncio.sleep(0)
        if not sending.is_set():  # Only the first send is slow.
            sending.set()
            await finish_sending.wait()

    meridian.async_send_otp = AsyncMock(side_effect=send)
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    flow_id = result["flow_id"]
    first = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {CONF_EMAIL: _TEST_EMAIL})
    )
    await sending.wait()
    second = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {CONF_EMAIL: _TEST_EMAIL})
    )
    await asyncio.sleep(0.05)
    # The second waits for the first, rather than answer with the email form.
    assert not second.done()
    finish_sending.set()
    for answer in (await first, await second):
        assert answer["type"] is FlowResultType.FORM
        assert answer["step_id"] == "otp"
    meridian.async_send_otp.assert_awaited_once()

    result = await hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    assert result["type"] is FlowResultType.SHOW_PROGRESS
    sent_journey = meridian.async_send_otp.await_args.args[1]
    assert meridian.async_validate_otp.await_args.args[2] == sent_journey
    result = await _finish_import(hass, flow_id)
    assert result["type"] is FlowResultType.CREATE_ENTRY


@pytest.mark.asyncio
async def test_a_cancelled_second_submission_leaves_the_first_alone(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """A waiting submission whose request is cancelled does not end the first."""
    sending = asyncio.Event()
    finish_sending = asyncio.Event()

    async def send(*_args: object) -> None:
        sending.set()
        await finish_sending.wait()

    meridian.async_send_otp = AsyncMock(side_effect=send)
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    flow_id = result["flow_id"]
    first = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {CONF_EMAIL: _TEST_EMAIL})
    )
    await sending.wait()
    second = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {CONF_EMAIL: _TEST_EMAIL})
    )
    await asyncio.sleep(0)
    second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second
    finish_sending.set()

    result = await first
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "otp"
    meridian.async_send_otp.assert_awaited_once()


@pytest.mark.parametrize("outcome", ["several_accounts", "no_accounts", "rejected"])
@pytest.mark.asyncio
async def test_a_second_code_submission_shows_the_step_the_first_produced(
    hass: HomeAssistant, meridian: _FakeMeridian, outcome: str
) -> None:
    """
    Both answers to a code pressed twice carry the step the first produced.

    The frontend replaces the dialog's form with the first answer it gets
    and drops a later one. The second used to come back at once with the
    code form, so the dialog stayed there while the flow moved on: its
    Submit then failed validation, or the account selection never showed.
    """
    checking = asyncio.Event()
    finish_check = asyncio.Event()

    async def validate(*_args: object) -> MeridianTokenSet:
        checking.set()
        await finish_check.wait()
        if outcome == "rejected":
            raise MeridianOtpError("OTP_INVALID", "synthetic")
        return _tokens()

    meridian.async_validate_otp = AsyncMock(side_effect=validate)
    meridian.async_get_accounts = AsyncMock(
        return_value=()
        if outcome == "no_accounts"
        else (_test_account(), _account("A-TEST-2"))
    )
    result = await _start_user_flow(hass)
    flow_id = result["flow_id"]
    first = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    )
    await checking.wait()
    second = hass.async_create_task(
        hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
    )
    await asyncio.sleep(0.05)
    assert not second.done()
    finish_check.set()
    answers = [await first, await second]

    meridian.async_validate_otp.assert_awaited_once()
    expected = {
        "several_accounts": (FlowResultType.FORM, "accounts", {}),
        "no_accounts": (FlowResultType.ABORT, None, None),
        "rejected": (FlowResultType.FORM, "otp", {"base": "otp_invalid"}),
    }[outcome]
    for answer in answers:
        assert (answer["type"], answer.get("step_id"), answer.get("errors")) == expected
    if outcome == "no_accounts":
        assert [answer["reason"] for answer in answers] == ["no_accounts"] * 2
        assert _flow_ids(hass) == set()
        return
    if outcome == "rejected":
        return
    # The form both answers show can be submitted.
    result = await hass.config_entries.flow.async_configure(
        flow_id, {CONF_SELECTED_ACCOUNTS: [_TEST_ACCOUNT]}
    )
    assert result["type"] is FlowResultType.SHOW_PROGRESS
    result = await _finish_import(hass, flow_id)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_SELECTED_ACCOUNTS] == [_TEST_ACCOUNT]


@pytest.mark.asyncio
async def test_closing_the_dialog_warns_after_removing_only_new_statistics(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Closing the dialog cancels the import; the warning follows the rollback.

    The import is held at the grid export's second week, once the grid
    import has walked all its weeks and imported them.
    """
    meridian.account = _test_account(feed_in=True)
    meridian.blocked_direction = "GENERATION"
    meridian.blocked_after = 1
    key = property_key(_TEST_ACCOUNT, _TEST_PROPERTY)
    energy_id, cost_id = consumption_ids(key)
    generation_id, _ = generation_ids(key)
    # Generation history from an earlier installation is not the flow's to remove.
    async_add_external_statistics(
        hass,
        _energy_metadata(generation_id, "Meridian grid export"),
        [{"start": _HISTORY_START, "state": 1.0, "sum": 1.0}],
    )
    await async_wait_recording_done(hass)
    events: list[str] = []
    real_clear = config_flow_module.async_clear_statistics

    async def spy_clear(hass_: HomeAssistant, statistic_ids: set[str]) -> bool:
        events.append(f"clear {sorted(statistic_ids)}")
        removed = await real_clear(hass_, statistic_ids)
        events.append("cleared")
        return removed

    class _Recorder(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if record.levelno == logging.WARNING:
                events.append(f"warning {record.getMessage()}")

    handler = _Recorder()
    flow_logger = logging.getLogger(config_flow_module.__name__)
    flow_logger.addHandler(handler)
    try:
        with patch.object(config_flow_module, "async_clear_statistics", spy_clear):
            result = await _submit_login(hass)
            flow_id = result["flow_id"]
            await _wait_until(lambda: meridian.calls("GENERATION") == 2)
            assert await _rows(hass, energy_id)
            task = hass.config_entries.flow._progress[flow_id].async_get_progress_task()
            assert task is not None
            hass.config_entries.flow.async_abort(flow_id)
            # hass.async_block_till_done skips tasks that are being cancelled.
            await asyncio.wait({task}, timeout=10)
            assert task.cancelled()
            await hass.async_block_till_done()
    finally:
        flow_logger.removeHandler(handler)

    flow_created = sorted({energy_id, cost_id, *generation_ids(key)} - {generation_id})
    assert events[:2] == [f"clear {flow_created}", "cleared"]
    assert len(events) == 3
    assert events[2].startswith(
        "warning Meridian setup was cancelled before the history import finished"
    )
    assert "because the setup dialog was closed" in events[2]
    assert "the statistics it created were removed" in events[2]
    assert "operation=measurements direction=GENERATION" in events[2]
    assert "page=1" in events[2]
    assert await _rows(hass, energy_id) == []
    assert await _rows(hass, cost_id) == []
    assert len(await _rows(hass, generation_id)) == 1
    assert not _import_failure_records(caplog)
    assert hass.config_entries.async_entries(DOMAIN) == []


@pytest.mark.asyncio
async def test_closing_the_dialog_warns_when_the_removal_is_still_queued(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    If the recorder has not removed the rows within its wait, say so, once.

    The removal itself does not log its timeout as well: the one warning for
    the closed dialog says the removal is queued.
    """
    meridian.blocked_direction = "CONSUMPTION"
    recorder = RecorderHold(hass)
    with patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2):
        try:
            result = await _submit_login(hass)
            flow_id = result["flow_id"]
            await _wait_until(lambda: bool(meridian.measurement_calls))
            task = hass.config_entries.flow._progress[flow_id].async_get_progress_task()
            assert task is not None
            recorder.hold_once()
            hass.config_entries.flow.async_abort(flow_id)
            # hass.async_block_till_done skips tasks that are being cancelled.
            await asyncio.wait({task}, timeout=10)
        finally:
            recorder.release()
        await get_instance(hass).async_block_till_done()

    assert task.cancelled()
    (record,) = _integration_warnings(caplog)
    assert record.getMessage().startswith("Meridian setup was cancelled")
    assert "removal of the statistics it created is queued" in record.getMessage()
    assert "were removed" not in record.getMessage()


@pytest.mark.asyncio
async def test_closing_the_dialog_during_a_failed_attempts_rollback_warns_once(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    The log is not silent when the dialog closes while a failure rolls back.

    The flow is gone by then, so it never logs the failure itself: the one
    warning names the failure, with its traceback, and the queued removal.
    Its times end at the failure, leaving out the rollback.
    """

    def slow_failure(_direction: str, _before: str | None) -> BaseException:
        clock.now += 3.0
        return _gateway_timeout()

    meridian.failure = slow_failure
    clearing = asyncio.Event()

    async def slow_clear(hass_: HomeAssistant, statistic_ids: set[str]) -> bool:
        del hass_, statistic_ids
        clock.now += 25.0
        clearing.set()
        await asyncio.Event().wait()  # the recorder is busy
        return True

    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(config_flow_module, "async_clear_statistics", slow_clear),
    ):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        await asyncio.wait_for(clearing.wait(), 10)
        task = hass.config_entries.flow._progress[flow_id].async_get_progress_task()
        assert task is not None
        hass.config_entries.flow.async_abort(flow_id)
        # hass.async_block_till_done skips tasks that are being cancelled.
        await asyncio.wait({task}, timeout=10)

    assert task.cancelled()
    (record,) = _flow_warnings(caplog)
    message = record.getMessage()
    assert message.startswith(
        "Meridian setup was cancelled while removing the statistics from a "
        "failed attempt, because the setup dialog was closed; removal of the "
        "statistics it created is queued"
    )
    assert "failure=UpdateFailed" in message
    assert "operation=measurements direction=CONSUMPTION" in message
    # This fake sends no HTTP requests, so there is no median to give. The
    # gateway timeout was sent once more after its cooldown, and failed again.
    assert "request_elapsed=3.0s total_elapsed=6.0s median_request=-)" in message
    assert "attempt 1 of 5" in message
    assert record.exc_info is not None
    assert record.exc_info[0] is UpdateFailed
    assert not re.search(r"\d{4}-\d{2}-\d{2}", message)


@pytest.mark.parametrize(
    ("ended_by", "removal_finished"),
    [("closing", True), ("closing", False), ("new-setup", True)],
    ids=["closed-removed", "closed-queued", "replaced-removed"],
)
@pytest.mark.asyncio
async def test_closing_the_dialog_while_a_retry_repeats_the_removal_warns_once(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
    *,
    ended_by: str,
    removal_finished: bool,
) -> None:
    """
    Closing the dialog while a retry removes the failed attempt's rows is logged.

    No attempt runs then, so no import task logs the cancellation: the flow
    logs it once the repeated removal has finished or been queued. The failed
    attempt's own warning already has its failure and traceback. A new setup
    for the same email removes the flow at that form in the same way.
    """
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)
    assert result["step_id"] == "import_failed"
    (failure_record,) = _flow_warnings(caplog)
    calls = len(meridian.measurement_calls)
    clearing = asyncio.Event()
    release = asyncio.Event()

    async def held_clear(hass_: HomeAssistant, statistic_ids: set[str]) -> bool:
        del hass_, statistic_ids
        clearing.set()
        await release.wait()  # the recorder is busy
        return removal_finished

    clock.now += 120
    with patch.object(config_flow_module, "async_clear_statistics", held_clear):
        submission = asyncio.ensure_future(
            hass.config_entries.flow.async_configure(flow_id, {})
        )
        await asyncio.wait_for(clearing.wait(), 10)
        if ended_by == "closing":
            hass.config_entries.flow.async_abort(flow_id)
        else:
            assert (await _start_user_flow(hass))["step_id"] == "otp"
        assert flow_id not in _flow_ids(hass)
        release.set()
        result = await submission

    assert result["type"] is FlowResultType.ABORT
    assert len(meridian.measurement_calls) == calls
    assert hass.config_entries.async_entries(DOMAIN) == []
    first, record = _flow_warnings(caplog)
    assert first is failure_record
    removal = (
        "the statistics it created were removed"
        if removal_finished
        else "removal of the statistics it created is queued"
    )
    assert record.getMessage() == (
        "Meridian setup was cancelled while removing the statistics from a "
        "failed attempt before a retry, because the setup dialog was closed or "
        f"a new setup replaced it; {removal} (attempt 1 of 5)"
    )
    assert record.exc_info is None


@pytest.mark.parametrize(
    ("cause", "reason"),
    [
        ("cancelled", "while the setup dialog was still open"),
        ("stopping", "because Home Assistant is stopping"),
    ],
)
@pytest.mark.asyncio
async def test_cancelling_a_failed_attempts_rollback_with_the_flow_open_warns_once(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    caplog: pytest.LogCaptureFixture,
    cause: str,
    reason: str,
) -> None:
    """
    The cancellation warning names the failure even when the flow is still open.

    Home Assistant stopping removes no flow, and nor does a task cancelled
    from outside the flow. The flow then sees only the cancellation, so it
    never logs the failure: the one warning carries it, with its traceback,
    and the form shows the cancellation.
    """
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    clearing = asyncio.Event()

    async def slow_clear(hass_: HomeAssistant, statistic_ids: set[str]) -> bool:
        del hass_, statistic_ids
        clearing.set()
        await asyncio.Event().wait()  # the recorder is busy
        return True

    with patch.object(config_flow_module, "async_clear_statistics", slow_clear):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        await asyncio.wait_for(clearing.wait(), 10)
        task = hass.config_entries.flow._progress[flow_id].async_get_progress_task()
        assert task is not None
        if cause == "stopping":
            await hass.async_stop()
        else:
            task.cancel()
        # hass.async_block_till_done skips tasks that are being cancelled.
        await asyncio.wait({task}, timeout=10)

    assert task.cancelled()
    assert flow_id in hass.config_entries.flow._progress
    (record,) = _flow_warnings(caplog)
    message = record.getMessage()
    assert message.startswith(
        "Meridian setup was cancelled while removing the statistics from a "
        f"failed attempt, {reason}; removal of the statistics it created is queued"
    )
    assert "failure=UpdateFailed" in message
    assert record.exc_info is not None
    assert record.exc_info[0] is UpdateFailed

    result = await hass.config_entries.flow.async_configure(flow_id)
    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "unknown"}
    assert result["description_placeholders"]["detail"].startswith(
        "the import was cancelled "
    )
    assert len(_flow_warnings(caplog)) == 1


@pytest.mark.asyncio
async def test_stopping_home_assistant_cancels_the_import_in_time_to_remove_it(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    The flow cancels its import at the stop event, while the recorder still runs.

    Home Assistant would cancel the task only after the recorder had made its
    final write and stopped, so the rollback's removal never ran, and it
    logged a warning and an error about the task.
    """
    meridian.account = _test_account(feed_in=True)
    meridian.blocked_direction = "GENERATION"
    meridian.blocked_after = 1
    key = property_key(_TEST_ACCOUNT, _TEST_PROPERTY)
    removals: list[list[str]] = []
    real_run = ClearStatisticsTask.run

    def spy_run(task: ClearStatisticsTask, instance: Recorder) -> None:
        removals.append(sorted(task.statistic_ids))
        real_run(task, instance)

    monkeypatch.setattr(ClearStatisticsTask, "run", spy_run)
    await _submit_login(hass)
    await _wait_until(lambda: meridian.calls("GENERATION") == 2)
    energy_id, _ = consumption_ids(key)
    assert await _rows(hass, energy_id)
    removals.clear()

    await hass.async_stop()

    assert removals == [sorted({*consumption_ids(key), *generation_ids(key)})]
    (record,) = _flow_warnings(caplog)
    assert record.getMessage().startswith(
        "Meridian setup was cancelled before the history import finished, "
        "because Home Assistant is stopping; the statistics it created were removed"
    )
    assert "operation=measurements direction=GENERATION" in record.getMessage()
    assert [
        record
        for record in caplog.records
        if record.name == "homeassistant.core" and record.levelno >= logging.WARNING
    ] == []


@pytest.mark.asyncio
async def test_an_attempt_that_ends_stops_listening_for_home_assistant_stopping(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    listeners = hass.bus.async_listeners().get(EVENT_HOMEASSISTANT_STOP, 0)
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    assert hass.bus.async_listeners()[EVENT_HOMEASSISTANT_STOP] == listeners + 1
    result = await _finish_import(hass, flow_id)
    assert result["step_id"] == "import_failed"
    assert hass.bus.async_listeners().get(EVENT_HOMEASSISTANT_STOP, 0) == listeners

    meridian.failure = lambda _direction, _before: None
    clock.now += 120
    await hass.config_entries.flow.async_configure(flow_id, {})
    result = await _finish_import(hass, flow_id)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert hass.bus.async_listeners().get(EVENT_HOMEASSISTANT_STOP, 0) == listeners


@pytest.mark.parametrize("ready", [True, False], ids=["settled", "still-queued"])
@pytest.mark.parametrize("retry", [False, True], ids=["first-attempt", "retry"])
@pytest.mark.asyncio
async def test_no_attempt_starts_once_home_assistant_has_started_to_stop(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
    *,
    retry: bool,
    ready: bool,
) -> None:
    """
    A submission whose wait on the recorder ends after the stop event ends too.

    Its attempt would start after the event, so nothing would cancel it: it
    would hold up the shutdown, and its rollback would not run once the
    recorder had stopped. The flow does not outlive the restart anyway, so
    it ends even while the recorder is still behind, rather than ask the user
    to wait. A retry has repeated the failed attempt's removal by then, and
    no attempt runs to log the cancellation, so the flow logs it.
    """
    waiting = asyncio.Event()
    release = asyncio.Event()
    # The first attempt waits on the recorder; a retry repeats the rollback.
    held = {"wait": not retry, "clear": False}
    real_wait = config_flow_module.async_wait_for_recorder
    real_clear = config_flow_module.async_clear_statistics

    async def held_wait(hass_: HomeAssistant, **passes: int) -> bool:
        settled = await real_wait(hass_, **passes)
        if held["wait"]:
            waiting.set()
            await release.wait()
            return settled and ready
        return settled

    async def held_clear(hass_: HomeAssistant, statistic_ids: set[str]) -> bool:
        removed = await real_clear(hass_, statistic_ids)
        if held["clear"]:
            waiting.set()
            await release.wait()
            return removed and ready
        return removed

    with (
        patch.object(config_flow_module, "async_wait_for_recorder", held_wait),
        patch.object(config_flow_module, "async_clear_statistics", held_clear),
    ):
        if retry:
            meridian.failure = lambda _direction, _before: _gateway_timeout()
            result = await _submit_login(hass)
            flow_id = result["flow_id"]
            result = await _finish_import(hass, flow_id)
            assert result["step_id"] == "import_failed"
            meridian.failure = lambda _direction, _before: None
            clock.now += 120
            held["clear"] = True
            submission = asyncio.ensure_future(
                hass.config_entries.flow.async_configure(flow_id, {})
            )
        else:
            submission = asyncio.ensure_future(_submit_login(hass))
        calls = len(meridian.measurement_calls)
        await asyncio.wait_for(waiting.wait(), 10)

        @callback
        def release_at_stop(_event: Event) -> None:
            release.set()
            # Home Assistant stays at this stage until the submission has
            # returned, as it would while the recorder was still busy.
            hass.async_create_task(asyncio.wait({submission}))

        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, release_at_stop)
        await hass.async_stop()

    result = submission.result()
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "login_expired"
    assert len(meridian.measurement_calls) == calls
    assert hass.config_entries.async_entries(DOMAIN) == []
    assert [
        record
        for record in caplog.records
        if record.name == "homeassistant.core" and record.levelno >= logging.WARNING
    ] == []
    warnings = [record.getMessage() for record in _flow_warnings(caplog)]
    if not retry:
        # Nothing was imported, so there is nothing to report.
        assert warnings == []
        return
    removal = (
        "the statistics it created were removed"
        if ready
        else "removal of the statistics it created is queued"
    )
    assert len(warnings) == 2
    assert warnings[0].startswith("Initial Meridian history import failed")
    assert warnings[1] == (
        "Meridian setup was cancelled while removing the statistics from a "
        "failed attempt before a retry, because Home Assistant is stopping; "
        f"{removal} (attempt 1 of 5)"
    )


@pytest.mark.asyncio
async def test_failure_warning_has_the_traceback_and_the_page_in_flight(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    # Two channels: the newest week needs a second page, which times out.
    # Its newest day alone, after the cooldown, needs one page; the day
    # before it needs two, and its second times out too: a bad day.
    meridian.kraken.channels = 2
    meridian.failure = lambda _direction, before: (
        _gateway_timeout() if before is not None else None
    )
    result = await _submit_login(hass)
    await _finish_import(hass, result["flow_id"])

    (record,) = _import_failure_records(caplog)
    assert record.levelno == logging.WARNING
    assert record.exc_info is not None
    error_type, error, _ = record.exc_info
    assert error_type is UpdateFailed
    assert isinstance(error, UpdateFailed)
    assert isinstance(error.__cause__, MeridianUpstreamError)
    message = record.getMessage()
    for field in (
        "attempt 1 of 5",
        "reason=upstream_timeout",
        "cause=MeridianUpstreamError",
        "root=MeridianHttpError",
        "status=504",
        "operation=measurements",
        "direction=CONSUMPTION",
        "account=1/1",
        "property=1/1",
        "window=1/13",
        "page=2",
        "bound=before",
        "calls=5",
        "in_flight=yes",
    ):
        assert field in message
    assert message.endswith(" rollback=removed")
    formatted = logging.Formatter().format(record)
    assert "Traceback" in formatted
    assert "MeridianHttpError" in formatted
    for private in (_TEST_ACCOUNT, _TEST_PROPERTY, _TEST_EMAIL, "synthetic-meter"):
        assert private not in formatted
    assert not re.search(r"\d{4}-\d{2}-\d{2}", message)


def _slow_removal(clock: _Clock, seconds: float) -> Callable[..., Awaitable[bool]]:
    """Return a rollback clear that takes ``seconds``, as a busy recorder would."""
    real_clear = config_flow_module.async_clear_statistics

    async def slow_clear(hass_: HomeAssistant, statistic_ids: set[str]) -> bool:
        clock.now += seconds
        return await real_clear(hass_, statistic_ids)

    return slow_clear


@pytest.mark.asyncio
async def test_failure_warning_times_the_request_and_leaves_out_the_rollback(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    The elapsed times end when the attempt failed, not once its rollback ended.

    A gateway error that came back after 3 s, and again after 3 s on the
    second page of the day before today, asked for alone after the cooldown,
    followed by a removal the busy recorder took 25 s over, must not read
    like the client's 30 s timeout.
    """

    def slow_pages(_direction: str, before: str | None) -> BaseException | None:
        # Each first page takes 2 s; each second page fails after 3 s.
        clock.now += 2.0 if before is None else 3.0
        return _gateway_timeout() if before is not None else None

    # Two channels, so the newest week needs a second page.
    meridian.kraken.channels = 2
    meridian.failure = slow_pages
    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(
            config_flow_module, "async_clear_statistics", _slow_removal(clock, 25.0)
        ),
    ):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        result = await _finish_import(hass, flow_id)
        assert result["step_id"] == "import_failed"

        (record,) = _import_failure_records(caplog)
        # This fake sends no HTTP requests, so there is no median to give.
        assert (
            "window=1/13 page=2 bound=before calls=5 in_flight=yes "
            "request_elapsed=3.0s total_elapsed=12.0s median_request=-"
        ) in record.getMessage()
        # The retry gap still counts from when the failure could be shown.
        result = await hass.config_entries.flow.async_configure(flow_id, {})
    assert result["errors"] == {"base": "retry_too_soon"}
    assert result["description_placeholders"]["seconds"] == "120"


@pytest.mark.asyncio
async def test_cancellation_warning_times_the_request_and_leaves_out_the_rollback(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    meridian.blocked_direction = "CONSUMPTION"
    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(
            config_flow_module, "async_clear_statistics", _slow_removal(clock, 25.0)
        ),
    ):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        await _wait_until(lambda: bool(meridian.measurement_calls))
        clock.now += 4.0  # the first page has been waiting for 4 s
        task = hass.config_entries.flow._progress[flow_id].async_get_progress_task()
        assert task is not None
        hass.config_entries.flow.async_abort(flow_id)
        # hass.async_block_till_done skips tasks that are being cancelled.
        await asyncio.wait({task}, timeout=10)

    (record,) = _flow_warnings(caplog)
    assert "the statistics it created were removed" in record.getMessage()
    assert (
        "window=1/13 page=1 bound=dates calls=1 in_flight=yes request_elapsed=4.0s "
        "total_elapsed=4.0s median_request=-)"
    ) in record.getMessage()


@pytest.mark.asyncio
async def test_failure_warning_leaves_out_a_malformed_upstream_timestamp(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """History Meridian sent in an unusable shape stays out of the traceback."""
    malformed = "2026-07-16 noon"
    with pytest.raises(ValueError, match="Invalid timestamp") as caught:
        parse_datetime(malformed, "measurement start")
    meridian.failure = lambda _direction, _before: caught.value
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["errors"] == {"base": "import_invalid_data"}
    (record,) = _import_failure_records(caplog)
    formatted = logging.Formatter().format(record)
    assert "ValueError: Invalid timestamp for measurement start" in formatted
    assert malformed not in formatted


@pytest.mark.asyncio
async def test_a_failure_after_the_last_request_is_not_blamed_on_it(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """A recorder failure after the last page names that page as finished."""
    del meridian

    async def broken_import(*_args: object, **_kwargs: object) -> None:
        await asyncio.sleep(0)
        raise HomeAssistantError("synthetic recorder failure")

    with patch.object(coordinator_module, "async_replay", broken_import):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["errors"] == {"base": "unknown"}
    assert result["description_placeholders"]["detail"] == (
        "HomeAssistantError after requesting grid import history, window 13 of 13, "
        "page 1 (account 1 of 1, property 1 of 1)"
    )
    (record,) = _import_failure_records(caplog)
    assert "window=13/13 page=1 bound=dates calls=13 in_flight=no" in (
        record.getMessage()
    )


@pytest.mark.asyncio
async def test_statistics_the_recorder_never_confirms_fail_the_attempt(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    An import whose replay is not confirmed shows its own failure form.

    The rows the replay queued are not taken for saved history: the attempt
    fails, its rollback removes what it created, and a retry imports again.
    """
    del meridian

    async def unconfirmed(*_args: object, **_kwargs: object) -> None:
        await asyncio.sleep(0)
        raise StatisticsWriteError("synthetic: not confirmed")

    with patch.object(coordinator_module, "async_replay", unconfirmed):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "import_statistics_write"}
    assert result["description_placeholders"]["detail"] == (
        "Home Assistant did not confirm that it had saved the statistics after "
        "requesting grid import history, window 13 of 13, page 1 (account 1 of 1, "
        "property 1 of 1)"
    )
    (record,) = _import_failure_records(caplog)
    assert "reason=import_statistics_write cause=StatisticsWriteError" in (
        record.getMessage()
    )
    assert record.getMessage().endswith("rollback=removed")


@pytest.mark.asyncio
async def test_statistics_with_no_running_total_to_continue_refuse_the_import(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Kept statistics older than 800 days before the history refuse the import.

    The replay would otherwise start the running total from zero below them.
    The form says so, rather than that Home Assistant may be busy: waiting
    does not help, and a retry refuses again until those statistics are
    removed. Nothing is written, and the kept statistics stay as they were.
    """
    del meridian
    energy_id, cost_id = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    old = (_NOW - timedelta(days=1000)).replace(minute=0)
    async_add_external_statistics(
        hass,
        _energy_metadata(energy_id, "Meridian grid import"),
        [{"start": old, "state": 1.0, "sum": 1.0}],
    )
    await async_wait_recording_done(hass)

    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "import_statistics_refused"}
    assert result["description_placeholders"]["detail"].startswith(
        "older statistics have no running total the import could continue after "
        "requesting grid import history"
    )
    (record,) = _import_failure_records(caplog)
    assert "reason=import_statistics_refused cause=StatisticsAnchorError" in (
        record.getMessage()
    )
    stored = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        old - timedelta(days=1),
        None,
        {energy_id, cost_id},
        "hour",
        None,
        {"sum"},
    )
    assert [row["start"] for row in stored[energy_id]] == [old.timestamp()]
    assert cost_id not in stored


@pytest.mark.asyncio
async def test_a_retry_writes_the_cost_a_failed_attempt_left_out_of_kept_statistics(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    A retry reaches back to the oldest hour a failed attempt half saved.

    The statistics were kept from an earlier installation, so the rollback
    leaves the rows the first attempt added. The recorder saved that
    attempt's usage and dropped its cost for good, and the attempt failed.
    Its retry's coordinator is a new one, which finds the usage saved up to
    now: it runs a 14-day restart, and left the cost of the 76 days before
    with no row, for good, though setup then succeeded. The flow keeps the
    oldest hour each attempt left unconfirmed (``_note_unconfirmed``), so
    the retry reaches back to it, whatever its mode.
    """
    del meridian
    energy_id, cost_id = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    earlier = (_NOW - timedelta(days=100)).replace(minute=0)
    for metadata in (
        _energy_metadata(energy_id, "Meridian grid import"),
        _cost_metadata(cost_id, "Meridian grid import cost"),
    ):
        async_add_external_statistics(
            hass, metadata, [{"start": earlier, "state": 1.0, "sum": 1.0}]
        )
    await async_wait_recording_done(hass)
    real_import = recorder_statistics.import_statistics
    dropped: list[str] = []

    def drop_cost_once(
        instance: Recorder, metadata: Any, statistics: Any, table: Any
    ) -> bool:
        if metadata["statistic_id"] == cost_id and not dropped:
            dropped.append(cost_id)
            # What the recorder's job returns after a permanent error.
            return True
        return real_import(instance, metadata, statistics, table)

    with patch(
        "homeassistant.components.recorder.tasks.statistics.import_statistics",
        side_effect=drop_cost_once,
    ):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        result = await _finish_import(hass, flow_id)
    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "import_statistics_write"}
    assert dropped == [cost_id]
    first = min(
        row["start"]
        for row in await _rows(hass, energy_id)
        if row["start"] > earlier.timestamp()
    )
    assert first <= (_NOW - timedelta(days=89)).timestamp()

    clock.now += 3 * 3600
    with patch(
        "custom_components.meridian_energy.coordinator._utcnow",
        return_value=_NOW + timedelta(hours=3),
    ):
        result = await hass.config_entries.flow.async_configure(flow_id, {})
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    energy = await _rows(hass, energy_id)
    costs = await _rows(hass, cost_id)
    assert [row["start"] for row in costs] == [row["start"] for row in energy]
    assert energy[1]["start"] == first
    assert _running_sum_breaks(energy) == []
    assert _running_sum_breaks(costs) == []
    # Confirmed by the retry: the entry is handed no hour to reach back to.
    handoff = hass.data[DOMAIN][SETUP_HANDOFF][_TEST_EMAIL]
    assert handoff.memory.unconfirmed == {}


@pytest.mark.asyncio
async def test_going_on_without_history_hands_over_an_unconfirmed_replay(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    Kept statistics a failed attempt half saved still reach back after setup.

    The grid export's statistics were kept from an earlier installation:
    the attempt's deep replay of them saved the usage and lost the cost,
    while the grid import met only gateway timeouts, so the form offers to
    go on without history. Chosen, setup hands the entry the oldest hour
    of that replay, as a retry would have reached back to it.
    """
    meridian.account = _test_account(feed_in=True)
    key = property_key(_TEST_ACCOUNT, _TEST_PROPERTY)
    energy_id, cost_id = generation_ids(key)
    earlier = (_NOW - timedelta(days=100)).replace(minute=0)
    for metadata in (
        _energy_metadata(energy_id, "Meridian grid export"),
        _cost_metadata(cost_id, "Meridian grid export value"),
    ):
        async_add_external_statistics(
            hass, metadata, [{"start": earlier, "state": 1.0, "sum": 1.0}]
        )
    await async_wait_recording_done(hass)
    real_import = recorder_statistics.import_statistics
    dropped: list[str] = []

    def drop_cost_once(
        instance: Recorder, metadata: Any, statistics: Any, table: Any
    ) -> bool:
        if metadata["statistic_id"] == cost_id and not dropped:
            dropped.append(cost_id)
            return True
        return real_import(instance, metadata, statistics, table)

    meridian.failure = lambda direction, _before: (
        _gateway_timeout() if direction == "CONSUMPTION" else None
    )
    with patch(
        "homeassistant.components.recorder.tasks.statistics.import_statistics",
        side_effect=drop_cost_once,
    ):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        result = await _finish_import(hass, flow_id)
    assert result["errors"] == {"base": "upstream_timeout"}
    assert dropped == [cost_id]
    assert _schema_fields(result) == ["continue_without_history"]

    clock.now += 121
    result = await hass.config_entries.flow.async_configure(
        flow_id, {"continue_without_history": True}
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    handoff = hass.data[DOMAIN][SETUP_HANDOFF][_TEST_EMAIL]
    assert handoff.imported is False
    (cache_key,) = handoff.memory.unconfirmed
    assert cache_key == (key, "GENERATION")
    assert handoff.memory.unconfirmed[cache_key] <= _NOW - timedelta(days=89)


@pytest.mark.asyncio
async def test_closing_the_dialog_while_saving_says_no_request_was_in_flight(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    del meridian
    saving = asyncio.Event()

    async def slow_import(*_args: object, **_kwargs: object) -> None:
        saving.set()
        await asyncio.Event().wait()

    with patch.object(coordinator_module, "async_replay", slow_import):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        await asyncio.wait_for(saving.wait(), 10)
        task = hass.config_entries.flow._progress[flow_id].async_get_progress_task()
        assert task is not None
        hass.config_entries.flow.async_abort(flow_id)
        # hass.async_block_till_done skips tasks that are being cancelled.
        await asyncio.wait({task}, timeout=10)

    assert task.cancelled()
    (record,) = [
        record
        for record in caplog.records
        if record.getMessage().startswith("Meridian setup was cancelled")
    ]
    assert "window=13/13 page=1 bound=dates calls=13 in_flight=no" in (
        record.getMessage()
    )


@pytest.mark.asyncio
async def test_cancelled_import_task_with_the_flow_still_open_offers_a_retry(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """A task cancelled from outside the flow still ends on the failure form."""
    meridian.blocked_direction = "CONSUMPTION"
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    for _ in range(100):
        if meridian.measurement_calls:
            break
        await asyncio.sleep(0.01)
    progress_task = hass.config_entries.flow._progress[
        flow_id
    ].async_get_progress_task()
    assert progress_task is not None
    progress_task.cancel()
    # hass.async_block_till_done skips tasks that are being cancelled.
    await asyncio.wait({progress_task}, timeout=10)
    result = await _finish_import(hass, flow_id)

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "unknown"}
    assert result["description_placeholders"]["detail"] == (
        "the import was cancelled while requesting grid import history, window 1 of "
        "13, page 1 (account 1 of 1, property 1 of 1)"
    )
    # One warning for one event. It does not claim the dialog was closed, nor
    # guess at what else cancelled the task.
    (record,) = _flow_warnings(caplog)
    message = record.getMessage()
    assert message.startswith(
        "Meridian setup was cancelled before the history import finished, "
        "while the setup dialog was still open; "
    )
    assert "dialog was closed" not in message
    assert "stopping" not in message
    assert "operation=measurements direction=CONSUMPTION" in message
    assert record.exc_info is None


def _flow_ids(hass: HomeAssistant) -> set[str]:
    return {
        flow["flow_id"]
        for flow in hass.config_entries.flow.async_progress(include_uninitialized=True)
    }


async def _start_user_flow(
    hass: HomeAssistant, email: str = _TEST_EMAIL
) -> ConfigFlowResult:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    return await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_EMAIL: email}
    )


def _flow_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Return every record the config flow wrote, at any level."""
    return [
        record
        for record in caplog.records
        if record.name == config_flow_module.__name__
    ]


@pytest.mark.parametrize("step", ["import_failed", "import_failed_last"])
@pytest.mark.asyncio
async def test_new_setup_replaces_a_flow_parked_at_the_failure_form(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
    step: str,
) -> None:
    """
    A failure form left open in a closed browser tab does not block setup.

    Home Assistant does not list user-started flows, so the parked flow could
    not be resumed or dismissed, and it held the email as its unique ID. So
    does the last failure's form, which offers only to go on without
    history. Replacing either logs nothing: the flow logs only failed and
    cancelled imports.
    """
    caplog.set_level(logging.DEBUG, logger=config_flow_module.__name__)
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    first = await _submit_login(hass)
    first = await _finish_import(hass, first["flow_id"])
    if step == "import_failed_last":
        for _attempt in range(4):
            clock.now += 120
            await hass.config_entries.flow.async_configure(first["flow_id"], {})
            first = await _finish_import(hass, first["flow_id"])
    assert first["step_id"] == step
    caplog.clear()

    second = await _start_user_flow(hass)

    assert second["type"] is FlowResultType.FORM
    assert second["step_id"] == "otp"
    assert first["flow_id"] not in _flow_ids(hass)
    assert second["flow_id"] in _flow_ids(hass)
    assert _flow_records(caplog) == []


@pytest.mark.parametrize(
    "succeeded", [True, False], ids=["import-succeeded", "import-failed"]
)
@pytest.mark.asyncio
async def test_new_setup_replaces_a_setup_whose_import_ended_unwatched(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    succeeded: bool,
) -> None:
    """
    A setup whose import ended in a closed browser tab does not block the email.

    Nobody fetched the step after the import, so the flow waits at progress
    done, still holding the email as its unique ID. A successful import's
    statistics stay, and the new setup continues them.
    """
    if not succeeded:
        meridian.failure = lambda _direction, _before: _gateway_timeout()
    first = await _submit_login(hass)
    await hass.async_block_till_done()
    parked = hass.config_entries.flow._progress[first["flow_id"]].cur_step
    assert parked is not None
    assert parked["type"] is FlowResultType.SHOW_PROGRESS_DONE
    assert parked["step_id"] == ("finish" if succeeded else "import_failed")

    second = await _start_user_flow(hass)

    assert second["type"] is FlowResultType.FORM
    assert second["step_id"] == "otp"
    assert _flow_ids(hass) == {second["flow_id"]}
    if not succeeded:
        return
    first_import_calls = len(meridian.measurement_calls)
    result = await hass.config_entries.flow.async_configure(
        second["flow_id"], {"otp": "123456"}
    )
    result = await _finish_import(hass, second["flow_id"])
    assert result["type"] is FlowResultType.CREATE_ENTRY
    # Only a catch-up: the earlier import's history was kept and continued.
    assert len(meridian.measurement_calls) - first_import_calls < first_import_calls
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    rows = await _rows(hass, energy_id)
    assert len(rows) == 90 * 24 - 1
    assert rows[0]["sum"] == pytest.approx(rows[0]["state"])
    for previous, current in pairwise(rows):
        assert current["sum"] - previous["sum"] == pytest.approx(current["state"])


@pytest.mark.parametrize("parked_step", ["user", "otp", "discovery_retry", "accounts"])
@pytest.mark.asyncio
async def test_new_setup_replaces_a_flow_parked_at_a_login_form(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    caplog: pytest.LogCaptureFixture,
    parked_step: str,
) -> None:
    caplog.set_level(logging.DEBUG, logger=config_flow_module.__name__)
    if parked_step == "user":
        # A failed send leaves the email form open, already holding the email
        # as the flow's unique ID.
        meridian.async_send_otp = AsyncMock(
            side_effect=[MeridianConnectionError(), None]
        )
    elif parked_step == "discovery_retry":
        meridian.async_get_accounts = AsyncMock(side_effect=MeridianConnectionError())
    elif parked_step == "accounts":
        meridian.async_get_accounts = AsyncMock(
            return_value=(_test_account(), _account("A-TEST-2"))
        )
    first = await _start_user_flow(hass)
    if parked_step in {"discovery_retry", "accounts"}:
        first = await hass.config_entries.flow.async_configure(
            first["flow_id"], {"otp": "123456"}
        )
    assert first["type"] is FlowResultType.FORM
    assert first["step_id"] == parked_step
    caplog.clear()

    second = await _start_user_flow(hass)

    assert second["step_id"] == "otp"
    assert _flow_ids(hass) == {second["flow_id"]}
    assert _flow_records(caplog) == []


@pytest.mark.asyncio
async def test_new_setup_leaves_a_running_import_alone(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    meridian.blocked_direction = "CONSUMPTION"
    first = await _submit_login(hass)
    assert first["type"] is FlowResultType.SHOW_PROGRESS
    for _ in range(100):
        if meridian.measurement_calls:
            break
        await asyncio.sleep(0.01)

    second = await _start_user_flow(hass)

    assert second["type"] is FlowResultType.ABORT
    assert second["reason"] == "already_in_progress"
    # Home Assistant's own text, as when the abort is raised to it.
    assert second.get("translation_domain") == "homeassistant"
    assert _flow_ids(hass) == {first["flow_id"]}
    meridian.release.set()
    result = await _finish_import(hass, first["flow_id"])
    assert result["type"] is FlowResultType.CREATE_ENTRY


@pytest.mark.asyncio
async def test_new_setup_leaves_a_setup_that_is_creating_its_entry_alone(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    A setup whose entry is being set up keeps its flow until that returns.

    Home Assistant adds the entry, then waits for the entry's own setup with
    the flow still at progress done to finish. A new setup for the email
    took that flow for a parked one and aborted it, so the setup that had
    succeeded ended with an unknown-flow error in its dialog.
    """
    del meridian
    first = await _submit_login(hass)
    await hass.async_block_till_done()
    setting_up = asyncio.Event()
    finish_setup = asyncio.Event()

    async def slow_setup_entry(hass_: HomeAssistant, entry: object) -> bool:
        del hass_, entry
        setting_up.set()
        await finish_setup.wait()
        return True

    with patch(
        "custom_components.meridian_energy.async_setup_entry", new=slow_setup_entry
    ):
        finishing = hass.async_create_task(
            hass.config_entries.flow.async_configure(first["flow_id"])
        )
        await setting_up.wait()

        second = await _start_user_flow(hass)

        assert second["type"] is FlowResultType.ABORT
        assert second["reason"] == "already_configured"
        assert _flow_ids(hass) == {first["flow_id"]}
        finish_setup.set()
        result = await finishing

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    assert _flow_ids(hass) == set()


@pytest.mark.asyncio
async def test_new_setup_refuses_a_configured_email_before_touching_any_flow(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    A configured email is refused before any parked flow is looked for.

    That refusal is what leaves a reauthentication alone, even one waiting
    at the code form: it runs only while its entry exists, because Home
    Assistant aborts it when the entry is removed or reloaded.
    """
    del meridian
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id=_TEST_EMAIL,
        data={
            CONF_EMAIL: _TEST_EMAIL,
            CONF_REFRESH_TOKEN: "synthetic-refresh",
            CONF_FIREBASE_USER_ID: "synthetic-user",
            CONF_SELECTED_ACCOUNTS: [_TEST_ACCOUNT],
        },
    )
    entry.add_to_hass(hass)
    entry.async_start_reauth(hass)
    await hass.async_block_till_done()
    (reauth,) = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    reauth = await hass.config_entries.flow.async_configure(reauth["flow_id"], {})
    assert reauth["step_id"] == "otp"
    other = await _start_user_flow(hass, "someone@example.com")
    assert other["step_id"] == "otp"

    result = await _start_user_flow(hass)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    assert _flow_ids(hass) == {reauth["flow_id"], other["flow_id"]}


@pytest.mark.asyncio
async def test_new_setup_leaves_parked_setups_for_other_emails_alone(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    del meridian
    other = await _start_user_flow(hass, "someone@example.com")
    assert other["step_id"] == "otp"

    result = await _start_user_flow(hass)

    assert result["step_id"] == "otp"
    assert _flow_ids(hass) == {other["flow_id"], result["flow_id"]}


@pytest.mark.asyncio
async def test_flow_removed_during_login_starts_no_import(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """A flow aborted while its code is being checked never starts an import."""
    validating = asyncio.Event()
    finish_validation = asyncio.Event()

    async def slow_validate(*_args: object) -> MeridianTokenSet:
        validating.set()
        await finish_validation.wait()
        return _tokens()

    meridian.async_validate_otp = AsyncMock(side_effect=slow_validate)
    first = await _start_user_flow(hass)
    submitted = hass.async_create_task(
        hass.config_entries.flow.async_configure(first["flow_id"], {"otp": "123456"})
    )
    await validating.wait()

    second = await _start_user_flow(hass)
    assert second["step_id"] == "otp"
    finish_validation.set()
    result = await submitted
    await hass.async_block_till_done()

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "login_expired"
    assert meridian.measurement_calls == []
    assert _flow_ids(hass) == {second["flow_id"]}


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (RequestContext("accounts", 1, 0.0), "the account list"),
        (
            RequestContext("billing", 4, 0.0, account_index=2, account_count=2),
            "the billing period (account 2 of 2)",
        ),
        (
            RequestContext(
                "measurements",
                3,
                0.0,
                direction="GENERATION",
                account_index=1,
                account_count=1,
                property_index=2,
                property_count=2,
                page=3,
                bound="before",
            ),
            "grid export history, page 3 (account 1 of 1, property 2 of 2)",
        ),
        (RequestContext("other", 1, 0.0), "other"),
    ],
)
def test_failure_detail_describes_the_call_without_identifiers(
    call: RequestContext, expected: str
) -> None:
    assert _describe_call(call) == expected


def _caused_by(outer: BaseException, inner: BaseException) -> BaseException:
    outer.__cause__ = inner
    return outer


@pytest.mark.parametrize(
    ("error", "detail", "root"),
    [
        (
            _caused_by(
                MeridianConnectionError("session"),
                MeridianTimeoutError("late", error_class="ServerTimeoutError"),
            ),
            "no answer within the time limit",
            "ServerTimeoutError",
        ),
        (
            _caused_by(
                MeridianConnectionError("network"),
                MeridianTransportError("down", error_class="ClientConnectorDNSError"),
            ),
            "a network error (ClientConnectorDNSError)",
            "ClientConnectorDNSError",
        ),
        (
            _caused_by(
                MeridianConnectionError("unreadable"),
                MeridianUnreadableResponseError(200, "text"),
            ),
            "an unreadable answer (HTTP 200, text)",
            "MeridianUnreadableResponseError",
        ),
        (
            MeridianTransportError("bare"),
            "MeridianTransportError",
            "MeridianTransportError",
        ),
        (RuntimeError("unexpected"), "RuntimeError", "RuntimeError"),
    ],
)
def test_failure_detail_names_the_failure_in_words(
    error: BaseException, detail: str, root: str
) -> None:
    """The form describes a failure in words; the warning names its root class."""
    assert _describe_failure(error) == detail
    assert _root_name(error) == root


def test_a_cancelled_import_is_described_as_such() -> None:
    assert _describe_failure(None) == "the import was cancelled"


@pytest.mark.asyncio
async def test_rotated_tokens_before_pending_data_are_kept(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    rotated = _tokens()

    await flow._async_store_discovery_tokens(rotated)

    assert flow._tokens == rotated
    assert flow._pending_data is None


@pytest.mark.asyncio
async def test_failed_initial_import_rolls_back_on_real_recorder(hass) -> None:
    """A partial first import is removed through the recorder's own thread.

    This module runs every test with the real recorder (``recorder_mock``), so
    the rollback is not mocked: clearing from the wrong thread raises here.
    """
    account = _account()
    key = property_key(account.number, account.properties[0].id)
    energy_id, cost_id = consumption_ids(key)
    unrelated_id = "meridian_energy:consumption_unrelated000"
    async_add_external_statistics(
        hass,
        _energy_metadata(unrelated_id, "Unrelated"),
        [{"start": datetime(2026, 8, 1, tzinfo=UTC), "state": 1.0, "sum": 1.0}],
    )
    await async_wait_recording_done(hass)

    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._accounts = (account,)
    flow._selected_accounts = frozenset({account.number})
    await flow._async_prepare_import_rollback()
    assert flow._initial_statistic_ids == {energy_id, cost_id}
    assert flow._existing_statistic_ids == set()

    async def partial_import(
        tokens: MeridianTokenSet,
        selected_accounts: frozenset[str],
        time_limit: asyncio.Timeout | None,
    ) -> None:
        del tokens, selected_accounts, time_limit
        start = datetime(2026, 8, 1, tzinfo=UTC)
        await async_replay(
            hass,
            stat_energy_id=energy_id,
            stat_cost_id=cost_id,
            energy_name="Meridian grid import",
            cost_name="Meridian grid import cost",
            measurements=[
                MeridianMeasurement(
                    start=start,
                    end=start + timedelta(hours=1),
                    value_kwh=Decimal("1.5"),
                    quality="ACTUAL",
                    direction="CONSUMPTION",
                    channel_id="synthetic-meter:synthetic-register",
                    cost_cents=Decimal(45),
                )
            ],
        )
        await async_wait_recording_done(hass)
        assert await async_has_statistics(hass, energy_id)
        raise MeridianConnectionError

    with (
        patch.object(flow, "_async_initial_import", side_effect=partial_import),
        pytest.raises(MeridianConnectionError),
    ):
        await flow._async_initial_import_with_rollback(
            _tokens(), frozenset({account.number})
        )

    assert not await async_has_statistics(hass, energy_id)
    assert not await async_has_statistics(hass, cost_id)
    assert await async_has_statistics(hass, unrelated_id)


@pytest.mark.asyncio
async def test_initial_import_and_finish_expired_guards(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}

    import_result = await flow.async_step_initial_import()
    failed_result = await flow.async_step_import_failed()
    finish_result = await flow.async_step_finish()
    start_result = await flow._start_initial_import()
    accounts_result = await flow.async_step_accounts()

    # A progress step may not abort, so its guard ends through the failure step.
    assert import_result["type"] is FlowResultType.SHOW_PROGRESS_DONE
    assert import_result["step_id"] == "import_failed"
    assert failed_result["reason"] == "login_expired"
    assert finish_result["reason"] == "login_expired"
    assert start_result["reason"] == "login_expired"
    assert accounts_result["reason"] == "login_expired"


@pytest.mark.asyncio
async def test_cancelled_initial_import_rolls_back_new_statistics(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._initial_statistic_ids = {"new", "existing"}
    flow._existing_statistic_ids = {"existing"}
    with (
        patch.object(
            flow,
            "_async_initial_import",
            new=AsyncMock(side_effect=asyncio.CancelledError),
        ),
        patch(
            "custom_components.meridian_energy.config_flow.async_clear_statistics",
            new=AsyncMock(),
        ) as clear,
        pytest.raises(asyncio.CancelledError),
    ):
        await flow._async_initial_import_with_rollback(
            _tokens(), frozenset({"synthetic-account"})
        )

    clear.assert_awaited_once_with(hass, {"new"})


@pytest.mark.asyncio
async def test_initial_import_uses_authenticated_coordinator(hass) -> None:
    tokens = _tokens()
    coordinator = MagicMock()
    coordinator.async_fetch_and_import = AsyncMock()

    with patch(
        "custom_components.meridian_energy.config_flow.MeridianDataCoordinator",
        return_value=coordinator,
    ) as coordinator_class:
        flow = MeridianEnergyConfigFlow()
        flow.hass = hass
        flow.context = {}
        await flow._async_initial_import(tokens, frozenset({"synthetic-account"}))

    client = coordinator_class.call_args.args[1]
    assert client.tokens == tokens
    # Setup leaves billing to the entry's first sync, shares the attempt's
    # time limit with the import, and moves the progress ring on.
    coordinator.async_fetch_and_import.assert_awaited_once_with(
        setup_import=True,
        time_budget=config_flow_module.SETUP_IMPORT_BUDGET_SECONDS,
        time_limit=None,
        progress=flow.async_update_progress,
    )
    assert coordinator_class.call_args.kwargs["selected_accounts"] == {
        "synthetic-account"
    }


@pytest.mark.asyncio
async def test_otp_form_is_serializable_and_validates_locally(hass) -> None:
    """The native config-flow API must be able to serialize the OTP form."""
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._email = "person@example.com"
    flow._journey_id = "journey"
    client = MagicMock()
    client.async_validate_otp = AsyncMock()

    result = await flow.async_step_otp()
    probatio.to_field_list(
        result["data_schema"], custom_serializer=cv.custom_serializer
    )

    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await flow.async_step_otp({"otp": "12ab"})

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "otp_invalid"}
    client.async_validate_otp.assert_not_awaited()


@pytest.mark.asyncio
async def test_user_flow_prevents_duplicate(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="person@example.com",
        data={CONF_EMAIL: "person@example.com"},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_EMAIL: "person@example.com"},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


_UPSTREAM_ERRORS = [
    (MeridianUpstreamError(504, gateway=True), "upstream_timeout"),
    (MeridianUpstreamError(None, gateway=True), "upstream_timeout"),
    (MeridianUpstreamError(503, gateway=False), "upstream_unavailable"),
]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (MeridianConnectionError(), "cannot_connect"),
        (MeridianAuthenticationError(), "email_not_found"),
        *_UPSTREAM_ERRORS,
    ],
)
@pytest.mark.asyncio
async def test_user_flow_send_errors(hass, error: Exception, expected: str) -> None:
    client = MagicMock()
    client.async_send_otp = AsyncMock(side_effect=error)
    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_EMAIL: "person@example.com"}
        )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": expected}


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("OTP_EXPIRED", "otp_expired"),
        ("OTP_INVALID", "otp_invalid"),
        ("OTP_TOO_MANY_ATTEMPTS", "otp_too_many_attempts"),
        ("OTP_NOT_FOUND", "otp_not_found"),
        ("OTHER", "invalid_auth"),
    ],
)
@pytest.mark.asyncio
async def test_otp_errors(hass, code: str, expected: str) -> None:
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(side_effect=MeridianOtpError(code, "hidden"))
    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_EMAIL: "person@example.com"}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": expected}


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (MeridianConnectionError(), "cannot_connect"),
        (MeridianAuthenticationError(), "invalid_auth"),
        *_UPSTREAM_ERRORS,
    ],
)
@pytest.mark.asyncio
async def test_otp_service_errors(hass, error: Exception, expected: str) -> None:
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(side_effect=error)
    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_EMAIL: "person@example.com"}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )

    assert result["errors"] == {"base": expected}


@pytest.mark.asyncio
async def test_reauth_updates_existing_entry(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="person@example.com",
        unique_id="person@example.com",
        data={
            CONF_EMAIL: "person@example.com",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(return_value=_tokens())
    with (
        patch.object(
            MeridianEnergyConfigFlow,
            "_client",
            new_callable=PropertyMock,
            return_value=client,
        ),
        patch.object(hass.config_entries, "async_reload", AsyncMock()),
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=dict(entry.data),
        )
        assert result["step_id"] == "reauth_confirm"
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
        assert result["step_id"] == "otp"
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    assert entry.data[CONF_REFRESH_TOKEN] == "synthetic-refresh"


@pytest.mark.asyncio
async def test_a_second_reauth_confirmation_sends_no_second_code(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """A confirmation sent again while the code is being sent sends no other."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id=_TEST_EMAIL,
        data={
            CONF_EMAIL: _TEST_EMAIL,
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
        },
    )
    entry.add_to_hass(hass)
    sending = asyncio.Event()
    finish_sending = asyncio.Event()

    async def send(*_args: object) -> None:
        await asyncio.sleep(0)
        if not sending.is_set():  # Only the first send is slow.
            sending.set()
            await finish_sending.wait()

    meridian.async_send_otp = AsyncMock(side_effect=send)
    with patch.object(hass.config_entries, "async_reload", AsyncMock()):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=dict(entry.data),
        )
        flow_id = result["flow_id"]
        first = hass.async_create_task(
            hass.config_entries.flow.async_configure(flow_id, {})
        )
        await sending.wait()
        second = hass.async_create_task(
            hass.config_entries.flow.async_configure(flow_id, {})
        )
        await asyncio.sleep(0.05)
        assert not second.done()
        finish_sending.set()
        for answer in (await first, await second):
            assert answer["type"] is FlowResultType.FORM
            assert answer["step_id"] == "otp"
        meridian.async_send_otp.assert_awaited_once()
        result = await hass.config_entries.flow.async_configure(
            flow_id, {"otp": "123456"}
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    sent_journey = meridian.async_send_otp.await_args.args[1]
    assert meridian.async_validate_otp.await_args.args[2] == sent_journey
    assert entry.data[CONF_REFRESH_TOKEN] == "synthetic-refresh"


@pytest.mark.asyncio
async def test_expired_flow_guards(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}

    otp_result = await flow.async_step_otp()
    confirm_result = await flow.async_step_reauth_confirm({})
    discovery_result = await flow.async_step_discovery_retry({})

    assert otp_result["reason"] == "login_expired"
    assert confirm_result["reason"] == "login_expired"
    assert discovery_result["reason"] == "login_expired"


@pytest.mark.asyncio
async def test_reauth_aborts_on_unique_id_mismatch(hass) -> None:
    """
    Home Assistant's unique-ID check is wired into reauthentication.

    Setup never creates an entry whose stored email differs from its unique ID,
    so this builds that inconsistent entry directly to reach the abort.
    """
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id="person@example.com",
        data={
            CONF_EMAIL: "someone-else@example.com",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(return_value=_tokens())
    with (
        patch.object(
            MeridianEnergyConfigFlow,
            "_client",
            new_callable=PropertyMock,
            return_value=client,
        ),
        patch.object(hass.config_entries, "async_reload", AsyncMock()) as reload_entry,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=dict(entry.data),
        )
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"otp": "123456"}
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "unique_id_mismatch"
    assert entry.data[CONF_REFRESH_TOKEN] == "old-refresh"
    reload_entry.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_second_code_submission_gets_the_abort_the_first_raised(
    hass: HomeAssistant,
) -> None:
    """
    A code pressed twice during reauthentication gets one abort, twice.

    The unique-ID check raises its abort once the code is accepted; the
    second submission waited for the first and ends the same way, rather
    than coming back with the code form of a flow that no longer exists.
    """
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id="person@example.com",
        data={
            CONF_EMAIL: "someone-else@example.com",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_FIREBASE_USER_ID: "old-user",
        },
    )
    entry.add_to_hass(hass)
    checking = asyncio.Event()
    finish_check = asyncio.Event()

    async def validate(*_args: object) -> MeridianTokenSet:
        checking.set()
        await finish_check.wait()
        return _tokens()

    client = MagicMock()
    client.async_send_otp = AsyncMock()
    client.async_validate_otp = AsyncMock(side_effect=validate)
    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=dict(entry.data),
        )
        flow_id = result["flow_id"]
        result = await hass.config_entries.flow.async_configure(flow_id, {})
        assert result["step_id"] == "otp"
        first = hass.async_create_task(
            hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
        )
        await checking.wait()
        second = hass.async_create_task(
            hass.config_entries.flow.async_configure(flow_id, {"otp": "123456"})
        )
        await asyncio.sleep(0.05)
        assert not second.done()
        finish_check.set()
        answers = [await first, await second]

    for answer in answers:
        assert answer["type"] is FlowResultType.ABORT
        assert answer["reason"] == "unique_id_mismatch"
    client.async_validate_otp.assert_awaited_once()
    assert _flow_ids(hass) == set()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (MeridianConnectionError(), "cannot_connect"),
        (MeridianAuthenticationError(), "email_not_found"),
        *_UPSTREAM_ERRORS,
    ],
)
@pytest.mark.asyncio
async def test_reauth_send_errors(hass, error: Exception, expected: str) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id="person@example.com",
        data={
            CONF_EMAIL: "person@example.com",
            CONF_REFRESH_TOKEN: "synthetic-refresh",
            CONF_FIREBASE_USER_ID: "synthetic-user",
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_send_otp = AsyncMock(side_effect=error)

    with patch.object(
        MeridianEnergyConfigFlow,
        "_client",
        new_callable=PropertyMock,
        return_value=client,
    ):
        result = await entry.start_reauth_flow(hass)
        assert result["step_id"] == "reauth_confirm"
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})

    assert result["step_id"] == "reauth_confirm"
    assert result["errors"] == {"base": expected}


@pytest.mark.asyncio
async def test_a_login_code_that_cannot_be_sent_for_no_connection(
    hass: HomeAssistant,
) -> None:
    """
    A connection that is never made gives the message about reaching Meridian.

    A real aiohttp session whose name lookups never answer stands in for a
    stalled local network: nothing reached Meridian, so the form must not
    say that Meridian's service did not answer.
    """
    async with stalled_session() as stalled:
        with (
            patch.object(
                config_flow_module, "async_get_clientsession", return_value=stalled
            ),
            patch.object(api_module, "AUTH_EMAIL_URL", STALLED_URL),
            patch.object(transport_module, "CONNECT_TIMEOUT_SECONDS", 0.05),
        ):
            result = await hass.config_entries.flow.async_init(
                DOMAIN, context={"source": SOURCE_USER}
            )
            result = await hass.config_entries.flow.async_configure(
                result["flow_id"], {CONF_EMAIL: _TEST_EMAIL}
            )

    assert result["step_id"] == "user"
    assert result["errors"] == {"base": "cannot_connect"}


@pytest.mark.asyncio
async def test_client_property_uses_home_assistant_session(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    assert flow._client is not None


# What a stubbed attempt start returns: the progress of the import it started.
_STARTED = ConfigFlowResult(type=FlowResultType.SHOW_PROGRESS, step_id="started")


@pytest.mark.asyncio
async def test_account_discovery_retries_without_reusing_otp(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._email = "person@example.com"
    flow._journey_id = "journey"
    client = MagicMock()
    client.async_get_accounts = AsyncMock(
        side_effect=[MeridianConnectionError(), (_account(),)]
    )
    with (
        patch.object(flow, "_authenticated_client", return_value=client),
        patch.object(
            flow, "_start_initial_import", new=AsyncMock(return_value=_STARTED)
        ) as start,
    ):
        result = await flow._async_prepare_accounts(_tokens(), {})
        assert result["step_id"] == "discovery_retry"
        assert result["errors"] == {"base": "cannot_connect"}
        result = await flow.async_step_discovery_retry({})

    assert result == _STARTED
    assert client.async_get_accounts.await_count == 2
    start.assert_awaited_once()


@pytest.mark.asyncio
async def test_account_discovery_retains_rotated_credentials(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    client = MagicMock()
    client.async_get_accounts = AsyncMock(
        side_effect=[MeridianConnectionError(), (_account(),)]
    )
    rotated = MeridianTokenSet(
        id_token="rotated-id",
        refresh_token="rotated-refresh",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        user_id="rotated-user",
    )
    with (
        patch(
            "custom_components.meridian_energy.config_flow.MeridianApiClient",
            return_value=client,
        ) as client_class,
        patch.object(
            flow, "_start_initial_import", new=AsyncMock(return_value=_STARTED)
        ) as start,
    ):
        result = await flow._async_prepare_accounts(
            _tokens(),
            {
                CONF_REFRESH_TOKEN: "synthetic-refresh",
                CONF_FIREBASE_USER_ID: "synthetic-user",
            },
        )
        assert result["step_id"] == "discovery_retry"
        await client_class.call_args.kwargs["token_update_callback"](rotated)
        result = await flow.async_step_discovery_retry({})

    assert result == _STARTED
    assert flow._tokens == rotated
    assert flow._pending_data is not None
    assert flow._pending_data[CONF_REFRESH_TOKEN] == "rotated-refresh"
    assert flow._pending_data[CONF_FIREBASE_USER_ID] == "rotated-user"
    start.assert_awaited_once()


# A gateway's error page, with nothing in it the logs may repeat.
_GATEWAY_PAGE = b"<html><body>504 Gateway Time-out from 203.0.113.7</body></html>"


def _discovery_failure(kind: str, stalled: Any) -> Response | StalledConnection:
    """Return the answer to the account list for one kind of failure."""
    if kind == "gateway_timeout":
        return Response(
            _GATEWAY_PAGE, status=504, headers={"Content-Type": "text/html"}
        )
    if kind == "service_error":
        return Response(b"{}", status=503)
    if kind == "rate_limited":
        return Response(
            b"{}",
            status=429,
            headers={"Content-Type": "application/json", "Retry-After": "120"},
        )
    if kind == "no_connection":
        return StalledConnection(stalled)
    if kind == "graphql_error":
        # Meridian's free-text message, which must reach no record.
        message = f"No property synthetic-property on account {_TEST_ACCOUNT}"
        return json_response(
            {"errors": [{"message": message, "extensions": {"errorCode": "KT-CT-1"}}]}
        )
    return json_response({"data": {"viewer": {"accounts": "not a list"}}})


_DISCOVERY_FAILURES = [
    ("gateway_timeout", "upstream_timeout", "MeridianUpstreamError status=504"),
    ("service_error", "upstream_unavailable", "MeridianUpstreamError status=503"),
    ("rate_limited", "rate_limited", "MeridianRateLimitError status=429"),
    ("no_connection", "cannot_connect", "MeridianConnectionError status=-"),
    ("graphql_error", "invalid_data", "MeridianGraphQLError"),
    ("unusable_data", "invalid_data", "ValueError"),
]


@pytest.mark.parametrize("flow", ["setup", "reconfigure"])
@pytest.mark.parametrize(
    "failure", _DISCOVERY_FAILURES, ids=[kind for kind, *_ in _DISCOVERY_FAILURES]
)
@pytest.mark.asyncio
async def test_account_discovery_failures_are_named_and_logged(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
    flow: str,
    failure: tuple[str, str, str],
) -> None:
    """
    Finding the accounts, at setup or Reconfigure, says what failed and logs it.

    Data Home Assistant cannot use is a defect to report, so it leaves one
    warning with its traceback rather than no trace at all. A failure to get
    an answer is already on the form, and the transport describes the
    answer, so it gets one debug line. The real client and transport run in
    front of a synthetic service, through Home Assistant's flow manager.
    """
    kind, expected, error = failure
    caplog.set_level(logging.DEBUG, logger=config_flow_module.__name__)
    session = FakeMeridianSession(_NOW)
    async with stalled_session() as stalled:
        failures = {"remaining": 1}

        def fail_the_account_list(
            operation: str, _variables: dict[str, Any]
        ) -> Response | StalledConnection | None:
            if operation == "accountsList" and failures["remaining"]:
                failures["remaining"] -= 1
                return _discovery_failure(kind, stalled)
            return None

        session.hook = fail_the_account_list
        with (
            patch.object(
                config_flow_module, "async_get_clientsession", return_value=session
            ),
            patch.object(transport_module, "CONNECT_TIMEOUT_SECONDS", 0.05),
        ):
            if flow == "setup":
                result = await _submit_login(hass)
                step = "discovery_retry"
            else:
                entry = MockConfigEntry(
                    domain=DOMAIN,
                    title=NAME,
                    unique_id=_TEST_EMAIL,
                    data={
                        CONF_EMAIL: _TEST_EMAIL,
                        CONF_REFRESH_TOKEN: "synthetic-refresh",
                        CONF_FIREBASE_USER_ID: "synthetic-user",
                        CONF_SELECTED_ACCOUNTS: [_TEST_ACCOUNT],
                    },
                )
                entry.add_to_hass(hass)
                result = await entry.start_reconfigure_flow(hass)
                step = "reconfigure_discovery"

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == step
    assert result["errors"] == {"base": expected}
    records = [
        record
        for record in caplog.records
        if record.name == config_flow_module.__name__
    ]
    if expected == "invalid_data":
        (record,) = records
        assert record.levelno == logging.WARNING
        assert record.getMessage() == (
            "Meridian account discovery returned data Home Assistant could not "
            f"use ({error})"
        )
        assert record.exc_info is not None
        assert type(record.exc_info[1]).__name__ == error
    else:
        assert [(record.levelno, record.getMessage()) for record in records] == [
            (logging.DEBUG, f"Meridian account discovery failed: error={error}")
        ]
    # caplog.text holds each record as formatted, its traceback included.
    for text in ("203.0.113.7", "synthetic-property", _TEST_ACCOUNT):
        assert text not in caplog.text
    if flow == "reconfigure":
        # Submitting the form tries again, and this time the list comes.
        with patch.object(
            config_flow_module, "async_get_clientsession", return_value=session
        ):
            result = await hass.config_entries.flow.async_configure(
                result["flow_id"], {}
            )
        assert result["step_id"] == "reconfigure"


@pytest.mark.asyncio
async def test_account_discovery_auth_failure_requires_new_login(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=MeridianAuthenticationError())
    with patch.object(flow, "_authenticated_client", return_value=client):
        result = await flow._async_prepare_accounts(_tokens(), {})

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "login_expired"


@pytest.mark.asyncio
async def test_account_discovery_aborts_when_none_are_available(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=())
    with patch.object(flow, "_authenticated_client", return_value=client):
        result = await flow._async_prepare_accounts(_tokens(), {})
    assert result["reason"] == "no_accounts"


@pytest.mark.asyncio
async def test_multiple_accounts_are_selected_before_import(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._tokens = _tokens()
    flow._pending_data = {}
    flow._accounts = (_account("first"), _account("second"))

    form = await flow.async_step_accounts()
    invalid = await flow.async_step_accounts({CONF_SELECTED_ACCOUNTS: []})
    with patch.object(
        flow, "_start_initial_import", new=AsyncMock(return_value=_STARTED)
    ) as start:
        result = await flow.async_step_accounts({CONF_SELECTED_ACCOUNTS: ["second"]})

    assert form["step_id"] == "accounts"
    assert invalid["errors"] == {"base": "select_account"}
    assert result == _STARTED
    assert flow._selected_accounts == {"second"}
    start.assert_awaited_once()


@pytest.mark.asyncio
async def test_prepare_accounts_routes_multiple_accounts_to_selection(hass) -> None:
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    client = MagicMock()
    client.async_get_accounts = AsyncMock(
        return_value=(_account("first"), _account("second"))
    )
    with patch.object(flow, "_authenticated_client", return_value=client):
        result = await flow._async_prepare_accounts(_tokens(), {})

    assert result["step_id"] == "accounts"


@pytest.mark.asyncio
async def test_import_rollback_tracks_consumption_generation_and_existing(hass) -> None:
    feed_in = _account("feed-in")
    feed_in = MeridianAccount(
        feed_in.number,
        feed_in.status,
        (
            MeridianProperty(
                feed_in.properties[0].id,
                "  1 Synthetic\nStreet  ",
                (MeridianMeterPoint("meter", "market", has_feed_in=True),),
            ),
        ),
    )
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {}
    flow._accounts = (feed_in, _account("ignored"))
    flow._selected_accounts = frozenset({"feed-in"})
    flow._tokens = _tokens()
    flow._pending_data = {}

    with patch(
        "custom_components.meridian_energy.config_flow.async_has_statistics",
        new=AsyncMock(side_effect=[True, False, False, False]),
    ):
        await flow._async_prepare_import_rollback()

    assert len(flow._initial_statistic_ids) == 4
    assert len(flow._existing_statistic_ids) == 1
    assert _account_label(feed_in).startswith("1 Synthetic Street ·")
    multi_property = MeridianAccount(
        feed_in.number,
        feed_in.status,
        (
            *feed_in.properties,
            MeridianProperty("second", "2 Synthetic Street", ()),
        ),
    )
    assert _account_label(multi_property).startswith("2 properties ·")
    no_property = MeridianAccount("A-TEST-9", "ACTIVE", ())
    assert _account_label(no_property) == "Meridian account · account ending ST-9"


@pytest.mark.asyncio
async def test_reconfigure_refreshes_and_saves_account_selection(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        data={
            CONF_REFRESH_TOKEN: "refresh",
            CONF_FIREBASE_USER_ID: "user",
            CONF_SELECTED_ACCOUNTS: ["first"],
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_get_accounts = AsyncMock(
        return_value=(_account("first"), _account("second"))
    )
    client.tokens = None
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {"source": SOURCE_RECONFIGURE, "entry_id": entry.entry_id}
    with (
        patch(
            "custom_components.meridian_energy.config_flow.MeridianApiClient",
            return_value=client,
        ) as client_class,
        patch.object(hass.config_entries, "async_reload", AsyncMock()),
    ):
        form = await flow.async_step_reconfigure()
        await client_class.call_args.kwargs["token_update_callback"](
            MeridianTokenSet(
                id_token="rotated-id",
                refresh_token="rotated-refresh",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
                user_id="rotated-user",
            )
        )
        invalid = await flow.async_step_reconfigure({CONF_SELECTED_ACCOUNTS: []})
        saved = await flow.async_step_reconfigure({CONF_SELECTED_ACCOUNTS: ["second"]})

    assert form["step_id"] == "reconfigure"
    assert invalid["errors"] == {"base": "select_account"}
    assert saved["type"] is FlowResultType.ABORT
    assert saved["reason"] == "reconfigure_successful"
    assert entry.data[CONF_SELECTED_ACCOUNTS] == ["second"]
    assert entry.data[CONF_AUTO_ADD_ACCOUNTS] is False
    assert entry.data[CONF_REFRESH_TOKEN] == "rotated-refresh"
    assert entry.data[CONF_FIREBASE_USER_ID] == "rotated-user"
    assert "id_token" not in entry.data


@pytest.mark.asyncio
async def test_reconfigure_persists_rotated_credentials_when_cancelled(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        data={
            CONF_REFRESH_TOKEN: "refresh",
            CONF_FIREBASE_USER_ID: "user",
            CONF_SELECTED_ACCOUNTS: ["first"],
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account("first"),))
    client.tokens = None
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {"source": SOURCE_RECONFIGURE, "entry_id": entry.entry_id}
    with (
        patch(
            "custom_components.meridian_energy.config_flow.MeridianApiClient",
            return_value=client,
        ) as client_class,
        patch.object(hass.config_entries, "async_reload", AsyncMock()) as reload_entry,
    ):
        result = await flow.async_step_reconfigure()
        await client_class.call_args.kwargs["token_update_callback"](
            MeridianTokenSet(
                id_token="rotated-id",
                refresh_token="rotated-refresh",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
                user_id="rotated-user",
            )
        )
        await hass.async_block_till_done()

    assert result["step_id"] == "reconfigure"
    assert entry.data[CONF_REFRESH_TOKEN] == "rotated-refresh"
    assert entry.data[CONF_FIREBASE_USER_ID] == "rotated-user"
    reload_entry.assert_not_awaited()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (MeridianRateLimitError(120), "rate_limited"),
        (MeridianConnectionError(), "cannot_connect"),
        *_UPSTREAM_ERRORS,
        (MeridianGraphQLError("accounts", ("UPSTREAM",)), "invalid_data"),
        (ValueError("bad shape"), "invalid_data"),
    ],
)
@pytest.mark.asyncio
async def test_reconfigure_discovery_errors(
    hass, error, expected: str, caplog: pytest.LogCaptureFixture
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        data={
            CONF_REFRESH_TOKEN: "refresh",
            CONF_FIREBASE_USER_ID: "user",
            CONF_SELECTED_ACCOUNTS: ["first"],
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=[error, (_account("first"),)])
    with patch(
        "custom_components.meridian_energy.config_flow.MeridianApiClient",
        return_value=client,
    ):
        result = await entry.start_reconfigure_flow(hass)
        retry = await hass.config_entries.flow.async_configure(result["flow_id"], {})

    assert result["step_id"] == "reconfigure_discovery"
    assert result["errors"] == {"base": expected}
    assert retry["step_id"] == "reconfigure"
    warnings = _flow_warnings(caplog)
    assert len(warnings) == (1 if expected == "invalid_data" else 0)


@pytest.mark.asyncio
async def test_reconfigure_with_cached_accounts_preserves_credentials(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        data={
            CONF_REFRESH_TOKEN: "refresh",
            CONF_FIREBASE_USER_ID: "user",
            CONF_SELECTED_ACCOUNTS: ["first"],
        },
    )
    entry.add_to_hass(hass)
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {"source": SOURCE_RECONFIGURE, "entry_id": entry.entry_id}
    flow._accounts = (_account("first"),)
    with patch.object(hass.config_entries, "async_reload", AsyncMock()):
        result = await flow.async_step_reconfigure({CONF_SELECTED_ACCOUNTS: ["first"]})

    assert result["reason"] == "reconfigure_successful"
    assert entry.data[CONF_REFRESH_TOKEN] == "refresh"
    assert entry.data[CONF_FIREBASE_USER_ID] == "user"


@pytest.mark.asyncio
async def test_reconfigure_auth_failure_routes_to_reauthentication(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        unique_id="person@example.com",
        data={
            CONF_EMAIL: "person@example.com",
            CONF_REFRESH_TOKEN: "refresh",
            CONF_FIREBASE_USER_ID: "user",
            CONF_SELECTED_ACCOUNTS: ["first"],
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_get_accounts = AsyncMock(side_effect=MeridianAuthenticationError())
    auth_client = MagicMock()
    auth_client.async_send_otp = AsyncMock()
    auth_client.async_validate_otp = AsyncMock(return_value=_tokens())
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {"source": SOURCE_RECONFIGURE, "entry_id": entry.entry_id}
    with (
        patch(
            "custom_components.meridian_energy.config_flow.MeridianApiClient",
            return_value=client,
        ),
        patch.object(hass.config_entries, "async_reload", AsyncMock()) as reload_entry,
    ):
        result = await flow.async_step_reconfigure()
        assert result["step_id"] == "reauth_confirm"
        assert flow.context["source"] == SOURCE_REAUTH
        with patch.object(
            MeridianEnergyConfigFlow,
            "_client",
            new_callable=PropertyMock,
            return_value=auth_client,
        ):
            result = await flow.async_step_reauth_confirm({})
            assert result["step_id"] == "otp"
            result = await flow.async_step_otp({"otp": "123456"})

    assert result["reason"] == "reauth_successful"
    assert entry.data[CONF_REFRESH_TOKEN] == "synthetic-refresh"
    reload_entry.assert_awaited_once_with(entry.entry_id)


@pytest.mark.asyncio
async def test_reconfigure_aborts_when_no_accounts_are_available(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        data={
            CONF_REFRESH_TOKEN: "refresh",
            CONF_FIREBASE_USER_ID: "user",
            CONF_SELECTED_ACCOUNTS: ["first"],
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=())
    client.tokens = None
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {"source": SOURCE_RECONFIGURE, "entry_id": entry.entry_id}
    with patch(
        "custom_components.meridian_energy.config_flow.MeridianApiClient",
        return_value=client,
    ):
        result = await flow.async_step_reconfigure()

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "no_accounts"


@pytest.mark.asyncio
async def test_reconfigure_uses_loaded_runtime_client(hass) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=NAME,
        data={
            CONF_REFRESH_TOKEN: "refresh",
            CONF_FIREBASE_USER_ID: "user",
            CONF_SELECTED_ACCOUNTS: ["first"],
        },
    )
    rotated = _tokens()
    runtime_client = MagicMock()
    runtime_client.async_get_accounts = AsyncMock(return_value=(_account("first"),))
    runtime_client.tokens = rotated
    entry.runtime_data = MagicMock(client=runtime_client)
    entry.add_to_hass(hass)
    flow = MeridianEnergyConfigFlow()
    flow.hass = hass
    flow.context = {"source": SOURCE_RECONFIGURE, "entry_id": entry.entry_id}
    with patch(
        "custom_components.meridian_energy.config_flow.MeridianApiClient"
    ) as temporary_client:
        result = await flow.async_step_reconfigure()

    assert result["step_id"] == "reconfigure"
    runtime_client.async_get_accounts.assert_awaited_once()
    temporary_client.assert_not_called()
    assert flow._reconfigure_tokens == rotated


@pytest.mark.asyncio
async def test_upstream_service_error_reaches_the_form(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    def unavailable(_direction: str, _before: str | None) -> BaseException:
        error = MeridianUpstreamError(503, gateway=False)
        error.__cause__ = MeridianHttpError(503, {})
        return error

    meridian.failure = unavailable
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "upstream_unavailable"}
    assert result["description_placeholders"]["detail"] == (
        "HTTP 503 while requesting grid import history, window 1 of 13, page 1 "
        "(account 1 of 1, property 1 of 1)"
    )


@pytest.mark.asyncio
async def test_setup_time_limit_ends_the_attempt_with_import_timeout(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    An import still running at the setup time limit is stopped and rolled back.

    The form says the limit ran out and which request was waiting, so a slow
    import is never mistaken for a gateway error.
    """
    meridian.blocked_direction = "CONSUMPTION"
    energy_id, cost_id = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    with patch.object(config_flow_module, "SETUP_IMPORT_TIMEOUT_SECONDS", 0.05):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "import_timeout"}
    assert result["description_placeholders"]["detail"] == (
        "the setup time limit ran out while requesting grid import history, "
        "window 1 of 13, page 1 (account 1 of 1, property 1 of 1)"
    )
    assert await _rows(hass, energy_id) == []
    assert await _rows(hass, cost_id) == []
    (record,) = _flow_warnings(caplog)
    message = record.getMessage()
    assert message.startswith("Initial Meridian history import failed")
    assert "reason=import_timeout" in message
    # The limit names itself, not the cancellation asyncio.timeout stopped
    # the task with, which would read like a closed dialog.
    assert "cause=TimeoutError root=SetupTimeLimitError status=-" in message
    assert "operation=measurements direction=CONSUMPTION" in message
    assert "in_flight=yes" in message


@pytest.mark.asyncio
async def test_too_little_time_left_for_the_next_request_ends_the_attempt(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    No request starts that the setup time limit could cut off.

    Each page here takes 200 s, and pages of 100 rows make the newest week
    two. Once less than 80 s of the import's 4 minutes would be left, the
    next request does not start: with no week of history imported, the
    attempt ends with the time-limit failure, and says which request came
    last.
    """

    def slow_windows(_direction: str, _before: str | None) -> None:
        clock.now += 200.0

    meridian.failure = slow_windows
    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(coordinator_module, "page_size", return_value=100),
    ):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["errors"] == {"base": "import_timeout"}
    assert result["description_placeholders"]["detail"] == (
        "too little of the import's 4 minutes was left for another request after "
        "requesting grid import history, window 1 of 13, page 1 (account 1 of 1, "
        "property 1 of 1)"
    )
    # The first page starts at 0 s; at 200 s only 40 s would remain.
    assert len(meridian.measurement_calls) == 1
    (record,) = _import_failure_records(caplog)
    assert "cause=RunBudgetError root=SetupTimeLimitError" in record.getMessage()
    assert "in_flight=no" in record.getMessage()


@pytest.mark.parametrize(("seconds", "requests"), [(8.0, 21), (3.0, 52)])
@pytest.mark.asyncio
async def test_four_directions_keep_what_their_request_times_allow(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    seconds: float,
    requests: int,
) -> None:
    """
    Two properties with grid export: setup keeps what 8 s a request allows.

    Each of the 4 directions asks for its 13 weeks, every request taking
    8 s, the newest week of each first. No request starts once less than
    80 s of the import's 4 minutes would be left: the 21st starts at 160 s,
    and setup creates the entry with the weeks that came back, for the
    entry's updates to complete. At 3 s a request the 52 fit, and the
    history is whole.
    """
    meridian.account = MeridianAccount(
        number=_TEST_ACCOUNT,
        status="ACTIVE",
        properties=tuple(
            MeridianProperty(
                id=f"synthetic-property-{index}",
                address=f"{index} Synthetic Street",
                meter_points=(
                    MeridianMeterPoint(
                        id=f"synthetic-meter-{index}",
                        market_identifier=f"synthetic-icp-{index}",
                        has_feed_in=True,
                    ),
                ),
            )
            for index in (1, 2)
        ),
    )

    def request_time(_direction: str, _before: str | None) -> None:
        clock.now += seconds

    meridian.failure = request_time
    started = clock.now
    with patch.object(coordinator_module, "monotonic", side_effect=clock):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["description"] == ("partial" if requests < 52 else None)
    assert len(meridian.measurement_calls) == requests
    # Every direction's newest week first.
    assert len(set(meridian.measurement_calls[:4])) == 2
    assert sorted({direction for direction, _ in meridian.measurement_calls}) == [
        "CONSUMPTION",
        "GENERATION",
    ]
    assert all(before is None for _, before in meridian.measurement_calls)
    assert clock.now - started == requests * seconds


@pytest.mark.parametrize("raised_by", ["raise", "asyncio_timeout"])
@pytest.mark.asyncio
async def test_a_timeout_that_is_not_the_setup_limit_is_unexpected(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    caplog: pytest.LogCaptureFixture,
    raised_by: str,
) -> None:
    """
    Another time limit is an unexpected failure, named by its own class.

    One that ``asyncio.timeout`` raised is chained from the cancellation it
    stopped the task with; that cancellation is how it worked, not what
    failed, so neither the form nor the log names it.
    """
    del meridian

    async def timed_out(*_args: object, **_kwargs: object) -> None:
        if raised_by == "raise":
            await asyncio.sleep(0)
            raise TimeoutError
        async with asyncio.timeout(0):
            await asyncio.sleep(1)

    with patch.object(MeridianEnergyConfigFlow, "_async_initial_import", timed_out):
        result = await _submit_login(hass)
        # hass.async_block_till_done skips a task while it is being cancelled.
        await _wait_for_progress_done(hass, result["flow_id"])
        result = await hass.config_entries.flow.async_configure(result["flow_id"])

    assert result["errors"] == {"base": "unknown"}
    assert result["description_placeholders"]["detail"] == "TimeoutError"
    (record,) = _import_failure_records(caplog)
    assert "cause=- root=TimeoutError status=-" in record.getMessage()


def _http_setup(
    clock: _Clock, seconds: Callable[[str, dict[str, Any]], float]
) -> tuple[FakeMeridianSession, AbstractContextManager[Any]]:
    """
    Run setup's real client and transport against a synthetic service.

    Each GraphQL answer moves both clocks on by ``seconds(operation,
    variables)``, so request times and the setup's duration are exact.
    """
    session = FakeMeridianSession(_NOW)

    def answer(operation: str, variables: dict[str, Any]) -> None:
        clock.now += seconds(operation, variables)

    session.hook = answer
    patches = ExitStack()
    for target, attribute, value in (
        (
            config_flow_module,
            "async_get_clientsession",
            MagicMock(return_value=session),
        ),
        (coordinator_module, "_utcnow", MagicMock(return_value=_NOW)),
        (transport_module, "monotonic", clock),
        (coordinator_module, "monotonic", clock),
        (config_flow_module, "monotonic", clock),
        (integration_module, "async_setup_entry", AsyncMock(return_value=True)),
    ):
        patches.enter_context(patch.object(target, attribute, value))
    return session, patches


@pytest.mark.asyncio
async def test_successful_setup_logs_one_info_line_with_its_request_times(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Real setup durations and HTTP request times, to tune the time limit from.

    The 13 weekly windows take 3, 4 and 5 s in turn; the import uses the
    accounts the flow found, so it sends no other request. The measurements
    median is the one the limit depends on.
    """
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
    clock = _Clock()
    pages = itertools.cycle([3.0, 4.0, 5.0])
    _session, patches = _http_setup(
        clock,
        lambda operation, _variables: (
            next(pages) if operation == "measurements" else 1.0
        ),
    )
    with patches:
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    (record,) = [
        record
        for record in caplog.records
        if record.name == config_flow_module.__name__ and record.levelno == logging.INFO
    ]
    assert record.getMessage() == (
        "Initial Meridian history import finished (attempt 1 of 5): "
        "duration=51.0s requests=13 directions=1 median_request=4.0s "
        "median_measurements=4.0s windows_ok=13 windows_empty=0 "
        "windows_failed=0 retries=0 splits=0 history_start=no units_failed=0 "
        "partial=no saved=yes"
    )


@pytest.mark.asyncio
async def test_failure_warning_gives_the_median_of_its_http_requests(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The failed requests count too: each ended, with an answer.

    The newest window's gateway timeout comes after 30 s, and that of its
    newest day alone, the probe after the cooldown, after 40 s, which stops
    the walk: smaller requests do not help.
    """
    clock = _Clock()
    measurements = {"sent": 0}

    def seconds(operation: str, _variables: dict[str, Any]) -> float:
        if operation != "measurements":
            return 1.0
        measurements["sent"] += 1
        return 30.0 if measurements["sent"] == 1 else 40.0

    session, patches = _http_setup(clock, seconds)
    answer = session.hook

    def time_out(operation: str, variables: dict[str, Any]) -> Response | None:
        answer(operation, variables)
        if operation == "measurements":
            return Response(b"", status=504, headers={})
        return None

    session.hook = time_out
    with patches:
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["errors"] == {"base": "upstream_timeout"}
    (record,) = _import_failure_records(caplog)
    assert record.getMessage().endswith(
        "window=1/13 page=1 bound=dates calls=2 in_flight=yes "
        "request_elapsed=40.0s total_elapsed=70.0s median_request=35.0s "
        "rollback=removed"
    )


@pytest.mark.asyncio
async def test_setup_skips_billing_and_the_first_sync_continues_the_import(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    Setup waits until the recorder has saved its import, so nothing repeats.

    Billing is no longer fetched during setup, and that was the flow's only
    wait on the recorder between the import and creating the entry. The
    recorder here is held for a moment while the import's rows are queued,
    and the entry's first sync is told the recorder has caught up at once,
    so only setup's own wait stands between them: without it, the first sync
    would read no history, choose a new 90-day import and send the request
    that failed, all over again.
    """
    hold = RecorderHold(hass)

    def hold_the_recorder(_direction: str, _before: str | None) -> None:
        if not meridian.measurement_calls[1:]:
            hold.hold_once()
            hold.release_after(0.5)

    meridian.failure = hold_the_recorder
    try:
        with (
            patch.object(
                coordinator_module,
                "async_wait_for_recorder",
                new=AsyncMock(return_value=True),
            ),
            patch.object(integration_module, "async_setup_entry", _REAL_SETUP_ENTRY),
            patch.object(
                integration_module, "MeridianApiClient", return_value=meridian
            ),
            patch.object(
                hass.config_entries, "async_forward_entry_setups", AsyncMock()
            ),
        ):
            result = await _submit_login(hass)
            result = await _finish_import(hass, result["flow_id"])
            assert result["type"] is FlowResultType.CREATE_ENTRY
            await hass.async_block_till_done(wait_background_tasks=True)
    finally:
        hold.release()

    assert meridian.billing_calls == 1  # the entry's first sync, not setup
    (entry,) = hass.config_entries.async_entries(DOMAIN)
    first_sync = entry.runtime_data.coordinator.data
    assert first_sync is not None
    # It continues setup's import with the hourly update: one request, and
    # no account list, which setup handed over.
    assert first_sync.sync_mode is SyncMode.TIP
    assert len(meridian.measurement_calls) == 13 + 1


def _running_sum_breaks(rows: list[dict[str, Any]]) -> list[float]:
    """Return the start of each row whose sum is not the last sum plus its state."""
    return [
        later["start"]
        for earlier, later in pairwise(rows)
        if later["sum"] - earlier["sum"] != pytest.approx(later["state"])
    ]


@pytest.mark.parametrize("retried", [False, True], ids=["saved", "retried"])
@pytest.mark.asyncio
async def test_first_sync_waits_for_a_recorder_still_saving_the_setup_import(
    hass: HomeAssistant, meridian: _FakeMeridian, retried: bool
) -> None:
    """
    The entry's first sync waits for the recorder, so it never imports twice.

    Setup waits a minute at most for the recorder after its import. With the
    recorder still behind after that (a backup, a migration, slow storage),
    the first sync read no history, started a second 90-day import with its
    sums from zero and queued it behind setup's unsaved rows. An hour
    boundary between the two left the stored running sum an hour's usage
    short, for good. The import's own replay now confirms its rows
    before it returns, so the recorder is held here once they are saved,
    as other work would hold it. On MySQL or MariaDB, setup's energy rows
    can also meet a lock error (a lock-wait timeout or a deadlock, say): the
    recorder then queues them again, and the replay waits behind a second
    barrier until they are saved.
    """
    hold = RecorderHold(hass)
    now = {"value": _NOW}
    first_sync_waiting = asyncio.Event()
    real_wait = coordinator_module.async_wait_for_recorder
    real_import = recorder_statistics.import_statistics
    energy_id, cost_id = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    energy_writes: list[int] = []

    async def first_sync_wait(
        hass_: HomeAssistant, *, attempts: int, passes: int
    ) -> bool:
        first_sync_waiting.set()
        return await real_wait(hass_, attempts=attempts, passes=passes)

    def retried_once(
        instance: Recorder, metadata: Any, statistics: Any, table: Any
    ) -> bool:
        if metadata["statistic_id"] == energy_id:
            energy_writes.append(len(energy_writes))
            if len(energy_writes) == 1:
                # What the recorder's retryable database job returns.
                return False
            if len(energy_writes) == 2:
                time.sleep(0.5)
        return real_import(instance, metadata, statistics, table)

    try:
        with (
            _hold_after_replays(hold),
            patch.object(
                coordinator_module, "_utcnow", side_effect=lambda: now["value"]
            ),
            (
                patch(
                    "homeassistant.components.recorder.tasks.statistics."
                    "import_statistics",
                    side_effect=retried_once,
                )
                if retried
                else nullcontext()
            ),
            # Setup gives up after two short waits; the first sync waits on, as
            # long as the recorder takes to save setup's rows once released.
            patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
            patch.object(coordinator_module, "FIRST_SYNC_RECORDER_WAITS", 100),
            patch.object(
                coordinator_module, "async_wait_for_recorder", first_sync_wait
            ),
            patch.object(integration_module, "async_setup_entry", _REAL_SETUP_ENTRY),
            patch.object(
                integration_module, "MeridianApiClient", return_value=meridian
            ),
            patch.object(
                hass.config_entries, "async_forward_entry_setups", AsyncMock()
            ),
        ):
            result = await _submit_login(hass)
            await asyncio.wait_for(_wait_for_progress_done(hass, result["flow_id"]), 10)
            # An import of several minutes often crosses an hour boundary,
            # and the hour that ended since is the first sync's to import.
            now["value"] = meridian.kraken.now = _NOW + timedelta(hours=1)
            result = await hass.config_entries.flow.async_configure(result["flow_id"])
            assert result["type"] is FlowResultType.CREATE_ENTRY
            (entry,) = hass.config_entries.async_entries(DOMAIN)
            await asyncio.wait_for(first_sync_waiting.wait(), 10)
            # Longer than one wait: the first sync keeps waiting, unread.
            await asyncio.sleep(0.5)
            # Setup's 13 windows, and nothing from the waiting first sync.
            assert len(meridian.measurement_calls) == 13
            assert entry.runtime_data.coordinator.data is None
            synced = asyncio.Event()
            stop_listening = entry.runtime_data.coordinator.async_add_listener(
                synced.set
            )
            # From its next wait on, one wait outlasts the recorder catching
            # up, so the first sync is not saved by waits that timed out
            # behind the retried rows.
            statistics_module.CLEAR_STATISTICS_TIMEOUT_SECONDS = 10
            await asyncio.sleep(0.5)
            hold.release()
            # The recorder first saves setup's rows, which takes a moment.
            await asyncio.wait_for(synced.wait(), 30)
            stop_listening()
    finally:
        hold.release()

    first_sync = entry.runtime_data.coordinator.data
    # Setup's import is continued with the hourly update, which reaches back
    # to the newest hour setup stored.
    assert first_sync.sync_mode is SyncMode.TIP
    for statistic_id in (energy_id, cost_id):
        rows = await _rows(hass, statistic_id)
        assert len(rows) == 90 * 24
        assert _running_sum_breaks(rows) == []
    # Setup's write, its retry, then the first sync's catch-up.
    assert len(energy_writes) == (3 if retried else 0)


@pytest.mark.asyncio
async def test_setup_import_fetches_no_billing(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert meridian.billing_calls == 0


@pytest.mark.asyncio
async def test_a_recorder_task_error_while_saving_does_not_hang_setup(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The wait for the recorder gives up, says so, and setup still finishes.

    A recorder task that fails never calls back. Without a time limit on the
    wait, the dialog would spin forever, the original symptom.
    """
    del meridian  # the fake Meridian the flow talks to
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
    real_clear = recorder_statistics.clear_statistics
    real_replay = coordinator_module.async_replay
    imported = {"done": False}

    async def replay(*args: Any, **kwargs: Any) -> tuple[int, int]:
        written = await real_replay(*args, **kwargs)
        imported["done"] = True
        return written

    def fail_once_imported(instance: Recorder, statistic_ids: list[str]) -> None:
        if imported["done"]:
            raise OperationalError("DELETE", {}, Exception("database is locked"))
        real_clear(instance, statistic_ids)

    with (
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
        patch.object(recorder_statistics, "clear_statistics", fail_once_imported),
        patch.object(coordinator_module, "async_replay", replay),
    ):
        result = await _submit_login(hass)
        await asyncio.wait_for(_wait_for_progress_done(hass, result["flow_id"]), 10)
        result = await hass.config_entries.flow.async_configure(result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert any(
        record.getMessage().startswith(
            "Home Assistant had not finished saving the imported Meridian history"
        )
        for record in caplog.records
        if record.name == statistics_module.__name__
    )
    assert any(
        record.getMessage().endswith("saved=no")
        for record in caplog.records
        if record.name == config_flow_module.__name__
    )


@pytest.mark.asyncio
async def test_closing_the_dialog_while_waiting_for_the_recorder_rolls_back(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The wait after the import is still part of the attempt it rolls back.

    The recorder is held once the import's rows are saved, as other work
    would hold it: the removal then waits behind it.
    """
    del meridian  # the fake Meridian the flow talks to
    hold = RecorderHold(hass)
    waiting = asyncio.Event()
    real_confirm = config_flow_module.async_confirm_visible

    async def confirm(hass_: HomeAssistant, newest: dict[str, datetime]) -> bool:
        waiting.set()
        return await real_confirm(hass_, newest)

    energy_id, cost_id = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    try:
        with (
            _hold_after_replays(hold),
            patch.object(config_flow_module, "async_confirm_visible", confirm),
        ):
            result = await _submit_login(hass)
            flow_id = result["flow_id"]
            await asyncio.wait_for(waiting.wait(), 10)
            await asyncio.sleep(0.05)
            task = hass.config_entries.flow._progress[flow_id].async_get_progress_task()
            assert task is not None
            assert not task.done()
            hass.config_entries.flow.async_abort(flow_id)
            hold.release()
            # hass.async_block_till_done skips tasks that are being cancelled.
            await asyncio.wait({task}, timeout=10)
    finally:
        hold.release()

    assert task.cancelled()
    assert await _rows(hass, energy_id) == []
    assert await _rows(hass, cost_id) == []
    (record,) = _flow_warnings(caplog)
    message = record.getMessage()
    # The import had finished: the warning says setup was waiting for Home
    # Assistant to save it, so it does not point at Meridian.
    assert message.startswith(
        "Meridian setup was cancelled while Home Assistant was saving the "
        "imported history, because the setup dialog was closed; the statistics "
        "it created were removed"
    )
    assert "in_flight=no" in message
    assert hass.config_entries.async_entries(DOMAIN) == []


@pytest.mark.asyncio
async def test_the_time_limit_running_out_while_saving_keeps_the_import(
    hass: HomeAssistant, meridian: _FakeMeridian, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The setup time limit stops an import, never one that has finished.

    When it runs out while setup waits for the recorder to save a finished
    import, it ends that wait as the wait's own timeout would: setup goes
    on, the recorder saves the rows later, and the entry's first sync waits
    for them. Treated as a failure, a busy recorder alone would remove a
    complete history, with a removal queued behind the same busy recorder.
    """
    del meridian  # the fake Meridian the flow talks to
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")
    # A limit no other time limit uses, so the test can find this one.
    limit_seconds = 4321.0
    real_timeout = asyncio.timeout
    limits: list[asyncio.Timeout] = []

    def recording_timeout(delay: float | None) -> asyncio.Timeout:
        limit = real_timeout(delay)
        if delay == limit_seconds:
            limits.append(limit)
        return limit

    hold = RecorderHold(hass)
    real_confirm = config_flow_module.async_confirm_visible

    async def confirm(hass_: HomeAssistant, newest: dict[str, datetime]) -> bool:
        # The import has returned, and the recorder is held behind its
        # saved rows: let the limit run out during the wait.
        (limit,) = limits
        limit.reschedule(asyncio.get_running_loop().time() + 0.05)
        return await real_confirm(hass_, newest)

    energy_id, cost_id = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    try:
        with (
            _hold_after_replays(hold),
            patch.object(
                config_flow_module, "SETUP_IMPORT_TIMEOUT_SECONDS", limit_seconds
            ),
            patch.object(asyncio, "timeout", recording_timeout),
            patch.object(config_flow_module, "async_confirm_visible", confirm),
            # Longer than the limit has left: only the limit ends the wait.
            patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 5),
        ):
            result = await _submit_login(hass)
            await asyncio.wait_for(_wait_for_progress_done(hass, result["flow_id"]), 10)
            result = await hass.config_entries.flow.async_configure(result["flow_id"])
    finally:
        hold.release()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert len(limits) == 1
    for statistic_id in (energy_id, cost_id):
        assert len(await _rows(hass, statistic_id)) == 90 * 24 - 1
    assert _flow_warnings(caplog) == []
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == config_flow_module.__name__
    ]
    assert (
        "The setup time limit ran out while Home Assistant was saving the "
        "imported Meridian history; setup continues, and the first update "
        "waits until it is saved"
    ) in messages
    (finished,) = [
        message
        for message in messages
        if message.startswith("Initial Meridian history import finished")
    ]
    assert finished.endswith("saved=no")


@pytest.mark.asyncio
async def test_a_replay_waiting_for_the_recorder_leaves_the_setup_time_limit_alone(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    The setup time limit counts the time spent on Meridian, not on the recorder.

    Each request here leaves the limit 1 s, as one sent near its end would,
    and the first direction's replay takes 1.5 s, as one confirmed only
    behind a busy recorder (a backup, say) can. The limit does not run out
    while the replay waits, and moves on by the time it took, so the other
    direction still fetches its older weeks, and setup imports both.
    Counted, the replay's time would have ended the attempt while it
    waited, with ``import_timeout``, and rolled back a history that only
    Home Assistant was slow to save.
    """
    meridian.account = _test_account(feed_in=True)
    # A limit no other time limit uses, so the test can find this one.
    limit_seconds = 4321.0
    real_timeout = asyncio.timeout
    limits: list[asyncio.Timeout] = []

    def recording_timeout(delay: float | None) -> asyncio.Timeout:
        limit = real_timeout(delay)
        if delay == limit_seconds:
            limits.append(limit)
        return limit

    def near_the_limit(_direction: str, _before: str | None) -> None:
        (limit,) = limits
        limit.reschedule(asyncio.get_running_loop().time() + 1.0)

    real_replay = coordinator_module.async_replay
    replayed: list[str] = []

    async def slow_replay(*args: Any, **kwargs: Any) -> tuple[int, int]:
        replayed.append(kwargs["stat_energy_id"])
        if len(replayed) == 1:
            await asyncio.sleep(1.5)
        return await real_replay(*args, **kwargs)

    meridian.failure = near_the_limit
    with (
        patch.object(config_flow_module, "SETUP_IMPORT_TIMEOUT_SECONDS", limit_seconds),
        patch.object(asyncio, "timeout", recording_timeout),
        patch.object(coordinator_module, "async_replay", slow_replay),
    ):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert len(limits) == 1
    assert len(replayed) == 2
    key = property_key(_TEST_ACCOUNT, _TEST_PROPERTY)
    for statistic_id in (*consumption_ids(key), *generation_ids(key)):
        assert len(await _rows(hass, statistic_id)) == 90 * 24 - 1, statistic_id


@pytest.mark.asyncio
async def test_no_answer_in_time_is_named_on_the_form(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """A request that timed out is described as such, not by an error class."""

    def no_answer(_direction: str, _before: str | None) -> BaseException:
        error = MeridianUpstreamError(None, gateway=True)
        error.__cause__ = TimeoutError()
        return error

    meridian.failure = no_answer
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["errors"] == {"base": "upstream_timeout"}
    assert result["description_placeholders"]["detail"] == (
        "no answer within the time limit while requesting grid import history, "
        "window 1 of 13, page 1 (account 1 of 1, property 1 of 1)"
    )


# ------------------------------------- partial setup and going on without history


def _schema_fields(result: ConfigFlowResult) -> list[str]:
    """Return the names of a form's fields."""
    schema = result["data_schema"]
    return [str(key) for key in schema.schema] if schema is not None else []


def _entry_setup(hass: HomeAssistant, meridian: _FakeMeridian) -> ExitStack:
    """Set the new entry up for real, against the fake, with its first sync."""
    patches = ExitStack()
    patches.enter_context(
        patch.object(
            coordinator_module,
            "async_wait_for_recorder",
            new=AsyncMock(return_value=True),
        )
    )
    patches.enter_context(
        patch.object(integration_module, "async_setup_entry", _REAL_SETUP_ENTRY)
    )
    patches.enter_context(
        patch.object(integration_module, "MeridianApiClient", return_value=meridian)
    )
    patches.enter_context(
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock())
    )
    return patches


@pytest.mark.asyncio
async def test_the_time_limit_leaves_setup_the_weeks_it_fetched(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Each week takes 100 s: setup imports two, newest first, and finishes.

    No request starts once less than 80 s of the import's 4 minutes would be
    left, so the third does not; what came back is imported, and the dialog
    says some history is still to come. Until now the import failed, and
    offered to import those days alone on a retry.
    """
    caplog.set_level(logging.INFO, logger="custom_components.meridian_energy")

    def slow_windows(_direction: str, _before: str | None) -> None:
        clock.now += 100.0

    meridian.failure = slow_windows
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    with patch.object(coordinator_module, "monotonic", side_effect=clock):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["description"] == "partial"
    assert len(meridian.measurement_calls) == 2
    rows = await _rows(hass, energy_id)
    first = datetime.fromtimestamp(rows[0]["start"], UTC)
    floor = _NOW.astimezone(NZ).date() - timedelta(days=13)
    assert first == datetime.combine(floor, datetime.min.time(), tzinfo=NZ)
    assert _running_sum_breaks(rows) == []
    (record,) = [
        record
        for record in caplog.records
        if record.getMessage().startswith("Initial Meridian history import finished")
    ]
    assert " partial=yes " in record.getMessage()
    handoff = hass.data[DOMAIN][SETUP_HANDOFF][_TEST_EMAIL]
    assert handoff.imported is True
    assert handoff.memory is not None
    strings = json.loads(
        (Path(config_flow_module.__file__).parent / "strings.json").read_text()
    )
    partial = strings["config"]["create_entry"]["partial"]
    assert "background" in partial
    assert "Current bill" in partial


@pytest.mark.asyncio
async def test_a_partial_setup_is_completed_by_the_entrys_updates(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    Setup keeps two weeks; the entry's first updates import the other 76 days.

    The first sync runs the reconciliation the import owes and asks for no
    older history, so the entities get their state without waiting for it.
    The next update asks for 6 windows of the older days, and the one after
    for the other 5. The history then reaches back 90 days with no hour
    missing and every running sum consistent.
    """

    def slow_setup(direction: str, _before: str | None) -> None:
        if meridian.calls(direction) <= 2:
            clock.now += 100.0

    meridian.failure = slow_setup
    energy_id, cost_id = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        _entry_setup(hass, meridian),
    ):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])
        assert result["type"] is FlowResultType.CREATE_ENTRY
        assert result["description"] == "partial"
        await hass.async_block_till_done(wait_background_tasks=True)
        (entry,) = hass.config_entries.async_entries(DOMAIN)
        coordinator = entry.runtime_data.coordinator
        first = coordinator.data
        await coordinator.async_refresh()
        second = coordinator.data
        await coordinator.async_refresh()
        third = coordinator.data

    assert first is not None
    assert second is not None
    assert third is not None
    assert (
        first.coverage_windows,
        second.coverage_windows,
        third.coverage_windows,
    ) == (0, 6, 5)
    rows = await _rows(hass, energy_id)
    since = (_NOW - timedelta(days=90)).timestamp()
    starts = [row["start"] for row in rows if row["start"] >= since]
    assert starts[0] < since + 3600
    assert all(later - earlier == 3600 for earlier, later in pairwise(starts))
    assert _running_sum_breaks(rows) == []
    assert _running_sum_breaks(await _rows(hass, cost_id)) == []


@pytest.mark.parametrize("seconds", [4.0, 8.0, 34.0])
@pytest.mark.asyncio
async def test_one_gateway_timeout_on_the_newest_week_at_slow_answers(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock, seconds: float
) -> None:
    """
    The newest week times out after 60 s; every request after it takes a while.

    At 4 s a request, the probe and the week's other days fit the setup's
    4 minutes after the cooldown, and setup finishes with part of the
    history. Slower, the newest week cannot finish in time: setup fails,
    and it fails with the gateway timeout that cost the time, so the form
    offers to go on without history.
    """
    failed: list[int] = []

    def failure(_direction: str, _before: str | None) -> BaseException | None:
        if not failed:
            failed.append(1)
            clock.now += 60.0
            return _gateway_timeout()
        clock.now += seconds
        return None

    async def wait(delay: float) -> None:
        clock.now += delay

    meridian.failure = failure
    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(coordinator_module, "_async_wait", side_effect=wait),
    ):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    if seconds < 5:
        assert result["type"] is FlowResultType.CREATE_ENTRY
        assert result["description"] == "partial"
        return
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "upstream_timeout"}
    assert _schema_fields(result) == ["continue_without_history"]


@pytest.mark.asyncio
async def test_a_gateway_timeout_that_cost_setup_its_time_names_its_own_cause(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    The newest week's request runs out of time; the answers after it are slow.

    Setup fails with that timeout, as above, and its warning names the error
    behind it, as for any request that ran out of time: the timeout is
    raised as it came, not from the time budget it left too short.
    """
    timed_out = await _client_error(_TimedOutRequest())
    failed: list[int] = []

    def failure(_direction: str, _before: str | None) -> BaseException | None:
        if not failed:
            failed.append(1)
            clock.now += 60.0
            return timed_out
        clock.now += 8.0
        return None

    async def wait(delay: float) -> None:
        clock.now += delay

    meridian.failure = failure
    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(coordinator_module, "_async_wait", side_effect=wait),
    ):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["errors"] == {"base": "upstream_timeout"}
    assert _schema_fields(result) == ["continue_without_history"]
    (record,) = _import_failure_records(caplog)
    assert "cause=MeridianUpstreamError root=TimeoutError" in record.getMessage()


@pytest.mark.parametrize("cause", ["older_week", "time_budget"])
@pytest.mark.asyncio
async def test_a_degraded_direction_does_not_hide_the_failure_that_blocked_setup(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    caplog: pytest.LogCaptureFixture,
    cause: str,
) -> None:
    """
    The form shows the failure of the direction that missed its newest week.

    Grid import, listed first, imports its newest week, then stops at its
    second with unusable data, or once 40 s requests have used up the
    import's 4 minutes: it is degraded. Grid export fails at its newest week
    with Meridian's service error, which is what kept setup from finishing.
    The form says so and offers to continue without history, and the
    warning names grid export's request, not grid import's.
    """
    meridian.account = _test_account(feed_in=True)

    def failure(direction: str, _before: str | None) -> BaseException | None:
        if direction == "GENERATION":
            clock.now += 1.0
            error = MeridianUpstreamError(503, gateway=False)
            error.__cause__ = MeridianHttpError(503, {})
            return error
        if cause == "time_budget":
            clock.now += 40.0
        elif meridian.calls(direction) > 1:
            return MeridianGraphQLError("measurements", ("UNKNOWN",))
        return None

    async def wait(delay: float) -> None:
        clock.now += delay

    meridian.failure = failure
    with (
        patch.object(coordinator_module, "monotonic", side_effect=clock),
        patch.object(coordinator_module, "_async_wait", side_effect=wait),
    ):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])

    assert result["step_id"] == "import_failed"
    assert result["errors"] == {"base": "upstream_unavailable"}
    assert _schema_fields(result) == ["continue_without_history"]
    assert result["description_placeholders"]["detail"].startswith(
        "HTTP 503 while requesting grid export history, window 1 of 13, page 1"
    )
    (record,) = _import_failure_records(caplog)
    message = record.getMessage()
    for field in (
        "reason=upstream_unavailable",
        "status=503",
        "direction=GENERATION",
        "window=1/13",
    ):
        assert field in message


@pytest.mark.asyncio
async def test_setup_can_go_on_without_history_after_meridian_failed(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    Meridian's service did not answer: setup may finish with no history.

    The form offers to go on without history. The choice waits as long as a
    retry would, and stays ticked meanwhile; then setup creates the entry
    without asking Meridian for anything more, and hands it the accounts
    alone. The entry's first sync imports the 90 days as a new one does.
    """
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    with _entry_setup(hass, meridian):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        result = await _finish_import(hass, flow_id)
        assert result["errors"] == {"base": "upstream_timeout"}
        assert _schema_fields(result) == ["continue_without_history"]
        assert _schema_default(result, "continue_without_history") is False

        result = await hass.config_entries.flow.async_configure(
            flow_id, {"continue_without_history": True}
        )
        # It says to wait before continuing, as the last failure's form does,
        # not before trying again, which was not chosen.
        assert result["errors"] == {"base": "continue_too_soon"}
        assert _schema_default(result, "continue_without_history") is True

        meridian.failure = lambda _direction, _before: None
        sent = len(meridian.measurement_calls)
        clock.now += 121
        with patch.object(
            config_flow_module.MeridianEnergyConfigFlow,
            "_start_initial_import",
            side_effect=AssertionError("no import attempt"),
        ):
            result = await hass.config_entries.flow.async_configure(
                flow_id, {"continue_without_history": True}
            )
        assert result["type"] is FlowResultType.CREATE_ENTRY
        assert result["description"] == "partial"
        assert len(meridian.measurement_calls) == sent
        assert await _rows(hass, energy_id) == []
        await hass.async_block_till_done(wait_background_tasks=True)

    (entry,) = hass.config_entries.async_entries(DOMAIN)
    coordinator = entry.runtime_data.coordinator
    assert coordinator.data is not None
    assert coordinator.data.sync_mode is SyncMode.INITIAL
    assert len(meridian.measurement_calls) == sent + 13
    rows = await _rows(hass, energy_id)
    assert rows
    assert _running_sum_breaks(rows) == []
    assert hass.data[DOMAIN][SETUP_HANDOFF] == {}


@pytest.mark.asyncio
async def test_going_on_without_history_reconciles_statistics_kept_from_before(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock
) -> None:
    """
    Statistics kept from an earlier installation: the first sync reconciles them.

    Setup imported nothing, so its first sync has no import of setup's to
    continue: it reconciles the last 14 days of the kept history, with 3
    requests, as after a restart, rather than continue with an hourly
    update that would ask for 1.
    """
    key = property_key(_TEST_ACCOUNT, _TEST_PROPERTY)
    energy_id, cost_id = consumption_ids(key)
    first = (_NOW - timedelta(days=100)).replace(minute=0)
    hours = int((_NOW - timedelta(days=1) - first) / timedelta(hours=1))
    for metadata, value in (
        (_energy_metadata(energy_id, "Meridian grid import"), 0.5),
        (_cost_metadata(cost_id, "Meridian grid import cost"), 0.15),
    ):
        async_add_external_statistics(
            hass,
            metadata,
            [
                {
                    "start": first + timedelta(hours=offset),
                    "state": value,
                    "sum": value * (offset + 1),
                }
                for offset in range(hours)
            ],
        )
    await async_wait_recording_done(hass)
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    with _entry_setup(hass, meridian):
        result = await _submit_login(hass)
        flow_id = result["flow_id"]
        result = await _finish_import(hass, flow_id)
        assert result["errors"] == {"base": "upstream_timeout"}
        meridian.failure = lambda _direction, _before: None
        sent = len(meridian.measurement_calls)
        clock.now += 121
        result = await hass.config_entries.flow.async_configure(
            flow_id, {"continue_without_history": True}
        )
        assert result["type"] is FlowResultType.CREATE_ENTRY
        await hass.async_block_till_done(wait_background_tasks=True)

    (entry,) = hass.config_entries.async_entries(DOMAIN)
    data = entry.runtime_data.coordinator.data
    assert data is not None
    assert data.sync_mode is SyncMode.RESTART
    assert data.coverage_windows == 0
    assert len(meridian.measurement_calls) - sent == 3
    assert _running_sum_breaks(await _rows(hass, energy_id)) == []


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        pytest.param(
            _gateway_timeout, ("upstream_timeout", True), id="gateway-timeout"
        ),
        pytest.param(
            lambda: MeridianUpstreamError(503, gateway=False),
            ("upstream_unavailable", True),
            id="service-error",
        ),
        pytest.param(
            lambda: MeridianGraphQLError("measurements", ("UNKNOWN",)),
            ("import_invalid_data", False),
            id="unusable-data",
        ),
        pytest.param(
            lambda: MeridianRateLimitError(120),
            ("import_rate_limited", False),
            id="rate-limited",
        ),
        pytest.param(
            lambda: MeridianConnectionError("synthetic"),
            ("import_cannot_connect", False),
            id="not-reached",
        ),
    ],
)
@pytest.mark.asyncio
async def test_going_on_without_history_is_offered_after_meridians_service_failed(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    clock: _Clock,
    failure: Callable[[], BaseException],
    expected: tuple[str, bool],
) -> None:
    """
    Only Meridian's service failing or not answering offers it.

    The entry's updates may get past that later. Unusable data, a rate
    limit or a connection that could not be made would most likely fail
    them the same way, with no form to say why: the form offers the retry
    alone.
    """
    error, offered = expected
    meridian.failure = lambda _direction, _before: failure()
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    result = await _finish_import(hass, flow_id)

    assert result["errors"] == {"base": error}
    assert _schema_fields(result) == (["continue_without_history"] if offered else [])
    if offered:
        return
    clock.now += 3601
    meridian.failure = lambda _direction, _before: None
    result = await hass.config_entries.flow.async_configure(flow_id, {})
    assert result["type"] is FlowResultType.SHOW_PROGRESS
    result = await _finish_import(hass, flow_id)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["description"] is None


@pytest.mark.parametrize("ended", ["cleanup-pending", "dialog-closed"])
@pytest.mark.asyncio
async def test_going_on_without_history_waits_for_the_failed_attempts_removal(
    hass: HomeAssistant, meridian: _FakeMeridian, clock: _Clock, ended: str
) -> None:
    """
    The entry is created only once the failed attempt's statistics are gone.

    While the recorder is still behind the form comes back, as before a
    retry; a flow removed while it waited ends without an entry.
    """
    meridian.failure = lambda _direction, _before: _gateway_timeout()
    result = await _submit_login(hass)
    flow_id = result["flow_id"]
    await _finish_import(hass, flow_id)
    clock.now += 121
    flow = hass.config_entries.flow._progress[flow_id]

    async def removal(*_args: object) -> bool:
        if ended == "dialog-closed":
            flow.async_remove()
            return True
        return False

    with patch.object(
        config_flow_module.MeridianEnergyConfigFlow,
        "_async_rollback_complete",
        side_effect=removal,
    ):
        result = await hass.config_entries.flow.async_configure(
            flow_id, {"continue_without_history": True}
        )

    if ended == "cleanup-pending":
        assert result["errors"] == {"base": "cleanup_pending"}
        assert _schema_default(result, "continue_without_history") is True
    else:
        assert result["type"] is FlowResultType.ABORT
        assert result["reason"] == "login_expired"
    assert hass.config_entries.async_entries(DOMAIN) == []


@pytest.mark.asyncio
async def test_a_property_without_readings_does_not_finish_setup_alone(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    A property whose 90 days are empty does not create the entry alone.

    The ended property listed first has no readings in its 90 days; one
    date of the active property, 20 days back, never answers in time. The
    active property keeps its 19 newer days, so setup creates the entry
    with those, and says some history is still to come; the empty property
    alone would have created it with no history at all.
    """
    ended = "synthetic-ended-property"
    meridian.account = MeridianAccount(
        number=_TEST_ACCOUNT,
        status="ACTIVE",
        properties=(
            MeridianProperty(
                id=ended,
                address="2 Synthetic Street",
                meter_points=(
                    MeridianMeterPoint("synthetic-meter-2", "synthetic-icp-2", False),
                ),
            ),
            _test_account().properties[0],
        ),
    )
    real = meridian.async_get_measurements
    bad_day = _NOW.astimezone(NZ).date() - timedelta(days=20)

    async def measurements(**kwargs: Any) -> MeasurementPage:
        if kwargs["property_id"] == ended:
            await asyncio.sleep(0)
            return MeasurementPage((), False, None)
        if kwargs["start_on"] <= bad_day <= kwargs["end_on"]:
            await asyncio.sleep(0)
            raise _gateway_timeout()
        return await real(**kwargs)

    meridian.async_get_measurements = measurements  # type: ignore[method-assign]
    result = await _submit_login(hass)
    result = await _finish_import(hass, result["flow_id"])

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["description"] == "partial"
    energy_id, _ = consumption_ids(property_key(_TEST_ACCOUNT, _TEST_PROPERTY))
    rows = await _rows(hass, energy_id)
    first = datetime.fromtimestamp(rows[0]["start"], UTC)
    assert first == datetime.combine(
        bad_day + timedelta(days=1), datetime.min.time(), tzinfo=NZ
    )


@pytest.mark.asyncio
async def test_the_entry_continues_setups_import_without_asking_again(
    hass: HomeAssistant, meridian: _FakeMeridian
) -> None:
    """
    The entry's setup asks Meridian nothing; its first sync is one request.

    Setup hands the entry the accounts it found and when its import ran, so
    the entry neither fetches the accounts a third time nor repeats a 14-day
    reconciliation of the history just imported.
    """
    accounts = AsyncMock(side_effect=meridian.async_get_accounts)
    meridian.async_get_accounts = accounts  # type: ignore[method-assign]
    with (
        patch.object(
            coordinator_module,
            "async_wait_for_recorder",
            new=AsyncMock(return_value=True),
        ),
        patch.object(integration_module, "async_setup_entry", _REAL_SETUP_ENTRY),
        patch.object(integration_module, "MeridianApiClient", return_value=meridian),
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])
        assert result["type"] is FlowResultType.CREATE_ENTRY
        await hass.async_block_till_done(wait_background_tasks=True)

    accounts.assert_awaited_once()  # the flow's discovery
    (entry,) = hass.config_entries.async_entries(DOMAIN)
    coordinator = entry.runtime_data.coordinator
    assert coordinator.data is not None
    assert coordinator.data.sync_mode is SyncMode.TIP
    assert len(meridian.measurement_calls) == 13 + 1
    # Read once: nothing is left for a later setup of the entry.
    assert hass.data[DOMAIN][SETUP_HANDOFF] == {}


@pytest.mark.asyncio
async def test_the_entry_does_not_search_again_what_setup_found_empty(
    hass: HomeAssistant,
    meridian: _FakeMeridian,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    A supply that ended 100 days ago: setup searches its 90 days, once.

    Setup finds no readings and creates the entry, which it tells that it
    searched that grid import in full. The entry's first sync is then the
    hourly update, one request, instead of the same 13 again, and the line
    saying no history was found is logged once.
    """
    caplog.set_level(logging.INFO, logger=coordinator_module.__name__)
    meridian.kraken.withdrawn_from = _NOW - timedelta(days=100)
    with (
        patch.object(
            coordinator_module,
            "async_wait_for_recorder",
            new=AsyncMock(return_value=True),
        ),
        patch.object(integration_module, "async_setup_entry", _REAL_SETUP_ENTRY),
        patch.object(integration_module, "MeridianApiClient", return_value=meridian),
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
    ):
        result = await _submit_login(hass)
        result = await _finish_import(hass, result["flow_id"])
        assert result["type"] is FlowResultType.CREATE_ENTRY
        assert len(meridian.measurement_calls) == 13
        await hass.async_block_till_done(wait_background_tasks=True)

    (entry,) = hass.config_entries.async_entries(DOMAIN)
    coordinator = entry.runtime_data.coordinator
    assert coordinator.last_update_success
    assert coordinator.data is not None
    assert coordinator.data.sync_mode is SyncMode.TIP
    assert len(meridian.measurement_calls) == 13 + 1
    (searched,) = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("No Meridian usage history was found")
    ]
    assert searched.endswith("in the last 90 days (direction=CONSUMPTION)")
