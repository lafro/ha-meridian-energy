"""Statistics behaviour against a real Home Assistant recorder (no mocks).

The rest of the statistics tests patch the recorder. These tests use the
``recorder_mock`` fixture, which runs the real recorder thread on an in-memory
SQLite database, so recorder thread-safety rules are enforced.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
import sqlalchemy.exc
from homeassistant.components.recorder import Recorder, get_instance
from homeassistant.components.recorder import statistics as recorder_statistics
from homeassistant.components.recorder.db_schema import StatisticsBase
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    clear_statistics,
    get_metadata,
)
from homeassistant.components.recorder.tasks import RecorderTask
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.meridian_energy import statistics as statistics_module
from custom_components.meridian_energy.models import MeridianMeasurement
from custom_components.meridian_energy.statistics import (
    _cost_metadata,
    _energy_metadata,
    async_clear_statistics,
    async_confirm_visible,
    async_has_statistics,
    async_replay,
    async_stored_hours,
    async_wait_for_recorder,
    consumption_ids,
    property_key,
)

from .recorder_hold import RecorderHold

_START = datetime(2026, 8, 1, tzinfo=UTC)


@pytest.fixture
def mock_recorder_before_hass(recorder_db_url: str) -> None:
    """Prepare Recorder storage before the Home Assistant fixture starts."""
    del recorder_db_url


def _measurements(hours: int) -> list[MeridianMeasurement]:
    return [
        MeridianMeasurement(
            start=_START + timedelta(hours=offset),
            end=_START + timedelta(hours=offset + 1),
            value_kwh=Decimal("1.25"),
            quality="ACTUAL",
            direction="CONSUMPTION",
            channel_id="synthetic-meter:synthetic-register",
            cost_cents=Decimal(40),
        )
        for offset in range(hours)
    ]


async def _metadata_ids(hass: HomeAssistant, statistic_ids: set[str]) -> set[str]:
    metadata = await get_instance(hass).async_add_executor_job(
        lambda: get_metadata(hass, statistic_ids=statistic_ids)
    )
    return set(metadata)


@pytest.mark.asyncio
async def test_clear_statistics_removes_only_requested_ids(recorder_mock, hass) -> None:
    """Clearing runs on the recorder thread and completes before returning."""
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-1", "P-TEST-1"))
    unrelated_id = "meridian_energy:consumption_unrelated000"
    await async_replay(
        hass,
        stat_energy_id=energy_id,
        stat_cost_id=cost_id,
        energy_name="Meridian grid import",
        cost_name="Meridian grid import cost",
        measurements=_measurements(3),
    )
    async_add_external_statistics(
        hass,
        _energy_metadata(unrelated_id, "Unrelated"),
        [{"start": _START, "state": 1.0, "sum": 1.0}],
    )
    await async_wait_recording_done(hass)
    assert await _metadata_ids(hass, {energy_id, cost_id, unrelated_id}) == {
        energy_id,
        cost_id,
        unrelated_id,
    }

    assert await async_clear_statistics(hass, {cost_id, energy_id}) is True

    # No extra wait: the helper must only return once the recorder has cleared.
    assert not await async_has_statistics(hass, energy_id)
    assert not await async_has_statistics(hass, cost_id)
    assert await _metadata_ids(hass, {energy_id, cost_id, unrelated_id}) == {
        unrelated_id
    }
    assert await async_has_statistics(hass, unrelated_id)


@pytest.mark.asyncio
async def test_clear_statistics_with_no_ids_leaves_history(recorder_mock, hass) -> None:
    """An empty rollback set is a no-op."""
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-2", "P-TEST-2"))
    await async_replay(
        hass,
        stat_energy_id=energy_id,
        stat_cost_id=cost_id,
        energy_name="Meridian grid import",
        cost_name="Meridian grid import cost",
        measurements=_measurements(2),
    )
    await async_wait_recording_done(hass)

    await async_clear_statistics(hass, set())

    assert await async_has_statistics(hass, energy_id)
    assert await async_has_statistics(hass, cost_id)


@pytest.mark.asyncio
async def test_wait_for_recorder_returns_once_queued_rows_are_committed(
    recorder_mock, hass
) -> None:
    """The setup's wait for earlier recorder work, on the real recorder thread."""
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-3", "P-TEST-3"))
    _queue_rows(hass, energy_id, cost_id, 2)

    assert await async_wait_for_recorder(hass) is True
    # No other wait: the rows the import queued are committed by now.
    assert await async_has_statistics(hass, energy_id)
    assert await async_has_statistics(hass, cost_id)


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


@pytest.mark.asyncio
async def test_wait_for_recorder_waits_for_a_removal_already_running(
    recorder_mock, hass
) -> None:
    """
    A removal the recorder thread has taken off its queue is still waited for.

    With the queue empty and the removal running, the recorder's commit
    future is None, so a wait on it returned before the rows were deleted.
    """
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-4", "P-TEST-4"))
    await async_replay(
        hass,
        stat_energy_id=energy_id,
        stat_cost_id=cost_id,
        energy_name="Meridian grid import",
        cost_name="Meridian grid import cost",
        measurements=_measurements(2),
    )
    await async_wait_recording_done(hass)
    removal = _SlowRemoval([energy_id, cost_id])
    get_instance(hass).queue_task(removal)
    for _ in range(1000):
        if removal.started.is_set():
            break
        await asyncio.sleep(0.01)
    assert removal.started.is_set()

    try:
        waiting = hass.async_create_task(async_wait_for_recorder(hass))
        await asyncio.sleep(0.1)
        assert not waiting.done()
    finally:
        removal.release.set()

    assert await waiting is True
    assert not await async_has_statistics(hass, energy_id)
    assert not await async_has_statistics(hass, cost_id)


@pytest.mark.asyncio
async def test_wait_for_recorder_tries_again_after_a_recorder_task_error(
    recorder_mock, hass
) -> None:
    """
    A wait whose recorder task failed is tried again, behind a new task.

    The recorder logs a failed task and moves on without calling on_done, so
    that wait can only time out; the next one can succeed.
    """
    del recorder_mock
    real_clear = clear_statistics
    failures = {"remaining": 1}

    def fail_once(instance: Recorder, statistic_ids: list[str]) -> None:
        if failures["remaining"]:
            failures["remaining"] -= 1
            raise sqlalchemy.exc.OperationalError(
                "DELETE", {}, Exception("database is locked")
            )
        real_clear(instance, statistic_ids)

    with (
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
        patch(
            "homeassistant.components.recorder.tasks.statistics.clear_statistics",
            side_effect=fail_once,
        ),
    ):
        assert await async_wait_for_recorder(hass) is False
        failures["remaining"] = 1
        assert await async_wait_for_recorder(hass, attempts=2) is True


@pytest.mark.asyncio
async def test_a_second_pass_waits_for_an_import_the_recorder_queued_again(
    recorder_mock, hass
) -> None:
    """
    A wait in two passes also covers an import the recorder had to retry.

    On MySQL or MariaDB, an import that meets a lock error (a lock-wait
    timeout or a deadlock, say) queues itself again at the back of the
    recorder's queue, behind a wait queued before it ran. That wait then
    returns with the rows still unsaved. A second pass, queued once the
    first has run, is behind the retry. The retry here takes a moment, so a
    wait that returned early would find the rows missing.
    """
    del recorder_mock
    key = property_key("A-TEST-11", "P-TEST-11")
    energy_id, _cost_id = consumption_ids(key)
    real_import = recorder_statistics.import_statistics
    calls: list[str] = []

    def retried_once(
        instance: Recorder,
        metadata: StatisticMetaData,
        statistics: Iterable[StatisticData],
        table: type[StatisticsBase],
    ) -> bool:
        if metadata["statistic_id"] == energy_id:
            calls.append("energy")
            if len(calls) == 1:
                # What the recorder's retryable database job returns.
                return False
            time.sleep(0.5)
        return real_import(instance, metadata, statistics, table)

    hold = RecorderHold(hass)
    hold.hold_once()
    with patch(
        "homeassistant.components.recorder.tasks.statistics.import_statistics",
        side_effect=retried_once,
    ):
        try:
            await _import_hours(hass, key, 3)
            waiting = hass.async_create_task(
                async_wait_for_recorder(hass, attempts=10, passes=2)
            )
            await asyncio.sleep(0.1)
            hold.release()
            assert await waiting is True
            # No other wait: the retried rows are saved by now.
            assert await async_has_statistics(hass, energy_id)
        finally:
            hold.release()
        await async_wait_recording_done(hass)

    assert calls == ["energy", "energy"]


def _confirm_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == statistics_module.__name__ and record.levelno == logging.INFO
    ]


async def _import_hours(hass: HomeAssistant, key: str, hours: int) -> str:
    """
    Queue a first import's rows on the recorder thread, without confirming them.

    As releases before 0.3.0 imported, and as any writer may leave rows
    queued: the waits tested here must cover rows still in the queue.
    """
    energy_id, cost_id = consumption_ids(key)
    _queue_rows(hass, energy_id, cost_id, hours)
    return energy_id


def _queue_rows(hass: HomeAssistant, energy_id: str, cost_id: str, hours: int) -> None:
    """Queue ``hours`` rows of energy and cost from ``_START``, sums from zero."""
    for metadata, state in (
        (_energy_metadata(energy_id, "Meridian grid import"), 1.25),
        (_cost_metadata(cost_id, "Meridian grid import cost"), 0.4),
    ):
        async_add_external_statistics(
            hass,
            metadata,
            [
                {
                    "start": _START + timedelta(hours=offset),
                    "state": state,
                    "sum": state * (offset + 1),
                }
                for offset in range(hours)
            ],
        )


@pytest.mark.asyncio
async def test_stored_hours_are_the_hours_with_a_numeric_sum(
    recorder_mock, hass
) -> None:
    """
    The stored hours of each statistic are those with a running sum, in the span.

    A row without a sum is no stored hour: an import would not anchor on it,
    so it is no hour a missing reading could strand. Hours before the span's
    start, or from its end on, and a statistic with no rows, give nothing.
    """
    del recorder_mock
    energy_id, cost_id = consumption_ids(property_key("A-TEST-1", "P-TEST-1"))
    empty_id = "meridian_energy:consumption_empty0000000"
    hours = [_START + timedelta(hours=offset) for offset in range(6)]
    async_add_external_statistics(
        hass,
        _energy_metadata(energy_id, "Meridian grid import"),
        [
            {"start": hour, "state": 1.0}
            if offset == 2
            else {"start": hour, "state": 1.0, "sum": float(offset)}
            for offset, hour in enumerate(hours)
        ],
    )
    async_add_external_statistics(
        hass,
        _energy_metadata(cost_id, "Meridian grid import cost"),
        [
            {"start": hours[offset], "state": 0.4, "sum": 0.4 * offset}
            for offset in (0, 3, 4)
        ],
    )
    await async_wait_recording_done(hass)

    stored = await async_stored_hours(
        hass, {energy_id, cost_id, empty_id}, hours[1], hours[4]
    )

    assert stored == {
        energy_id: frozenset({hours[1], hours[3]}),
        cost_id: frozenset({hours[3]}),
        empty_id: frozenset(),
    }


@pytest.mark.asyncio
async def test_confirm_visible_returns_once_the_import_can_be_read(
    recorder_mock, hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Rows queued just before the barrier are readable once it returns.

    The import's writes are not awaited, and the entry's first sync reads
    statistics from another thread: without the barrier it could find no
    history and repeat the whole import. The recorder is held, as a backlog
    would hold it, until a moment after the import, so the rows can only be
    readable by the time the barrier returns.
    """
    del recorder_mock
    hold = RecorderHold(hass)
    hold.hold_once()
    try:
        energy_id = await _import_hours(hass, property_key("A-TEST-5", "P-TEST-5"), 3)
        # Still held: nothing the import queued can be read yet.
        assert not await async_has_statistics(hass, energy_id)
        timer = hold.release_after(0.3)
        newest = _START + timedelta(hours=2)
        assert await async_confirm_visible(hass, {energy_id: newest}) is True
        timer.join()
    finally:
        hold.release()

    # No other wait: the rows the import queued are readable by now.
    assert await async_has_statistics(hass, energy_id)
    assert not _confirm_records(caplog)


@pytest.mark.asyncio
async def test_confirm_visible_with_nothing_imported_does_not_wait(
    recorder_mock, hass
) -> None:
    del recorder_mock
    with patch.object(statistics_module, "async_wait_for_recorder") as wait:
        assert await async_confirm_visible(hass, {}) is True
    wait.assert_not_called()


@pytest.mark.asyncio
async def test_confirm_visible_waits_once_more_when_the_newest_row_is_missing(
    recorder_mock, hass, caplog: pytest.LogCaptureFixture
) -> None:
    """A row that is still not readable gets one more barrier, then an INFO."""
    del recorder_mock
    caplog.set_level(logging.INFO, logger=statistics_module.__name__)
    energy_id = await _import_hours(hass, property_key("A-TEST-6", "P-TEST-6"), 3)
    real_wait = statistics_module.async_wait_for_recorder
    waits = AsyncMock(side_effect=real_wait)

    with patch.object(statistics_module, "async_wait_for_recorder", waits):
        # An hour the import never wrote cannot become readable.
        visible = await async_confirm_visible(
            hass, {energy_id: _START + timedelta(hours=5)}
        )

    assert visible is False
    assert waits.await_count == 2
    (record,) = _confirm_records(caplog)
    assert record.getMessage() == (
        "The imported Meridian history could not be read back after saving it; "
        "setup continues, and the first update may import it again"
    )


@pytest.mark.asyncio
async def test_confirm_visible_accepts_a_row_readable_after_the_second_barrier(
    recorder_mock, hass, caplog: pytest.LogCaptureFixture
) -> None:
    del recorder_mock
    caplog.set_level(logging.INFO, logger=statistics_module.__name__)
    energy_id = await _import_hours(hass, property_key("A-TEST-7", "P-TEST-7"), 1)
    checks = AsyncMock(side_effect=[False, True])

    with patch.object(statistics_module, "_async_newest_rows_visible", checks):
        assert await async_confirm_visible(hass, {energy_id: _START}) is True

    assert checks.await_count == 2
    assert not _confirm_records(caplog)


@pytest.mark.asyncio
async def test_confirm_visible_waits_once_more_after_a_barrier_timed_out(
    recorder_mock, hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A first barrier that timed out gets the second one, as a mismatch does.

    The recorder may catch up a moment later, and the import it saves then
    may have queued itself again behind that first barrier.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=statistics_module.__name__)
    energy_id = await _import_hours(hass, property_key("A-TEST-12", "P-TEST-12"), 1)
    real_wait = statistics_module.async_wait_for_recorder

    async def first_times_out(hass_: HomeAssistant) -> bool:
        return waits.await_count > 1 and await real_wait(hass_)

    waits = AsyncMock(side_effect=first_times_out)
    with patch.object(statistics_module, "async_wait_for_recorder", waits):
        assert await async_confirm_visible(hass, {energy_id: _START}) is True

    assert waits.await_count == 2
    assert not _confirm_records(caplog)


@pytest.mark.asyncio
async def test_confirm_visible_gives_up_after_a_recorder_task_error(
    recorder_mock, hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A barrier whose recorder task fails never calls back: wait 30 s, not forever.

    The recorder logs the task's error and moves on without calling on_done,
    so a barrier with no time limit left the setup dialog spinning.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=statistics_module.__name__)
    energy_id = await _import_hours(hass, property_key("A-TEST-8", "P-TEST-8"), 1)
    await async_wait_recording_done(hass)
    locked = sqlalchemy.exc.OperationalError(
        "DELETE", {}, Exception("database is locked")
    )

    with (
        patch.object(statistics_module, "CLEAR_STATISTICS_TIMEOUT_SECONDS", 0.2),
        patch(
            "homeassistant.components.recorder.tasks.statistics.clear_statistics",
            side_effect=locked,
        ),
    ):
        visible = await asyncio.wait_for(
            async_confirm_visible(hass, {energy_id: _START}), 10
        )

    assert visible is False
    (record,) = _confirm_records(caplog)
    assert record.getMessage() == (
        "Home Assistant had not finished saving the imported Meridian history "
        "after 0 s; setup continues, and the first update waits until it is saved"
    )


@pytest.mark.asyncio
async def test_confirm_visible_says_how_long_it_waited_in_all(
    recorder_mock, hass, caplog: pytest.LogCaptureFixture
) -> None:
    """
    The message gives the whole wait, the second barrier's included.

    A read-back that found nothing on the first pass waits once more, so
    setup can wait twice 30 s, not 30 s, before it gives up.
    """
    del recorder_mock
    caplog.set_level(logging.INFO, logger=statistics_module.__name__)
    energy_id = await _import_hours(hass, property_key("A-TEST-10", "P-TEST-10"), 1)
    clock = {"now": 500.0}

    async def thirty_seconds(_hass: HomeAssistant) -> bool:
        clock["now"] += 30.0
        return waits.await_count == 1

    waits = AsyncMock(side_effect=thirty_seconds)
    with (
        patch.object(statistics_module, "monotonic", side_effect=lambda: clock["now"]),
        patch.object(statistics_module, "async_wait_for_recorder", waits),
        patch.object(
            statistics_module,
            "_async_newest_rows_visible",
            new=AsyncMock(return_value=False),
        ),
    ):
        assert await async_confirm_visible(hass, {energy_id: _START}) is False

    assert waits.await_count == 2
    (record,) = _confirm_records(caplog)
    assert record.getMessage() == (
        "Home Assistant had not finished saving the imported Meridian history "
        "after 60 s; setup continues, and the first update waits until it is saved"
    )


@pytest.mark.asyncio
async def test_confirm_visible_never_raises_when_the_read_back_fails(
    recorder_mock, hass, caplog: pytest.LogCaptureFixture
) -> None:
    del recorder_mock
    caplog.set_level(logging.INFO, logger=statistics_module.__name__)
    energy_id = await _import_hours(hass, property_key("A-TEST-9", "P-TEST-9"), 1)

    with patch.object(
        statistics_module,
        "get_last_statistics",
        side_effect=sqlalchemy.exc.OperationalError("SELECT", {}, Exception("x")),
    ):
        assert await async_confirm_visible(hass, {energy_id: _START}) is False

    (record,) = _confirm_records(caplog)
    assert record.getMessage() == (
        "Could not read back the imported Meridian history (OperationalError); "
        "setup continues, and the first update may import it again"
    )
