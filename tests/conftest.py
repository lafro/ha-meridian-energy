"""Shared test configuration."""

from __future__ import annotations

from collections.abc import Generator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, PropertyMock, patch

import pytest
from pytest_homeassistant_custom_component.syrupy import (
    HomeAssistantSnapshotExtension,
)
from syrupy.assertion import SnapshotAssertion

from custom_components.meridian_energy import api as api_module
from custom_components.meridian_energy import coordinator as coordinator_module

# ha_deprecations fails the run on any deprecation Home Assistant reports for
# this integration; see that module.
pytest_plugins = ["pytest_homeassistant_custom_component", "tests.ha_deprecations"]


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Enable custom integrations for every test."""


@pytest.fixture(autouse=True)
def recorder_stand_in(request: pytest.FixtureRequest) -> Generator[SimpleNamespace]:
    """
    Stand in for the statistics reads and writes where no recorder runs.

    Tests that use ``recorder_mock`` read and write for real. For the
    others, no hour is stored and every write is queued: ``stored`` and
    ``write`` are the mocks, so a test can give stored hours or check what
    was written.
    """
    if "recorder_mock" in request.fixturenames:
        yield SimpleNamespace(stored=None, write=None)
        return

    async def no_hours(
        _hass: Any, statistic_ids: set[str], *_span: Any
    ) -> dict[str, frozenset[Any]]:
        return {statistic_id: frozenset() for statistic_id in statistic_ids}

    with (
        patch.object(
            coordinator_module,
            "async_stored_hours",
            new=AsyncMock(side_effect=no_hours),
        ) as stored,
        patch.object(
            coordinator_module, "async_write_hours", new=AsyncMock(return_value=True)
        ) as write,
    ):
        yield SimpleNamespace(stored=stored, write=write)


@pytest.fixture(autouse=True)
def waits() -> Generator[AsyncMock]:
    """
    Make the waits between requests return at once.

    The coordinator paces requests at least 1 s apart, and the client waits
    2-5 s before a resend. The returned mock records each wait asked for,
    so a test can check them.
    """
    wait = AsyncMock()
    with (
        patch.object(coordinator_module, "_async_wait", new=wait),
        patch.object(api_module, "_async_wait", new=wait),
    ):
        yield wait


@pytest.fixture
def snapshot(snapshot: SnapshotAssertion) -> SnapshotAssertion:
    """
    Use Home Assistant's snapshot serializer.

    The harness and syrupy both register a ``snapshot`` fixture as plugins, and
    plugin load order differs between machines, so pin the choice here.
    """
    return snapshot.use_extension(HomeAssistantSnapshotExtension)


@pytest.fixture
def entity_registry_enabled_by_default() -> Generator[None]:
    """Enable entities that are disabled by default, as Home Assistant's tests do."""
    with patch(
        "homeassistant.helpers.entity.Entity.entity_registry_enabled_default",
        new_callable=PropertyMock,
        return_value=True,
    ):
        yield
