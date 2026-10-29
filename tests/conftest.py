"""Shared test configuration."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import AsyncMock, PropertyMock, patch

import pytest
from pytest_homeassistant_custom_component.syrupy import (
    HomeAssistantSnapshotExtension,
)
from syrupy.assertion import SnapshotAssertion

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy import lanes as lanes_module

# ha_deprecations fails the run on any deprecation Home Assistant reports for
# this integration; see that module.
pytest_plugins = ["pytest_homeassistant_custom_component", "tests.ha_deprecations"]


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Enable custom integrations for every test."""


@pytest.fixture(autouse=True)
def recorder_wait_without_recorder(request: pytest.FixtureRequest) -> Generator[None]:
    """
    Stand in for the first sync's recorder wait where no recorder runs.

    An entry's first sync waits for the recorder before it reads statistics,
    and a direction with statistics reads its stored hours.
    Tests that use ``recorder_mock`` run that wait for real; the others
    patch the statistics they read, and have no recorder to wait for. The
    module is patched as imported here: patched by name, it could not be
    found in a run of test files that never import it themselves. Nor do
    the others fill in older history after their walks (the lanes'
    coverage): it compares what Meridian returns with the stored rows, and
    the few stored hours such a test gives its walks are no history to
    complete.
    """
    if "recorder_mock" in request.fixturenames:
        yield
        return
    with (
        patch.object(
            coordinator_module,
            "async_wait_for_recorder",
            new=AsyncMock(return_value=True),
        ),
        # The stored hours a direction with statistics reads once per sync.
        patch.object(
            coordinator_module, "async_stored_hours", new=AsyncMock(return_value={})
        ),
        patch.object(
            coordinator_module.MeridianDataCoordinator,
            "_async_cover_lanes",
            new=AsyncMock(),
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def no_revision_pass(request: pytest.FixtureRequest) -> Generator[None]:
    """
    Keep each lane's monthly revision pass out of tests not about it.

    A lane starts that pass on one day of the month, spread by its key, so
    a test whose dates fall on that day for its synthetic account would see
    its requests too. A test that asks for the ``revision_pass`` fixture
    keeps the real day.
    """
    if "revision_pass" in request.fixturenames:
        yield
        return
    with patch.object(lanes_module, "revision_day", return_value=0):
        yield


@pytest.fixture
def revision_pass() -> None:
    """Let lanes start their monthly revision pass on their own day."""


@pytest.fixture(autouse=True)
def waits() -> Generator[AsyncMock]:
    """
    Make the coordinator's waits between requests return at once.

    Requests are paced at least 1 s apart, a resend waits 2-5 s and a gateway
    timeout's cooldown 75 s. The returned mock records each wait asked for,
    so a test can check them.
    """
    with patch.object(coordinator_module, "_async_wait", new=AsyncMock()) as wait:
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
