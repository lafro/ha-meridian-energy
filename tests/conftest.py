"""Shared test configuration."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import PropertyMock, patch

import pytest
from pytest_homeassistant_custom_component.syrupy import (
    HomeAssistantSnapshotExtension,
)
from syrupy.assertion import SnapshotAssertion

pytest_plugins = "pytest_homeassistant_custom_component"


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Enable custom integrations for every test."""


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
