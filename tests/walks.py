"""
Walk one direction's windows with the coordinator's own walk, for tests.

A sync plans each unit's walk (``_async_plan_unit``), walks every unit's
newest window first, then the rest, and records a failure as its unit's.
The tests that use this helper check the walk itself, its windows, pages,
checks and failures, so it plans one walk as a sync does (``_unit_walk``),
walks it to its end and raises the failure that stopped it.
"""

from __future__ import annotations

from datetime import datetime

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import MeasurementFetchResult, SyncMode


async def fetch_since(
    coordinator: MeridianDataCoordinator,
    *,
    account_number: str,
    property_id: str,
    direction: str,
    since: datetime,
    mode: SyncMode | None = None,
    position: coordinator_module._Position | None = None,
    stored_energy: frozenset[datetime] = frozenset(),
    has_statistics: bool = False,
) -> MeasurementFetchResult:
    """Fetch one direction's windows, newest first, back to ``since``."""
    walk = coordinator._unit_walk(
        account_number=account_number,
        property_id=property_id,
        direction=direction,
        since=since,
        mode=mode,
        position=position,
        stored_energy=stored_energy,
        has_statistics=has_statistics,
        now=coordinator_module._utcnow(),
    )
    await coordinator._async_walk_unit(walk)
    result = coordinator._walk_result(walk)
    if walk.error is not None:
        raise walk.error
    return result
