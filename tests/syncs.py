"""
Shared helpers for sync tests: a fixed clock, a real client, stored hours.

``NOW`` is 12:30 on 16 July 2026 in New Zealand (NZST): its 90 days back
hold no daylight-saving change. Every value is synthetic.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import patch

from homeassistant.core import HomeAssistant

from custom_components.meridian_energy import coordinator as coordinator_module
from custom_components.meridian_energy.api import MeridianApiClient
from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import MeridianSyncData, MeridianTokenSet
from custom_components.meridian_energy.windows import date_hours, nz_date

from .meridian_http import ID_TOKEN, REFRESH_TOKEN, FakeMeridianSession

HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
NOW = datetime(2026, 7, 16, 0, 30, tzinfo=UTC)
TODAY = nz_date(NOW)
# The target of every unit's history: New Zealand midnight 90 days back.
TARGET = TODAY - 90 * DAY


def real_client(session: FakeMeridianSession) -> MeridianApiClient:
    """Return the integration's own client, with a session that stays valid."""
    return MeridianApiClient(
        session,  # type: ignore[arg-type]
        tokens=MeridianTokenSet(
            id_token=ID_TOKEN,
            refresh_token=REFRESH_TOKEN,
            expires_at=datetime(2100, 1, 1, tzinfo=UTC),
            user_id="synthetic-user-0123",
        ),
    )


def coordinator_for(
    hass: HomeAssistant, session: FakeMeridianSession, **kwargs: Any
) -> MeridianDataCoordinator:
    """Return a coordinator in front of a synthetic Meridian service."""
    return MeridianDataCoordinator(hass, real_client(session), **kwargs)


def clock(at: datetime) -> Any:
    """Fix the coordinator's clock."""
    return patch.object(coordinator_module, "_utcnow", return_value=at)


async def sync(
    coordinator: MeridianDataCoordinator,
    at: datetime,
    session: FakeMeridianSession | None = None,
) -> MeridianSyncData:
    """Run one update at a time, the service's clock moved there too."""
    if session is not None:
        session.kraken.now = at
    with clock(at):
        return await coordinator._async_update_data()


async def refresh(
    coordinator: MeridianDataCoordinator,
    at: datetime,
    session: FakeMeridianSession | None = None,
) -> None:
    """Run one update through Home Assistant's own refresh, which logs failures."""
    if session is not None:
        session.kraken.now = at
    with clock(at):
        await coordinator.async_refresh()


def measurement_dates(session: FakeMeridianSession) -> list[tuple[date, date]]:
    """Return each hourly window asked for, as (startOn, endOn), first pages only."""
    return [
        (date.fromisoformat(item["startOn"]), date.fromisoformat(item["endOn"]))
        for item in session.kraken.requests
        if "before" not in item and "frequency" not in item
    ]


def hours_of(days: Iterable[date]) -> list[datetime]:
    """Return the UTC hours of some New Zealand dates."""
    return [hour for day in days for hour in date_hours(day)]


class Statistics:
    """
    A stand-in for the recorder's statistics: what was written, by hour.

    ``write`` and ``read`` replace ``async_write_hours`` and
    ``async_stored_hours`` (the ``recorder_stand_in`` fixture's mocks).
    """

    def __init__(self) -> None:
        self.hours: dict[str, dict[datetime, Decimal]] = defaultdict(dict)
        self.writes: list[dict[str, Mapping[datetime, Decimal]]] = []

    def attach(self, stand_in: Any) -> Statistics:
        """Answer the fixture's mocks from this store."""
        stand_in.write.side_effect = self.write
        stand_in.stored.side_effect = self.read
        return self

    async def write(self, _hass: Any, statistics: Iterable[Any]) -> bool:
        written: dict[str, Mapping[datetime, Decimal]] = {}
        for metadata, hours in statistics:
            written[metadata["statistic_id"]] = hours
            self.hours[metadata["statistic_id"]].update(hours)
        self.writes.append(written)
        return True

    async def read(
        self, _hass: Any, statistic_ids: set[str], start: datetime, end: datetime
    ) -> dict[str, frozenset[datetime]]:
        return {
            statistic_id: frozenset(
                hour for hour in self.hours[statistic_id] if start <= hour < end
            )
            for statistic_id in statistic_ids
        }

    def store(self, statistic_ids: Iterable[str], hours: Iterable[datetime]) -> None:
        """Store some hours for statistics, as an earlier sync would have."""
        hours = list(hours)
        for statistic_id in statistic_ids:
            self.hours[statistic_id].update(dict.fromkeys(hours, Decimal("0.5")))
