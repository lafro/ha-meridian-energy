"""Data coordinator for Meridian Energy statistics."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from homeassistant.config_entries import ConfigEntry, ConfigEntryAuthFailed
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianError,
)
from .const import (
    INITIAL_BACKFILL,
    NAME,
    READING_CONSUMPTION,
    READING_GENERATION,
    REVISION_OVERLAP,
    UPDATE_INTERVAL,
)
from .models import (
    MeridianMeasurement,
    MeridianProperty,
    MeridianSyncData,
    PropertySyncResult,
)
from .statistics import (
    async_has_statistics,
    async_import_measurements,
    consumption_ids,
    generation_ids,
    property_key,
)

_LOGGER = logging.getLogger(__name__)
_NZ = ZoneInfo("Pacific/Auckland")


class MeridianDataCoordinator(DataUpdateCoordinator[MeridianSyncData]):
    """Fetch Meridian data and import it into the recorder."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: MeridianApiClient,
        *,
        config_entry: ConfigEntry | None = None,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=NAME,
            update_interval=UPDATE_INTERVAL,
        )
        self.client = client

    async def _async_update_data(self) -> MeridianSyncData:
        try:
            accounts = await self.client.async_get_accounts()
            results: list[PropertySyncResult] = []
            for account in accounts:
                for property_data in account.properties:
                    results.append(
                        await self._async_sync_property(
                            account.number,
                            property_data,
                        )
                    )
            return MeridianSyncData(
                account_count=len(accounts),
                property_count=sum(len(account.properties) for account in accounts),
                results=tuple(results),
                synced_at=datetime.now(UTC),
            )
        except MeridianAuthenticationError as err:
            raise ConfigEntryAuthFailed from err
        except MeridianConnectionError as err:
            raise UpdateFailed("Unable to reach Meridian") from err
        except (MeridianError, ValueError) as err:
            raise UpdateFailed("Meridian returned invalid energy data") from err

    async def _async_sync_property(
        self,
        account_number: str,
        property_data: MeridianProperty,
    ) -> PropertySyncResult:
        key = property_key(account_number, property_data.id)
        consumption_energy_id, consumption_cost_id = consumption_ids(key)
        existing = await async_has_statistics(self.hass, consumption_energy_id)
        since = datetime.now(UTC) - (REVISION_OVERLAP if existing else INITIAL_BACKFILL)

        consumption = await self._async_fetch_since(
            account_number=account_number,
            property_id=property_data.id,
            direction=READING_CONSUMPTION,
            since=since,
        )
        consumption_rows, _ = await async_import_measurements(
            self.hass,
            stat_energy_id=consumption_energy_id,
            stat_cost_id=consumption_cost_id,
            energy_name=f"Meridian consumption — {property_data.address}",
            cost_name=f"Meridian cost — {property_data.address}",
            measurements=consumption,
        )

        generation_rows = 0
        if any(meter.has_feed_in for meter in property_data.meter_points):
            generation = await self._async_fetch_since(
                account_number=account_number,
                property_id=property_data.id,
                direction=READING_GENERATION,
                since=since,
            )
            generation_energy_id, generation_credit_id = generation_ids(key)
            generation_rows, _ = await async_import_measurements(
                self.hass,
                stat_energy_id=generation_energy_id,
                stat_cost_id=generation_credit_id,
                energy_name=f"Meridian export — {property_data.address}",
                cost_name=f"Meridian export credit — {property_data.address}",
                measurements=generation,
            )

        return PropertySyncResult(
            property_key=key,
            consumption_rows=consumption_rows,
            generation_rows=generation_rows,
            latest_reading=max((item.start for item in consumption), default=None),
            estimated_rows=sum(item.quality != "ACTUAL" for item in consumption),
        )

    async def _async_fetch_since(
        self,
        *,
        account_number: str,
        property_id: str,
        direction: str,
        since: datetime,
    ) -> tuple[MeridianMeasurement, ...]:
        """Fetch backwards until the requested UTC cutoff, with loop guards."""
        before: str | None = None
        measurements: dict[tuple[datetime, str], MeridianMeasurement] = {}
        now = datetime.now(UTC)
        end_on = now.astimezone(_NZ).date().isoformat()

        for _page_number in range(24):
            page = await self.client.async_get_measurements(
                account_number=account_number,
                property_id=property_id,
                direction=direction,
                end_on=end_on,
                before=before,
            )
            if not page.measurements:
                break
            for measurement in page.measurements:
                interval_end = measurement.end or measurement.start
                if (
                    measurement.start.astimezone(UTC) >= since
                    and interval_end.astimezone(UTC) <= now
                ):
                    key = (measurement.start, measurement.channel_id)
                    existing = measurements.get(key)
                    if existing is None or (
                        existing.quality != "ACTUAL" and measurement.quality == "ACTUAL"
                    ):
                        measurements[key] = measurement
            oldest = min(item.start.astimezone(UTC) for item in page.measurements)
            if oldest < since or not page.has_previous_page:
                break
            if not page.start_cursor or page.start_cursor == before:
                raise ValueError("Meridian pagination did not advance")
            before = page.start_cursor
        else:
            raise ValueError("Meridian pagination exceeded the safety limit")

        return tuple(sorted(measurements.values(), key=lambda item: item.start))
