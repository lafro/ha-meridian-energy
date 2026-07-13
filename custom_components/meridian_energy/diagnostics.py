"""Privacy-preserving diagnostics for Meridian Energy."""

from __future__ import annotations

from typing import Any

from homeassistant.core import HomeAssistant

from . import MeridianConfigEntry


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: MeridianConfigEntry
) -> dict[str, Any]:
    """Return diagnostics without account, address, email, usage or token data."""
    del hass
    data = entry.runtime_data.coordinator.data
    return {
        "config_entry": {
            "version": entry.version,
            "minor_version": entry.minor_version,
        },
        "coordinator": {
            "last_update_success": entry.runtime_data.coordinator.last_update_success,
            "account_count": data.account_count,
            "property_count": data.property_count,
            "property_results": [
                {
                    "consumption_rows": result.consumption_rows,
                    "generation_rows": result.generation_rows,
                    "latest_reading": result.latest_reading,
                    "estimated_rows": result.estimated_rows,
                }
                for result in data.results
            ],
            "synced_at": data.synced_at,
        },
    }
