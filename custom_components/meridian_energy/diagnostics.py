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
    coordinator = entry.runtime_data.coordinator
    data = coordinator.data
    last_exception = coordinator.last_exception
    billing_cache_age = coordinator.billing_metadata_cache_age_seconds
    return {
        "config_entry": {
            "version": entry.version,
            "minor_version": entry.minor_version,
        },
        "coordinator": {
            "last_update_success": coordinator.last_update_success,
            "last_exception_type": (
                type(last_exception).__name__ if last_exception is not None else None
            ),
            "account_count": data.account_count,
            "property_count": data.property_count,
            "sync_mode": data.sync_mode,
            "topology_refreshed": data.topology_refreshed,
            "topology_cache_age_seconds": data.topology_cache_age_seconds,
            "sync_duration_seconds": round(data.sync_duration_seconds, 3),
            "billing_metadata_unavailable_count": (
                coordinator.billing_metadata_unavailable_count
            ),
            "billing_metadata_cache_age_seconds": (
                round(billing_cache_age, 3) if billing_cache_age is not None else None
            ),
            "property_results": [
                {
                    "sync_mode": result.sync_mode,
                    "requested_window_start": result.requested_since,
                    "requested_window_end": data.synced_at,
                    "api_pages": {
                        "consumption": result.consumption_pages,
                        "generation": result.generation_pages,
                    },
                    "rows_received": {
                        "consumption": result.consumption_received_rows,
                        "generation": result.generation_received_rows,
                    },
                    "rows_retained": {
                        "consumption": result.consumption_retained_rows,
                        "generation": result.generation_retained_rows,
                    },
                    "rows_imported": {
                        "consumption": result.consumption_rows,
                        "generation": result.generation_rows,
                    },
                    "latest_reading": result.latest_reading,
                    "estimated_rows": result.estimated_rows,
                    "oldest_provisional_interval": result.oldest_estimated,
                    "newest_provisional_interval": result.newest_estimated,
                    "upstream_quality_counts": dict(result.quality_counts),
                    "observed_rows_per_hour": result.observed_rows_per_hour,
                }
                for result in data.results
            ],
            "account_results": [
                {
                    "billing_metadata_available": result.billing_period is not None,
                    "billing_period_type": (
                        result.billing_period.period_length
                        if result.billing_period is not None
                        else None
                    ),
                    "billing_period_fixed": (
                        result.billing_period.is_fixed
                        if result.billing_period is not None
                        else None
                    ),
                    "billing_data_complete": result.billing_data_complete,
                    "has_feed_in": result.has_feed_in,
                    "usage_available": result.current_bill_usage is not None,
                    "cost_available": result.current_bill_cost is not None,
                    "export_available": result.current_bill_export is not None,
                    "credit_available": result.current_bill_credit is not None,
                }
                for result in data.account_results
            ],
            "synced_at": data.synced_at,
        },
    }
