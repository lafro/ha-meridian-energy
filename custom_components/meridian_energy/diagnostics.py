"""Privacy-preserving diagnostics for Meridian Energy."""

from __future__ import annotations

from typing import Any

from homeassistant.core import HomeAssistant

from . import MeridianConfigEntry


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: MeridianConfigEntry
) -> dict[str, Any]:
    """
    Return diagnostics without account, address, email, usage or token data.

    Units are named by their account's and property's positions and their
    direction, never by an identifier; the last failure by its category and
    the class of the error behind it.
    """
    del hass
    coordinator = entry.runtime_data.coordinator
    data = coordinator.data
    last_exception = coordinator.last_exception
    cause = getattr(last_exception, "__cause__", None)
    billing_cache_age = coordinator.billing_metadata_cache_age_seconds
    diagnostics: dict[str, Any] = {
        "config_entry": {
            "version": entry.version,
            "minor_version": entry.minor_version,
        },
        "coordinator": {
            "last_update_success": coordinator.last_update_success,
            "last_failure": (
                {
                    "key": getattr(last_exception, "translation_key", None),
                    "cause": type(cause).__name__ if cause is not None else None,
                }
                if last_exception is not None
                else None
            ),
            "first_sync_pending": data is None,
            "account_count": len(coordinator.accounts),
            "billing_metadata_unavailable_count": (
                coordinator.billing_metadata_unavailable_count
            ),
            "billing_metadata_cache_age_seconds": (
                round(billing_cache_age, 3) if billing_cache_age is not None else None
            ),
            "units": coordinator.unit_diagnostics(),
        },
    }
    if data is None:
        return diagnostics
    diagnostics["coordinator"].update(
        {
            "property_count": data.property_count,
            "sync_duration_seconds": round(data.sync_duration_seconds, 3),
            "requests": data.requests,
            "synced_at": data.synced_at,
            "property_results": [
                {
                    "latest_reading": result.latest_reading,
                    "estimated_rows": result.estimated_rows,
                    "oldest_provisional_interval": result.oldest_estimated,
                    "newest_provisional_interval": result.newest_estimated,
                    "upstream_quality_counts": dict(result.quality_counts),
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
                    "billing_data_complete": {
                        "usage": result.usage_complete,
                        "cost": result.cost_complete,
                        "export": result.export_complete,
                        "credit": result.credit_complete,
                    },
                    "has_feed_in": result.has_feed_in,
                    "usage_available": result.current_bill_usage is not None,
                    "cost_available": result.current_bill_cost is not None,
                    "export_available": result.current_bill_export is not None,
                    "credit_available": result.current_bill_credit is not None,
                }
                for result in data.account_results
            ],
        }
    )
    return diagnostics
