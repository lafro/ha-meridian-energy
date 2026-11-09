"""Privacy-preserving diagnostics for Meridian Energy."""

from __future__ import annotations

from typing import Any

from homeassistant.core import HomeAssistant

from . import MeridianConfigEntry


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: MeridianConfigEntry
) -> dict[str, Any]:
    """
    Return diagnostics without identifiers, addresses, amounts or tokens.

    Units are named by their account's and property's positions and their
    direction, never by an identifier; the last failure by its category and
    the class of the error behind it. Of each account's figures, only
    whether each is known. Skipped accounts and rate limits are counts.
    """
    del hass
    coordinator = entry.runtime_data.coordinator
    data = coordinator.data
    last_exception = coordinator.last_exception
    cause = getattr(last_exception, "__cause__", None)
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
            # Accounts the latest list left out for their status.
            "skipped_account_count": coordinator.client.skipped_accounts,
            # Rate limits met since the entry loaded.
            "rate_limits": coordinator.rate_limits_met,
            "extras": coordinator.extras_diagnostics(),
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
                    "has_feed_in": result.has_feed_in,
                    "price_sensors": result.price_sensors,
                    "known": {
                        "bill_usage": result.bill.usage is not None,
                        "bill_cost": result.bill.cost is not None,
                        "bill_export": result.bill.export is not None,
                        "bill_credit": result.bill.credit is not None,
                        "bill_estimate": result.bill.estimate is not None,
                        "total_due": result.total_due is not None,
                        "last_bill": result.last_bill is not None,
                        "last_payment": result.last_payment is not None,
                        "daily_charge": result.prices is not None
                        and result.prices.daily_charge is not None,
                        "anytime_price": result.prices is not None
                        and result.prices.anytime is not None,
                    },
                }
                for result in data.account_results
            ],
        }
    )
    return diagnostics
