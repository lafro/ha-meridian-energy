"""
The current bill's totals against a real recorder, for an account of two properties.

An account's bill totals add up every property's statistics for the billing
period. Export and credit add up those of every property with a feed-in
register, so one whose export has no readings in the period leaves them
unknown rather than too low. Every value is synthetic.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from homeassistant.core import HomeAssistant

from custom_components.meridian_energy.coordinator import MeridianDataCoordinator
from custom_components.meridian_energy.models import (
    MeridianAccount,
    MeridianBillingPeriod,
    MeridianMeterPoint,
    MeridianProperty,
)
from custom_components.meridian_energy.windows import local_midnight

from .kraken_fake import FakeKraken
from .test_lanes_recorder import DAY, HOUR, NOW, TODAY, _floor_hour, _sync
from .test_windows_recorder import (
    mock_recorder_before_hass,  # noqa: F401 - the recorder fixture's setup
)

_NUMBER = "A-TEST-1"
_PROPERTIES = ("synthetic-property-1", "synthetic-property-2")


def _account() -> MeridianAccount:
    return MeridianAccount(
        number=_NUMBER,
        status="ACTIVE",
        properties=tuple(
            MeridianProperty(
                id=property_id,
                address=f"{index} Synthetic Street",
                meter_points=(
                    MeridianMeterPoint(f"synthetic-meter-{index}", "synthetic", True),
                ),
            )
            for index, property_id in enumerate(_PROPERTIES, start=1)
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("second_exports", [True, False])
async def test_an_export_missing_from_one_property_leaves_the_bill_export_unknown(
    recorder_mock, hass: HomeAssistant, second_exports: bool
) -> None:
    del recorder_mock
    kraken = FakeKraken(NOW, history_start=NOW - 400 * DAY, future_rows=False)
    # The second property's feed-in register has no readings in the period.
    silent = FakeKraken(
        NOW, history_start=NOW - 400 * DAY, withdrawn_from=NOW - 400 * DAY
    )

    async def measurements(**kwargs: Any) -> Any:
        source = kraken
        if (
            not second_exports
            and kwargs["property_id"] == _PROPERTIES[1]
            and kwargs["direction"] == "GENERATION"
        ):
            source = silent
        return await source.async_get_measurements(**kwargs)

    start = TODAY - 10 * DAY
    client = MagicMock()
    client.async_get_accounts = AsyncMock(return_value=(_account(),))
    client.async_get_billing_period = AsyncMock(
        return_value=MeridianBillingPeriod(
            period_length="MONTHLY",
            period_length_multiplier=1,
            is_fixed=True,
            start=start,
            end=TODAY + 20 * DAY,
            next_billing_date=TODAY + 21 * DAY,
            period_start_day=start.day,
        )
    )
    client.async_get_measurements = AsyncMock(side_effect=measurements)
    coordinator = MeridianDataCoordinator(hass, client)

    for update in range(2):
        at = NOW + update * HOUR
        kraken.now = silent.now = at
        data = await _sync(hass, coordinator, at)

    (totals,) = data.account_results
    hours = (_floor_hour(at) - local_midnight(start)) / HOUR
    # Usage counts both properties either way.
    assert totals.usage_complete is True
    assert float(totals.current_bill_usage or 0) == pytest.approx(2 * 0.5 * hours)
    if second_exports:
        assert (totals.export_complete, totals.credit_complete) == (True, True)
        assert float(totals.current_bill_export or 0) == pytest.approx(2 * 0.5 * hours)
        assert float(totals.current_bill_credit or 0) == pytest.approx(2 * 0.15 * hours)
    else:
        # Not the first property's export alone, reported as complete.
        assert (totals.current_bill_export, totals.export_complete) == (None, False)
        assert (totals.current_bill_credit, totals.credit_complete) == (None, False)
