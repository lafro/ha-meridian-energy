"""The pure window rules: dates, request sizes, kept rows and hourly sums."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from custom_components.meridian_energy.const import AGGREGATE_CHANNEL_ID
from custom_components.meridian_energy.models import MeridianMeasurement
from custom_components.meridian_energy.windows import (
    NZ,
    MeasurementWindow,
    Reading,
    date_hours,
    hourly_readings,
    local_midnight,
    nz_date,
    windows_for,
)

HOUR = timedelta(hours=1)
# 1 July 2026 is a NZST date: New Zealand midnight is 12:00 UTC the day before.
JULY = date(2026, 7, 1)


def _row(
    start: datetime,
    *,
    channel: str = "register-1",
    quality: str = "ACTUAL",
    value: str = "0.5",
    cost: str | None = "15",
    hours: int = 1,
) -> MeridianMeasurement:
    return MeridianMeasurement(
        start=start,
        end=start + HOUR * hours,
        value_kwh=Decimal(value),
        quality=quality,
        direction="CONSUMPTION",
        channel_id=channel,
        cost_cents=None if cost is None else Decimal(cost),
    )


def test_a_date_has_23_24_or_25_hours() -> None:
    assert len(date_hours(JULY)) == 24
    assert date_hours(JULY)[0] == datetime(2026, 6, 30, 12, tzinfo=UTC)
    # Daylight saving starts on 27 September 2026 and ends on 5 April 2026.
    assert len(date_hours(date(2026, 9, 27))) == 23
    assert len(date_hours(date(2026, 4, 5))) == 25


def test_nz_date_and_midnight_agree() -> None:
    midnight = local_midnight(JULY)
    assert midnight == datetime(2026, 6, 30, 12, tzinfo=UTC)
    assert nz_date(midnight) == JULY
    assert nz_date(midnight - HOUR) == JULY - timedelta(days=1)


def test_a_window_runs_from_midnight_to_midnight_with_room_to_spare() -> None:
    window = MeasurementWindow(JULY, JULY + timedelta(days=6))

    assert window.start == local_midnight(JULY)
    assert window.end == local_midnight(JULY + timedelta(days=7))
    assert window.hours == 168
    assert window.days == 7
    assert window.last == 214  # 168 x 1.25 = 210, and 4 spare
    assert window.dates() == [JULY + timedelta(days=offset) for offset in range(7)]
    assert MeasurementWindow(JULY, JULY).last == 34


def test_dates_group_newest_first_into_windows_of_at_most_seven() -> None:
    dates = [JULY - timedelta(days=offset) for offset in range(21)]

    windows = windows_for(dates)

    assert windows == (
        MeasurementWindow(JULY - timedelta(days=6), JULY),
        MeasurementWindow(JULY - timedelta(days=13), JULY - timedelta(days=7)),
        MeasurementWindow(JULY - timedelta(days=20), JULY - timedelta(days=14)),
    )


def test_a_window_spans_dates_between_missing_ones_but_never_more_than_seven() -> None:
    dates = {JULY, JULY - timedelta(days=3), JULY - timedelta(days=7)}

    assert windows_for(dates) == (
        MeasurementWindow(JULY - timedelta(days=3), JULY),
        MeasurementWindow(JULY - timedelta(days=7), JULY - timedelta(days=7)),
    )
    assert windows_for([]) == ()


def test_only_ended_hours_inside_the_window_are_kept() -> None:
    window = MeasurementWindow(JULY, JULY)
    first = window.start
    now = first + 5 * HOUR + timedelta(minutes=30)
    rows = [
        _row(first - HOUR),  # the date before
        _row(first),
        _row(first + 4 * HOUR),
        _row(first + 5 * HOUR),  # still running
        _row(window.end),  # the date after
    ]

    readings = hourly_readings(rows, window, now)

    assert sorted(readings) == [first, first + 4 * HOUR]
    assert readings[first] == Reading(Decimal("0.5"), Decimal(15), "ACTUAL")


def test_channels_are_summed_and_an_hour_without_every_cost_has_none() -> None:
    window = MeasurementWindow(JULY, JULY)
    hour = window.start
    rows = [
        _row(hour, channel="register-1", value="0.25", cost="10"),
        _row(hour, channel="register-2", value="0.5", cost="5"),
        _row(hour + HOUR, channel="register-1", cost=None),
        _row(hour + HOUR, channel="register-2", cost="5"),
    ]

    readings = hourly_readings(rows, window, window.end)

    assert readings[hour] == Reading(Decimal("0.75"), Decimal(15), "ACTUAL")
    assert readings[hour + HOUR] == Reading(Decimal("1.0"), None, "ACTUAL")


def test_an_actual_reading_wins_over_an_estimate_of_the_same_channel() -> None:
    window = MeasurementWindow(JULY, JULY)
    hour = window.start
    rows = [
        _row(hour, quality="ESTIMATE", value="9"),
        _row(hour, value="0.5"),
        _row(hour, quality="ESTIMATE", value="7"),
        _row(hour + HOUR, quality="ESTIMATE", value="2"),
        _row(hour + HOUR, channel="register-2"),
    ]

    readings = hourly_readings(rows, window, window.end)

    assert readings[hour] == Reading(Decimal("0.5"), Decimal(15), "ACTUAL")
    # One channel still estimated makes the hour an estimate.
    assert readings[hour + HOUR] == Reading(Decimal("2.5"), Decimal(30), "ESTIMATE")


def test_a_repeated_identical_reading_counts_once() -> None:
    window = MeasurementWindow(JULY, JULY)
    rows = [_row(window.start), _row(window.start)]

    assert hourly_readings(rows, window, window.end)[window.start].energy == Decimal(
        "0.5"
    )


@pytest.mark.parametrize(
    "rows",
    [
        # An aggregate row beside a register's.
        lambda hour: [_row(hour), _row(hour, channel=AGGREGATE_CHANNEL_ID)],
        lambda hour: [_row(hour, channel=AGGREGATE_CHANNEL_ID), _row(hour)],
        # Two aggregate rows of one quality.
        lambda hour: [
            _row(hour, channel=AGGREGATE_CHANNEL_ID),
            _row(hour, channel=AGGREGATE_CHANNEL_ID),
        ],
        # Two different readings of one register and quality.
        lambda hour: [_row(hour, value="0.5"), _row(hour, value="0.75")],
    ],
)
def test_readings_that_cannot_be_told_apart_are_refused(rows) -> None:
    window = MeasurementWindow(JULY, JULY)

    with pytest.raises(ValueError, match="ambiguous"):
        hourly_readings(rows(window.start), window, window.end)


@pytest.mark.parametrize(
    "row",
    [
        lambda hour: _row(hour + timedelta(minutes=30)),
        lambda hour: _row(hour, value="NaN"),
        lambda hour: _row(hour, cost="Infinity"),
    ],
)
def test_a_reading_off_the_hour_or_not_finite_is_refused(row) -> None:
    window = MeasurementWindow(JULY, JULY)

    with pytest.raises(ValueError, match="Meridian measurement"):
        hourly_readings([row(window.start)], window, window.end)


def test_a_reading_given_in_new_zealand_time_is_keyed_by_its_utc_hour() -> None:
    window = MeasurementWindow(JULY, JULY)
    local = window.start.astimezone(NZ)

    readings = hourly_readings([_row(local)], window, window.end)

    assert list(readings) == [window.start]
    assert next(iter(readings)).tzinfo is UTC
