from datetime import datetime

import pytest

from ispypsa.translator.snapshots import _create_complete_snapshots_index
from ispypsa.translator.temporal_filters import (
    _filter_snapshots_for_representative_weeks,
)


def test_create_representative_weeks_filter_one_week_start_of_fy():
    snapshot = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2025,
        operational_temporal_resolution_min=30,
        year_type="fy",
    )
    snapshots = _filter_snapshots_for_representative_weeks(
        representative_weeks=[1],
        snapshots=snapshot,
        start_year=2025,
        end_year=2025,
        year_type="fy",
    )
    expected_first_datetime = datetime(year=2024, month=7, day=1, minute=30)
    expected_last_datetime = datetime(year=2024, month=7, day=8, minute=0)
    assert snapshots["snapshots"].iloc[0] == expected_first_datetime
    assert snapshots["snapshots"].iloc[-1] == expected_last_datetime
    assert len(snapshots.index) == 24 * 2 * 7


def test_create_representative_weeks_filter_one_week_start_of_calendar_year():
    snapshots = _create_complete_snapshots_index(
        start_year=2024,
        end_year=2024,
        operational_temporal_resolution_min=30,
        year_type="calendar",
    )
    snapshots = _filter_snapshots_for_representative_weeks(
        representative_weeks=[1],
        snapshots=snapshots,
        start_year=2024,
        end_year=2024,
        year_type="calendar",
    )
    expected_first_datetime = datetime(year=2024, month=1, day=1, minute=30)
    expected_last_datetime = datetime(year=2024, month=1, day=8, minute=0)
    assert snapshots["snapshots"].iloc[0] == expected_first_datetime
    assert snapshots["snapshots"].iloc[-1] == expected_last_datetime
    assert len(snapshots.index) == 24 * 2 * 7


def test_create_representative_weeks_filter_two_weeks_three_year_snapshot():
    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2027,
        operational_temporal_resolution_min=30,
        year_type="fy",
    )
    snapshots = _filter_snapshots_for_representative_weeks(
        representative_weeks=[1, 3],
        snapshots=snapshots,
        start_year=2025,
        end_year=2027,
        year_type="fy",
    )
    expected_first_datetime = datetime(year=2024, month=7, day=1, minute=30)
    expected_last_datetime = datetime(year=2026, month=7, day=27, minute=0)
    assert snapshots["snapshots"].iloc[0] == expected_first_datetime
    assert snapshots["snapshots"].iloc[-1] == expected_last_datetime
    assert len(snapshots.index) == 24 * 2 * 7 * 2 * 3


def test_create_representative_weeks_filter_two_weeks_of_calendar_year_three_year_snapshot():
    snapshots = _create_complete_snapshots_index(
        start_year=2024,
        end_year=2026,
        operational_temporal_resolution_min=30,
        year_type="calendar",
    )
    snapshots = _filter_snapshots_for_representative_weeks(
        representative_weeks=[1, 3],
        snapshots=snapshots,
        start_year=2024,
        end_year=2026,
        year_type="calendar",
    )
    expected_first_datetime = datetime(year=2024, month=1, day=1, minute=30)
    expected_last_datetime = datetime(year=2026, month=1, day=26, minute=0)
    assert snapshots["snapshots"].iloc[0] == expected_first_datetime
    assert snapshots["snapshots"].iloc[-1] == expected_last_datetime
    assert len(snapshots.index) == 24 * 2 * 7 * 2 * 3


def test_create_representative_weeks_filter_fail_with_out_of_range_week_number():
    snapshots = _create_complete_snapshots_index(
        start_year=2024,
        end_year=2026,
        operational_temporal_resolution_min=30,
        year_type="calendar",
    )
    with pytest.raises(ValueError):
        _filter_snapshots_for_representative_weeks(
            representative_weeks=[1, 3, 52],
            snapshots=snapshots,
            start_year=2024,
            end_year=2026,
            year_type="calendar",
        )
