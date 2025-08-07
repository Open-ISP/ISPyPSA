from datetime import datetime

import pandas as pd
import pytest

from ispypsa.translator.snapshots import _create_complete_snapshots_index
from ispypsa.translator.temporal_filters import (
    _filter_snapshots_for_representative_weeks,
)


def test_create_representative_weeks_filter_one_week_start_of_fy(csv_str_to_df):
    """Test filtering for first week of financial year.

    Financial year 2025 starts July 1, 2024. Week 1 is the first full week,
    starting after the first Monday.
    """
    # Create full snapshots for FY2025
    snapshot = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2025,
        temporal_resolution_min=30,
        year_type="fy",
    )

    # Filter for week 1
    result = _filter_snapshots_for_representative_weeks(
        representative_weeks=[1],
        snapshots=snapshot,
        start_year=2025,
        end_year=2025,
        year_type="fy",
    )

    # Expected result: snapshots from July 1-8, 2024 (30-minute intervals)
    # First snapshot is at 00:30 on July 1, last is at 00:00 on July 8
    assert result["snapshots"].iloc[0] == datetime(year=2024, month=7, day=1, minute=30)
    assert result["snapshots"].iloc[-1] == datetime(year=2024, month=7, day=8, minute=0)
    assert len(result.index) == 24 * 2 * 7  # 336 half-hour intervals in a week


def test_create_representative_weeks_filter_one_week_start_of_calendar_year(
    csv_str_to_df,
):
    """Test filtering for first week of calendar year.

    2024 starts on Monday, so week 1 runs from Jan 1-7.
    """
    # Create full snapshots for 2024
    snapshots = _create_complete_snapshots_index(
        start_year=2024,
        end_year=2024,
        temporal_resolution_min=30,
        year_type="calendar",
    )

    # Filter for week 1
    result = _filter_snapshots_for_representative_weeks(
        representative_weeks=[1],
        snapshots=snapshots,
        start_year=2024,
        end_year=2024,
        year_type="calendar",
    )

    # Verify first and last snapshots
    assert result["snapshots"].iloc[0] == datetime(year=2024, month=1, day=1, minute=30)
    assert result["snapshots"].iloc[-1] == datetime(year=2024, month=1, day=8, minute=0)
    assert len(result.index) == 24 * 2 * 7  # 336 half-hour intervals


def test_create_representative_weeks_filter_two_weeks_three_year_snapshot(
    csv_str_to_df,
):
    """Test filtering for weeks 1 and 3 across three financial years.

    Should return 2 weeks per year * 3 years = 6 weeks total.
    """
    # Create full snapshots for FY2025-2027
    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2027,
        temporal_resolution_min=30,
        year_type="fy",
    )

    # Filter for weeks 1 and 3
    result = _filter_snapshots_for_representative_weeks(
        representative_weeks=[1, 3],
        snapshots=snapshots,
        start_year=2025,
        end_year=2027,
        year_type="fy",
    )

    # First week starts July 1, 2024; last week 3 ends July 27, 2026
    assert result["snapshots"].iloc[0] == datetime(year=2024, month=7, day=1, minute=30)
    assert result["snapshots"].iloc[-1] == datetime(
        year=2026, month=7, day=27, minute=0
    )
    assert len(result.index) == 24 * 2 * 7 * 2 * 3  # 2016 intervals


def test_create_representative_weeks_filter_two_weeks_of_calendar_year_three_year_snapshot(
    csv_str_to_df,
):
    """Test filtering for weeks 1 and 3 across three calendar years.

    Should return 2 weeks per year * 3 years = 6 weeks total.
    """
    # Create full snapshots for 2024-2026
    snapshots = _create_complete_snapshots_index(
        start_year=2024,
        end_year=2026,
        temporal_resolution_min=30,
        year_type="calendar",
    )

    # Filter for weeks 1 and 3
    result = _filter_snapshots_for_representative_weeks(
        representative_weeks=[1, 3],
        snapshots=snapshots,
        start_year=2024,
        end_year=2026,
        year_type="calendar",
    )

    # Verify boundary snapshots
    assert result["snapshots"].iloc[0] == datetime(year=2024, month=1, day=1, minute=30)
    assert result["snapshots"].iloc[-1] == datetime(
        year=2026, month=1, day=26, minute=0
    )
    assert len(result.index) == 24 * 2 * 7 * 2 * 3  # 2016 intervals


def test_create_representative_weeks_filter_fail_with_out_of_range_week_number(
    csv_str_to_df,
):
    """Test that week 52 raises an error for years without 52 full weeks."""
    # Create full snapshots
    snapshots = _create_complete_snapshots_index(
        start_year=2024,
        end_year=2026,
        temporal_resolution_min=30,
        year_type="calendar",
    )

    # Week 52 would extend beyond year end
    with pytest.raises(
        ValueError, match="Representative week 52 ends after end of model year"
    ):
        _filter_snapshots_for_representative_weeks(
            representative_weeks=[1, 3, 52],
            snapshots=snapshots,
            start_year=2024,
            end_year=2026,
            year_type="calendar",
        )
