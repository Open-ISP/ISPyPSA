"""Tests for temporal filter functions in ispypsa.translator.temporal_filters.

This module tests the functionality of filtering snapshots based on named representative weeks.
The tests cover:
1. Calendar year filtering with various week types (peak demand, minimum demand, etc.)
2. Financial year filtering with various week types
3. Multiple years to ensure correct week selection per year
4. Residual demand calculations (demand minus renewable generation)
5. Edge cases and error handling

Test Structure:
- Each test creates synthetic demand and renewable generation data with known patterns
- The test verifies that the correct weeks are selected based on the criteria
- Results are compared against hardcoded expected DataFrames for clarity
"""

from datetime import datetime

import pandas as pd
import pytest

from ispypsa.translator.temporal_filters import (
    _filter_snapshots_for_named_representative_weeks,
)


def test_calendar_year_peak_demand_weeks(csv_str_to_df):
    """Test selection of peak demand weeks for calendar years.

    This test creates demand data where the peak demand occurs on specific days.
    The function should select the entire week containing the peak demand day.
    """
    # Create snapshots for 2024-2025
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    2024-01-18__12:00:00
    2024-01-22__00:00:00
    2024-12-31__00:00:00
    2025-01-01__00:00:00
    2025-02-10__00:00:00
    2025-02-13__12:00:00
    2025-02-17__00:00:00
    2025-12-31__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with clear peaks
    demand_csv = """
    Datetime,Value
    2024-01-01__00:00:00,500
    2024-01-08__00:00:00,600
    2024-01-15__00:00:00,700
    2024-01-18__12:00:00,1000
    2024-01-22__00:00:00,400
    2024-12-31__00:00:00,300
    2025-01-01__00:00:00,600
    2025-02-10__00:00:00,800
    2025-02-13__12:00:00,1200
    2025-02-17__00:00:00,500
    2025-12-31__00:00:00,400
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for peak demand weeks
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2026,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected: snapshots from the week containing Jan 18, 2024 and Feb 13, 2025
    # Monday 00:00:00 belongs to previous week, so we get snapshots after that time
    expected_csv = """
    snapshots
    2024-01-18__12:00:00
    2024-01-22__00:00:00
    2025-02-13__12:00:00
    2025-02-17__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_financial_year_minimum_demand_weeks(csv_str_to_df):
    """Test selection of minimum demand weeks for financial years.

    This test creates demand data where:
    - FY2024 (Jul 2023 - Jun 2024) Week 10: Minimum demand of 100 MW on Sep 11, 2023
    - FY2025 (Jul 2024 - Jun 2025) Week 15: Minimum demand of 80 MW on Oct 21, 2024

    These weeks should be selected as they contain the lowest instantaneous demand
    for each financial year.
    """
    # Create snapshots for FY2024-2025
    snapshots_csv = """
    snapshots
    2023-07-01__00:00:00
    2023-09-11__00:00:00
    2023-09-15__00:00:00
    2024-06-30__00:00:00
    2024-07-01__00:00:00
    2024-10-21__00:00:00
    2024-10-25__00:00:00
    2025-06-30__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with clear minimums
    demand_csv = """
    Datetime,Value
    2023-07-01__00:00:00,500
    2023-09-11__00:00:00,100
    2023-09-15__00:00:00,300
    2024-06-30__00:00:00,400
    2024-07-01__00:00:00,600
    2024-10-21__00:00:00,80
    2024-10-25__00:00:00,200
    2025-06-30__00:00:00,500
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for minimum demand weeks
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["minimum-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2026,
        year_type="fy",
        demand_data=demand_data,
    )

    # Expected: snapshots from the weeks containing minimum demand
    # Sep 11 and Oct 21 are Mondays at 00:00:00, which mark the END of their weeks
    expected_csv = """
    snapshots
    2023-09-11__00:00:00
    2024-10-21__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_residual_peak_demand_weeks(csv_str_to_df):
    """Test selection of residual peak demand weeks (demand minus renewables).

    This test creates:
    - Demand data with high values
    - Renewable generation that reduces the effective demand
    - 2024 Week 2: High demand (900) but also high renewable (400) = residual 500
    - 2024 Week 4: Moderate demand (700) but low renewable (100) = residual 600 (peak)

    Week 4 should be selected as it has the highest residual demand.
    """
    # Create snapshots
    snapshots_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-10__00:00:00
    2024-01-22__00:00:00
    2024-01-24__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data
    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,900
    2024-01-10__00:00:00,800
    2024-01-22__00:00:00,700
    2024-01-24__00:00:00,600
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Create renewable data
    renewable_csv = """
    Datetime,Value
    2024-01-08__00:00:00,400
    2024-01-10__00:00:00,350
    2024-01-22__00:00:00,100
    2024-01-24__00:00:00,50
    """
    renewable_data = csv_str_to_df(renewable_csv)
    renewable_data["Datetime"] = pd.to_datetime(renewable_data["Datetime"])

    # Filter for residual peak demand weeks
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["residual-peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
        renewable_data=renewable_data,
    )

    # Expected: snapshots from week 4 (highest residual demand)
    # Jan 22 has the highest residual (600) and marks the END of its week
    expected_csv = """
    snapshots
    2024-01-22__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_multiple_week_types_selection(csv_str_to_df):
    """Test selection of multiple week types simultaneously.

    This test verifies that when multiple criteria are specified, all matching
    weeks are included without duplication.

    Important: Monday 00:00:00 timestamps mark the END of a week (covering the
    period ending at that time). Therefore:
    - Jan 8 00:00:00 with 1000 MW is assigned to Week 1 (Jan 1-7)
    - Jan 22 00:00:00 with 200 MW is assigned to Week 3 (Jan 15-21)

    Selected weeks:
    - Week 1: Peak demand (1000 MW) AND peak consumption (avg 1000 MW)
    - Week 3: Minimum demand (200 MW)

    Note: Week 1 wins peak consumption despite having only one data point
    because its average (1000 MW) exceeds other weeks' averages.
    """
    # Create hourly snapshots for better consumption calculation
    snapshots_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    2024-01-09__00:00:00
    2024-01-22__00:00:00
    2024-01-22__12:00:00
    2024-01-23__00:00:00
    2024-02-05__00:00:00
    2024-02-05__12:00:00
    2024-02-06__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data
    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,1000
    2024-01-08__12:00:00,900
    2024-01-09__00:00:00,800
    2024-01-22__00:00:00,200
    2024-01-22__12:00:00,250
    2024-01-23__00:00:00,300
    2024-02-05__00:00:00,955
    2024-02-05__12:00:00,955
    2024-02-06__00:00:00,955
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for multiple week types
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=[
            "peak-demand",
            "minimum-demand",
            "peak-consumption",
        ],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected: snapshots from selected weeks
    # Jan 8 00:00:00 is the END of week with peak demand
    # Jan 22 00:00:00 is the END of week with minimum demand
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-22__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_no_demand_data_returns_empty(csv_str_to_df):
    """Test that function returns empty DataFrame when no demand data provided."""
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=None,
    )

    expected = pd.DataFrame({"snapshots": []})

    pd.testing.assert_frame_equal(result, expected)


def test_residual_metrics_without_renewable_data(csv_str_to_df):
    """Test residual metrics when renewable data is not provided.

    When renewable data is None but residual metrics are requested,
    residual demand should equal demand (assuming zero renewable generation).
    """
    snapshots_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,1000
    2024-01-15__00:00:00,800
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Request residual metrics without providing renewable data
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["residual-peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
        renewable_data=None,
    )

    # Should select week with highest demand (since residual = demand)
    # Jan 8 has highest demand, so we get snapshots from that week
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_year_boundary_week(csv_str_to_df):
    """Test handling of weeks that span across year boundaries.

    This test verifies that weeks at year boundaries are handled correctly,
    particularly when the peak/minimum occurs in a week that spans Dec/Jan.
    """
    # Create snapshots spanning year boundary
    snapshots_csv = """
    snapshots
    2024-12-23__00:00:00
    2024-12-26__00:00:00
    2024-12-30__00:00:00
    2025-01-02__00:00:00
    2025-01-06__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with peak in year-boundary week
    demand_csv = """
    Datetime,Value
    2024-12-23__00:00:00,500
    2024-12-26__00:00:00,800
    2024-12-30__00:00:00,1500
    2025-01-02__00:00:00,1200
    2025-01-06__00:00:00,600
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for peak demand week
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2026,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected: Peak week for each year
    # 2024: Week ending Dec 30 (contains peak of 1500)
    # 2025: Week ending Jan 6 (only week with 2025 data)
    expected_csv = """
    snapshots
    2024-12-26__00:00:00
    2024-12-30__00:00:00
    2025-01-02__00:00:00
    2025-01-06__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_residual_minimum_demand_week(csv_str_to_df):
    """Test selection of residual minimum demand weeks.

    This test verifies the selection of weeks with the lowest residual demand
    (demand minus renewable generation).
    """
    snapshots_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    2024-01-22__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,1000
    2024-01-15__00:00:00,800
    2024-01-22__00:00:00,900
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Renewable data that creates lowest residual in week 2
    renewable_csv = """
    Datetime,Value
    2024-01-08__00:00:00,200
    2024-01-15__00:00:00,700
    2024-01-22__00:00:00,100
    """
    renewable_data = csv_str_to_df(renewable_csv)
    renewable_data["Datetime"] = pd.to_datetime(renewable_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["residual-minimum-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
        renewable_data=renewable_data,
    )

    # Week 2 has residual of 100 (800-700), which is the minimum
    expected_csv = """
    snapshots
    2024-01-15__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_residual_peak_consumption_week(csv_str_to_df):
    """Test selection of residual peak consumption weeks.

    This test verifies selection of weeks with highest average residual demand.
    """
    # Create multiple snapshots per week for consumption calculation
    snapshots_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    2024-01-09__00:00:00
    2024-01-15__00:00:00
    2024-01-15__12:00:00
    2024-01-16__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,1000
    2024-01-08__12:00:00,1100
    2024-01-09__00:00:00,900
    2024-01-15__00:00:00,800
    2024-01-15__12:00:00,850
    2024-01-16__00:00:00,900
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    renewable_csv = """
    Datetime,Value
    2024-01-08__00:00:00,100
    2024-01-08__12:00:00,150
    2024-01-09__00:00:00,50
    2024-01-15__00:00:00,400
    2024-01-15__12:00:00,450
    2024-01-16__00:00:00,500
    """
    renewable_data = csv_str_to_df(renewable_csv)
    renewable_data["Datetime"] = pd.to_datetime(renewable_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["residual-peak-consumption"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
        renewable_data=renewable_data,
    )

    # Week 1 avg residual: (900+950+850)/3 = 900
    # Week 2 avg residual: (400+400+400)/3 = 400
    # Week 1 has higher average residual consumption
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_multiple_weeks_with_identical_metrics(csv_str_to_df):
    """Test handling when multiple weeks have identical peak/minimum values.

    When multiple weeks have the same metric value, the first occurrence
    should be selected.
    """
    snapshots_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    2024-01-22__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with identical peaks in weeks 1 and 3
    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,1000
    2024-01-15__00:00:00,800
    2024-01-22__00:00:00,1000
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Should select the first week with peak demand (Week 1)
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_negative_residual_demand(csv_str_to_df):
    """Test handling of negative residual demand (renewable > demand).

    This can occur when renewable generation exceeds demand, resulting
    in negative residual values.
    """
    snapshots_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,500
    2024-01-15__00:00:00,600
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Renewable exceeds demand
    renewable_csv = """
    Datetime,Value
    2024-01-08__00:00:00,800
    2024-01-15__00:00:00,400
    """
    renewable_data = csv_str_to_df(renewable_csv)
    renewable_data["Datetime"] = pd.to_datetime(renewable_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["residual-minimum-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
        renewable_data=renewable_data,
    )

    # Week 1 has residual of -300, which is the minimum
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_sparse_data_coverage(csv_str_to_df):
    """Test handling of sparse data where some weeks have very few data points.

    This tests the robustness when weeks have uneven data coverage.
    """
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    2024-01-08__06:00:00
    2024-01-08__12:00:00
    2024-01-08__18:00:00
    2024-01-15__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Week 1 has only one data point, Week 2 has multiple
    demand_csv = """
    Datetime,Value
    2024-01-01__00:00:00,700
    2024-01-08__00:00:00,800
    2024-01-08__06:00:00,900
    2024-01-08__12:00:00,1000
    2024-01-08__18:00:00,850
    2024-01-15__00:00:00,600
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-consumption"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Week 2 should have highest average consumption
    # Note: Jan 15 00:00:00 is Monday and marks END of week 2
    expected_csv = """
    snapshots
    2024-01-08__06:00:00
    2024-01-08__12:00:00
    2024-01-08__18:00:00
    2024-01-15__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_empty_snapshots_dataframe(csv_str_to_df):
    """Test handling of empty snapshots DataFrame."""
    snapshots = pd.DataFrame({"snapshots": pd.Series([], dtype="datetime64[ns]")})

    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,1000
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
    )

    expected = pd.DataFrame({"snapshots": pd.Series([], dtype="datetime64[ns]")})

    pd.testing.assert_frame_equal(result, expected)


def test_mismatched_datetime_indices(csv_str_to_df):
    """Test handling when demand and renewable data have different timestamps.

    The function should handle merging data with non-overlapping timestamps.
    """
    snapshots_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    2024-01-15__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Demand data with all timestamps
    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,1000
    2024-01-08__12:00:00,900
    2024-01-15__00:00:00,800
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Renewable data missing some timestamps
    renewable_csv = """
    Datetime,Value
    2024-01-08__00:00:00,200
    2024-01-15__00:00:00,300
    """
    renewable_data = csv_str_to_df(renewable_csv)
    renewable_data["Datetime"] = pd.to_datetime(renewable_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["residual-peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
        renewable_data=renewable_data,
    )

    # Week 1: Jan 8 00:00:00 has residual 800
    # Week 2: Jan 8 12:00:00 has residual 900 (peak), Jan 15 has residual 500
    expected_csv = """
    snapshots
    2024-01-08__12:00:00
    2024-01-15__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_all_residual_week_types(csv_str_to_df):
    """Test all residual week type combinations together.

    This ensures all residual metrics work correctly when used simultaneously.
    """
    snapshots_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    2024-01-15__00:00:00
    2024-01-15__12:00:00
    2024-01-22__00:00:00
    2024-01-22__12:00:00
    2024-01-29__00:00:00
    2024-01-29__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    demand_csv = """
    Datetime,Value
    2024-01-08__00:00:00,1000
    2024-01-08__12:00:00,1100
    2024-01-15__00:00:00,600
    2024-01-15__12:00:00,700
    2024-01-22__00:00:00,800
    2024-01-22__12:00:00,850
    2024-01-29__00:00:00,900
    2024-01-29__12:00:00,950
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    renewable_csv = """
    Datetime,Value
    2024-01-08__00:00:00,100
    2024-01-08__12:00:00,150
    2024-01-15__00:00:00,500
    2024-01-15__12:00:00,600
    2024-01-22__00:00:00,200
    2024-01-22__12:00:00,250
    2024-01-29__00:00:00,50
    2024-01-29__12:00:00,100
    """
    renewable_data = csv_str_to_df(renewable_csv)
    renewable_data["Datetime"] = pd.to_datetime(renewable_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=[
            "residual-peak-demand",
            "residual-minimum-demand",
            "residual-peak-consumption",
        ],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
        renewable_data=renewable_data,
    )

    # Week 1: residual peak = 900, avg = 900 (peak consumption)
    # Week 2: residual peak = 950 (peak demand), min = 100 (minimum demand)
    # Week 3: residual peak = 600, min = 100, avg = 350
    # Week 4: residual peak = 850, avg = 725
    # Week 5: residual peak = 850, avg = 850
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    2024-01-15__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_calendar_year_boundary_peak_spans_years(csv_str_to_df):
    """Test when peak demand week spans from one year to the next.

    This test verifies correct handling when the peak demand occurs in a week
    that starts in December of one year and ends in January of the next.
    """
    # Create snapshots spanning Dec 2024 to Jan 2025
    snapshots_csv = """
    snapshots
    2024-12-23__00:00:00
    2024-12-26__00:00:00
    2024-12-30__00:00:00
    2025-01-02__00:00:00
    2025-01-06__00:00:00
    2025-01-13__00:00:00
    2025-01-20__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data where peak spans year boundary
    demand_csv = """
    Datetime,Value
    2024-12-23__00:00:00,500
    2024-12-26__00:00:00,800
    2024-12-30__00:00:00,600
    2025-01-02__00:00:00,1500
    2025-01-06__00:00:00,1200
    2025-01-13__00:00:00,700
    2025-01-20__00:00:00,500
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for peak demand weeks
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2026,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected:
    # 2024: Week ending Dec 30 (max demand 800 in that year)
    # 2025: Week ending Jan 6 (max demand 1500)
    expected_csv = """
    snapshots
    2024-12-26__00:00:00
    2024-12-30__00:00:00
    2025-01-02__00:00:00
    2025-01-06__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_financial_year_boundary_minimum_demand_simplified(csv_str_to_df):
    """Test financial year boundary with minimum demand selection.

    Tests the June/July boundary for financial years when minimum
    demand occurs in a week spanning the boundary.
    """
    # Create snapshots spanning FY2025 (Jul 2024 - Jun 2025) and FY2026 (Jul 2025 - Jun 2026)
    snapshots_csv = """
    snapshots
    2024-07-01__00:00:00
    2024-07-04__00:00:00
    2024-07-08__00:00:00
    2025-06-23__00:00:00
    2025-06-30__00:00:00
    2025-07-07__00:00:00
    2025-07-14__00:00:00
    2026-06-29__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with minimums around FY boundaries
    demand_csv = """
    Datetime,Value
    2024-07-01__00:00:00,300
    2024-07-04__00:00:00,200
    2024-07-08__00:00:00,500
    2025-06-23__00:00:00,450
    2025-06-30__00:00:00,150
    2025-07-07__00:00:00,600
    2025-07-14__00:00:00,400
    2026-06-29__00:00:00,350
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for minimum demand weeks in FY2025 and FY2026
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["minimum-demand"],
        snapshots=snapshots,
        start_year=2025,
        end_year=2026,
        year_type="fy",
        demand_data=demand_data,
    )

    # Expected:
    # FY2025: Week ending Jun 30, 2025 has min 150
    # FY2026: Week ending Jun 29, 2026 has min 350
    expected_csv = """
    snapshots
    2025-06-30__00:00:00
    2026-06-29__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_calendar_year_week_52_53_handling(csv_str_to_df):
    """Test handling of week 52/53 at year end.

    Some years have 53 weeks when Jan 1 is late in the week.
    This test ensures proper handling of these edge cases.
    """
    # Test with both 2020 and 2021 data to handle week 53 of 2020 that spans into 2021
    snapshots_csv = """
    snapshots
    2020-12-21__00:00:00
    2020-12-24__00:00:00
    2020-12-28__00:00:00
    2020-12-31__00:00:00
    2021-01-04__00:00:00
    2021-01-11__00:00:00
    2021-12-27__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    demand_csv = """
    Datetime,Value
    2020-12-21__00:00:00,700
    2020-12-24__00:00:00,800
    2020-12-28__00:00:00,900
    2020-12-31__00:00:00,1000
    2021-01-04__00:00:00,600
    2021-01-11__00:00:00,500
    2021-12-27__00:00:00,850
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for peak demand in 2020 and 2021
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2020,
        end_year=2021,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected:
    # 2020: Week ending Dec 28, 2020 contains peak value 1000 from Dec 31
    # 2021: Week ending Dec 27, 2021 has peak value 850
    expected_csv = """
    snapshots
    2020-12-24__00:00:00
    2020-12-28__00:00:00
    2021-12-27__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_financial_year_residual_metrics_boundary(csv_str_to_df):
    """Test residual metrics across financial year boundaries.

    Verifies that residual demand calculations work correctly
    when data spans financial year boundaries.
    """
    # Snapshots for FY2025 (Jul 2024 - Jun 2025)
    snapshots_csv = """
    snapshots
    2024-07-01__00:00:00
    2024-07-01__12:00:00
    2024-07-08__00:00:00
    2025-06-23__00:00:00
    2025-06-30__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    demand_csv = """
    Datetime,Value
    2024-07-01__00:00:00,900
    2024-07-01__12:00:00,950
    2024-07-08__00:00:00,800
    2025-06-23__00:00:00,1000
    2025-06-30__00:00:00,1100
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    renewable_csv = """
    Datetime,Value
    2024-07-01__00:00:00,600
    2024-07-01__12:00:00,650
    2024-07-08__00:00:00,100
    2025-06-23__00:00:00,200
    2025-06-30__00:00:00,250
    """
    renewable_data = csv_str_to_df(renewable_csv)
    renewable_data["Datetime"] = pd.to_datetime(renewable_data["Datetime"])

    # Filter for residual metrics in FY2025
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["residual-peak-demand", "residual-minimum-demand"],
        snapshots=snapshots,
        start_year=2025,
        end_year=2025,
        year_type="fy",
        demand_data=demand_data,
        renewable_data=renewable_data,
    )

    # FY2025 data (Jul 1, 2024 - Jun 30, 2025):
    # Jul 1: residual = 900-600=300, Jul 1 12:00: residual = 950-650=300
    # Jul 8: residual = 800-100=700
    # Jun 23: residual = 1000-200=800, Jun 30: residual = 1100-250=850 (peak)
    # Week ending Jul 1 has minimum residual, week ending Jun 30 has peak residual
    expected_csv = """
    snapshots
    2024-07-01__00:00:00
    2025-06-30__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_multiple_years_calendar_boundary_consistency_fixed(csv_str_to_df):
    """Test consistent handling across multiple calendar year boundaries.

    Verifies that the function handles multiple years consistently,
    especially when weeks span year boundaries.
    """
    # Create data spanning 2 years with consistent weekly pattern
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    2024-12-23__00:00:00
    2024-12-30__00:00:00
    2025-01-06__00:00:00
    2025-01-13__00:00:00
    2025-12-22__00:00:00
    2025-12-29__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Each year has peak in specific weeks
    demand_csv = """
    Datetime,Value
    2024-01-01__00:00:00,950
    2024-01-08__00:00:00,900
    2024-12-23__00:00:00,800
    2024-12-30__00:00:00,850
    2025-01-06__00:00:00,820
    2025-01-13__00:00:00,810
    2025-12-22__00:00:00,700
    2025-12-29__00:00:00,750
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Get peak consumption weeks for 2024 and 2025
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-consumption"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Each year should have its peak consumption week selected
    # 2024: Week ending Jan 1 has peak 950
    # 2025: Week ending Jan 6 has peak 820
    expected_csv = """
    snapshots
    2024-01-01__00:00:00
    2025-01-06__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_leap_year_february_week_handling(csv_str_to_df):
    """Test handling of weeks in February during leap years.

    2024 is a leap year with Feb 29. This test ensures proper
    week assignment around the leap day.
    """
    # Create snapshots around Feb 2024 (leap year)
    snapshots_csv = """
    snapshots
    2024-02-26__00:00:00
    2024-02-28__00:00:00
    2024-02-29__00:00:00
    2024-03-01__00:00:00
    2024-03-04__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    demand_csv = """
    Datetime,Value
    2024-02-26__00:00:00,700
    2024-02-28__00:00:00,800
    2024-02-29__00:00:00,900
    2024-03-01__00:00:00,850
    2024-03-04__00:00:00,750
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Peak is on Feb 29 (leap day)
    # Week containing Feb 29 should be selected
    expected_csv = """
    snapshots
    2024-02-28__00:00:00
    2024-02-29__00:00:00
    2024-03-01__00:00:00
    2024-03-04__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_week_spanning_calendar_year_boundary_assignment(csv_str_to_df):
    """Test how weeks spanning calendar year boundaries are assigned.

    Weeks are assigned to the year in which they END (Monday timestamp).
    A week ending on Monday Jan 6, 2025 belongs to 2025 even if it
    starts on Dec 31, 2024.
    """
    # Create snapshots with a week spanning Dec 2024 to Jan 2025
    snapshots_csv = """
    snapshots
    2024-12-23__00:00:00
    2024-12-30__00:00:00
    2025-01-06__00:00:00
    2025-01-13__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with peak in the spanning week
    demand_csv = """
    Datetime,Value
    2024-12-23__00:00:00,500
    2024-12-30__00:00:00,1000
    2025-01-06__00:00:00,300
    2025-01-13__00:00:00,400
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for both years to see full behavior
    result_both = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected:
    # 2024: Week ending Dec 30 (peak=1000, assigned to 2024 since it ends in 2024)
    # 2025: Week ending Jan 13 (peak=400 for 2025, Jan 6 only has 300)
    # Note: The week Dec 31-Jan 6 is assigned to 2025 based on its ending date
    expected_csv = """
    snapshots
    2024-12-30__00:00:00
    2025-01-13__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result_both = result_both.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_both, expected)


def test_week_spanning_financial_year_boundary_assignment(csv_str_to_df):
    """Test how weeks spanning financial year boundaries are assigned.

    Weeks are assigned to the financial year in which they END (Monday timestamp).
    A week ending on Monday Jul 1 belongs to the FY that includes that date.
    """
    # Create snapshots with a week spanning June/July boundary
    snapshots_csv = """
    snapshots
    2024-06-24__00:00:00
    2024-07-01__00:00:00
    2024-07-08__00:00:00
    2024-07-15__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data
    demand_csv = """
    Datetime,Value
    2024-06-24__00:00:00,500
    2024-07-01__00:00:00,1000
    2024-07-08__00:00:00,300
    2024-07-15__00:00:00,400
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for both FY2024 and FY2025
    result_both = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="fy",
        demand_data=demand_data,
    )

    # Expected:
    # FY2024: Week ending Jun 24 (demand=500, ends in FY2024)
    # FY2025: Week ending Jul 1 (peak=1000, ends in FY2025 even though it starts in FY2024)
    # Note: Jul 1 is assigned to FY2025 since that's when the week ends
    expected_csv = """
    snapshots
    2024-06-24__00:00:00
    2024-07-01__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result_both = result_both.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_both, expected)


def test_partial_week_at_start_of_timeseries(csv_str_to_df):
    """Test how partial weeks at the start of the time series are handled.

    When the time series starts mid-week, that partial week is EXCLUDED
    from the analysis to ensure consistent comparison between weeks.
    """
    # Create snapshots starting on Thursday Jan 4, 2024 (mid-week)
    # Include full week data for Jan 15 and Jan 22
    snapshots_csv = """
    snapshots
    2024-01-04__00:00:00
    2024-01-05__00:00:00
    2024-01-08__00:00:00
    2024-01-09__00:00:00
    2024-01-10__00:00:00
    2024-01-11__00:00:00
    2024-01-12__00:00:00
    2024-01-13__00:00:00
    2024-01-14__00:00:00
    2024-01-15__00:00:00
    2024-01-16__00:00:00
    2024-01-17__00:00:00
    2024-01-18__00:00:00
    2024-01-19__00:00:00
    2024-01-20__00:00:00
    2024-01-21__00:00:00
    2024-01-22__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with peak in complete week
    demand_csv = """
    Datetime,Value
    2024-01-04__00:00:00,900
    2024-01-05__00:00:00,950
    2024-01-08__00:00:00,700
    2024-01-09__00:00:00,720
    2024-01-10__00:00:00,730
    2024-01-11__00:00:00,740
    2024-01-12__00:00:00,750
    2024-01-13__00:00:00,760
    2024-01-14__00:00:00,770
    2024-01-15__00:00:00,800
    2024-01-16__00:00:00,650
    2024-01-17__00:00:00,640
    2024-01-18__00:00:00,630
    2024-01-19__00:00:00,620
    2024-01-20__00:00:00,610
    2024-01-21__00:00:00,605
    2024-01-22__00:00:00,600
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for peak demand in 2024
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2024,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected:
    # The partial week ending Jan 8 is EXCLUDED (only has data from Jan 4-8)
    # Week ending Jan 15 has peak demand (800)
    expected_csv = """
    snapshots
    2024-01-15__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


def test_partial_week_exclusion_with_later_first_monday(csv_str_to_df):
    """Test partial week exclusion when first Monday is later in the year.

    Uses 2025 where January 1st is a Wednesday, so the first Monday is January 6.
    This clearly demonstrates how partial weeks before the first Monday are excluded.
    """
    # Create snapshots starting Thursday Jan 2, 2025
    snapshots_csv = """
    snapshots
    2025-01-02__00:00:00
    2025-01-03__00:00:00
    2025-01-06__00:00:00
    2025-01-07__00:00:00
    2025-01-08__00:00:00
    2025-01-09__00:00:00
    2025-01-10__00:00:00
    2025-01-11__00:00:00
    2025-01-12__00:00:00
    2025-01-13__00:00:00
    2025-01-14__00:00:00
    2025-01-20__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data
    # Partial week (Jan 2-6): has highest values but should be excluded
    # Complete week (Jan 7-13): has peak among complete weeks
    demand_csv = """
    Datetime,Value
    2025-01-02__00:00:00,1000
    2025-01-03__00:00:00,1100
    2025-01-06__00:00:00,900
    2025-01-07__00:00:00,700
    2025-01-08__00:00:00,720
    2025-01-09__00:00:00,730
    2025-01-10__00:00:00,740
    2025-01-11__00:00:00,750
    2025-01-12__00:00:00,760
    2025-01-13__00:00:00,800
    2025-01-14__00:00:00,650
    2025-01-20__00:00:00,600
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for peak demand in 2025
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2025,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected:
    # The partial week ending Jan 6 is EXCLUDED (only has Thu/Fri/Mon)
    # Week ending Jan 13 has peak demand (800) among complete weeks
    expected_csv = """
    snapshots
    2025-01-07__00:00:00
    2025-01-08__00:00:00
    2025-01-09__00:00:00
    2025-01-10__00:00:00
    2025-01-11__00:00:00
    2025-01-12__00:00:00
    2025-01-13__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_partial_week_at_start_of_financial_year(csv_str_to_df):
    """Test partial week at the start of a financial year.

    When data starts mid-week at the beginning of a financial year,
    that partial week is EXCLUDED from the analysis.
    """
    # Create snapshots starting Thursday July 4, 2024 (mid-week in FY2025)
    snapshots_csv = """
    snapshots
    2024-07-04__00:00:00
    2024-07-05__00:00:00
    2024-07-08__00:00:00
    2024-07-15__00:00:00
    2024-07-22__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data
    demand_csv = """
    Datetime,Value
    2024-07-04__00:00:00,1000
    2024-07-05__00:00:00,1100
    2024-07-08__00:00:00,700
    2024-07-15__00:00:00,800
    2024-07-22__00:00:00,600
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for peak demand in FY2025
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2025,
        end_year=2025,
        year_type="fy",
        demand_data=demand_data,
    )

    # Expected:
    # The partial week ending Jul 8 is EXCLUDED (only has data from Jul 4-8)
    # Week ending Jul 15 has peak demand (800)
    expected_csv = """
    snapshots
    2024-07-15__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


def test_financial_year_single_week_data(csv_str_to_df):
    """Test financial year with data in only one week.

    Edge case where a financial year has data for only one week,
    which should be selected for all requested metrics.
    """
    snapshots_csv = """
    snapshots
    2024-07-01__00:00:00
    2024-07-03__00:00:00
    2024-07-05__00:00:00
    2024-07-08__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    demand_csv = """
    Datetime,Value
    2024-07-01__00:00:00,500
    2024-07-03__00:00:00,600
    2024-07-05__00:00:00,700
    2024-07-08__00:00:00,550
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=[
            "peak-demand",
            "minimum-demand",
            "peak-consumption",
        ],
        snapshots=snapshots,
        start_year=2025,
        end_year=2026,
        year_type="fy",
        demand_data=demand_data,
    )

    # All metrics should select the same (only) week
    expected_csv = """
    snapshots
    2024-07-01__00:00:00
    2024-07-03__00:00:00
    2024-07-05__00:00:00
    2024-07-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)
