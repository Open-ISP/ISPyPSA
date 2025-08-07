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

import pandas as pd

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
    2024-07-03__00:00:00
    2024-07-05__00:00:00
    2024-07-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_calendar_year_partial_weeks_excluded(csv_str_to_df):
    """Test that weeks partially within a calendar year are excluded.

    This test verifies that weeks spanning year boundaries (Dec/Jan) are
    not considered when selecting representative weeks. A week that starts
    in December 2023 and ends in January 2024 should not be included in
    either year's analysis.
    """
    # Create snapshots around year boundaries
    snapshots_csv = """
    snapshots
    2023-12-25__00:00:00
    2023-12-28__12:00:00
    2023-12-31__00:00:00
    2024-01-01__00:00:00
    2024-01-03__00:00:00
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    2024-12-23__00:00:00
    2024-12-26__12:00:00
    2024-12-30__00:00:00
    2025-01-06__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with peaks in partial weeks and full weeks
    demand_csv = """
    Datetime,Value
    2023-12-25__00:00:00,800
    2023-12-28__12:00:00,900
    2023-12-31__00:00:00,1500
    2024-01-01__00:00:00,1600
    2024-01-03__00:00:00,1000
    2024-01-08__00:00:00,700
    2024-01-15__00:00:00,600
    2024-12-23__00:00:00,750
    2024-12-26__12:00:00,850
    2024-12-30__00:00:00,1300
    2025-01-06__00:00:00,500
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for peak demand weeks in 2024
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2025,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected: Despite high demand values in the partial weeks (1200 on Jan 1 1600),
    # these weeks should be excluded. The peak (1300) is on Dec 30 2024 which is in a full week.
    # Week ending Dec 30 2024 is fully within 2024 (Dec 23-29).
    expected_csv = """
    snapshots
    2024-12-26__12:00:00
    2024-12-30__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_financial_year_partial_weeks_excluded(csv_str_to_df):
    """Test that weeks partially within a financial year are excluded.

    This test verifies that weeks spanning financial year boundaries (Jun/Jul)
    are not considered when selecting representative weeks.
    """
    # Create snapshots around FY boundaries
    snapshots_csv = """
    snapshots
    2023-06-26__00:00:00
    2023-06-29__12:00:00
    2023-07-01__00:00:00
    2023-07-03__00:00:00
    2023-07-10__00:00:00
    2023-07-17__00:00:00
    2024-06-24__00:00:00
    2024-06-27__12:00:00
    2024-06-30__00:00:00
    2024-07-01__00:00:00
    2024-07-08__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with peaks in partial weeks
    demand_csv = """
    Datetime,Value
    2023-06-26__00:00:00,800
    2023-06-29__12:00:00,900
    2023-07-01__00:00:00,1600
    2023-07-03__00:00:00,1400
    2023-07-10__00:00:00,700
    2023-07-17__00:00:00,600
    2024-06-24__00:00:00,850
    2024-06-27__12:00:00,950
    2024-06-30__00:00:00,1500
    2024-07-01__00:00:00,1100
    2024-07-08__00:00:00,500
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Filter for peak demand weeks in FY2024 (July 2023 - June 2024)
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2024,
        end_year=2024,  # Only FY2024, not FY2025
        year_type="fy",
        demand_data=demand_data,
    )

    # Expected: Despite high demand in partial week (1600),
    # these should be excluded. Peak (1500) is in week ending July 1, 2024.
    # This week (June 24-30, 2024) is fully within FY2024.
    # Snapshots from this week are those > June 24 00:00:00 and <= July 1 00:00:00
    expected_csv = """
    snapshots
    2024-06-27__12:00:00
    2024-06-30__00:00:00
    2024-07-01__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_calendar_years_partial_weeks_excluded(csv_str_to_df):
    """Test that weeks partially within calendar years are excluded.

    This test verifies that partial weeks are consistently excluded
    across multiple calendar years. A week that spans year boundaries
    (Dec/Jan) should not be considered for either year.
    """
    # Create data spanning 2022-2024 with partial weeks at each boundary
    snapshots_csv = """
    snapshots
    2021-12-27__00:00:00
    2021-12-30__00:00:00
    2022-01-03__00:00:00
    2022-01-10__00:00:00
    2022-07-04__00:00:00
    2022-12-26__00:00:00
    2022-12-29__00:00:00
    2023-01-02__00:00:00
    2023-01-09__00:00:00
    2023-07-03__00:00:00
    2023-12-25__00:00:00
    2023-12-28__00:00:00
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with highest values in partial weeks
    demand_csv = """
    Datetime,Value
    2021-12-27__00:00:00,2000
    2021-12-30__00:00:00,2100
    2022-01-03__00:00:00,1900
    2022-01-10__00:00:00,800
    2022-07-04__00:00:00,700
    2022-12-26__00:00:00,2200
    2022-12-29__00:00:00,2300
    2023-01-02__00:00:00,2150
    2023-01-09__00:00:00,900
    2023-07-03__00:00:00,750
    2023-12-25__00:00:00,2400
    2023-12-28__00:00:00,2500
    2024-01-01__00:00:00,2350
    2024-01-08__00:00:00,1000
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Test calendar years
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2022,
        end_year=2024,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected: Only full weeks should be considered
    # 2022: Week ending Dec 26 has peak value 2200
    # 2023: Week ending Jan 1 2024 (Dec 25-31, 2023) has peak value 2500
    expected_csv = """
    snapshots
    2022-12-26__00:00:00
    2023-12-28__00:00:00
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_financial_years_partial_weeks_excluded(csv_str_to_df):
    """Test that weeks partially within financial years are excluded.

    This test verifies that partial weeks are consistently excluded
    across multiple financial years. A week that spans financial year
    boundaries (Jun/Jul) should not be considered for either year.
    """
    # Create data spanning FY2022-FY2024 with partial weeks at each FY boundary
    snapshots_csv = """
    snapshots
    2021-06-28__00:00:00
    2021-06-30__00:00:00
    2021-07-01__00:00:00
    2021-07-05__00:00:00
    2021-07-12__00:00:00
    2022-01-10__00:00:00
    2022-06-27__00:00:00
    2022-06-29__00:00:00
    2022-07-01__00:00:00
    2022-07-04__00:00:00
    2022-07-11__00:00:00
    2023-01-09__00:00:00
    2023-06-26__00:00:00
    2023-06-28__00:00:00
    2023-07-03__00:00:00
    2023-07-10__00:00:00
    2024-01-08__00:00:00
    2024-06-24__00:00:00
    2024-06-27__00:00:00
    2024-07-01__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data with highest values in partial weeks to test exclusion
    demand_csv = """
    Datetime,Value
    2021-06-28__00:00:00,2100
    2021-06-30__00:00:00,2200
    2021-07-01__00:00:00,2000
    2021-07-05__00:00:00,800
    2021-07-12__00:00:00,700
    2022-01-10__00:00:00,900
    2022-06-27__00:00:00,2300
    2022-06-29__00:00:00,2400
    2022-07-01__00:00:00,3000
    2022-07-04__00:00:00,850
    2022-07-11__00:00:00,750
    2023-01-09__00:00:00,950
    2023-06-26__00:00:00,2500
    2023-06-28__00:00:00,2600
    2023-07-03__00:00:00,3000
    2023-07-10__00:00:00,800
    2024-01-08__00:00:00,1000
    2024-06-24__00:00:00,2700
    2024-06-27__00:00:00,2800
    2024-07-01__00:00:00,2650
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    # Test financial years FY2022-FY2024
    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2022,
        end_year=2024,
        year_type="fy",
        demand_data=demand_data,
    )

    # Expected: Only full weeks within each FY should be considered
    # FY2022 (Jul 2021 - Jun 2022): Peak in week ending Jun 27, 2022 (value 2400)
    #   - This week (Jun 20-26) is fully within FY2022
    # FY2023 (Jul 2022 - Jun 2023): Peak in week ending Jun 26, 2023 (value 2600)
    #   - This week (Jun 19-25) is fully within FY2023
    # FY2024 (Jul 2023 - Jun 2024): Peak in week ending Jul 1, 2024 (value 2800)
    #   - This week (Jun 24-30) is fully within FY2024 (ends June 30)
    #   - July 1 00:00:00 marks the END of this week
    # Note: Partial weeks spanning FY boundaries are excluded
    expected_csv = """
    snapshots
    2022-06-27__00:00:00
    2023-06-26__00:00:00
    2024-06-27__00:00:00
    2024-07-01__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_year_starting_on_monday(csv_str_to_df):
    """Test edge case where year starts on Monday.

    When January 1 is a Monday, the first week is complete and should be included.
    2024 is such a year.
    """
    # Create snapshots for 2024 where Jan 1 is a Monday
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-01__12:00:00
    2024-01-04__00:00:00
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data
    demand_csv = """
    Datetime,Value
    2024-01-01__00:00:00,1000
    2024-01-01__12:00:00,1100
    2024-01-04__00:00:00,900
    2024-01-08__00:00:00,800
    2024-01-15__00:00:00,700
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

    # Expected: First week (ending Jan 8) has peak demand
    expected_csv = """
    snapshots
    2024-01-01__12:00:00
    2024-01-04__00:00:00
    2024-01-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_year_ending_on_sunday(csv_str_to_df):
    """Test edge case where year ends on Sunday.

    When December 31 is a Sunday, the last week is complete and should be included.
    2023 is such a year.
    """
    # Create snapshots for end of 2023 where Dec 31 is a Sunday
    snapshots_csv = """
    snapshots
    2023-12-18__00:00:00
    2023-12-25__00:00:00
    2023-12-27__00:00:00
    2023-12-29__00:00:00
    2023-12-31__00:00:00
    2023-12-31__23:00:00
    2024-01-01__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data
    demand_csv = """
    Datetime,Value
    2023-12-18__00:00:00,600
    2023-12-25__00:00:00,700
    2023-12-27__00:00:00,800
    2023-12-29__00:00:00,900
    2023-12-31__00:00:00,1000
    2023-12-31__23:00:00,1000
    2024-01-01__00:00:00,1100
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    result = _filter_snapshots_for_named_representative_weeks(
        named_representative_weeks=["peak-demand"],
        snapshots=snapshots,
        start_year=2023,
        end_year=2024,
        year_type="calendar",
        demand_data=demand_data,
    )

    # Expected: Last week (Dec 25-31) has peak demand and is fully within 2023
    expected_csv = """
    snapshots
    2023-12-27__00:00:00
    2023-12-29__00:00:00
    2023-12-31__00:00:00
    2023-12-31__23:00:00
    2024-01-01__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    result = result.sort_values("snapshots").reset_index(drop=True)
    expected = expected.sort_values("snapshots").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)
