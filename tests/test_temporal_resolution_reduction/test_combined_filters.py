"""Tests for combining representative_weeks and named_representative_weeks filters."""

from unittest.mock import Mock, patch

import pandas as pd

from ispypsa.config import TemporalAggregationConfig, TemporalRangeConfig
from ispypsa.translator.temporal_filters import _filter_snapshots


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_combined_representative_and_named_weeks(mock_get_data, csv_str_to_df):
    """Test using both representative_weeks and named_representative_weeks together."""

    # Mock sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    # Create complete snapshots for January 2024
    # Note: representative_weeks filter excludes exact start time (00:00:00)
    snapshots_csv = """
    snapshots
    2024-01-01__00:30:00
    2024-01-01__12:00:00
    2024-01-07__23:00:00
    2024-01-08__00:30:00
    2024-01-08__12:00:00
    2024-01-14__23:00:00
    2024-01-15__00:30:00
    2024-01-15__12:00:00
    2024-01-21__23:00:00
    2024-01-22__00:30:00
    2024-01-22__12:00:00
    2024-01-28__23:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create mock demand data with clear peak in week 3
    demand_data_csv = """
    Datetime,                Value
    2024-01-01__00:30:00,    1000.0
    2024-01-01__12:00:00,    1100.0
    2024-01-07__23:00:00,    1200.0
    2024-01-08__00:30:00,    1300.0
    2024-01-08__12:00:00,    1400.0
    2024-01-14__23:00:00,    1500.0
    2024-01-15__00:30:00,    2000.0
    2024-01-15__12:00:00,    2100.0
    2024-01-21__23:00:00,    2200.0
    2024-01-22__00:30:00,    1000.0
    2024-01-22__12:00:00,    1100.0
    2024-01-28__23:00:00,    1200.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Create config with both representative and named weeks
    temporal_aggregation_config = TemporalAggregationConfig(
        representative_weeks=[1, 2],  # Weeks 1 and 2
        named_representative_weeks=["peak-demand-week"],  # Should be week 3
    )

    temporal_range = TemporalRangeConfig(
        start_year=2024,
        end_year=2024,
    )

    # Call the function
    result = _filter_snapshots(
        year_type="calendar",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_aggregation_config,
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
    )

    # Expected: weeks 1, 2, and 3 (union of both filters)
    expected_csv = """
    snapshots
    2024-01-01__00:30:00
    2024-01-01__12:00:00
    2024-01-07__23:00:00
    2024-01-08__00:30:00
    2024-01-08__12:00:00
    2024-01-14__23:00:00
    2024-01-15__00:30:00
    2024-01-15__12:00:00
    2024-01-21__23:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_combined_filters_with_overlapping_weeks(mock_get_data, csv_str_to_df):
    """Test that duplicate weeks are properly removed when filters overlap."""

    # Mock sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    # Create snapshots for weeks 1-3
    snapshots_csv = """
    snapshots
    2024-01-01__00:30:00
    2024-01-01__12:00:00
    2024-01-08__00:30:00
    2024-01-08__12:00:00
    2024-01-15__00:30:00
    2024-01-15__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create mock demand data with minimum in week 2
    demand_data_csv = """
    Datetime,                Value
    2024-01-01__00:30:00,    1500.0
    2024-01-01__12:00:00,    1600.0
    2024-01-08__00:30:00,    1000.0
    2024-01-08__12:00:00,    1100.0
    2024-01-15__00:30:00,    1700.0
    2024-01-15__12:00:00,    1800.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Create config where both filters select week 2
    temporal_aggregation_config = TemporalAggregationConfig(
        representative_weeks=[2],  # Week 2
        named_representative_weeks=["minimum-demand-week"],  # Also week 2
    )

    temporal_range = TemporalRangeConfig(
        start_year=2024,
        end_year=2024,
    )

    # Call the function
    result = _filter_snapshots(
        year_type="calendar",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_aggregation_config,
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
    )

    # Expected: only week 2 snapshots (no duplicates)
    expected_csv = """
    snapshots
    2024-01-08__00:30:00
    2024-01-08__12:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_combined_filters_financial_year(mock_get_data, csv_str_to_df):
    """Test combined filters work correctly with financial years."""

    # Mock sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    # Create snapshots for FY2024 (July 2023 - June 2024)
    # July 1, 2023 is Saturday, so first Monday is July 3
    snapshots_csv = """
    snapshots
    2023-07-03__00:30:00
    2023-07-03__12:00:00
    2023-07-10__00:30:00
    2023-07-10__12:00:00
    2023-07-17__00:30:00
    2023-07-17__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create mock demand data with peak in week 3
    demand_data_csv = """
    Datetime,                Value
    2023-07-03__00:30:00,    1000.0
    2023-07-03__12:00:00,    1100.0
    2023-07-10__00:30:00,    1200.0
    2023-07-10__12:00:00,    1300.0
    2023-07-17__00:30:00,    2000.0
    2023-07-17__12:00:00,    2100.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Create config with both types of weeks
    temporal_aggregation_config = TemporalAggregationConfig(
        representative_weeks=[1],  # Week 1
        named_representative_weeks=["peak-demand-week"],  # Should be week 3
    )

    temporal_range = TemporalRangeConfig(
        start_year=2024,
        end_year=2024,
    )

    # Call the function
    result = _filter_snapshots(
        year_type="fy",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_aggregation_config,
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
    )

    # Expected: weeks 1 and 3
    expected_csv = """
    snapshots
    2023-07-03__00:30:00
    2023-07-03__12:00:00
    2023-07-17__00:30:00
    2023-07-17__12:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


def test_only_representative_weeks_still_works(csv_str_to_df):
    """Test that using only representative_weeks still works correctly."""

    # Create snapshots with more complete data
    # 2024 starts on a Monday, so week 1 is Jan 1-7, week 2 is Jan 8-14
    snapshots_csv = """
    snapshots
    2024-01-01__01:00:00
    2024-01-01__12:00:00
    2024-01-07__12:00:00
    2024-01-08__01:00:00
    2024-01-08__12:00:00
    2024-01-14__12:00:00
    2024-01-15__01:00:00
    2024-01-15__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create config with only representative weeks
    temporal_aggregation_config = TemporalAggregationConfig(
        representative_weeks=[2],
        named_representative_weeks=None,
    )

    temporal_range = TemporalRangeConfig(
        start_year=2024,
        end_year=2024,
    )

    # Call the function
    result = _filter_snapshots(
        year_type="calendar",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_aggregation_config,
        snapshots=snapshots,
    )

    # Expected: only week 2 snapshots (Jan 8-14)
    expected_csv = """
    snapshots
    2024-01-08__01:00:00
    2024-01-08__12:00:00
    2024-01-14__12:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected.reset_index(drop=True)
    )


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_only_named_weeks_still_works(mock_get_data, csv_str_to_df):
    """Test that using only named_representative_weeks still works correctly."""

    # Mock sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    # Create snapshots
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create mock demand data
    demand_data_csv = """
    Datetime,                Value
    2024-01-01__00:00:00,    1000.0
    2024-01-08__00:00:00,    2000.0
    2024-01-15__00:00:00,    1500.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Create config with only named weeks
    temporal_aggregation_config = TemporalAggregationConfig(
        representative_weeks=None,
        named_representative_weeks=["peak-demand-week"],
    )

    temporal_range = TemporalRangeConfig(
        start_year=2024,
        end_year=2024,
    )

    # Call the function
    result = _filter_snapshots(
        year_type="calendar",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_aggregation_config,
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
    )

    # Expected: week 2 (peak demand)
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected.reset_index(drop=True)
    )


def test_no_filters_returns_original_snapshots(csv_str_to_df):
    """Test that when no filters are specified, original snapshots are returned."""

    # Create snapshots
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create config with no filters
    temporal_aggregation_config = TemporalAggregationConfig(
        representative_weeks=None,
        named_representative_weeks=None,
    )

    temporal_range = TemporalRangeConfig(
        start_year=2024,
        end_year=2024,
    )

    # Call the function
    result = _filter_snapshots(
        year_type="calendar",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_aggregation_config,
        snapshots=snapshots,
    )

    # Expected: original snapshots unchanged
    pd.testing.assert_frame_equal(result, snapshots)
