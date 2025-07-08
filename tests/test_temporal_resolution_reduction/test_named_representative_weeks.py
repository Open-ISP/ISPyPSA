"""
Test suite for named representative weeks filtering functionality.

This module tests the ability to filter time series snapshots based on named week criteria
such as peak demand weeks, minimum demand weeks, and residual demand weeks. The tests are
organized into the following categories:

1. CONFIGURATION AND CONSTANTS TESTS
   - test_week_metrics_constants: Verifies WEEK_METRICS dictionary structure

2. UNIT TESTS FOR INTERNAL FUNCTIONS
   - test_calculate_week_metrics_vectorized_calendar_year: Tests vectorized week metric calculation
   - test_filter_snapshots_by_named_weeks_basic: Tests core implementation with pre-loaded data
   - test_filter_multiple_criteria: Tests filtering with multiple criteria using implementation
   - test_residual_demand_calculation: Tests residual demand calculation in implementation
   - test_empty_data_handling: Tests edge case of empty data

3. INTEGRATION TESTS (SINGLE YEAR)
   - test_filter_peak_demand_week_single_year: Full pipeline test for peak demand week
   - test_filter_minimum_demand_week: Full pipeline test for minimum demand week
   - test_filter_residual_peak_demand_week: Full pipeline with renewable generation

4. INTEGRATION TESTS (MULTIPLE YEARS)
   - test_filter_peak_demand_week_multiple_years: Tests year-by-year peak selection
   - test_financial_year_filtering: Tests financial year handling

5. INTEGRATION TESTS (MULTIPLE CRITERIA)
   - test_filter_multiple_named_weeks: Tests combining peak and minimum demand weeks

6. VALIDATION AND ERROR HANDLING TESTS
   - test_unsupported_named_week_raises_error: Tests error for invalid week types
   - test_residual_demand_requires_generators: Tests error when generators missing

7. WRAPPER FUNCTION TEST
   - test_compatible_wrapper: Tests the main entry point function

TEST OVERLAPS:
- test_filter_peak_demand_week_single_year and test_filter_snapshots_by_named_weeks_basic
  both test peak demand week selection, but at different levels (integration vs unit)
- test_filter_multiple_named_weeks and test_filter_multiple_criteria both test multiple
  criteria, but at different levels

POTENTIAL GAPS IN COVERAGE:
- No tests for "peak-consumption-week" (using mean instead of total)
- No tests for "residual-minimum-demand-week" or "residual-peak-consumption-week"
- No tests for edge cases like weeks spanning year boundaries
- No tests for different regional_granularity options
- No tests for multiple renewable generator types (Wind + Solar)
- No tests for leap years or partial weeks
- No performance/stress tests with large datasets
"""

from unittest.mock import patch

import pandas as pd
import pytest

from ispypsa.translator.named_weeks_filter import (
    WEEK_METRICS,
    _filter_snapshots_by_named_weeks_impl,
    _filter_snapshots_for_named_weeks,
    calculate_week_metrics_vectorized,
)

# =============================================================================
# 1. CONFIGURATION AND CONSTANTS TESTS
# =============================================================================


def test_week_metrics_constants():
    """Test that week metrics are properly defined."""
    assert "peak-demand-week" in WEEK_METRICS
    assert WEEK_METRICS["peak-demand-week"] == ("total", "max")
    assert WEEK_METRICS["minimum-demand-week"] == ("total", "min")
    assert WEEK_METRICS["peak-consumption-week"] == ("mean", "max")
    assert WEEK_METRICS["residual-peak-demand-week"] == ("total", "max")
    assert WEEK_METRICS["residual-minimum-demand-week"] == ("total", "min")
    assert WEEK_METRICS["residual-peak-consumption-week"] == ("mean", "max")


# =============================================================================
# 2. UNIT TESTS FOR INTERNAL FUNCTIONS
# =============================================================================


def test_calculate_week_metrics_vectorized_calendar_year(csv_str_to_df):
    """Test vectorized week metrics calculation."""
    trace_data_csv = """
    Datetime,                Value
    2024-01-01__00:00:00,    100.0
    2024-01-01__12:00:00,    150.0
    2024-01-08__00:00:00,    200.0
    2024-01-08__12:00:00,    250.0
    2024-01-15__00:00:00,    300.0
    2024-01-15__12:00:00,    350.0
    """
    trace_data = csv_str_to_df(trace_data_csv, parse_dates=["Datetime"])

    result = calculate_week_metrics_vectorized(trace_data, "calendar", 2024, 2024)

    # Check that we got 3 weeks
    assert len(result) == 3
    assert list(result["week_of_year"]) == [1, 2, 3]

    # Check totals
    assert result.loc[result["week_of_year"] == 1, "total"].iloc[0] == 250.0
    assert result.loc[result["week_of_year"] == 2, "total"].iloc[0] == 450.0
    assert result.loc[result["week_of_year"] == 3, "total"].iloc[0] == 650.0

    # Check means
    assert result.loc[result["week_of_year"] == 1, "mean"].iloc[0] == 125.0
    assert result.loc[result["week_of_year"] == 2, "mean"].iloc[0] == 225.0


def test_filter_snapshots_by_named_weeks_basic(csv_str_to_df):
    """Test basic filtering with peak demand week."""
    # Create snapshots
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

    # Create demand data with peak in week 2
    demand_csv = """
    Datetime,                Value
    2024-01-01__00:30:00,    100.0
    2024-01-01__12:00:00,    150.0
    2024-01-08__00:30:00,    300.0
    2024-01-08__12:00:00,    350.0
    2024-01-15__00:30:00,    200.0
    2024-01-15__12:00:00,    250.0
    """
    demand_traces = csv_str_to_df(demand_csv, parse_dates=["Datetime"])

    result = _filter_snapshots_by_named_weeks_impl(
        named_weeks=["peak-demand-week"],
        snapshots=snapshots,
        demand_traces=demand_traces,
        year_type="calendar",
        start_year=2024,
        end_year=2024,
    )

    # Should return week 2 snapshots
    expected_csv = """
    snapshots
    2024-01-08__00:30:00
    2024-01-08__12:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


def test_filter_multiple_criteria(csv_str_to_df):
    """Test filtering with multiple named weeks."""
    # Create snapshots
    snapshots_csv = """
    snapshots
    2024-01-01__00:30:00
    2024-01-08__00:30:00
    2024-01-15__00:30:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create demand data
    demand_csv = """
    Datetime,                Value
    2024-01-01__00:30:00,    50.0
    2024-01-08__00:30:00,    300.0
    2024-01-15__00:30:00,    150.0
    """
    demand_traces = csv_str_to_df(demand_csv, parse_dates=["Datetime"])

    result = _filter_snapshots_by_named_weeks_impl(
        named_weeks=["peak-demand-week", "minimum-demand-week"],
        snapshots=snapshots,
        demand_traces=demand_traces,
        year_type="calendar",
        start_year=2024,
        end_year=2024,
    )

    # Should return weeks 1 (minimum) and 2 (peak)
    expected_csv = """
    snapshots
    2024-01-01__00:30:00
    2024-01-08__00:30:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


def test_residual_demand_calculation(csv_str_to_df):
    """Test filtering with residual demand calculations."""
    # Create snapshots
    snapshots_csv = """
    snapshots
    2024-01-01__00:30:00
    2024-01-08__00:30:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create demand and renewable data
    demand_csv = """
    Datetime,                Value
    2024-01-01__00:30:00,    1000.0
    2024-01-08__00:30:00,    1000.0
    """
    demand_traces = csv_str_to_df(demand_csv, parse_dates=["Datetime"])

    # High renewable in week 1, low in week 2
    renewable_csv = """
    Datetime,                Value
    2024-01-01__00:30:00,    800.0
    2024-01-08__00:30:00,    100.0
    """
    renewable_traces = csv_str_to_df(renewable_csv, parse_dates=["Datetime"])

    result = _filter_snapshots_by_named_weeks_impl(
        named_weeks=["residual-peak-demand-week"],
        snapshots=snapshots,
        demand_traces=demand_traces,
        year_type="calendar",
        start_year=2024,
        end_year=2024,
        renewable_traces=renewable_traces,
    )

    # Week 2 should have higher residual demand (1000-100 vs 1000-800)
    expected_csv = """
    snapshots
    2024-01-08__00:30:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


def test_empty_data_handling():
    """Test handling of empty data."""
    empty_snapshots = pd.DataFrame({"snapshots": []})
    empty_demand = pd.DataFrame({"Datetime": [], "Value": []})

    result = _filter_snapshots_by_named_weeks_impl(
        named_weeks=["peak-demand-week"],
        snapshots=empty_snapshots,
        demand_traces=empty_demand,
        year_type="calendar",
        start_year=2024,
        end_year=2024,
    )

    assert result.empty
    assert "snapshots" in result.columns


# =============================================================================
# 3. INTEGRATION TESTS (SINGLE YEAR)
# =============================================================================


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_filter_peak_demand_week_single_year(mock_get_data, csv_str_to_df):
    """Test filtering for peak demand week in a single year."""
    # Mock sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    # Create complete snapshots for January 2024
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-01__12:00:00
    2024-01-07__23:00:00
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    2024-01-14__23:00:00
    2024-01-15__00:00:00
    2024-01-15__12:00:00
    2024-01-21__23:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create mock demand data with clear peak in week 2
    demand_data_csv = """
    Datetime,                Value
    2024-01-01__00:00:00,    1000.0
    2024-01-01__12:00:00,    1100.0
    2024-01-07__23:00:00,    1200.0
    2024-01-08__00:00:00,    2000.0
    2024-01-08__12:00:00,    2100.0
    2024-01-14__23:00:00,    2200.0
    2024-01-15__00:00:00,    1000.0
    2024-01-15__12:00:00,    1100.0
    2024-01-21__23:00:00,    1200.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Call the function
    result = _filter_snapshots_for_named_weeks(
        named_weeks=["peak-demand-week"],
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
        year_type="calendar",
        start_year=2024,
        end_year=2024,
    )

    # Expected: only week 2 snapshots (Jan 8-14)
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    2024-01-14__23:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(
        result.sort_values("snapshots").reset_index(drop=True), expected
    )


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_filter_peak_demand_week_multiple_years(mock_get_data, csv_str_to_df):
    """Test filtering for peak demand week across multiple years."""
    # Mock sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    # Create snapshots for 2024 and 2025
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    2024-01-15__00:00:00
    2024-01-15__12:00:00
    2025-01-06__00:00:00
    2025-01-13__00:00:00
    2025-01-13__12:00:00
    2025-01-20__00:00:00
    2025-01-20__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create mock demand data with different peaks in each year
    demand_data_csv = """
    Datetime,                Value
    2024-01-01__00:00:00,    1000.0
    2024-01-08__00:00:00,    2000.0
    2024-01-08__12:00:00,    2100.0
    2024-01-15__00:00:00,    1000.0
    2024-01-15__12:00:00,    1100.0
    2025-01-06__00:00:00,    1000.0
    2025-01-13__00:00:00,    1200.0
    2025-01-13__12:00:00,    1300.0
    2025-01-20__00:00:00,    2500.0
    2025-01-20__12:00:00,    2600.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Call the function
    result = _filter_snapshots_for_named_weeks(
        named_weeks=["peak-demand-week"],
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019, 2025: 2020},
        year_type="calendar",
        start_year=2024,
        end_year=2025,
    )

    # Expected: week 2 from 2024 and week 3 from 2025
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    2025-01-20__00:00:00
    2025-01-20__12:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(
        result.sort_values("snapshots").reset_index(drop=True), expected
    )


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_filter_minimum_demand_week(mock_get_data, csv_str_to_df):
    """Test filtering for minimum demand week."""
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
    2024-01-15__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create mock demand data with minimum total in week 1
    demand_data_csv = """
    Datetime,                Value
    2024-01-01__00:00:00,    1000.0
    2024-01-08__00:00:00,    1100.0
    2024-01-15__00:00:00,    500.0
    2024-01-15__12:00:00,    600.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Call the function
    result = _filter_snapshots_for_named_weeks(
        named_weeks=["minimum-demand-week"],
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
        year_type="calendar",
        start_year=2024,
        end_year=2024,
    )

    # Expected: week 1 snapshots (minimum total demand)
    expected_csv = """
    snapshots
    2024-01-01__00:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_filter_residual_peak_demand_week(mock_get_data, csv_str_to_df):
    """Test filtering for residual peak demand week."""
    # Mock sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    # Mock existing generators with solar
    existing_generators_csv = """
    duid,     technology_type_id,  reg_cap
    SOLAR1,   Solar,               1000.0
    """
    existing_generators = csv_str_to_df(existing_generators_csv)

    # Create snapshots
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-01__12:00:00
    2024-01-08__00:00:00
    2024-01-08__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create mock demand data
    demand_data_csv = """
    Datetime,                Value
    2024-01-01__00:00:00,    2000.0
    2024-01-01__12:00:00,    2500.0
    2024-01-08__00:00:00,    1800.0
    2024-01-08__12:00:00,    2200.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Mock solar generation data - high in week 2, low in week 1
    solar_data_csv = """
    Datetime,                Value
    2024-01-01__00:00:00,    0.0
    2024-01-01__12:00:00,    0.2
    2024-01-08__00:00:00,    0.0
    2024-01-08__12:00:00,    0.8
    """
    solar_data = csv_str_to_df(solar_data_csv, parse_dates=["Datetime"])

    # Configure mocks
    mock_get_data.demand_multiple_reference_years.return_value = demand_data
    mock_get_data.solar_project_multiple_reference_years.return_value = solar_data

    # Call the function
    result = _filter_snapshots_for_named_weeks(
        named_weeks=["residual-peak-demand-week"],
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
        year_type="calendar",
        start_year=2024,
        end_year=2024,
        existing_generators=existing_generators,
    )

    # Week 1 should have higher residual demand (lower solar generation)
    expected_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-01__12:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(
        result.sort_values("snapshots").reset_index(drop=True), expected
    )


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_filter_multiple_named_weeks(mock_get_data, csv_str_to_df):
    """Test filtering for multiple named weeks."""
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
    2024-01-01__00:00:00,    500.0
    2024-01-08__00:00:00,    2000.0
    2024-01-15__00:00:00,    1000.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Call the function with multiple week types
    result = _filter_snapshots_for_named_weeks(
        named_weeks=["peak-demand-week", "minimum-demand-week"],
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
        year_type="calendar",
        start_year=2024,
        end_year=2024,
    )

    # Should return both week 1 (minimum) and week 2 (peak)
    expected_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(
        result.sort_values("snapshots").reset_index(drop=True), expected
    )


def test_unsupported_named_week_raises_error(csv_str_to_df):
    """Test that unsupported named week raises ValueError."""
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    with pytest.raises(ValueError, match="Unsupported named weeks: {'invalid-week'}"):
        _filter_snapshots_for_named_weeks(
            named_weeks=["invalid-week"],
            snapshots=snapshots,
            isp_sub_regions=isp_sub_regions,
            trace_data_path="/fake/path",
            scenario="Step Change",
            regional_granularity="sub_regions",
            reference_year_mapping={2024: 2019},
            year_type="calendar",
            start_year=2024,
            end_year=2024,
        )


def test_residual_demand_requires_generators(csv_str_to_df):
    """Test that residual demand calculations require existing_generators."""
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    with pytest.raises(ValueError, match="existing_generators DataFrame required"):
        _filter_snapshots_for_named_weeks(
            named_weeks=["residual-peak-demand-week"],
            snapshots=snapshots,
            isp_sub_regions=isp_sub_regions,
            trace_data_path="/fake/path",
            scenario="Step Change",
            regional_granularity="sub_regions",
            reference_year_mapping={2024: 2019},
            year_type="calendar",
            start_year=2024,
            end_year=2024,
            existing_generators=None,
        )


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_financial_year_filtering(mock_get_data, csv_str_to_df):
    """Test filtering for financial year."""
    # Mock sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    # Create snapshots for FY2024 (July 2023 - June 2024)
    snapshots_csv = """
    snapshots
    2023-07-03__00:00:00
    2023-07-10__00:00:00
    2023-12-25__00:00:00
    2024-01-01__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Create mock demand data with peak in week 2
    demand_data_csv = """
    Datetime,                Value
    2023-07-03__00:00:00,    1000.0
    2023-07-10__00:00:00,    2000.0
    2023-12-25__00:00:00,    1100.0
    2024-01-01__00:00:00,    1200.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Call the function
    result = _filter_snapshots_for_named_weeks(
        named_weeks=["peak-demand-week"],
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
        year_type="fy",
        start_year=2024,
        end_year=2024,
    )

    # Expected: week 2 of FY2024
    expected_csv = """
    snapshots
    2023-07-10__00:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


# =============================================================================
# 6. VALIDATION AND ERROR HANDLING TESTS
# =============================================================================


def test_unsupported_named_week_raises_error(csv_str_to_df):
    """Test that unsupported named week raises ValueError."""
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    with pytest.raises(ValueError, match="Unsupported named weeks: {'invalid-week'}"):
        _filter_snapshots_for_named_weeks(
            named_weeks=["invalid-week"],
            snapshots=snapshots,
            isp_sub_regions=isp_sub_regions,
            trace_data_path="/fake/path",
            scenario="Step Change",
            regional_granularity="sub_regions",
            reference_year_mapping={2024: 2019},
            year_type="calendar",
            start_year=2024,
            end_year=2024,
        )


def test_residual_demand_requires_generators(csv_str_to_df):
    """Test that residual demand calculations require existing_generators."""
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    with pytest.raises(ValueError, match="existing_generators DataFrame required"):
        _filter_snapshots_for_named_weeks(
            named_weeks=["residual-peak-demand-week"],
            snapshots=snapshots,
            isp_sub_regions=isp_sub_regions,
            trace_data_path="/fake/path",
            scenario="Step Change",
            regional_granularity="sub_regions",
            reference_year_mapping={2024: 2019},
            year_type="calendar",
            start_year=2024,
            end_year=2024,
            existing_generators=None,
        )


# =============================================================================
# 7. WRAPPER FUNCTION TEST
# =============================================================================


@patch("ispypsa.translator.named_weeks_filter.get_data")
def test_compatible_wrapper(mock_get_data, csv_str_to_df):
    """Test the main entry point function."""
    # Mock sub_regions
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    REGION1,            NSW1
    """
    isp_sub_regions = csv_str_to_df(sub_regions_csv)

    # Mock snapshots and demand data
    snapshots_csv = """
    snapshots
    2024-01-01__00:30:00
    2024-01-08__00:30:00
    """
    snapshots = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    demand_data_csv = """
    Datetime,                Value
    2024-01-01__00:30:00,    100.0
    2024-01-08__00:30:00,    200.0
    """
    demand_data = csv_str_to_df(demand_data_csv, parse_dates=["Datetime"])

    # Configure mock
    mock_get_data.demand_multiple_reference_years.return_value = demand_data

    # Test the wrapper
    result = _filter_snapshots_for_named_weeks(
        named_weeks=["peak-demand-week"],
        snapshots=snapshots,
        isp_sub_regions=isp_sub_regions,
        trace_data_path="/fake/path",
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2024: 2019},
        year_type="calendar",
        start_year=2024,
        end_year=2024,
    )

    # Should return week 2 (peak)
    expected_csv = """
    snapshots
    2024-01-08__00:30:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)
