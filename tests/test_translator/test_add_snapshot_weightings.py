from datetime import datetime

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from ispypsa.translator.snapshots import _add_snapshot_weightings


def test_add_snapshot_weightings_two_investment_periods(csv_str_to_df):
    """Test snapshot weightings with two investment periods having different snapshot counts."""

    # Input: snapshots with 2 investment periods
    # Period 2026 has 4 snapshots, period 2028 has 2 snapshots
    # Using 60-minute (1 hour) temporal resolution
    input_csv = """
    investment_periods,  snapshots
    2026,                2026-01-01__01:00:00
    2026,                2026-01-01__02:00:00
    2026,                2026-01-01__03:00:00
    2026,                2026-01-01__04:00:00
    2028,                2028-01-01__01:00:00
    2028,                2028-01-01__02:00:00
    """

    input_df = csv_str_to_df(input_csv, parse_dates=["snapshots"])

    # Call the function
    result = _add_snapshot_weightings(input_df, temporal_resolution_min=60)

    # Expected output
    # For 2026: 4 snapshots -> objective = generators = 8760/4 = 2190.0
    # For 2028: 2 snapshots -> objective = generators = 8760/2 = 4380.0
    # For stores: 60 min / 60 = 1.0 hour for all snapshots
    expected_csv = """
    investment_periods,  snapshots,               objective,  generators,  stores
    2026,                2026-01-01__01:00:00,     2190.0,     2190.0,      1.0
    2026,                2026-01-01__02:00:00,     2190.0,     2190.0,      1.0
    2026,                2026-01-01__03:00:00,     2190.0,     2190.0,      1.0
    2026,                2026-01-01__04:00:00,     2190.0,     2190.0,      1.0
    2028,                2028-01-01__01:00:00,     4380.0,     4380.0,      1.0
    2028,                2028-01-01__02:00:00,     4380.0,     4380.0,      1.0
    """

    expected_df = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    # Assert DataFrames are equal
    assert_frame_equal(
        result.sort_values(["investment_periods", "snapshots"]).reset_index(drop=True),
        expected_df.sort_values(["investment_periods", "snapshots"]).reset_index(
            drop=True
        ),
    )


def test_add_snapshot_weightings_30_min_resolution(csv_str_to_df):
    """Test snapshot weightings with 30-minute temporal resolution."""

    # Input: snapshots with 1 investment period, 30-minute resolution
    # Period 2026 has 6 snapshots
    input_csv = """
    investment_periods,  snapshots
    2026,                2026-01-01__00:30:00
    2026,                2026-01-01__01:00:00
    2026,                2026-01-01__01:30:00
    2026,                2026-01-01__02:00:00
    2026,                2026-01-01__02:30:00
    2026,                2026-01-01__03:00:00
    """

    input_df = csv_str_to_df(input_csv, parse_dates=["snapshots"])

    # Call the function
    result = _add_snapshot_weightings(input_df, temporal_resolution_min=30)

    # Expected output
    # For 2026: 6 snapshots -> objective = generators = 8760/6 = 1460.0
    # For stores: 30 min / 60 = 0.5 hour for all snapshots
    expected_csv = """
    investment_periods,  snapshots,               objective,  generators,  stores
    2026,                2026-01-01__00:30:00,     1460.0,     1460.0,      0.5
    2026,                2026-01-01__01:00:00,     1460.0,     1460.0,      0.5
    2026,                2026-01-01__01:30:00,     1460.0,     1460.0,      0.5
    2026,                2026-01-01__02:00:00,     1460.0,     1460.0,      0.5
    2026,                2026-01-01__02:30:00,     1460.0,     1460.0,      0.5
    2026,                2026-01-01__03:00:00,     1460.0,     1460.0,      0.5
    """

    expected_df = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    # Assert DataFrames are equal
    assert_frame_equal(
        result.sort_values(["investment_periods", "snapshots"]).reset_index(drop=True),
        expected_df.sort_values(["investment_periods", "snapshots"]).reset_index(
            drop=True
        ),
    )


def test_add_snapshot_weightings_240_min_resolution(csv_str_to_df):
    """Test snapshot weightings with 240-minute (4-hour) temporal resolution."""

    # Input: snapshots with 2 investment periods, 240-minute resolution
    # Period 2026 has 3 snapshots, period 2028 has 1 snapshot
    input_csv = """
    investment_periods,  snapshots
    2026,                2026-01-01__04:00:00
    2026,                2026-01-01__08:00:00
    2026,                2026-01-01__12:00:00
    2028,                2028-01-01__04:00:00
    """

    input_df = csv_str_to_df(input_csv, parse_dates=["snapshots"])

    # Call the function
    result = _add_snapshot_weightings(input_df, temporal_resolution_min=240)

    # Expected output
    # For 2026: 3 snapshots -> objective = generators = 8760/3 = 2920.0
    # For 2028: 1 snapshot -> objective = generators = 8760/1 = 8760.0
    # For stores: 240 min / 60 = 4.0 hours for all snapshots
    expected_csv = """
    investment_periods,  snapshots,               objective,  generators,  stores
    2026,                2026-01-01__04:00:00,     2920.0,     2920.0,      4.0
    2026,                2026-01-01__08:00:00,     2920.0,     2920.0,      4.0
    2026,                2026-01-01__12:00:00,     2920.0,     2920.0,      4.0
    2028,                2028-01-01__04:00:00,     8760.0,     8760.0,      4.0
    """

    expected_df = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    # Assert DataFrames are equal
    assert_frame_equal(
        result.sort_values(["investment_periods", "snapshots"]).reset_index(drop=True),
        expected_df.sort_values(["investment_periods", "snapshots"]).reset_index(
            drop=True
        ),
    )


def test_add_snapshot_weightings_single_investment_period(csv_str_to_df):
    """Test snapshot weightings with a single investment period."""

    # Input: snapshots with 1 investment period
    # Period 2030 has 8 snapshots
    input_csv = """
    investment_periods,  snapshots
    2030,                2030-01-01__01:00:00
    2030,                2030-01-01__02:00:00
    2030,                2030-01-01__03:00:00
    2030,                2030-01-01__04:00:00
    2030,                2030-01-01__05:00:00
    2030,                2030-01-01__06:00:00
    2030,                2030-01-01__07:00:00
    2030,                2030-01-01__08:00:00
    """

    input_df = csv_str_to_df(input_csv, parse_dates=["snapshots"])

    # Call the function
    result = _add_snapshot_weightings(input_df, temporal_resolution_min=60)

    # Expected output
    # For 2030: 8 snapshots -> objective = generators = 8760/8 = 1095.0
    # For stores: 60 min / 60 = 1.0 hour for all snapshots
    expected_csv = """
    investment_periods,  snapshots,               objective,  generators,  stores
    2030,                2030-01-01__01:00:00,     1095.0,     1095.0,      1.0
    2030,                2030-01-01__02:00:00,     1095.0,     1095.0,      1.0
    2030,                2030-01-01__03:00:00,     1095.0,     1095.0,      1.0
    2030,                2030-01-01__04:00:00,     1095.0,     1095.0,      1.0
    2030,                2030-01-01__05:00:00,     1095.0,     1095.0,      1.0
    2030,                2030-01-01__06:00:00,     1095.0,     1095.0,      1.0
    2030,                2030-01-01__07:00:00,     1095.0,     1095.0,      1.0
    2030,                2030-01-01__08:00:00,     1095.0,     1095.0,      1.0
    """

    expected_df = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    # Assert DataFrames are equal
    assert_frame_equal(
        result.sort_values(["investment_periods", "snapshots"]).reset_index(drop=True),
        expected_df.sort_values(["investment_periods", "snapshots"]).reset_index(
            drop=True
        ),
    )


def test_add_snapshot_weightings_many_snapshots(csv_str_to_df):
    """Test snapshot weightings with a large number of snapshots (edge case)."""

    # Input: Create a realistic scenario with many snapshots
    # Simulating one week of hourly data for one investment period (168 hours)
    snapshots_list = pd.date_range("2025-01-01 01:00:00", periods=168, freq="h")
    input_df = pd.DataFrame(
        {"investment_periods": [2025] * 168, "snapshots": snapshots_list}
    )

    # Call the function
    result = _add_snapshot_weightings(input_df, temporal_resolution_min=60)

    # Verify the results
    # For 2025: 168 snapshots -> objective = generators = 8760/168 = 52.142857...
    # For stores: 60 min / 60 = 1.0 hour for all snapshots
    assert len(result) == 168
    assert all(result["investment_periods"] == 2025)
    assert all(result["stores"] == 1.0)

    # Check objective and generators are approximately 8760/168
    expected_weight = 8760.0 / 168
    assert all(abs(result["objective"] - expected_weight) < 0.0001)
    assert all(abs(result["generators"] - expected_weight) < 0.0001)

    # Verify all snapshots are present
    assert_frame_equal(
        result[["snapshots"]].reset_index(drop=True),
        input_df[["snapshots"]].reset_index(drop=True),
    )
