from pathlib import Path

import pandas as pd
import pytest

from ispypsa.translator.create_pypsa_friendly_inputs import _filter_and_save_timeseries


def test_filter_and_save_timeseries_demand_traces(tmp_path):
    """Test filtering and saving demand timeseries data."""

    # Create test snapshots with investment periods
    snapshots = pd.DataFrame(
        {
            "snapshots": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",
                    "2025-01-01 01:00:00",
                    "2025-01-01 02:00:00",
                    "2026-01-01 00:00:00",
                    "2026-01-01 01:00:00",
                ]
            ),
            "investment_periods": [2025, 2025, 2025, 2026, 2026],
        }
    )

    # Create test demand timeseries data
    demand_trace1 = pd.DataFrame(
        {
            "Datetime": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",
                    "2025-01-01 01:00:00",
                    "2025-01-01 02:00:00",
                    "2025-01-01 03:00:00",
                    "2026-01-01 00:00:00",
                    "2026-01-01 01:00:00",
                    "2026-01-01 02:00:00",
                ]
            ),
            "Value": [100.0, 110.0, 120.0, 130.0, 200.0, 210.0, 220.0],
        }
    )

    demand_trace2 = pd.DataFrame(
        {
            "Datetime": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",
                    "2025-01-01 01:00:00",
                    "2025-01-01 02:00:00",
                    "2025-01-01 03:00:00",
                    "2026-01-01 00:00:00",
                    "2026-01-01 01:00:00",
                ]
            ),
            "Value": [50.0, 55.0, 60.0, 65.0, 150.0, 155.0],
        }
    )

    # Create dictionary of timeseries data
    timeseries_data = {"NSW": demand_trace1, "VIC": demand_trace2}

    # Call the function
    _filter_and_save_timeseries(
        timeseries_data=timeseries_data,
        snapshots=snapshots,
        output_path=tmp_path,
        trace_type="demand_traces",
    )

    # Check that files were created
    output_dir = tmp_path / "demand_traces"
    assert output_dir.exists()
    assert (output_dir / "NSW.parquet").exists()
    assert (output_dir / "VIC.parquet").exists()

    # Check NSW trace content
    expected_nsw = pd.DataFrame(
        {
            "investment_periods": [2025, 2025, 2025, 2026, 2026],
            "snapshots": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",
                    "2025-01-01 01:00:00",
                    "2025-01-01 02:00:00",
                    "2026-01-01 00:00:00",
                    "2026-01-01 01:00:00",
                ]
            ),
            "p_set": [100.0, 110.0, 120.0, 200.0, 210.0],
        }
    )

    got_nsw = pd.read_parquet(output_dir / "NSW.parquet")
    pd.testing.assert_frame_equal(expected_nsw, got_nsw)

    # Check VIC trace content
    expected_vic = pd.DataFrame(
        {
            "investment_periods": [2025, 2025, 2025, 2026, 2026],
            "snapshots": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",
                    "2025-01-01 01:00:00",
                    "2025-01-01 02:00:00",
                    "2026-01-01 00:00:00",
                    "2026-01-01 01:00:00",
                ]
            ),
            "p_set": [50.0, 55.0, 60.0, 150.0, 155.0],
        }
    )

    got_vic = pd.read_parquet(output_dir / "VIC.parquet")
    pd.testing.assert_frame_equal(expected_vic, got_vic)


def test_filter_and_save_timeseries_solar_traces(tmp_path):
    """Test filtering and saving solar generator timeseries data."""

    # Create test snapshots
    snapshots = pd.DataFrame(
        {
            "snapshots": pd.to_datetime(
                [
                    "2025-06-01 12:00:00",
                    "2025-06-01 13:00:00",
                    "2025-06-01 14:00:00",
                ]
            ),
            "investment_periods": [2025, 2025, 2025],
        }
    )

    # Create test solar timeseries data
    solar_trace = pd.DataFrame(
        {
            "Datetime": pd.to_datetime(
                [
                    "2025-06-01 11:00:00",
                    "2025-06-01 12:00:00",
                    "2025-06-01 13:00:00",
                    "2025-06-01 14:00:00",
                    "2025-06-01 15:00:00",
                ]
            ),
            "Value": [0.2, 0.8, 0.9, 0.7, 0.4],
        }
    )

    # Create dictionary of timeseries data
    timeseries_data = {"Solar_Farm_1": solar_trace}

    # Call the function
    _filter_and_save_timeseries(
        timeseries_data=timeseries_data,
        snapshots=snapshots,
        output_path=tmp_path,
        trace_type="solar_traces",
    )

    # Check that file was created
    output_dir = tmp_path / "solar_traces"
    assert output_dir.exists()
    assert (output_dir / "Solar_Farm_1.parquet").exists()

    # Check content - note that solar uses p_max_pu instead of p_set
    expected_solar = pd.DataFrame(
        {
            "investment_periods": [2025, 2025, 2025],
            "snapshots": pd.to_datetime(
                [
                    "2025-06-01 12:00:00",
                    "2025-06-01 13:00:00",
                    "2025-06-01 14:00:00",
                ]
            ),
            "p_max_pu": [0.8, 0.9, 0.7],
        }
    )

    got_solar = pd.read_parquet(output_dir / "Solar_Farm_1.parquet")
    pd.testing.assert_frame_equal(expected_solar, got_solar)


def test_filter_and_save_timeseries_wind_traces(tmp_path):
    """Test filtering and saving wind generator timeseries data."""

    # Create test snapshots
    snapshots = pd.DataFrame(
        {
            "snapshots": pd.to_datetime(
                [
                    "2025-03-15 00:00:00",
                    "2025-03-15 06:00:00",
                ]
            ),
            "investment_periods": [2025, 2025],
        }
    )

    # Create test wind timeseries data
    wind_trace = pd.DataFrame(
        {
            "Datetime": pd.to_datetime(
                [
                    "2025-03-15 00:00:00",
                    "2025-03-15 06:00:00",
                    "2025-03-15 12:00:00",
                ]
            ),
            "Value": [0.3, 0.7, 0.5],
        }
    )

    # Create dictionary of timeseries data
    timeseries_data = {"Wind_Farm_A": wind_trace}

    # Call the function
    _filter_and_save_timeseries(
        timeseries_data=timeseries_data,
        snapshots=snapshots,
        output_path=tmp_path,
        trace_type="wind_traces",
    )

    # Check content - wind also uses p_max_pu
    expected_wind = pd.DataFrame(
        {
            "investment_periods": [2025, 2025],
            "snapshots": pd.to_datetime(
                [
                    "2025-03-15 00:00:00",
                    "2025-03-15 06:00:00",
                ]
            ),
            "p_max_pu": [0.3, 0.7],
        }
    )

    got_wind = pd.read_parquet(tmp_path / "wind_traces" / "Wind_Farm_A.parquet")
    pd.testing.assert_frame_equal(expected_wind, got_wind)


def test_filter_and_save_timeseries_creates_directories(tmp_path):
    """Test that the function creates necessary directories if they don't exist."""

    # Create minimal test data
    snapshots = pd.DataFrame(
        {
            "snapshots": pd.to_datetime(["2025-01-01 00:00:00"]),
            "investment_periods": [2025],
        }
    )

    trace = pd.DataFrame(
        {"Datetime": pd.to_datetime(["2025-01-01 00:00:00"]), "Value": [100.0]}
    )

    timeseries_data = {"test_node": trace}

    # Ensure output directory doesn't exist
    output_subdir = tmp_path / "new_subdir"
    assert not output_subdir.exists()

    # Call the function with a non-existent subdirectory
    _filter_and_save_timeseries(
        timeseries_data=timeseries_data,
        snapshots=snapshots,
        output_path=output_subdir,
        trace_type="demand_traces",
    )

    # Check that directories were created
    assert output_subdir.exists()
    assert (output_subdir / "demand_traces").exists()
    assert (output_subdir / "demand_traces" / "test_node.parquet").exists()


def test_filter_and_save_timeseries_empty_data(tmp_path):
    """Test handling of empty timeseries data dictionary."""

    snapshots = pd.DataFrame(
        {
            "snapshots": pd.to_datetime(["2025-01-01 00:00:00"]),
            "investment_periods": [2025],
        }
    )

    # Empty timeseries data
    timeseries_data = {}

    # Call the function - should create directory but no files
    _filter_and_save_timeseries(
        timeseries_data=timeseries_data,
        snapshots=snapshots,
        output_path=tmp_path,
        trace_type="demand_traces",
    )

    # Check that directory was created but is empty
    output_dir = tmp_path / "demand_traces"
    assert output_dir.exists()
    assert len(list(output_dir.iterdir())) == 0


def test_filter_and_save_timeseries_misaligned_timestamps(tmp_path):
    """Test that function handles timeseries data with timestamps not in snapshots."""

    # Create snapshots with specific times
    snapshots = pd.DataFrame(
        {
            "snapshots": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",
                    "2025-01-01 06:00:00",
                    "2025-01-01 12:00:00",
                ]
            ),
            "investment_periods": [2025, 2025, 2025],
        }
    )

    # Create trace with some timestamps not in snapshots
    trace = pd.DataFrame(
        {
            "Datetime": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",  # in snapshots
                    "2025-01-01 03:00:00",  # NOT in snapshots
                    "2025-01-01 06:00:00",  # in snapshots
                    "2025-01-01 09:00:00",  # NOT in snapshots
                    "2025-01-01 12:00:00",  # in snapshots
                ]
            ),
            "Value": [100.0, 150.0, 200.0, 250.0, 300.0],
        }
    )

    timeseries_data = {"test_node": trace}

    # Call the function
    _filter_and_save_timeseries(
        timeseries_data=timeseries_data,
        snapshots=snapshots,
        output_path=tmp_path,
        trace_type="demand_traces",
    )

    # Check that only matching timestamps were saved
    expected = pd.DataFrame(
        {
            "investment_periods": [2025, 2025, 2025],
            "snapshots": pd.to_datetime(
                [
                    "2025-01-01 00:00:00",
                    "2025-01-01 06:00:00",
                    "2025-01-01 12:00:00",
                ]
            ),
            "p_set": [100.0, 200.0, 300.0],
        }
    )

    got = pd.read_parquet(tmp_path / "demand_traces" / "test_node.parquet")
    pd.testing.assert_frame_equal(expected, got)
