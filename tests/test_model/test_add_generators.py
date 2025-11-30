from pathlib import Path

import pandas as pd
import pypsa
import pytest
from pandas.testing import assert_series_equal

from ispypsa.pypsa_build.generators import (
    _add_generator_to_network,
    _get_marginal_cost_timeseries,
    _get_trace_data,
)


@pytest.fixture
def mock_network(csv_str_to_df):
    """Create a minimal PyPSA network for testing."""
    network = pypsa.Network()
    network.add("Bus", "test_bus")

    # Create sample trace data
    snapshots_csv = """
    investment_periods,     snapshots
    2024,                   2023-07-01__12:00:00
    2024,                   2023-10-01__12:00:00
    2024,                   2024-01-01__12:00:00
    2024,                   2024-04-01__12:00:00
    2025,                   2024-07-01__12:00:00
    2025,                   2024-10-01__12:00:00
    2025,                   2025-01-01__12:00:00
    2025,                   2025-04-01__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # set network snapshots:
    network.snapshots = snapshots

    return network


@pytest.fixture
def mock_trace_paths(tmp_path, csv_str_to_df):
    """Create temporary directories and mock trace files."""
    # Create directories
    solar_path = tmp_path / "solar_traces"
    wind_path = tmp_path / "wind_traces"
    marginal_costs_path = tmp_path / "marginal_cost_timeseries"

    solar_path.mkdir()
    wind_path.mkdir()
    marginal_costs_path.mkdir()

    # Create sample trace data
    snapshots_csv = """
    investment_periods,     snapshots
    2024,                   2023-07-01__12:00:00
    2024,                   2023-10-01__12:00:00
    2024,                   2024-01-01__12:00:00
    2024,                   2024-04-01__12:00:00
    2025,                   2024-07-01__12:00:00
    2025,                   2024-10-01__12:00:00
    2025,                   2025-01-01__12:00:00
    2025,                   2025-04-01__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Sample trace data for wind/solar - arbitrary numbers
    sample_trace_data = snapshots.copy()
    sample_trace_data["p_max_pu"] = [1, 2, 3, 4, 5, 6, 7, 8]

    # Create sample marginal cost data
    sample_marginal_cost_data = snapshots.copy()
    sample_marginal_cost_data["marginal_cost"] = [50, 49, 48, 47, 46, 45, 44, 43]

    # Save mock files
    sample_trace_data.to_parquet(solar_path / "solar_gen.parquet")
    sample_trace_data.to_parquet(wind_path / "wind_gen.parquet")
    sample_marginal_cost_data.to_parquet(
        marginal_costs_path / "test_gen_marginal_cost.parquet"
    )

    return {
        "solar": solar_path,
        "wind": wind_path,
        "marginal_costs": marginal_costs_path,
    }


def test_add_generator_to_network_static_marginal_cost(mock_network, mock_trace_paths):
    """Test adding a generator with static marginal cost."""
    # Generator definition with static marginal cost
    generator_def = {
        "name": "static_gen",
        "bus": "test_bus",
        "carrier": "Gas",
        "p_nom": 100,
        "marginal_cost": 45.0,  # Static value
        "isp_custom_attribute": "custom_value",  # Should be filtered out
    }

    # Call the function
    _add_generator_to_network(
        generator_def,
        mock_network,
        mock_trace_paths["solar"],
        mock_trace_paths["wind"],
        mock_trace_paths["marginal_costs"],
    )

    # Check the generator was added correctly
    assert "static_gen" in mock_network.generators.index
    assert mock_network.generators.at["static_gen", "marginal_cost"] == 45.0
    assert "isp_custom_attribute" not in mock_network.generators.columns


def test_add_generator_to_network_dynamic_marginal_cost(
    mock_network, mock_trace_paths, csv_str_to_df
):
    """Test adding a generator with dynamic marginal cost."""
    # Generator definition with dynamic marginal cost
    generator_def = {
        "name": "test_gen",
        "bus": "test_bus",
        "carrier": "Gas",
        "p_nom": 100,
        "marginal_cost": "test_gen_marginal_cost",  # String reference to marginal cost file
    }

    # Call the function
    _add_generator_to_network(
        generator_def,
        mock_network,
        mock_trace_paths["solar"],
        mock_trace_paths["wind"],
        mock_trace_paths["marginal_costs"],
    )

    # Check the generator was added correctly
    assert "test_gen" in mock_network.generators.index

    # Check the marginal cost is a Series with the correct values
    expected_values = [50.0, 49.0, 48.0, 47.0, 46.0, 45.0, 44.0, 43.0]

    assert "marginal_cost" in mock_network.generators_t
    assert all(
        mock_network.generators_t.marginal_cost["test_gen"].values == expected_values
    )


def test_add_generator_to_network_with_traces(mock_network, mock_trace_paths):
    """Test adding wind/solar generators with availability traces."""
    # Test for Wind
    wind_gen_def = {
        "name": "wind_gen",
        "bus": "test_bus",
        "carrier": "Wind",
        "p_nom": 100,
        "marginal_cost": 0.0,
    }

    _add_generator_to_network(
        wind_gen_def,
        mock_network,
        mock_trace_paths["solar"],
        mock_trace_paths["wind"],
        mock_trace_paths["marginal_costs"],
    )

    # Test for Solar
    solar_gen_def = {
        "name": "solar_gen",
        "bus": "test_bus",
        "carrier": "Solar",
        "p_nom": 100,
        "marginal_cost": 0.0,
    }

    _add_generator_to_network(
        solar_gen_def,
        mock_network,
        mock_trace_paths["solar"],
        mock_trace_paths["wind"],
        mock_trace_paths["marginal_costs"],
    )

    # Check generators were added with correct p_max_pu values
    assert "wind_gen" in mock_network.generators.index
    assert "solar_gen" in mock_network.generators.index

    # Check the marginal cost is a Series with the correct values
    expected_values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]

    assert all(expected_values == mock_network.generators_t.p_max_pu["wind_gen"].values)
    assert all(
        expected_values == mock_network.generators_t.p_max_pu["solar_gen"].values
    )
