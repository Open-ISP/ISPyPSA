import tempfile
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from ispypsa.plotting.plot import (
    create_capacity_expansion_plot_suite,
    create_operational_plot_suite,
    flatten_dict_with_file_paths_as_keys,
    save_plots,
)


def test_flatten_dict_with_file_paths_as_keys_single_level():
    """Test flattening a single level nested dictionary."""
    nested_dict = {
        "transmission": {
            "flows": {"plot": "fig1", "data": "df1"},
            "capacity": {"plot": "fig2", "data": "df2"},
        }
    }

    result = flatten_dict_with_file_paths_as_keys(nested_dict)

    expected_keys = [
        Path("transmission/flows.html"),
        Path("transmission/capacity.html"),
    ]

    assert len(result) == 2
    for key in expected_keys:
        assert key in result

    assert result[Path("transmission/flows.html")] == {"plot": "fig1", "data": "df1"}
    assert result[Path("transmission/capacity.html")] == {"plot": "fig2", "data": "df2"}


def test_flatten_dict_with_file_paths_as_keys_deeply_nested():
    """Test flattening a deeply nested dictionary."""
    nested_dict = {
        "dispatch": {
            "regional": {
                "NSW1": {
                    "2030": {
                        "week1": {"plot": "fig1", "data": "df1"},
                        "week2": {"plot": "fig2", "data": "df2"},
                    }
                }
            }
        }
    }

    result = flatten_dict_with_file_paths_as_keys(nested_dict)

    expected_keys = [
        Path("dispatch/regional/NSW1/2030/week1.html"),
        Path("dispatch/regional/NSW1/2030/week2.html"),
    ]

    assert len(result) == 2
    for key in expected_keys:
        assert key in result

    assert result[Path("dispatch/regional/NSW1/2030/week1.html")] == {
        "plot": "fig1",
        "data": "df1",
    }


def test_flatten_dict_with_file_paths_as_keys_multiple_branches():
    """Test flattening a dictionary with multiple branches."""
    nested_dict = {
        "transmission": {
            "flows": {"plot": "fig1", "data": "df1"},
        },
        "dispatch": {
            "regional": {"plot": "fig2", "data": "df2"},
            "sub_regional": {"plot": "fig3", "data": "df3"},
        },
    }

    result = flatten_dict_with_file_paths_as_keys(nested_dict)

    expected_keys = [
        Path("transmission/flows.html"),
        Path("dispatch/regional.html"),
        Path("dispatch/sub_regional.html"),
    ]

    assert len(result) == 3
    for key in expected_keys:
        assert key in result


def test_flatten_dict_with_file_paths_as_keys_empty_dict():
    """Test flattening an empty dictionary."""
    result = flatten_dict_with_file_paths_as_keys({})

    assert result == {}


def test_create_capacity_expansion_plot_suite(csv_str_to_df):
    """Test create_capacity_expansion_plot_suite with minimal representative data."""

    # 1. Setup Input Data using csv_str_to_df fixture

    # Mapping
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    R1,            S1,                REZ1
    R2,            S2,
    """

    # Transmission Expansion (Capacity)
    tx_expansion_csv = """
    isp_name, isp_type,  investment_period, node_from, node_to, forward_capacity_mw, reverse_capacity_mw
    Path1,    flow_path, 2030,              S1,        S2,      100,                 100
    REZ1,     rez,       2030,              S1,        S1,      50,                  0
    """

    # Transmission Flows
    tx_flows_csv = """
    isp_name, investment_period, timestep,             flow_mw
    Path1,    2030,              2030-01-01 00:00:00,  10
    """

    # Regional Transmission Flows
    nem_flows_csv = """
    nem_region_id, investment_period, timestep,             imports_mw, exports_mw, net_imports_mw
    R1,            2030,              2030-01-01 00:00:00,  0,          10,         -10
    R2,            2030,              2030-01-01 00:00:00,  10,         0,          10
    """

    # Sub-regional Transmission Flows
    sub_flows_csv = """
    isp_sub_region_id, investment_period, timestep,             imports_mw, exports_mw, net_imports_mw
    S1,                2030,              2030-01-01 00:00:00,  0,          10,         -10
    S2,                2030,              2030-01-01 00:00:00,  10,         0,          10
    """

    # Generator Dispatch
    dispatch_csv = """
    generator, node, carrier, investment_period, timestep,             dispatch_mw
    Gen1,      S1,   Coal,    2030,              2030-01-01 00:00:00,  50
    Gen2,      S2,   Wind,    2030,              2030-01-01 00:00:00,  30
    """

    # Demand
    demand_csv = """
    node, load,  investment_period, timestep,             demand_mw
    S1,   Load1, 2030,              2030-01-01 00:00:00,  40
    S2,   Load2, 2030,              2030-01-01 00:00:00,  40
    """

    # Compile results dictionary
    results = {
        "regions_and_zones_mapping": csv_str_to_df(mapping_csv),
        "transmission_expansion": csv_str_to_df(tx_expansion_csv),
        "transmission_flows": csv_str_to_df(tx_flows_csv),
        "nem_region_transmission_flows": csv_str_to_df(nem_flows_csv),
        "isp_sub_region_transmission_flows": csv_str_to_df(sub_flows_csv),
        "generator_dispatch": csv_str_to_df(dispatch_csv),
        "demand": csv_str_to_df(demand_csv),
    }

    # 2. Run the function
    plots = create_capacity_expansion_plot_suite(results)

    # 3. Verify Output Structure with explicit paths
    expected_paths = [
        # Transmission
        "transmission/aggregate_transmission_capacity.html",
        # Regional Expansion
        "transmission/regional_expansion/R1/rez_capacity.html",
        "transmission/regional_expansion/R1/flow_path_capacity.html",
        "transmission/regional_expansion/R2/rez_capacity.html",
        "transmission/regional_expansion/R2/flow_path_capacity.html",
        # Flows (Path1) - Week starting 2029-12-31 (Monday before Tuesday 2030-01-01)
        "transmission/flows/flow_path/Path1/2030/2029-12-31.html",
        # Dispatch - Week starting 2029-12-31
        "dispatch/nem_region_id/R1/2030/2029-12-31.html",
        "dispatch/nem_region_id/R2/2030/2029-12-31.html",
        "dispatch/isp_sub_region_id/S1/2030/2029-12-31.html",
        "dispatch/isp_sub_region_id/S2/2030/2029-12-31.html",
    ]

    assert len(plots) == len(expected_paths), (
        f"Expected {len(expected_paths)} plots, but found {len(plots)}"
    )

    for path_str in expected_paths:
        assert Path(path_str) in plots, f"Expected path {path_str} not found in plots"

    # 4. Verify Content of one plot
    agg_cap_key = Path("transmission/aggregate_transmission_capacity.html")
    content = plots[agg_cap_key]

    data = content["data"]
    r1_2030 = data[
        (data["nem_region_id"] == "R1") & (data["investment_period"] == 2030)
    ]
    assert r1_2030["capacity_mw"].iloc[0] == 100


def test_create_operational_plot_suite(csv_str_to_df):
    """Test create_operational_plot_suite with minimal representative data."""

    # 1. Setup Input Data using csv_str_to_df fixture
    # Re-using similar data structure as capacity expansion test

    # Mapping
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    R1,            S1,                REZ1
    R2,            S2,
    """

    # Transmission Expansion (Used for limits)
    tx_expansion_csv = """
    isp_name, isp_type,  investment_period, node_from, node_to, forward_capacity_mw, reverse_capacity_mw
    Path1,    flow_path, 2030,              S1,        S2,      100,                 100
    """

    # Transmission Flows
    tx_flows_csv = """
    isp_name, investment_period, timestep,             flow_mw
    Path1,    2030,              2030-01-01 00:00:00,  10
    """

    # Regional Transmission Flows
    nem_flows_csv = """
    nem_region_id, investment_period, timestep,             imports_mw, exports_mw, net_imports_mw
    R1,            2030,              2030-01-01 00:00:00,  0,          10,         -10
    R2,            2030,              2030-01-01 00:00:00,  10,         0,          10
    """

    # Sub-regional Transmission Flows
    sub_flows_csv = """
    isp_sub_region_id, investment_period, timestep,             imports_mw, exports_mw, net_imports_mw
    S1,                2030,              2030-01-01 00:00:00,  0,          10,         -10
    S2,                2030,              2030-01-01 00:00:00,  10,         0,          10
    """

    # Generator Dispatch
    dispatch_csv = """
    generator, node, carrier, investment_period, timestep,             dispatch_mw
    Gen1,      S1,   Coal,    2030,              2030-01-01 00:00:00,  50
    Gen2,      S2,   Wind,    2030,              2030-01-01 00:00:00,  30
    """

    # Demand
    demand_csv = """
    node, load,  investment_period, timestep,             demand_mw
    S1,   Load1, 2030,              2030-01-01 00:00:00,  40
    S2,   Load2, 2030,              2030-01-01 00:00:00,  40
    """

    # Compile results dictionary
    results = {
        "regions_and_zones_mapping": csv_str_to_df(mapping_csv),
        "transmission_expansion": csv_str_to_df(tx_expansion_csv),
        "transmission_flows": csv_str_to_df(tx_flows_csv),
        "nem_region_transmission_flows": csv_str_to_df(nem_flows_csv),
        "isp_sub_region_transmission_flows": csv_str_to_df(sub_flows_csv),
        "generator_dispatch": csv_str_to_df(dispatch_csv),
        "demand": csv_str_to_df(demand_csv),
    }

    # 2. Run the function
    plots = create_operational_plot_suite(results)

    # 3. Verify Output Structure with explicit paths
    expected_paths = [
        # Flows (Path1) - Week starting 2029-12-31
        "transmission/flows/flow_path/Path1/2030/2029-12-31.html",
        # Regional Dispatch (R1, R2)
        "dispatch/regional/R1/2030/2029-12-31.html",
        "dispatch/regional/R2/2030/2029-12-31.html",
        # Sub-regional Dispatch (S1, S2)
        "dispatch/sub_regional/S1/2030/2029-12-31.html",
        "dispatch/sub_regional/S2/2030/2029-12-31.html",
    ]

    assert len(plots) == len(expected_paths), (
        f"Expected {len(expected_paths)} plots, but found {len(plots)}"
    )

    for path_str in expected_paths:
        assert Path(path_str) in plots, f"Expected path {path_str} not found in plots"


def test_save_plots(csv_str_to_df):
    """Test saving plots and data to files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)

        # Create test data
        data_csv = """
        region,  value
        NSW1,    100
        QLD1,    200
        """
        test_data = csv_str_to_df(data_csv)

        # Create a simple plotly figure
        fig = go.Figure(data=[go.Bar(x=["NSW1", "QLD1"], y=[100, 200])])

        # Create charts dictionary with nested path structure
        charts = {
            Path("transmission/capacity.html"): {
                "plot": fig,
                "data": test_data,
            },
            Path("dispatch/regional/NSW1.html"): {
                "plot": fig,
                "data": test_data,
            },
        }

        # Save plots
        save_plots(charts, base_path)

        # Verify HTML files were created
        html_path1 = base_path / "transmission" / "capacity.html"
        html_path2 = base_path / "dispatch" / "regional" / "NSW1.html"
        assert html_path1.exists()
        assert html_path2.exists()

        # Verify CSV files were created
        csv_path1 = base_path / "transmission" / "capacity.csv"
        csv_path2 = base_path / "dispatch" / "regional" / "NSW1.csv"
        assert csv_path1.exists()
        assert csv_path2.exists()

        # Verify CSV content
        loaded_data = pd.read_csv(csv_path1)
        pd.testing.assert_frame_equal(loaded_data, test_data)

        # Verify HTML content contains plotly
        html_content = html_path1.read_text(encoding="utf-8")
        assert "plotly" in html_content.lower()
