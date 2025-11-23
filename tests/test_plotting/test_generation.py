import pandas as pd
import plotly.graph_objects as go

from ispypsa.plotting.generation import (
    _prepare_transmission_data,
    plot_node_level_dispatch,
    prepare_demand_data,
    prepare_dispatch_data,
)


def test_prepare_dispatch_data(csv_str_to_df):
    """Test prepare_dispatch_data aggregation and mapping."""
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    RegionA,       SubA,              Rez1
    RegionA,       SubB,
    RegionB,       SubC,              Rez2
    """

    dispatch_csv = """
    generator, node, carrier, investment_period, timestep,             dispatch_mw
    Gen1,      SubA, Coal,    2024,              2024-01-01 12:00:00,  100
    Gen2,      SubB, Coal,    2024,              2024-01-01 12:00:00,  50
    Gen3,      Rez1, Wind,    2024,              2024-01-01 12:00:00,  30
    Gen4,      SubC, Gas,     2024,              2024-01-01 12:00:00,  20
    Gen1,      SubA, Coal,    2024,              2024-01-08 12:00:00,  100
    """

    expected_csv = """
    node,    investment_period, carrier, timestep,             dispatch_mw, week_starting
    RegionA, 2024,              Coal,    2024-01-01 12:00:00,  150,         2024-01-01
    RegionA, 2024,              Coal,    2024-01-08 12:00:00,  100,         2024-01-08
    RegionA, 2024,              Wind,    2024-01-01 12:00:00,  30,          2024-01-01
    RegionB, 2024,              Gas,     2024-01-01 12:00:00,  20,          2024-01-01
    """

    regions_and_zones_mapping = csv_str_to_df(mapping_csv)
    dispatch = csv_str_to_df(dispatch_csv)
    expected_df = csv_str_to_df(expected_csv)

    # Adjust expected types to match function output
    expected_df["timestep"] = pd.to_datetime(expected_df["timestep"])
    expected_df["week_starting"] = pd.to_datetime(expected_df["week_starting"]).dt.date

    result = prepare_dispatch_data(
        dispatch,
        regions_and_zones_mapping,
        geography_level="nem_region_id",
    )

    pd.testing.assert_frame_equal(result, expected_df)


def test_prepare_demand_data(csv_str_to_df):
    """Test prepare_demand_data aggregation and mapping."""
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    RegionA,       SubA,              Rez1
    RegionA,       SubB,
    RegionB,       SubC,              Rez2
    """

    demand_csv = """
    node, load,  investment_period, timestep,             demand_mw
    SubA, Load1, 2024,              2024-01-01 12:00:00,  100
    SubB, Load2, 2024,              2024-01-01 12:00:00,  50
    SubC, Load3, 2024,              2024-01-01 12:00:00,  20
    SubA, Load1, 2024,              2024-01-08 12:00:00,  100
    """

    expected_csv = """
    node,    investment_period, timestep,             demand_mw, week_starting
    RegionA, 2024,              2024-01-01 12:00:00,  150,       2024-01-01
    RegionA, 2024,              2024-01-08 12:00:00,  100,       2024-01-08
    RegionB, 2024,              2024-01-01 12:00:00,  20,        2024-01-01
    """

    regions_and_zones_mapping = csv_str_to_df(mapping_csv)
    demand = csv_str_to_df(demand_csv)
    expected_df = csv_str_to_df(expected_csv)

    # Adjust expected types to match function output
    expected_df["timestep"] = pd.to_datetime(expected_df["timestep"])
    expected_df["week_starting"] = pd.to_datetime(expected_df["week_starting"]).dt.date

    result = prepare_demand_data(
        demand,
        regions_and_zones_mapping,
        geography_level="nem_region_id",
    )

    # Sort by node and timestep to ensure order matches
    result = result.sort_values(["node", "timestep"]).reset_index(drop=True)
    expected_df = expected_df.sort_values(["node", "timestep"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df)


def test_prepare_transmission_data(csv_str_to_df):
    """Test _prepare_transmission_data adds week_starting and node columns."""
    transmission_csv = """
    isp_sub_region_id, exports_mw, imports_mw, timestep
    SubA,              100,        50,         2024-01-01 00:00:00
    SubA,              120,        60,         2024-01-01 00:30:00
    SubB,              200,        80,         2024-01-08 00:30:00
    """

    expected_csv = """
    isp_sub_region_id, exports_mw, imports_mw, timestep,            week_starting, node
    SubA,              100,        50,         2024-01-01 00:00:00, 2023-12-25,    SubA
    SubA,              120,        60,         2024-01-01 00:30:00, 2024-01-01,    SubA
    SubB,              200,        80,         2024-01-08 00:30:00, 2024-01-08,    SubB
    """

    transmission = csv_str_to_df(transmission_csv)
    expected_df = csv_str_to_df(expected_csv)

    # Adjust expected types
    expected_df["timestep"] = pd.to_datetime(expected_df["timestep"])
    expected_df["week_starting"] = pd.to_datetime(expected_df["week_starting"]).dt.date

    result = _prepare_transmission_data(
        transmission, geography_level="isp_sub_region_id"
    )

    pd.testing.assert_frame_equal(result, expected_df)


def test_plot_node_level_dispatch(csv_str_to_df):
    """Test plot_node_level_dispatch returns expected dictionary structure."""
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    RegionA,       SubA,              Rez1
    """

    dispatch_csv = """
    generator, node, carrier, investment_period, timestep,             dispatch_mw
    Gen1,      SubA, Coal,    2024,              2024-01-01 12:00:00,  100
    """

    demand_csv = """
    node, load,  investment_period, timestep,             demand_mw
    SubA, Load1, 2024,              2024-01-01 12:00:00,  80
    """

    transmission_csv = """
    nem_region_id, investment_period, timestep,             imports_mw, exports_mw, net_imports_mw
    RegionA,       2024,              2024-01-01 12:00:00,  10,         20,         -10
    """

    regions_and_zones_mapping = csv_str_to_df(mapping_csv)
    dispatch = csv_str_to_df(dispatch_csv)
    demand = csv_str_to_df(demand_csv)
    transmission_flows = csv_str_to_df(transmission_csv)

    result = plot_node_level_dispatch(
        dispatch,
        demand,
        regions_and_zones_mapping,
        geography_level="nem_region_id",
        transmission_flows=transmission_flows,
    )

    # Check structure
    assert "RegionA" in result
    assert "2024" in result["RegionA"]
    # 2024-01-01 is a Monday, so week_starting is 2024-01-01
    assert "2024-01-01" in result["RegionA"]["2024"]

    entry = result["RegionA"]["2024"]["2024-01-01"]
    assert isinstance(entry["plot"], go.Figure)
    assert isinstance(entry["data"], pd.DataFrame)
    assert entry["data"]["dispatch_mw"].sum() == 100
    assert entry["data"]["carrier"].iloc[0] == "Coal"
