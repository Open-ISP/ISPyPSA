import pandas as pd
import plotly.graph_objects as go

from ispypsa.plotting.generation import (
    _prepare_transmission_data,
    plot_dispatch,
    plot_generation_capacity_expansion,
    prepare_demand_data,
    prepare_dispatch_data,
    prepare_generation_capacity,
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
    generator, node, fuel_type, investment_period, timestep,             dispatch_mw
    Gen1,      SubA, Coal,      2024,              2024-01-01 12:00:00,  100
    Gen2,      SubB, Coal,      2024,              2024-01-01 12:00:00,  50
    Gen3,      Rez1, Wind,      2024,              2024-01-01 12:00:00,  30
    Gen4,      SubC, Gas,       2024,              2024-01-01 12:00:00,  20
    Gen1,      SubA, Coal,      2024,              2024-01-08 12:00:00,  100
    """

    expected_csv = """
    node,    investment_period, fuel_type, timestep,             dispatch_mw, week_starting
    RegionA, 2024,              Coal,      2024-01-01 12:00:00,  150,         2024-01-01
    RegionA, 2024,              Coal,      2024-01-08 12:00:00,  100,         2024-01-08
    RegionA, 2024,              Wind,      2024-01-01 12:00:00,  30,          2024-01-01
    RegionB, 2024,              Gas,       2024-01-01 12:00:00,  20,          2024-01-01
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


def test_plot_dispatch_with_geography(csv_str_to_df):
    """Test plot_dispatch with geography_level returns expected dictionary structure."""
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    RegionA,       SubA,              Rez1
    """

    dispatch_csv = """
    generator, node, fuel_type, investment_period, timestep,             dispatch_mw
    Gen1,      SubA, Coal,      2024,              2024-01-01 12:00:00,  100
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

    result = plot_dispatch(
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
    assert entry["data"]["fuel_type"].iloc[0] == "Coal"


def test_plot_dispatch_system_level(csv_str_to_df):
    """Test plot_dispatch without geography_level returns system-level structure."""
    dispatch_csv = """
    generator, node, fuel_type, investment_period, timestep,             dispatch_mw
    Gen1,      SubA, Coal,      2024,              2024-01-01 12:00:00,  100
    Gen2,      SubB, Wind,      2024,              2024-01-01 12:00:00,  50
    """

    demand_csv = """
    node, load,  investment_period, timestep,             demand_mw
    SubA, Load1, 2024,              2024-01-01 12:00:00,  80
    SubB, Load2, 2024,              2024-01-01 12:00:00,  70
    """

    dispatch = csv_str_to_df(dispatch_csv)
    demand = csv_str_to_df(demand_csv)

    result = plot_dispatch(dispatch, demand)

    # Check structure - no node level, just investment_period and week_starting
    assert "2024" in result
    # 2024-01-01 is a Monday, so week_starting is 2024-01-01
    assert "2024-01-01" in result["2024"]

    entry = result["2024"]["2024-01-01"]
    assert isinstance(entry["plot"], go.Figure)
    assert isinstance(entry["data"], pd.DataFrame)
    # Total dispatch should be 150 (100 + 50)
    assert entry["data"]["dispatch_mw"].sum() == 150
    assert set(entry["data"]["fuel_type"].unique()) == {"Coal", "Wind"}


def test_prepare_generation_capacity(csv_str_to_df):
    """Test prepare_generation_capacity aggregation with closures."""
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    RegionA,       SubA,              Rez1
    RegionA,       SubB,              Rez2
    RegionB,       SubC,              Rez3
    RegionB,       SubD,
    """

    # Input generators with various build years, closures, and regions.
    # A generator is active in a year when: investment_period <= year < closure_year
    # Gen7 (Unserved Energy) should be filtered out entirely.
    generation_csv = """
    generator, fuel_type,        node, capacity_mw, investment_period, closure_year
    Gen1,      Coal,             SubA, 100,         2025,              2030
    Gen2,      Gas,              SubB, 50,          2025,              2035
    Gen3,      Wind,             Rez1, 30,          2027,              2032
    Gen4,      Solar,            SubC, 80,          2025,              2028
    Gen5,      Coal,             SubD, 200,         2029,              2040
    Gen6,      Wind,             Rez3, 120,         2025,              2035
    Gen7,      Unserved Energy,  SubA, 9999,        2025,              2050
    Gen8,      Gas,              SubA, 25,          2025,              2035
    Gen9,      Solar,            SubC, 150,         2027,              2040
    Gen10,     Solar,            Rez3, 200,         2029,              2045
    """

    # Expected output aggregated by region and fuel type for each year.
    # Years come from unique investment_periods: 2025, 2027, 2029
    #
    # Year 2025:
    #   RegionA: Coal=100 (Gen1), Gas=75 (Gen2+Gen8)
    #   RegionB: Solar=80 (Gen4), Wind=120 (Gen6)
    #
    # Year 2027:
    #   RegionA: Coal=100 (Gen1), Gas=75 (Gen2+Gen8), Wind=30 (Gen3 comes online)
    #   RegionB: Solar=230 (Gen4+Gen9), Wind=120 (Gen6)
    #
    # Year 2029:
    #   RegionA: Coal=100 (Gen1 still active, closes 2030), Gas=75, Wind=30
    #   RegionB: Coal=200 (Gen5 comes online), Solar=350 (Gen9+Gen10, Gen4 closed), Wind=120
    expected_csv = """
    nem_region_id, fuel_type, year, capacity_mw
    RegionA,       Coal,      2025, 100
    RegionA,       Coal,      2027, 100
    RegionA,       Coal,      2029, 100
    RegionA,       Gas,       2025, 75
    RegionA,       Gas,       2027, 75
    RegionA,       Gas,       2029, 75
    RegionA,       Wind,      2027, 30
    RegionA,       Wind,      2029, 30
    RegionB,       Coal,      2029, 200
    RegionB,       Solar,     2025, 80
    RegionB,       Solar,     2027, 230
    RegionB,       Solar,     2029, 350
    RegionB,       Wind,      2025, 120
    RegionB,       Wind,      2027, 120
    RegionB,       Wind,      2029, 120
    """

    regions_and_zones_mapping = csv_str_to_df(mapping_csv)
    generation_expansion = csv_str_to_df(generation_csv)
    expected_df = csv_str_to_df(expected_csv)

    result = prepare_generation_capacity(
        generation_expansion,
        regions_and_zones_mapping,
        geography_level="nem_region_id",
    )

    # Sort both for consistent comparison
    sort_cols = ["nem_region_id", "fuel_type", "year"]
    result = result.sort_values(sort_cols).reset_index(drop=True)
    expected_df = expected_df.sort_values(sort_cols).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df)


def test_plot_generation_capacity_expansion(csv_str_to_df):
    """Test plot_generation_capacity_expansion returns correct structure."""
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    RegionA,       SubA,              Rez1
    RegionA,       SubB,
    RegionB,       SubC,
    """

    generation_csv = """
    generator, fuel_type, node, capacity_mw, investment_period, closure_year
    Gen1,      Coal,      SubA, 100,         2025,              2035
    Gen2,      Gas,       SubB, 50,          2025,              2035
    Gen3,      Wind,      Rez1, 30,          2025,              2035
    Gen4,      Solar,     SubC, 20,          2025,              2035
    """

    regions_and_zones_mapping = csv_str_to_df(mapping_csv)
    generation_expansion = csv_str_to_df(generation_csv)

    result = plot_generation_capacity_expansion(
        generation_expansion, regions_and_zones_mapping
    )

    # Check top-level structure
    assert "aggregate_capacity" in result
    assert "regional" in result
    assert "sub_regional" in result
    assert "rez" in result

    # Check system-wide plot
    assert isinstance(result["aggregate_capacity"]["plot"], go.Figure)
    assert isinstance(result["aggregate_capacity"]["data"], pd.DataFrame)
    assert set(result["aggregate_capacity"]["data"].columns) == {
        "fuel_type",
        "year",
        "capacity_mw",
    }

    # Check regional plots
    assert "RegionA" in result["regional"]
    assert "RegionB" in result["regional"]
    assert isinstance(result["regional"]["RegionA"]["plot"], go.Figure)
    assert isinstance(result["regional"]["RegionA"]["data"], pd.DataFrame)

    # Check sub-regional plots
    assert "SubA" in result["sub_regional"]
    assert "SubB" in result["sub_regional"]
    assert "SubC" in result["sub_regional"]

    # Check REZ plots
    assert "Rez1" in result["rez"]

    # Check data content - RegionA should have Coal + Gas + Wind
    region_a_fuels = set(result["regional"]["RegionA"]["data"]["fuel_type"].unique())
    assert region_a_fuels == {"Coal", "Gas", "Wind"}
