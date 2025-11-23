from unittest.mock import MagicMock

import pandas as pd
import pypsa
import pytest

from ispypsa.results.transmission import (
    _build_node_to_geography_mapping,
    _calculate_transmission_flows_by_geography,
    _extract_raw_link_flows,
    extract_isp_sub_region_transmission_flows,
    extract_nem_region_transmission_flows,
    extract_rez_transmission_flows,
    extract_transmission_expansion_results,
    extract_transmission_flows,
)


def test_extract_transmission_expansion_results(csv_str_to_df):
    """Test extraction of transmission expansion results with various build years."""

    # 1. Prepare Input Data
    # Scenario:
    # - Link-AB: Base capacity (Year 0), no expansion
    # - Link-CD: Base capacity (Year 0), expanded in 2030
    # - Link-EF: No base capacity, built in 2025, expanded in 2030
    links_csv = """
    bus0, bus1, build_year, p_nom_opt, p_min_pu, isp_name, isp_type
    A,    B,    0,          100,       -1,       Link-AB,  interconnector
    C,    D,    0,          200,       -1,       Link-CD,  interconnector
    C,    D,    2030,       50,        -1,       Link-CD,  interconnector
    E,    F,    2025,       300,       -1,       Link-EF,  interconnector
    E,    F,    2030,       100,       -1,       Link-EF,  interconnector
    """
    links_df = csv_str_to_df(links_csv)

    network = MagicMock(spec=pypsa.Network)
    network.links = links_df

    # 2. Execute Function
    result = extract_transmission_expansion_results(network)

    # 3. Prepare Expected Output
    # Note:
    # - Forward capacity is cumulative p_nom_opt
    # - Reverse capacity is cumulative (p_nom_opt * p_min_pu)
    # - Rows are reindexed to include all combinations of isp_name and investment_period (0, 2025, 2030)
    # - Missing values are forward filled (ffill). Pre-existence values (Link-EF at Year 0) remain NaN.
    expected_csv = """
    isp_name, investment_period, node_from, node_to, isp_type,        forward_capacity_mw, reverse_capacity_mw
    Link-AB,  0,                 A,         B,       interconnector,  100.0,               -100.0
    Link-AB,  2025,              A,         B,       interconnector,  100.0,               -100.0
    Link-AB,  2030,              A,         B,       interconnector,  100.0,               -100.0
    Link-CD,  0,                 C,         D,       interconnector,  200.0,               -200.0
    Link-CD,  2025,              C,         D,       interconnector,  200.0,               -200.0
    Link-CD,  2030,              C,         D,       interconnector,  250.0,               -250.0
    Link-EF,  0,                 E,         F,       interconnector,  0.0,                 0.0
    Link-EF,  2025,              E,         F,       interconnector,  300.0,               -300.0
    Link-EF,  2030,              E,         F,       interconnector,  400.0,               -400.0
    """
    expected_df = csv_str_to_df(expected_csv)

    # Sort to ensure matching order (function sorts by isp_name, investment_period via reindex/groupby)
    expected_df = expected_df.sort_values(
        ["isp_name", "investment_period"]
    ).reset_index(drop=True)

    # Adjust data types for exact matching if needed (NaNs often force float/object)
    # We rely on pandas flexibility but ensure column order/presence

    pd.testing.assert_frame_equal(result, expected_df, check_like=True)


def test_extract_raw_link_flows(csv_str_to_df):
    """Test extraction of raw link flows."""

    # 1. Prepare Input Data
    # Link static data
    links_csv = """
    Link,   bus0, bus1, isp_name
    Link_1, A,    B,    Link-AB
    Link_2, C,    D,    Link-CD
    """
    links_df = csv_str_to_df(links_csv).set_index("Link")

    # Flow data (p0) in time series format
    # Mocking PyPSA multi-period structure where index is MultiIndex (period, timestep)
    iterables = [[2025, 2030], [0, 1]]
    index = pd.MultiIndex.from_product(iterables, names=["period", "timestep"])

    data = {"Link_1": [10, 20, 30, 40], "Link_2": [50, 60, 70, 80]}
    p0_df = pd.DataFrame(data, index=index)

    network = MagicMock(spec=pypsa.Network)
    network.links = links_df
    network.links_t = MagicMock()
    network.links_t.p0 = p0_df

    # 2. Execute Function
    result = _extract_raw_link_flows(network)

    # 3. Prepare Expected Output
    # The function melts the dataframe.
    expected_csv = """
    investment_period, timestep, Link,   flow_mw, bus0, bus1, isp_name
    2025,              0,        Link_1, 10,      A,    B,    Link-AB
    2025,              1,        Link_1, 20,      A,    B,    Link-AB
    2030,              0,        Link_1, 30,      A,    B,    Link-AB
    2030,              1,        Link_1, 40,      A,    B,    Link-AB
    2025,              0,        Link_2, 50,      C,    D,    Link-CD
    2025,              1,        Link_2, 60,      C,    D,    Link-CD
    2030,              0,        Link_2, 70,      C,    D,    Link-CD
    2030,              1,        Link_2, 80,      C,    D,    Link-CD
    """
    expected_df = csv_str_to_df(expected_csv)

    # Sort to ensure matching order
    result = result.sort_values(["Link", "investment_period", "timestep"]).reset_index(
        drop=True
    )
    expected_df = expected_df.sort_values(
        ["Link", "investment_period", "timestep"]
    ).reset_index(drop=True)

    # Enforce types for comparison
    pd.testing.assert_frame_equal(result, expected_df, check_like=True)


def test_extract_transmission_flows(csv_str_to_df):
    """Test extraction and aggregation of transmission flows by ISP name."""

    # 1. Prepare Input Data
    # Two parallel links belonging to the same ISP path "Link-AB"
    # This tests the aggregation logic in extract_transmission_flows
    links_csv = """
    Link,   bus0, bus1, isp_name
    Link_1, A,    B,    Link-AB
    Link_2, A,    B,    Link-AB
    """
    links_df = csv_str_to_df(links_csv).set_index("Link")

    iterables = [[2025], [0, 1]]
    index = pd.MultiIndex.from_product(iterables, names=["period", "timestep"])

    data = {"Link_1": [10, 20], "Link_2": [30, 40]}
    p0_df = pd.DataFrame(data, index=index)

    network = MagicMock(spec=pypsa.Network)
    network.links = links_df
    network.links_t = MagicMock()
    network.links_t.p0 = p0_df

    # 2. Execute Function
    result = extract_transmission_flows(network)

    # 3. Prepare Expected Output
    # Should sum flows for Link_1 and Link_2 for each timestamp
    expected_csv = """
    isp_name, from_node, to_node, investment_period, timestep, flow_mw
    Link-AB,  A,         B,       2025,              0,        40
    Link-AB,  A,         B,       2025,              1,        60
    """
    expected_df = csv_str_to_df(expected_csv)

    # Sort to ensure matching order
    result = result.sort_values(
        ["isp_name", "investment_period", "timestep"]
    ).reset_index(drop=True)
    expected_df = expected_df.sort_values(
        ["isp_name", "investment_period", "timestep"]
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df, check_like=True)


def test_build_node_to_geography_mapping(csv_str_to_df):
    """Test node to geography mapping for regions, subregions, and REZs."""

    # 1. Prepare Input Data
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    NSW1,          CNSW,              N1
    NSW1,          NNSW,              N2
    QLD1,          SEQ,
    """
    # Note: Empty value for SEQ's rez_id is handled as NaN by csv_str_to_df/pandas
    mapping_df = csv_str_to_df(mapping_csv)

    # 2. Test Region Mapping
    # Should map subregions -> regions AND REZs -> regions
    result_region = _build_node_to_geography_mapping(mapping_df, "region")
    expected_region = {
        "CNSW": "NSW1",
        "NNSW": "NSW1",
        "SEQ": "QLD1",
        "N1": "NSW1",
        "N2": "NSW1",
    }
    assert result_region == expected_region

    # 3. Test Subregion Mapping
    # Should map subregions -> subregions AND REZs -> subregions
    result_subregion = _build_node_to_geography_mapping(mapping_df, "subregion")
    expected_subregion = {
        "CNSW": "CNSW",
        "NNSW": "NNSW",
        "SEQ": "SEQ",
        "N1": "CNSW",
        "N2": "NNSW",
    }
    assert result_subregion == expected_subregion

    # 4. Test REZ Mapping
    # Should map REZs -> REZs only
    result_rez = _build_node_to_geography_mapping(mapping_df, "rez")
    expected_rez = {"N1": "N1", "N2": "N2"}
    assert result_rez == expected_rez

    # 5. Test Invalid Geography Level
    with pytest.raises(ValueError, match="Unknown geography_level"):
        _build_node_to_geography_mapping(mapping_df, "invalid_level")


def test_calculate_transmission_flows_by_geography(csv_str_to_df):
    """Test calculation of transmission flows between geographic regions."""

    # 1. Prepare Input Data
    # We simulate a flow between two regions (GeoA, GeoB) and one internal flow (GeoA).
    # Columns expected by the function: from_node, to_node, flow_mw, investment_period, timestep

    # Case 4: Parallel lines (multiple flows between GeoA and GeoB in same timestep)
    # Case 1: Negative flow (from_node=A, to_node=B, flow=-15 implies B->A)
    flow_long_csv = """
    investment_period, timestep, from_node, to_node, flow_mw
    2025,              0,        Node_A1,   Node_B1, 100
    2025,              0,        Node_B1,   Node_A1, 20
    2025,              0,        Node_A1,   Node_A2, 10
    2025,              1,        Node_A1,   Node_B1, 50
    2025,              1,        Node_A2,   Node_B1, 30
    2025,              1,        Node_A1,   Node_B1, -15
    """
    flow_long = csv_str_to_df(flow_long_csv)

    node_to_geography = {"Node_A1": "GeoA", "Node_A2": "GeoA", "Node_B1": "GeoB"}

    geography_col = "region_id"

    # 2. Execute Function
    result = _calculate_transmission_flows_by_geography(
        flow_long, node_to_geography, geography_col
    )

    # 3. Prepare Expected Output
    # Calculations:
    # T0:
    #   GeoA:
    #     - Export to GeoB: 100 (Node_A1 -> Node_B1)
    #     - Import from GeoB: 20 (Node_B1 -> Node_A1)
    #     - Internal (Node_A1 -> Node_A2): Ignored
    #     - Net Import: 20 - 100 = -80
    #   GeoB:
    #     - Import from GeoA: 100
    #     - Export to GeoA: 20
    #     - Net Import: 100 - 20 = 80
    #
    # T1:
    #   GeoA:
    #     - Export to GeoB (Flow 50): Exp 50, Imp 0
    #     - Export to GeoB (Flow 30): Exp 30, Imp 0
    #     - Flow from GeoB (Flow -15): Exp 0, Imp 15
    #     - Total: Exp 80, Imp 15, Net = 15 - 80 = -65
    #   GeoB:
    #     - Import from GeoA (Flow 50): Imp 50, Exp 0
    #     - Import from GeoA (Flow 30): Imp 30, Exp 0
    #     - Flow to GeoA (Flow -15): Imp 0, Exp 15
    #     - Total: Imp 80, Exp 15, Net = 80 - 15 = 65

    expected_csv = """
    region_id, investment_period, timestep, imports_mw, exports_mw, net_imports_mw
    GeoA,      2025,              0,        20,         100,        -80
    GeoB,      2025,              0,        100,        20,         80
    GeoA,      2025,              1,        15,         80,         -65
    GeoB,      2025,              1,        80,         15,         65
    """
    expected_df = csv_str_to_df(expected_csv)

    # Sort both to ensure reliable comparison
    result = result.sort_values(
        [geography_col, "investment_period", "timestep"]
    ).reset_index(drop=True)
    expected_df = expected_df.sort_values(
        [geography_col, "investment_period", "timestep"]
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df, check_like=True)


def test_extract_nem_region_transmission_flows(csv_str_to_df):
    """Test integration of extracting NEM region transmission flows.

    This tests the end-to-end flow from link flows to aggregated region flows,
    verifying that:
    1. Sub-regions are correctly mapped to regions
    2. REZs are correctly mapped to regions
    3. Intra-region flows are filtered out
    4. Inter-region flows are correctly aggregated
    """

    # 1. Prepare Input Data
    # Link flows (output format from extract_transmission_flows)
    # We need from_node and to_node columns as expected by the internal logic
    # Scenario:
    # - Link 1: SubA1 (RegionA) -> SubB1 (RegionB). Flow 100. (Inter-region)
    # - Link 2: SubA1 (RegionA) -> SubA2 (RegionA). Flow 50. (Intra-region)
    # - Link 3: RezA1 (RegionA) -> SubA1 (RegionA). Flow 300. (Intra-region, standard REZ connection)
    link_flows_csv = """
    investment_period, timestep, from_node, to_node, flow_mw, isp_name
    2025,              0,        SubA1,     SubB1,   100,     Link-AB
    2025,              0,        SubA1,     SubA2,   50,      Link-A1-A2
    2025,              0,        RezA1,     SubA1,   300,     Link-RezA-A
    """
    link_flows = csv_str_to_df(link_flows_csv)

    # Mapping table
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    RegionA,       SubA1,             RezA1
    RegionA,       SubA2,
    RegionB,       SubB1,
    """
    regions_and_zones_mapping = csv_str_to_df(mapping_csv)

    # 2. Execute Function
    result = extract_nem_region_transmission_flows(
        link_flows, regions_and_zones_mapping
    )

    # 3. Prepare Expected Output
    # Totals for RegionA -> RegionB: 100 MW
    # RegionA: Export 100, Import 0, Net = -100
    # RegionB: Import 100, Export 0, Net = 100

    expected_csv = """
    nem_region_id, investment_period, timestep, imports_mw, exports_mw, net_imports_mw
    RegionA,       2025,              0,        0,          100,        -100
    RegionB,       2025,              0,        100,        0,          100
    """
    expected_df = csv_str_to_df(expected_csv)

    # Sort for comparison
    result = result.sort_values(["nem_region_id"]).reset_index(drop=True)
    expected_df = expected_df.sort_values(["nem_region_id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df, check_like=True)


def test_extract_isp_sub_region_transmission_flows(csv_str_to_df):
    """Test integration of extracting ISP sub-region transmission flows."""

    # 1. Prepare Input Data
    # Scenario:
    # - Link 1: SubA1 -> SubB1. Flow 100. (Inter-subregion)
    # - Link 2: SubA1 -> SubA2. Flow 50. (Inter-subregion, same region but diff subregion)
    # - Link 3: RezA1 (in SubA1) -> SubA1. Flow 300. (Intra-subregion)
    link_flows_csv = """
    investment_period, timestep, from_node, to_node, flow_mw, isp_name
    2025,              0,        SubA1,     SubB1,   100,     Link-AB
    2025,              0,        SubA1,     SubA2,   50,      Link-A1-A2
    2025,              0,        RezA1,     SubA1,   300,     Link-RezA-A
    """
    link_flows = csv_str_to_df(link_flows_csv)

    # Mapping table
    # Note: RezA1 maps to SubA1
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    RegionA,       SubA1,             RezA1
    RegionA,       SubA2,
    RegionB,       SubB1,
    """
    regions_and_zones_mapping = csv_str_to_df(mapping_csv)

    # 2. Execute Function
    result = extract_isp_sub_region_transmission_flows(
        link_flows, regions_and_zones_mapping
    )

    # 3. Prepare Expected Output
    # SubA1:
    # - Export to SubB1 (100)
    # - Export to SubA2 (50)
    # - Internal REZ flow (300) is ignored because RezA1 maps to SubA1
    # - Total Export: 150

    # SubA2:
    # - Import from SubA1 (50)

    # SubB1:
    # - Import from SubA1 (100)

    expected_csv = """
    isp_sub_region_id, investment_period, timestep, imports_mw, exports_mw, net_imports_mw
    SubA1,             2025,              0,        0,          150,        -150
    SubA2,             2025,              0,        50,         0,          50
    SubB1,             2025,              0,        100,        0,          100
    """
    expected_df = csv_str_to_df(expected_csv)

    # Sort for comparison
    result = result.sort_values(["isp_sub_region_id"]).reset_index(drop=True)
    expected_df = expected_df.sort_values(["isp_sub_region_id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df, check_like=True)


def test_extract_rez_transmission_flows(csv_str_to_df):
    """Test integration of extracting REZ transmission flows."""

    # 1. Prepare Input Data
    # Scenario:
    # - Link 1: RezA1 -> SubA1. Flow 300. (Link-Rez-Grid)
    # - Link 2: RezA2 -> SubA2. Flow 50. (Link-Rez-Grid)
    # Note: REZs do not connect to each other directly, only to the grid (subregions).
    # The function maps nodes to 'rez_id'. Subregions map to NaN.
    # Flows between REZ and NaN (Subregion) are considered "inter-geography" flows
    # and are counted as exports from the REZ.

    link_flows_csv = """
    investment_period, timestep, from_node, to_node, flow_mw, isp_name
    2025,              0,        RezA1,     SubA1,   300,     Link-RezA1-Grid
    2025,              0,        RezA2,     SubA2,   50,      Link-RezA2-Grid
    """
    link_flows = csv_str_to_df(link_flows_csv)

    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    RegionA,       SubA1,             RezA1
    RegionA,       SubA2,             RezA2
    """
    regions_and_zones_mapping = csv_str_to_df(mapping_csv)

    # 2. Execute Function
    result = extract_rez_transmission_flows(link_flows, regions_and_zones_mapping)

    # 3. Prepare Expected Output
    # RezA1:
    # - Export to SubA1 (300) (SubA1 is 'foreign' to RezA1)
    # - Total Export: 300
    # RezA2:
    # - Export to SubA2 (50) (SubA2 is 'foreign' to RezA2)
    # - Total Export: 50

    expected_csv = """
    rez_id, investment_period, timestep, imports_mw, exports_mw, net_imports_mw
    RezA1,  2025,              0,        0,          300,        -300
    RezA2,  2025,              0,        0,          50,         -50
    """
    expected_df = csv_str_to_df(expected_csv)

    # Filter NaNs (non-REZ nodes like SubA1) which appear in the result
    result = result.dropna(subset=["rez_id"])

    # Sort for comparison
    result = result.sort_values(["rez_id"]).reset_index(drop=True)
    expected_df = expected_df.sort_values(["rez_id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df, check_like=True)
