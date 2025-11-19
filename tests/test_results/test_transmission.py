"""Tests for transmission flow extraction functions."""

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
    extract_transmission_flows,
)


@pytest.fixture
def basic_network_with_flows():
    """Create a basic PyPSA network with link flows for testing."""
    network = pypsa.Network()

    # Set up multi-period snapshots
    network.set_snapshots(
        pd.MultiIndex.from_product(
            [
                [2025, 2030],  # investment periods
                pd.date_range("2025-01-01 00:00", periods=4, freq="h"),  # timesteps
            ],
            names=["period", "timestep"],
        )
    )

    # Add buses
    network.add("Bus", "NodeA")
    network.add("Bus", "NodeB")
    network.add("Bus", "NodeC")

    # Add links with static data
    network.add(
        "Link",
        "LinkAB",
        bus0="NodeA",
        bus1="NodeB",
        isp_name="A-B",
    )
    network.add(
        "Link",
        "LinkBC",
        bus0="NodeB",
        bus1="NodeC",
        isp_name="B-C",
    )

    # Add time series flows (p0 = flow from bus0 to bus1)
    # LinkAB: varying flows across periods and timesteps
    network.links_t.p0["LinkAB"] = [100, 150, 200, 250, 300, 350, 400, 450]
    # LinkBC: different pattern
    network.links_t.p0["LinkBC"] = [50, 75, 100, 125, 150, 175, 200, 225]

    return network


@pytest.fixture
def network_with_inter_intra_flows():
    """Network with both inter-geography and intra-geography flows."""
    network = pypsa.Network()

    network.set_snapshots(
        pd.MultiIndex.from_product(
            [[2025], pd.date_range("2025-01-01 00:00", periods=2, freq="h")],
            names=["period", "timestep"],
        )
    )

    # Add buses representing different geographic levels
    network.add("Bus", "NSW_CNSW")  # Sub-region in NSW
    network.add("Bus", "NSW_NNSW")  # Another sub-region in NSW
    network.add("Bus", "NSW_CNSW_REZ1")  # REZ within CNSW
    network.add("Bus", "VIC")  # Different region

    # Add links
    # Inter-regional: NSW ↔ VIC
    network.add("Link", "NSW-VIC", bus0="NSW_CNSW", bus1="VIC", isp_name="NSW-VIC")

    # Intra-regional: NSW_CNSW ↔ NSW_NNSW (both in NSW)
    network.add(
        "Link", "CNSW-NNSW", bus0="NSW_CNSW", bus1="NSW_NNSW", isp_name="CNSW-NNSW"
    )

    # Intra-sub-region: REZ ↔ sub-region within same sub-region
    network.add(
        "Link", "REZ1-CNSW", bus0="NSW_CNSW_REZ1", bus1="NSW_CNSW", isp_name="REZ1-CNSW"
    )

    # Set flows
    network.links_t.p0["NSW-VIC"] = [1000, 1100]
    network.links_t.p0["CNSW-NNSW"] = [500, 550]
    network.links_t.p0["REZ1-CNSW"] = [200, 220]

    return network


# Tests for _extract_raw_link_flows
def test_extract_raw_link_flows_basic(basic_network_with_flows):
    """Test basic extraction of raw link flows."""
    result = _extract_raw_link_flows(basic_network_with_flows)

    # Check structure
    expected_columns = [
        "investment_period",
        "timestep",
        "Link",
        "flow_mw",
        "bus0",
        "bus1",
        "isp_name",
    ]
    assert list(result.columns) == expected_columns

    # Check we have data for both links across both periods
    assert len(result) == 2 * 2 * 4  # 2 links × 2 periods × 4 timesteps = 16 rows

    # Check link names
    assert set(result["Link"].unique()) == {"LinkAB", "LinkBC"}

    # Check periods
    assert set(result["investment_period"].unique()) == {2025, 2030}

    # Check a specific flow value
    link_ab_2025_first = result[
        (result["Link"] == "LinkAB") & (result["investment_period"] == 2025)
    ].iloc[0]
    assert link_ab_2025_first["flow_mw"] == 100
    assert link_ab_2025_first["bus0"] == "NodeA"
    assert link_ab_2025_first["bus1"] == "NodeB"
    assert link_ab_2025_first["isp_name"] == "A-B"


def test_extract_raw_link_flows_empty_network():
    """Test extraction from network with no links."""
    network = pypsa.Network()

    # Set up multi-period snapshots (matching the structure expected by the function)
    network.set_snapshots(
        pd.MultiIndex.from_product(
            [[2025], pd.date_range("2025-01-01", periods=2, freq="h")],
            names=["period", "timestep"],
        )
    )

    # Add isp_name column to network.links so it exists even when empty
    network.links["isp_name"] = pd.Series(dtype=str)

    result = _extract_raw_link_flows(network)

    # Should return empty DataFrame with correct schema
    expected_columns = [
        "investment_period",
        "timestep",
        "Link",
        "flow_mw",
        "bus0",
        "bus1",
        "isp_name",
    ]
    assert list(result.columns) == expected_columns
    assert len(result) == 0


# Tests for extract_transmission_flows
def test_extract_transmission_flows_basic(basic_network_with_flows):
    """Test aggregation by ISP name."""
    link_flows = _extract_raw_link_flows(basic_network_with_flows)
    result = extract_transmission_flows(link_flows)

    # Check structure
    expected_columns = ["isp_name", "investment_period", "timestep", "flow"]
    assert list(result.columns) == expected_columns

    # Check aggregation worked
    assert len(result) == 2 * 2 * 4  # 2 isp_names × 2 periods × 4 timesteps
    assert set(result["isp_name"].unique()) == {"A-B", "B-C"}

    # Verify a specific aggregated value
    ab_2025_first = result[
        (result["isp_name"] == "A-B") & (result["investment_period"] == 2025)
    ].iloc[0]
    assert ab_2025_first["flow"] == 100


def test_extract_transmission_flows_empty():
    """Test with empty link flows."""
    empty_flows = pd.DataFrame(
        columns=[
            "Link",
            "investment_period",
            "timestep",
            "flow_mw",
            "bus0",
            "bus1",
            "isp_name",
        ]
    )
    result = extract_transmission_flows(empty_flows)

    assert list(result.columns) == ["isp_name", "investment_period", "timestep", "flow"]
    assert len(result) == 0


# Tests for _build_node_to_geography_mapping
def test_build_node_to_geography_mapping_region_level(csv_str_to_df):
    """Test building node-to-region mapping."""
    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               REZ1
    NSW,            NNSW,               REZ2
    VIC,            VIC,                NaN
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    result = _build_node_to_geography_mapping(regions_mapping, "region")

    # Should map both sub-regions and REZs to regions
    assert result["CNSW"] == "NSW"
    assert result["NNSW"] == "NSW"
    assert result["REZ1"] == "NSW"
    assert result["REZ2"] == "NSW"
    assert result["VIC"] == "VIC"


def test_build_node_to_geography_mapping_subregion_level(csv_str_to_df):
    """Test building node-to-sub-region mapping."""
    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               REZ1
    NSW,            NNSW,               REZ2
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    result = _build_node_to_geography_mapping(regions_mapping, "subregion")

    # Sub-regions map to themselves, REZs map to parent sub-regions
    assert result["CNSW"] == "CNSW"
    assert result["NNSW"] == "NNSW"
    assert result["REZ1"] == "CNSW"
    assert result["REZ2"] == "NNSW"


def test_build_node_to_geography_mapping_rez_level(csv_str_to_df):
    """Test building node-to-REZ mapping."""
    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               REZ1
    NSW,            NNSW,               REZ2
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    result = _build_node_to_geography_mapping(regions_mapping, "rez")

    # Only REZs map (to themselves), sub-regions don't appear
    assert result["REZ1"] == "REZ1"
    assert result["REZ2"] == "REZ2"
    assert "CNSW" not in result
    assert "NNSW" not in result


def test_build_node_to_geography_mapping_no_rez_column(csv_str_to_df):
    """Test mapping when there's no rez_id column."""
    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    VIC,            VIC
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    # Region level should still work
    result_region = _build_node_to_geography_mapping(regions_mapping, "region")
    assert result_region["CNSW"] == "NSW"
    assert result_region["VIC"] == "VIC"

    # REZ level should return empty dict
    result_rez = _build_node_to_geography_mapping(regions_mapping, "rez")
    assert result_rez == {}


# Tests for _calculate_transmission_flows_by_geography
def test_calculate_transmission_flows_filters_intra_geography():
    """Test that intra-geography flows are filtered out."""
    # Create flow data with both inter and intra flows
    flow_long = pd.DataFrame(
        {
            "Link": ["Inter", "Intra"],
            "investment_period": [2025, 2025],
            "timestep": pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 00:00:00"]),
            "flow_mw": [1000, 500],
            "bus0": ["CNSW", "CNSW"],
            "bus1": ["VIC", "NNSW"],
            "isp_name": ["CNSW-VIC", "CNSW-NNSW"],
        }
    )

    # Mapping: CNSW and NNSW both in NSW, VIC in VIC
    node_to_geography = {"CNSW": "NSW", "NNSW": "NSW", "VIC": "VIC"}

    result = _calculate_transmission_flows_by_geography(
        flow_long, node_to_geography, "region_id"
    )

    # Should only have flows for NSW and VIC (the inter-regional flow)
    # Intra-NSW flow should be filtered out
    assert len(result) == 2  # One for NSW, one for VIC

    # NSW should show exports
    nsw_flow = result[result["region_id"] == "NSW"].iloc[0]
    assert nsw_flow["exports_mw"] == 1000
    assert nsw_flow["imports_mw"] == 0
    assert nsw_flow["net_imports_mw"] == -1000

    # VIC should show imports
    vic_flow = result[result["region_id"] == "VIC"].iloc[0]
    assert vic_flow["imports_mw"] == 1000
    assert vic_flow["exports_mw"] == 0
    assert vic_flow["net_imports_mw"] == 1000


def test_calculate_transmission_flows_bidirectional():
    """Test import/export calculation with bidirectional flows."""
    flow_long = pd.DataFrame(
        {
            "Link": ["Link1", "Link1"],
            "investment_period": [2025, 2025],
            "timestep": pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 01:00:00"]),
            "flow_mw": [500, -300],
            "bus0": ["A", "A"],
            "bus1": ["B", "B"],
            "isp_name": ["A-B", "A-B"],
        }
    )

    node_to_geography = {"A": "RegionA", "B": "RegionB"}

    result = _calculate_transmission_flows_by_geography(
        flow_long, node_to_geography, "region_id"
    )

    # At timestep 00:00 (positive flow):
    # RegionA exports 500, RegionB imports 500
    region_a_t0 = result[
        (result["region_id"] == "RegionA")
        & (result["timestep"] == pd.Timestamp("2025-01-01 00:00:00"))
    ].iloc[0]
    assert region_a_t0["exports_mw"] == 500
    assert region_a_t0["imports_mw"] == 0
    assert region_a_t0["net_imports_mw"] == -500

    # At timestep 01:00 (negative flow = reverse):
    # RegionA imports 300, RegionB exports 300
    region_a_t1 = result[
        (result["region_id"] == "RegionA")
        & (result["timestep"] == pd.Timestamp("2025-01-01 01:00:00"))
    ].iloc[0]
    assert region_a_t1["imports_mw"] == 300
    assert region_a_t1["exports_mw"] == 0
    assert region_a_t1["net_imports_mw"] == 300


def test_calculate_transmission_flows_unmapped_nodes():
    """Test that flows with unmapped nodes are excluded."""
    flow_long = pd.DataFrame(
        {
            "Link": ["Mapped", "Unmapped1", "Unmapped2"],
            "investment_period": [2025, 2025, 2025],
            "timestep": pd.to_datetime(
                ["2025-01-01 00:00:00", "2025-01-01 00:00:00", "2025-01-01 00:00:00"]
            ),
            "flow_mw": [1000, 500, 300],
            "bus0": ["NodeA", "Unknown", "NodeA"],
            "bus1": ["NodeB", "NodeB", "Unknown2"],
            "isp_name": ["A-B", "U-B", "A-U"],
        }
    )

    # Only map some nodes
    node_to_geography = {"NodeA": "GeoA", "NodeB": "GeoB"}

    result = _calculate_transmission_flows_by_geography(
        flow_long, node_to_geography, "geo_id"
    )

    # Should only include the mapped flow
    assert len(result) == 2  # GeoA and GeoB
    assert result["geo_id"].isin(["GeoA", "GeoB"]).all()


def test_calculate_transmission_flows_empty_after_filtering():
    """Test when all flows are filtered out (all intra-geography)."""
    flow_long = pd.DataFrame(
        {
            "Link": ["Intra1", "Intra2"],
            "investment_period": [2025, 2025],
            "timestep": pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 00:00:00"]),
            "flow_mw": [500, 300],
            "bus0": ["A", "B"],
            "bus1": ["B", "C"],
            "isp_name": ["A-B", "B-C"],
        }
    )

    # All nodes in same geography
    node_to_geography = {"A": "SameGeo", "B": "SameGeo", "C": "SameGeo"}

    result = _calculate_transmission_flows_by_geography(
        flow_long, node_to_geography, "geo_id"
    )

    # Should return empty with correct schema
    assert len(result) == 0
    expected_columns = [
        "geo_id",
        "investment_period",
        "timestep",
        "imports_mw",
        "exports_mw",
        "net_imports_mw",
    ]
    assert list(result.columns) == expected_columns


# Tests for geographic-specific extraction functions
def test_extract_nem_region_transmission_flows_basic(
    network_with_inter_intra_flows, csv_str_to_df
):
    """Test NEM region transmission flows extraction."""
    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            NSW_CNSW,           NSW_CNSW_REZ1
    NSW,            NSW_NNSW,           NaN
    VIC,            VIC,                NaN
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    link_flows = _extract_raw_link_flows(network_with_inter_intra_flows)
    result = extract_nem_region_transmission_flows(link_flows, regions_mapping)

    # Should only have NSW-VIC flows (inter-regional)
    # Intra-regional flows (CNSW-NNSW, REZ1-CNSW) should be filtered out
    assert len(result) == 4  # 2 regions × 2 timesteps
    assert set(result["nem_region_id"].unique()) == {"NSW", "VIC"}

    # Verify NSW exports to VIC
    nsw_flows = result[result["nem_region_id"] == "NSW"]
    assert (nsw_flows["exports_mw"] > 0).all()
    assert (nsw_flows["imports_mw"] == 0).all()

    # Verify VIC imports from NSW
    vic_flows = result[result["nem_region_id"] == "VIC"]
    assert (vic_flows["imports_mw"] > 0).all()
    assert (vic_flows["exports_mw"] == 0).all()


def test_extract_nem_region_transmission_flows_excludes_intra_regional(
    network_with_inter_intra_flows, csv_str_to_df
):
    """Test that intra-regional flows are excluded (critical bug test)."""
    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            NSW_CNSW,           NSW_CNSW_REZ1
    NSW,            NSW_NNSW,           NaN
    VIC,            VIC,                NaN
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    link_flows = _extract_raw_link_flows(network_with_inter_intra_flows)

    # Get all flows (no filtering)
    all_links = link_flows["Link"].unique()
    assert len(all_links) == 3  # NSW-VIC, CNSW-NNSW, REZ1-CNSW

    # Get regional flows (should filter out intra-regional)
    result = extract_nem_region_transmission_flows(link_flows, regions_mapping)

    # Should only count the 1 inter-regional link (NSW-VIC)
    # Not the 2 intra-regional links (CNSW-NNSW and REZ1-CNSW)
    unique_flows = result.groupby(["nem_region_id", "investment_period"]).size()
    # Each region × period should have 2 timesteps
    assert (unique_flows == 2).all()


def test_extract_isp_sub_region_transmission_flows_basic(csv_str_to_df):
    """Test sub-region transmission flows extraction."""
    # Create simple network
    network = pypsa.Network()
    network.set_snapshots(
        pd.MultiIndex.from_product(
            [[2025], pd.date_range("2025-01-01", periods=2, freq="h")],
            names=["period", "timestep"],
        )
    )
    network.add("Bus", "CNSW")
    network.add("Bus", "VIC")
    network.add("Link", "CNSW-VIC", bus0="CNSW", bus1="VIC", isp_name="CNSW-VIC")
    network.links_t.p0["CNSW-VIC"] = [800, 900]

    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    VIC,            VIC
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    link_flows = _extract_raw_link_flows(network)
    result = extract_isp_sub_region_transmission_flows(link_flows, regions_mapping)

    # Should have flows for both sub-regions
    assert set(result["isp_sub_region_id"].unique()) == {"CNSW", "VIC"}
    assert len(result) == 4  # 2 sub-regions × 2 timesteps


def test_extract_isp_sub_region_transmission_flows_excludes_intra_subregion(
    csv_str_to_df,
):
    """Test that intra-sub-region flows (e.g., REZ to sub-region) are excluded."""
    network = pypsa.Network()
    network.set_snapshots(
        pd.MultiIndex.from_product(
            [[2025], pd.date_range("2025-01-01", periods=2, freq="h")],
            names=["period", "timestep"],
        )
    )
    network.add("Bus", "CNSW")
    network.add("Bus", "CNSW_REZ1")
    network.add("Bus", "VIC")

    # Inter-sub-region flow
    network.add("Link", "CNSW-VIC", bus0="CNSW", bus1="VIC", isp_name="CNSW-VIC")
    network.links_t.p0["CNSW-VIC"] = [800, 900]

    # Intra-sub-region flow (REZ within CNSW)
    network.add(
        "Link", "REZ1-CNSW", bus0="CNSW_REZ1", bus1="CNSW", isp_name="REZ1-CNSW"
    )
    network.links_t.p0["REZ1-CNSW"] = [200, 220]

    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               CNSW_REZ1
    VIC,            VIC,                NaN
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    link_flows = _extract_raw_link_flows(network)
    result = extract_isp_sub_region_transmission_flows(link_flows, regions_mapping)

    # Should only have CNSW-VIC flow, not REZ1-CNSW
    assert set(result["isp_sub_region_id"].unique()) == {"CNSW", "VIC"}

    # Verify flows are from the inter-sub-region link only
    cnsw_exports = result[result["isp_sub_region_id"] == "CNSW"]["exports_mw"]
    assert (cnsw_exports > 700).all()  # Should be ~800-900, not ~200-220


def test_extract_rez_transmission_flows_basic(csv_str_to_df):
    """Test REZ transmission flows extraction."""
    network = pypsa.Network()
    network.set_snapshots(
        pd.MultiIndex.from_product(
            [[2025], pd.date_range("2025-01-01", periods=2, freq="h")],
            names=["period", "timestep"],
        )
    )
    network.add("Bus", "REZ1")
    network.add("Bus", "REZ2")
    network.add("Link", "REZ1-REZ2", bus0="REZ1", bus1="REZ2", isp_name="REZ1-REZ2")
    network.links_t.p0["REZ1-REZ2"] = [100, 150]

    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               REZ1
    NSW,            NNSW,               REZ2
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    link_flows = _extract_raw_link_flows(network)
    result = extract_rez_transmission_flows(link_flows, regions_mapping)

    # Should have flows for both REZs
    assert set(result["rez_id"].unique()) == {"REZ1", "REZ2"}
    assert len(result) == 4  # 2 REZs × 2 timesteps


def test_extract_rez_transmission_flows_excludes_non_rez(csv_str_to_df):
    """Test that flows not involving REZs are excluded."""
    network = pypsa.Network()
    network.set_snapshots(
        pd.MultiIndex.from_product(
            [[2025], pd.date_range("2025-01-01", periods=2, freq="h")],
            names=["period", "timestep"],
        )
    )
    network.add("Bus", "REZ1")
    network.add("Bus", "CNSW")  # Sub-region, not REZ

    # Flow between REZ and sub-region (shouldn't appear in REZ results)
    network.add("Link", "REZ1-CNSW", bus0="REZ1", bus1="CNSW", isp_name="REZ1-CNSW")
    network.links_t.p0["REZ1-CNSW"] = [100, 150]

    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               REZ1
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    link_flows = _extract_raw_link_flows(network)
    result = extract_rez_transmission_flows(link_flows, regions_mapping)

    # Should be empty - CNSW is not a REZ, so flow is filtered out
    assert len(result) == 0


def test_multiple_investment_periods(csv_str_to_df):
    """Test that flows are correctly separated by investment period."""
    network = pypsa.Network()
    network.set_snapshots(
        pd.MultiIndex.from_product(
            [[2025, 2030, 2035], pd.date_range("2025-01-01", periods=2, freq="h")],
            names=["period", "timestep"],
        )
    )
    network.add("Bus", "NodeA")
    network.add("Bus", "NodeB")
    network.add("Link", "A-B", bus0="NodeA", bus1="NodeB", isp_name="A-B")
    # Different flows for each period
    network.links_t.p0["A-B"] = [100, 110, 200, 210, 300, 310]

    regions_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    RegionA,        NodeA
    RegionB,        NodeB
    """
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    link_flows = _extract_raw_link_flows(network)
    result = extract_nem_region_transmission_flows(link_flows, regions_mapping)

    # Should have separate entries for each period
    assert set(result["investment_period"].unique()) == {2025, 2030, 2035}

    # Verify different flow values by period
    period_2025 = result[result["investment_period"] == 2025]["exports_mw"].max()
    period_2030 = result[result["investment_period"] == 2030]["exports_mw"].max()
    period_2035 = result[result["investment_period"] == 2035]["exports_mw"].max()

    assert period_2025 < period_2030 < period_2035
