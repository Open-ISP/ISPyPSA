import pandas as pd
import pytest

from ispypsa.plotting.transmission import (
    prepare_flow_data,
    prepare_flow_path_capacity_by_region,
    prepare_rez_capacity_by_region,
    prepare_transmission_capacity_by_region,
)


def test_prepare_transmission_capacity_by_region_intra_region(csv_str_to_df):
    """Test capacity allocation for intra-region transmission lines."""

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    CNSW-SNSW, flow_path,   2030,        CNSW,       SNSW,     1000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    NSW,            SNSW
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_transmission_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: 100% of capacity allocated to NSW (both nodes in same region)
    expected_csv = """
    build_year,  nem_region_id,  capacity_gw
    2030,        NSW,            1.0
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
        expected.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
    )


def test_prepare_transmission_capacity_by_region_inter_region(csv_str_to_df):
    """Test capacity allocation for inter-region transmission lines."""

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    NSW-VIC,   flow_path,   2030,        SNSW,       VIC,      2000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            SNSW
    VIC,            VIC
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_transmission_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: 50% to each region (1000 MW = 1 GW each)
    expected_csv = """
    build_year,  nem_region_id,  capacity_gw
    2030,        NSW,            1.0
    2030,        VIC,            1.0
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
        expected.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
    )


def test_prepare_transmission_capacity_by_region_multiple_years(csv_str_to_df):
    """Test cumulative capacity calculation across multiple years."""

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    NSW-VIC,   flow_path,   2030,        SNSW,       VIC,      1000
    NSW-VIC,   flow_path,   2035,        SNSW,       VIC,      1000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            SNSW
    VIC,            VIC
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_transmission_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: Cumulative values (500 MW in 2030, 1000 MW in 2035 for each region)
    expected_csv = """
    build_year,  nem_region_id,  capacity_gw
    2030,        NSW,            0.5
    2030,        VIC,            0.5
    2035,        NSW,            1.0
    2035,        VIC,            1.0
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
        expected.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
    )


def test_prepare_transmission_capacity_by_region_with_rez(csv_str_to_df):
    """Test capacity allocation with REZ connections."""

    transmission_expansion_csv = """
    isp_name,               isp_type,  build_year,  node_from,                node_to,  forward_direction_nominal_capacity_mw
    Central-West__Orana,    rez,       2030,        Central-West__Orana__REZ, CNSW,     3000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               Central-West__Orana__REZ
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_transmission_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: 100% to NSW (REZ and node both in same region)
    expected_csv = """
    build_year,  nem_region_id,  capacity_gw
    2030,        NSW,            3.0
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
        expected.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
    )


def test_prepare_transmission_capacity_by_region_filters_rez_no_limit(csv_str_to_df):
    """Test that rez_no_limit type is filtered out."""

    transmission_expansion_csv = """
    isp_name,  isp_type,       build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    NSW-VIC,   flow_path,      2030,        SNSW,       VIC,      1000
    REZ-unlimited, rez_no_limit, 2030,      CNSW,       SNSW,     9999
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            SNSW
    NSW,            CNSW
    VIC,            VIC
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_transmission_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: Only NSW-VIC should be included (500 MW each)
    expected_csv = """
    build_year,  nem_region_id,  capacity_gw
    2030,        NSW,            0.5
    2030,        VIC,            0.5
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
        expected.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
    )


def test_prepare_transmission_capacity_by_region_empty_input(csv_str_to_df):
    """Test with empty transmission expansion data."""

    transmission_expansion = pd.DataFrame(
        columns=[
            "isp_name",
            "isp_type",
            "build_year",
            "node_from",
            "node_to",
            "forward_direction_nominal_capacity_mw",
        ]
    )

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    """
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_transmission_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: Empty DataFrame with correct columns
    assert result.empty
    assert list(result.columns) == ["build_year", "nem_region_id", "capacity_gw"]


def test_prepare_transmission_capacity_by_region_multiple_lines_same_year(
    csv_str_to_df,
):
    """Test aggregation of multiple transmission lines in the same year and region."""

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    Line1,     flow_path,   2030,        CNSW,       SNSW,     1000
    Line2,     flow_path,   2030,        CNSW,       NNSW,     500
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    NSW,            SNSW
    NSW,            NNSW
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_transmission_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: NSW should have 1.5 GW (1000 + 500 MW)
    expected_csv = """
    build_year,  nem_region_id,  capacity_gw
    2030,        NSW,            1.5
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
        expected.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
    )


def test_prepare_transmission_capacity_by_region_mixed_intra_inter_same_year(
    csv_str_to_df,
):
    """Test interaction between intra-region and inter-region expansion in the same year."""

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    Intra,     flow_path,   2030,        CNSW,       SNSW,     1000
    Inter,     flow_path,   2030,        SNSW,       VIC,      2000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    NSW,            SNSW
    VIC,            VIC
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_transmission_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected:
    # NSW: 1000 MW (intra) + 1000 MW (50% of inter) = 2000 MW = 2.0 GW
    # VIC: 1000 MW (50% of inter) = 1.0 GW
    expected_csv = """
    build_year,  nem_region_id,  capacity_gw
    2030,        NSW,            2.0
    2030,        VIC,            1.0
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
        expected.sort_values(["build_year", "nem_region_id"]).reset_index(drop=True),
    )


def test_prepare_rez_capacity_by_region_basic(csv_str_to_df):
    """Test REZ capacity data preparation."""

    transmission_expansion_csv = """
    isp_name,               isp_type,  build_year,  node_from,                node_to,  forward_direction_nominal_capacity_mw
    Central-West__Orana,    rez,       2030,        Central-West__Orana__REZ, CNSW,     3000
    New__England,           rez,       2030,        New__England__REZ,        NNSW,     2000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               Central-West__Orana__REZ
    NSW,            NNSW,               New__England__REZ
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_rez_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected output
    expected_csv = """
    nem_region_id,  rez_id,                       build_year,  capacity_mw
    NSW,            Central-West__Orana__REZ,     2030,        3000
    NSW,            New__England__REZ,            2030,        2000
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["nem_region_id", "rez_id"]).reset_index(drop=True),
        expected.sort_values(["nem_region_id", "rez_id"]).reset_index(drop=True),
    )


def test_prepare_rez_capacity_by_region_cumulative(csv_str_to_df):
    """Test cumulative REZ capacity calculation."""

    transmission_expansion_csv = """
    isp_name,               isp_type,  build_year,  node_from,                node_to,  forward_direction_nominal_capacity_mw
    Central-West__Orana,    rez,       2030,        Central-West__Orana__REZ, CNSW,     1000
    Central-West__Orana,    rez,       2035,        Central-West__Orana__REZ, CNSW,     500
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               Central-West__Orana__REZ
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_rez_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: Cumulative values
    expected_csv = """
    nem_region_id,  rez_id,                       build_year,  capacity_mw
    NSW,            Central-West__Orana__REZ,     2030,        1000
    NSW,            Central-West__Orana__REZ,     2035,        1500
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_prepare_flow_path_capacity_by_region_intra_region(csv_str_to_df):
    """Test flow path capacity for intra-region paths."""

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    CNSW-SNSW, flow_path,   2030,        CNSW,       SNSW,     1000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    NSW,            SNSW
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_flow_path_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: One entry for NSW (intra-region)
    expected_csv = """
    nem_region_id,  flow_path,  build_year,  capacity_mw
    NSW,            CNSW-SNSW,  2030,        1000
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_prepare_flow_path_capacity_by_region_inter_region(csv_str_to_df):
    """Test flow path capacity for inter-region paths (appears in both regions)."""

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    NSW-VIC,   flow_path,   2030,        SNSW,       VIC,      2000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            SNSW
    VIC,            VIC
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_flow_path_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: Appears in both regions
    expected_csv = """
    nem_region_id,  flow_path,  build_year,  capacity_mw
    NSW,            NSW-VIC,    2030,        2000
    VIC,            NSW-VIC,    2030,        2000
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["nem_region_id", "flow_path"]).reset_index(drop=True),
        expected.sort_values(["nem_region_id", "flow_path"]).reset_index(drop=True),
    )


def test_prepare_flow_path_capacity_by_region_cumulative(csv_str_to_df):
    """Test cumulative flow path capacity calculation."""

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    CNSW-SNSW, flow_path,   2030,        CNSW,       SNSW,     1000
    CNSW-SNSW, flow_path,   2035,        CNSW,       SNSW,     500
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    NSW,            SNSW
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_flow_path_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: Cumulative values
    expected_csv = """
    nem_region_id,  flow_path,  build_year,  capacity_mw
    NSW,            CNSW-SNSW,  2030,        1000
    NSW,            CNSW-SNSW,  2035,        1500
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_prepare_rez_capacity_by_region_empty(csv_str_to_df):
    """Test with no REZ data."""

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  node_from,  node_to,  forward_direction_nominal_capacity_mw
    CNSW-SNSW, flow_path,   2030,        CNSW,       SNSW,     1000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               NaN
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_rez_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: Empty with correct columns
    assert result.empty
    assert list(result.columns) == [
        "nem_region_id",
        "rez_id",
        "build_year",
        "capacity_mw",
    ]


def test_prepare_flow_path_capacity_by_region_empty(csv_str_to_df):
    """Test with no flow path data."""

    transmission_expansion_csv = """
    isp_name,               isp_type,  build_year,  node_from,                node_to,  forward_direction_nominal_capacity_mw
    Central-West__Orana,    rez,       2030,        Central-West__Orana__REZ, CNSW,     3000
    """

    regions_and_zones_mapping_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

    result = prepare_flow_path_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    # Expected: Empty with correct columns
    assert result.empty
    assert list(result.columns) == [
        "nem_region_id",
        "flow_path",
        "build_year",
        "capacity_mw",
    ]


def test_prepare_flow_data_basic(csv_str_to_df):
    """Test basic flow data preparation with isp_type mapping and timestep conversion."""

    flows_csv = """
    isp_name,  investment_period,  timestep,  flow
    NSW-VIC,   2030,               2030-01-01T00:00:00, 100
    NSW-VIC,   2030,               2030-01-01T01:00:00, 150
    """

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  forward_direction_nominal_capacity_mw,  reverse_direction_nominal_capacity_mw
    NSW-VIC,   flow_path,   2030,        500,                                     400
    """

    flows = csv_str_to_df(flows_csv)
    transmission_expansion = csv_str_to_df(transmission_expansion_csv)

    result = prepare_flow_data(flows, transmission_expansion)

    # Verify isp_type was mapped
    assert all(result["isp_type"] == "flow_path")

    # Verify timestep is datetime
    assert pd.api.types.is_datetime64_any_dtype(result["timestep"])

    # Verify week_starting exists and is correct type
    assert "week_starting" in result.columns

    # Verify capacity limits are present
    assert "forward_limit" in result.columns
    assert "reverse_limit" in result.columns
    assert all(result["forward_limit"] == 500)
    assert all(result["reverse_limit"] == 400)


def test_prepare_flow_data_capacity_limits_single_period(csv_str_to_df):
    """Test capacity limit calculation for a single investment period."""

    flows_csv = """
    isp_name,  investment_period,  timestep,  flow
    NSW-VIC,   2030,               2030-01-01T00:00:00, 100
    """

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  forward_direction_nominal_capacity_mw,  reverse_direction_nominal_capacity_mw
    NSW-VIC,   flow_path,   2030,        1000,                                    800
    """

    flows = csv_str_to_df(flows_csv)
    transmission_expansion = csv_str_to_df(transmission_expansion_csv)

    result = prepare_flow_data(flows, transmission_expansion)

    # Verify limits match transmission expansion
    assert result["forward_limit"].iloc[0] == 1000
    assert result["reverse_limit"].iloc[0] == 800


def test_prepare_flow_data_capacity_limits_cumulative(csv_str_to_df):
    """Test capacity limit calculation with cumulative capacity across build years."""

    flows_csv = """
    isp_name,  investment_period,  timestep,  flow
    NSW-VIC,   2030,               2030-01-01T00:00:00, 100
    NSW-VIC,   2035,               2035-01-01T00:00:00, 200
    """

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  forward_direction_nominal_capacity_mw,  reverse_direction_nominal_capacity_mw
    NSW-VIC,   flow_path,   2030,        500,                                     400
    NSW-VIC,   flow_path,   2035,        300,                                     200
    """

    flows = csv_str_to_df(flows_csv)
    transmission_expansion = csv_str_to_df(transmission_expansion_csv)

    result = prepare_flow_data(flows, transmission_expansion)

    # For 2030: only 2030 capacity
    result_2030 = result[result["investment_period"] == 2030]
    assert result_2030["forward_limit"].iloc[0] == 500
    assert result_2030["reverse_limit"].iloc[0] == 400

    # For 2035: cumulative (2030 + 2035)
    result_2035 = result[result["investment_period"] == 2035]
    assert result_2035["forward_limit"].iloc[0] == 800  # 500 + 300
    assert result_2035["reverse_limit"].iloc[0] == 600  # 400 + 200


def test_prepare_flow_data_multiple_lines(csv_str_to_df):
    """Test flow data preparation with multiple transmission lines."""

    flows_csv = """
    isp_name,  investment_period,  timestep,  flow
    NSW-VIC,   2030,               2030-01-01T00:00:00, 100
    NSW-QLD,   2030,               2030-01-01T00:00:00, 50
    """

    transmission_expansion_csv = """
    isp_name,  isp_type,    build_year,  forward_direction_nominal_capacity_mw,  reverse_direction_nominal_capacity_mw
    NSW-VIC,   flow_path,   2030,        1000,                                    800
    NSW-QLD,   flow_path,   2030,        600,                                     500
    """

    flows = csv_str_to_df(flows_csv)
    transmission_expansion = csv_str_to_df(transmission_expansion_csv)

    result = prepare_flow_data(flows, transmission_expansion)

    # Verify NSW-VIC limits
    nsw_vic = result[result["isp_name"] == "NSW-VIC"]
    assert nsw_vic["forward_limit"].iloc[0] == 1000
    assert nsw_vic["reverse_limit"].iloc[0] == 800

    # Verify NSW-QLD limits
    nsw_qld = result[result["isp_name"] == "NSW-QLD"]
    assert nsw_qld["forward_limit"].iloc[0] == 600
    assert nsw_qld["reverse_limit"].iloc[0] == 500


def test_prepare_flow_data_rez_no_limit(csv_str_to_df):
    """Test flow data preparation with rez_no_limit type."""

    flows_csv = """
    isp_name,      investment_period,  timestep,  flow
    REZ-unlimited, 2030,               2030-01-01T00:00:00, 500
    """

    transmission_expansion_csv = """
    isp_name,      isp_type,       build_year,  forward_direction_nominal_capacity_mw,  reverse_direction_nominal_capacity_mw
    REZ-unlimited, rez_no_limit,   2030,        0,                                       0
    """

    flows = csv_str_to_df(flows_csv)
    transmission_expansion = csv_str_to_df(transmission_expansion_csv)

    result = prepare_flow_data(flows, transmission_expansion)

    # Verify isp_type was mapped correctly
    assert result["isp_type"].iloc[0] == "rez_no_limit"

    # Limits should be 0 for rez_no_limit
    assert result["forward_limit"].iloc[0] == 0
    assert result["reverse_limit"].iloc[0] == 0


def test_prepare_flow_data_empty_input():
    """Test with empty flow data."""

    flows = pd.DataFrame(columns=["isp_name", "investment_period", "timestep", "flow"])
    transmission_expansion = pd.DataFrame(
        columns=[
            "isp_name",
            "isp_type",
            "build_year",
            "forward_direction_nominal_capacity_mw",
            "reverse_direction_nominal_capacity_mw",
        ]
    )

    result = prepare_flow_data(flows, transmission_expansion)

    # Should return empty DataFrame
    assert result.empty
