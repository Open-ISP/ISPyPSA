import pandas as pd
import pytest

from ispypsa.translator.custom_constraints import _create_expansion_limit_constraints


def test_create_expansion_limit_constraints_basic(csv_str_to_df):
    """Test basic creation of expansion limit constraints for links and flow paths."""
    
    # Input: links with some extendable
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_existing,  AC,       BUS1,   BUS2,   1000,   False
    PATH1,     PATH1_exp_2025,  AC,       BUS1,   BUS2,   0,      True
    PATH2,     PATH2_exp_2025,  AC,       BUS3,   BUS4,   0,      True
    """
    links = csv_str_to_df(links_csv)
    
    # Input: flow path expansion costs
    flow_paths_csv = """
    flow_path,  additional_network_capacity_mw
    PATH1,      500
    PATH2,      800
    """
    flow_paths = csv_str_to_df(flow_paths_csv)
    
    # No constraint generators or REZ connections
    constraint_generators = None
    rez_connections = None
    
    # Call the function under test
    lhs, rhs = _create_expansion_limit_constraints(
        links, constraint_generators, flow_paths, rez_connections
    )
    
    # Expected LHS output (only extendable links)
    expected_lhs_csv = """
    constraint_name,           variable_name,   component,  attribute,  coefficient
    PATH1_expansion_limit,     PATH1_exp_2025,  Link,       p_nom,      1.0
    PATH2_expansion_limit,     PATH2_exp_2025,  Link,       p_nom,      1.0
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)
    
    # Expected RHS output
    expected_rhs_csv = """
    constraint_name,           rhs,  constraint_type
    PATH1_expansion_limit,     500,  <=
    PATH2_expansion_limit,     800,  <=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)
    
    # Compare DataFrames
    pd.testing.assert_frame_equal(
        lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True),
        expected_lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True)
    )
    
    pd.testing.assert_frame_equal(
        rhs.sort_values("constraint_name").reset_index(drop=True),
        expected_rhs.sort_values("constraint_name").reset_index(drop=True)
    )


def test_create_expansion_limit_constraints_with_generators(csv_str_to_df):
    """Test creation of expansion limit constraints with constraint generators."""
    
    # Input: links with some extendable
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_exp_2025,  AC,       BUS1,   BUS2,   0,      True
    """
    links = csv_str_to_df(links_csv)
    
    # Input: constraint generators
    constraint_generators_csv = """
    constraint_name,  name,             isp_name,  bus,                            p_nom,  p_nom_extendable
    REZ_NSW,          REZ_NSW_exp_2025, REZ_NSW,   bus_for_custom_constraint_gens, 0.0,    True
    REZ_NSW,          REZ_NSW_exp_2030, REZ_NSW,   bus_for_custom_constraint_gens, 0.0,    True
    REZ_VIC,          REZ_VIC_exp_2025, REZ_VIC,   bus_for_custom_constraint_gens, 0.0,    True
    """
    constraint_generators = csv_str_to_df(constraint_generators_csv)
    
    # Input: flow paths and REZ connections
    flow_paths_csv = """
    flow_path,  additional_network_capacity_mw
    PATH1,      500
    """
    flow_paths = csv_str_to_df(flow_paths_csv)
    
    rez_connections_csv = """
    rez_constraint_id,  additional_network_capacity_mw
    REZ_NSW,            1000
    REZ_VIC,            750
    """
    rez_connections = csv_str_to_df(rez_connections_csv)
    
    # Call the function under test
    lhs, rhs = _create_expansion_limit_constraints(
        links, constraint_generators, flow_paths, rez_connections
    )
    
    # Expected LHS output (links and generators)
    expected_lhs_csv = """
    constraint_name,           variable_name,    component,   attribute,  coefficient
    PATH1_expansion_limit,     PATH1_exp_2025,   Link,        p_nom,      1.0
    REZ_NSW_expansion_limit,   REZ_NSW_exp_2025, Generator,   p_nom,      1.0
    REZ_NSW_expansion_limit,   REZ_NSW_exp_2030, Generator,   p_nom,      1.0
    REZ_VIC_expansion_limit,   REZ_VIC_exp_2025, Generator,   p_nom,      1.0
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)
    
    # Expected RHS output
    expected_rhs_csv = """
    constraint_name,           rhs,   constraint_type
    PATH1_expansion_limit,     500,   <=
    REZ_NSW_expansion_limit,   1000,  <=
    REZ_VIC_expansion_limit,   750,   <=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)
    
    # Compare DataFrames
    pd.testing.assert_frame_equal(
        lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True),
        expected_lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True)
    )
    
    pd.testing.assert_frame_equal(
        rhs.sort_values("constraint_name").reset_index(drop=True),
        expected_rhs.sort_values("constraint_name").reset_index(drop=True)
    )


def test_create_expansion_limit_constraints_no_extendable_links(csv_str_to_df):
    """Test when no links are extendable."""
    
    # Input: links with none extendable
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_existing,  AC,       BUS1,   BUS2,   1000,   False
    PATH2,     PATH2_existing,  AC,       BUS3,   BUS4,   2000,   False
    """
    links = csv_str_to_df(links_csv)
    
    # Input: flow paths (but no extendable links to constrain)
    flow_paths_csv = """
    flow_path,  additional_network_capacity_mw
    PATH1,      500
    PATH2,      800
    """
    flow_paths = csv_str_to_df(flow_paths_csv)
    
    # Call the function under test
    lhs, rhs = _create_expansion_limit_constraints(
        links, None, flow_paths, None
    )
    
    # Should return empty DataFrames since no links are extendable
    assert lhs.empty
    assert rhs.empty


def test_create_expansion_limit_constraints_filter_rhs_by_lhs(csv_str_to_df):
    """Test that RHS is filtered to only include constraints with corresponding LHS."""
    
    # Input: links with only PATH1 extendable
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_exp_2025,  AC,       BUS1,   BUS2,   0,      True
    PATH2,     PATH2_existing,  AC,       BUS3,   BUS4,   2000,   False
    """
    links = csv_str_to_df(links_csv)
    
    # Input: flow paths for both PATH1 and PATH2
    flow_paths_csv = """
    flow_path,  additional_network_capacity_mw
    PATH1,      500
    PATH2,      800
    """
    flow_paths = csv_str_to_df(flow_paths_csv)
    
    # Call the function under test
    lhs, rhs = _create_expansion_limit_constraints(
        links, None, flow_paths, None
    )
    
    # Expected LHS output (only PATH1)
    expected_lhs_csv = """
    constraint_name,           variable_name,   component,  attribute,  coefficient
    PATH1_expansion_limit,     PATH1_exp_2025,  Link,       p_nom,      1.0
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)
    
    # Expected RHS output (only PATH1, PATH2 filtered out)
    expected_rhs_csv = """
    constraint_name,           rhs,  constraint_type
    PATH1_expansion_limit,     500,  <=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)
    
    # Compare DataFrames
    pd.testing.assert_frame_equal(
        lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True),
        expected_lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True)
    )
    
    pd.testing.assert_frame_equal(
        rhs.sort_values("constraint_name").reset_index(drop=True),
        expected_rhs.sort_values("constraint_name").reset_index(drop=True)
    )


def test_create_expansion_limit_constraints_all_none(csv_str_to_df):
    """Test when all inputs are None."""
    
    # Call the function under test with all None
    lhs, rhs = _create_expansion_limit_constraints(
        None, None, None, None
    )
    
    # Should return empty DataFrames
    assert lhs.empty
    assert rhs.empty


def test_create_expansion_limit_constraints_empty_inputs(csv_str_to_df):
    """Test when all inputs are empty DataFrames."""
    
    # Call the function under test with empty DataFrames
    lhs, rhs = _create_expansion_limit_constraints(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    )
    
    # Should return empty DataFrames
    assert lhs.empty
    assert rhs.empty


def test_create_expansion_limit_constraints_mixed_empty_none(csv_str_to_df):
    """Test with mix of None and empty inputs.

    This output is expect from _create_expansion_limit_constraints, but will raise an error at the
    validation stage in _translate_custom_constraints
    """
    
    # Input: extendable links
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_exp_2025,  AC,       BUS1,   BUS2,   0,      True
    """
    links = csv_str_to_df(links_csv)
    
    # Flow paths is None, REZ connections is empty
    flow_paths = None
    rez_connections = pd.DataFrame()
    
    # Call the function under test
    lhs, rhs = _create_expansion_limit_constraints(
        links, None, flow_paths, rez_connections
    )
    
    # LHS should have the link constraint but RHS should be empty (no RHS data)
    expected_lhs_csv = """
    constraint_name,           variable_name,   component,  attribute,  coefficient
    PATH1_expansion_limit,     PATH1_exp_2025,  Link,       p_nom,      1.0
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)
    
    pd.testing.assert_frame_equal(
        lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True),
        expected_lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True)
    )
    
    # RHS should be empty since no RHS data was provided
    assert rhs.empty


def test_create_expansion_limit_constraints_only_rhs_no_lhs(csv_str_to_df):
    """Test when RHS data exists but no LHS (no extendable components)."""
    
    # Input: no links
    links = None
    
    # Input: flow paths
    flow_paths_csv = """
    flow_path,  additional_network_capacity_mw
    PATH1,      500
    PATH2,      800
    """
    flow_paths = csv_str_to_df(flow_paths_csv)
    
    # Call the function under test
    lhs, rhs = _create_expansion_limit_constraints(
        links, None, flow_paths, None
    )
    
    # Should return empty DataFrames since there's no LHS
    assert lhs.empty
    assert rhs.empty


def test_create_expansion_limit_constraints_missing_rhs_columns(csv_str_to_df):
    """Test that ValueError is raised when RHS data is missing expected columns."""
    
    # Input: links
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_exp_2025,  AC,       BUS1,   BUS2,   0,      True
    """
    links = csv_str_to_df(links_csv)
    
    # Input: flow paths with wrong column names
    flow_paths_csv = """
    wrong_column,  wrong_capacity_column
    PATH1,         500
    """
    flow_paths = csv_str_to_df(flow_paths_csv)
    
    # Should raise ValueError due to missing required columns
    with pytest.raises(ValueError, match="RHS components missing required columns after processing: \\['constraint_name', 'rhs'\\]"):
        _create_expansion_limit_constraints(
            links, None, flow_paths, None
        )


def test_create_expansion_limit_constraints_duplicate_constraints(csv_str_to_df):
    """Test handling of duplicate constraint names from different sources."""
    
    # Input: links
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    TEST1,     TEST1_exp_2025,  AC,       BUS1,   BUS2,   0,      True
    """
    links = csv_str_to_df(links_csv)
    
    # Input: flow paths and REZ connections with same constraint name
    flow_paths_csv = """
    flow_path,  additional_network_capacity_mw
    TEST1,      500
    """
    flow_paths = csv_str_to_df(flow_paths_csv)
    
    rez_connections_csv = """
    rez_constraint_id,  additional_network_capacity_mw
    TEST1,              1000
    """
    rez_connections = csv_str_to_df(rez_connections_csv)
    
    # Call the function under test
    lhs, rhs = _create_expansion_limit_constraints(
        links, None, flow_paths, rez_connections
    )
    
    # Expected LHS output
    expected_lhs_csv = """
    constraint_name,         variable_name,   component,  attribute,  coefficient
    TEST1_expansion_limit,   TEST1_exp_2025,  Link,       p_nom,      1.0
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)
    
    # Expected RHS output (both entries concatenated)
    expected_rhs_csv = """
    constraint_name,         rhs,   constraint_type
    TEST1_expansion_limit,   500,   <=
    TEST1_expansion_limit,   1000,  <=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)
    
    # Compare DataFrames
    pd.testing.assert_frame_equal(
        lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True),
        expected_lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True)
    )
    
    pd.testing.assert_frame_equal(
        rhs.sort_values(["constraint_name", "rhs"]).reset_index(drop=True),
        expected_rhs.sort_values(["constraint_name", "rhs"]).reset_index(drop=True)
    )