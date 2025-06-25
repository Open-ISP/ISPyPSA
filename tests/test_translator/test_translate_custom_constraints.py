import pandas as pd
import pytest

from ispypsa.translator.custom_constraints import _translate_custom_constraints


def test_translate_custom_constraints_duplicate_constraint_names(csv_str_to_df):
    """Test that ValueError is raised when duplicate constraint names exist in RHS."""

    # Input: manual custom constraints
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    CONS1,          generator_capacity,  GEN1,     1.0
    CONS2,          generator_capacity,  GEN2,     1.0
    """

    custom_constraints_rhs_csv = """
    constraint_id,  summer_typical
    CONS1,          5000
    CONS2,          3000
    """

    # Input: flow path expansion costs (will create CONS1_expansion_limit)
    flow_path_expansion_costs_csv = """
    flow_path,  additional_network_capacity_mw
    CONS1,      500
    """

    # Input: links
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    CONS1,     CONS1_exp_2025,  AC,       BUS1,   BUS2,   0,      True
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
        "flow_path_expansion_costs": csv_str_to_df(flow_path_expansion_costs_csv),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # This will create two constraints named CONS1:
    # - One from manual constraints
    # - One from flow path expansion limits (CONS1_expansion_limit)
    # These don't conflict because they have different names
    # Let's create a real duplicate by having duplicate manual constraints

    # Create duplicate manual constraints
    custom_constraints_rhs_with_dup_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          5000,  <=
    CONS1,          6000,  <=
    CONS2,          3000,  <=
    """

    ispypsa_tables_with_dup = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_with_dup_csv),
    }

    # Should raise ValueError due to duplicate CONS1 in RHS
    with pytest.raises(
        ValueError,
        match="Duplicate constraint names found in custom constraints RHS: \\['CONS1'\\]",
    ):
        _translate_custom_constraints(config, ispypsa_tables_with_dup, links)


def test_translate_custom_constraints_basic_integration(csv_str_to_df):
    """Test basic integration of manual constraints and expansion limit constraints."""

    # Input: manual custom constraints
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    MANUAL1,        generator_capacity,  GEN1,     1.0
    MANUAL2,        link_flow,           PATH1,    2.0
    """

    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    MANUAL1,        5000,  <=
    MANUAL2,        3000,  <=
    """

    # Input: flow path expansion costs
    flow_path_expansion_costs_csv = """
    flow_path,  additional_network_capacity_mw
    PATH1,      500
    PATH2,      800
    """

    # Input: links
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_existing,  AC,       BUS1,   BUS2,   1000,   False
    PATH1,     PATH1_exp_2025,  AC,       BUS1,   BUS2,   0,      True
    PATH2,     PATH2_exp_2025,  AC,       BUS3,   BUS4,   0,      True
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
        "flow_path_expansion_costs": csv_str_to_df(flow_path_expansion_costs_csv),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Call the function under test
    result = _translate_custom_constraints(config, ispypsa_tables, links)

    # Check that both manual and expansion limit constraints are present
    assert "custom_constraints_lhs" in result
    assert "custom_constraints_rhs" in result

    # Expected LHS should have manual constraints and expansion limit constraints
    lhs = result["custom_constraints_lhs"]
    rhs = result["custom_constraints_rhs"]

    # Check constraint names
    expected_constraint_names = {
        "MANUAL1",
        "MANUAL2",
        "PATH1_expansion_limit",
        "PATH2_expansion_limit",
    }
    assert set(lhs["constraint_name"].unique()) == expected_constraint_names
    assert set(rhs["constraint_name"].unique()) == expected_constraint_names

    # Verify specific constraints
    manual1_lhs = lhs[lhs["constraint_name"] == "MANUAL1"]
    assert len(manual1_lhs) == 1
    assert manual1_lhs.iloc[0]["variable_name"] == "GEN1"
    assert manual1_lhs.iloc[0]["component"] == "Generator"

    # Link flow constraints should be expanded
    manual2_lhs = lhs[lhs["constraint_name"] == "MANUAL2"]
    assert len(manual2_lhs) == 2  # PATH1_existing and PATH1_exp_2025

    # Check RHS values
    manual1_rhs = rhs[rhs["constraint_name"] == "MANUAL1"]
    assert manual1_rhs.iloc[0]["rhs"] == 5000

    path1_rhs = rhs[rhs["constraint_name"] == "PATH1_expansion_limit"]
    assert path1_rhs.iloc[0]["rhs"] == 500


def test_translate_custom_constraints_no_constraints(csv_str_to_df):
    """Test when no custom constraints are provided."""

    # Empty inputs
    ispypsa_tables = {}
    links = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Call the function under test
    result = _translate_custom_constraints(config, ispypsa_tables, links)

    # Should return empty dictionary
    assert result == {}


def test_translate_custom_constraints_mismatched_lhs_rhs(csv_str_to_df):
    """Test that ValueError is raised when LHS and RHS constraints don't match."""

    # Input: manual constraints with mismatched LHS/RHS
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    CONS1,          generator_capacity,  GEN1,     1.0
    CONS2,          generator_capacity,  GEN2,     1.0
    """

    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          5000,  <=
    CONS3,          3000,  <=
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Should raise ValueError due to mismatched constraints
    with pytest.raises(
        ValueError,
        match="LHS constraints do not have corresponding RHS definitions.*CONS2",
    ):
        _translate_custom_constraints(config, ispypsa_tables, links)


def test_translate_custom_constraints_duplicate_from_different_sources(csv_str_to_df):
    """Test duplicate constraint names from different sources."""

    # Input: manual constraints
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    PATH1,          generator_capacity,  GEN1,     1.0
    PATH1,          generator_capacity,  GEN2,     1.0
    """

    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    PATH1,          5000,  <=
    PATH1,          3000,  <=
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # This creates duplicate PATH1 constraints in RHS
    # Should raise ValueError
    with pytest.raises(
        ValueError,
        match="Duplicate constraint names found in custom constraints RHS: \\['PATH1'\\]",
    ):
        _translate_custom_constraints(config, ispypsa_tables, links)
