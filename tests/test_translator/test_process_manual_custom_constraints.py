import pandas as pd
import pytest

from ispypsa.translator.custom_constraints import _process_manual_custom_constraints


def test_process_manual_custom_constraints_basic(csv_str_to_df):
    """Test basic processing of manual custom constraints without REZ expansion."""

    # Input: custom_constraints_lhs
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    CONS1,          generator_capacity,  GEN1,     1.0
    CONS1,          generator_capacity,  GEN2,     1.0
    CONS2,          link_flow,           PATH1,    2.0
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,  constraint_type
    CONS1,          5000, <=
    CONS2,          3000, >=
    """

    # Input: links
    links_csv = """
    isp_name,  name,          carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_existing, AC,      BUS1,   BUS2,   1000,   False
    PATH1,     PATH1_exp_2025, AC,      BUS1,   BUS2,   0,      True
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration with REZ expansion disabled
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Call the function under test
    lhs, rhs, generators = _process_manual_custom_constraints(
        config, ispypsa_tables, links
    )

    # Expected RHS output
    expected_rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    CONS2,            3000,  >=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)

    # Expected LHS output
    expected_lhs_csv = """
    constraint_name,  variable_name,    coefficient,  component,   attribute
    CONS1,            GEN1,             1.0,          Generator,   p_nom
    CONS1,            GEN2,             1.0,          Generator,   p_nom
    CONS2,            PATH1_existing,   2.0,          Link,        p
    CONS2,            PATH1_exp_2025,   2.0,          Link,        p
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)

    # Sort DataFrames for comparison
    lhs_sorted = lhs.sort_values(["constraint_name", "variable_name"]).reset_index(
        drop=True
    )
    expected_lhs_sorted = expected_lhs.sort_values(
        ["constraint_name", "variable_name"]
    ).reset_index(drop=True)

    rhs_sorted = rhs.sort_values("constraint_name").reset_index(drop=True)
    expected_rhs_sorted = expected_rhs.sort_values("constraint_name").reset_index(
        drop=True
    )

    # Assert results
    pd.testing.assert_frame_equal(lhs_sorted, expected_lhs_sorted)
    pd.testing.assert_frame_equal(rhs_sorted, expected_rhs_sorted)

    # When REZ expansion is disabled, generators should be None
    assert generators is None


def test_process_manual_custom_constraints_empty_tables(csv_str_to_df):
    """Test processing when custom constraint tables are empty."""

    # Create empty input data
    ispypsa_tables = {}
    links = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Call the function under test
    lhs, rhs, generators = _process_manual_custom_constraints(
        config, ispypsa_tables, links
    )

    # All outputs should be empty DataFrames
    assert lhs.empty
    assert rhs.empty
    assert generators is None


def test_process_manual_custom_constraints_missing_rhs(csv_str_to_df):
    """Test that ValueError is raised when LHS exists but RHS is missing."""

    # Input: only custom_constraints_lhs (missing RHS)
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    CONS1,          generator_capacity,  GEN1,     1.0
    """

    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
    }
    links = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Should raise ValueError for incomplete tables
    with pytest.raises(
        ValueError, match="Incomplete manual custom constraints tables provided"
    ):
        _process_manual_custom_constraints(config, ispypsa_tables, links)


def test_process_manual_custom_constraints_missing_lhs(csv_str_to_df):
    """Test that ValueError is raised when RHS exists but LHS is missing."""

    # Input: only custom_constraints_rhs (missing LHS)
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,  constraint_type
    CONS1,          5000, <=
    """

    ispypsa_tables = {
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Should raise ValueError for incomplete tables
    with pytest.raises(
        ValueError, match="Incomplete manual custom constraints tables provided"
    ):
        _process_manual_custom_constraints(config, ispypsa_tables, links)


def test_process_manual_custom_constraints_with_rez_expansion(csv_str_to_df):
    """Test processing with REZ expansion enabled and custom generators."""

    # Input: custom_constraints_lhs
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    REZ_NSW,        generator_capacity,  GEN1,     1.0
    REZ_VIC,        generator_capacity,  GEN2,     1.0
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    REZ_NSW,        10000, <=
    REZ_VIC,        8000,  <=
    """

    # Input: REZ transmission expansion costs
    rez_transmission_expansion_costs_csv = """
    rez_constraint_id,  2024_25_$/mw,  2029_30_$/mw,  2034_35_$/mw
    REZ_NSW,            100,           110,           120
    REZ_VIC,            150,           160,           170
    REZ_QLD,            200,           210,           220
    """

    # Input: links
    links_csv = """
    isp_name,  name,          carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_existing, AC,      BUS1,   BUS2,   1000,   False
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
        "rez_transmission_expansion_costs": csv_str_to_df(
            rez_transmission_expansion_costs_csv
        ),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration with REZ expansion enabled
    class MockNetworkConfig:
        rez_transmission_expansion = True
        annuitisation_lifetime = 30

    class MockTemporalConfig:
        class MockCapacityExpansion:
            investment_periods = [2025, 2030, 2035]

        capacity_expansion = MockCapacityExpansion()
        year_type = "fy"

    class MockConfig:
        network = MockNetworkConfig()
        temporal = MockTemporalConfig()
        wacc = 0.07

    config = MockConfig()

    # Call the function under test
    lhs, rhs, generators = _process_manual_custom_constraints(
        config, ispypsa_tables, links
    )

    # Expected RHS output
    expected_rhs_csv = """
    constraint_name,  rhs,   constraint_type
    REZ_NSW,          10000, <=
    REZ_VIC,          8000,  <=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)

    # Assert RHS is correct
    rhs_sorted = rhs.sort_values("constraint_name").reset_index(drop=True)
    expected_rhs_sorted = expected_rhs.sort_values("constraint_name").reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(rhs_sorted, expected_rhs_sorted)

    # Expected generators output
    expected_generators_csv = """
    name,               isp_name,  bus,                            p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
    REZ_NSW_exp_2025,   REZ_NSW,   bus_for_custom_constraint_gens,  0.0,    True,              2025,        inf,       9.43396
    REZ_NSW_exp_2030,   REZ_NSW,   bus_for_custom_constraint_gens,  0.0,    True,              2030,        inf,       10.37736
    REZ_NSW_exp_2035,   REZ_NSW,   bus_for_custom_constraint_gens,  0.0,    True,              2035,        inf,       11.32075
    REZ_VIC_exp_2025,   REZ_VIC,   bus_for_custom_constraint_gens,  0.0,    True,              2025,        inf,       14.15094
    REZ_VIC_exp_2030,   REZ_VIC,   bus_for_custom_constraint_gens,  0.0,    True,              2030,        inf,       15.09434
    REZ_VIC_exp_2035,   REZ_VIC,   bus_for_custom_constraint_gens,  0.0,    True,              2035,        inf,       16.03774
    """
    expected_generators = csv_str_to_df(expected_generators_csv)

    # Compare generators (excluding capital_cost which is calculated)
    generators_sorted = generators.sort_values("name").reset_index(drop=True)
    expected_generators_sorted = expected_generators.sort_values("name").reset_index(
        drop=True
    )

    # Compare all columns except capital_cost
    cols_to_compare = [
        "name",
        "isp_name",
        "bus",
        "p_nom",
        "p_nom_extendable",
        "build_year",
    ]
    pd.testing.assert_frame_equal(
        generators_sorted[cols_to_compare], expected_generators_sorted[cols_to_compare]
    )

    # Check lifetime is infinity
    assert all(generators["lifetime"] == float("inf"))

    # Check capital costs are positive
    assert all(generators["capital_cost"] > 0)

    # Expected LHS output (including both original and generator constraints)
    expected_lhs_csv = """
    constraint_name,  variable_name,      coefficient,  component,   attribute
    REZ_NSW,          GEN1,               1.0,          Generator,   p_nom
    REZ_VIC,          GEN2,               1.0,          Generator,   p_nom
    REZ_NSW,          REZ_NSW_exp_2025,   -1.0,         Generator,   p_nom
    REZ_NSW,          REZ_NSW_exp_2030,   -1.0,         Generator,   p_nom
    REZ_NSW,          REZ_NSW_exp_2035,   -1.0,         Generator,   p_nom
    REZ_VIC,          REZ_VIC_exp_2025,   -1.0,         Generator,   p_nom
    REZ_VIC,          REZ_VIC_exp_2030,   -1.0,         Generator,   p_nom
    REZ_VIC,          REZ_VIC_exp_2035,   -1.0,         Generator,   p_nom
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)

    # Compare LHS
    lhs_sorted = lhs.sort_values(
        ["constraint_name", "coefficient", "variable_name"]
    ).reset_index(drop=True)
    expected_lhs_sorted = expected_lhs.sort_values(
        ["constraint_name", "coefficient", "variable_name"]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(lhs_sorted, expected_lhs_sorted)


def test_process_manual_custom_constraints_mixed_term_types(csv_str_to_df):
    """Test processing with mixed term types (generator capacity, link flow, generator output)."""

    # Input: custom_constraints_lhs with mixed term types
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,   coefficient
    CONS1,          generator_capacity,  GEN1,      1.0
    CONS1,          link_flow,           PATH1,     -0.5
    CONS1,          generator_output,    GEN4,      2.0
    CONS2,          generator_capacity,  GEN2,      3.0
    CONS2,          generator_capacity,  GEN3,      -1.5
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          15000, <=
    CONS2,          20000, <=
    """

    # Input: links
    links_csv = """
    isp_name,  name,            carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_existing,  AC,       BUS1,   BUS2,   1000,   False
    PATH1,     PATH1_exp_2030,  AC,       BUS1,   BUS2,   0,      True
    PATH2,     PATH2_existing,  AC,       BUS3,   BUS4,   2000,   False
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Call the function under test
    lhs, rhs, generators = _process_manual_custom_constraints(
        config, ispypsa_tables, links
    )

    # Expected RHS output
    expected_rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            15000, <=
    CONS2,            20000, <=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)

    # Assert RHS is correct
    rhs_sorted = rhs.sort_values("constraint_name").reset_index(drop=True)
    expected_rhs_sorted = expected_rhs.sort_values("constraint_name").reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(rhs_sorted, expected_rhs_sorted)

    # Expected LHS output with mixed term types
    expected_lhs_csv = """
    constraint_name,  variable_name,    coefficient,  component,   attribute
    CONS1,            GEN1,             1.0,          Generator,   p_nom
    CONS1,            GEN4,             2.0,          Generator,   p
    CONS1,            PATH1_existing,   -0.5,         Link,        p
    CONS1,            PATH1_exp_2030,   -0.5,         Link,        p
    CONS2,            GEN2,             3.0,          Generator,   p_nom
    CONS2,            GEN3,             -1.5,         Generator,   p_nom
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)

    # Compare LHS
    lhs_sorted = lhs.sort_values(["constraint_name", "variable_name"]).reset_index(
        drop=True
    )
    expected_lhs_sorted = expected_lhs.sort_values(
        ["constraint_name", "variable_name"]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(lhs_sorted, expected_lhs_sorted)

    # Generators should be None when REZ expansion is disabled
    assert generators is None


def test_process_manual_custom_constraints_no_matching_links(csv_str_to_df):
    """Test that ValueError is raised when link flow constraints reference non-existent links."""

    # Input: custom_constraints_lhs with link flow term
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,        coefficient
    CONS1,          generator_capacity,  GEN1,           1.0
    CONS1,          link_flow,           NONEXISTENT,    2.0
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          5000,  <=
    """

    # Input: links (doesn't contain NONEXISTENT)
    links_csv = """
    isp_name,  name,           carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_existing, AC,       BUS1,   BUS2,   1000,   False
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Should raise ValueError for non-existent link
    with pytest.raises(
        ValueError,
        match="The following link_flow terms reference links that don't exist: \\['NONEXISTENT'\\]",
    ):
        _process_manual_custom_constraints(config, ispypsa_tables, links)


def test_process_manual_custom_constraints_multiple_no_matching_links(csv_str_to_df):
    """Test that ValueError is raised with multiple non-existent links in error message."""

    # Input: custom_constraints_lhs with multiple link flow terms referencing non-existent links
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,         coefficient
    CONS1,          generator_capacity,  GEN1,            1.0
    CONS1,          link_flow,           NONEXISTENT1,    2.0
    CONS2,          link_flow,           NONEXISTENT2,    1.5
    CONS2,          link_flow,           PATH1,           -1.0
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          5000,  <=
    CONS2,          3000,  <=
    """

    # Input: links (contains PATH1 but not NONEXISTENT1 or NONEXISTENT2)
    links_csv = """
    isp_name,  name,           carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,     PATH1_existing, AC,       BUS1,   BUS2,   1000,   False
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Should raise ValueError for non-existent links
    with pytest.raises(
        ValueError,
        match="The following link_flow terms reference links that don't exist: \\['NONEXISTENT1', 'NONEXISTENT2'\\]",
    ):
        _process_manual_custom_constraints(config, ispypsa_tables, links)


def test_process_manual_custom_constraints_empty_lhs_table(csv_str_to_df):
    """Test processing when LHS table exists but is empty."""

    # Input: empty custom_constraints_lhs
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,  term_id,  coefficient
    """

    # Input: empty custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    """

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

    # Call the function under test
    lhs, rhs, generators = _process_manual_custom_constraints(
        config, ispypsa_tables, links
    )

    # All outputs should be empty
    assert lhs.empty
    assert rhs.empty
    assert generators is None


def test_process_manual_custom_constraints_none_tables(csv_str_to_df):
    """Test processing when tables contain None values."""

    ispypsa_tables = {
        "custom_constraints_lhs": None,
        "custom_constraints_rhs": None,
    }
    links = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Call the function under test
    lhs, rhs, generators = _process_manual_custom_constraints(
        config, ispypsa_tables, links
    )

    # All outputs should be empty
    assert lhs.empty
    assert rhs.empty
    assert generators is None


def test_process_manual_custom_constraints_empty_links_with_link_flow(csv_str_to_df):
    """Test that ValueError is raised when links is empty but link_flow terms exist."""

    # Input: custom_constraints_lhs with link_flow term
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    CONS1,          generator_capacity,  GEN1,     1.0
    CONS1,          link_flow,           PATH1,    2.0
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          5000,  <=
    """

    # Create input data with empty links DataFrame
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = pd.DataFrame()  # Empty DataFrame

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Should raise ValueError because PATH1 doesn't exist in empty links
    with pytest.raises(
        ValueError,
        match="The following link_flow terms reference links that don't exist: \\['PATH1'\\]",
    ):
        _process_manual_custom_constraints(config, ispypsa_tables, links)


def test_process_manual_custom_constraints_empty_links_without_link_flow(csv_str_to_df):
    """Test processing with empty links when no link_flow terms exist."""

    # Input: custom_constraints_lhs without link_flow terms
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    CONS1,          generator_capacity,  GEN1,     1.0
    CONS1,          generator_capacity,  GEN2,     1.5
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          5000,  <=
    """

    # Create input data with empty links DataFrame
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = pd.DataFrame()  # Empty DataFrame

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Call the function under test
    lhs, rhs, generators = _process_manual_custom_constraints(
        config, ispypsa_tables, links
    )

    # Expected RHS output
    expected_rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)

    # Expected LHS output (only generator constraints)
    expected_lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,   attribute
    CONS1,            GEN1,           1.0,          Generator,   p_nom
    CONS1,            GEN2,           1.5,          Generator,   p_nom
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)

    # Compare DataFrames
    pd.testing.assert_frame_equal(
        rhs.sort_values("constraint_name").reset_index(drop=True),
        expected_rhs.sort_values("constraint_name").reset_index(drop=True),
    )

    pd.testing.assert_frame_equal(
        lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True),
        expected_lhs.sort_values(["constraint_name", "variable_name"]).reset_index(
            drop=True
        ),
    )

    assert generators is None


def test_process_manual_custom_constraints_none_links_with_link_flow(csv_str_to_df):
    """Test that ValueError is raised when links is None but link_flow terms exist."""

    # Input: custom_constraints_lhs with link_flow term
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    CONS1,          generator_capacity,  GEN1,     1.0
    CONS1,          link_flow,           PATH1,    2.0
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          5000,  <=
    """

    # Create input data with None links
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = None  # None instead of DataFrame

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Should raise ValueError because PATH1 doesn't exist (links is None)
    with pytest.raises(
        ValueError,
        match="The following link_flow terms reference links that don't exist: \\['PATH1'\\]",
    ):
        _process_manual_custom_constraints(config, ispypsa_tables, links)


def test_process_manual_custom_constraints_none_links_without_link_flow(csv_str_to_df):
    """Test processing with None links when no link_flow terms exist."""

    # Input: custom_constraints_lhs without link_flow terms
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    CONS1,          generator_capacity,  GEN1,     1.0
    CONS1,          generator_capacity,  GEN2,     1.5
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          5000,  <=
    """

    # Create input data with None links
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = None  # None instead of DataFrame

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # When there are no link_flow terms, links is not accessed, so it should work fine
    lhs, rhs, generators = _process_manual_custom_constraints(
        config, ispypsa_tables, links
    )

    # Expected RHS output
    expected_rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)

    # Expected LHS output (only generator constraints)
    expected_lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,   attribute
    CONS1,            GEN1,           1.0,          Generator,   p_nom
    CONS1,            GEN2,           1.5,          Generator,   p_nom
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)

    # Compare DataFrames
    pd.testing.assert_frame_equal(
        rhs.sort_values("constraint_name").reset_index(drop=True),
        expected_rhs.sort_values("constraint_name").reset_index(drop=True),
    )

    pd.testing.assert_frame_equal(
        lhs.sort_values(["constraint_name", "variable_name"]).reset_index(drop=True),
        expected_lhs.sort_values(["constraint_name", "variable_name"]).reset_index(
            drop=True
        ),
    )

    assert generators is None


def test_process_manual_custom_constraints_links_missing_columns(csv_str_to_df):
    """Test that appropriate error is raised when links is missing required columns."""

    # Input: custom_constraints_lhs with link_flow term
    custom_constraints_lhs_csv = """
    constraint_id,  term_type,           term_id,  coefficient
    CONS1,          generator_capacity,  GEN1,     1.0
    CONS1,          link_flow,           PATH1,    2.0
    """

    # Input: custom_constraints_rhs
    custom_constraints_rhs_csv = """
    constraint_id,  rhs,   constraint_type
    CONS1,          5000,  <=
    """

    # Input: links without required columns (missing isp_name)
    links_csv = """
    wrong_column,  name,           carrier,  bus0,   bus1,   p_nom,  p_nom_extendable
    PATH1,         PATH1_existing, AC,       BUS1,   BUS2,   1000,   False
    """

    # Create input data
    ispypsa_tables = {
        "custom_constraints_lhs": csv_str_to_df(custom_constraints_lhs_csv),
        "custom_constraints_rhs": csv_str_to_df(custom_constraints_rhs_csv),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Should raise KeyError when trying to access "isp_name" column
    with pytest.raises(KeyError, match="isp_name"):
        _process_manual_custom_constraints(config, ispypsa_tables, links)
