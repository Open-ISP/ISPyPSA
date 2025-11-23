import pandas as pd
import pytest

from ispypsa.translator.custom_constraints import (
    _create_vre_build_and_resource_limit_constraints,
    _translate_custom_constraints,
    _validate_lhs_rhs_constraints,
)


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
        "renewable_energy_zones": pd.DataFrame(),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False
        annuitisation_lifetime = 30

    class MockCapacityExpansionConfig:
        investment_periods = [2025, 2030]

    class MockTemporalConfig:
        year_type = "fy"
        capacity_expansion = MockCapacityExpansionConfig()

    class MockConfig:
        wacc = 0.05
        network = MockNetworkConfig()
        temporal = MockTemporalConfig()

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
        "renewable_energy_zones": pd.DataFrame(),
    }

    # Should raise ValueError due to duplicate CONS1 in RHS
    with pytest.raises(
        ValueError,
        match="Duplicate constraint names found in custom constraints RHS: \\['CONS1'\\]",
    ):
        _translate_custom_constraints(
            config, ispypsa_tables_with_dup, links, pd.DataFrame()
        )


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
        "renewable_energy_zones": pd.DataFrame(),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False
        annuitisation_lifetime = 30

    class MockCapacityExpansionConfig:
        investment_periods = [2025, 2030]

    class MockTemporalConfig:
        year_type = "fy"
        capacity_expansion = MockCapacityExpansionConfig()

    class MockConfig:
        wacc = 0.05
        network = MockNetworkConfig()
        temporal = MockTemporalConfig()

    config = MockConfig()

    # Call the function under test
    result = _translate_custom_constraints(
        config, ispypsa_tables, links, pd.DataFrame()
    )

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
    generators = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False
        annuitisation_lifetime = 30

    class MockCapacityExpansionConfig:
        investment_periods = [2025, 2030]

    class MockTemporalConfig:
        year_type = "fy"
        capacity_expansion = MockCapacityExpansionConfig()

    class MockConfig:
        wacc = 0.05
        network = MockNetworkConfig()
        temporal = MockTemporalConfig()

    config = MockConfig()

    # Call the function under test
    result = _translate_custom_constraints(config, ispypsa_tables, links, generators)

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
        "renewable_energy_zones": pd.DataFrame(),
    }
    links = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False
        annuitisation_lifetime = 30

    class MockCapacityExpansionConfig:
        investment_periods = [2025, 2030]

    class MockTemporalConfig:
        year_type = "fy"
        capacity_expansion = MockCapacityExpansionConfig()

    class MockConfig:
        wacc = 0.05
        network = MockNetworkConfig()
        temporal = MockTemporalConfig()

    config = MockConfig()

    # Should raise ValueError due to mismatched constraints
    with pytest.raises(
        ValueError,
        match="LHS constraints do not have corresponding RHS definitions.*CONS2",
    ):
        _translate_custom_constraints(config, ispypsa_tables, links, pd.DataFrame())


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
        "renewable_energy_zones": pd.DataFrame(),
    }
    links = pd.DataFrame()

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False
        annuitisation_lifetime = 30

    class MockCapacityExpansionConfig:
        investment_periods = [2025, 2030]

    class MockTemporalConfig:
        year_type = "fy"
        capacity_expansion = MockCapacityExpansionConfig()

    class MockConfig:
        wacc = 0.05
        network = MockNetworkConfig()
        temporal = MockTemporalConfig()

    config = MockConfig()

    # This creates duplicate PATH1 constraints in RHS
    # Should raise ValueError
    with pytest.raises(
        ValueError,
        match="Duplicate constraint names found in custom constraints RHS: \\['PATH1'\\]",
    ):
        _translate_custom_constraints(config, ispypsa_tables, links, pd.DataFrame())


def test_validate_lhs_rhs_constraints_matching_constraints(csv_str_to_df):
    """Test validation passes when LHS and RHS constraints match exactly."""

    # Create matching LHS constraints
    lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,  attribute
    CONS1,            GEN1,           1.0,          Generator,  p_nom
    CONS1,            GEN2,           2.0,          Generator,  p_nom
    CONS2,            LINK1,          1.0,          Link,       p
    """
    lhs = csv_str_to_df(lhs_csv)

    # Create matching RHS constraints
    rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    CONS2,            3000,  >=
    """
    rhs = csv_str_to_df(rhs_csv)

    # Should not raise any exception
    _validate_lhs_rhs_constraints(lhs, rhs)


def test_validate_lhs_rhs_constraints_empty_dataframes(csv_str_to_df):
    """Test validation passes when both LHS and RHS are empty."""

    lhs = pd.DataFrame()
    rhs = pd.DataFrame()

    # Should not raise any exception
    _validate_lhs_rhs_constraints(lhs, rhs)


def test_validate_lhs_rhs_constraints_lhs_without_rhs(csv_str_to_df):
    """Test validation fails when LHS has constraints without matching RHS."""

    # Create LHS constraints
    lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,  attribute
    CONS1,            GEN1,           1.0,          Generator,  p_nom
    CONS2,            GEN2,           2.0,          Generator,  p_nom
    CONS3,            LINK1,          1.0,          Link,       p
    """
    lhs = csv_str_to_df(lhs_csv)

    # Create RHS with missing CONS3
    rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    CONS2,            3000,  >=
    """
    rhs = csv_str_to_df(rhs_csv)

    # Should raise ValueError
    with pytest.raises(
        ValueError,
        match="The following LHS constraints do not have corresponding RHS definitions: \\['CONS3'\\]",
    ):
        _validate_lhs_rhs_constraints(lhs, rhs)


def test_validate_lhs_rhs_constraints_rhs_without_lhs(csv_str_to_df):
    """Test validation fails when RHS has constraints without matching LHS."""

    # Create LHS constraints
    lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,  attribute
    CONS1,            GEN1,           1.0,          Generator,  p_nom
    CONS2,            GEN2,           2.0,          Generator,  p_nom
    """
    lhs = csv_str_to_df(lhs_csv)

    # Create RHS with extra CONS3
    rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    CONS2,            3000,  >=
    CONS3,            1000,  ==
    """
    rhs = csv_str_to_df(rhs_csv)

    # Should raise ValueError
    with pytest.raises(
        ValueError,
        match="The following RHS constraints do not have corresponding LHS definitions: \\['CONS3'\\]",
    ):
        _validate_lhs_rhs_constraints(lhs, rhs)


def test_validate_lhs_rhs_constraints_multiple_mismatches(csv_str_to_df):
    """Test validation with multiple mismatched constraints on both sides."""

    # Create LHS constraints
    lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,  attribute
    CONS1,            GEN1,           1.0,          Generator,  p_nom
    CONS2,            GEN2,           2.0,          Generator,  p_nom
    CONS3,            LINK1,          1.0,          Link,       p
    CONS4,            LINK2,          2.0,          Link,       p
    """
    lhs = csv_str_to_df(lhs_csv)

    # Create RHS with different constraints
    rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    CONS2,            3000,  >=
    CONS5,            2000,  ==
    CONS6,            4000,  <=
    """
    rhs = csv_str_to_df(rhs_csv)

    # Should raise ValueError about LHS without RHS (checked first)
    with pytest.raises(
        ValueError,
        match="The following LHS constraints do not have corresponding RHS definitions: \\['CONS3', 'CONS4'\\]",
    ):
        _validate_lhs_rhs_constraints(lhs, rhs)


def test_validate_lhs_rhs_constraints_empty_lhs_nonempty_rhs(csv_str_to_df):
    """Test validation fails when LHS is empty but RHS has constraints."""

    lhs = pd.DataFrame()

    # Create non-empty RHS
    rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    CONS2,            3000,  >=
    """
    rhs = csv_str_to_df(rhs_csv)

    # Should raise ValueError
    with pytest.raises(
        ValueError,
        match="The following RHS constraints do not have corresponding LHS definitions: \\['CONS1', 'CONS2'\\]",
    ):
        _validate_lhs_rhs_constraints(lhs, rhs)


def test_validate_lhs_rhs_constraints_nonempty_lhs_empty_rhs(csv_str_to_df):
    """Test validation fails when LHS has constraints but RHS is empty."""

    # Create non-empty LHS
    lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,  attribute
    CONS1,            GEN1,           1.0,          Generator,  p_nom
    CONS2,            GEN2,           2.0,          Generator,  p_nom
    """
    lhs = csv_str_to_df(lhs_csv)

    rhs = pd.DataFrame()

    # Should raise ValueError
    with pytest.raises(
        ValueError,
        match="The following LHS constraints do not have corresponding RHS definitions: \\['CONS1', 'CONS2'\\]",
    ):
        _validate_lhs_rhs_constraints(lhs, rhs)


def test_validate_lhs_rhs_constraints_duplicate_lhs_entries(csv_str_to_df):
    """Test validation with multiple LHS entries for the same constraint (valid case)."""

    # Create LHS with multiple entries for CONS1
    lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,  attribute
    CONS1,            GEN1,           1.0,          Generator,  p_nom
    CONS1,            GEN2,           2.0,          Generator,  p_nom
    CONS1,            GEN3,           -1.0,         Generator,  p_nom
    CONS2,            LINK1,          1.0,          Link,       p
    """
    lhs = csv_str_to_df(lhs_csv)

    # Create matching RHS
    rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    CONS2,            3000,  >=
    """
    rhs = csv_str_to_df(rhs_csv)

    # Should not raise any exception - multiple LHS entries per constraint is valid
    _validate_lhs_rhs_constraints(lhs, rhs)


def test_validate_lhs_rhs_constraints_case_sensitive_names(csv_str_to_df):
    """Test that constraint name matching is case-sensitive."""

    # Create LHS with lowercase names
    lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,  attribute
    cons1,            GEN1,           1.0,          Generator,  p_nom
    CONS2,            GEN2,           2.0,          Generator,  p_nom
    """
    lhs = csv_str_to_df(lhs_csv)

    # Create RHS with uppercase names
    rhs_csv = """
    constraint_name,  rhs,   constraint_type
    CONS1,            5000,  <=
    CONS2,            3000,  >=
    """
    rhs = csv_str_to_df(rhs_csv)

    # Should raise ValueError due to case mismatch
    with pytest.raises(
        ValueError,
        match="The following LHS constraints do not have corresponding RHS definitions: \\['cons1'\\]",
    ):
        _validate_lhs_rhs_constraints(lhs, rhs)


def test_create_vre_build_limit_constraints_basic(csv_str_to_df):
    """Test basic functionality of VRE build limit constraints creation."""
    # Input: renewable energy zones with resource limits
    renewable_energy_zones_csv = """
    rez_id,         wind_generation_total_limits_mw_high,   wind_generation_total_limits_mw_medium, solar_pv_plus_solar_thermal_limits_mw_solar,    wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  land_use_limits_mw_wind,     land_use_limits_mw_solar,   rez_resource_limit_violation_penalty_factor_$/mw
    REZ1,           1000,                                   800,                                    1500,                                           0.0,                                                0.0,                                             2300,                        6000,                       290000
    REZ2,           1200,                                   900,                                    1800,                                           0.0,                                                0.0,                                             5000,                        10000,                      290000
    """
    renewable_energy_zones = csv_str_to_df(renewable_energy_zones_csv)

    # Input: generators in REZs
    generators_csv = """
    name,               bus,    carrier,    p_nom,  p_nom_extendable,   build_year,     isp_resource_type
    wind_REZ1_2025,     REZ1,   Wind,       0,      True,               2025,           WH
    wind_REZ1_2030,     REZ1,   Wind,       0,      True,               2030,           WH
    wind_REZ2_2025,     REZ2,   Wind,       0,      True,               2025,           WM
    solar_REZ1_2025,    REZ1,   Solar,      0,      True,               2025,           SAT
    """
    generators = csv_str_to_df(generators_csv)

    # Test parameters
    investment_periods = [2025, 2030]
    wacc = 0.05
    asset_lifetime = 30

    # Call the function under test
    lhs, rhs, dummy_generators = _create_vre_build_and_resource_limit_constraints(
        renewable_energy_zones, generators, investment_periods, wacc, asset_lifetime
    )

    # Check that LHS, RHS and dummy generators are returned
    assert not lhs.empty
    assert not rhs.empty
    assert not dummy_generators.empty

    expected_lhs_csv = """
    constraint_name,            variable_name,                          coefficient,    component,      attribute
    REZ1_WH_resource_limit,     wind_REZ1_2025,                         1.0,            Generator,      p_nom
    REZ1_WH_resource_limit,     wind_REZ1_2030,                         1.0,            Generator,      p_nom
    REZ1_WH_resource_limit,     REZ1_WH_resource_limit_relax_2025,      -1.0,           Generator,      p_nom
    REZ1_WH_resource_limit,     REZ1_WH_resource_limit_relax_2030,      -1.0,           Generator,      p_nom
    REZ1_Solar_resource_limit,  solar_REZ1_2025,                        1.0,            Generator,      p_nom
    REZ1_Solar_resource_limit,  REZ1_Solar_resource_limit_relax_2025,   -1.0,           Generator,      p_nom
    REZ1_Solar_resource_limit,  REZ1_Solar_resource_limit_relax_2030,   -1.0,           Generator,      p_nom
    REZ2_WM_resource_limit,     wind_REZ2_2025,                         1.0,            Generator,      p_nom
    REZ2_WM_resource_limit,     REZ2_WM_resource_limit_relax_2025,      -1.0,           Generator,      p_nom
    REZ2_WM_resource_limit,     REZ2_WM_resource_limit_relax_2030,      -1.0,           Generator,      p_nom
    REZ1_Wind_build_limit,      wind_REZ1_2025,                         1.0,            Generator,      p_nom
    REZ1_Wind_build_limit,      wind_REZ1_2030,                         1.0,            Generator,      p_nom
    REZ2_Wind_build_limit,      wind_REZ2_2025,                         1.0,            Generator,      p_nom
    REZ1_Solar_build_limit,     solar_REZ1_2025,                        1.0,            Generator,      p_nom
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)

    expected_rhs_csv = """
    constraint_name,            rhs,        constraint_type
    REZ1_WH_resource_limit,     1000.0,     <=
    REZ1_Solar_resource_limit,  1500.0,     <=
    REZ2_WM_resource_limit,     900.0,      <=
    REZ1_Wind_build_limit,      2300.0,     <=
    REZ1_Solar_build_limit,     6000.0,     <=
    REZ2_Wind_build_limit,      5000.0,     <=
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)

    lhs_col_order = [
        "constraint_name",
        "variable_name",
        "coefficient",
        "component",
        "attribute",
    ]

    rhs_col_order = ["constraint_name", "constraint_type", "rhs"]

    pd.testing.assert_frame_equal(
        lhs[lhs_col_order].sort_values(by="constraint_name").reset_index(drop=True),
        expected_lhs[lhs_col_order]
        .sort_values(by="constraint_name")
        .reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        rhs[rhs_col_order].sort_values(by="constraint_name").reset_index(drop=True),
        expected_rhs[rhs_col_order]
        .sort_values(by="constraint_name")
        .reset_index(drop=True),
        check_dtype=False,
    )

    # Check that dummy generators were created for resource limits
    expected_dummy_generators_csv = """
    name,                                   bus,                                p_nom,      p_nom_extendable,   build_year,     lifetime,   isp_name
    REZ1_WH_resource_limit_relax_2025,      bus_for_custom_constraint_gens,     0.0,        True,               2025,           30,         REZ1_WH_resource_limit
    REZ1_WH_resource_limit_relax_2030,      bus_for_custom_constraint_gens,     0.0,        True,               2030,           30,         REZ1_WH_resource_limit
    REZ1_WM_resource_limit_relax_2025,      bus_for_custom_constraint_gens,     0.0,        True,               2025,           30,         REZ1_WM_resource_limit
    REZ1_WM_resource_limit_relax_2030,      bus_for_custom_constraint_gens,     0.0,        True,               2030,           30,         REZ1_WM_resource_limit
    REZ1_Solar_resource_limit_relax_2025,   bus_for_custom_constraint_gens,     0.0,        True,               2025,           30,         REZ1_Solar_resource_limit
    REZ1_Solar_resource_limit_relax_2030,   bus_for_custom_constraint_gens,     0.0,        True,               2030,           30,         REZ1_Solar_resource_limit
    REZ2_WM_resource_limit_relax_2025,      bus_for_custom_constraint_gens,     0.0,        True,               2025,           30,         REZ2_WM_resource_limit
    REZ2_WM_resource_limit_relax_2030,      bus_for_custom_constraint_gens,     0.0,        True,               2030,           30,         REZ2_WM_resource_limit
    REZ2_WH_resource_limit_relax_2025,      bus_for_custom_constraint_gens,     0.0,        True,               2025,           30,         REZ2_WH_resource_limit
    REZ2_WH_resource_limit_relax_2030,      bus_for_custom_constraint_gens,     0.0,        True,               2030,           30,         REZ2_WH_resource_limit
    REZ2_Solar_resource_limit_relax_2025,   bus_for_custom_constraint_gens,     0.0,        True,               2025,           30,         REZ2_Solar_resource_limit
    REZ2_Solar_resource_limit_relax_2030,   bus_for_custom_constraint_gens,     0.0,        True,               2030,           30,         REZ2_Solar_resource_limit
    """
    expected_dummy_generators = csv_str_to_df(expected_dummy_generators_csv)
    cols_no_capex_in_order = [
        "name",
        "isp_name",
        "bus",
        "p_nom",
        "p_nom_extendable",
        "build_year",
        "lifetime",
    ]

    # check capital_cost is present and float before checking other columns for equality:
    assert dummy_generators is not None
    assert "capital_cost" in dummy_generators.columns
    assert dummy_generators["capital_cost"].dtype == "float64"

    pd.testing.assert_frame_equal(
        dummy_generators[cols_no_capex_in_order]
        .sort_values(by="name")
        .reset_index(drop=True),
        expected_dummy_generators[cols_no_capex_in_order]
        .sort_values(by="name")
        .reset_index(drop=True),
    )


def test_create_vre_build_limit_constraints_offshore_wind(csv_str_to_df):
    """Test VRE build limit constraints for offshore wind which has hard constraints."""
    # Input: renewable energy zones with offshore wind limits
    renewable_energy_zones_csv = """
    rez_id,         wind_generation_total_limits_mw_high,   wind_generation_total_limits_mw_medium, solar_pv_plus_solar_thermal_limits_mw_solar,    wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  land_use_limits_mw_wind,     land_use_limits_mw_solar,   rez_resource_limit_violation_penalty_factor_$/mw
    REZ1,           0.0,                                    0.0,                                    0.0,                                            1300,                                               750,                                             NaN,                         NaN,                        NaN
    """
    renewable_energy_zones = csv_str_to_df(renewable_energy_zones_csv)

    # Input: generators in REZs
    generators_csv = """
    name,               bus,    carrier,    p_nom,  p_nom_extendable,   build_year,     isp_resource_type
    WFL_REZ1_2025,      REZ1,   Wind,       0,      True,               2025,           WFL
    WFX_REZ1_2030,      REZ1,   Wind,       0,      True,               2030,           WFX
    """
    generators = csv_str_to_df(generators_csv)

    # Test parameters
    investment_periods = [2025, 2030]
    wacc = 0.05
    asset_lifetime = 30

    # Call the function under test
    lhs, rhs, dummy_generators = _create_vre_build_and_resource_limit_constraints(
        renewable_energy_zones, generators, investment_periods, wacc, asset_lifetime
    )

    # Check that LHS, RHS are returned but no dummy generators (offshore is hard constrained)
    expected_lhs_csv = """
    constraint_name,            variable_name,     component,      attribute,      coefficient
    REZ1_WFL_build_limit,       WFL_REZ1_2025,     Generator,      p_nom,          1.0
    REZ1_WFX_build_limit,       WFX_REZ1_2030,     Generator,      p_nom,          1.0
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)

    expected_rhs_csv = """
    constraint_name,            rhs,        constraint_type
    REZ1_WFL_build_limit,       1300.0,     "<="
    REZ1_WFX_build_limit,       750.0,      "<="
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)

    lhs_col_order = [
        "constraint_name",
        "variable_name",
        "coefficient",
        "component",
        "attribute",
    ]

    rhs_col_order = ["constraint_name", "constraint_type", "rhs"]

    pd.testing.assert_frame_equal(
        lhs[lhs_col_order].sort_values(by="constraint_name").reset_index(drop=True),
        expected_lhs[lhs_col_order]
        .sort_values(by="constraint_name")
        .reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        rhs[rhs_col_order].sort_values(by="constraint_name").reset_index(drop=True),
        expected_rhs[rhs_col_order]
        .sort_values(by="constraint_name")
        .reset_index(drop=True),
        check_dtype=False,
    )

    # Offshore wind uses hard constraints (can_be_relaxed=False), so no dummy generators
    # should be created for these constraints
    assert dummy_generators is None


def test_create_vre_build_limit_constraints_empty_inputs(csv_str_to_df):
    """Test with empty inputs."""
    # Empty inputs
    renewable_energy_zones = pd.DataFrame()
    generators = pd.DataFrame()
    investment_periods = [2025, 2030]
    wacc = 0.05
    asset_lifetime = 30

    # Call the function under test
    lhs, rhs, dummy_generators = _create_vre_build_and_resource_limit_constraints(
        renewable_energy_zones, generators, investment_periods, wacc, asset_lifetime
    )

    # Should return empty DataFrames
    assert lhs.empty
    assert rhs.empty
    assert dummy_generators is None


def test_create_vre_build_limit_constraints_no_matching_generators(csv_str_to_df):
    """Test when there are resource limits but no matching generators."""
    # Input: renewable energy zones with resource limits
    renewable_energy_zones_csv = """
    rez_id,         wind_generation_total_limits_mw_high,   wind_generation_total_limits_mw_medium, solar_pv_plus_solar_thermal_limits_mw_solar,    wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  land_use_limits_mw_wind,     land_use_limits_mw_solar,   rez_resource_limit_violation_penalty_factor_$/mw
    REZ1,           1000,                                   800,                                    1500,                                           0.0,                                                0.0,                                             2300,                        6000,                       290000
    """
    renewable_energy_zones = csv_str_to_df(renewable_energy_zones_csv)

    # Input: generators not in REZs or not matching constraint types
    generators_csv = """
    name,           bus,    carrier,    p_nom,  p_nom_extendable,   build_year, isp_resource_type
    wind_REZ3_2025, REZ3,   Wind,       0,      True,               2025,       WH
    other_gen_2025, REZ1,   Gas,        0,      True,               2025,       Gas
    """
    generators = csv_str_to_df(generators_csv)

    # Test parameters
    investment_periods = [2025, 2030]
    wacc = 0.05
    asset_lifetime = 30

    # Call the function under test
    lhs, rhs, dummy_generators = _create_vre_build_and_resource_limit_constraints(
        renewable_energy_zones, generators, investment_periods, wacc, asset_lifetime
    )

    # Should create constraints, but no generators should be included in them
    assert rhs.empty  # RHS should be returned empty
    assert lhs.empty  # No generators in LHS

    # Dummy generators should not be returned either:
    assert dummy_generators is None
