import io
import re

import pandas as pd
import pytest

from ispypsa.translator.lines import (
    _translate_existing_flow_path_capacity_to_lines,
    _translate_expansion_costs_to_lines,
    _translate_flow_paths_to_lines,
)


def test_translate_existing_flow_path_capacity_to_lines(csv_str_to_df):
    """Test that existing flow paths are correctly translated to lines."""
    # Create sample data for testing
    existing_flow_paths_csv = """
    flow_path,      carrier,  node_from,  node_to,  forward_direction_mw_summer_typical
    PathA-PathB,    AC,       NodeA,      NodeB,    1000
    PathB-PathC,    AC,       NodeB,      NodeC,    2000
    """
    existing_flow_paths = csv_str_to_df(existing_flow_paths_csv)

    # Expected result
    expected_lines_csv = """
    name,                 carrier,  bus0,     bus1,     s_nom,  capital_cost,  s_nom_extendable
    PathA-PathB_existing, AC,       NodeA,    NodeB,    1000,   ,              False
    PathB-PathC_existing, AC,       NodeB,    NodeC,    2000,   ,              False
    """
    expected_lines = csv_str_to_df(expected_lines_csv)
    expected_lines["capital_cost"] = pd.to_numeric(
        expected_lines["capital_cost"], errors="coerce"
    )

    # Convert the flow paths to lines
    result = _translate_existing_flow_path_capacity_to_lines(existing_flow_paths)

    # Assert the results match expectations
    pd.testing.assert_frame_equal(
        result.sort_index(axis=1), expected_lines.sort_index(axis=1)
    )


def test_translate_expansion_costs_to_lines(csv_str_to_df):
    """Test that flow path expansion costs are correctly translated to lines."""
    # Create sample data for testing
    flow_path_expansion_costs_csv = """
    flow_path,     additional_network_capacity_mw,  2025_26_$/mw,  2026_27_$/mw
    NodeA-NodeB,   500,                             ,          1200
    NodeB-NodeC,   800,                             1500,          1800
    """
    flow_path_expansion_costs = csv_str_to_df(flow_path_expansion_costs_csv)

    existing_lines_csv = """
    name,                 carrier,  bus0,    bus1,    s_nom
    NodeA-NodeB_existing, AC,       NodeA,   NodeB,   1000
    NodeB-NodeC_existing, AC,       NodeB,   NodeC,   2000
    """
    existing_lines_df = csv_str_to_df(existing_lines_csv)

    investment_periods = [2026, 2027]
    year_type = "fy"
    wacc = 0.07
    asset_lifetime = 30

    result = _translate_expansion_costs_to_lines(
        flow_path_expansion_costs,
        existing_lines_df,
        investment_periods,
        year_type,
        wacc,
        asset_lifetime,
        id_column="flow_path",
        match_column="name",
    )

    # Expected result structure - use a fixed capital_cost for assertion purposes
    # The actual values depend on the annuitization formula
    expected_result_csv = """
    name,                 bus0,   bus1,  carrier, s_nom,  s_nom_extendable,  build_year,  lifetime
    NodeB-NodeC_exp_2026, NodeB, NodeC,  AC,      0.0,    True,              2026,        30
    NodeA-NodeB_exp_2027, NodeA, NodeB,  AC,      0.0,    True,              2027,        30
    NodeB-NodeC_exp_2027, NodeB, NodeC,  AC,      0.0,    True,              2027,        30
    """
    expected_result = csv_str_to_df(expected_result_csv)

    # Sort both result and expected result for comparison
    result = result.sort_values(["name"]).reset_index(drop=True)
    expected_result = expected_result.sort_values(["name"]).reset_index(drop=True)

    # Check capital costs separately - should be greater than 0
    assert all(result["capital_cost"] > 0)
    result = result.drop(columns="capital_cost")

    # Check that column names match
    assert set(expected_result.columns).issubset(set(result.columns))

    # Check all columns except capital_cost (which uses the annuitization formula)
    for col in expected_result.columns:
        pd.testing.assert_series_equal(
            result[col],
            expected_result[col],
            check_dtype=False,  # Allow float vs int differences
            check_names=False,  # Ignore index names
        )


def test_translate_expansion_costs_to_lines_empty(csv_str_to_df):
    """Test that empty flow path expansion costs result in empty DataFrame."""
    # Create empty DataFrame
    flow_path_expansion_costs_csv = """
    flow_path,additional_network_capacity_mw,2025_26_$/mw
    """
    flow_path_expansion_costs = csv_str_to_df(flow_path_expansion_costs_csv)

    existing_lines_csv = """
    name,                 carrier,  bus0,    bus1,    s_nom
    PathA-PathB_existing, AC,       NodeA,   NodeB,   1000
    """
    existing_lines_df = csv_str_to_df(existing_lines_csv)

    result = _translate_expansion_costs_to_lines(
        flow_path_expansion_costs,
        existing_lines_df,
        [2026],
        "fy",
        0.07,
        30,
        id_column="flow_path",
        match_column="name",
    )

    # The result should be an empty DataFrame
    assert result.empty


def test_translate_expansion_costs_to_lines_no_matching_years(csv_str_to_df):
    """Test when none of the expansion costs match the investment periods."""
    # Create sample data for testing
    flow_path_expansion_costs_csv = """
    flow_path,     additional_network_capacity_mw,  2025_26_$/mw
    PathA-PathB,   500,                             1000
    """
    flow_path_expansion_costs = csv_str_to_df(flow_path_expansion_costs_csv)

    existing_lines_csv = """
    name,                 carrier,  bus0,    bus1,    s_nom
    PathA-PathB_existing, AC,       NodeA,   NodeB,   1000
    """
    existing_lines_df = csv_str_to_df(existing_lines_csv)

    # Investment periods don't include 2026
    investment_periods = [2027, 2028]
    year_type = "fy"
    wacc = 0.07
    asset_lifetime = 30

    # Call the function with updated parameters
    result = _translate_expansion_costs_to_lines(
        flow_path_expansion_costs,
        existing_lines_df,
        investment_periods,
        year_type,
        wacc,
        asset_lifetime,
        id_column="flow_path",
        match_column="name",
    )

    # The result should be an empty DataFrame since no years match
    assert result.empty


def test_translate_flow_paths_to_lines_with_expansion(csv_str_to_df):
    """Test that flow paths are translated to lines with expansion."""
    # Create sample input data
    flow_paths_csv = """
    flow_path,      carrier,  node_from,  node_to,  forward_direction_mw_summer_typical
    PathA-PathB,    AC,       NodeA,      NodeB,    1000
    PathB-PathC,    AC,       NodeB,      NodeC,    2000
    """

    flow_path_expansion_costs_csv = """
    flow_path,     additional_network_capacity_mw,  2025_26_$/mw,  2026_27_$/mw
    PathA-PathB,   500,                             1000,          1200
    PathB-PathC,   800,                             1500,          1800
    """

    ispypsa_tables = {
        "flow_paths": csv_str_to_df(flow_paths_csv),
        "flow_path_expansion_costs": csv_str_to_df(flow_path_expansion_costs_csv),
    }

    # Mock config with expansion enabled
    class MockTemporalConfig:
        class MockCapacityExpansion:
            investment_periods = [2026, 2027]

        year_type = "fy"
        capacity_expansion = MockCapacityExpansion()

    class MockNetworkConfig:
        annuitisation_lifetime = 30
        transmission_expansion = True  # This is the key parameter needed

    class MockConfig:
        temporal = MockTemporalConfig()
        network = MockNetworkConfig()
        wacc = 0.07

    config = MockConfig()

    # Call the function
    result = _translate_flow_paths_to_lines(ispypsa_tables, config)

    # Check the result is of the expected length
    assert len(result) == 6

    # Check that the result includes both existing and expansion lines
    assert any("_existing" in name for name in result["name"])
    assert any("_exp_" in name for name in result["name"])


def test_translate_flow_paths_to_lines_without_expansion(csv_str_to_df):
    """Test that flow paths are translated to lines without expansion."""
    # Create sample input data
    flow_paths_csv = """
    flow_path,      carrier,  node_from,  node_to,  forward_direction_mw_summer_typical
    PathA-PathB,    AC,       NodeA,      NodeB,    1000
    PathB-PathC,    AC,       NodeB,      NodeC,    2000
    """

    flow_path_expansion_costs_csv = """
    flow_path,     additional_network_capacity_mw,  2025_26_$/mw,  2026_27_$/mw
    PathA-PathB,   500,                             1000,          1200
    PathB-PathC,   800,                             1500,          1800
    """

    ispypsa_tables = {
        "flow_paths": csv_str_to_df(flow_paths_csv),
        "flow_path_expansion_costs": csv_str_to_df(flow_path_expansion_costs_csv),
    }

    # Mock config with expansion disabled
    class MockTemporalConfig:
        class MockCapacityExpansion:
            investment_periods = [2026, 2027]

        year_type = "fy"
        capacity_expansion = MockCapacityExpansion()

    class MockNetworkConfig:
        annuitisation_lifetime = 30
        transmission_expansion = False  # This is the key parameter needed

    class MockConfig:
        temporal = MockTemporalConfig()
        network = MockNetworkConfig()
        wacc = 0.07

    config = MockConfig()

    # Call the function
    result = _translate_flow_paths_to_lines(ispypsa_tables, config)

    # Expected result - only existing lines, no expansion lines
    expected_result_csv = """
    name,                 bus0,     bus1,     s_nom,  capital_cost,  s_nom_extendable,  carrier
    PathA-PathB_existing, NodeA,    NodeB,    1000,   ,              False,             AC
    PathB-PathC_existing, NodeB,    NodeC,    2000,   ,              False,             AC
    """
    expected_result = csv_str_to_df(expected_result_csv)
    expected_result["capital_cost"] = pd.to_numeric(
        expected_result["capital_cost"], errors="coerce"
    )

    # Sort both dataframes for comparison
    result = result.sort_values("name").reset_index(drop=True)
    expected_result = expected_result.sort_values("name").reset_index(drop=True)

    # Assert the results match expectations
    for col in expected_result.columns:
        pd.testing.assert_series_equal(
            result[col],
            expected_result[col],
            check_dtype=False,
            check_names=False,
        )


def test_translate_expansion_costs_to_lines_calendar_year_error(csv_str_to_df):
    """Test that calendar year type raises a NotImplementedError."""
    # Create sample data
    flow_path_expansion_costs_csv = """
    flow_path,     additional_network_capacity_mw,  2025_26_$/mw
    PathA-PathB,   500,                             1000
    """
    flow_path_expansion_costs = csv_str_to_df(flow_path_expansion_costs_csv)

    existing_lines_csv = """
    name,                 carrier,  bus0,    bus1,    s_nom
    PathA-PathB_existing, AC,       NodeA,   NodeB,   1000
    """
    existing_lines_df = csv_str_to_df(existing_lines_csv)

    investment_periods = [2026]
    year_type = "calendar"  # This should trigger the error
    wacc = 0.07
    asset_lifetime = 30

    # Check that the correct error is raised
    with pytest.raises(
        NotImplementedError,
        match="Calendar years not implemented for transmission costs",
    ):
        _translate_expansion_costs_to_lines(
            flow_path_expansion_costs,
            existing_lines_df,
            investment_periods,
            year_type,
            wacc,
            asset_lifetime,
            id_column="flow_path",
            match_column="name",
        )


def test_translate_expansion_costs_to_lines_invalid_year_type(csv_str_to_df):
    """Test that an invalid year type raises a ValueError."""
    # Create sample data
    flow_path_expansion_costs_csv = """
    flow_path,     additional_network_capacity_mw,  2025_26_$/mw
    PathA-PathB,   500,                             1000
    """
    flow_path_expansion_costs = csv_str_to_df(flow_path_expansion_costs_csv)

    existing_lines_csv = """
    name,                 carrier,  bus0,    bus1,    s_nom
    PathA-PathB_existing, AC,       NodeA,   NodeB,   1000
    """
    existing_lines_df = csv_str_to_df(existing_lines_csv)

    investment_periods = [2026]
    year_type = "invalid_year_type"  # This should trigger the error
    wacc = 0.07
    asset_lifetime = 30

    # Check that the correct error is raised
    with pytest.raises(ValueError, match="Unknown year_type"):
        _translate_expansion_costs_to_lines(
            flow_path_expansion_costs,
            existing_lines_df,
            investment_periods,
            year_type,
            wacc,
            asset_lifetime,
            id_column="flow_path",
            match_column="name",
        )
