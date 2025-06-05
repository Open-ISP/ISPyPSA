import numpy as np
import pandas as pd
import pytest

from ispypsa.templater.flow_paths import (
    _get_actionable_projects_table,
    _get_augmentation_table,
    _get_cost_table,
    _get_least_cost_options,
    _get_prep_activities_table,
    _template_sub_regional_flow_path_costs,
    process_transmission_costs,
)
from ispypsa.templater.mappings import (
    _FLOW_PATH_AGUMENTATION_TABLES,
    _FLOW_PATH_CONFIG,
)


def test_template_sub_regional_flow_path_costs_simple_least_cost_option():
    # Augmentation tables for NNSW-SQ and TAS-VIC
    aug_table_nnsw_sq = pd.DataFrame(
        {
            "Flow path": ["NNSW-SQ", "NNSW-SQ", "NNSW-SQ"],
            "Option Name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2", "NNSW–SQ Option 5"],
            "forward_capacity_increase": [100, 200, 40],
            "reverse_capacity_increase": [90, 140, 50],
        }
    )
    aug_table_tas_vic = pd.DataFrame(
        {
            "Flow path": ["TAS-VIC", "TAS-VIC"],
            "Option Name": [
                "TAS-VIC Option 1 (Project Marinus Stage 1)",
                "TAS-VIC Option 2 (Project Marinus Stage 2)",
            ],
            "forward_capacity_increase": [130, 70],
            "reverse_capacity_increase": [150, 65],
        }
    )
    # Cost tables for NNSW-SQ and TAS-VIC
    # Option 2 is least cost and has the largest increase so should be chosen.
    cost_table_nnsw_sq = pd.DataFrame(
        {
            "Flow path": ["NNSW-SQ", "NNSW-SQ"],
            "Option Name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2"],
            "2024_25": [50, 40],
            "2025_26": [55, 45],
        }
    )
    # Option 1 is least cost and has the largest increase so should be chosen.
    cost_table_tas_vic = pd.DataFrame(
        {
            "Flow path": ["TAS-VIC", "TAS-VIC"],
            "Option Name": [
                "TAS-VIC Option 1 (Project Marinus Stage 1)",
                "TAS-VIC Option 2 (Project Marinus Stage 2)",
            ],
            "2024_25": [70, np.nan],  # actionable ISP option has NaN
            "2025_26": [75, np.nan],
        }
    )
    # Preparatory activities and actionable ISP tables (should not be chosen)
    # Note: ISPyPSA contains internal mappings which match the names used in Preparatory
    # and actionable isp cost tables to the names used in the augmentation tables.
    prep_acts = pd.DataFrame(
        {
            "Flow path": ["500kV QNI Connect (NSW works)"],
            "2024_25": [100],
            "2025_26": [110],
        }
    )
    actionable_isp = pd.DataFrame(
        {
            "Flow path": ["Project Marinus Stage 1"],
            "2024_25": [999],
            "2025_26": [999],
        }
    )
    # Compose iasr_tables dict
    iasr_tables = {
        "flow_path_augmentation_options_NNSW-SQ": aug_table_nnsw_sq,
        "flow_path_augmentation_options_TAS-VIC": aug_table_tas_vic,
        "flow_path_augmentation_costs_progressive_change_NNSW-SQ": cost_table_nnsw_sq,
        "flow_path_augmentation_costs_progressive_change_TAS-VIC": cost_table_tas_vic,
        "flow_path_augmentation_costs_progressive_change_preparatory_activities": prep_acts,
        "flow_path_augmentation_costs_progressive_change_actionable_isp_projects": actionable_isp,
    }
    scenario = "Progressive Change"
    # Run function
    result = _template_sub_regional_flow_path_costs(iasr_tables, scenario)
    # Check least cost options are chosen for NNSW-SQ and TAS-VIC
    nnsw_sq_row = result[result["flow_path"] == "NNSW-SQ"]
    tas_vic_row = result[result["flow_path"] == "TAS-VIC"]
    assert nnsw_sq_row["option"].iloc[0] == "NNSW-SQ Option 2"
    assert tas_vic_row["option"].iloc[0] == "TAS-VIC Option 1 (Project Marinus Stage 1)"
    # Check nominal_flow_limit_increase_mw is correct
    assert nnsw_sq_row["additional_network_capacity_mw"].iloc[0] == 200
    assert tas_vic_row["additional_network_capacity_mw"].iloc[0] == 150
    # Check cost per year column is correct (cost divided by nominal limit)
    # For NNSW-SQ Option 2: 2024_25 = 40/200 = 0.2, 2025_26 = 45/200 = 0.225
    # For TAS-VIC Option 1: 2024_25 = 70/150 ≈ 0.4667, 2025_26 = 75/150 = 0.5
    assert abs(nnsw_sq_row["2024_25_$/mw"].iloc[0] - 0.2) < 1e-6
    assert abs(nnsw_sq_row["2025_26_$/mw"].iloc[0] - 0.225) < 1e-6
    assert abs(tas_vic_row["2024_25_$/mw"].iloc[0] - (70 / 150)) < 1e-6
    assert abs(tas_vic_row["2025_26_$/mw"].iloc[0] - 0.5) < 1e-6


def test_template_sub_regional_flow_path_costs_prep_and_actionable_chosen():
    """
    The cost of the non preparatory activities and non actionable isp projects
    have been made very high and therefore preparatory activities and
    actionable isp projects should be chosen.
    """
    # Augmentation tables for NNSW-SQ and TAS-VIC
    aug_table_nnsw_sq = pd.DataFrame(
        {
            "Flow path": ["NNSW-SQ", "NNSW-SQ", "NNSW-SQ"],
            "Option Name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2", "NNSW–SQ Option 5"],
            "forward_capacity_increase": [100, 150, 200],
            "reverse_capacity_increase": [100, 150, 150],
        }
    )
    aug_table_tas_vic = pd.DataFrame(
        {
            "Flow path": ["TAS-VIC", "TAS-VIC"],
            "Option Name": [
                "TAS-VIC Option 1 (Project Marinus Stage 1)",
                "TAS-VIC Option 2 (Project Marinus Stage 2)",
            ],
            "forward_capacity_increase": [140, 150],
            "reverse_capacity_increase": [145, 130],
        }
    )
    # Standard cost tables (set high or NaN)
    cost_table_nnsw_sq = pd.DataFrame(
        {
            "Flow path": ["NNSW-SQ", "NNSW-SQ"],
            "Option Name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2"],
            "2024_25": [1000, 1000],
            "2025_26": [1000, 1000],
        }
    )
    cost_table_tas_vic = pd.DataFrame(
        {
            "Flow path": ["TAS-VIC", "TAS-VIC"],
            "Option Name": [
                "TAS-VIC Option 1 (Project Marinus Stage 1)",
                "TAS-VIC Option 2 (Project Marinus Stage 2)",
            ],
            "2024_25": [1000, np.nan],
            "2025_26": [1000, np.nan],
        }
    )
    # Preparatory activities and actionable ISP tables (set low cost)
    # Note: ISPyPSA contains internal mappings which match the names used in Preparatory
    # and actionable isp cost tables to the names used in the augmentation tables.
    prep_acts = pd.DataFrame(
        {
            "Flow path": ["500kV QNI Connect (NSW works)"],
            "2024-25": [10],
            "2025-26": [20],
        }
    )
    actionable_isp = pd.DataFrame(
        {
            "Flow path": ["Project Marinus Stage 2"],
            "2024-25": [15],
            "2025-26": [25],
        }
    )
    # Compose iasr_tables dict
    iasr_tables = {
        "flow_path_augmentation_options_NNSW-SQ": aug_table_nnsw_sq,
        "flow_path_augmentation_options_TAS-VIC": aug_table_tas_vic,
        "flow_path_augmentation_costs_progressive_change_NNSW-SQ": cost_table_nnsw_sq,
        "flow_path_augmentation_costs_progressive_change_TAS-VIC": cost_table_tas_vic,
        "flow_path_augmentation_costs_progressive_change_preparatory_activities": prep_acts,
        "flow_path_augmentation_costs_progressive_change_actionable_isp_projects": actionable_isp,
    }
    scenario = "Progressive Change"
    # Run function
    result = _template_sub_regional_flow_path_costs(iasr_tables, scenario)
    # Check that the prep activity is chosen for NNSW-SQ and actionable ISP for TAS-VIC
    nnsw_sq_row = result[result["flow_path"] == "NNSW-SQ"]
    tas_vic_row = result[result["flow_path"] == "TAS-VIC"]
    assert nnsw_sq_row["option"].iloc[0] == "NNSW–SQ Option 5"
    assert tas_vic_row["option"].iloc[0] == "TAS-VIC Option 2 (Project Marinus Stage 2)"
    # Check nominal_flow_limit_increase_mw is correct
    assert nnsw_sq_row["additional_network_capacity_mw"].iloc[0] == 200
    assert tas_vic_row["additional_network_capacity_mw"].iloc[0] == 150
    # Check cost per year column is correct (cost divided by nominal limit)
    assert abs(nnsw_sq_row["2024_25_$/mw"].iloc[0] - (10 / 200)) < 1e-6
    assert abs(nnsw_sq_row["2025_26_$/mw"].iloc[0] - (20 / 200)) < 1e-6
    assert abs(tas_vic_row["2024_25_$/mw"].iloc[0] - (15 / 150)) < 1e-6
    assert abs(tas_vic_row["2025_26_$/mw"].iloc[0] - (25 / 150)) < 1e-6


def test_template_sub_regional_flow_path_costs_use_first_year_with_valid_costs():
    """
    Test that the first year with non-nan cost data for all options is used.
    """
    # NNSW-SQ: only 2025_26 has all non-nan costs
    aug_table_nnsw_sq = pd.DataFrame(
        {
            "Flow path": ["NNSW-SQ", "NNSW-SQ", "NNSW-SQ"],
            "Option Name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2", "NNSW-SQ Option 3"],
            "forward_capacity_increase": [150, 200, 200],
            "reverse_capacity_increase": [200, 150, 150],
        }
    )
    # Even though option 3 is cheaper than option 2 in 2024_25, option 2 should get
    # chosen because 2025_26 is used as the comparison year.
    cost_table_nnsw_sq = pd.DataFrame(
        {
            "Flow path": ["NNSW-SQ", "NNSW-SQ", "NNSW-SQ"],
            "Option Name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2", "NNSW-SQ Option 3"],
            "2024_25": [np.nan, 50, 10],
            "2025_26": [35, 45, 50],
        }
    )
    # TAS-VIC: all years have valid costs
    aug_table_tas_vic = pd.DataFrame(
        {
            "Flow path": ["TAS-VIC", "TAS-VIC"],
            "Option Name": ["TAS-VIC Option 1", "TAS-VIC Option 2"],
            "forward_capacity_increase": [90, 100],
            "reverse_capacity_increase": [100, 90],
        }
    )
    cost_table_tas_vic = pd.DataFrame(
        {
            "Flow path": ["TAS-VIC", "TAS-VIC"],
            "Option Name": ["TAS-VIC Option 1", "TAS-VIC Option 2"],
            "2024_25": [100, 10],
            "2025_26": [10, 100],
        }
    )
    iasr_tables = {
        "flow_path_augmentation_options_NNSW-SQ": aug_table_nnsw_sq,
        "flow_path_augmentation_options_TAS-VIC": aug_table_tas_vic,
        "flow_path_augmentation_costs_progressive_change_NNSW-SQ": cost_table_nnsw_sq,
        "flow_path_augmentation_costs_progressive_change_TAS-VIC": cost_table_tas_vic,
    }
    scenario = "Progressive Change"
    result = _template_sub_regional_flow_path_costs(iasr_tables, scenario)
    # NNSW-SQ: Only 2025_26 has all non-nan costs, so selection is based on that year for all years
    nnsw_sq_row = result[result["flow_path"] == "NNSW-SQ"]
    assert nnsw_sq_row["option"].iloc[0] == "NNSW-SQ Option 1"
    assert nnsw_sq_row["additional_network_capacity_mw"].iloc[0] == 200
    assert np.isnan(nnsw_sq_row["2024_25_$/mw"].iloc[0])
    assert abs(nnsw_sq_row["2025_26_$/mw"].iloc[0] - (35 / 200)) < 1e-6
    # TAS-VIC: both years valid, Option 2 is the least cost only in first,
    # but should be chosen on this basis.
    tas_vic_row = result[result["flow_path"] == "TAS-VIC"]
    assert tas_vic_row["option"].iloc[0] == "TAS-VIC Option 2"
    assert tas_vic_row["additional_network_capacity_mw"].iloc[0] == 100
    assert abs(tas_vic_row["2024_25_$/mw"].iloc[0] - (10 / 100)) < 1e-6
    assert abs(tas_vic_row["2025_26_$/mw"].iloc[0] - (100 / 100)) < 1e-6


def test_get_least_cost_options_logs_unmatched(caplog):
    """
    Test that _get_least_cost_options logs dropped flow_path/option_name pairs from both tables.
    """
    # Augmentation table has one extra option not in cost table
    aug_table = pd.DataFrame(
        {
            "id": ["A", "A", "B"],
            "option": ["opt1", "opt2", "opt3"],
            "nominal_capacity_increase": [100, 200, 300],
        }
    )
    # Cost table has one extra option not in aug table
    cost_table = pd.DataFrame(
        {
            "id": ["A", "A", "B"],
            "option": ["opt1", "opt2", "opt4"],
            "2024_25": [10, 20, 30],
            "2025_26": [15, 25, 35],
        }
    )
    # Only the (B, opt3) and (B, opt4) pairs should be dropped
    with caplog.at_level("INFO"):
        result = _get_least_cost_options(aug_table, cost_table, _FLOW_PATH_CONFIG)
    # Check logs for both dropped pairs
    assert "Dropped options from augmentation table: [('B', 'opt3')]" in caplog.text
    assert "Dropped options from cost table: [('B', 'opt4')]" in caplog.text


def test_get_full_flow_path_aug_table_logs_missing_tables(caplog):
    """
    Test that _get_augmentation_table logs a warning when augmentation tables are missing.
    """
    # Only provide one of the required augmentation tables
    present_table = _FLOW_PATH_CONFIG["table_names"]["augmentation"][0]
    iasr_tables = {
        present_table: pd.DataFrame(
            {
                "Flow path": ["A"],
                "Option Name": ["opt1"],
                "forward_capacity_increase": [100],
                "reverse_capacity_increase": [90],
            }
        )
    }
    missing = [
        t
        for t in _FLOW_PATH_CONFIG["table_names"]["augmentation"]
        if t != present_table
    ]
    with caplog.at_level("WARNING"):
        _get_augmentation_table(iasr_tables, _FLOW_PATH_CONFIG)
    # Check that the warning about missing tables is logged
    assert f"Missing augmentation tables: {missing}" in caplog.text


def test_get_cleaned_flow_path_cost_tables_logs_missing_tables(caplog):
    """
    Test that _get_cost_table logs a warning when cost tables are missing.
    """
    # Only provide one of the required cost tables
    cost_scenario = "progressive_change"
    cost_table_names = _FLOW_PATH_CONFIG["table_names"]["cost"][cost_scenario]
    present_table = cost_table_names[0]
    iasr_tables = {
        present_table: pd.DataFrame(
            {
                "id": ["A"],
                "option": ["opt1"],
                "2024_25": [10],
            }
        )
    }
    missing = [t for t in cost_table_names if t != present_table]
    with caplog.at_level("WARNING"):
        _get_cost_table(iasr_tables, cost_scenario, _FLOW_PATH_CONFIG)
    # Check that the warning about missing tables is logged
    assert f"Missing cost tables: {missing}" in caplog.text


def test_template_sub_regional_flow_path_costs_invalid_scenario():
    """Test that an invalid scenario raises a ValueError."""
    iasr_tables = {
        "flow_path_augmentation_options_NNSW-SQ": pd.DataFrame(
            {
                "Flow path": ["NNSW-SQ"],
                "Option Name": ["Option 1"],
                "forward_capacity_increase": [100],
                "reverse_capacity_increase": [100],
            }
        )
    }

    # Test with invalid scenario
    with pytest.raises(ValueError, match="scenario: Invalid Scenario not recognised"):
        _template_sub_regional_flow_path_costs(iasr_tables, "Invalid Scenario")


def test_template_sub_regional_flow_path_costs_no_augmentation_tables():
    """Test that missing all augmentation tables raises a ValueError."""
    iasr_tables = {}  # No tables at all

    with pytest.raises(ValueError, match="No flow_path augmentation tables found"):
        _template_sub_regional_flow_path_costs(iasr_tables, "Step Change")


def test_template_sub_regional_flow_path_costs_no_cost_tables():
    """Test that missing all cost tables raises a ValueError."""
    # Only augmentation tables, no cost tables
    iasr_tables = {
        "flow_path_augmentation_options_NNSW-SQ": pd.DataFrame(
            {
                "Flow path": ["NNSW-SQ"],
                "Option Name": ["Option 1"],
                "forward_capacity_increase": [100],
                "reverse_capacity_increase": [100],
            }
        )
    }

    with pytest.raises(ValueError, match="No cost tables found"):
        _template_sub_regional_flow_path_costs(iasr_tables, "Step Change")


def test_template_sub_regional_flow_path_costs_no_year_columns():
    """Test that cost tables without year columns raise a ValueError."""
    iasr_tables = {
        "flow_path_augmentation_options_NNSW-SQ": pd.DataFrame(
            {
                "Flow path": ["NNSW-SQ"],
                "Option Name": ["Option 1"],
                "forward_capacity_increase": [100],
                "reverse_capacity_increase": [100],
            }
        ),
        "flow_path_augmentation_costs_step_change_and_green_energy_exports_NNSW-SQ": pd.DataFrame(
            {
                "Flow path": ["NNSW-SQ"],
                "Option Name": ["Option 1"],
                # No year columns, only metadata
            }
        ),
    }

    with pytest.raises(ValueError, match="No financial year columns found"):
        _template_sub_regional_flow_path_costs(iasr_tables, "Step Change")


def test_get_prep_activities_missing_flow_path_mapping(csv_str_to_df):
    """Test that missing flow path mapping in preparatory activities raises ValueError."""
    # Create preparatory activities with an unmapped flow path
    prep_acts_csv = """
    Flow__path,              2024-25,    2025-26
    Unknown__Flow__Path,      100,        110
    """
    prep_acts = csv_str_to_df(prep_acts_csv)

    iasr_tables = {
        "flow_path_augmentation_costs_progressive_change_preparatory_activities": prep_acts
    }

    # This should raise ValueError about missing mapping
    with pytest.raises(
        ValueError,
        match="Missing mapping values for the flow paths provided: \\['Unknown Flow Path'\\]",
    ):
        _get_prep_activities_table(iasr_tables, "progressive_change", _FLOW_PATH_CONFIG)


def test_get_prep_activities_missing_option_name_mapping(csv_str_to_df):
    """Test that missing option name mapping in preparatory activities raises ValueError."""
    # Create a custom config with incomplete mappings
    custom_config = _FLOW_PATH_CONFIG.copy()
    custom_config["mappings"] = {
        "prep_activities_name_to_option": {
            "Test Flow Path": "Test Option"  # This will map fine
        },
        "option_to_id": {
            # Missing "Test Option" mapping - this will cause the error
        },
    }

    prep_acts_csv = """
    Flow__path,          2024-25,    2025-26
    Test__Flow__Path,     100,        110
    """
    prep_acts = csv_str_to_df(prep_acts_csv)

    iasr_tables = {
        "flow_path_augmentation_costs_progressive_change_preparatory_activities": prep_acts
    }

    # This should raise ValueError about missing option name mapping
    with pytest.raises(
        ValueError,
        match="Missing mapping values for the option names provided: \\['Test Option'\\]",
    ):
        _get_prep_activities_table(iasr_tables, "progressive_change", custom_config)


def test_get_actionable_projects_missing_flow_path_mapping(csv_str_to_df):
    """Test that missing flow path mapping in actionable projects raises ValueError."""
    # Create actionable projects with an unmapped flow path
    actionable_csv = """
    Flow__path,              2024-25,    2025-26
    Unknown__Project,        999,        999
    """
    actionable = csv_str_to_df(actionable_csv)

    iasr_tables = {
        "flow_path_augmentation_costs_progressive_change_actionable_isp_projects": actionable
    }

    # This should raise ValueError about missing mapping
    with pytest.raises(
        ValueError,
        match="Missing mapping values for the flow paths provided: \\['Unknown Project'\\]",
    ):
        _get_actionable_projects_table(
            iasr_tables, "progressive_change", _FLOW_PATH_CONFIG
        )


def test_get_actionable_projects_missing_option_name_mapping(csv_str_to_df):
    """Test that missing option name mapping in actionable projects raises ValueError."""
    # Create a custom config with incomplete mappings
    custom_config = _FLOW_PATH_CONFIG.copy()
    custom_config["mappings"] = {
        "actionable_name_to_option": {
            "Test Project": "Test Option"  # This will map fine
        },
        "actionable_option_to_id": {
            # Missing "Test Option" mapping - this will cause the error
        },
    }

    actionable_csv = """
    Flow__path,       2024-25,    2025-26
    Test__Project,    999,        999
    """
    actionable = csv_str_to_df(actionable_csv)

    iasr_tables = {
        "flow_path_augmentation_costs_progressive_change_actionable_isp_projects": actionable
    }

    # This should raise ValueError about missing option name mapping
    with pytest.raises(
        ValueError,
        match="Missing mapping values for the option names provided: \\['Test Option'\\]",
    ):
        _get_actionable_projects_table(iasr_tables, "progressive_change", custom_config)


def test_template_rez_transmission_costs_missing_rez_mapping(csv_str_to_df):
    """Test that missing REZ mapping in preparatory activities raises ValueError for REZ config."""
    from ispypsa.templater.mappings import _REZ_CONFIG

    # Create REZ preparatory activities with an unmapped REZ
    prep_acts_csv = """
    REZ__Name,            Option,      2024-25,    2025-26
    Unknown__REZ,         Option 1,    100,        110
    """
    prep_acts = csv_str_to_df(prep_acts_csv)

    iasr_tables = {
        "rez_augmentation_costs_progressive_change_preparatory_activities": prep_acts
    }

    # This should raise ValueError about missing REZ mapping
    with pytest.raises(
        ValueError,
        match="Missing mapping values for the REZ names provided: \\['Unknown REZ'\\]",
    ):
        _get_prep_activities_table(iasr_tables, "progressive_change", _REZ_CONFIG)
