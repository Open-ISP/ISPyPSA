import pandas as pd
import numpy as np
import pytest
from ispypsa.templater.flow_paths import (
    _template_sub_regional_flow_path_costs,
    _get_least_cost_options,
    _get_augmentation_table,
    process_transmission_costs,
    _get_cost_table,
)
from ispypsa.templater.mappings import (
    FLOW_PATH_CONFIG,
    _FLOW_PATH_AGUMENTATION_TABLES,
)

def test_template_sub_regional_flow_path_costs_simple_least_cost_option():
    # Augmentation tables for NNSW-SQ and TAS-VIC
    aug_table_nnsw_sq = pd.DataFrame({
        "id": ["NNSW-SQ", "NNSW-SQ", "NNSW-SQ"],
        "option": ["NNSW-SQ Option 1", "NNSW-SQ Option 2", "NNSW–SQ Option 5"],
        "capacity": [100, 150, 50],
    })
    aug_table_tas_vic = pd.DataFrame({
        "id": ["TAS-VIC", "TAS-VIC"],
        "option": [
            "TAS-VIC Option 1\n(Project Marinus Stage 1)",
            "TAS-VIC Option 2\n(Project Marinus Stage 2)",
        ],
        "capacity": [150, 70],
    })
    # Cost tables for NNSW-SQ and TAS-VIC
    # Option 2 is least cost and has largest increase so should be chosen.
    cost_table_nnsw_sq = pd.DataFrame({
        "id": ["NNSW-SQ", "NNSW-SQ"],
        "option": ["NNSW-SQ Option 1", "NNSW-SQ Option 2"],
        "2024_25": [50, 40],  
        "2025_26": [55, 45],
    })
    # Option 1 is least cost and has largest increase so should be chosen.
    cost_table_tas_vic = pd.DataFrame({
        "id": ["TAS-VIC", "TAS-VIC"],
        "option": [
            "TAS-VIC Option 1\n(Project Marinus Stage 1)",
            "TAS-VIC Option 2\n(Project Marinus Stage 2)"
        ],
        "2024_25": [70, np.nan],  # actionable ISP option has NaN
        "2025_26": [75, np.nan],
    })
    # Prepatory activities and actionable ISP tables (should not be chosen)
    prep_acts = pd.DataFrame({
        "id": ["500kV QNI Connect (NSW works)"],
        "2024_25": [100],
        "2025_26": [110],
    })
    actionable_isp = pd.DataFrame({
        "id": ["Project Marinus Stage 1"],
        "2024_25": [999],
        "2025_26": [999],
    })
    # Compose iasr_tables dict
    iasr_tables = {
        "flow_path_augmentation_options_NNSW-SQ": aug_table_nnsw_sq,
        "flow_path_augmentation_options_TAS-VIC": aug_table_tas_vic,
        "flow_path_costs_forecast_progressive_change_NNSW-SQ": cost_table_nnsw_sq,
        "flow_path_costs_forecast_progressive_change_TAS-VIC": cost_table_tas_vic,
        "flow_path_costs_forecast_progressive_change_preparatory_activities": prep_acts,
        "flow_path_costs_forecast_progressive_change_actionable_isp_projects": actionable_isp,
    }
    scenario = "Progressive Change"
    # Run function
    result = _template_sub_regional_flow_path_costs(iasr_tables, scenario)
    # Check least cost options are chosen for NNSW-SQ and TAS-VIC
    nnsw_sq_row = result[result["id"] == "NNSW-SQ"]
    tas_vic_row = result[result["id"] == "TAS-VIC"]
    assert nnsw_sq_row["option"].iloc[0] == "NNSW-SQ Option 2"
    assert tas_vic_row["option"].iloc[0] == "TAS-VIC Option 1\n(Project Marinus Stage 1)"
    # Check nominal_flow_limit_increase_mw is correct
    assert nnsw_sq_row["nominal_flow_limit_increase_mw"].iloc[0] == 200
    assert tas_vic_row["nominal_flow_limit_increase_mw"].iloc[0] == 150
    # Check cost per year column is correct (cost divided by nominal limit)
    # For NNSW-SQ Option 2: 2024_25 = 40/200 = 0.2, 2025_26 = 45/200 = 0.225
    # For TAS-VIC Option 1: 2024_25 = 70/150 ≈ 0.4667, 2025_26 = 75/150 = 0.5
    assert abs(nnsw_sq_row["2024_25_$/mw"].iloc[0] - 0.2) < 1e-6
    assert abs(nnsw_sq_row["2025_26_$/mw"].iloc[0] - 0.225) < 1e-6
    assert abs(tas_vic_row["2024_25_$/mw"].iloc[0] - (70/150)) < 1e-6
    assert abs(tas_vic_row["2025_26_$/mw"].iloc[0] - 0.5) < 1e-6


def test_template_sub_regional_flow_path_costs_prep_and_actionable_chosen():
    """
    The cost of the non prepatory activities and non actionable isp projects
    have been made very high and therefore prepatory activities and 
    actionable isp projects should be chosen.
    """
    # Augmentation tables for NNSW-SQ and TAS-VIC
    aug_table_nnsw_sq = pd.DataFrame({
        "Flow path": ["NNSW-SQ", "NNSW-SQ", "NNSW-SQ"],
        "Option name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2", "NNSW–SQ Option 5"],
        "transfer_increase_forward_direction_mw": [100, 150, 200],
        "transfer_increase_reverse_direction_mw": [100, 150, 150],
    })
    aug_table_tas_vic = pd.DataFrame({
        "Flow path": ["TAS-VIC", "TAS-VIC"],
        "Option name": [
            "TAS-VIC Option 1\n(Project Marinus Stage 1)",
            "TAS-VIC Option 2\n(Project Marinus Stage 2)"
        ],
        "transfer_increase_forward_direction_mw": [140, 150],
        "transfer_increase_reverse_direction_mw": [150, 130],
    })
    # Standard cost tables (set high or NaN)
    cost_table_nnsw_sq = pd.DataFrame({
        "Flow path": ["NNSW-SQ", "NNSW-SQ"],
        "Option name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2"],
        "2024_25": [1000, 1000],
        "2025_26": [1000, 1000],
    })
    cost_table_tas_vic = pd.DataFrame({
        "Flow path": ["TAS-VIC", "TAS-VIC"],
        "Option name": [
            "TAS-VIC Option 1\n(Project Marinus Stage 1)",
            "TAS-VIC Option 2\n(Project Marinus Stage 2)"
        ],
        "2024_25": [1000, np.nan],
        "2025_26": [1000, np.nan],
    })
    # Prepatory activities and actionable ISP tables (set low cost)
    prep_acts = pd.DataFrame({
        "Flow path": ["500kV QNI Connect (NSW works)"],
        "2024-25": [10],
        "2025-26": [20],
    })
    actionable_isp = pd.DataFrame({
        "Flow path": ["Project Marinus Stage 2"],
        "2024-25": [15],
        "2025-26": [25],
    })
    # Compose iasr_tables dict
    iasr_tables = {
        "flow_path_augmentation_options_NNSW-SQ": aug_table_nnsw_sq,
        "flow_path_augmentation_options_TAS-VIC": aug_table_tas_vic,
        "flow_path_costs_forecast_progressive_change_NNSW-SQ": cost_table_nnsw_sq,
        "flow_path_costs_forecast_progressive_change_TAS-VIC": cost_table_tas_vic,
        "flow_path_costs_forecast_progressive_change_preparatory_activities": prep_acts,
        "flow_path_costs_forecast_progressive_change_actionable_isp_projects": actionable_isp,
    }
    scenario = "Progressive Change"
    # Run function
    result = _template_sub_regional_flow_path_costs(iasr_tables, scenario)
    # Check that the prep activity is chosen for NNSW-SQ and actionable ISP for TAS-VIC
    nnsw_sq_row = result[result["id"] == "NNSW-SQ"]
    tas_vic_row = result[result["id"] == "TAS-VIC"]
    assert nnsw_sq_row["option"].iloc[0] == "NNSW–SQ Option 5"
    assert tas_vic_row["option"].iloc[0] == "TAS-VIC Option 2\n(Project Marinus Stage 2)"
    # Check nominal_flow_limit_increase_mw is correct
    assert nnsw_sq_row["nominal_flow_limit_increase_mw"].iloc[0] == 200
    assert tas_vic_row["nominal_flow_limit_increase_mw"].iloc[0] == 150
    # Check cost per year column is correct (cost divided by nominal limit)
    assert abs(nnsw_sq_row["2024_25_$/mw"].iloc[0] - (10/200)) < 1e-6
    assert abs(nnsw_sq_row["2025_26_$/mw"].iloc[0] - (20/200)) < 1e-6
    assert abs(tas_vic_row["2024_25_$/mw"].iloc[0] - (15/150)) < 1e-6
    assert abs(tas_vic_row["2025_26_$/mw"].iloc[0] - (25/150)) < 1e-6


def test_template_sub_regional_flow_path_costs_use_first_year_with_valid_costs():
    """
    Test that the first year with non-nan cost data for all options is used.
    """
    # NNSW-SQ: only 2025_26 has all non-nan costs
    aug_table_nnsw_sq = pd.DataFrame({
        "Flow path": ["NNSW-SQ", "NNSW-SQ"],
        "Option name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2"],
        "forward_capacity_increase": [150, 200],
        "reverse_capacity_increase": [200, 150],
    })
    cost_table_nnsw_sq = pd.DataFrame({
        "Flow path": ["NNSW-SQ", "NNSW-SQ"],
        "Option name": ["NNSW-SQ Option 1", "NNSW-SQ Option 2"],
        "2024_25": [np.nan, 50],
        "2025_26": [35, 45],
    })
    # TAS-VIC: all years have valid costs
    aug_table_tas_vic = pd.DataFrame({
        "Flow path": ["TAS-VIC", "TAS-VIC"],
        "Option name": ["TAS-VIC Option 1", "TAS-VIC Option 2"],
        "forward_capacity_increase": [90, 100],
        "reverse_capacity_increase": [100, 90],
    })
    cost_table_tas_vic = pd.DataFrame({
        "Flow path": ["TAS-VIC", "TAS-VIC"],
        "Option name": ["TAS-VIC Option 1", "TAS-VIC Option 2"],
        "2024_25": [100, 10],
        "2025_26": [10, 100],
    })
    iasr_tables = {
        "flow_path_augmentation_options_NNSW-SQ": aug_table_nnsw_sq,
        "flow_path_augmentation_options_TAS-VIC": aug_table_tas_vic,
        "flow_path_costs_forecast_progressive_change_NNSW-SQ": cost_table_nnsw_sq,
        "flow_path_costs_forecast_progressive_change_TAS-VIC": cost_table_tas_vic,
    }
    scenario = "Progressive Change"
    result = _template_sub_regional_flow_path_costs(iasr_tables, scenario)
    # NNSW-SQ: Only 2025_26 has all non-nan costs, so selection is based on that year for all years
    nnsw_sq_row = result[result["flow_path"] == "NNSW-SQ"]
    assert nnsw_sq_row["option"].iloc[0] == "NNSW-SQ Option 1"
    assert nnsw_sq_row["nominal_capacity_increase"].iloc[0] == 200
    assert np.isnan(nnsw_sq_row["2024_25_$/mw"].iloc[0])
    assert abs(nnsw_sq_row["2025_26_$/mw"].iloc[0] - (35/200)) < 1e-6
    # TAS-VIC: both years valid, Option 2 is least cost only in first, 
    # but should be chosen on this basis.
    tas_vic_row = result[result["flow_path"] == "TAS-VIC"]
    assert tas_vic_row["option"].iloc[0] == "TAS-VIC Option 2"
    assert tas_vic_row["nominal_capacity_increase"].iloc[0] == 100
    assert abs(tas_vic_row["2024_25_$/mw"].iloc[0] - (10/100)) < 1e-6
    assert abs(tas_vic_row["2025_26_$/mw"].iloc[0] - (100/100)) < 1e-6


def test_get_least_cost_options_logs_unmatched(caplog):
    """
    Test that _get_least_cost_options logs dropped flow_path/option_name pairs from both tables.
    """
    # Augmentation table has one extra option not in cost table
    aug_table = pd.DataFrame({
        "id": ["A", "A", "B"],
        "option": ["opt1", "opt2", "opt3"],
        "capacity": [100, 200, 300],
    })
    # Cost table has one extra option not in aug table
    cost_table = pd.DataFrame({
        "id": ["A", "A", "B"],
        "option": ["opt1", "opt2", "opt4"],
        "2024_25": [10, 20, 30],
        "2025_26": [15, 25, 35],
    })
    # Only the (B, opt3) and (B, opt4) pairs should be dropped
    with caplog.at_level("INFO"):
        result = _get_least_cost_options(aug_table, cost_table)
    # Check logs for both dropped pairs
    assert "Dropped options from augmentation table: [('B', 'opt3')]" in caplog.text
    assert "Dropped options from cost table: [('B', 'opt4')]" in caplog.text


def test_get_full_flow_path_aug_table_logs_missing_tables(caplog):
    """
    Test that _get_augmentation_table logs a warning when augmentation tables are missing.
    """
    # Only provide one of the required augmentation tables
    present_table = FLOW_PATH_CONFIG["table_names"]["augmentation"][0]
    iasr_tables = {
        present_table: pd.DataFrame({
            "Flow path": ["A"],
            "Option Name": ["opt1"],
            "transfer_increase_forward_direction_mw": [100],
            "transfer_increase_reverse_direction_mw": [90],
        })
    }
    missing = [t for t in FLOW_PATH_CONFIG["table_names"]["augmentation"] if t != present_table]
    with caplog.at_level("WARNING"):
        _get_augmentation_table(iasr_tables, FLOW_PATH_CONFIG)
    # Check that the warning about missing tables is logged
    assert f"Missing augmentation tables: {missing}" in caplog.text


def test_get_cleaned_flow_path_cost_tables_logs_missing_tables(caplog):
    """
    Test that _get_cost_table logs a warning when cost tables are missing.
    """
    # Only provide one of the required cost tables
    cost_scenario = "progressive_change"
    cost_table_names = FLOW_PATH_CONFIG["table_names"]["cost"][cost_scenario]
    present_table = cost_table_names[0]
    iasr_tables = {
        present_table: pd.DataFrame({
            "id": ["A"],
            "option": ["opt1"],
            "2024_25": [10],
        })
    }
    missing = [t for t in cost_table_names if t != present_table]
    with caplog.at_level("WARNING"):
        _get_cost_table(iasr_tables, cost_scenario, FLOW_PATH_CONFIG)
    # Check that the warning about missing tables is logged
    assert f"Missing cost tables: {missing}" in caplog.text