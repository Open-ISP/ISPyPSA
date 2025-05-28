import numpy as np
import pandas as pd
import pytest

from ispypsa.templater.flow_paths import (
    _get_augmentation_table,
    _get_cost_table,
    _get_least_cost_options,
    _template_rez_transmission_costs,
    process_transmission_costs,
)
from ispypsa.templater.mappings import (
    _REZ_CONFIG,
    _REZ_PREPATORY_ACTIVITIES_NAME_TO_REZ_AND_OPTION_NAME,
)


def test_template_rez_transmission_costs_simple_least_cost_option():
    # Augmentation tables for SWQLD1 and SWV1 REZs
    aug_table_swqld = pd.DataFrame(
        {
            "REZ constraint ID": ["SWQLD1", "SWQLD1", "SWQLD1"],
            "Option": ["Option 1", "Option 2", "Option 3"],
            "Additional network capacity (MW)": [100, 200, 40],
        }
    )
    aug_table_swv = pd.DataFrame(
        {
            "REZ constraint ID": ["SWV1", "SWV1", "SWV1"],
            "Option": [
                "Option 1A",
                "Option 1B",
                "Option 2A",
            ],
            "Additional network capacity (MW)": [150, 70, 120],
        }
    )
    # Cost tables for SWQLD1 and SWV1 REZs
    # Option 2 is least cost and has the largest increase so should be chosen.
    cost_table_swqld = pd.DataFrame(
        {
            "REZ constraint ID": ["SWQLD1", "SWQLD1", "SWQLD1"],
            "Option": ["Option 1", "Option 2", "Option 3"],
            "2024_25": [50, 40, 60],
            "2025_26": [55, 45, 65],
        }
    )
    # Option 1A is least cost and has the largest increase so should be chosen.
    cost_table_swv = pd.DataFrame(
        {
            "REZ constraint ID": ["SWV1", "SWV1", "SWV1"],
            "Option": ["Option 1A", "Option 1B", "Option 2A"],
            "2024_25": [70, 80, 100],
            "2025_26": [75, 85, 110],
        }
    )
    # Preparatory activities table (should not be chosen due to higher costs)
    # Using entries that exist in _REZ_PREPATORY_ACTIVITIES_NAME_TO_REZ_AND_OPTION_NAME
    prep_acts = pd.DataFrame(
        {
            "REZ": [
                "Darling Downs REZ Expansion(Stage 1)",
                "South West Victoria REZ Option 1A",
            ],
            "2024_25": [100, 110],
            "2025_26": [110, 120],
        }
    )

    # Compose iasr_tables dict with correct table names
    iasr_tables = {
        "rez_augmentation_options_QLD": aug_table_swqld,
        "rez_augmentation_options_VIC": aug_table_swv,
        "rez_augmentation_costs_progressive_change_QLD": cost_table_swqld,
        "rez_augmentation_costs_progressive_change_VIC": cost_table_swv,
        "rez_augmentation_costs_progressive_change_preparatory_activities": prep_acts,
    }
    scenario = "Progressive Change"
    # Run function
    result = _template_rez_transmission_costs(iasr_tables, scenario, ["SWV1", "SWQLD1"])
    # Check least cost options are chosen for SWQLD1 and SWV1
    swqld_row = result[result["rez_constraint_id"] == "SWQLD1"]
    swv_row = result[result["rez_constraint_id"] == "SWV1"]
    assert swqld_row["option"].iloc[0] == "Option 2"
    assert swv_row["option"].iloc[0] == "Option 1A"
    # Check additional_network_capacity_mw is correct
    assert swqld_row["additional_network_capacity_mw"].iloc[0] == 200
    assert swv_row["additional_network_capacity_mw"].iloc[0] == 150
    # Check cost per year column is correct (cost divided by capacity)
    # For SWQLD1 Option 2: 2024_25 = 40/200 = 0.2, 2025_26 = 45/200 = 0.225
    # For SWV1 Option 1A: 2024_25 = 70/150 ≈ 0.4667, 2025_26 = 75/150 = 0.5
    assert abs(swqld_row["2024_25_$/mw"].iloc[0] - 0.2) < 1e-6
    assert abs(swqld_row["2025_26_$/mw"].iloc[0] - 0.225) < 1e-6
    assert abs(swv_row["2024_25_$/mw"].iloc[0] - (70 / 150)) < 1e-6
    assert abs(swv_row["2025_26_$/mw"].iloc[0] - 0.5) < 1e-6


def test_template_rez_transmission_costs_prep_activities_chosen():
    """
    The cost of the non preparatory activities have been made very high
    and therefore preparatory activities should be chosen.
    """
    # Augmentation tables for SWQLD1 and SWV1 REZs
    aug_table_swqld = pd.DataFrame(
        {
            "REZ constraint ID": ["SWQLD1", "SWQLD1", "SWQLD1"],
            "Option": ["Option 1", "Option 2", "Option 3"],
            "Additional network capacity (MW)": [100, 150, 200],
        }
    )
    aug_table_swv = pd.DataFrame(
        {
            "REZ constraint ID": ["SWV1", "SWV1", "SWV1"],
            "Option": ["Option 1A", "Option 1B", "Option 2A"],
            "Additional network capacity (MW)": [140, 150, 160],
        }
    )
    # Standard cost tables - options that have costs in prep activities should have NaN here
    cost_table_swqld = pd.DataFrame(
        {
            "REZ constraint ID": ["SWQLD1", "SWQLD1", "SWQLD1"],
            "Option": ["Option 1", "Option 2", "Option 3"],
            "2024_25": [
                np.nan,
                1000,
                1000,
            ],  # Option 1 has NaN since it's in prep activities
            "2025_26": [np.nan, 1000, 1000],
        }
    )
    cost_table_swv = pd.DataFrame(
        {
            "REZ constraint ID": ["SWV1", "SWV1", "SWV1"],
            "Option": ["Option 1A", "Option 1B", "Option 2A"],
            "2024_25": [
                1000,
                1000,
                np.nan,
            ],  # Option 2A has NaN since it's in prep activities
            "2025_26": [1000, 1000, np.nan],
        }
    )
    # Preparatory activities table (set low cost)
    # Using entries that exist in _REZ_PREPATORY_ACTIVITIES_NAME_TO_REZ_AND_OPTION_NAME
    prep_acts = pd.DataFrame(
        {
            "REZ": [
                "Darling Downs REZ Expansion(Stage 1)",
                "South West Victoria REZ Option 2A",
            ],
            "2024_25": [10, 15],
            "2025_26": [20, 25],
        }
    )

    # Compose iasr_tables dict
    iasr_tables = {
        "rez_augmentation_options_QLD": aug_table_swqld,
        "rez_augmentation_options_VIC": aug_table_swv,
        "rez_augmentation_costs_progressive_change_QLD": cost_table_swqld,
        "rez_augmentation_costs_progressive_change_VIC": cost_table_swv,
        "rez_augmentation_costs_progressive_change_preparatory_activities": prep_acts,
    }
    scenario = "Progressive Change"
    # Run function
    result = _template_rez_transmission_costs(iasr_tables, scenario, ["SWV1", "SWQLD1"])
    # Check that the prep activity is chosen for SWQLD1 and SWV1
    swqld_row = result[result["rez_constraint_id"] == "SWQLD1"]
    swv_row = result[result["rez_constraint_id"] == "SWV1"]
    assert swqld_row["option"].iloc[0] == "Option 1"
    assert swv_row["option"].iloc[0] == "Option 2A"
    # Check additional_network_capacity_mw is correct
    assert swqld_row["additional_network_capacity_mw"].iloc[0] == 100
    assert swv_row["additional_network_capacity_mw"].iloc[0] == 160
    # Check cost per year column is correct (cost divided by capacity)
    assert abs(swqld_row["2024_25_$/mw"].iloc[0] - (10 / 100)) < 1e-6
    assert abs(swqld_row["2025_26_$/mw"].iloc[0] - (20 / 100)) < 1e-6
    assert abs(swv_row["2024_25_$/mw"].iloc[0] - (15 / 160)) < 1e-6
    assert abs(swv_row["2025_26_$/mw"].iloc[0] - (25 / 160)) < 1e-6


def test_template_rez_transmission_costs_use_first_year_with_valid_costs():
    """
    Test that the first year with non-nan cost data for all options is used.
    """
    # SWQLD1: only 2025_26 has all non-nan costs
    aug_table_swqld = pd.DataFrame(
        {
            "REZ constraint ID": ["SWQLD1", "SWQLD1", "SWQLD1"],
            "Option": ["Option 1", "Option 2", "Option 3"],
            "Additional network capacity (MW)": [150, 150, 150],
        }
    )
    # Even though option 3 is cheaper than option 2 in 2024_25, option 1 should get
    # chosen because 2025_26 is used as the comparison year and it has the lowest cost there.
    cost_table_swqld = pd.DataFrame(
        {
            "REZ constraint ID": ["SWQLD1", "SWQLD1", "SWQLD1"],
            "Option": ["Option 1", "Option 2", "Option 3"],
            "2024_25": [np.nan, 50, 10],
            "2025_26": [35, 45, 50],
        }
    )
    # SWV1: all years have valid costs
    aug_table_swv = pd.DataFrame(
        {
            "REZ constraint ID": ["SWV1", "SWV1"],
            "Option": ["Option 1A", "Option 1B"],
            "Additional network capacity (MW)": [90, 100],
        }
    )
    cost_table_swv = pd.DataFrame(
        {
            "REZ constraint ID": ["SWV1", "SWV1"],
            "Option": ["Option 1A", "Option 1B"],
            "2024_25": [100, 10],
            "2025_26": [10, 100],
        }
    )
    iasr_tables = {
        "rez_augmentation_options_QLD": aug_table_swqld,
        "rez_augmentation_options_VIC": aug_table_swv,
        "rez_augmentation_costs_progressive_change_QLD": cost_table_swqld,
        "rez_augmentation_costs_progressive_change_VIC": cost_table_swv,
    }
    scenario = "Progressive Change"
    result = _template_rez_transmission_costs(iasr_tables, scenario, ["SWV1", "SWQLD1"])
    # SWQLD1: Only 2025_26 has all non-nan costs, so selection is based on that year for all years
    swqld_row = result[result["rez_constraint_id"] == "SWQLD1"]
    assert swqld_row["option"].iloc[0] == "Option 1"
    assert swqld_row["additional_network_capacity_mw"].iloc[0] == 150
    assert np.isnan(swqld_row["2024_25_$/mw"].iloc[0])
    assert abs(swqld_row["2025_26_$/mw"].iloc[0] - (35 / 150)) < 1e-6
    # SWV1: both years valid, Option 1B is the least cost only in first,
    # but should be chosen on this basis.
    swv_row = result[result["rez_constraint_id"] == "SWV1"]
    assert swv_row["option"].iloc[0] == "Option 1B"
    assert swv_row["additional_network_capacity_mw"].iloc[0] == 100
    assert abs(swv_row["2024_25_$/mw"].iloc[0] - (10 / 100)) < 1e-6
    assert abs(swv_row["2025_26_$/mw"].iloc[0] - (100 / 100)) < 1e-6
