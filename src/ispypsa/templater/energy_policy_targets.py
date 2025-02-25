import logging
import re
from pathlib import Path

import pandas as pd

from .mappings import _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP


def template_renewable_share_targets(
    iasr_tables: dict[str : pd.DataFrame],
) -> pd.DataFrame:
    """Creates ISPyPSA templates for renewable share targets from trajectory CSVs.
    Uses TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP to identify files and their
        corresponding regions.

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: Template containing renewable share targets with columns for
            financial year, region_id, policy_id, and percentage values in decimal form
    """
    logging.info("Creating template for renewable share targets")
    state_renewable_share_targets = []

    # Get mapping for this function
    target_files = _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP[
        "template_renewable_share_targets"
    ]

    for target in target_files:
        df = iasr_tables[target["csv"]]

        df = df.melt(id_vars=df.columns[0], var_name="FY", value_name="pct")
        df = df[df[df.columns[0]].str.contains("share", case=False)]
        df["region_id"] = target["region_id"]
        df["policy_id"] = target["policy_id"]
        df["pct"] = df["pct"].astype(float)

        state_renewable_share_targets.append(
            df[["FY", "region_id", "policy_id", "pct"]]
        )

    merged_state_renewable_share_targets = pd.concat(
        state_renewable_share_targets, ignore_index=True
    )
    merged_state_renewable_share_targets["FY"] = merged_state_renewable_share_targets[
        "FY"
    ].str.replace("-", "_")

    return merged_state_renewable_share_targets


def template_powering_australia_plan(
    power_aus_plan: Path | str, scenario: str
) -> pd.DataFrame:
    """Creates ISPyPSA template for the Powering Australia Plan renewable share
    trajectories for selected scenarios.

    Args:
        powering_aus: pd.DataFrame table from IASR workbook specifying Powering Australia Plan renewable share targets. 
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: Template containing Powering Australia Plan targets
            with columns for financial year, policy_id and percentage values in
            decimal form for the selected scenario
    """
    logging.info("Creating template for Powering Australia Plan")

    # Remove rows containing "Notes" in the first column
    power_aus_plan = power_aus_plan[~power_aus_plan.iloc[:, 0].str.contains("Notes", case=False, na=False)]

    # Filter for rows where the first column matches the specified scenario
    power_aus_plan = power_aus_plan[power_aus_plan.iloc[:, 0].eq(scenario)]

    # Drop the first column (scenario name) to keep only year values
    power_aus_plan = power_aus_plan.iloc[:, 1:].reset_index(drop=True)

    # Melt the dataframe, excluding the first column from id_vars
    power_aus_plan = power_aus_plan.melt(var_name="FY", value_name="pct").dropna(subset=["pct"])

    # Convert percentage to decimal if needed
    power_aus_plan["pct"] = power_aus_plan["pct"].astype(float)

    power_aus_plan["FY"] = power_aus_plan["FY"].str.replace("-", "_")

    # append new column which is the policy_id
    power_aus_plan["policy_id"] = "power_aus"
    return power_aus_plan



def template_technology_capacity_targets(
    iasr_tables: dict[str : pd.DataFrame],
) -> pd.DataFrame:
    """Creates ISPyPSA templates for technology capacity targets including
    CIS renewable target and storage and offshore wind trajectories.
    Uses TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP to identify
    files and their corresponding regions.

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.
    Returns:
        `pd.DataFrame`: Template containing technology capacity trajectories
            with columns for financial year, region_id and capacity in MW
    """
    logging.info("Creating template for technology capacity targets")

    technology_capacity_targets = []
    target_files = _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP[
        "template_technology_capacity_targets"
    ]

    for target in target_files:
        df = iasr_tables[target["csv"]]
        # Extract technology type from the row containing "target (MW)"
        target_row_mask = df.iloc[:, 0].str.contains("target", case=False) & df.iloc[
            :, 0
        ].str.contains("MW", case=False)

        target_row_idx = df.index[target_row_mask][0]
        # Create a new dataframe with just FY and capacity
        values_df = pd.DataFrame(
            {"FY": df.columns[1:], "capacity_mw": df.iloc[target_row_idx, 1:]}
        )

        values_df["capacity_mw"] = values_df["capacity_mw"].astype(float)
        values_df["region_id"] = target["region_id"]
        values_df["policy_id"] = target["policy_id"]

        technology_capacity_targets.append(values_df)

    merged_technology_capacity_targets = pd.concat(
        technology_capacity_targets, ignore_index=True
    )
    merged_technology_capacity_targets["FY"] = merged_technology_capacity_targets[
        "FY"
    ].str.replace("-", "_")

    merged_technology_capacity_targets = merged_technology_capacity_targets.sort_values(
        ["region_id", "policy_id", "FY"]
    ).reset_index(drop=True)

    return merged_technology_capacity_targets


def template_renewable_generation_targets(
    iasr_tables: dict[str : pd.DataFrame],
) -> pd.DataFrame:
    """Creates ISPyPSA templates for renewable generation targets.
    Uses TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP to identify files and their corresponding regions.

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: Template containing renewable capacity trajectories with columns for
            financial year, region_id and capacity in MW (converted from GWh)

    """
    logging.info("Creating template for renewable generation trajectories")

    renewable_generation_targets = []
    target_files = _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP[
        "template_renewable_generation_targets"
    ]

    for target in target_files:
        df = iasr_tables[target["csv"]]
        # Check for GWh in row indices
        if not df.iloc[:, 0].str.contains("GWh", case=False).any():
            raise ValueError(f"No GWh values found in {target['csv']}.csv")

        # if exists, remove the "Notes" row
        df = df[~df.iloc[:, 0].str.contains("Notes", case=False)]

        renewable_gen_target = df.melt(
            id_vars=df.columns[0], var_name="FY", value_name="capacity_gwh"
        )

        # Convert GWh to MWh
        renewable_gen_target["capacity_mwh"] = (
            renewable_gen_target["capacity_gwh"].astype(float) * 1000
        )
        renewable_gen_target["region_id"] = target["region_id"]
        renewable_gen_target["policy_id"] = target["policy_id"]
        renewable_generation_targets.append(
            renewable_gen_target[["FY", "region_id", "policy_id", "capacity_mwh"]]
        )

    # Combine all dataframes
    merged_renewable_generation_targets = pd.concat(
        renewable_generation_targets, ignore_index=True
    )
    merged_renewable_generation_targets["FY"] = merged_renewable_generation_targets[
        "FY"
    ].str.replace("-", "_")

    return merged_renewable_generation_targets
