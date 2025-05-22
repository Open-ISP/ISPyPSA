from typing import Dict, List

import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.helpers import _annuitised_investment_costs
from ispypsa.translator.mappings import _LINE_ATTRIBUTES


def _translate_flow_paths_to_lines(
    ispypsa_tables: Dict[str, pd.DataFrame],
    config: ModelConfig,
) -> pd.DataFrame:
    """Process network line data into a format aligned with PyPSA inputs.

    Separates existing capacity from expansion options and handles financial year costs.

    Args:
        ispypsa_tables: Dictionary of ISPyPSA DataFrames, expecting "flow_paths"
                        and "flow_path_expansion_costs".
        config: Configuration object with temporal, WACC, and network lifetime settings.

    Returns:
        pd.DataFrame: PyPSA style line attributes in tabular format, including both
                      existing lines and potential expansion lines.
    """
    existing_flow_paths_df = ispypsa_tables["flow_paths"]
    existing_lines = _translate_existing_flow_path_capacity_to_lines(
        existing_flow_paths_df
    )

    if config.network.transmission_expansion:
        expansion_lines = _translate_expansion_costs_to_lines(
            ispypsa_tables["flow_path_expansion_costs"],
            existing_lines.copy(),
            config.temporal.capacity_expansion.investment_periods,
            config.temporal.year_type,
            config.wacc,
            config.network.annuitisation_lifetime,
        )
    else:
        expansion_lines = pd.DataFrame()

    all_lines = pd.concat(
        [existing_lines, expansion_lines], ignore_index=True, sort=False
    )

    return all_lines


def _translate_existing_flow_path_capacity_to_lines(
    existing_flow_paths: pd.DataFrame,
) -> pd.DataFrame:
    """Translates existing flow path capacities to PyPSA line components.

    Args:
        existing_flow_paths: DataFrame from ispypsa_tables["flow_paths"].

    Returns:
        `pd.DataFrame`: PyPSA style line attributes in tabular format.
    """
    lines_df = existing_flow_paths.loc[:, list(_LINE_ATTRIBUTES.keys())].copy()
    lines_df = lines_df.rename(columns=_LINE_ATTRIBUTES)

    lines_df["name"] = lines_df["name"] + "_existing"

    lines_df["s_nom_extendable"] = False
    lines_df["capital_cost"] = np.nan

    return lines_df


def _translate_expansion_costs_to_lines(
    expansion_costs: pd.DataFrame,
    existing_lines_df: pd.DataFrame,
    investment_periods: list[int],
    year_type: str,
    wacc: float,
    asset_lifetime: int,
    id_column: str = "flow_path",
    match_column: str = "name",
) -> pd.DataFrame:
    """Translates expansion costs to PyPSA line components.

    This function uses the generic _translate_time_varying_expansion_costs function
    to process the expansion costs, then creates appropriate line components.

    Args:
        expansion_costs: `ISPyPSA` formatted pd.DataFrame detailing
            the expansion costs with financial year columns.
        existing_lines_df: `PyPSA` style line attributes in tabular format.
            Used to source bus/carrier data.
        investment_periods: List of investment years (e.g., [2025, 2030]).
        year_type: Temporal configuration, e.g., "fy" or "calendar".
        wacc: Weighted average cost of capital.
        asset_lifetime: Nominal asset lifetime in years.
        id_column: Column name in expansion_costs containing the identifier.
        match_column: Column name in existing_lines_df to match with id_column.

    Returns:
        `pd.DataFrame`: PyPSA style line attributes in tabular format.
    """
    # Use the generic function to process costs
    processed_costs = _translate_time_varying_expansion_costs(
        expansion_costs=expansion_costs,
        cost_column_suffix="_$/mw",
        investment_periods=investment_periods,
        year_type=year_type,
        wacc=wacc,
        asset_lifetime=asset_lifetime,
    )

    if processed_costs.empty:
        return pd.DataFrame()

    # Prepare for merging with existing lines data
    pypsa_attributes_to_carry = ["bus0", "bus1", "carrier"]

    # For merging, we need to handle the case where match_column might need cleaning
    existing_lines_copy = existing_lines_df.copy()
    if "_existing" in existing_lines_copy[match_column].iloc[0]:
        existing_lines_copy[match_column] = existing_lines_copy[
            match_column
        ].str.replace("_existing", "")

    # Merge with existing lines to get attributes like bus0, bus1, carrier
    df_merged = pd.merge(
        processed_costs,
        existing_lines_copy[[match_column] + pypsa_attributes_to_carry],
        left_on=id_column,
        right_on=match_column,
    )

    # Directly modify df_merged to create the expansion lines
    df_merged["name"] = (
        df_merged["bus0"]
        + "-"
        + df_merged["bus1"]
        + "_exp_"
        + df_merged["investment_year"].astype(str)
    )
    df_merged["s_nom"] = 0.0
    df_merged["s_nom_extendable"] = True
    df_merged["build_year"] = df_merged["investment_year"]
    df_merged["lifetime"] = asset_lifetime

    # Keep only the columns needed for PyPSA lines
    expansion_cols = [
        "name",
        "bus0",
        "bus1",
        "carrier",
        "s_nom",
        "s_nom_extendable",
        "build_year",
        "lifetime",
        "capital_cost",
    ]
    expansion_lines = df_merged[expansion_cols]

    return expansion_lines


def _translate_time_varying_expansion_costs(
    expansion_costs: pd.DataFrame,
    cost_column_suffix: str,
    investment_periods: list[int],
    year_type: str,
    wacc: float,
    asset_lifetime: int,
) -> pd.DataFrame:
    """Generic function to process time-varying expansion costs.

    This function handles the common processing logic for both line and generator expansion costs.

    Args:
        expansion_costs: DataFrame containing expansion cost data with time-varying costs.
        id_column: Name of the column that contains the component identifier.
        cost_column_suffix: Suffix for cost columns (e.g. "_$/mw").
        investment_periods: List of investment years (e.g., [2025, 2030]).
        year_type: Temporal configuration, e.g., "fy" or "calendar".
        wacc: Weighted average cost of capital.
        asset_lifetime: Nominal asset lifetime in years.

    Returns:
        pd.DataFrame: Processed expansion costs with parsed years and annuitized costs.
    """
    if expansion_costs.empty:
        return pd.DataFrame()

    # Extract cost columns (those ending with the specified suffix)
    cost_cols = [
        col for col in expansion_costs.columns if col.endswith(cost_column_suffix)
    ]
    id_vars = [col for col in expansion_costs.columns if col not in cost_cols]

    # Melt the dataframe to convert from wide to long format
    df_melted = expansion_costs.melt(
        id_vars=id_vars,
        value_vars=cost_cols,
        var_name="cost_year_raw_with_suffix",
        value_name="cost_per_unit",
    )

    # Drop rows with NaN costs
    df_melted = df_melted.dropna(subset=["cost_per_unit"])
    if df_melted.empty:
        return pd.DataFrame()

    # Parse financial year from cost column names
    def parse_cost_year(cost_year_raw: str) -> int:
        year_part = cost_year_raw.split(cost_column_suffix)[0]  # e.g., "2025_26"
        if year_type == "fy":
            # For financial year format like "2025_26"
            yy_part = year_part.split("_")[1]  # e.g., "26"
            return 2000 + int(yy_part)  # e.g., 2026, as per spec
        elif year_type == "calendar":
            raise NotImplementedError(
                f"Calendar years not implemented for transmission costs"
            )
        else:
            raise ValueError(f"Unknown year_type: {year_type}")

    df_melted["investment_year"] = df_melted["cost_year_raw_with_suffix"].apply(
        parse_cost_year
    )

    # Filter to only include costs relevant to our investment periods
    df_melted = df_melted[df_melted["investment_year"].isin(investment_periods)]
    if df_melted.empty:
        return pd.DataFrame()

    # Annuitize the costs
    df_melted["capital_cost"] = df_melted["cost_per_unit"].apply(
        lambda x: _annuitised_investment_costs(x, wacc, asset_lifetime)
    )

    return df_melted
