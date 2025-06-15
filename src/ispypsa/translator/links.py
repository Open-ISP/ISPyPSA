import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.helpers import _annuitised_investment_costs
from ispypsa.translator.mappings import _LINK_ATTRIBUTES


def _translate_flow_paths_to_links(
    ispypsa_tables: dict[str, pd.DataFrame],
    config: ModelConfig,
) -> pd.DataFrame:
    """Process network line data into the PyPSA friendly format.

    Args:
        ispypsa_tables: Dictionary of ISPyPSA DataFrames, expecting "flow_paths"
                        and "flow_path_expansion_costs".
        config: Configuration object with temporal, WACC, and network lifetime settings.

    Returns:
        pd.DataFrame: PyPSA style links attributes in tabular format, including both
                      existing links and potential expansion links.
    """
    existing_flow_paths_df = ispypsa_tables["flow_paths"]
    existing_links = _translate_existing_flow_path_capacity_to_links(
        existing_flow_paths_df
    )

    if config.network.transmission_expansion:
        expansion_links = _translate_expansion_costs_to_links(
            ispypsa_tables["flow_path_expansion_costs"],
            existing_links.copy(),
            config.temporal.capacity_expansion.investment_periods,
            config.temporal.year_type,
            config.wacc,
            config.network.annuitisation_lifetime,
            "flow_path",
            "isp_name",
        )
    else:
        expansion_links = pd.DataFrame()

    all_links = pd.concat(
        [existing_links, expansion_links], ignore_index=True, sort=False
    )

    return all_links


def _translate_existing_flow_path_capacity_to_links(
    existing_flow_paths: pd.DataFrame,
) -> pd.DataFrame:
    """Translates existing flow path capacities to PyPSA link components.

    Args:
        existing_flow_paths: DataFrame from ispypsa_tables["flow_paths"].

    Returns:
        `pd.DataFrame`: PyPSA style link attributes in tabular format.
    """
    links_df = existing_flow_paths.loc[:, list(_LINK_ATTRIBUTES.keys())].copy()
    links_df = links_df.rename(columns=_LINK_ATTRIBUTES)

    links_df["isp_name"] = links_df["name"].copy()

    links_df["name"] = links_df["name"] + "_existing"
    links_df["p_nom_extendable"] = False
    links_df["p_min_pu"] = -1.0 * (links_df["p_nom_reverse"] / links_df["p_nom"])
    links_df["build_year"] = 0
    links_df["lifetime"] = np.inf
    links_df = links_df.drop(columns=["p_nom_reverse"])
    links_df["capital_cost"] = np.nan

    col_order = [
        "isp_name",
        "name",
        "carrier",
        "bus0",
        "bus1",
        "p_nom",
        "p_min_pu",
        "build_year",
        "lifetime",
        "capital_cost",
        "p_nom_extendable",
    ]

    return links_df.loc[:, col_order]


def _translate_expansion_costs_to_links(
    expansion_costs: pd.DataFrame,
    existing_links_df: pd.DataFrame,
    investment_periods: list[int],
    year_type: str,
    wacc: float,
    asset_lifetime: int,
    id_column: str = "flow_path",
    match_column: str = "name",
) -> pd.DataFrame:
    """Translates expansion costs to PyPSA link components.

    This function uses the generic _translate_time_varying_expansion_costs function
    to process the expansion costs, then creates appropriate link components.

    Args:
        expansion_costs: `ISPyPSA` formatted pd.DataFrame detailing
            the expansion costs with financial year columns.
        existing_links_df: `PyPSA` style link attributes in tabular format.
            Used to source bus/carrier data.
        investment_periods: List of investment years (e.g., [2025, 2030]).
        year_type: Temporal configuration, e.g., "fy" or "calendar".
        wacc: Weighted average cost of capital.
        asset_lifetime: Nominal asset lifetime in years.
        id_column: Column name in expansion_costs containing the identifier.
        match_column: Column name in existing_links_df to match with id_column.

    Returns:
        `pd.DataFrame`: PyPSA style link attributes in tabular format.
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

    # Prepare for merging with existing links data
    attributes_to_carry = ["isp_name", "bus0", "bus1", "carrier"]

    # Merge with existing links to get attributes like bus0, bus1, carrier
    df_merged = pd.merge(
        processed_costs,
        existing_links_df.loc[:, attributes_to_carry],
        left_on=id_column,
        right_on=match_column,
    )

    # Directly modify df_merged to create the expansion links
    df_merged["name"] = (
        df_merged["isp_name"] + "_exp_" + df_merged["investment_year"].astype(str)
    )
    df_merged["p_nom"] = 0.0
    df_merged["p_nom_extendable"] = True
    df_merged["p_min_pu"] = -1.0
    df_merged["build_year"] = df_merged["investment_year"]
    df_merged["lifetime"] = np.inf

    # Keep only the columns needed for PyPSA links
    expansion_cols = [
        "isp_name",
        "name",
        "bus0",
        "bus1",
        "carrier",
        "p_nom",
        "p_nom_extendable",
        "p_min_pu",
        "build_year",
        "lifetime",
        "capital_cost",
    ]
    expansion_links = df_merged[expansion_cols]

    return expansion_links


def _translate_time_varying_expansion_costs(
    expansion_costs: pd.DataFrame,
    cost_column_suffix: str,
    investment_periods: list[int],
    year_type: str,
    wacc: float,
    asset_lifetime: int,
) -> pd.DataFrame:
    """Process time-varying expansion costs for flow paths and rezs.

    Converts from years as columns to years as rows, extracts model year from column
    name, and annuitises expansion costs.

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
