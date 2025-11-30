import re
from pathlib import Path
from typing import List, Literal

import numpy as np
import pandas as pd
from isp_trace_parser import get_data

from ispypsa.translator.helpers import (
    _add_investment_periods_as_build_years,
    _annuitised_investment_costs,
    _get_commissioning_or_build_year_as_int,
    _get_financial_year_int_from_string,
)
from ispypsa.translator.mappings import (
    _BATTERY_ATTRIBUTE_ORDER,
    _ECAA_BATTERY_ATTRIBUTES,
    _NEW_ENTRANT_BATTERY_ATTRIBUTES,
)


def _translate_ecaa_batteries(
    ispypsa_tables: dict[str, pd.DataFrame],
    investment_periods: list[int],
    regional_granularity: str = "sub_regions",
    rez_handling: str = "discrete_nodes",
    year_type: str = "fy",
) -> pd.DataFrame:
    """Process data on existing, committed, anticipated, and additional (ECAA) batteries
    into a format aligned with PyPSA inputs.

    Args:
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).
        investment_periods: list of years in which investment periods start obtained
            from the model configuration.
        regional_granularity: Regional granularity of the nodes obtained from the model
            configuration. Defaults to "sub_regions".
        year_type: str which should be "fy" or "calendar". If "fy" then investment
            period ints are interpreted as specifying financial years (according to the
            calendar year the financial year ends in).
        rez_handling: str from the model configuration that defines whether REZs are
            modelled as distinct nodes, one of "discrete_nodes" or "attached_to_parent_node". Defaults to "discrete_nodes".

    Returns:
        `pd.DataFrame`: `PyPSA` style ECAA battery attributes in tabular format.
    """

    ecaa_batteries = ispypsa_tables["ecaa_batteries"]
    if ecaa_batteries.empty:
        # TODO: log
        # raise error?
        return pd.DataFrame()

    # calculate lifetime based on expected closure_year - build_year:
    ecaa_batteries["lifetime"] = ecaa_batteries["closure_year"].map(
        lambda x: float(x - investment_periods[0]) if x > 0 else np.inf
    )
    ecaa_batteries = ecaa_batteries[ecaa_batteries["lifetime"] > 0].copy()

    battery_attributes = _ECAA_BATTERY_ATTRIBUTES.copy()
    # Decide which column to rename to be the bus column.
    if regional_granularity == "sub_regions":
        bus_column = "sub_region_id"
    elif regional_granularity == "nem_regions":
        bus_column = "region_id"
    elif regional_granularity == "single_region":
        # No existing column to use for bus, so create a new one.
        ecaa_batteries["bus"] = "NEM"
        bus_column = "bus"  # Name doesn't need to change.
    battery_attributes[bus_column] = "bus"

    if rez_handling == "discrete_nodes":
        # make sure batteries are still connected to the REZ bus where applicable
        rez_mask = ~ecaa_batteries["rez_id"].isna()
        ecaa_batteries.loc[rez_mask, bus_column] = ecaa_batteries.loc[
            rez_mask, "rez_id"
        ]

    ecaa_batteries["commissioning_date"] = ecaa_batteries["commissioning_date"].apply(
        _get_commissioning_or_build_year_as_int,
        default_build_year=investment_periods[0],
        year_type=year_type,
    )

    ecaa_batteries["p_nom_extendable"] = False
    ecaa_batteries["capital_cost"] = 0.0

    # filter and rename columns according to PyPSA input names:
    ecaa_batteries_pypsa_format = ecaa_batteries.loc[
        :, battery_attributes.keys()
    ].rename(columns=battery_attributes)

    columns_in_order = [
        col
        for col in _BATTERY_ATTRIBUTE_ORDER
        if col in ecaa_batteries_pypsa_format.columns
    ]

    return ecaa_batteries_pypsa_format[columns_in_order]


def _translate_new_entrant_batteries(
    ispypsa_tables: dict[str, pd.DataFrame],
    investment_periods: list[int],
    wacc: float,
    regional_granularity: str = "sub_regions",
    rez_handling: str = "discrete_nodes",
) -> pd.DataFrame:
    """Process data on new entrant batteries into a format aligned with `PyPSA` inputs.

    Args:
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).
        investment_periods: list of years in which investment periods start obtained
            from the model configuration.
        wacc: as float, weighted average cost of capital, an interest rate specifying
            how expensive it is to borrow money for the asset investment.
        regional_granularity: Regional granularity of the nodes obtained from the model
            configuration. Defaults to "sub_regions".
        rez_handling: str from the model configuration that defines whether REZs are
            modelled as distinct nodes, one of "discrete_nodes" or "attached_to_parent_node". Defaults to "discrete_nodes".

    Returns:
        `pd.DataFrame`: `PyPSA` style new entrant battery attributes in tabular format.
    """

    new_entrant_batteries = ispypsa_tables["new_entrant_batteries"]
    if new_entrant_batteries.empty:
        # TODO: log
        # raise error?
        return pd.DataFrame()

    battery_attributes = _NEW_ENTRANT_BATTERY_ATTRIBUTES.copy()
    # Decide which column to rename to be the bus column.
    if regional_granularity == "sub_regions":
        bus_column = "sub_region_id"
    elif regional_granularity == "nem_regions":
        bus_column = "region_id"
    elif regional_granularity == "single_region":
        # No existing column to use for bus, so create a new one.
        new_entrant_batteries["bus"] = "NEM"
        bus_column = "bus"  # Name doesn't need to change.
    battery_attributes[bus_column] = "bus"

    if rez_handling == "discrete_nodes":
        # make sure batteries are still connected to the REZ bus where applicable
        rez_mask = ~new_entrant_batteries["rez_id"].isna()
        new_entrant_batteries.loc[rez_mask, bus_column] = new_entrant_batteries.loc[
            rez_mask, "rez_id"
        ]

    # create a row for each new entrant battery in each possible build year (investment period)
    new_entrant_batteries_all_build_years = _add_investment_periods_as_build_years(
        new_entrant_batteries, investment_periods
    )
    battery_df_with_build_costs = _add_new_entrant_battery_build_costs(
        new_entrant_batteries_all_build_years,
        ispypsa_tables["new_entrant_build_costs"],
    )
    battery_df_with_capital_costs = (
        _calculate_annuitised_new_entrant_battery_capital_costs(
            battery_df_with_build_costs,
            wacc,
        )
    )
    # nan capex -> build limit of 0.0MW (no build of this battery in this region allowed)
    battery_df_with_capital_costs = battery_df_with_capital_costs[
        ~battery_df_with_capital_costs["capital_cost"].isna()
    ].copy()

    # then add build_year to battery name to maintain unique battery ID column
    battery_df_with_capital_costs["storage_name"] = (
        battery_df_with_capital_costs["storage_name"]
        + "_"
        + battery_df_with_capital_costs["build_year"].astype(str)
    )

    # add a p_nom column set to 0.0 for all new entrants:
    battery_df_with_capital_costs["p_nom"] = 0.0

    # Convert p_min_pu from percentage to float between 0-1:
    battery_df_with_capital_costs["p_nom_extendable"] = True

    # filter and rename columns to PyPSA format
    new_entrant_batteries_pypsa_format = battery_df_with_capital_costs.loc[
        :, battery_attributes.keys()
    ].rename(columns=battery_attributes)

    columns_in_order = [
        col
        for col in _BATTERY_ATTRIBUTE_ORDER
        if col in new_entrant_batteries_pypsa_format.columns
    ]

    return new_entrant_batteries_pypsa_format[columns_in_order]


def _add_new_entrant_battery_build_costs(
    new_entrant_batteries: pd.DataFrame,
    new_entrant_build_costs: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge build costs into new_entrant_batteries table.

    Args:
        new_entrant_batteries table: dataframe containing `ISPyPSA` formatted
            new-entrant battery detail, with a row for each battery in every possible
            build year. Must have column "build_year" with integer values.
        new_entrant_build_costs: `ISPyPSA` formatted dataframe with build costs in $/MW for
            each new-entrant battery type and build year.

    Returns:
        pd.DataFrame: new_entrant_batteries table with build costs merged in as new column
            called "build_cost_$/mw".

    Raises:
        ValueError: if new_entrant_batteries table does not have column "build_year" or
            if any new entrant batteries have build costs missing/undefined.

    Notes:
        1. The function assumes that new_entrant_build_costs has a "technology" column
           that matches with the "technology_type" column in new_entrant_batteries table.
    """

    if "build_year" not in new_entrant_batteries.columns:
        raise ValueError(
            "new_entrant_batteries table must have column 'build_year' to merge in build costs."
        )

    build_costs = new_entrant_build_costs.melt(
        id_vars=["technology"], var_name="build_year", value_name="build_cost_$/mw"
    ).rename(columns={"technology": "technology_type"})
    # get the financial year int from build_year string:
    build_costs["build_year"] = build_costs["build_year"].apply(
        _get_financial_year_int_from_string,
        args=("new entrant battery build costs", "fy"),
    )
    # make sure new_entrant_batteries has build_year column with ints:
    new_entrant_batteries["build_year"] = new_entrant_batteries["build_year"].astype(
        "int64"
    )
    # return battery table with build costs merged in
    new_entrants_with_build_costs = new_entrant_batteries.merge(build_costs, how="left")

    # check for empty/undefined build costs:
    undefined_build_cost_batteries = (
        new_entrants_with_build_costs[
            new_entrants_with_build_costs["build_cost_$/mw"].isna()
        ]["technology_type"]
        .unique()
        .tolist()
    )
    if undefined_build_cost_batteries:
        raise ValueError(
            f"Undefined build costs for new entrant batteries: {undefined_build_cost_batteries}"
        )

    return new_entrants_with_build_costs


def _calculate_annuitised_new_entrant_battery_capital_costs(
    new_entrant_batteries: pd.DataFrame,
    wacc: float,
) -> pd.DataFrame:
    """Calculates annuitised capital cost of each new entrant battery in each possible
    build year.

    Args:
        new_entrant_batteries: dataframe containing `ISPyPSA` formatted
            new-entrant battery detail, with a row for each battery in every possible
            build year and sub-region.
        wacc: as float, weighted average cost of capital, an interest rate specifying
            how expensive it is to borrow money for the asset investment.

    Returns:
        new_entrant_batteries: `ISPyPSA` formatted dataframe with new column
            "capital_cost" containing the annuitised capital cost of each new entrant
            battery in each possible build year.
    """
    new_entrant_batteries["capital_cost"] = (
        new_entrant_batteries["build_cost_$/mw"]
        * (new_entrant_batteries["technology_specific_lcf_%"] / 100)
        + new_entrant_batteries["connection_cost_$/mw"]
    )
    # annuitise:
    new_entrant_batteries["capital_cost"] = new_entrant_batteries.apply(
        lambda x: _annuitised_investment_costs(x["capital_cost"], wacc, x["lifetime"]),
        axis=1,
    )
    # add annual fixed opex (first converting to $/MW/annum)
    new_entrant_batteries["capital_cost"] += (
        new_entrant_batteries["fom_$/kw/annum"] * 1000
    )
    return new_entrant_batteries
