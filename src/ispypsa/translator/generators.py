import re
from pathlib import Path
from typing import List, Literal

import numpy as np
import pandas as pd
from isp_trace_parser import get_data

from ispypsa.templater.helpers import (
    _fuzzy_match_names,
    _snakecase_string,
    _where_any_substring_appears,
)
from ispypsa.translator.helpers import (
    _add_investment_periods_as_build_years,
    _annuitised_investment_costs,
    _get_commissioning_or_build_year_as_int,
    _get_financial_year_int_from_string,
)
from ispypsa.translator.mappings import (
    _CARRIER_TO_FUEL_COST_TABLES,
    _ECAA_GENERATOR_ATTRIBUTES,
    _GENERATOR_ATTRIBUTE_ORDER,
    _NEW_ENTRANT_GENERATOR_ATTRIBUTES,
)
from ispypsa.translator.temporal_filters import _time_series_filter
from ispypsa.translator.time_series_checker import _check_time_series


def _translate_ecaa_generators(
    ispypsa_tables: dict[str, pd.DataFrame],
    investment_periods: list[int],
    regional_granularity: str = "sub_regions",
    rez_handling: str = "discrete_nodes",
    year_type: str = "fy",
) -> pd.DataFrame:
    """Process data on existing, committed, anticipated, and additional (ECAA) generators
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
        `pd.DataFrame`: `PyPSA` style ECAA generator attributes in tabular format.
    """

    ecaa_generators = ispypsa_tables["ecaa_generators"]
    if ecaa_generators.empty:
        # TODO: log
        # raise error?
        return pd.DataFrame()

    # calculate lifetime based on expected closure_year - build_year:
    ecaa_generators["lifetime"] = ecaa_generators["closure_year"].map(
        lambda x: float(x - investment_periods[0]) if x > 0 else np.inf
    )
    ecaa_generators = ecaa_generators[ecaa_generators["lifetime"] > 0].copy()

    gen_attributes = _ECAA_GENERATOR_ATTRIBUTES.copy()
    # Decide which column to rename to be the bus column.
    if regional_granularity == "sub_regions":
        bus_column = "sub_region_id"
    elif regional_granularity == "nem_regions":
        bus_column = "region_id"
    elif regional_granularity == "single_region":
        # No existing column to use for bus, so create a new one.
        ecaa_generators["bus"] = "NEM"
        bus_column = "bus"  # Name doesn't need to change.
    gen_attributes[bus_column] = "bus"

    if rez_handling == "discrete_nodes":
        # make sure generators are still connected to the REZ bus where applicable
        rez_mask = ~ecaa_generators["rez_id"].isna()
        ecaa_generators.loc[rez_mask, bus_column] = ecaa_generators.loc[
            rez_mask, "rez_id"
        ]

    ecaa_generators["commissioning_date"] = ecaa_generators["commissioning_date"].apply(
        _get_commissioning_or_build_year_as_int,
        default_build_year=investment_periods[0],
        year_type=year_type,
    )
    # Add marginal_cost col with a string mapping to the name of parquet file
    ecaa_generators["marginal_cost"] = ecaa_generators["generator"].apply(
        lambda gen_name: _snakecase_string(re.sub(r"[/\\]", " ", gen_name))
    )

    ecaa_generators = ecaa_generators
    # p_min_pu -> convert minimum_load_mw values to be per unit
    ecaa_generators["minimum_load_mw"] /= ecaa_generators["maximum_capacity_mw"]
    ecaa_generators["p_nom_extendable"] = False
    ecaa_generators["capital_cost"] = 0.0

    # filter and rename columns according to PyPSA input names:
    ecaa_generators_pypsa_format = ecaa_generators.loc[:, gen_attributes.keys()].rename(
        columns=gen_attributes
    )

    columns_in_order = [
        col
        for col in _GENERATOR_ATTRIBUTE_ORDER
        if col in ecaa_generators_pypsa_format.columns
    ]

    return ecaa_generators_pypsa_format[columns_in_order]


def _translate_new_entrant_generators(
    ispypsa_tables: dict[str, pd.DataFrame],
    investment_periods: list[int],
    wacc: float,
    regional_granularity: str = "sub_regions",
    rez_handling: str = "discrete_nodes",
) -> pd.DataFrame:
    """Process data on new entrant generators into a format aligned with `PyPSA` inputs.

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
        `pd.DataFrame`: `PyPSA` style new entrant generator attributes in tabular format.
    """

    new_entrant_generators = ispypsa_tables["new_entrant_generators"]
    if new_entrant_generators.empty:
        # TODO: log
        # raise error?
        return pd.DataFrame()

    gen_attributes = _NEW_ENTRANT_GENERATOR_ATTRIBUTES.copy()
    # Decide which column to rename to be the bus column.
    if regional_granularity == "sub_regions":
        bus_column = "sub_region_id"
    elif regional_granularity == "nem_regions":
        bus_column = "region_id"
    elif regional_granularity == "single_region":
        # No existing column to use for bus, so create a new one.
        new_entrant_generators["bus"] = "NEM"
        bus_column = "bus"  # Name doesn't need to change.
    gen_attributes[bus_column] = "bus"

    if rez_handling == "discrete_nodes":
        # make sure generators are still connected to the REZ bus where applicable
        rez_mask = ~new_entrant_generators["rez_id"].isna()
        new_entrant_generators.loc[rez_mask, bus_column] = new_entrant_generators.loc[
            rez_mask, "rez_id"
        ]

    # drop non-rez VRE generators for now while they're not yet set up fully:
    new_entrant_generators = new_entrant_generators[
        ~new_entrant_generators["rez_id"].isin(["N0", "V0"])
    ].copy()

    # create a row for each new entrant gen in each possible build year (investment period)
    new_entrant_generators_all_build_years = _add_investment_periods_as_build_years(
        new_entrant_generators, investment_periods
    )
    gen_df_with_build_costs = _add_new_entrant_generator_build_costs(
        new_entrant_generators_all_build_years,
        ispypsa_tables["new_entrant_build_costs"],
    )
    gen_df_with_connection_costs = _add_new_entrant_generator_connection_costs(
        gen_df_with_build_costs,
        ispypsa_tables.get("new_entrant_wind_and_solar_connection_costs"),
        ispypsa_tables.get("new_entrant_non_vre_connection_costs"),
    )
    gen_df_with_capital_costs = _calculate_annuitised_new_entrant_gen_capital_costs(
        gen_df_with_connection_costs,
        wacc,
    )
    # nan capex -> build limit of 0.0MW (no build of this generator in this region allowed)
    gen_df_with_capital_costs = gen_df_with_capital_costs[
        ~gen_df_with_capital_costs["capital_cost"].isna()
    ].copy()

    # Add marginal_cost col with a string mapping to the name of parquet file
    gen_df_with_capital_costs["marginal_cost"] = gen_df_with_capital_costs[
        "generator"
    ].copy()

    # then add build_year to generator name to maintain unique generator ID column
    gen_df_with_capital_costs["generator"] = (
        gen_df_with_capital_costs["generator"]
        + "_"
        + gen_df_with_capital_costs["build_year"].astype(str)
    )

    # add a p_nom column set to 0.0 for all new entrants:
    gen_df_with_capital_costs["p_nom"] = 0.0

    # Convert p_min_pu from percentage to float between 0-1:
    gen_df_with_capital_costs["minimum_stable_level_%"] /= 100.0
    gen_df_with_capital_costs["p_nom_extendable"] = True

    # filter and rename columns to PyPSA format
    new_entrant_generators_pypsa_format = gen_df_with_capital_costs.loc[
        :, gen_attributes.keys()
    ].rename(columns=gen_attributes)

    # fill NaNs with PyPSA default values for p_nom_max and p_nom_mod
    pypsa_default_p_noms = {"p_nom_max": np.inf, "p_nom_mod": 0.0}
    new_entrant_generators_pypsa_format = new_entrant_generators_pypsa_format.fillna(
        pypsa_default_p_noms
    )

    columns_in_order = [
        col
        for col in _GENERATOR_ATTRIBUTE_ORDER
        if col in new_entrant_generators_pypsa_format.columns
    ]

    return new_entrant_generators_pypsa_format[columns_in_order]


def _add_new_entrant_generator_build_costs(
    new_entrant_generators_table: pd.DataFrame,
    new_entrant_build_costs: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge build costs into new_entrant_generators_table.

    Args:
        new_entrant_generators_table: dataframe containing `ISPyPSA` formatted
            new-entrant generator detail, with a row for each generator in every possible
            build year. Must have column "build_year" with integer values.
        new_entrant_build_costs: `ISPyPSA` formatted dataframe with build costs in $/MW for
            each new-entrant generator type and build year.

    Returns:
        pd.DataFrame: new_entrant_generators_table with build costs merged in as new column
            called "build_cost_$/mw".

    Raises:
        ValueError: if new_entrant_generators_table does not have column "build_year" or
            if any new entrant generators have build costs missing/undefined.

    Notes:
        1. The function assumes that new_entrant_build_costs has a "technology" column
           that matches with the "generator_name" column in new_entrant_generators_table.
    """

    if "build_year" not in new_entrant_generators_table.columns:
        raise ValueError(
            "new_entrant_generators_table must have column 'build_year' to merge in build costs."
        )

    build_costs = new_entrant_build_costs.melt(
        id_vars=["technology"], var_name="build_year", value_name="build_cost_$/mw"
    ).rename(columns={"technology": "generator_name"})
    # get the financial year int from build_year string:
    build_costs["build_year"] = build_costs["build_year"].apply(
        _get_financial_year_int_from_string,
        args=("new entrant generator build costs", "fy"),
    )
    # make sure new_entrant_generators_table has build_year column with ints:
    new_entrant_generators_table["build_year"] = new_entrant_generators_table[
        "build_year"
    ].astype("int64")

    # return generator table with build costs merged in
    new_entrants_with_build_costs = new_entrant_generators_table.merge(
        build_costs, how="left"
    )

    # check for empty/undefined build costs:
    undefined_build_cost_gens = (
        new_entrants_with_build_costs[
            new_entrants_with_build_costs["build_cost_$/mw"].isna()
        ]["generator_name"]
        .unique()
        .tolist()
    )
    if undefined_build_cost_gens:
        raise ValueError(
            f"Undefined build costs for new entrant generators: {undefined_build_cost_gens}"
        )

    return new_entrants_with_build_costs


def _add_new_entrant_generator_connection_costs(
    new_entrant_generators_table: pd.DataFrame,
    new_entrant_wind_and_solar_connection_costs: pd.DataFrame | None,
    new_entrant_non_vre_connection_costs: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Merge VRE and non-VRE connection costs into new_entrant_generators_table.

    Args:
        new_entrant_generators_table: dataframe containing `ISPyPSA` formatted
            new-entrant generator detail, with a row for each generator in every possible
            build year. Must have columns "connection_cost_rez/_region_id" and "fuel_type".
        new_entrant_wind_and_solar_connection_costs: `ISPyPSA` formatted dataframe
            containing connection cost details (including system strength costs) in $/MW for
            new VRE (wind and solar) generators in each REZ by financial year.
        new_entrant_non_vre_connection_costs: `ISPyPSA` formatted dataframe with
            connection costs  in $/MW for non-VRE new entrant generators in each NEM region.

    Returns:
        pd.DataFrame: new_entrant_generators_table with connection costs in $/MW merged in
            as new column named "connection_cost_$/mw".
    """
    new_entrant_generators_table["connection_cost_$/mw"] = new_entrant_generators_table[
        "connection_cost_rez/_region_id"
    ]

    def _add_build_year_or_technology_string(row):
        if row["fuel_type"] in ["Wind", "Solar"]:
            return f"{row['connection_cost_$/mw']}_{row['build_year']}"
        else:
            snake_case_technology = _snakecase_string(row["connection_cost_technology"])
            return f"{row['connection_cost_$/mw']}_{snake_case_technology}"

    new_entrant_generators_table["connection_cost_$/mw"] = (
        new_entrant_generators_table.apply(_add_build_year_or_technology_string, axis=1)
    )
    # VRE
    vre_connection_cost_dict = {}
    if new_entrant_wind_and_solar_connection_costs is not None:
        vre_connection_cost_dict = _get_vre_connection_costs_dict(
            new_entrant_wind_and_solar_connection_costs
        )
    # NON-VRE
    non_vre_connection_cost_dict = {}
    if new_entrant_non_vre_connection_costs is not None:
        non_vre_connection_cost_dict = _get_non_vre_connection_costs_dict(
            new_entrant_non_vre_connection_costs
        )

    # COMBINE & FILL
    connection_cost_dict = vre_connection_cost_dict | non_vre_connection_cost_dict
    new_entrant_generators_table["connection_cost_$/mw"] = new_entrant_generators_table[
        "connection_cost_$/mw"
    ].replace(connection_cost_dict)

    new_entrant_generators_table = _set_offshore_wind_connection_costs_to_zero(
        new_entrant_generators_table
    )

    missing_connection_costs = new_entrant_generators_table["connection_cost_$/mw"].map(
        lambda x: not isinstance(x, (float, int))
    )
    gens_missing_connection_costs = new_entrant_generators_table.loc[
        missing_connection_costs
    ]
    if not gens_missing_connection_costs.empty:
        raise ValueError(
            f"Missing connection costs for the following generators: {gens_missing_connection_costs['generator'].unique()}"
        )

    return new_entrant_generators_table


def _get_vre_connection_costs_dict(
    new_entrant_wind_and_solar_connection_costs: pd.DataFrame,
) -> dict[str, float]:
    """
    Creates a dictionary mapping REZ name and generator build year to connection costs for
    new entrant VRE.

    VRE connection costs include both connection costs by REZ and system strength costs
    where applicable.

    Args:
        new_entrant_wind_and_solar_connection_costs: `ISPyPSA` formatted dataframe
            containing connection cost details (including system strength costs) in $/MW for
            new VRE (wind and solar) generators in each REZ by financial year.

    Returns:
        new_vre_connection_costs_mapping: dictionary mapping REZ name and generator build
            year to connection costs in $/MW. The keys are strings of the form "{rez}_{build_year}".

    Notes:
        1. The function assumes that new_entrant_wind_and_solar_connection_costs has a "REZ names"
           column that matches with the "connection_cost_rez/_region_id" column in
           new_entrant_generators_table.
    """
    # only keep "REZ names" and columns that contain cost values ("$" in col name)
    new_vre_connection_costs = new_entrant_wind_and_solar_connection_costs[
        ["REZ names"]
        + [
            col
            for col in new_entrant_wind_and_solar_connection_costs.columns
            if "$" in col
        ]
    ]
    new_vre_connection_costs_long = new_vre_connection_costs.melt(
        id_vars=["REZ names", "system_strength_connection_cost_$/mw"],
        var_name="build_year",
        value_name="connection_cost_$/mw",
    ).fillna(0.0)
    # set build_year to int to merge with new_entrant_generators_table:
    new_vre_connection_costs_long["build_year"] = new_vre_connection_costs_long[
        "build_year"
    ].apply(
        _get_financial_year_int_from_string,
        args=("new entrant VRE generator connection costs", "fy"),
    )
    # sum the connection costs and system strength connection costs for each year:
    new_vre_connection_costs_long["connection_cost_$/mw"] = (
        new_vre_connection_costs_long[
            ["connection_cost_$/mw", "system_strength_connection_cost_$/mw"]
        ].sum(axis=1)
    )
    # create mapping col and dict to merge with new_entrant_generators_table:
    new_vre_connection_costs_long["rez_build_year_mapping"] = (
        new_vre_connection_costs_long["REZ names"]
        + "_"
        + new_vre_connection_costs_long["build_year"].astype(str)
    )
    new_vre_connection_costs_mapping = (
        new_vre_connection_costs_long[
            ["rez_build_year_mapping", "connection_cost_$/mw"]
        ]
        .set_index("rez_build_year_mapping")
        .squeeze()
        .to_dict()
    )
    return new_vre_connection_costs_mapping


def _get_non_vre_connection_costs_dict(
    new_entrant_non_vre_connection_costs: pd.DataFrame,
) -> pd.DataFrame:
    """
    Creates a dictionary mapping REZ name and generator technology to connection costs
    for new entrant non-VRE generators.

    Args:
        new_entrant_non_vre_connection_costs: `ISPyPSA` formatted dataframe with
            connection costs in $/MW for non-VRE new entrant generators in each NEM region.

    Returns:
        non_vre_connection_costs_mapping: dictionary mapping REZ name and technology type
            to connection costs in $/MW. The keys are strings of the form "{rez}_{connection_cost_technology}".
    """
    non_vre_connection_costs = new_entrant_non_vre_connection_costs.melt(
        id_vars=["Region"],
        var_name="connection_cost_technology",
        value_name="connection_cost_$/mw",
    ).fillna(0.0)
    # remove units "_$/mw" from the "connection_cost_technology" values so they
    # line up with "connection_cost_technology" values in new_entrant_generators_table
    non_vre_connection_costs = non_vre_connection_costs.replace(
        to_replace=r"_\$\/mw", value="", regex=True
    )
    non_vre_connection_costs["region_technology_mapping"] = (
        non_vre_connection_costs["Region"]
        + "_"
        + non_vre_connection_costs["connection_cost_technology"]
    )
    non_vre_connection_costs_mapping = (
        non_vre_connection_costs[["region_technology_mapping", "connection_cost_$/mw"]]
        .set_index("region_technology_mapping")
        .squeeze()
        .to_dict()
    )
    return non_vre_connection_costs_mapping


def _set_offshore_wind_connection_costs_to_zero(
    new_entrant_generators_table: pd.DataFrame,
) -> pd.DataFrame:
    """
    Set the connection costs of offshore wind generators to zero.

    The function sets the "connection_cost_$/mw" column to zero for all rows in
    new_entrant_generators_table where the generator is named "Wind - offshore (fixed)"
    or "Wind - offshore (floating)". This is based on v6.0 of the IASR workbook,
    in which connection costs for generators in new offshore REZs are included in
    the REZ expansion costs and generator build costs already.

    Args:
        new_entrant_generators_table (pd.DataFrame): dataframe containing `ISPyPSA` formatted
            new-entrant generator detail, with a row for each generator in every possible
            build year. Must have columns "generator_name" and "connection_cost_$/mw".

    Returns:
        pd.DataFrame: dataframe containing `ISPyPSA` formatted new-entrant generator
            detail with offshore wind connection costs set to $0.0/MW.
    """

    offshore_wind_generator_names = [
        "Wind - offshore (fixed)",
        "Wind - offshore (floating)",
    ]

    new_entrant_generators_table.loc[
        new_entrant_generators_table["generator_name"].isin(
            offshore_wind_generator_names
        ),
        "connection_cost_$/mw",
    ] = 0.0

    return new_entrant_generators_table


def _calculate_annuitised_new_entrant_gen_capital_costs(
    new_entrant_generators_table: pd.DataFrame,
    wacc: float,
) -> pd.DataFrame:
    """Calculates annuitised capital cost of each new entrant generator in each possible
    build year.

    Args:
        new_entrant_generators_table: dataframe containing `ISPyPSA` formatted
            new-entrant generator detail, with a row for each generator in every possible
            build year and region/REZ.
        wacc: as float, weighted average cost of capital, an interest rate specifying
            how expensive it is to borrow money for the asset investment.

    Returns:
        new_entrant_generators_table: `ISPyPSA` formatted dataframe with new column
            "capital_cost" containing the annuitised capital cost of each new entrant
            generator in each possible build year.
    """
    new_entrant_generators_table["capital_cost"] = (
        new_entrant_generators_table["build_cost_$/mw"]
        * (new_entrant_generators_table["technology_specific_lcf_%"] / 100)
        + new_entrant_generators_table["connection_cost_$/mw"]
    )
    # annuitise:
    new_entrant_generators_table["capital_cost"] = new_entrant_generators_table.apply(
        lambda x: _annuitised_investment_costs(x["capital_cost"], wacc, x["lifetime"]),
        axis=1,
    )
    # add annual fixed opex (first converting to $/MW/annum)
    new_entrant_generators_table["capital_cost"] += (
        new_entrant_generators_table["fom_$/kw/annum"] * 1000
    )
    return new_entrant_generators_table


def create_pypsa_friendly_dynamic_marginal_costs(
    ispypsa_tables: dict[str, pd.DataFrame],
    generators: pd.DataFrame,
    snapshots: pd.DataFrame,
    pypsa_inputs_path: Path | str,
) -> None:
    """
    Args:
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).
        generators: `PyPSA` formatted pd.DataFrame containing details of generators
            to be added to the PyPSA network.
        snapshots: `PyPSA` formatted pd.DataFrame containing the expected time series values.
        pypsa_inputs_path: Path to directory where input translated to `PyPSA` format will
            be saved.

    Returns:
        None
    """

    output_dir = Path(pypsa_inputs_path, "marginal_cost_timeseries")
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    # If a non-string marginal_cost has been given then it is assumed to be a constant value across all snapshots
    # so doesn't need a dynamic marginal cost time series
    # first check if any marginal costs are undefined/NaN and raise an error if any:
    nan_marginal_cost_generators = generators[generators["marginal_cost"].isna()]
    if not nan_marginal_cost_generators.empty:
        raise ValueError(
            f"Undefined marginal cost for generator(s): {set(nan_marginal_cost_generators['name'].values)}"
        )

    where_marginal_cost_is_string = generators["marginal_cost"].apply(
        lambda x: isinstance(x, str)
    )
    time_varying_marginal_cost_generators = generators[where_marginal_cost_is_string]
    if time_varying_marginal_cost_generators.empty:
        # TODO: log this - but isn't an error
        return

    fuel_prices = _get_dynamic_fuel_prices(
        ispypsa_tables, time_varying_marginal_cost_generators, snapshots
    )
    fuel_prices = fuel_prices.set_index(["carrier", "isp_fuel_cost_mapping"])

    unique_marginal_cost_generators = (
        time_varying_marginal_cost_generators.copy()
        .drop_duplicates(subset=["marginal_cost"], keep="first")
        .set_index("marginal_cost")
    )
    for name, row in unique_marginal_cost_generators.iterrows():
        gen_fuel_prices = (
            fuel_prices.loc[(row["carrier"], row["isp_fuel_cost_mapping"]), :].fillna(
                0.0
            )
            # .squeeze()
        )
        marginal_costs_one_gen = _calculate_dynamic_marginal_costs_single_generator(
            row, gen_fuel_prices, snapshots
        )
        marginal_costs_one_gen.to_parquet(
            Path(output_dir, f"{name}.parquet"), index=False
        )


def _calculate_dynamic_marginal_costs_single_generator(
    generator_row: pd.Series,
    gen_fuel_prices: pd.Series,
    snapshots: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculates dynamic marginal costs for a single generator over all snapshots.

    Creates a pd.Series of marginal costs for the given generator from fuel prices,
    heat rate and VOM.

    Args:
        generator_row: pd.Series detailing the generator attributes 'isp_heat_rate_gj/mwh'
            and 'isp_vom_$/mwh_sent_out'.
        gen_fuel_prices: pd.Series detailing the fuel prices applicable to the generator,
            with index values formatted as `YYYY_YY_$/gj` (FY) and given in $/GJ.
        snapshots: `PyPSA` formatted dataframe containing all snapshots for the model.

    Returns:
        pd.DataFrame with dynamic marginal costs in $/MWh for each model snapshot for
            one given generator.
    """
    # check that gen_fuel_prices is a series or DataFrame (and squeeze):
    if isinstance(gen_fuel_prices, pd.DataFrame):
        gen_fuel_prices = gen_fuel_prices.squeeze()

    if not isinstance(gen_fuel_prices, pd.Series):
        raise TypeError(
            f"Expected gen_fuel_prices to be a series, got {type(gen_fuel_prices)}"
        )

    # dynamic_marginal_cost calculation = fuel_price * heat_rate + VOM
    dynamic_marginal_costs = (
        gen_fuel_prices * generator_row["isp_heat_rate_gj/mwh"]
    ) + generator_row["isp_vom_$/mwh_sent_out"]

    dynamic_marginal_costs.name = "marginal_cost"
    dynamic_marginal_costs = dynamic_marginal_costs.to_frame()

    # add a column containing the start date of each financial year present:
    def _set_fy_start_date(fy_cost_string: str) -> pd.Timestamp:
        financial_year_int = _get_financial_year_int_from_string(
            fy_cost_string, "generator marginal costs", "fy"
        )
        july_1st_date = pd.to_datetime(f"{financial_year_int - 1}-07-01")
        return july_1st_date

    dynamic_marginal_costs["start_date"] = dynamic_marginal_costs.index
    dynamic_marginal_costs["start_date"] = dynamic_marginal_costs["start_date"].apply(
        _set_fy_start_date
    )
    # Now align with snapshots and investment periods:
    marginal_cost_timeseries = pd.merge_asof(
        snapshots,
        dynamic_marginal_costs,
        left_on="snapshots",
        right_on="start_date",
    )
    return marginal_cost_timeseries.drop(columns=["start_date"])


def _get_dynamic_fuel_prices(
    ispypsa_tables: dict[str, pd.DataFrame],
    generators_df: pd.DataFrame,
    snapshots: pd.DataFrame,
) -> pd.DataFrame:
    """Gets all dynamic fuel prices as dataframes including gas, liquid fuel, hyblend, coal,
    biomass and hydrogen.

    Gas and hyblend prices are calculated considering dynamic fuel blending as described
    in the IASR and workbook. All fuel cost values are given in $/GJ, and wind, solar and
    water fuel prices are set to $0.0/GJ.

    Args:
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).
        generators_df: dataframe containing combined ecaa and new entrant generator
            data where each row contains at minimum the generator name and carrier
            (fuel type).
        snapshots: `PyPSA` formatted dataframe containing all snapshots for the model.

    Returns:
        `pd.DataFrame` : dataframe containing fuel prices for each unique carrier
            and fuel_cost_mapping pair in generators_df.
    """

    unique_carriers = generators_df["carrier"].unique()
    all_dynamic_fuel_prices = []
    for carrier in unique_carriers:
        if carrier in _CARRIER_TO_FUEL_COST_TABLES.keys():
            carrier_prices_table = _get_single_carrier_fuel_prices(
                carrier, generators_df, ispypsa_tables
            )
            all_dynamic_fuel_prices.append(carrier_prices_table)

    non_fuel_carriers = ["Wind", "Water", "Solar"]
    non_fuel_prices_df = pd.DataFrame(
        generators_df.loc[
            generators_df["carrier"].isin(non_fuel_carriers),
            ["isp_fuel_cost_mapping", "carrier"],
        ]
    ).drop_duplicates()

    if all_dynamic_fuel_prices:
        all_dynamic_fuel_prices.append(non_fuel_prices_df)
        dynamic_fuel_prices = pd.concat(
            all_dynamic_fuel_prices, axis=0, ignore_index=True
        )
    else:
        # add financial year strings as columns for non-fuel carriers for each year present in snapshots:
        snapshot_years = snapshots["snapshots"].dt.year.unique()
        dynamic_fuel_prices = non_fuel_prices_df.assign(
            **{f"{year}_{str(year + 1)[-2:]}_$/mw": np.nan for year in snapshot_years}
        )

    # Set fuel prices for "free" carriers (wind, solar, water) to 0.0 for all years
    dynamic_fuel_prices.loc[
        dynamic_fuel_prices["carrier"].isin(non_fuel_carriers),
        [col for col in dynamic_fuel_prices.columns if "$" in col],
    ] = 0.0

    return pd.DataFrame(dynamic_fuel_prices)


def _get_single_carrier_fuel_prices(
    carrier: str,
    generators_df: pd.DataFrame,
    ispypsa_tables: dict[str, pd.DataFrame],
):
    """Gets fuel prices for a given carrier, calculating blended prices where necessary,
    for each available financial year.

    Args:
        carrier: string name of the carrier (fuel type) to get/calculate fuel prices for.
        generators_df: dataframe containing combined ecaa and new entrant generator
            data where each row contains at minimum the generator name and carrier
            (fuel type).
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).

    Returns:
        `pd.DataFrame`: fetched or calculated fuel prices in tabular format
    """

    table_mapping = _CARRIER_TO_FUEL_COST_TABLES[carrier]
    base_prices_table = ispypsa_tables[table_mapping["base_table"]]

    # set the index of base_prices_table to the column containing the equivalent
    # fuel cost mapping strings and set the name of the index
    if "fuel_cost_mapping_col" in table_mapping.keys():
        base_prices_table = base_prices_table.set_index(
            table_mapping["fuel_cost_mapping_col"]
        )
        base_prices_table.index.name = "isp_fuel_cost_mapping"
    else:
        base_prices_table["isp_fuel_cost_mapping"] = carrier
        base_prices_table = base_prices_table.set_index("isp_fuel_cost_mapping")

    # Calculate prices for fuels that are blended with low-emissions alternatives
    # over time (in v6.0, hyblend, gas)
    if "blend_table" in table_mapping.keys():
        generator_to_fuel_cost_mapping_dict = None
        if carrier == "Hyblend":
            generator_to_fuel_cost_mapping_dict = (
                generators_df.loc[
                    generators_df["carrier"] == "Hyblend",
                    ["name", "isp_fuel_cost_mapping"],
                ]
                .set_index("name")
                .squeeze("columns")
                .to_dict()
            )
            base_prices_table = base_prices_table.loc[
                generator_to_fuel_cost_mapping_dict.values(), :
            ].copy()

        base_prices_table = _calculate_blended_fuel_prices(
            base_prices_table,
            ispypsa_tables[table_mapping["blend_table"]],
            table_mapping["fuel_cost_mapping_col"],
            ispypsa_tables[table_mapping["blend_percent_table"]],
            generator_to_fuel_cost_mapping=generator_to_fuel_cost_mapping_dict,
        )

    # filter here to only return useful fuel price mappings for this carrier:
    base_prices_table = base_prices_table.loc[
        base_prices_table.index.isin(
            generators_df.loc[
                generators_df["carrier"] == carrier, "isp_fuel_cost_mapping"
            ]
        )
    ]
    # add a column with the carrier name:
    base_prices_table["carrier"] = carrier
    return base_prices_table.reset_index()


def _calculate_blended_fuel_prices(
    base_prices: pd.DataFrame,
    blend_prices: pd.DataFrame,
    fuel_cost_mapping_col: str,
    base_percentages: pd.DataFrame,
    generator_to_fuel_cost_mapping: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    Calculate prices for fuels that are blended with low-emissions alternatives
    over time (in v6.0, hyblend, gas) in $/GJ.

    The formula for calculating blended prices is:
        `blended_price = base_price * base_percentage + blend_price * (1 - base_percentage)`

    Args:
        base_prices: DataFrame containing base fuel prices in $/GJ for each FY,
            indexed by fuel cost mapping.
        blend_prices: DataFrame containing blend fuel prices in $/GJ for each FY.
            Expects only one row corresponding to the ISP scenario set in config.
        fuel_cost_mapping_col (str): name of column containing fuel cost mappings in
            base_prices. These can either be generator names (ECAA generators) or
            general fuel cost mappings (new entrant generators).
        base_percentages: DataFrame containing percentage of the base fuel in the blend
            over time. The df should be indexed by fuel_cost_mapping_col if it contains
            more than one row. Given as percentage values (0-100) for each FY.
        generator_to_fuel_cost_mapping: optional dictionary mapping generator name
            to fuel cost mapping, used to align the index of base_percentages
            with the index of base_prices for Hyblend carrier generators (v6.0).
            Structure should follow {generator_name: fuel_cost_mapping}.
            Defaults to None.

    Returns:
        pd.DataFrame: DataFrame containing blended fuel prices in $/GJ for each FY,
            indexed by fuel cost mapping.
    """

    years_in_base_prices = list(base_prices.columns)
    # rename percentage columns to match price columns:
    base_percentages = base_percentages.rename(
        columns={col: col.replace("%", "$/gj") for col in base_percentages.columns}
    )
    # Then check input data structures
    set_of_carriers_or_mappings = set(base_prices.index)
    base_percentages_is_df = False
    if fuel_cost_mapping_col in base_percentages.columns:
        if generator_to_fuel_cost_mapping:
            # align the index with fuel cost mappings in base_prices if they're given
            base_percentages[fuel_cost_mapping_col] = base_percentages[
                fuel_cost_mapping_col
            ].replace(generator_to_fuel_cost_mapping)
        base_percentages = base_percentages.set_index(fuel_cost_mapping_col)
        base_percentages_is_df = True
        # if there are any missing carriers_or_mappings from this index, raise an error
        # (can't calculate fuel prices for these carriers/mappings)
        missing_carriers_or_mappings = set_of_carriers_or_mappings - set(
            base_percentages.index
        )
        if missing_carriers_or_mappings:
            raise ValueError(
                f"base_percentages missing values for: ({missing_carriers_or_mappings})"
            )
    elif isinstance(base_percentages.squeeze(axis=0), pd.Series):
        # Squeeze into a series only keeping the FY columns needed (matching base_prices columns)
        percentage_series = base_percentages[years_in_base_prices].squeeze(axis=0)

    else:
        raise ValueError(
            f"base_percentages must have column '{fuel_cost_mapping_col}' if more than one row present."
        )

    # make sure blend_prices is a series before calculating:
    if isinstance(blend_prices, pd.DataFrame):
        if len(blend_prices) > 1:
            raise ValueError(
                "Expected blend_prices for a single scenario (row), received multiple."
            )
        blend_prices = blend_prices[years_in_base_prices].squeeze(axis=0)

    blended_fuel_prices = []
    for fuel_cost_mapping, mapping_base_price in base_prices.iterrows():
        if base_percentages_is_df:
            percentage_series = base_percentages.loc[fuel_cost_mapping, :].squeeze(
                axis=0
            )
        # convert percentage_series to decimal from %:
        percentage_series_as_decimal = percentage_series / 100

        mapping_blended_fuel_price = (
            mapping_base_price * percentage_series_as_decimal
            + (1 - percentage_series_as_decimal) * blend_prices
        )
        mapping_blended_fuel_price.name = fuel_cost_mapping
        blended_fuel_prices.append(mapping_blended_fuel_price)

    blended_fuel_prices_df = pd.DataFrame(blended_fuel_prices)
    blended_fuel_prices_df.index.name = "isp_fuel_cost_mapping"

    return blended_fuel_prices_df


def _create_unserved_energy_generators(
    buses: pd.DataFrame, cost: float, max_per_node: float
) -> pd.DataFrame:
    """Create unserved energy generators for each bus in the network.

    These generators allow the model to opt for unserved energy at a very high cost
    when other options are exhausted or infeasible, preventing model infeasibility.

    Args:
        buses: DataFrame containing bus information with a 'name' column
        cost: Marginal cost of unserved energy ($/MWh)
        max_per_node: Size of unserved energy generators (MW)

    Returns:
        DataFrame containing unserved energy generators in PyPSA format
    """

    generators = pd.DataFrame(
        {
            "name": "unserved_energy_" + buses["name"],
            "carrier": "Unserved Energy",
            "bus": buses["name"],
            "p_nom": max_per_node,
            "p_nom_extendable": False,
            "p_min_pu": 0.0,
            "build_year": 0,
            "lifetime": np.inf,
            "capital_cost": 0.0,
            "marginal_cost": cost,
            # add below columns in case time-varying marginal cost desired for USE
            "isp_fuel_cost_mapping": "Unserved Energy",
            "isp_technology_type": "Unserved Energy",
            "isp_vom_$/mwh_sent_out": 0.0,
            "isp_heat_rate_gj/mwh": 0.0,
        }
    )

    return generators


def create_pypsa_friendly_ecaa_generator_timeseries(
    ecaa_generators: pd.DataFrame,
    trace_data_path: Path | str,
    generator_types: List[Literal["solar", "wind"]],
    reference_year_mapping: dict[int, int],
    year_type: Literal["fy", "calendar"],
) -> dict[str, dict[str, pd.DataFrame]]:
    """Gets trace data for generators by constructing a timeseries from the start to end
    year using the reference year cycle provided. Returns a dictionary organized by
    generator type, with generator names as keys in the nested dictionaries.

    Args:
        ecaa_generators: `ISPyPSA` formatted pd.DataFrame detailing the ECAA generators.
        trace_data_path: Path to directory containing trace data parsed by
            isp-trace-parser
        reference_year_mapping: dict[int: int], mapping model years to trace data
            reference years
        generator_types: List[Literal['solar', 'wind']], which types of generator to
            translate trace data for.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial
            year with start_year and end_year specifiying the financial year to return
            data for, using year ending nomenclature (2016 -> FY2015/2016). If
            'calendar', then filtering is by calendar year.

    Returns:
        dict[str, dict[str, pd.DataFrame]]: Dictionary with generator types as keys
            ('solar', 'wind'), each containing a dictionary with generator names as keys
            and trace dataframes as values. Each dataframe contains columns: Datetime, Value
    """

    if ecaa_generators.empty:
        # TODO: add logging
        # raise error? (prob not necessary to error...)
        return

    trace_data_paths = {
        gen_type: trace_data_path / Path(gen_type) for gen_type in generator_types
    }

    generator_types_caps = [gen_type.capitalize() for gen_type in generator_types]

    generators = ecaa_generators[
        ecaa_generators["fuel_type"].isin(generator_types_caps)
    ].copy()

    query_functions = {
        "solar": get_data.solar_project_multiple_reference_years,
        "wind": get_data.wind_project_multiple_reference_years,
    }

    gen_to_type = dict(zip(ecaa_generators["generator"], ecaa_generators["fuel_type"]))

    # Initialize dict with generator types
    generator_traces = {gen_type: {} for gen_type in generator_types}

    for _, gen_row in generators.iterrows():
        gen = gen_row["generator"]
        gen_type = gen_to_type[gen].lower()

        trace = query_functions[gen_type](
            reference_years=reference_year_mapping,
            project=gen,
            directory=trace_data_paths[gen_type],
            year_type=year_type,
        )
        # datetime in nanoseconds required by PyPSA
        trace["Datetime"] = trace["Datetime"].astype("datetime64[ns]")
        generator_traces[gen_type][gen] = trace

        return generator_traces


def create_pypsa_friendly_new_entrant_generator_timeseries(
    new_entrant_generators: pd.DataFrame,
    trace_data_path: Path | str,
    pypsa_inputs_path: Path | str,
    generator_types: List[Literal["solar", "wind"]],
    reference_year_mapping: dict[int, int],
    year_type: Literal["fy", "calendar"],
    snapshots: pd.DataFrame,
) -> None:
    """Gets trace data for generators by constructing a timeseries from the start to end
    year using the reference year cycle provided. Trace data is then saved as a parquet
    file to subdirectories labeled with their generator type.

    Args:
        new_entrant_generators: `ISPyPSA` formatted pd.DataFrame detailing the new
            entrant generators.
        trace_data_path: Path to directory containing trace data parsed by
            isp-trace-parser
        pypsa_inputs_path: Path to directory where input translated to pypsa format will
            be saved
        reference_year_mapping: dict[int: int], mapping model years to trace data
            reference years
        generator_types: List[Literal['solar', 'wind']], which types of generator to
            translate trace data for.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial
            year with start_year and end_year specifiying the financial year to return
            data for, using year ending nomenclature (2016 -> FY2015/2016). If
            'calendar', then filtering is by calendar year.
        snapshots: pd.DataFrame containing the expected time series values.

    Returns:
        None
    """

    if new_entrant_generators.empty:
        # TODO: log
        # raise error? (prob not necessary to error...)
        return

    trace_data_paths = {
        gen_type: trace_data_path / Path(gen_type) for gen_type in generator_types
    }

    output_paths = {
        gen_type: Path(pypsa_inputs_path, f"{gen_type}_traces")
        for gen_type in generator_types
    }

    for output_trace_path in output_paths.values():
        if not output_trace_path.exists():
            output_trace_path.mkdir(parents=True)

    where_gen_type = _where_any_substring_appears(
        new_entrant_generators["fuel_type"], generator_types
    )
    generators = list(new_entrant_generators.loc[where_gen_type, "generator"])
    gen_to_type = dict(
        zip(
            new_entrant_generators["generator"],
            new_entrant_generators["fuel_type"],
        )
    )
    gen_to_isp_resource_type = dict(
        zip(
            new_entrant_generators["generator"],
            new_entrant_generators["isp_resource_type"],
        )
    )
    gen_to_rez_id = dict(
        zip(
            new_entrant_generators["generator"],
            new_entrant_generators["rez_id"],
        )
    )

    query_functions = {
        "solar": get_data.solar_area_multiple_reference_years,
        "wind": get_data.wind_area_multiple_reference_years,
    }

    for gen in generators:
        generator_type = gen_to_type[gen].lower()
        area_abbreviation = gen_to_rez_id[gen]
        technology_or_resource_quality = gen_to_isp_resource_type[gen]
        trace = query_functions[generator_type](
            reference_year_mapping,
            area_abbreviation,
            technology_or_resource_quality,
            directory=trace_data_paths[generator_type],
            year_type=year_type,
        )
        # datetime in nanoseconds required by PyPSA
        trace["Datetime"] = trace["Datetime"].astype("datetime64[ns]")
        trace = trace.rename(columns={"Datetime": "snapshots", "Value": "p_max_pu"})

        trace = _time_series_filter(trace, snapshots)
        _check_time_series(
            trace["snapshots"],
            snapshots["snapshots"],
            "generator trace data",
            str(gen),
        )
        trace = pd.merge(trace, snapshots, on="snapshots")
        trace = trace.loc[:, ["investment_periods", "snapshots", "p_max_pu"]]
        trace.to_parquet(
            Path(output_paths[generator_type], f"{gen}.parquet"), index=False
        )
