import re
from hmac import new
from pathlib import Path
from typing import List, Literal

import numpy as np
import pandas as pd
from isp_trace_parser import get_data

from ispypsa.config import ModelConfig
from ispypsa.model import generators
from ispypsa.templater.helpers import (
    _fuzzy_match_names,
    _snakecase_string,
    _where_any_substring_appears,
)
from ispypsa.translator.helpers import (
    _annuitised_investment_costs,
    _get_commissioning_date_year_as_int,
    _get_financial_year_int_from_string,
)
from ispypsa.translator.mappings import (
    _CARRIER_TO_FUEL_COST_TABLES,
    _ECAA_GENERATOR_ATTRIBUTES,
    _NEW_ENTRANT_GENERATOR_ATTRIBUTES,
)
from ispypsa.translator.temporal_filters import _time_series_filter
from ispypsa.translator.time_series_checker import _check_time_series


def _translate_ecaa_generators(
    ispypsa_tables: dict[str, pd.DataFrame],
    investment_periods: list[int],
    regional_granularity: str = "sub_regions",
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

    Returns:
        `pd.DataFrame`: `PyPSA` style ECAA generator attributes in tabular format.
    """

    ecaa_generators = ispypsa_tables["ecaa_generators"]
    # calculate lifetime based on expected closure_year - build_year:
    ecaa_generators = _add_closure_year_column(
        ecaa_generators, ispypsa_tables["closure_years"], investment_periods
    )
    ecaa_generators["lifetime"] = (
        ecaa_generators["closure_year"] - investment_periods[0]
    ).astype(float)
    ecaa_generators = ecaa_generators[ecaa_generators["lifetime"] > 0].copy()

    gen_attributes = _ECAA_GENERATOR_ATTRIBUTES.copy()
    if regional_granularity == "sub_regions":
        gen_attributes["sub_region_id"] = "bus"
    elif regional_granularity == "nem_regions":
        gen_attributes["region_id"] = "bus"

    if regional_granularity == "single_region":
        ecaa_generators["bus"] = "NEM"
        gen_attributes["bus"] = "bus"

    ecaa_generators["commissioning_date"] = ecaa_generators["commissioning_date"].apply(
        _get_commissioning_date_year_as_int, args=(year_type,)
    )
    # Add marginal_cost col with a string mapping to the name of parquet file
    ecaa_generators["marginal_cost"] = ecaa_generators["generator"].apply(
        lambda gen_name: _snakecase_string(re.sub(r"[/\\]", " ", gen_name))
    )
    # filter and rename columns according to PyPSA input names:
    ecaa_generators_pypsa_format = ecaa_generators.loc[:, gen_attributes.keys()]
    ecaa_generators_pypsa_format = ecaa_generators_pypsa_format.rename(
        columns=gen_attributes
    )
    # p_min_pu: convert values to be per unit (given in MW)
    ecaa_generators_pypsa_format["p_min_pu"] /= ecaa_generators_pypsa_format["p_nom"]
    ecaa_generators_pypsa_format["p_nom_extendable"] = False
    ecaa_generators_pypsa_format["capital_cost"] = 0.0

    ecaa_column_order = [
        "name",
        "bus",
        "p_nom",
        "p_nom_extendable",
        "p_min_pu",
        "carrier",
        "marginal_cost",
        "build_year",
        "lifetime",
        "capital_cost",
        "isp_technology_type",
        "isp_fuel_cost_mapping",
        "isp_vom_$/mwh_sent_out",
        "isp_heat_rate_gj/mwh",
    ]

    return ecaa_generators_pypsa_format[ecaa_column_order]


def _create_unserved_energy_generators(
    buses: pd.DataFrame, cost: float, generator_size_mw: float
) -> pd.DataFrame:
    """Create unserved energy generators for each bus in the network.

    These generators allow the model to opt for unserved energy at a very high cost
    when other options are exhausted or infeasible, preventing model infeasibility.

    Args:
        buses: DataFrame containing bus information with a 'name' column
        cost: Marginal cost of unserved energy ($/MWh)
        generator_size_mw: Size of unserved energy generators (MW)

    Returns:
        DataFrame containing unserved energy generators in PyPSA format
    """

    generators = pd.DataFrame(
        {
            "name": "unserved_energy_" + buses["name"],
            "carrier": "Unserved Energy",
            "bus": buses["name"],
            "p_nom": generator_size_mw,
            "p_nom_extendable": False,
            "marginal_cost": cost,
        }
    )

    return generators


def _translate_new_entrant_generators(
    ispypsa_tables: dict[str, pd.DataFrame],
    investment_periods: list[int],
    wacc: float,
    regional_granularity: str = "sub_regions",
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

    Returns:
        `pd.DataFrame`: `PyPSA` style new entrant generator attributes in tabular format.
    """

    new_entrant_generators = ispypsa_tables["new_entrant_generators"].copy()
    gen_attributes = _NEW_ENTRANT_GENERATOR_ATTRIBUTES.copy()
    if regional_granularity == "sub_regions":
        gen_attributes["sub_region_id"] = "bus"
    elif regional_granularity == "nem_regions":
        gen_attributes["region_id"] = "bus"

    if regional_granularity == "single_region":
        new_entrant_generators["bus"] = "NEM"
        gen_attributes["bus"] = "bus"

    # create a row for each new entrant gen in each possible build year (investment period)
    new_entrant_generators["build_year"] = "investment_periods"
    new_entrant_generators["build_year"] = new_entrant_generators["build_year"].map(
        {"investment_periods": investment_periods}
    )
    new_entrant_generators_all_build_years = new_entrant_generators.explode(
        "build_year"
    ).reset_index(drop=True)

    gen_df_with_build_costs = _add_new_entrant_generator_build_costs(
        new_entrant_generators_all_build_years,
        ispypsa_tables["new_entrant_build_costs"],
    )
    gen_df_with_connection_costs = _add_new_entrant_generator_connection_costs(
        gen_df_with_build_costs,
        ispypsa_tables["new_entrant_wind_and_solar_connection_costs"],
        ispypsa_tables["new_entrant_non_vre_connection_costs"],
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
    ].apply(lambda gen_name: _snakecase_string(re.sub(r"[/\\]", " ", gen_name)))

    # then add build_year to generator name to maintain unique generator ID column
    gen_df_with_capital_costs["generator"] = (
        gen_df_with_capital_costs["generator"]
        + "_"
        + gen_df_with_capital_costs["build_year"].astype(str)
    )

    # filter and rename columns to PyPSA format
    new_entrant_generators_pypsa_format = gen_df_with_capital_costs.loc[
        :, gen_attributes.keys()
    ].rename(columns=gen_attributes)

    # Convert p_min_pu from percentage to float between 0-1:
    new_entrant_generators_pypsa_format["p_min_pu"] /= 100.0
    new_entrant_generators_pypsa_format["p_nom_extendable"] = True

    # fill NaNs with PyPSA default values for p_nom_max and p_nom_mod
    pypsa_default_p_noms = {"p_nom_max": np.inf, "p_nom_mod": 0.0}
    new_entrant_generators_pypsa_format = new_entrant_generators_pypsa_format.fillna(
        pypsa_default_p_noms
    )

    new_entrant_column_order = [
        "name",
        "bus",
        "p_nom_mod",
        "p_nom_extendable",
        "p_nom_max",
        "p_min_pu",
        "carrier",
        "marginal_cost",
        "build_year",
        "lifetime",
        "capital_cost",
        "isp_name",
        "isp_technology_type",
        "isp_fuel_cost_mapping",
        "isp_vom_$/mwh_sent_out",
        "isp_heat_rate_gj/mwh",
    ]

    return new_entrant_generators_pypsa_format[new_entrant_column_order]


def _add_closure_year_column(
    ecaa_generators: pd.DataFrame,
    closure_years: pd.DataFrame,
    investment_periods: list[int],
) -> pd.DataFrame:
    """Adds a column containing the expected closure year (calendar year) for ECAA generators.

    Note: currently only one generator object is templated and translated per ECAA
    generator, while some generators have multiple units with different expected closure
    years. This function makes the OPINIONATED choice to return the earliest expected
    year given in closure_years table for each set of generating units.

    Args:
        ecaa_generators: `ISPyPSA` formatted pd.DataFrame detailing the ECAA generators.
        closure_years: `ISPyPSA` formatted pd.Dataframe containing expected closure years
            for the ECAA generators, by unit. Given as integers.
        investment_periods: list of years in which investment periods start obtained
            from the model configuration. Used in this function to set the default
            closure year to 100 years beyond the last investment period.

    Returns:
        `pd.DataFrame`: ECAA generator attributes table with additional closure year column.
    """
    # set default to 100 years beyond last investment period (no closure during model run)
    default_closure_year = investment_periods[-1] + 100

    if ecaa_generators is None or ecaa_generators.empty:
        raise ValueError("Can't add closure years to empty ecaa_generators table.")

    if closure_years is None or closure_years.empty:
        # NOTE: log if no closure years are given?
        ecaa_generators["closure_year"] = default_closure_year
        return ecaa_generators

    ecaa_generators["closure_year"] = ecaa_generators["generator"]

    closure_years = (
        closure_years.sort_values("expected_closure_year_calendar_year")
        .drop(columns=["duid"])
        .drop_duplicates(subset=["generator"], keep="first")
        .dropna(subset=["expected_closure_year_calendar_year"])
    )

    closure_years_dict = closure_years.set_index("generator").squeeze(axis=1).to_dict()

    where_str = ecaa_generators["closure_year"].apply(lambda x: isinstance(x, str))
    ecaa_generators.loc[where_str, "closure_year"] = _fuzzy_match_names(
        ecaa_generators.loc[where_str, "closure_year"],
        closure_years_dict.keys(),
        f"adding closure_year column to ecaa_generators table",
        not_match="existing",
        threshold=85,
    )
    # If no closure year is given, set to the final investment_period year + 100
    # to ensure lifetime beyond end of model:
    ecaa_generators["closure_year"] = ecaa_generators["closure_year"].apply(
        lambda closure_year: closure_years_dict.get(closure_year, default_closure_year)
    )
    return ecaa_generators


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

    Notes:
        1. The function assumes that new_entrant_build_costs has a "technology" column
           that matches with the "generator_name" column in new_entrant_generators_table.
    """

    if "build_year" not in new_entrant_generators_table.columns:
        raise ValueError(
            "new_entrant_generators_table must have column 'build_year' to merge in build costs."
        )
    # line up "technology_type" names with "generator_name" options
    new_entrant_build_costs.loc[:, "technology"] = _fuzzy_match_names(
        new_entrant_build_costs.loc[:, "technology"],
        new_entrant_generators_table["generator_name"].unique(),
        "adding build_costs for capital_cost calculation for new entrant generators",
        not_match="existing",
        threshold=90,
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
    ].astype(int)

    # return generator table with build costs merged in
    return new_entrant_generators_table.merge(build_costs, how="left")


def _add_new_entrant_generator_connection_costs(
    new_entrant_generators_table: pd.DataFrame,
    new_entrant_wind_and_solar_connection_costs: pd.DataFrame,
    new_entrant_non_vre_connection_costs: pd.DataFrame,
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
    vre_connection_cost_dict = _get_vre_connection_costs_dict(
        new_entrant_wind_and_solar_connection_costs
    )
    # NON-VRE
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
    # set any remaining connection costs to $0.0/MW
    new_entrant_generators_table["connection_cost_$/mw"] = new_entrant_generators_table[
        "connection_cost_$/mw"
    ].apply(lambda x: 0.0 if isinstance(x, str) else x)

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

    fuel_prices = _get_dynamic_fuel_prices(ispypsa_tables, generators)
    fuel_prices = fuel_prices.set_index(["carrier", "isp_fuel_cost_mapping"])

    unique_marginal_cost_generators = (
        generators.copy()
        .drop_duplicates(subset=["marginal_cost"], keep="first")
        .set_index("marginal_cost")
    )
    for name, row in unique_marginal_cost_generators.iterrows():
        gen_fuel_prices = (
            fuel_prices.loc[(row["carrier"], row["isp_fuel_cost_mapping"]), :]
            .fillna(0.0)
            .squeeze()
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
    # check that gen_fuel_prices is a series (only one set of fuel prices can
    # be applied to each generator)
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
    ispypsa_tables: dict[str, pd.DataFrame], generators_df: pd.DataFrame
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

    Returns:
        `pd.DataFrame` : dataframe containing fuel prices for each unique carrier
            and fuel_cost_mapping pair in generators_df.
    """

    unique_carriers = generators_df["carrier"].unique()
    all_dynamic_fuel_prices = []
    for carrier in unique_carriers:
        fuel_price_mappings_for_unique_carrier = generators_df.loc[
            generators_df["carrier"] == carrier, "isp_fuel_cost_mapping"
        ]
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

    all_dynamic_fuel_prices.append(non_fuel_prices_df)
    dynamic_fuel_prices = pd.concat(all_dynamic_fuel_prices, axis=0, ignore_index=True)
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
        # maybe this feels unnecessary to set index here?
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


def create_pypsa_friendly_ecaa_generator_timeseries(
    ecaa_generators: pd.DataFrame,
    trace_data_path: Path | str,
    pypsa_timeseries_inputs_path: Path | str,
    generator_types: List[Literal["solar", "wind"]],
    reference_year_mapping: dict[int, int],
    year_type: Literal["fy", "calendar"],
    snapshots: pd.DataFrame,
) -> None:
    """Gets trace data for generators by constructing a timeseries from the start to end
    year using the reference year cycle provided. Trace data is then saved as a parquet
    file to subdirectories labeled with their generator type.

    Args:
        ecaa_generators: `ISPyPSA` formatted pd.DataFrame detailing the ECAA generators.
        trace_data_path: Path to directory containing trace data parsed by
            isp-trace-parser
        pypsa_timeseries_inputs_path: Path to director where timeseries inputs
            translated to pypsa format will be saved
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

    trace_data_paths = {
        gen_type: trace_data_path / Path(gen_type) for gen_type in generator_types
    }

    output_paths = {
        gen_type: Path(pypsa_timeseries_inputs_path, f"{gen_type}_traces")
        for gen_type in generator_types
    }

    for output_trace_path in output_paths.values():
        if not output_trace_path.exists():
            output_trace_path.mkdir(parents=True)

    where_gen_type = _where_any_substring_appears(
        ecaa_generators["fuel_type"], generator_types
    )
    generators = list(ecaa_generators.loc[where_gen_type, "generator"])

    query_functions = {
        "solar": get_data.solar_project_multiple_reference_years,
        "wind": get_data.wind_project_multiple_reference_years,
    }

    gen_to_type = dict(zip(ecaa_generators["generator"], ecaa_generators["fuel_type"]))

    for gen in generators:
        gen_type = gen_to_type[gen].lower()
        trace = query_functions[gen_type](
            reference_years=reference_year_mapping,
            project=gen,
            directory=trace_data_paths[gen_type],
            year_type=year_type,
        )
        # datetime in nanoseconds required by PyPSA
        trace["Datetime"] = trace["Datetime"].astype("datetime64[ns]")
        trace = trace.rename(columns={"Datetime": "snapshots", "Value": "p_max_pu"})
        trace = _time_series_filter(trace, snapshots)
        _check_time_series(
            trace["snapshots"], snapshots["snapshots"], "generator trace data", gen
        )
        trace = pd.merge(trace, snapshots, on="snapshots")
        trace = trace.loc[:, ["investment_periods", "snapshots", "p_max_pu"]]
        trace.to_parquet(Path(output_paths[gen_type], f"{gen}.parquet"), index=False)


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

    query_functions = {
        "solar": get_data.solar_area_multiple_reference_years,
        "wind": get_data.wind_area_multiple_reference_years,
    }

    for gen in generators:
        generator_type = gen_to_type[gen].lower()
        area_abbreviation = re.search(r"[A-Z]\d+", str(gen))[0]
        technology_or_resource_quality = re.search(r"\d_([A-Z]{2,3})", str(gen)).group(
            1
        )
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
            trace["snapshots"], snapshots["snapshots"], "generator trace data", str(gen)
        )
        trace = pd.merge(trace, snapshots, on="snapshots")
        trace = trace.loc[:, ["investment_periods", "snapshots", "p_max_pu"]]
        trace.to_parquet(
            Path(output_paths[generator_type], f"{gen}.parquet"), index=False
        )
