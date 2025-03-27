import re
from pathlib import Path
from typing import List, Literal

import pandas as pd
from isp_trace_parser import get_data

from ispypsa.config import ModelConfig
from ispypsa.templater.helpers import (
    _fuzzy_match_names,
    _snakecase_string,
    _where_any_substring_appears,
)
from ispypsa.translator.helpers import _annuitised_investment_costs
from ispypsa.translator.mappings import (
    _CARRIER_TO_FUEL_COST_TABLES,
    _ECAA_GENERATOR_ATTRIBUTES,
    _NEW_ENTRANT_GENERATOR_ATTRIBUTES,
)
from ispypsa.translator.temporal_filters import _time_series_filter
from ispypsa.translator.time_series_checker import _check_time_series


def _translate_ecaa_generators(
    ispypsa_tables: dict[str : pd.DataFrame],
    investment_periods: list[int],
    regional_granularity: str = "sub_regions",
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

    Returns:
        `pd.DataFrame`: `PyPSA` style ECAA generator attributes in tabular format.
    """

    ecaa_generators = ispypsa_tables["ecaa_generators"]
    # calculate lifetime based on expected closure_year - build_year:
    ecaa_generators = _add_closure_year_column(
        ecaa_generators, ispypsa_tables["closure_years"]
    )
    # set build_year field to the first investment period (first year modelled)
    ecaa_generators["build_year"] = investment_periods[0]
    ecaa_generators["lifetime"] = (
        ecaa_generators["closure_year"] - ecaa_generators["build_year"]
    )

    gen_attributes = _ECAA_GENERATOR_ATTRIBUTES.copy()
    if regional_granularity == "sub_regions":
        gen_attributes["sub_region_id"] = "bus"
    elif regional_granularity == "nem_regions":
        gen_attributes["region_id"] = "bus"

    ecaa_generators_pypsa_format = ecaa_generators.loc[:, gen_attributes.keys()]
    ecaa_generators_pypsa_format = ecaa_generators_pypsa_format.rename(
        columns=gen_attributes
    )
    if regional_granularity == "single_region":
        ecaa_generators_pypsa_format["bus"] = "NEM"

    # Add marginal_cost col with a string mapping to the name of parquet file
    # containing marginal cost timeseries - replace non-word chars with underscores.
    ecaa_generators_pypsa_format["marginal_cost"] = ecaa_generators_pypsa_format[
        "name"
    ].apply(lambda gen_name: re.sub(r"[^\w]+", r"_", gen_name))

    # p_min_pu: convert values to be per unit (given in MW)
    ecaa_generators_pypsa_format["p_min_pu"] /= ecaa_generators_pypsa_format["p_nom"]
    ecaa_generators_pypsa_format["p_nom_extendable"] = False
    ecaa_generators_pypsa_format["capital_cost"] = 0.0

    # p_max_pu: include summer/winter ratings?
    return ecaa_generators_pypsa_format


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
    ispypsa_tables: dict[str : pd.DataFrame],
    investment_periods: list[int],
    wacc: float,
    regional_granularity: str = "sub_regions",
) -> pd.DataFrame:
    """Process data on new entrant generators into a format aligned with PyPSA inputs.

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

    new_entrant_generators = new_entrant_generators.loc[:, gen_attributes.keys()]
    new_entrant_generators_renamed = new_entrant_generators.rename(
        columns=gen_attributes
    )

    # create a row for each new entrant generator in each possible build year (investment period)
    # NOTE: could be separated into its own function?
    new_entrant_generators_by_build_year = []
    for year in investment_periods:
        new_entrant_generators_year = new_entrant_generators_renamed.copy()
        new_entrant_generators_year["build_year"] = year
        new_entrant_generators_by_build_year.append(new_entrant_generators_year)

    new_entrant_generators_pypsa_format = pd.concat(
        new_entrant_generators_by_build_year,
        axis=0,
        ignore_index=True,
    )

    if regional_granularity == "single_region":
        new_entrant_generators_pypsa_format["bus"] = "NEM"

    # Add marginal_cost col with a string mapping to the name of parquet file
    # containing marginal cost timeseries - replace non-word chars with underscores.
    new_entrant_generators_pypsa_format["marginal_cost"] = (
        new_entrant_generators_pypsa_format["name"].apply(
            lambda gen_name: re.sub(r"[^\w]+", r"_", gen_name)
        )
    )

    # then add build_year to new entrant generator names as well to maintain unique gens:
    new_entrant_generators_pypsa_format["name"] = (
        new_entrant_generators_pypsa_format["name"]
        + "_"
        + new_entrant_generators_pypsa_format["build_year"].astype(str)
    )

    # calculate capital costs for each generator:
    capital_costs = _calculate_annuitised_new_entrant_gen_capital_costs(
        ispypsa_tables, investment_periods, wacc
    )

    new_entrant_generators_pypsa_format["capital_cost"] = (
        new_entrant_generators_pypsa_format["name"].replace(capital_costs)
    )

    return new_entrant_generators_pypsa_format


def create_pypsa_friendly_dynamic_marginal_costs(
    ispypsa_tables: dict[str : pd.DataFrame],
    snapshots: pd.DataFrame,
    pypsa_inputs_path: Path | str,
) -> pd.DataFrame:
    """
    Args:
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).
        snapshots: pd.DataFrame containing the expected time series values.
        pypsa_inputs_path: Path to director where input translated to pypsa format will
            be saved

            # TODO: ADD EXAMPLE
    """

    useful_columns = ["fuel_cost_mapping", "vom_$/mwh_sent_out", "heat_rate_gj/mwh"]
    # first load in all generators and get name, fuel type and fuel cost mapping cols:
    ecaa_generators = ispypsa_tables["ecaa_generators"].copy()
    ecaa_fuel_cols_only = ecaa_generators.loc[
        :, list(_ECAA_GENERATOR_ATTRIBUTES.keys()) + useful_columns
    ]
    ecaa_fuel_cols_only = ecaa_fuel_cols_only.rename(columns=_ECAA_GENERATOR_ATTRIBUTES)

    # for new entrants drop rows with battery or water fuel type (hydro)
    new_entrant_generators = ispypsa_tables["new_entrant_generators"].copy()
    new_entrant_fuel_cols_only = new_entrant_generators.loc[
        ~new_entrant_generators["fuel_type"].isin(["Battery"]),
        list(_NEW_ENTRANT_GENERATOR_ATTRIBUTES.keys()) + useful_columns,
    ]
    new_entrant_fuel_cols_only = new_entrant_fuel_cols_only.rename(
        columns=_NEW_ENTRANT_GENERATOR_ATTRIBUTES
    )

    all_generators = pd.concat(
        [ecaa_fuel_cols_only, new_entrant_fuel_cols_only], axis=0
    )

    fuel_costs = _get_dynamic_fuel_costs(ispypsa_tables, all_generators)
    fuel_costs = fuel_costs.set_index(["carrier", "fuel_cost_mapping"])
    # maybe consider this in fuel_costs function or just handle more nicely!!
    all_generators = all_generators.set_index("name")
    marginal_costs_by_generator = []
    for name, row in all_generators.iterrows():
        gen_fuel_costs = fuel_costs.loc[
            (row["carrier"], row["fuel_cost_mapping"]), :
        ].squeeze()
        marginal_costs = (gen_fuel_costs * row["heat_rate_gj/mwh"]) + row[
            "vom_$/mwh_sent_out"
        ]
        marginal_costs.name = name
        marginal_costs_by_generator.append(marginal_costs)

    # concat in wide format because: later will need to map years to snapshots
    marginal_costs_by_generator = pd.concat(marginal_costs_by_generator, axis=1)
    # get the start date of the financial year for marginal cost
    marginal_costs_by_generator["start_date"] = (
        marginal_costs_by_generator.index.str.extract(r"(\d{4})", expand=False)
    ) + "-07-01"

    marginal_costs_by_generator["start_date"] = pd.to_datetime(
        marginal_costs_by_generator["start_date"]
    )
    # Now align with snapshots and investment periods:
    marginal_cost_timeseries = pd.merge_asof(
        snapshots,
        marginal_costs_by_generator,
        left_on="snapshots",
        right_on="start_date",
    )
    marginal_cost_timeseries = marginal_cost_timeseries.drop(
        columns=["start_date"]
    ).set_index(["investment_periods", "snapshots"])

    output_dir = Path(pypsa_inputs_path, "marginal_cost_timeseries")
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    # Rename columns so they can be written as filenames (removing special chars)
    marginal_cost_timeseries = marginal_cost_timeseries.rename(
        columns={
            col: re.sub(r"[^\w]+", r"_", col)
            for col in marginal_cost_timeseries.columns
        }
    )

    for generator in marginal_cost_timeseries.columns:
        gen_marginal_costs_ts = marginal_cost_timeseries.loc[:, generator].reset_index()
        gen_marginal_costs_ts.to_parquet(
            Path(output_dir, f"{generator}.parquet"), index=False
        )


def _get_dynamic_fuel_costs(
    ispypsa_tables: dict[str : pd.DataFrame], generators_df: pd.DataFrame
) -> dict[str : pd.DataFrame]:
    """Gets all dynamic fuel costs as dataframes including gas, liquid fuel, hyblend, coal,
    biomass and hydrogen.

    Gas and hyblend prices are calculated considering dynamic fuel blending as described
    in the IASR and workbook. All fuel cost values are given in $/GJ, and wind, solar and
    water fuel costs are set to $0.0/GJ.

    Args:
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).
        generators_df: dataframe containing combined ecaa and new entrant generator
            data where each row contains at minimum the generator name and carrier
            (fuel type).

    Returns:
        `pd.DataFrame` : dataframe containing fuel costs for each unique carrier
            and fuel_cost_mapping pair in generators_df.
    """

    unique_carriers = generators_df["carrier"].unique()

    all_dynamic_fuel_prices = []
    non_fuel_carriers = []
    for carrier in unique_carriers:
        if carrier in _CARRIER_TO_FUEL_COST_TABLES.keys():
            carrier_prices_table = _get_single_carrier_fuel_prices(
                carrier, generators_df, ispypsa_tables
            )
            carrier_prices_table["carrier"] = carrier
            all_dynamic_fuel_prices.append(carrier_prices_table)
        elif carrier in ["Wind", "Water", "Solar"]:
            # add any carriers without corresponding fuel cost tables to a separate list
            non_fuel_carriers.append(carrier)

    non_fuel_costs_df = generators_df.loc[
        generators_df["carrier"].isin(non_fuel_carriers),
        ["fuel_cost_mapping", "carrier"],
    ].drop_duplicates()

    all_dynamic_fuel_prices.append(non_fuel_costs_df)
    dynamic_fuel_prices = pd.concat(all_dynamic_fuel_prices, axis=0)
    # Set fuel prices for "free" carriers (wind, solar, water) to 0.0 for all years
    dynamic_fuel_prices.loc[
        dynamic_fuel_prices["carrier"].isin(non_fuel_carriers),
        [col for col in dynamic_fuel_prices.columns if "$" in col],
    ] = 0.0

    return dynamic_fuel_prices


def _get_single_carrier_fuel_prices(
    carrier: str,
    generators_df: pd.DataFrame,
    ispypsa_tables: dict[str : pd.DataFrame],
):
    """Gets fuel prices for a given carrier, calculating blended prices where necessary.

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

    # set the index of base_prices_table to "fuel_cost_mapping" to the column containing
    # the equivalent fuel cost mapping strings.
    if "fuel_cost_mapping_col" in table_mapping.keys():
        base_prices_table = base_prices_table.set_index(
            table_mapping["fuel_cost_mapping_col"]
        )
        base_prices_table.index.name = "fuel_cost_mapping"
    else:
        base_prices_table["fuel_cost_mapping"] = carrier
        base_prices_table = base_prices_table.set_index("fuel_cost_mapping")

    # Calculate prices for fuels that are blended with low-emissions alternatives
    # over time (hyblend, gas)
    if carrier == "Gas":
        base_prices_table = _calculate_gas_biomethane_blend_prices(
            base_prices_table, table_mapping, ispypsa_tables
        )

    if carrier == "Hyblend":
        hyblend_name_mapping = (
            generators_df.loc[
                generators_df["carrier"] == "Hyblend", ["name", "fuel_cost_mapping"]
            ]
            .set_index("name")
            .squeeze()
            .to_dict()
        )
        base_prices_table = _calculate_hyblend_prices(
            base_prices_table, hyblend_name_mapping, table_mapping, ispypsa_tables
        )

    return base_prices_table.reset_index()


def _calculate_hyblend_prices(
    base_prices_table: pd.DataFrame,
    hyblend_name_mapping: dict[str:str],
    table_mapping: dict[str:str],
    ispypsa_tables: dict[str : pd.DataFrame],
) -> pd.DataFrame:
    """Calculates gas prices including impacts of blending with hydrogen ("Hyblend"
    carrier/fuel_type).

    Args:
        base_prices_table: dataframe containing the prices of the fuel to be blended
            with hydrogen (in IASR v6, this is Gas).
        hyblend_name_mapping: dictionary containing a mapping between the fuel_cost_mapping
            string and generator name string for each generator with Hyblend carrier.
        table_mapping: dictionary with mapping to `ISPyPSA` input table names
            necessary to calculate Hyblend fuel prices.
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).

    Returns:
        `pd.DataFrame`: calculated gas prices including hydrogen blend in tabular
            format.
    """

    # Select only the gas prices that are directly referenced by hyblend fuel cost mappings
    base_prices_hyblend_only = base_prices_table.loc[hyblend_name_mapping.values(), :]

    # Get the hydrogen prices and mix percentages, replace generator names by fuel cost mapping:
    blend_prices_series = ispypsa_tables[table_mapping["blend_table"]].squeeze()

    blend_percentages = (
        ispypsa_tables[table_mapping["blend_percent_table"]]
        .replace(hyblend_name_mapping)
        .set_index(table_mapping["fuel_cost_mapping_col"])
    ) / 100
    blend_percentages.index.name = "fuel_cost_mapping"
    # rename columns to match base_prices_hyblend_only column names (preserving order)
    blend_percentages = blend_percentages.rename(
        columns={col: col.replace("%", "$/gj") for col in blend_percentages.columns}
    )

    base_prices_hyblend_only = (
        base_prices_hyblend_only * blend_percentages
        + blend_prices_series * (1 - blend_percentages)
    )

    return base_prices_hyblend_only


def _calculate_gas_biomethane_blend_prices(
    base_prices_table: pd.DataFrame,
    table_mapping: dict[str:str],
    ispypsa_tables: dict[str : pd.DataFrame],
) -> pd.DataFrame:
    """Calculates gas prices including impacts of blending with biomethane.

    Args:
        base_prices_table: dataframe containing the base gas prices.
        table_mapping: dictionary with mapping to `ISPyPSA` input table names
            necessary to calculate gas fuel prices.
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).

    Returns:
        `pd.DataFrame`: calculated gas prices including biomethane blend in tabular
            format.
    """
    biomethane_prices_series = ispypsa_tables[table_mapping["blend_table"]].squeeze()
    biomethane_mix_percentages = ispypsa_tables[table_mapping["blend_percent_table"]]
    # rename percentage columns so they match price table columns
    biomethane_mix_percentages = biomethane_mix_percentages.rename(
        columns={
            col: col.replace("%", "$/gj") for col in biomethane_mix_percentages.columns
        }
    )

    biomethane_percentage_series = biomethane_mix_percentages.squeeze() / 100

    base_prices_table = (
        base_prices_table * biomethane_percentage_series
        + biomethane_prices_series * (1 - biomethane_percentage_series)
    )
    return base_prices_table


def create_pypsa_friendly_ecaa_generator_timeseries(
    ecaa_generators: pd.DataFrame,
    trace_data_path: Path | str,
    pypsa_timeseries_inputs_path: Path | str,
    generator_types: List[Literal["solar", "wind"]],
    reference_year_mapping: dict[int:int],
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
    reference_year_mapping: dict[int:int],
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
        # temp solution to avoid errors with N12 traces not currently existing:
        if "N12" not in gen:
            generator_type = gen_to_type[gen].lower()
            try:
                area_abbreviation = re.search(r"[A-Z]\d+", gen)[0]
            except:
                print("yeep")
                area_abbreviation = re.search(r"[A-Z]\d+", gen)

            technology_or_resource_quality = re.search(r"\d_([A-Z]{2,3})", gen).group(1)
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
                trace["snapshots"], snapshots["snapshots"], "generator trace data", gen
            )
            trace = pd.merge(trace, snapshots, on="snapshots")
            trace = trace.loc[:, ["investment_periods", "snapshots", "p_max_pu"]]
            trace.to_parquet(
                Path(output_paths[generator_type], f"{gen}.parquet"), index=False
            )


def _add_closure_year_column(
    ecaa_generators: pd.DataFrame, closure_years: pd.DataFrame
) -> pd.DataFrame:
    """Adds a column containing the expected closure year (calendar year) for ECAA generators.

    Note: currently only one generator object is templated and translated per ECAA
    generator, while some generators have multiple units with different expected closure
    years. This function makes the OPINIONATED choice to return only the first expected
    year given in closure_years table for each set of generating units.

    Args:
        ecaa_generators: `ISPyPSA` formatted pd.DataFrame detailing the ECAA generators.
        closure_years: `ISPyPSA` formatted pd.Dataframe containing expected closure years
            for the ECAA generators, by unit.

    Returns:
        `pd.DataFrame`: ECAA generator attributes table with additional closure year column.
    """
    ecaa_generators["closure_year"] = ecaa_generators["generator"]

    closure_years = closure_years[
        ["generator", "expected_closure_year_calendar_year"]
    ].drop_duplicates(["generator"], keep="first")
    closure_years_dict = closure_years.set_index("generator").squeeze().to_dict()

    where_str = ecaa_generators["closure_year"].apply(lambda x: isinstance(x, str))
    ecaa_generators.loc[where_str, "closure_year"] = _fuzzy_match_names(
        ecaa_generators.loc[where_str, "closure_year"],
        closure_years_dict.keys(),
        f"adding closure_year column to ecaa_generators table",
        not_match="existing",
        threshold=90,
    )
    ecaa_generators["closure_year"] = ecaa_generators["closure_year"].replace(
        closure_years_dict
    )
    return ecaa_generators


def _calculate_annuitised_new_entrant_gen_capital_costs(
    ispypsa_tables: dict[str : pd.DataFrame],
    investment_periods: list[int],
    wacc: float,
) -> pd.DataFrame:
    """Calculates annuitised capital cost of each new entrant generator in each possible
    build year.

    Args:
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).
        investment_periods: list of years in which investment periods start obtained
            from the model configuration.
        wacc: as float, weighted average cost of capital, an interest rate specifying
            how expensive it is to borrow money for the asset investment.

    Returns:
        new_entrant_capital_costs_dict: dictionary containing mapping from `name` to
            annuitised `capital_cost` for each new entrant generator and build year,
            including fixed OPEX, build and connection costs, and system strength
            costs where relevant.
    """

    new_entrant_generators = ispypsa_tables["new_entrant_generators"].copy()
    new_entrant_generators_each_build_year = []
    for year in investment_periods:
        new_entrants_one_year = new_entrant_generators.copy()
        new_entrants_one_year["build_year"] = year
        new_entrant_generators_each_build_year.append(new_entrants_one_year)

    new_entrant_generators_each_build_year = pd.concat(
        new_entrant_generators_each_build_year, axis=0, ignore_index=True
    )

    build_costs = ispypsa_tables["new_entrant_build_costs"]
    build_costs.loc[:, "technology"] = _fuzzy_match_names(
        build_costs.loc[:, "technology"],
        new_entrant_generators_each_build_year["generator_name"].unique(),
        "adding build_costs to capital_cost calculation for new entrant generators",
        not_match="existing",
        threshold=90,
    )

    build_costs = build_costs.set_index("technology")

    build_year_strings = {
        f"{year - 1}_{str(year)[-2:]}_$/mw": year for year in investment_periods
    }
    build_costs = build_costs.loc[:, build_year_strings.keys()]
    build_costs = build_costs.rename(columns=build_year_strings)

    # add the system strength connection costs to build_year_strings dict to filter VRE connection costs:
    build_year_strings["system_strength_connection_cost_$/mw"] = "system_strength"
    wind_and_solar_connection_costs = ispypsa_tables[
        "new_entrant_wind_and_solar_connection_costs"
    ].set_index("REZ names")
    wind_and_solar_connection_costs = wind_and_solar_connection_costs.loc[
        :, build_year_strings.keys()
    ]
    wind_and_solar_connection_costs = wind_and_solar_connection_costs.rename(
        columns=build_year_strings
    ).fillna(0.0)

    non_vre_connection_costs = ispypsa_tables["new_entrant_non_vre_connection_costs"]
    non_vre_connection_costs = non_vre_connection_costs.melt(
        id_vars=["Region"],
        var_name="connection_cost_technology",
        value_name="connection_cost_$/mw",
    )
    non_vre_connection_costs = (
        non_vre_connection_costs.replace(to_replace=r"_\$\/mw", value="", regex=True)
        .fillna(0.0)
        .set_index(["Region", "connection_cost_technology"])
    )

    # force connection_cost_technology into snake_case in same format as table column names:
    new_entrant_generators_each_build_year["connection_cost_technology"] = (
        new_entrant_generators_each_build_year["connection_cost_technology"].apply(
            lambda tech_name: _snakecase_string(tech_name)
        )
    )
    new_entrant_capital_costs_dict = {}
    for idx, row in new_entrant_generators_each_build_year.iterrows():
        # build cost = build_cost * LCF (given as %, so divide by 100)
        build_cost = (
            build_costs.at[row["generator_name"], row["build_year"]]
            * row["technology_specific_lcf_%"]
            / 100
        )
        # connection_cost = connection_cost + strength_cost -> fuel_type dictates source for
        # these values
        if row["fuel_type"] in (["Wind", "Solar"]):
            connection_cost = (
                wind_and_solar_connection_costs.at[
                    row["connection_cost_rez/_region_id"], row["build_year"]
                ]
                + wind_and_solar_connection_costs.at[
                    row["connection_cost_rez/_region_id"], "system_strength"
                ]
            )
        else:
            connection_cost = non_vre_connection_costs.at[
                (row["region_id"], row["connection_cost_technology"]),
                "connection_cost_$/mw",
            ]

        build_and_connection_annuitised = _annuitised_investment_costs(
            (build_cost + connection_cost), wacc, row["lifetime"]
        )

        capital_cost = build_and_connection_annuitised + row["fom_$/kw/annum"]
        name = f"{row['generator']}_{row['build_year']}"

        new_entrant_capital_costs_dict[name] = capital_cost

    return new_entrant_capital_costs_dict
