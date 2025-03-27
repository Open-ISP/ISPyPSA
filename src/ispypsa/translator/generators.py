import re
from pathlib import Path
from typing import List, Literal

import pandas as pd
from isp_trace_parser import get_data

from ispypsa.config import ModelConfig
from ispypsa.translator.mappings import (
    _GENERATOR_ATTRIBUTES,
    _NEW_ENTRANT_GENERATOR_ATTRIBUTES,
)
from ispypsa.translator.temporal_filters import _time_series_filter
from ispypsa.translator.time_series_checker import _check_time_series


def _translate_ecaa_generators(
    ecaa_generators: pd.DataFrame, regional_granularity: str = "sub_regions"
) -> pd.DataFrame:
    """Process data on existing, committed, anticipated, and additional (ECAA) generators
    into a format aligned with PyPSA inputs.

    Args:
        ecaa_generators: `ISPyPSA` formatted pd.DataFrame detailing the ECAA generators.
        regional_granularity: Regional granularity of the nodes obtained from the model
            configuration. Defaults to "sub_regions".

    Returns:
        `pd.DataFrame`: `PyPSA` style generator attributes in tabular format.
    """

    gen_attributes = _GENERATOR_ATTRIBUTES.copy()

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

    marginal_costs = {
        "Black Coal": 50.0,
        "Brown Coal": 30.0,
        "Gas": 300.0,
        "Liquid Fuel": 400.0,
        "Water": 300.0,
        "Solar": 10.0,
        "Wind": 10.0,
        "Hyblend": 400.0,
    }

    ecaa_generators_pypsa_format["marginal_cost"] = ecaa_generators_pypsa_format[
        "carrier"
    ].map(marginal_costs)

    return ecaa_generators_pypsa_format


def _translate_new_entrant_generators(
    new_entrant_generators: pd.DataFrame, regional_granularity: str = "sub_regions"
) -> pd.DataFrame:
    """Process data on new entrant thermal and variable renewable generators
    into a format aligned with PyPSA inputs.

    Args:
        new_entrant_generators: `ISPyPSA` formatted pd.DataFrame detailing the new
            entrant generators.
        regional_granularity: Regional granularity of the nodes obtained from the model
            configuration. Defaults to "sub_regions".

    Returns:
        `pd.DataFrame`: PyPSA style generator attributes in tabular format.
    """
    gen_attributes = _NEW_ENTRANT_GENERATOR_ATTRIBUTES.copy()
    # drop battery storage and pumped hydro for the moment:
    new_entrant_generators_template = new_entrant_generators.loc[
        ~new_entrant_generators["fuel_type"].isin(["Battery", "Water"])
    ]

    if regional_granularity == "sub_regions":
        gen_attributes["sub_region_id"] = "bus"
    elif regional_granularity == "nem_regions":
        gen_attributes["region_id"] = "bus"

    new_entrant_generators_pypsa_format = new_entrant_generators_template.loc[
        :, gen_attributes.keys()
    ]
    new_entrant_generators_pypsa_format = new_entrant_generators_pypsa_format.rename(
        columns=gen_attributes
    )

    if regional_granularity == "single_region":
        new_entrant_generators_pypsa_format["bus"] = "NEM"

    marginal_costs = {  # dummy values - to be calculated in this step
        "Biomass": 30.0,
        "Gas": 300.0,
        "Water": 300.0,
        "Solar": 10.0,
        "Wind": 10.0,
        "Hydrogen": 400.0,
    }

    new_entrant_generators_pypsa_format["marginal_cost"] = (
        new_entrant_generators_pypsa_format["carrier"].map(marginal_costs)
    )

    return new_entrant_generators_pypsa_format


def _calculate_dynamic_marginal_costs(
    config: ModelConfig, ispypsa_tables: dict[str : pd.DataFrame]
) -> pd.DataFrame:
    """
    Args:
        config: `ISPyPSA` `ispypsa.config.ModelConfig` object (add link to config docs).
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).
    """
    import ipdb

    ipdb.set_trace()
    # fuel costs: some are dynamic, some are nothing
    useful_columns = ["fuel_cost_mapping", "vom_$/mwh_sent_out", "heat_rate_gj/mwh"]

    # first load in all generators and get name, fuel type and fuel cost mapping cols:
    ecaa_generators = ispypsa_tables["ecaa_generators"]
    ecaa_fuel_cols_only = ecaa_generators.loc[
        :, list(_GENERATOR_ATTRIBUTES.keys()) + useful_columns
    ]
    ecaa_fuel_cols_only = ecaa_fuel_cols_only.rename(columns=_GENERATOR_ATTRIBUTES)

    # for new entrants drop rows with battery or water fuel type (hydro)
    new_entrant_generators = ispypsa_tables["new_entrant_generators"]
    new_entrant_fuel_cols_only = new_entrant_generators.loc[
        ~new_entrant_generators["fuel_type"].isin(["Battery", "Water"]),
        list(_NEW_ENTRANT_GENERATOR_ATTRIBUTES.keys()) + useful_columns,
    ]
    new_entrant_fuel_cols_only = new_entrant_fuel_cols_only.rename(
        columns=_NEW_ENTRANT_GENERATOR_ATTRIBUTES
    )

    all_generators = pd.concat(
        [ecaa_fuel_cols_only, new_entrant_fuel_cols_only], axis=0
    )
    unique_carriers = all_generators["carrier"].unique()

    fuel_costs = _get_dynamic_fuel_costs(ispypsa_tables, unique_carriers)

    # config -> needed to get year type? And start/end, snapshots...??

    # expected output for this function:
    """
    dynamic_marginal_costs:
    year    generator_name_1    generator_name_2    generator_name_3
    2022-23             90.0                80.0                 0.0
    2023-24             89.0                70.4                 0.0
    ....                ....                ....                ....

    """
    return


def _get_dynamic_fuel_costs(
    ispypsa_tables: dict[str : pd.DataFrame], carriers: list[str]
) -> dict[str : pd.DataFrame]:
    """Gets all dynamic fuel costs as dataframes including gas, liquid fuel, hyblend, coal,
    biomass and hydrogen.

    Gas and hyblend prices are calculated including a dynamic emissions reduction factor.

    Args:
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).

    Returns:
        fuel_cost_tables: dictionary of dataframes containing fuel costs for each carrier
            (fuel_type) including calculated values. All fuel cost values are given
            in $/GJ.
    """

    fuel_cost_tables = {
        "Black Coal": ispypsa_tables["coal_prices"],
        "Brown Coal": ispypsa_tables["coal_prices"],
    }

    return fuel_cost_tables


def create_pypsa_friendly_existing_generator_timeseries(
    ecaa_generators: pd.DataFrame,
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
        ecaa_generators: `ISPyPSA` formatted pd.DataFrame detailing the ECAA generators.
        trace_data_path: Path to directory containing trace data parsed by
            isp-trace-parser
        pypsa_inputs_path: Path to director where input translated to pypsa format will
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

    generator_types_caps = [gen_type.capitalize() for gen_type in generator_types]

    generators = ecaa_generators[
        ecaa_generators["fuel_type"].isin(generator_types_caps)
    ].copy()

    generators = list(generators["generator"])

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
    snapshot: pd.DataFrame,
) -> None:
    """Gets trace data for generators by constructing a timeseries from the start to end
    year using the reference year cycle provided. Trace data is then saved as a parquet
    file to subdirectories labeled with their generator type.

    Args:
        new_entrant_generators: `ISPyPSA` formatted pd.DataFrame detailing the new
            entrant generators.
        trace_data_path: Path to directory containing trace data parsed by
            isp-trace-parser
        pypsa_inputs_path: Path to director where input translated to pypsa format will
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

    generator_types_caps = [gen_type.capitalize() for gen_type in generator_types]

    generators = new_entrant_generators[
        new_entrant_generators["fuel_type"].isin(generator_types_caps)
    ].copy()

    generators = list(generators["technology_location_id"])
    gen_to_type = dict(
        zip(
            new_entrant_generators["technology_location_id"],
            new_entrant_generators["fuel_type"],
        )
    )

    query_functions = {
        "solar": get_data.solar_area_multiple_reference_years,
        "wind": get_data.wind_area_multiple_reference_years,
    }

    for gen in generators:
        generator_type = gen_to_type[gen]
        area_abbreviation = re.search(r"[A-Z]\d+", gen)[0]
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
        trace = _time_series_filter(trace, snapshot)
        _check_time_series(
            trace["Datetime"], snapshot.index.to_series(), "generator trace data", gen
        )
        trace.to_parquet(
            Path(output_paths[generator_type], f"{gen}.parquet"), index=False
        )
