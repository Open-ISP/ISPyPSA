from pathlib import Path
from typing import List, Literal

import pandas as pd
from isp_trace_parser import get_data

from ispypsa.translator.mappings import _GENERATOR_ATTRIBUTES
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


def create_pypsa_friendly_existing_generator_timeseries(
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
            data for, using year ending nomenclature (2016 ->FY2015/2016). If
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
