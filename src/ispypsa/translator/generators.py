from pathlib import Path
from typing import Literal

import pandas as pd
from isp_trace_parser import get_data

from ispypsa.translator.mappings import _GENERATOR_ATTRIBUTES
from ispypsa.translator.time_series_checker import check_time_series


def _translate_ecaa_generators(
    ispypsa_inputs_path: Path | str, regional_granularity: str = "sub_regions"
) -> pd.DataFrame:
    """Process data on existing, committed, anticipated, and additional (ECAA) generators
    into a format aligned with PyPSA inputs.

    Args:
        ispypsa_inputs_path: Path to directory containing modelling input template CSVs.
        regional_granularity: Regional granularity of the nodes obtained from the model
            configuration. Defaults to "sub_regions".

    Returns:
        `pd.DataFrame`: PyPSA style generator attributes in tabular format.
    """
    ecaa_generators_template = pd.read_csv(
        ispypsa_inputs_path / Path("ecaa_generators.csv")
    )

    if regional_granularity == "sub_regions":
        _GENERATOR_ATTRIBUTES["sub_region_id"] = "bus"
    elif regional_granularity == "nem_regions":
        _GENERATOR_ATTRIBUTES["region_id"] = "bus"

    ecaa_generators_pypsa_format = ecaa_generators_template.loc[
        :, _GENERATOR_ATTRIBUTES.keys()
    ]
    ecaa_generators_pypsa_format = ecaa_generators_pypsa_format.rename(
        columns=_GENERATOR_ATTRIBUTES
    )

    if regional_granularity == "single_region":
        ecaa_generators_pypsa_format["bus"] = "NEM"

    marginal_costs = {
        "Black Coal": 50.0,
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

    ecaa_generators_pypsa_format = ecaa_generators_pypsa_format.set_index(
        "name", drop=True
    )
    return ecaa_generators_pypsa_format


def _translate_generator_timeseries(
    ispypsa_inputs_path: Path | str,
    trace_data_path: Path | str,
    pypsa_inputs_path: Path | str,
    generator_type: Literal["solar", "wind"],
    reference_year_mapping: dict[int:int],
    year_type: Literal["fy", "calendar"],
    snapshot: pd.DataFrame,
) -> None:
    """Gets trace data for generators by constructing a timeseries from the start to end year using the reference year
    cycle provided. Trace data is then saved as a parquet file to .

    Args:
        ispypsa_inputs_path: Path to directory containing modelling input template CSVs.
        trace_data_path: Path to directory containing trace data parsed by isp-trace-parser
        pypsa_inputs_path: Path to director where input translated to pypsa format will be saved
        reference_year_mapping: dict[int: int], mapping model years to trace data reference years
        generator_type: Literal['solar', 'wind'], which type of generator to translate trace data for.
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.
        snapshot: pd.DataFrame containing the expected time series values.

    Returns:
        None
    """
    ecaa_generators_template = pd.read_csv(
        ispypsa_inputs_path / Path("ecaa_generators.csv")
    )

    trace_data_path = trace_data_path / Path(generator_type)

    output_trace_path = Path(pypsa_inputs_path, f"{generator_type}_traces")

    if not output_trace_path.exists():
        output_trace_path.mkdir(parents=True)

    generators = ecaa_generators_template[
        ecaa_generators_template["fuel_type"] == generator_type.capitalize()
    ].copy()
    generators = list(generators["generator"])

    query_functions = {
        "solar": get_data.solar_project_multiple_reference_years,
        "wind": get_data.wind_project_multiple_reference_years,
    }

    for gen in generators:
        trace = query_functions[generator_type](
            reference_years=reference_year_mapping,
            project=gen,
            directory=trace_data_path,
            year_type=year_type,
        )
        # datetime in nanoseconds required by PyPSA
        trace["Datetime"] = trace["Datetime"].astype("datetime64[ns]")
        check_time_series(
            trace["Datetime"], pd.Series(snapshot.index), "generator trace data", gen
        )
        trace.to_parquet(Path(output_trace_path, f"{gen}.parquet"), index=False)
