from pathlib import Path
from typing import Literal

import pandas as pd
from isp_trace_parser import get_data

from ispypsa.translator.mappings import _BUS_ATTRIBUTES
from ispypsa.translator.temporal_filters import _time_series_filter
from ispypsa.translator.time_series_checker import _check_time_series


def _translate_isp_sub_regions_to_buses(isp_sub_regions: pd.DataFrame) -> pd.DataFrame:
    """Process ISP sub region data into the PyPSA format for buses.

    Args:
        isp_sub_regions: `ISPyPSA` formatted pd.DataFrame detailing ISP sub regions.

    Returns:
        `pd.DataFrame`: PyPSA style bus attributes in tabular format.
    """
    buses = isp_sub_regions.loc[:, ["isp_sub_region_id"]]
    buses = buses.rename(columns={"isp_sub_region_id": "name"})
    return buses


def _translate_nem_regions_to_buses(nem_regions: pd.DataFrame) -> pd.DataFrame:
    """Process NEM region data into the PyPSA format for buses.

    Args:
        nem_regions: `ISPyPSA` formatted pd.DataFrame detailing NEM regions.

    Returns:
        `pd.DataFrame`: PyPSA style bus attributes in tabular format.
    """
    buses = nem_regions.loc[:, ["nem_region_id"]]
    buses = buses.rename(columns={"nem_region_id": "name"})
    return buses


def _create_single_region_bus() -> pd.DataFrame:
    """Create table specifying the name of single region in the PyPSA format.

    Returns:
        `pd.DataFrame`: PyPSA style bus attributes in tabular format.
    """
    buses = pd.DataFrame({"name": ["NEM"]})
    return buses


def _translate_rezs_to_buses(renewable_energy_zones: pd.DataFrame) -> pd.DataFrame:
    """Process ISP Renewable Energy Zone location data into the PyPSA format for buses.

    Args:
        nem_regions: `ISPyPSA` formatted pd.DataFrame detailing Renewable Energy Zone
            locations.

    Returns:
        `pd.DataFrame`: PyPSA style bus attributes in tabular format.
    """
    buses = renewable_energy_zones.loc[:, ["rez_id"]]
    buses = buses.rename(columns={"rez_id": "name"})
    return buses


def create_pypsa_friendly_bus_demand_timeseries(
    isp_sub_regions: pd.DataFrame,
    trace_data_path: Path | str,
    scenario: str,
    regional_granularity: str,
    reference_year_mapping: dict[int:int],
    year_type: Literal["fy", "calendar"],
) -> dict[str, pd.DataFrame]:
    """Gets trace data for operational demand by constructing a timeseries from the
    start to end year using the reference year cycle provided. Returns a dictionary
    of dataframes with demand node names as keys.

    Args:
        isp_sub_regions: isp_sub_regions: `ISPyPSA` formatted pd.DataFrame detailing ISP
            sub regions.
        trace_data_path: Path to directory containing trace data parsed by
            isp-trace-parser
        scenario: str, ISP scenario to use demand traces from
        regional_granularity: Regional granularity of the nodes obtained from the model
            configuration. Defaults to "sub_regions".
        reference_year_mapping: dict[int: int], mapping model years to trace data
            reference years
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial
            year with start_year and end_year specifiying the financial year to return
            data for, using year ending nomenclature (2016 ->FY2015/2016). If
            'calendar', then filtering is by calendar year.

    Returns:
        dict[str, pd.DataFrame]: Dictionary with demand node names as keys and trace
            dataframes as values. Each dataframe contains columns: Datetime, Value
    """
    trace_data_path = trace_data_path / Path("demand")

    # remove "s" unless single_region for for type filtering
    if regional_granularity == "single_region":
        isp_sub_regions["demand_nodes"] = "NEM"
    elif regional_granularity == "nem_regions":
        isp_sub_regions["demand_nodes"] = isp_sub_regions["nem_region_id"]
    elif regional_granularity == "sub_regions":
        isp_sub_regions["demand_nodes"] = isp_sub_regions["isp_sub_region_id"]

    demand_nodes = list(isp_sub_regions["demand_nodes"].unique())

    demand_traces = {}

    for demand_node in demand_nodes:
        mask = isp_sub_regions["demand_nodes"] == demand_node
        sub_regions_to_aggregate = list(isp_sub_regions.loc[mask, "isp_sub_region_id"])

        node_traces = []
        for sub_regions in sub_regions_to_aggregate:
            trace = get_data.demand_multiple_reference_years(
                reference_years=reference_year_mapping,
                directory=trace_data_path,
                subregion=sub_regions,
                scenario=scenario,
                year_type=year_type,
                demand_type="OPSO_MODELLING",
                poe="POE50",
            )
            node_traces.append(trace)

        node_traces = pd.concat(node_traces)
        node_trace = node_traces.groupby("Datetime", as_index=False)["Value"].sum()
        # datetime in nanoseconds required by PyPSA
        node_trace["Datetime"] = node_trace["Datetime"].astype("datetime64[ns]")
        demand_traces[demand_node] = node_trace

    return demand_traces
