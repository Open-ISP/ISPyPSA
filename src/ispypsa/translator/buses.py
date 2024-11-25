from pathlib import Path
from typing import Literal

import pandas as pd
from isp_trace_parser import get_data

from ispypsa.translator.mappings import _BUS_ATTRIBUTES


def _translate_nodes_to_buses(ispypsa_inputs_path: Path | str) -> pd.DataFrame:
    """Process network node data into a format aligned with PyPSA inputs.

    Args:
        ispypsa_inputs_path: Path to directory containing modelling input template CSVs.

    Returns:
        `pd.DataFrame`: PyPSA style generator attributes in tabular format.
    """
    nodes = pd.read_csv(ispypsa_inputs_path / Path("nodes.csv"))

    buses = nodes.loc[:, _BUS_ATTRIBUTES.keys()]
    buses = buses.rename(columns=_BUS_ATTRIBUTES)

    buses = buses.set_index("name", drop=True)

    return buses


def _translate_buses_demand_timeseries(
    ispypsa_inputs_path: Path | str,
    trace_data_path: Path | str,
    pypsa_inputs_path: Path | str,
    scenario: str,
    regional_granularity: str,
    reference_year_mapping: dict[int:int],
    year_type: Literal["fy", "calendar"],
) -> None:
    """Gets trace data for operational demand by constructing a timeseries from the
    start to end year using the reference year cycle provided.

    Trace data is then saved as a parquet file to `pypsa_inputs_path`.

    Args:
        ispypsa_inputs_path: Path to directory containing modelling input template CSVs.
        trace_data_path: Path to directory containing trace data parsed by isp-trace-parser
        pypsa_inputs_path: Path to director where input translated to pypsa format will be saved
        scenario: str, ISP scenario to use demand traces from
        regional_granularity: Regional granularity of the nodes obtained from the model
            configuration. Defaults to "sub_regions".
        reference_year_mapping: dict[int: int], mapping model years to trace data reference years
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns:
        None
    """
    trace_data_path = trace_data_path / Path("demand")
    output_trace_path = Path(pypsa_inputs_path, "demand_traces")
    if not output_trace_path.exists():
        output_trace_path.mkdir(parents=True)

    all_nodes = pd.read_csv(ispypsa_inputs_path / Path("nodes.csv"))
    # remove "s" unless single_region for for type filtering
    if regional_granularity != "single_region":
        demand_node_type = regional_granularity[:-1]
    else:
        demand_node_type = regional_granularity
    demand_nodes = all_nodes.loc[all_nodes["type"] == demand_node_type]
    for demand_node in demand_nodes["node_id"]:
        node_traces = []
        if regional_granularity == "sub_regions":
            trace = get_data.demand_multiple_reference_years(
                reference_years=reference_year_mapping,
                directory=trace_data_path,
                subregion=demand_node,
                scenario=scenario,
                year_type=year_type,
                demand_type="OPSO_MODELLING",
                poe="POE50",
            )
            node_traces.append(trace)
        else:
            sub_regions_to_nem_regions = pd.read_csv(
                ispypsa_inputs_path / Path("mapping_sub_regions_to_nem_regions.csv")
            )
            if regional_granularity == "nem_regions":
                sub_regions_in_demand_node = sub_regions_to_nem_regions.loc[
                    sub_regions_to_nem_regions["nem_region_id"] == demand_node,
                    "isp_sub_region_id",
                ]
            elif regional_granularity == "single_region":
                sub_regions_in_demand_node = sub_regions_to_nem_regions.loc[
                    :, "isp_sub_region_id"
                ]
            for sub_region in sub_regions_in_demand_node:
                trace = get_data.demand_multiple_reference_years(
                    reference_years=reference_year_mapping,
                    directory=trace_data_path,
                    subregion=sub_region,
                    scenario=scenario,
                    year_type=year_type,
                    demand_type="OPSO_MODELLING",
                    poe="POE50",
                )
                node_traces.append(trace)
        node_traces = pd.concat(node_traces)
        node_trace = node_traces.groupby("Datetime", as_index=False)["Value"].sum()
        node_trace.to_parquet(
            Path(output_trace_path, f"{demand_node}.parquet"), index=False
        )
