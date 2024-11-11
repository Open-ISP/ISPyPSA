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


def _translate_buses_timeseries(
    ispypsa_inputs_path: Path | str,
    trace_data_path: Path | str,
    pypsa_inputs_path: Path | str,
    scenario: str,
    granularity: str,
    reference_year_mapping: dict[int:int],
    year_type: Literal["fy", "calendar"],
) -> None:
    """Gets trace data for operational demand by constructing a timeseries from the start to end year using the
    eference year cycle provided. Trace data is then saved as a parquet file to .

    Args:
        ispypsa_inputs_path: Path to directory containing modelling input template CSVs.
        trace_data_path: Path to directory containing trace data parsed by isp-trace-parser
        pypsa_inputs_path: Path to director where input translated to pypsa format will be saved
        scenario: str, ISP scenario to use demand traces from
        granularity: Geographical granularity obtained from the model configuration
        reference_year_mapping: dict[int: int], mapping model years to trace data reference years
        year_type: str, 'fy' or 'calendar', if 'fy' then time filtering is by financial year with start_year and
            end_year specifiying the financial year to return data for, using year ending nomenclature (2016 ->
            FY2015/2016). If 'calendar', then filtering is by calendar year.

    Returns:
        None
    """
    region_and_zone_mapping = pd.read_csv(
        ispypsa_inputs_path / Path("region_and_zone_mapping.csv")
    )

    region_mapping = region_and_zone_mapping.drop_duplicates(
        ["nem_region_id", "isp_sub_region_id"]
    )

    if granularity == "regional":
        region_mapping["node_id"] = "nem_region_id"
    elif granularity == "sub_regional":
        region_mapping["node_id"] = "isp_sub_region_id"

    nodes = pd.read_csv(ispypsa_inputs_path / Path("node_template.csv"))

    trace_data_path = trace_data_path / Path("demand")

    output_trace_path = Path(pypsa_inputs_path, "demand_traces")

    if not output_trace_path.exists():
        output_trace_path.mkdir(parents=True)

    for node in region_mapping["node_id"]:
        sub_regions_under_node = region_mapping[region_mapping["node_id"] == node]["isp_sub_region_id"]
        node_traces = []
        for sub_region in sub_regions_under_node:
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
        node_trace.to_parquet(Path(output_trace_path, f"{node}.parquet"), index=False)
