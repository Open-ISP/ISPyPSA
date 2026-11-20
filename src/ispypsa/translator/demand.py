"""Translate the new-format demand_subregions table into demand nodes and
their demand traces.

This module sits in the translator stage, between the templater that produces
the ISPyPSA tables and the PyPSA model build. It works out which buses carry
demand and builds the demand trace for each of them.

The input is the demand_subregions table, with one row per ISP sub-region:

    subregion_id  geo_id
    CNSW          NSW
    NNSW          NSW
    SQ            QLD

There are two outputs. The demand nodes table has one row per bus that
carries demand:

    name
    NSW
    QLD

The demand traces are a dictionary with one trace per demand node, keyed by
the node's name. Each trace has one row per half hour:

    datetime             value
    2024-07-01 00:30:00  8100.0
    2024-07-01 01:00:00  7950.0

Demand traces are published per ISP sub-region, whatever regional granularity
the model is built at, and the templater has already decided which geography
each sub-region's demand is placed at. So nothing here depends on the
regional granularity: the same steps serve "sub_regions", "nem_regions" and
"single_region" models.

_translate_demand_subregions_to_demand_nodes lists the distinct geographies
in the table. _create_demand_node_traces reads every sub-region's trace
for the model years, warns about sub-regions that have no trace data, and adds
up the traces of the sub-regions placed at the same node. A node's total is
then floored at zero, because the model has no use for negative demand.
"""

import logging
from pathlib import Path
from typing import Literal

import pandas as pd
from isp_trace_parser import get_data


def _translate_demand_subregions_to_demand_nodes(
    demand_subregions: pd.DataFrame,
) -> pd.DataFrame:
    """Lists the buses that carry demand, one row per bus.

    I/O Example:
        demand_subregions:
            subregion_id  geo_id
            CNSW          NSW
            NNSW          NSW     # shares the NSW bus with CNSW
            SQ            QLD

        returns:
            name
            NSW
            QLD
    """
    demand_nodes = demand_subregions[["geo_id"]].drop_duplicates()
    demand_nodes = demand_nodes.rename(columns={"geo_id": "name"})
    return demand_nodes.reset_index(drop=True)


def _create_demand_node_traces(
    demand_subregions: pd.DataFrame,
    trace_data_path: Path | str,
    scenario: str,
    reference_year_mapping: dict[int, int],
    year_type: Literal["fy", "calendar"],
) -> dict[str, pd.DataFrame]:
    """Builds the operational demand trace for each demand node, as a
    dictionary keyed by the node's name.

    The trace of every sub-region in ``demand_subregions`` is read for the
    model years in ``reference_year_mapping``, and the traces of the
    sub-regions placed at the same geo_id are added together. A sub-region
    with no trace data is logged and contributes nothing, so a node whose
    sub-regions all lack trace data has no entry in the result.

    Args:
        demand_subregions: `ISPyPSA` formatted table placing each ISP
            sub-region's demand at a geography.
        trace_data_path: Path to the directory of trace data parsed by
            isp-trace-parser. Demand traces are read from its "demand"
            subdirectory.
        scenario: ISP scenario to use demand traces from.
        reference_year_mapping: mapping of model years to the trace data
            reference years to use for them.
        year_type: 'fy' or 'calendar'. If 'fy' the model years are financial
            years, using year ending nomenclature (2016 -> FY2015/2016).

    I/O Example:
        demand_subregions:
            subregion_id  geo_id
            CNSW          NSW
            NNSW          NSW
            SQ            QLD
            CQ            QLD     # no trace data: logged, adds nothing

        sub-region traces read from trace_data_path:
            subregion  datetime             value
            CNSW       2024-07-01 00:30:00  5000.0
            NNSW       2024-07-01 00:30:00  500.0
            SQ         2024-07-01 00:30:00  3500.0

        returns["NSW"]:
            datetime             value
            2024-07-01 00:30:00  5500.0

        returns["QLD"]:
            datetime             value
            2024-07-01 00:30:00  3500.0
    """
    subregion_traces = get_data.get_demand_multiple_reference_years(
        reference_year_mapping=reference_year_mapping,
        subregion=list(demand_subregions["subregion_id"]),
        scenario=scenario,
        poe="POE50",
        demand_type="OPSO_MODELLING",
        directory=Path(trace_data_path) / "demand",
        year_type=year_type,
        select_columns=["subregion", "datetime", "value"],
    )
    _log_subregions_without_traces(demand_subregions, subregion_traces)
    node_traces = _sum_subregion_traces_per_demand_node(
        subregion_traces, demand_subregions
    )
    demand_traces = {}
    for node, trace in node_traces.groupby("geo_id"):
        demand_traces[node] = trace[["datetime", "value"]].reset_index(drop=True)
    return demand_traces


def _log_subregions_without_traces(
    demand_subregions: pd.DataFrame, subregion_traces: pd.DataFrame
) -> None:
    """Warns about the sub-regions whose demand trace was not found.

    I/O Example:
        demand_subregions:
            subregion_id  geo_id
            SQ            QLD
            CQ            QLD

        subregion_traces:
            subregion  datetime             value
            SQ         2024-07-01 00:30:00  3500.0

        logs:
            ISP sub-regions with no demand trace data (their demand is left
            out of the model): ['CQ']
    """
    has_trace = demand_subregions["subregion_id"].isin(subregion_traces["subregion"])
    without_trace = sorted(demand_subregions.loc[~has_trace, "subregion_id"])
    if without_trace:
        logging.warning(
            "ISP sub-regions with no demand trace data (their demand is left "
            f"out of the model): {without_trace}"
        )


def _sum_subregion_traces_per_demand_node(
    subregion_traces: pd.DataFrame, demand_subregions: pd.DataFrame
) -> pd.DataFrame:
    """Adds up the traces of the sub-regions placed at each demand node,
    flooring each node's total at zero.

    I/O Example:
        subregion_traces:
            subregion  datetime             value
            CNSW       2024-07-01 00:30:00  5000.0
            NNSW       2024-07-01 00:30:00  500.0
            SQ         2024-07-01 00:30:00  -20.0
            CNSW       2024-07-01 01:00:00  4800.0
            NNSW       2024-07-01 01:00:00  -100.0
            SQ         2024-07-01 01:00:00  3400.0

        demand_subregions:
            subregion_id  geo_id
            CNSW          NSW
            NNSW          NSW
            SQ            QLD

        returns:
            geo_id  datetime             value
            NSW     2024-07-01 00:30:00  5500.0
            NSW     2024-07-01 01:00:00  4700.0  # NNSW's negative value nets off CNSW
            QLD     2024-07-01 00:30:00  0.0     # a negative total is floored at zero
            QLD     2024-07-01 01:00:00  3400.0
    """
    traces = subregion_traces.merge(
        demand_subregions, left_on="subregion", right_on="subregion_id"
    )
    node_traces = traces.groupby(["geo_id", "datetime"], as_index=False)["value"].sum()
    node_traces["value"] = node_traces["value"].clip(lower=0.0)
    # datetime in nanoseconds required by PyPSA
    node_traces["datetime"] = node_traces["datetime"].astype("datetime64[ns]")
    return node_traces
