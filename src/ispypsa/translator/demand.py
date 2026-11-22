import logging
from pathlib import Path
from typing import Literal

import pandas as pd
from isp_trace_parser import get_data


def _translate_demand_nodes(
    network_geography_hierarchy: pd.DataFrame,
    network_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Lists the demand nodes: the buses that carry demand.

    Demand is published as one trace per ISP sub-region, whatever regional
    granularity the model is built at. The model may be built from the
    sub-regions, from the NEM regions, or from a single NEM-wide geography, so
    each sub-region's demand has to be placed at a geography the model has.

    network_geography lists the geographies the model is built from, each of
    which becomes a bus. network_geography_hierarchy says how the ISP's
    geographies nest: each REZ inside a sub-region, each sub-region inside a
    NEM region, and each NEM region inside the NEM. Its subregion_id,
    region_id and nem_id columns name, for each geography, the sub-region, NEM
    region and NEM it is or sits inside. A sub-region is a row whose geo_id
    equals its subregion_id.

    A sub-region's demand is placed at the model geography that is the
    sub-region itself or contains it. Where that is depends on the granularity
    network_geography was templated at:

    - "sub_regions": the sub-region itself, e.g. CNSW at CNSW.
    - "nem_regions": the NEM region containing it, e.g. CNSW at NSW.
    - "single_region": the NEM, e.g. CNSW at NEM.

    A sub-region with no such model geography, say CNSW when none of CNSW, NSW
    or NEM is in network_geography, has no demand in the model. No sub-region
    sits inside a REZ, so a REZ never carries demand.

    Returns a table with one row per model geography that has a sub-region's
    demand placed at it. Its one column, name, is the geo_id of that geography,
    which is also the name of its bus.

    I/O Example:
        network_geography_hierarchy:
            geo_id  subregion_id  region_id  nem_id
            NSW                   NSW        NEM
            QLD                   QLD        NEM
            CNSW    CNSW          NSW        NEM
            NNSW    NNSW          NSW        NEM     # shares the NSW bus with CNSW
            SQ      SQ            QLD        NEM
            N3      CNSW          NSW        NEM

        network_geography:
            geo_id  geo_type
            NSW     region
            QLD     region
            N3      rez                # a REZ: no demand

        returns:
            name
            NSW
            QLD
    """
    demand_subregions = _place_subregions_at_model_geographies(
        network_geography_hierarchy, network_geography
    )
    demand_nodes = demand_subregions[["geo_id"]].drop_duplicates()
    demand_nodes = demand_nodes.rename(columns={"geo_id": "name"})
    return demand_nodes.reset_index(drop=True)


def _create_demand_node_traces(
    network_geography_hierarchy: pd.DataFrame,
    network_geography: pd.DataFrame,
    trace_data_path: Path | str,
    scenario: str,
    reference_year_mapping: dict[int, int],
    year_type: Literal["fy", "calendar"],
) -> dict[str, pd.DataFrame]:
    """Builds the demand trace of each demand node.

    A demand node's trace is the sum of the traces of the sub-regions whose
    demand is placed at it. _translate_demand_nodes describes how each
    sub-region is placed, using network_geography_hierarchy and
    network_geography.

    The sub-region traces are read from the "demand" subdirectory of
    trace_data_path, a directory of trace data parsed by isp-trace-parser, for
    the given ISP scenario. reference_year_mapping names the model years to
    read and, for each, the reference year whose trace is used for it.

    Returns a dictionary with one trace per demand node, keyed by the node's
    name. Each trace is a table with one row per half hour of the model years:

    - datetime: the end of the half hour.
    - value: the node's demand in MW, never negative.

    A sub-region's trace can dip below zero. The traces are added together
    first and the node's total is then floored at zero, so one sub-region's
    negative demand still offsets its neighbours' at the same node.

    A sub-region with no trace data is named in a warning and adds nothing to
    its node, and a node whose sub-regions all lack trace data has no entry in
    the dictionary.

    I/O Example:
        network_geography_hierarchy:
            geo_id  subregion_id  region_id  nem_id
            CNSW    CNSW          NSW        NEM
            NNSW    NNSW          NSW        NEM
            SQ      SQ            QLD        NEM
            CQ      CQ            QLD        NEM     # suppose CQ's trace is missing: logged, adds nothing

        network_geography:
            geo_id  geo_type
            NSW     region
            QLD     region

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
    demand_subregions = _place_subregions_at_model_geographies(
        network_geography_hierarchy, network_geography
    )
    # The demand modelled is the 50% probability of exceedance (POE50) forecast
    # of operational sent-out demand (OPSO_MODELLING).
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


def _place_subregions_at_model_geographies(
    network_geography_hierarchy: pd.DataFrame, network_geography: pd.DataFrame
) -> pd.DataFrame:
    """Places each ISP sub-region at the model geography that is the
    sub-region itself or contains it.

    I/O Example:
        network_geography_hierarchy:
            geo_id  subregion_id  region_id  nem_id
            NSW                   NSW        NEM
            CNSW    CNSW          NSW        NEM
            NNSW    NNSW          NSW        NEM
            SQ      SQ            QLD        NEM
            SEV     SEV           VIC        NEM
            TAS     TAS           TAS        NEM
            N3      CNSW          NSW        NEM

        network_geography:
            geo_id  geo_type
            NSW     region
            SQ      subregion
            TAS     region
            N3      rez

        returns:
            subregion_id  geo_id
            CNSW          NSW       # CNSW is not in the model, but NSW contains it
            NNSW          NSW
            SQ            SQ        # SQ is in the model itself
            TAS           TAS       # both its subregion_id and region_id match: one row
                                    # SEV has no row: SEV, VIC and NEM are not in the model
    """
    is_subregion = (
        network_geography_hierarchy["geo_id"]
        == network_geography_hierarchy["subregion_id"]
    )
    # One row per sub-region and geography it is or sits inside.
    containing = network_geography_hierarchy[is_subregion].melt(
        id_vars="geo_id",
        value_vars=["subregion_id", "region_id", "nem_id"],
        value_name="containing_geo_id",
    )
    in_model = containing["containing_geo_id"].isin(network_geography["geo_id"])
    demand_subregions = pd.DataFrame(
        {
            "subregion_id": containing.loc[in_model, "geo_id"],
            "geo_id": containing.loc[in_model, "containing_geo_id"],
        }
    )
    # TAS is both a sub-region and a NEM region, so it matches itself twice.
    return demand_subregions.drop_duplicates().reset_index(drop=True)


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
    """Sums the traces of the sub-regions placed at each demand node,
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
