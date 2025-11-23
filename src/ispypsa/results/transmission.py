import pandas as pd
import pypsa

from ispypsa.results.helpers import _build_node_to_geography_mapping


def extract_transmission_expansion_results(network: pypsa.Network) -> pd.DataFrame:
    """Extract transmission expansion results from PyPSA network and rename columns according to ISP conventions.

    Existing capacity is reported with a build year of 0.

    Examples:

    >>> extract_transmission_expansion_results(network)
       isp_name node_from node_to investment_period  forward_capacity_mw reverse_capacity_mw
    0       A-B         A       B                 0                  100                 100
    1       A-B         A       B              2026                  300                 300
    2       A-B         A       B              2027                  400                 400

    Args:
        network: PyPSA network object

    Returns:
        pd.DataFrame: Transmission expansion results in ISP format. Columns: isp_name, node_from, node_to,
            investment_period, forward_capacity_mw, and reverse_capacity_mw.

    """

    results = network.links

    columns_to_rename = {
        "bus0": "node_from",
        "bus1": "node_to",
        "build_year": "investment_period",
    }

    results = results.rename(columns=columns_to_rename)

    results["forward_capacity_mw"] = results["p_nom_opt"]
    results["reverse_capacity_mw"] = results["p_nom_opt"] * results["p_min_pu"]

    cols_to_keep = [
        "isp_name",
        "isp_type",
        "node_from",
        "node_to",
        "investment_period",
        "forward_capacity_mw",
        "reverse_capacity_mw",
    ]

    results = results.loc[:, cols_to_keep].reset_index(drop=True)

    cumsum_cols = ["forward_capacity_mw", "reverse_capacity_mw"]
    results = results.sort_values("investment_period")
    results[cumsum_cols] = results.groupby("isp_name")[cumsum_cols].cumsum()

    # Make sure each transmission element has value for each year.
    investment_periods = sorted(results["investment_period"].unique())
    isp_names = sorted(results["isp_name"].unique())
    complete_index = pd.MultiIndex.from_product(
        [isp_names, investment_periods], names=["isp_name", "investment_period"]
    )
    results = results.set_index(["isp_name", "investment_period"])
    results = results.reindex(complete_index)
    results = results.groupby(level="isp_name").ffill()

    # Backfill static attributes for years before the first investment
    # Note: We use bfill() to propagate attributes like node names and ISP type
    # backwards to years before the link is built (where they are currently NaN).
    static_cols = ["node_from", "node_to", "isp_type"]
    results[static_cols] = results.groupby(level="isp_name")[static_cols].bfill()

    # Fill remaining capacity NaNs (years before first investment) with 0
    results[cumsum_cols] = results[cumsum_cols].fillna(0)

    results = results.reset_index()

    return results


def _extract_raw_link_flows(network: pypsa.Network) -> pd.DataFrame:
    """Extract raw link flows from PyPSA network (internal helper function).

    Returns flow data at the individual link level before any aggregation.

    Args:
        network: PyPSA network with solved optimization results

    Returns:
        DataFrame with columns: Link, investment_period, timestep, flow_mw, bus0, bus1, isp_name
    """
    # Get link static data
    links = network.links.loc[:, ["bus0", "bus1", "isp_name"]].reset_index()

    # Get flow time series (p0 = flow from bus0 to bus1)
    flow_t = network.links_t.p0.reset_index().rename(
        columns={"period": "investment_period"}
    )

    # Reshape to long format
    flow_long = flow_t.melt(
        id_vars=["investment_period", "timestep"],
        value_name="flow_mw",
        var_name="Link",
    )

    # Merge with link static data
    flow_long = flow_long.merge(links, on="Link", how="left")

    return flow_long


def extract_transmission_flows(network: pypsa.Network) -> pd.DataFrame:
    """Extract transmission flows aggregated by ISP name.

    Args:
        network: PyPSA network with solved optimization results

    Returns:
        DataFrame with columns: isp_name, investment_period, timestep, flow
    """

    link_flows = _extract_raw_link_flows(network)

    # Aggregate by isp_name
    flow_agg = (
        link_flows.groupby(
            ["isp_name", "bus0", "bus1", "investment_period", "timestep"]
        )
        .agg({"flow_mw": "sum"})
        .reset_index()
        .rename(columns={"bus0": "from_node", "bus1": "to_node"})
    )

    return flow_agg


def _calculate_transmission_flows_by_geography(
    flow_long: pd.DataFrame,
    node_to_geography: dict[str, str],
    geography_column_name: str,
) -> pd.DataFrame:
    """Generic helper to calculate imports/exports at any geographic aggregation level.

    Filters to only inter-geography flows (between different geographic units) and
    calculates imports/exports for each geographic unit.

    Args:
        flow_long: Raw link flows with columns: Link, investment_period, timestep, flow_mw, bus0, bus1
        node_to_geography: Mapping from node name to geographic unit ID
        geography_column_name: Name for the geography column in output

    Returns:
        DataFrame with columns: {geography_column_name}, investment_period, timestep,
            imports_mw, exports_mw, net_imports_mw
    """
    # Map nodes to geographic units
    flow_long = flow_long.copy()
    flow_long["from_node_geo"] = flow_long["from_node"].map(node_to_geography)
    flow_long["to_node_geo"] = flow_long["to_node"].map(node_to_geography)

    # Filter to only inter-geography flows (different geographic units)
    # Also filter out flows where either end doesn't map to this geography level
    inter_geo_flows = flow_long[
        (flow_long["from_node_geo"] != flow_long["to_node_geo"])
    ].copy()

    if inter_geo_flows.empty:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(
            columns=[
                geography_column_name,
                "investment_period",
                "timestep",
                "imports_mw",
                "exports_mw",
                "net_imports_mw",
            ]
        )

    # Calculate imports and exports from each geographic unit's perspective
    # For bus0 (origin): positive flow = export, negative flow = import
    bus0_flows = inter_geo_flows.copy()
    bus0_flows[geography_column_name] = bus0_flows["from_node_geo"]
    bus0_flows["exports_mw"] = bus0_flows["flow_mw"].clip(lower=0)
    bus0_flows["imports_mw"] = (-bus0_flows["flow_mw"]).clip(lower=0)

    # For bus1 (destination): positive flow = import, negative flow = export
    bus1_flows = inter_geo_flows.copy()
    bus1_flows[geography_column_name] = bus1_flows["to_node_geo"]
    bus1_flows["imports_mw"] = bus1_flows["flow_mw"].clip(lower=0)
    bus1_flows["exports_mw"] = (-bus1_flows["flow_mw"]).clip(lower=0)

    # Combine both perspectives
    all_flows = pd.concat([bus0_flows, bus1_flows], ignore_index=True)

    # Aggregate by geography, period, and timestep
    result = (
        all_flows.groupby([geography_column_name, "investment_period", "timestep"])
        .agg({"imports_mw": "sum", "exports_mw": "sum"})
        .reset_index()
    )

    # Calculate net imports
    result["net_imports_mw"] = result["imports_mw"] - result["exports_mw"]

    return result


def extract_rez_transmission_flows(
    link_flows: pd.DataFrame, regions_and_zones_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Extract inter-REZ transmission flows.

    Only counts flows between different REZs.

    Args:
        link_flows: Raw link flows from _extract_raw_link_flows()
        regions_and_zones_mapping: Mapping table with nem_region_id, isp_sub_region_id,
        and rez_id columns

    Returns:
        DataFrame with columns: rez_id, investment_period, timestep,
            imports_mw, exports_mw, net_imports_mw
    """
    node_to_rez = _build_node_to_geography_mapping(regions_and_zones_mapping, "rez_id")
    return _calculate_transmission_flows_by_geography(link_flows, node_to_rez, "rez_id")


def extract_isp_sub_region_transmission_flows(
    link_flows: pd.DataFrame, regions_and_zones_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Extract inter-sub-region transmission flows.

    Only counts flows between different ISP sub-regions.
    Intra-sub-region flows (e.g., REZ to sub-region within same sub-region) are excluded.

    Args:
        link_flows: Raw link flows from _extract_raw_link_flows()
        regions_and_zones_mapping: Mapping table with nem_region_id, isp_sub_region_id,
        and rez_id columns

    Returns:
        DataFrame with columns: isp_sub_region_id, investment_period, timestep,
            imports_mw, exports_mw, net_imports_mw
    """
    node_to_subregion = _build_node_to_geography_mapping(
        regions_and_zones_mapping, "isp_sub_region_id"
    )
    return _calculate_transmission_flows_by_geography(
        link_flows, node_to_subregion, "isp_sub_region_id"
    )


def extract_nem_region_transmission_flows(
    link_flows: pd.DataFrame, regions_and_zones_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Extract inter-regional transmission flows.

    Only counts flows between different NEM regions.
    Intra-regional flows (e.g., REZ to sub-region within same region) are excluded.

    Args:
        link_flows: Raw link flows from _extract_raw_link_flows()
        regions_and_zones_mapping: Mapping table with nem_region_id, isp_sub_region_id,
        and rez_id columns

    Returns:
        DataFrame with columns: nem_region_id, investment_period, timestep,
            imports_mw, exports_mw, net_imports_mw
    """
    node_to_region = _build_node_to_geography_mapping(
        regions_and_zones_mapping, "nem_region_id"
    )
    return _calculate_transmission_flows_by_geography(
        link_flows, node_to_region, "nem_region_id"
    )
