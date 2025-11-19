import pandas as pd
import pypsa


def extract_transmission_expansion_results(network: pypsa.Network) -> pd.DataFrame:
    """Extract transmission expansion results from PyPSA network and rename columns according to ISP conventions.

    Existing capacity is reported with a build year of 0.

    Examples:

    >>> extract_transmission_expansion_results(network)
    isp_name, node_from, node_to, build_year, forward_direction_nominal_capacity_mw, reverse_direction_nominal_capacity_mw
    A-B, A, B, 0, 100, 100
    A-B, A, B, 2026, 300, 300
    A-B, A, B, 2027, 400, 400

    Args:
        network: PyPSA network object

    Returns:
        pd.DataFrame: Transmission expansion results in ISP format. Columns: isp_name, node_from, node_to, build_year,
        forward_direction_nominal_capacity_mw, and reverse_direction_nominal_capacity_mw.

    """

    results = network.links

    # results = results[results["p_nom_opt"] > 0].copy()

    columns_to_rename = {
        "bus0": "node_from",
        "bus1": "node_to",
    }

    results = results.rename(columns=columns_to_rename)

    results["forward_direction_nominal_capacity_mw"] = results["p_nom_opt"]
    results["reverse_direction_nominal_capacity_mw"] = (
        results["p_nom_opt"] * results["p_min_pu"]
    )

    cols_to_keep = [
        "isp_name",
        "isp_type",
        "node_from",
        "node_to",
        "build_year",
        "forward_direction_nominal_capacity_mw",
        "reverse_direction_nominal_capacity_mw",
    ]

    results = results.loc[:, cols_to_keep].reset_index(drop=True)

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
    links = network.links[["bus0", "bus1", "isp_name"]].reset_index()

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


def extract_transmission_flows(link_flows: pd.DataFrame) -> pd.DataFrame:
    """Extract transmission flows aggregated by ISP name.

    Args:
        link_flows: Raw link flows from _extract_raw_link_flows()

    Returns:
        DataFrame with columns: isp_name, investment_period, timestep, flow
    """
    # Aggregate by isp_name
    flow_agg = (
        link_flows.groupby(["isp_name", "investment_period", "timestep"])
        .agg({"flow_mw": "sum"})
        .reset_index()
        .rename(columns={"flow_mw": "flow"})
    )

    return flow_agg


def _build_node_to_geography_mapping(
    regions_and_zones_mapping: pd.DataFrame, geography_level: str
) -> dict[str, str]:
    """Build mapping from nodes to geographic units at specified level.

    Args:
        regions_and_zones_mapping: Mapping table with nem_region_id, isp_sub_region_id, rez_id
        geography_level: One of 'rez', 'subregion', 'region'

    Returns:
        Dictionary mapping node names to geographic unit IDs
    """
    if geography_level == "region":
        # Map sub-regions to regions
        node_to_geo = dict(
            zip(
                regions_and_zones_mapping["isp_sub_region_id"],
                regions_and_zones_mapping["nem_region_id"],
            )
        )
        # Map REZs to regions
        if "rez_id" in regions_and_zones_mapping.columns:
            rez_to_geo = dict(
                zip(
                    regions_and_zones_mapping["rez_id"].dropna(),
                    regions_and_zones_mapping.loc[
                        regions_and_zones_mapping["rez_id"].notna(), "nem_region_id"
                    ],
                )
            )
            node_to_geo.update(rez_to_geo)

    elif geography_level == "subregion":
        # Map sub-regions to themselves
        node_to_geo = dict(
            zip(
                regions_and_zones_mapping["isp_sub_region_id"],
                regions_and_zones_mapping["isp_sub_region_id"],
            )
        )
        # Map REZs to their parent sub-regions
        if "rez_id" in regions_and_zones_mapping.columns:
            rez_to_geo = dict(
                zip(
                    regions_and_zones_mapping["rez_id"].dropna(),
                    regions_and_zones_mapping.loc[
                        regions_and_zones_mapping["rez_id"].notna(), "isp_sub_region_id"
                    ],
                )
            )
            node_to_geo.update(rez_to_geo)

    elif geography_level == "rez":
        # Only map REZs to themselves
        if "rez_id" in regions_and_zones_mapping.columns:
            node_to_geo = dict(
                zip(
                    regions_and_zones_mapping["rez_id"].dropna(),
                    regions_and_zones_mapping["rez_id"].dropna(),
                )
            )
        else:
            node_to_geo = {}

    else:
        raise ValueError(f"Unknown geography_level: {geography_level}")

    return node_to_geo


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
    flow_long["bus0_geo"] = flow_long["bus0"].map(node_to_geography)
    flow_long["bus1_geo"] = flow_long["bus1"].map(node_to_geography)

    # Filter to only inter-geography flows (different geographic units)
    # Also filter out flows where either end doesn't map to this geography level
    inter_geo_flows = flow_long[
        (flow_long["bus0_geo"].notna())
        & (flow_long["bus1_geo"].notna())
        & (flow_long["bus0_geo"] != flow_long["bus1_geo"])
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
    bus0_flows[geography_column_name] = bus0_flows["bus0_geo"]
    bus0_flows["exports_mw"] = bus0_flows["flow_mw"].clip(lower=0)
    bus0_flows["imports_mw"] = (-bus0_flows["flow_mw"]).clip(lower=0)

    # For bus1 (destination): positive flow = import, negative flow = export
    bus1_flows = inter_geo_flows.copy()
    bus1_flows[geography_column_name] = bus1_flows["bus1_geo"]
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
        regions_and_zones_mapping: Mapping table with rez_id column

    Returns:
        DataFrame with columns: rez_id, investment_period, timestep,
            imports_mw, exports_mw, net_imports_mw
    """
    node_to_rez = _build_node_to_geography_mapping(regions_and_zones_mapping, "rez")
    return _calculate_transmission_flows_by_geography(link_flows, node_to_rez, "rez_id")


def extract_isp_sub_region_transmission_flows(
    link_flows: pd.DataFrame, regions_and_zones_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Extract inter-sub-region transmission flows.

    Only counts flows between different ISP sub-regions.
    Intra-sub-region flows (e.g., REZ to sub-region within same sub-region) are excluded.

    Args:
        link_flows: Raw link flows from _extract_raw_link_flows()
        regions_and_zones_mapping: Mapping table with isp_sub_region_id column

    Returns:
        DataFrame with columns: isp_sub_region_id, investment_period, timestep,
            imports_mw, exports_mw, net_imports_mw
    """
    node_to_subregion = _build_node_to_geography_mapping(
        regions_and_zones_mapping, "subregion"
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
        regions_and_zones_mapping: Mapping table with nem_region_id column

    Returns:
        DataFrame with columns: nem_region_id, investment_period, timestep,
            imports_mw, exports_mw, net_imports_mw
    """
    node_to_region = _build_node_to_geography_mapping(
        regions_and_zones_mapping, "region"
    )
    return _calculate_transmission_flows_by_geography(
        link_flows, node_to_region, "nem_region_id"
    )
