from typing import Literal

import pandas as pd


def _build_node_to_geography_mapping(
    regions_and_zones_mapping: pd.DataFrame,
    geography_level: Literal["nem_region_id", "isp_sub_region_id", "rez_id"],
) -> dict[str, str]:
    """Build mapping from nodes to geographic units at specified level.

    Args:
        regions_and_zones_mapping: Mapping table with nem_region_id, isp_sub_region_id, rez_id
        geography_level: Level of geography to map to (e.g. "nem_region_id", "isp_sub_region_id", "rez_id")

    Returns:
        Dictionary mapping node names to geographic unit IDs
    """
    if geography_level == "nem_region_id":
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

    elif geography_level == "isp_sub_region_id":
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

    elif geography_level == "rez_id":
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
