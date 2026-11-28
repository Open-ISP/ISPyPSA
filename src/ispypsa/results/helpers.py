from typing import Literal

import pandas as pd

from ispypsa.feature_flags import FEATURE_FLAGS


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

    Under the new table format the mapping has a node column, and each node
    maps to its value in the geography_level column. A node that is blank at
    that level is left out, so looking it up gives NaN, as for a node the
    mapping doesn't list: a flow between it and a mapped node counts as
    crossing the mapped node's boundary, and anything totalled at it alone is
    dropped. The old-format mapping has no node column, so a node is matched
    by its id in the sub-region and REZ columns.

    I/O Example (new table format, sub_regions granularity):
        regions_and_zones_mapping:
            node  nem_region_id  isp_sub_region_id  rez_id
            CNSW  NSW            CNSW
            N3    NSW            CNSW               N3

        geography_level = "nem_region_id" returns:
            {"CNSW": "NSW", "N3": "NSW"}

        geography_level = "isp_sub_region_id" returns:
            {"CNSW": "CNSW", "N3": "CNSW"}

        geography_level = "rez_id" returns:
            {"N3": "N3"}    # CNSW is blank at this level
    """
    # FEATURE_FLAG_CLEANUP[use_new_table_format]: keep the first branch,
    # dedented, and delete the legacy body below it.
    if FEATURE_FLAGS["use_new_table_format"]:
        at_level = regions_and_zones_mapping.dropna(subset=[geography_level])
        return dict(zip(at_level["node"], at_level[geography_level]))

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
