import logging
from typing import Set, Tuple

import pandas as pd


def _filter_template(
    template: dict[str, pd.DataFrame],
    nem_regions: list[str] = None,
    isp_sub_regions: list[str] = None,
) -> dict[str, pd.DataFrame]:
    """Filter template tables to only include data for specified NEM regions or ISP sub-regions.

    This function filters all tables in the template to only include data relevant to the
    specified regions. It handles both direct filtering (for tables with region columns) and
    indirect filtering (for tables that reference other filtered entities).

    Args:
        template: Dictionary of template dataframes to filter
        nem_regions: List of NEM region IDs (e.g., ['NSW', 'VIC']) to filter by.
                    Cannot be specified together with isp_sub_regions.
        isp_sub_regions: List of ISP sub-region IDs (e.g., ['CNSW', 'VIC', 'TAS']) to filter by.
                        Cannot be specified together with nem_regions.

    Returns:
        Dictionary of filtered dataframes

    Raises:
        ValueError: If both or neither of nem_regions and isp_sub_regions are provided
    """
    # Validate inputs
    if (nem_regions is None) == (isp_sub_regions is None):
        raise ValueError(
            "Exactly one of nem_regions or isp_sub_regions must be provided"
        )

    # Get the sub_regions table for mapping
    sub_regions_df = template.get("sub_regions", pd.DataFrame())
    if sub_regions_df.empty:
        raise ValueError("No sub_regions found in template.")

    # Determine which regions to select
    selected_sub_regions, selected_nem_regions = _determine_selected_regions(
        sub_regions_df, nem_regions, isp_sub_regions
    )

    if not selected_sub_regions:
        raise ValueError("No sub_regions after filtering.")

    # Initialize result dictionary
    filtered = {}

    # Filter basic region tables
    region_tables = _filter_region_tables(
        template, selected_sub_regions, selected_nem_regions
    )
    filtered.update(region_tables)

    # Extract selected entities for subsequent filtering
    selected_rezs = _get_selected_rezs(filtered)
    selected_flow_paths = _get_selected_flow_paths(filtered)

    # Filter generators and get their names
    generator_tables, all_selected_generators = _filter_generators(
        template, selected_sub_regions
    )
    filtered.update(generator_tables)

    # Filter generator-dependent tables
    generator_dependent = _filter_generator_dependent_tables(
        template, all_selected_generators
    )
    filtered.update(generator_dependent)

    # Infer link names
    selected_link_names = _infer_link_names(
        filtered.get("flow_paths", pd.DataFrame()),
        filtered.get("renewable_energy_zones", pd.DataFrame()),
    )

    # Filter custom constraints
    custom_constraints, remaining_constraint_ids = _filter_custom_constraints(
        template, all_selected_generators, selected_link_names
    )
    filtered.update(custom_constraints)

    # Filter expansion costs
    expansion_costs = _filter_expansion_costs(
        template, selected_flow_paths, selected_rezs, remaining_constraint_ids
    )
    filtered.update(expansion_costs)

    # Filter policy tables
    policy_tables, selected_policy_ids = _filter_policy_tables(
        template, selected_nem_regions
    )
    filtered.update(policy_tables)

    # Filter other miscellaneous tables
    other_tables = _filter_other_tables(template, selected_nem_regions)
    filtered.update(other_tables)

    # Copy over tables that don't need filtering
    tables_no_filtering = [
        "build_costs",
        "biomass_prices",
        "biomethane_prices",
        "gas_prices",
        "gpg_emissions_reduction_biomethane",
        "hydrogen_prices",
        "gpg_emissions_reduction_h2",
        "new_entrant_build_costs",
        "new_entrant_wind_and_solar_connection_costs",
        "powering_australia_plan",
        "full_outage_forecasts",
        "liquid_fuel_prices",
        "partial_outage_forecasts",
    ]

    for table_name in tables_no_filtering:
        if table_name in template:
            filtered[table_name] = template[table_name].copy()

    # Check if all tables have been handled
    missing_tables = set(template.keys()) - set(filtered.keys())
    if missing_tables:
        raise ValueError(
            f"The following tables have no known filtering method: {sorted(missing_tables)}. "
            "All tables must be explicitly handled by the filtering logic."
        )

    return filtered


def _determine_selected_regions(
    sub_regions_df: pd.DataFrame,
    nem_regions: list[str] = None,
    isp_sub_regions: list[str] = None,
) -> Tuple[list[str], list[str]]:
    """Determine which sub-regions and NEM regions to keep based on input.

    Returns:
        Tuple of (selected_sub_regions, selected_nem_regions)
    """
    if nem_regions:
        # Convert NEM regions to sub-regions
        valid_nem_regions = sub_regions_df["nem_region_id"].unique()
        invalid_regions = set(nem_regions) - set(valid_nem_regions)
        for region in invalid_regions:
            logging.warning(f"NEM region '{region}' not found")

        # Get all sub-regions for valid NEM regions
        mask = sub_regions_df["nem_region_id"].isin(nem_regions)
        selected_sub_regions = (
            sub_regions_df[mask]["isp_sub_region_id"].unique().tolist()
        )
    else:
        # Use ISP sub-regions directly
        valid_sub_regions = sub_regions_df["isp_sub_region_id"].unique()
        invalid_regions = set(isp_sub_regions) - set(valid_sub_regions)
        for region in invalid_regions:
            logging.warning(f"ISP sub-region '{region}' not found")

        # Filter to valid sub-regions
        selected_sub_regions = [r for r in isp_sub_regions if r in valid_sub_regions]

    if not selected_sub_regions:
        logging.warning("No valid regions found for filtering")
        return [], []

    # Get corresponding NEM regions
    selected_nem_regions = (
        sub_regions_df[sub_regions_df["isp_sub_region_id"].isin(selected_sub_regions)][
            "nem_region_id"
        ]
        .unique()
        .tolist()
    )

    return selected_sub_regions, selected_nem_regions


def _filter_region_tables(
    template: dict[str, pd.DataFrame],
    selected_sub_regions: list[str],
    selected_nem_regions: list[str],
) -> dict[str, pd.DataFrame]:
    """Filter basic region-related tables."""
    filtered = {}

    # Filter sub_regions table
    if "sub_regions" in template:
        filtered["sub_regions"] = template["sub_regions"][
            template["sub_regions"]["isp_sub_region_id"].isin(selected_sub_regions)
        ].copy()

    # Filter nem_regions table if it exists
    if "nem_regions" in template:
        filtered["nem_regions"] = template["nem_regions"][
            template["nem_regions"]["nem_region_id"].isin(selected_nem_regions)
        ].copy()

    # Filter renewable_energy_zones
    if "renewable_energy_zones" in template:
        filtered["renewable_energy_zones"] = template["renewable_energy_zones"][
            template["renewable_energy_zones"]["isp_sub_region_id"].isin(
                selected_sub_regions
            )
        ].copy()

    # Filter flow_paths - only keep paths where both nodes are in selected regions
    if "flow_paths" in template:
        all_selected_nodes = selected_sub_regions + selected_nem_regions
        filtered["flow_paths"] = template["flow_paths"][
            (template["flow_paths"]["node_from"].isin(all_selected_nodes))
            & (template["flow_paths"]["node_to"].isin(all_selected_nodes))
        ].copy()

    return filtered


def _filter_generators(
    template: dict[str, pd.DataFrame],
    selected_sub_regions: list[str],
) -> Tuple[dict[str, pd.DataFrame], Set[str]]:
    """Filter generator tables and return filtered tables and generator names."""
    filtered = {}
    all_selected_generators = set()

    # Filter ecaa_generators
    if "ecaa_generators" in template:
        filtered["ecaa_generators"] = template["ecaa_generators"][
            template["ecaa_generators"]["sub_region_id"].isin(selected_sub_regions)
        ].copy()
        all_selected_generators.update(
            filtered["ecaa_generators"]["generator"].unique()
        )

    # Filter new_entrant_generators
    if "new_entrant_generators" in template:
        filtered["new_entrant_generators"] = template["new_entrant_generators"][
            template["new_entrant_generators"]["sub_region_id"].isin(
                selected_sub_regions
            )
        ].copy()
        all_selected_generators.update(
            filtered["new_entrant_generators"]["generator"].unique()
        )

    return filtered, all_selected_generators


def _filter_generator_dependent_tables(
    template: dict[str, pd.DataFrame],
    all_selected_generators: Set[str],
) -> dict[str, pd.DataFrame]:
    """Filter tables that depend on generator names."""
    filtered = {}

    # Filter tables that reference generators
    generator_based_tables = [
        "closure_years",
        "coal_prices",
        "seasonal_ratings",
    ]

    for table_name in generator_based_tables:
        if table_name in template:
            filtered[table_name] = template[table_name][
                template[table_name]["generator"].isin(all_selected_generators)
            ].copy()

    return filtered


def _get_selected_rezs(filtered_tables: dict[str, pd.DataFrame]) -> Set[str]:
    """Extract REZ IDs from filtered tables."""
    if (
        "renewable_energy_zones" in filtered_tables
        and not filtered_tables["renewable_energy_zones"].empty
    ):
        return set(filtered_tables["renewable_energy_zones"]["rez_id"].unique())
    return set()


def _get_selected_flow_paths(filtered_tables: dict[str, pd.DataFrame]) -> Set[str]:
    """Extract flow path names from filtered tables."""
    if "flow_paths" in filtered_tables and not filtered_tables["flow_paths"].empty:
        return set(filtered_tables["flow_paths"]["flow_path"].unique())
    return set()


def _infer_link_names(
    filtered_flow_paths: pd.DataFrame,
    filtered_rez: pd.DataFrame,
) -> Set[str]:
    """Infer link names from flow paths and REZs."""
    selected_link_names = set()

    # Links from flow_paths (format: node_from-node_to)
    if not filtered_flow_paths.empty:
        flow_links = (
            filtered_flow_paths["node_from"] + "-" + filtered_flow_paths["node_to"]
        )
        selected_link_names.update(flow_links.tolist())

    # Links from renewable_energy_zones (format: REZ_ID-SUBREGION)
    if not filtered_rez.empty:
        rez_links = filtered_rez["rez_id"] + "-" + filtered_rez["isp_sub_region_id"]
        selected_link_names.update(rez_links.tolist())

    return selected_link_names


def _filter_custom_constraints(
    template: dict[str, pd.DataFrame],
    all_selected_generators: Set[str],
    selected_link_names: Set[str],
) -> Tuple[dict[str, pd.DataFrame], Set[str]]:
    """Filter custom constraints tables."""
    filtered = {}
    remaining_constraint_ids = set()

    if "custom_constraints_lhs" in template:
        df = template["custom_constraints_lhs"].copy()

        # Vectorized validation of terms
        # Create masks for each term type
        generator_mask = (df["term_type"] == "generator_output") & df["term_id"].isin(
            all_selected_generators
        )
        storage_mask = (df["term_type"] == "storage_output") & df["term_id"].isin(
            all_selected_generators
        )
        link_mask = (df["term_type"] == "link_flow") & df["term_id"].isin(
            selected_link_names
        )

        # Check for unknown term types
        known_types = {"generator_output", "storage_output", "link_flow"}
        unknown_types = set(df["term_type"].unique()) - known_types
        if unknown_types:
            raise ValueError(f"Cannot filter unknown term types: {unknown_types}")

        # A term is valid if it matches one of the masks
        valid_terms = generator_mask | storage_mask | link_mask

        # Group by constraint_id and check if all terms in each constraint are valid
        constraint_validity = df.groupby("constraint_id")["term_id"].transform(
            lambda x: valid_terms.loc[x.index].all()
        )

        # Get unique valid constraint IDs
        valid_constraints = df[constraint_validity]["constraint_id"].unique().tolist()

        # Filter LHS to only include valid constraints
        filtered["custom_constraints_lhs"] = df[
            df["constraint_id"].isin(valid_constraints)
        ].copy()

        # Filter RHS to only include constraints that still have LHS terms
        if "custom_constraints_rhs" in template:
            filtered["custom_constraints_rhs"] = template["custom_constraints_rhs"][
                template["custom_constraints_rhs"]["constraint_id"].isin(
                    valid_constraints
                )
            ].copy()

        remaining_constraint_ids = set(valid_constraints)

    return filtered, remaining_constraint_ids


def _filter_expansion_costs(
    template: dict[str, pd.DataFrame],
    selected_flow_paths: Set[str],
    selected_rezs: Set[str],
    remaining_constraint_ids: Set[str],
) -> dict[str, pd.DataFrame]:
    """Filter expansion cost tables."""
    filtered = {}

    # Filter flow path expansion costs
    if "flow_path_expansion_costs" in template and selected_flow_paths:
        filtered["flow_path_expansion_costs"] = template["flow_path_expansion_costs"][
            template["flow_path_expansion_costs"]["flow_path"].isin(selected_flow_paths)
        ].copy()

    # Filter REZ transmission expansion costs
    if "rez_transmission_expansion_costs" in template:
        # Filter on rez_constraint_id column for all REZs and constraint IDs
        rez_constraint_ids_to_keep = selected_rezs | remaining_constraint_ids

        filtered["rez_transmission_expansion_costs"] = template[
            "rez_transmission_expansion_costs"
        ][
            template["rez_transmission_expansion_costs"]["rez_constraint_id"].isin(
                rez_constraint_ids_to_keep
            )
        ].copy()

    return filtered


def _filter_policy_tables(
    template: dict[str, pd.DataFrame],
    selected_nem_regions: list[str],
) -> Tuple[dict[str, pd.DataFrame], Set[str]]:
    """Filter policy-related tables."""
    filtered = {}
    selected_policy_ids = set()

    # Filter policy target tables
    policy_tables = [
        "renewable_generation_targets",
        "renewable_share_targets",
        "technology_capacity_targets",
    ]

    for table_name in policy_tables:
        if table_name in template:
            # Keep targets for selected regions and NEM-wide targets
            filtered[table_name] = template[table_name][
                (template[table_name]["region_id"].isin(selected_nem_regions))
                | (template[table_name]["region_id"] == "NEM")
            ].copy()

            # Collect policy_ids
            selected_policy_ids.update(filtered[table_name]["policy_id"].unique())

    # Filter policy_generator_types based on selected policy_ids
    if "policy_generator_types" in template:
        filtered["policy_generator_types"] = template["policy_generator_types"][
            template["policy_generator_types"]["policy_id"].isin(selected_policy_ids)
        ].copy()

    return filtered, selected_policy_ids


def _filter_other_tables(
    template: dict[str, pd.DataFrame],
    selected_nem_regions: list[str],
) -> dict[str, pd.DataFrame]:
    """Filter miscellaneous tables."""
    filtered = {}

    # Filter new_entrant_non_vre_connection_costs by Region column
    if "new_entrant_non_vre_connection_costs" in template:
        filtered["new_entrant_non_vre_connection_costs"] = template[
            "new_entrant_non_vre_connection_costs"
        ][
            template["new_entrant_non_vre_connection_costs"]["Region"].isin(
                selected_nem_regions
            )
        ].copy()

    return filtered
