import numpy as np
import pandas as pd

from ispypsa.config import (
    ModelConfig,
)
from ispypsa.translator.links import _translate_time_varying_expansion_costs
from ispypsa.translator.mappings import (
    _CUSTOM_CONSTRAINT_ATTRIBUTES,
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE,
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE,
)


def _translate_custom_constraints(
    config: ModelConfig,
    ispypsa_tables: dict[str, pd.DataFrame],
    links: pd.DataFrame,
):
    """Translate custom constraint tables into a PyPSA friendly format and define any
    endogenous custom constraints.

    Args:
        config: `ispypsa.config.ModelConfig` object
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            The relevant tables for this function are:
                - custom_constraints_rhs
                - custom_constraints_lhs
        links: pd.DataFrame specifying the Link components to be used in the PyPSA model

    Returns: dictionary of dataframes in the `PyPSA` friendly format, with the relevant
        tables for custom constraints. New tables are:
        - custom_constraints_rhs (optional)
        - custom_constraints_lhs (optional)
        - custom_constraint_generators (optional)

    """

    pypsa_inputs = {}
    lhs = []
    rhs = []

    # Process manual custom constraints
    manual_lhs, manual_rhs, generators = _process_manual_custom_constraints(
        config, ispypsa_tables, links
    )

    _append_if_not_empty(lhs, manual_lhs)
    _append_if_not_empty(rhs, manual_rhs)

    if generators is not None:
        pypsa_inputs["custom_constraints_generators"] = generators

    # Process transmission expansion limit constraints
    transmission_expansion_limit_lhs, transmission_expansion_limit_rhs = (
        _create_expansion_limit_constraints(
            links,
            pypsa_inputs.get("custom_constraints_generators"),
            ispypsa_tables.get("flow_path_expansion_costs"),
            ispypsa_tables.get("rez_transmission_expansion_costs"),
        )
    )
    _append_if_not_empty(lhs, transmission_expansion_limit_lhs)
    _append_if_not_empty(rhs, transmission_expansion_limit_rhs)

    # Concatenate all constraints
    if lhs and rhs:
        pypsa_inputs["custom_constraints_lhs"] = pd.concat(lhs)
        pypsa_inputs["custom_constraints_rhs"] = pd.concat(rhs)

        # Check for duplicate constraint names in RHS
        _check_duplicate_constraint_names(pypsa_inputs["custom_constraints_rhs"])

        # Validate that all constraints have matching LHS and RHS
        _validate_lhs_rhs_constraints(
            pypsa_inputs["custom_constraints_lhs"],
            pypsa_inputs["custom_constraints_rhs"],
        )

    return pypsa_inputs


def _append_if_not_empty(
    target_list: list[pd.DataFrame],
    dataframe: pd.DataFrame,
) -> None:
    """Append dataframe to list if it's not empty.

    Args:
        target_list: List to append to
        dataframe: DataFrame to potentially append
    """
    if not dataframe.empty:
        target_list.append(dataframe)


def _process_manual_custom_constraints(
    config: ModelConfig,
    ispypsa_tables: dict[str, pd.DataFrame],
    links: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    """Process manually specified custom constraints

    Args:
        config: `ispypsa.config.ModelConfig` object
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
        links: pd.DataFrame specifying the Link components to be used in the PyPSA model

    Returns:
        Tuple of (lhs, rhs, generators) where:
        - lhs: DataFrame with left-hand side constraint definitions
        - rhs: DataFrame with right-hand side constraint values
        - generators: DataFrame with custom constraint generators (None if REZ expansion is disabled)
    """

    if not _has_manual_custom_constraints(ispypsa_tables):
        return pd.DataFrame(), pd.DataFrame(), None

    lhs = _translate_custom_constraint_lhs(ispypsa_tables["custom_constraints_lhs"])
    lhs = _expand_link_flow_lhs_terms(lhs, links)
    rhs = _translate_custom_constraint_rhs(ispypsa_tables["custom_constraints_rhs"])

    generators = None

    # Create dummy generators for REZ transmission expansion
    if config.network.rez_transmission_expansion:
        generators = _translate_custom_constraints_generators(
            list(rhs["constraint_name"]),
            ispypsa_tables["rez_transmission_expansion_costs"],
            config.wacc,
            config.network.annuitisation_lifetime,
            config.temporal.capacity_expansion.investment_periods,
            config.temporal.year_type,
        )

        # Add generator constraints to LHS
        custom_constraint_generators_lhs = (
            _translate_custom_constraint_generators_to_lhs(generators)
        )

        lhs = pd.concat([lhs, custom_constraint_generators_lhs])

    return lhs, rhs, generators


def _has_manual_custom_constraints(
    ispypsa_tables: dict[str, pd.DataFrame],
) -> bool:
    """Check if manual custom constraint tables are present and not empty.

    Args:
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.

    Returns:
        True if both custom_constraints_lhs and custom_constraints_rhs exist and are not empty
    """
    lhs_exists = (
        "custom_constraints_lhs" in ispypsa_tables
        and ispypsa_tables["custom_constraints_lhs"] is not None
        and not ispypsa_tables["custom_constraints_lhs"].empty
    )

    rhs_exists = (
        "custom_constraints_rhs" in ispypsa_tables
        and ispypsa_tables["custom_constraints_rhs"] is not None
        and not ispypsa_tables["custom_constraints_rhs"].empty
    )

    if lhs_exists and rhs_exists:
        return True
    elif not lhs_exists and not rhs_exists:
        return False
    else:
        raise ValueError("Incomplete manual custom constraints tables provided.")


def _translate_custom_constraints_generators(
    custom_constraints: list[str],
    rez_expansion_costs: pd.DataFrame,
    wacc: float,
    asset_lifetime: int,
    investment_periods: list[int],
    year_type: str,
) -> pd.DataFrame:
    """Translates REZ network expansion data into custom generators for modelling
    rez constraint relaxation.

    Args:
        custom_constraints: list of custom constraints to create expansion generators
            for.
        rez_expansion_costs: pd.DataFrame with time-varying expansion costs.
        wacc: float indicating the weighted average coast of capital.
        asset_lifetime: int specifying the nominal asset lifetime in years.
        investment_periods: list of investment years for time-varying costs.
        year_type: temporal configuration ("fy" or "calendar") for time-varying costs.

    Returns: pd.DataFrame
    """
    rez_expansion_costs = rez_expansion_costs[
        rez_expansion_costs["rez_constraint_id"].isin(custom_constraints)
    ]

    expansion_generators = _translate_time_varying_expansion_costs(
        expansion_costs=rez_expansion_costs,
        cost_column_suffix="_$/mw",
        investment_periods=investment_periods,
        year_type=year_type,
        wacc=wacc,
        asset_lifetime=asset_lifetime,
    )

    expansion_generators = expansion_generators.rename(
        columns={
            "rez_constraint_id": "constraint_name",
            "investment_year": "build_year",
        }
    )

    return _format_expansion_generators(expansion_generators)


def _format_expansion_generators(
    expansion_generators: pd.DataFrame,
) -> pd.DataFrame:
    """Format expansion generators with required fields for PyPSA.

    Args:
        expansion_generators: DataFrame with basic generator info

    Returns:
        Formatted DataFrame with all required PyPSA generator columns
    """
    expansion_generators["name"] = (
        expansion_generators["constraint_name"]
        + "_exp_"
        + expansion_generators["build_year"].astype(str)
    )
    expansion_generators["isp_name"] = expansion_generators["constraint_name"]
    expansion_generators["bus"] = "bus_for_custom_constraint_gens"
    expansion_generators["p_nom"] = 0.0
    expansion_generators["p_nom_extendable"] = True
    expansion_generators["lifetime"] = np.inf

    # Keep only the columns needed for PyPSA generators
    expansion_cols = [
        "name",
        "isp_name",
        "bus",
        "p_nom",
        "p_nom_extendable",
        "build_year",
        "lifetime",
        "capital_cost",
    ]
    return expansion_generators[expansion_cols].reset_index(drop=True)


def _translate_custom_constraint_rhs(
    custom_constraint_rhs_table: pd.DataFrame,
) -> pd.DataFrame:
    """Change RHS custom constraints to PyPSA style.

    Args:
        custom_constraint_rhs_table: pd.DataFrame in `ISPyPSA` detailing
            custom constraints rhs values.

    Returns: pd.DataFrame
    """
    return custom_constraint_rhs_table.rename(columns=_CUSTOM_CONSTRAINT_ATTRIBUTES)


def _translate_custom_constraint_lhs(
    custom_constraint_lhs_table: pd.DataFrame,
) -> pd.DataFrame:
    """Change RHS custom constraints to PyPSA style.

    Args:
        custom_constraint_lhs_table: list of pd.DataFrames in `ISPyPSA` detailing
            custom constraints lhs values.

    Returns: pd.DataFrame
    """
    custom_constraint_lhs_values = custom_constraint_lhs_table.rename(
        columns=_CUSTOM_CONSTRAINT_ATTRIBUTES
    )
    return _add_component_and_attribute_columns(custom_constraint_lhs_values)


def _add_component_and_attribute_columns(
    lhs_values: pd.DataFrame,
) -> pd.DataFrame:
    """Add component and attribute columns based on term_type and remove term_type.

    Args:
        lhs_values: DataFrame with term_type column

    Returns:
        DataFrame with component and attribute columns, term_type removed
    """
    lhs_values["component"] = lhs_values["term_type"].map(
        _CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE
    )
    lhs_values["attribute"] = lhs_values["term_type"].map(
        _CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE
    )
    return lhs_values.drop(columns="term_type")


def _translate_custom_constraint_generators_to_lhs(
    custom_constraint_generators: pd.DataFrame,
) -> pd.DataFrame:
    """Create the lhs definitions to match the generators used to relax custom
    constraints

    Args:
        custom_constraint_generators: pd.DataFrames detailing the
            custom constraint generators

    Returns: pd.DataFrame
    """
    custom_constraint_generators = custom_constraint_generators.rename(
        columns={"isp_name": "constraint_name", "name": "variable_name"}
    )
    custom_constraint_generators["component"] = "Generator"
    custom_constraint_generators["attribute"] = "p_nom"
    custom_constraint_generators["coefficient"] = -1.0
    col_order = [
        "constraint_name",
        "variable_name",
        "component",
        "attribute",
        "coefficient",
    ]
    return custom_constraint_generators.loc[:, col_order]


def _expand_link_flow_lhs_terms(
    custom_constraint_lhs: pd.DataFrame,
    links: pd.DataFrame,
) -> pd.DataFrame:
    """Create lhs terms for each existing link component and each expansion option
    link component.

    Args:
        custom_constraint_lhs: pd.DataFrame specifying lhs terms of custom
            constraints in PyPSA friendly format.
        links: pd.DataFrame detailing the PyPSA links to be included in the model.

    Returns: pd.DataFrame specifying lhs terms of custom
        constraints in PyPSA friendly format.

    Raises:
        ValueError: If any link_flow terms reference links that don't exist.
    """
    link_flow_mask = (custom_constraint_lhs["component"] == "Link") & (
        custom_constraint_lhs["attribute"] == "p"
    )
    link_flow_terms = custom_constraint_lhs[link_flow_mask]
    non_link_flow_terms = custom_constraint_lhs[~link_flow_mask]

    # Check for unmatched link_flow terms
    if not link_flow_terms.empty:
        unique_link_flow_names = set(link_flow_terms["variable_name"])

        # Handle None or empty links DataFrame
        if links is None or links.empty:
            unique_link_isp_names = set()
        else:
            unique_link_isp_names = set(links["isp_name"])

        unmatched_links = unique_link_flow_names - unique_link_isp_names

        if unmatched_links:
            raise ValueError(
                f"The following link_flow terms reference links that don't exist: "
                f"{sorted(unmatched_links)}"
            )

    all_lhs_terms = [non_link_flow_terms]

    # Only perform merge if there are link_flow terms and links is not None/empty
    if not link_flow_terms.empty and links is not None and not links.empty:
        link_flow_terms = pd.merge(
            link_flow_terms,
            links.loc[:, ["isp_name", "name"]],
            left_on="variable_name",
            right_on="isp_name",
        )
        link_flow_terms = link_flow_terms.drop(columns=["isp_name", "variable_name"])
        link_flow_terms = link_flow_terms.rename(columns={"name": "variable_name"})
        all_lhs_terms.append(link_flow_terms)

    return pd.concat(all_lhs_terms)


def _create_expansion_limit_constraints(
    links: pd.DataFrame,
    constraint_generators: pd.DataFrame,
    flow_paths: pd.DataFrame,
    rez_connections: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create custom constraint lhs and rhs definitions to limit the total expansion
    on rez and flow links

    Args:
        links: DataFrame specifying the Link components to be added to the PyPSA model.
        constraint_generators: DataFrame specifying the generators used to model
            rez connection capacity.
        flow_paths: DataFrame specifying the total additional
            capacity allowed on flow paths.
        rez_connections: DataFrame specifying the total
            additional capacity allowed on flow paths.

    Returns: Two DataFrames, the first specifying the lhs values of the custom
        constraints created, and the second the rhs values.
    """
    lhs_parts = []
    links_lhs = _create_expansion_limit_lhs_for_links(links)
    _append_if_not_empty(lhs_parts, links_lhs)
    generators_lhs = _create_expansion_limit_lhs_for_generators(constraint_generators)
    _append_if_not_empty(lhs_parts, generators_lhs)
    lhs = _finalize_expansion_limit_lhs(lhs_parts)

    rhs_parts = []
    flow_path_rhs = _process_rhs_components(flow_paths)
    _append_if_not_empty(rhs_parts, flow_path_rhs)
    rez_connections = _get_isp_names_for_rez_connection(rez_connections, links)
    rez_rhs = _process_rhs_components(rez_connections)
    _append_if_not_empty(rhs_parts, rez_rhs)
    rhs = _finalize_expansion_limit_rhs(rhs_parts)
    rhs = _filter_rhs_by_lhs_constraints(rhs, lhs)

    return lhs, rhs


def _get_isp_names_for_rez_connection(
    rez_connections: pd.DataFrame,
    links: pd.DataFrame,
) -> pd.DataFrame:
    """Update the rez_connection limits to use names that match with links."""

    if (
        rez_connections is not None
        and not rez_connections.empty
        and links is not None
        and not links.empty
    ):
        # Merge in the isp flow path names.
        rez_connections = pd.merge(
            rez_connections,
            links.groupby("isp_name", as_index=False).first(),
            how="left",
            left_on="rez_constraint_id",
            right_on="bus0",
        )

        # If there aren't matching links that means the costs are for transmission
        # constraints modelled with dummy generators and the rez_constraint_id name should
        # be kept.
        rez_connections.loc[rez_connections["isp_name"].isna(), "isp_name"] = (
            rez_connections["rez_constraint_id"]
        )
        rez_connections = rez_connections.loc[
            :, ["isp_name", "additional_network_capacity_mw"]
        ]

    return rez_connections


def _process_rhs_components(
    rhs_components: pd.DataFrame | None,
) -> pd.DataFrame:
    """Process flow path expansion costs into RHS constraint format.

    Args:
        flow_paths: DataFrame with flow path expansion costs or None

    Returns:
        DataFrame with processed flow path constraints, empty if input is None

    Raises:
        ValueError: If required columns are missing after processing
    """
    if rhs_components is None or rhs_components.empty:
        return pd.DataFrame()

    rhs_cols = [
        col for col in rhs_components if col in _CUSTOM_CONSTRAINT_ATTRIBUTES.keys()
    ]

    rhs_components = rhs_components.loc[:, rhs_cols]
    result = rhs_components.rename(columns=_CUSTOM_CONSTRAINT_ATTRIBUTES)

    # Check that required columns are present after renaming
    required_columns = {"constraint_name", "rhs"}
    missing_columns = required_columns - set(result.columns)
    if missing_columns:
        raise ValueError(
            f"RHS components missing required columns after processing: {sorted(missing_columns)}"
        )

    return result


def _create_expansion_limit_lhs_for_links(
    links: pd.DataFrame | None,
) -> pd.DataFrame:
    """Create constraints LHS for link expansion limits.

    Args:
        links: DataFrame with link information

    Returns:
        DataFrame with LHS constraint definitions for links
    """
    if links is None or links.empty:
        return pd.DataFrame()

    links = links.loc[links["p_nom_extendable"], ["isp_name", "name"]]
    links = links.rename(columns=_CUSTOM_CONSTRAINT_ATTRIBUTES)
    links["component"] = "Link"
    links["attribute"] = "p_nom"
    links["coefficient"] = 1.0
    return links


def _create_expansion_limit_lhs_for_generators(
    constraint_generators: pd.DataFrame | None,
) -> pd.DataFrame:
    """Create LHS constraints for generator expansion limits.

    Args:
        constraint_generators: DataFrame with constraint generators or None

    Returns:
        DataFrame with LHS constraint definitions for generators, empty if input is None
    """
    if constraint_generators is None or constraint_generators.empty:
        return pd.DataFrame()

    generators_lhs = constraint_generators.loc[:, ["isp_name", "name"]]
    generators_lhs = generators_lhs.rename(columns=_CUSTOM_CONSTRAINT_ATTRIBUTES)
    generators_lhs["component"] = "Generator"
    generators_lhs["attribute"] = "p_nom"
    generators_lhs["coefficient"] = 1.0
    return generators_lhs


def _finalize_expansion_limit_rhs(
    rhs_parts: list[pd.DataFrame],
) -> pd.DataFrame:
    """Combine and finalize RHS constraint parts.

    Args:
        rhs_parts: List of DataFrames with RHS constraint definitions

    Returns:
        Combined DataFrame with finalized RHS constraints, empty if no parts
    """
    if not rhs_parts:
        return pd.DataFrame()

    rhs = pd.concat(rhs_parts)
    rhs["constraint_name"] = rhs["constraint_name"] + "_expansion_limit"
    rhs["constraint_type"] = "<="
    return rhs


def _finalize_expansion_limit_lhs(
    lhs_parts: list[pd.DataFrame],
) -> pd.DataFrame:
    """Combine and finalize LHS constraint parts.

    Args:
        lhs_parts: List of DataFrames with LHS constraint definitions

    Returns:
        Combined DataFrame with finalized LHS constraints, empty if no parts
    """
    if not lhs_parts:
        return pd.DataFrame()

    lhs = pd.concat(lhs_parts)
    lhs["constraint_name"] = lhs["constraint_name"] + "_expansion_limit"
    return lhs


def _filter_rhs_by_lhs_constraints(
    rhs: pd.DataFrame,
    lhs: pd.DataFrame,
) -> pd.DataFrame:
    """Filter RHS to only include constraints that have corresponding LHS.

    Args:
        rhs: DataFrame with RHS constraint values
        lhs: DataFrame with LHS constraint definitions

    Returns:
        Filtered RHS DataFrame
    """
    if lhs.empty or rhs.empty:
        return pd.DataFrame()
    return rhs[rhs["constraint_name"].isin(lhs["constraint_name"])]


def _check_duplicate_constraint_names(
    rhs: pd.DataFrame,
) -> None:
    """Check for duplicate constraint names in RHS.

    Args:
        rhs: DataFrame with RHS constraint values

    Raises:
        ValueError: If duplicate constraint names are found
    """
    if rhs.empty:
        return

    duplicates = rhs[rhs.duplicated(subset=["constraint_name"], keep=False)]
    if not duplicates.empty:
        duplicate_names = sorted(duplicates["constraint_name"].unique())
        raise ValueError(
            f"Duplicate constraint names found in custom constraints RHS: {duplicate_names}"
        )


def _validate_lhs_rhs_constraints(
    lhs: pd.DataFrame,
    rhs: pd.DataFrame,
) -> None:
    """Validate that all LHS constraints have RHS definitions and vice versa.

    Args:
        lhs: DataFrame with LHS constraint definitions
        rhs: DataFrame with RHS constraint values

    Raises:
        ValueError: If there are mismatched constraints
    """
    if lhs.empty and rhs.empty:
        return

    if not lhs.empty:
        lhs_constraint_names = set(lhs["constraint_name"].unique())
    else:
        lhs_constraint_names = set()

    if not rhs.empty:
        rhs_constraint_names = set(rhs["constraint_name"].unique())
    else:
        rhs_constraint_names = set()

    # Check for LHS without RHS
    lhs_without_rhs = lhs_constraint_names - rhs_constraint_names
    if lhs_without_rhs:
        raise ValueError(
            f"The following LHS constraints do not have corresponding RHS definitions: "
            f"{sorted(lhs_without_rhs)}"
        )

    # Check for RHS without LHS
    rhs_without_lhs = rhs_constraint_names - lhs_constraint_names
    if rhs_without_lhs:
        raise ValueError(
            f"The following RHS constraints do not have corresponding LHS definitions: "
            f"{sorted(rhs_without_lhs)}"
        )
