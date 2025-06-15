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
    _CUSTOM_GROUP_CONSTRAINTS,
    _CUSTOM_TRANSMISSION_LIMIT_CONSTRAINTS,
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
                - rez_group_constraints_rhs
                - rez_group_constraints_lhs
                - rez_transmission_limit_constraints_lhs
                - rez_transmission_limit_constraints_rhs
            Not all of these tables need to be present but if one of the tables in
            pair is present an error will be raised if the other is missing.
        links: pd.DataFrame specifying the Link components to be used in the PyPSA model

    Returns: dictionary of dataframes in the `PyPSA` friendly format, with the relevant
        tables for custom constraints.
    """
    _check_custom_constraint_table_sets_are_complete(ispypsa_tables)

    pypsa_inputs = {}

    all_custom_constraint_tables = (
        _CUSTOM_GROUP_CONSTRAINTS + _CUSTOM_TRANSMISSION_LIMIT_CONSTRAINTS
    )

    # Get all custom constraint tables that have been given.
    present_custom_constraint_tables = [
        table for table in all_custom_constraint_tables if table in ispypsa_tables
    ]

    lhs = []
    rhs = []

    # Translate custom constraints to PyPSA friendly format.
    if len(present_custom_constraint_tables) != 0:
        custom_constraint_rhs_tables = [
            ispypsa_tables[table]
            for table in present_custom_constraint_tables
            if "_rhs" in table
        ]

        manually_specified_rhs = _translate_custom_constraint_rhs(
            custom_constraint_rhs_tables
        )

        rhs.append(manually_specified_rhs)

        custom_constraint_lhs_tables = [
            ispypsa_tables[table]
            for table in present_custom_constraint_tables
            if "_lhs" in table
        ]

        # Create dummy generators to allow for expansion of rhs term limiting
        # constraints for modelling rez transmission.
        if config.network.rez_transmission_expansion:
            pypsa_inputs["custom_constraints_generators"] = (
                _translate_custom_constraints_generators(
                    list(manually_specified_rhs["constraint_name"]),
                    ispypsa_tables["rez_transmission_expansion_costs"],
                    config.wacc,
                    config.network.annuitisation_lifetime,
                    config.temporal.capacity_expansion.investment_periods,
                    config.temporal.year_type,
                )
            )

            custom_constraint_generators_lhs = (
                _translate_custom_constraint_generators_to_lhs(
                    pypsa_inputs["custom_constraints_generators"]
                )
            )
            custom_constraint_lhs_tables += [custom_constraint_generators_lhs]

        lhs.append(
            _translate_custom_constraint_lhs(custom_constraint_lhs_tables, links)
        )

    if not links.empty:
        # Create custom constraints that limit the total expansion of transmission
        # across multiple links / investment years.
        transmission_expansion_limit_lhs, transmission_expansion_limit_rhs = (
            _create_flow_path_and_rez_transmission_expansion_limit_constraints(
                links,
                pypsa_inputs.get("custom_constraints_generators"),
                ispypsa_tables.get("flow_path_expansion_costs"),
                ispypsa_tables.get("rez_transmission_expansion_costs"),
            )
        )
        lhs.append(transmission_expansion_limit_lhs)
        rhs.append(transmission_expansion_limit_rhs)

    if lhs and rhs:
        pypsa_inputs["custom_constraints_lhs"] = pd.concat(lhs)
        pypsa_inputs["custom_constraints_rhs"] = pd.concat(rhs)

    return pypsa_inputs


def _check_custom_constraint_table_sets_are_complete(
    ispypsa_tables: dict[str, pd.DataFrame],
):
    """Raise an error if a partially complete set of input tables has been provided
    for a set of custom constraints.
    """

    def check_for_partially_complete_inputs(input_table_list, input_set_name):
        tables_present = sum(
            table in ispypsa_tables.keys() for table in input_table_list
        )
        if tables_present != len(input_table_list) and tables_present > 0:
            raise ValueError(
                f"An incomplete set of inputs have been provided for {input_set_name}"
            )

    check_for_partially_complete_inputs(
        _CUSTOM_GROUP_CONSTRAINTS, "custom group constraints"
    )

    check_for_partially_complete_inputs(
        _CUSTOM_TRANSMISSION_LIMIT_CONSTRAINTS, "custom transmission limit constraints"
    )


def _translate_custom_constraints_generators(
    custom_constraints: list[int],
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

    expansion_generators["name"] = (
        expansion_generators["constraint_name"]
        + "_exp_"
        + expansion_generators["build_year"].astype(str)
    )
    expansion_generators["bus"] = "bus_for_custom_constraint_gens"
    expansion_generators["p_nom"] = 0.0
    expansion_generators["p_nom_extendable"] = True
    expansion_generators["lifetime"] = np.inf

    # Keep only the columns needed for PyPSA generators
    expansion_cols = [
        "name",
        "constraint_name",
        "bus",
        "p_nom",
        "p_nom_extendable",
        "build_year",
        "lifetime",
        "capital_cost",
    ]
    expansion_generators = expansion_generators[expansion_cols]
    return expansion_generators.reset_index(drop=True)


def _combine_custom_constraints_tables(custom_constraint_tables: list[pd.DataFrame]):
    """Combines a set of custom constraint data tables into a single data table,
    renaming the columns so that they are consistent.

    Args:
        custom_constraint_tables: list of pd.DataFrames specifying custom constraint
            details
    Returns: pd.DataFrame
    """
    combined_data = []
    for table in custom_constraint_tables:
        table = table.rename(columns=_CUSTOM_CONSTRAINT_ATTRIBUTES)
        cols_to_keep = [
            col
            for col in table.columns
            if col in _CUSTOM_CONSTRAINT_ATTRIBUTES.values()
        ]
        table = table.loc[:, cols_to_keep]
        combined_data.append(table)
    combined_data = pd.concat(combined_data)
    return combined_data


def _translate_custom_constraint_rhs(
    custom_constraint_rhs_tables: list[pd.DataFrame],
) -> pd.DataFrame:
    """Combines all tables specifying the rhs values of custom constraints into a single
    pd.Dataframe.

    Args:
        custom_constraint_rhs_tables:  list of pd.DataFrames in `ISPyPSA` detailing
            custom constraints rhs values.

    Returns: pd.DataFrame
    """
    custom_constraint_rhs_values = _combine_custom_constraints_tables(
        custom_constraint_rhs_tables
    )
    return custom_constraint_rhs_values


def _translate_custom_constraint_lhs(
    custom_constraint_lhs_tables: list[pd.DataFrame],
    links: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all tables specifying the lhs values of custom constraints into a single
    pd.Dataframe.

    Args:
        custom_constraint_lhs_tables: list of pd.DataFrames in `ISPyPSA` detailing
            custom constraints lhs values.
        links: pd.DataFrame specifying the Link components to be used in the PyPSA
            model.

    Returns: pd.DataFrame
    """
    custom_constraint_lhs_values = _combine_custom_constraints_tables(
        custom_constraint_lhs_tables
    )
    if not links.empty:
        custom_constraint_lhs_values = _expand_link_flow_lhs_terms(
            custom_constraint_lhs_values, links
        )
    custom_constraint_lhs_values["component"] = custom_constraint_lhs_values[
        "term_type"
    ].map(_CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE)

    custom_constraint_lhs_values["attribute"] = custom_constraint_lhs_values[
        "term_type"
    ].map(_CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE)

    custom_constraint_lhs_values = custom_constraint_lhs_values.drop(
        columns="term_type"
    )
    return custom_constraint_lhs_values


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
        columns={"constraint_name": "constraint_id", "name": "term_id"}
    )
    custom_constraint_generators["term_type"] = "generator_capacity"
    custom_constraint_generators["coefficient"] = -1.0
    col_order = ["constraint_id", "term_type", "term_id", "coefficient"]
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
    """
    link_flow_terms = custom_constraint_lhs[
        custom_constraint_lhs["term_type"] == "link_flow"
    ]
    non_link_flow_terms = custom_constraint_lhs[
        ~(custom_constraint_lhs["term_type"] == "link_flow")
    ]
    all_lhs_terms = [non_link_flow_terms]
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


def _create_flow_path_and_rez_transmission_expansion_limit_constraints(
    links,
    constraint_generators,
    flow_paths,
    rez_connections,
):
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
    lhs = []
    rhs = []

    if flow_paths is not None:
        flow_path_cols = [
            col for col in flow_paths if col in _CUSTOM_CONSTRAINT_ATTRIBUTES.keys()
        ]
        flow_paths = flow_paths.loc[:, flow_path_cols]
        flow_paths = flow_paths.rename(columns=_CUSTOM_CONSTRAINT_ATTRIBUTES)
        rhs.append(flow_paths)

    if rez_connections is not None:
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
        rez_connections = rez_connections.rename(columns=_CUSTOM_CONSTRAINT_ATTRIBUTES)
        rhs.append(rez_connections)

    # Check if there are any rhs constraints to concatenate
    if rhs:
        rhs = pd.concat(rhs)
        rhs["constraint_name"] = rhs["constraint_name"] + "_expansion_limit"
        rhs["term_type"] = "<="
    else:
        # Return empty DataFrames if no expansion costs are provided
        return pd.DataFrame(), pd.DataFrame()

    if constraint_generators is not None:
        # Find extendable links whose capacity is not modelled using custom constraints
        # and dummy generators.
        link_mask = (
            ~links["isp_name"].isin(constraint_generators["constraint_name"])
            & links["p_nom_extendable"]
        )
    else:
        link_mask = links["p_nom_extendable"]

    # Convert link data to lhs definitions
    links_lhs = links.loc[link_mask, ["isp_name", "name"]]
    links_lhs = links_lhs.rename(columns=_CUSTOM_CONSTRAINT_ATTRIBUTES)
    links_lhs["component"] = "Link"
    links_lhs["attribute"] = "p_nom"
    links_lhs["coefficient"] = 1.0
    lhs.append(links_lhs)

    if constraint_generators is not None:
        generators_lhs = constraint_generators.loc[:, ["constraint_name", "name"]]
        generators_lhs = generators_lhs.rename(columns=_CUSTOM_CONSTRAINT_ATTRIBUTES)
        generators_lhs["component"] = "Generator"
        generators_lhs["attribute"] = "p_nom"
        generators_lhs["coefficient"] = 1.0
        lhs.append(generators_lhs)

    lhs = pd.concat(lhs)
    lhs["constraint_name"] = lhs["constraint_name"] + "_expansion_limit"

    rhs = rhs[rhs["constraint_name"].isin(lhs["constraint_name"])]

    return lhs, rhs
