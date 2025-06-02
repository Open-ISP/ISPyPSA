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
    config: ModelConfig, ispypsa_tables: dict[str, pd.DataFrame]
):
    """Translate custom constraint tables into a PyPSA friendly format.

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

    Returns: dictionary of dataframes in the `PyPSA` friendly format, with the relevant
        tables for custom constraint.
    """
    _check_custom_constraint_table_sets_are_complete(ispypsa_tables)

    pypsa_inputs = {}

    all_custom_constraint_tables = (
        _CUSTOM_GROUP_CONSTRAINTS + _CUSTOM_TRANSMISSION_LIMIT_CONSTRAINTS
    )

    present_custom_constraint_tables = [
        table for table in all_custom_constraint_tables if table in ispypsa_tables
    ]

    if len(present_custom_constraint_tables) != 0:
        custom_constraint_rhs_tables = [
            ispypsa_tables[table]
            for table in all_custom_constraint_tables
            if "_rhs" in table
        ]
        pypsa_inputs["custom_constraints_rhs"] = _translate_custom_constraint_rhs(
            custom_constraint_rhs_tables
        )

        custom_constraint_lhs_tables = [
            ispypsa_tables[table]
            for table in all_custom_constraint_tables
            if "_lhs" in table
        ]

        if config.network.rez_transmission_expansion:
            pypsa_inputs["custom_constraints_generators"] = (
                _translate_custom_constraints_generators(
                    list(pypsa_inputs["custom_constraints_rhs"]["constraint_name"]),
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

        pypsa_inputs["custom_constraints_lhs"] = _translate_custom_constraint_lhs(
            custom_constraint_lhs_tables
        )

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
    expansion_generators["lifetime"] = asset_lifetime

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
) -> pd.DataFrame:
    """Combines all tables specifying the lhs values of custom constraints into a single
    pd.Dataframe.

    Args:
        custom_constraint_lhs_tables: list of pd.DataFrames in `ISPyPSA` detailing
            custom constraints lhs values.

    Returns: pd.DataFrame
    """
    custom_constraint_lhs_values = _combine_custom_constraints_tables(
        custom_constraint_lhs_tables
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
