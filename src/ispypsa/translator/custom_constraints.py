from pathlib import Path

import pandas as pd

from ispypsa.translator.helpers import _annuitised_investment_costs
from ispypsa.translator.mappings import (
    _CUSTOM_CONSTRAINT_ATTRIBUTES,
    _CUSTOM_CONSTRAINT_EXPANSION_COSTS,
    _CUSTOM_CONSTRAINT_LHS_TABLES,
    _CUSTOM_CONSTRAINT_RHS_TABLES,
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE,
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE,
)


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


def _translate_custom_constraints_generators(
    custom_constraint_generators: list[pd.DataFrame],
    expansion_on: bool,
    wacc: float,
    asset_lifetime: int,
) -> pd.DataFrame:
    """Combines all tables specifying the expansion costs of custom constraint
    rhs values into a single pd.Dataframe formatting the data so the rhs
    can be represented by PyPSA generator components. PyPSA can then invest in
    additional capacity for the generators which is used in the custom constraints
    to represent additional transmission capacity.

    Args:
        custom_constraint_generators: list of pd.DataFrames in `ISPyPSA` detailing
            custom constraint generator expansion costs.
        expansion_on: bool indicating if transmission line expansion is considered.
        wacc: float, as fraction, indicating the weighted average coast of capital for
            transmission line investment, for the purposes of annuitising capital
            costs.
        asset_lifetime: int specifying the nominal asset lifetime in years or the
            purposes of annuitising capital costs.

    Returns: pd.DataFrame
    """
    custom_constraint_generators = _combine_custom_constraints_tables(
        custom_constraint_generators
    )

    custom_constraint_generators = custom_constraint_generators.rename(
        columns={"variable_name": "name"}
    )

    custom_constraint_generators["bus"] = "bus_for_custom_constraint_gens"
    custom_constraint_generators["p_nom"] = 0.0

    # The generator size is only used for additional transmission capacity, so it
    # initial size is 0.0.
    custom_constraint_generators["capital_cost"] = custom_constraint_generators[
        "capital_cost"
    ].apply(lambda x: _annuitised_investment_costs(x, wacc, asset_lifetime))

    # not extendable by default
    custom_constraint_generators["p_nom_extendable"] = False
    mask = ~custom_constraint_generators["capital_cost"].isna()
    custom_constraint_generators.loc[mask, "p_nom_extendable"] = expansion_on

    return custom_constraint_generators


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
