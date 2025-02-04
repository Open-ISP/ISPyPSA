from pathlib import Path

import pandas as pd

from ispypsa.translator.helpers import annuitised_investment_costs
from ispypsa.translator.mappings import (
    _CUSTOM_CONSTRAINT_ATTRIBUTES,
    _CUSTOM_CONSTRAINT_EXPANSION_COSTS,
    _CUSTOM_CONSTRAINT_LHS_FILES,
    _CUSTOM_CONSTRAINT_RHS_FILES,
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE,
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE,
)


def _combine_custom_constraints_tables(
    ispypsa_inputs_path: Path | str, files: list[str]
):
    """Combines a set of custom constraint data tables into a single data table,
    renaming the columns so that they are consistent.

    Args:
        ispypsa_inputs_path: Path specifying where the files are located.
        files: list[str] specifying the names of the files to read and combine into
            a single dataframe.

    Returns: pd.DataFrame
    """
    combined_data = []
    for file in files:
        table = pd.read_csv(ispypsa_inputs_path / Path(file + ".csv"))
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
    ispypsa_inputs_path: Path | str,
    expansion_on: bool,
    wacc: float,
    asset_lifetime: int,
):
    """Combines all tables specifying the expansion costs of custom constraint
    rhs values into a single pd.Dataframe formatting the data so the rhs
    can be represented by PyPSA generator components. PyPSA can then invest in
    additional capacity for the generators which is used in the custom constraints
    to represent additional transmission capacity.

    Args:
        ispypsa_inputs_path: Path specifying where the files are located.
        expansion_on: bool indicating if transmission line expansion is considered.
        wacc: float, as fraction, indicating the weighted average coast of capital for
            transmission line investment, for the purposes of annuitising capital
            costs.
        asset_lifetime: int specifying the nominal asset lifetime in years or the
            purposes of annuitising capital costs.

    Returns: pd.DataFrame
    """
    custom_constraints_additional_variables = _combine_custom_constraints_tables(
        ispypsa_inputs_path, _CUSTOM_CONSTRAINT_EXPANSION_COSTS
    )

    custom_constraints_additional_variables = (
        custom_constraints_additional_variables.rename(
            columns={"variable_name": "name"}
        )
    )

    # The generator size is only used for additional transmission capacity so it
    # initial size is 0.0.
    custom_constraints_additional_variables["p_nom"] = 0.0

    custom_constraints_additional_variables["bus"] = "bus_for_custom_constraint_gens"

    custom_constraints_additional_variables["capital_cost"] = (
        custom_constraints_additional_variables["capital_cost"].apply(
            lambda x: annuitised_investment_costs(x, wacc, asset_lifetime)
        )
    )

    custom_constraints_additional_variables["p_nom_extendable"] = expansion_on

    return custom_constraints_additional_variables


def _translate_custom_constraint_rhs(ispypsa_inputs_path: Path | str):
    """Combines all tables specifying the rhs values of custom constraints into a single
    pd.Dataframe.

    Args:
        ispypsa_inputs_path: Path specifying where the files are located.

    Returns: pd.DataFrame
    """
    custom_constraint_rhs_values = _combine_custom_constraints_tables(
        ispypsa_inputs_path, _CUSTOM_CONSTRAINT_RHS_FILES
    )
    return custom_constraint_rhs_values


def _translate_custom_constraint_lhs(ispypsa_inputs_path: Path | str):
    """Combines all tables specifying the lhs values of custom constraints into a single
    pd.Dataframe. The term_type column is also converted to two columns specifying
    the pypsa component and attribute types.

    Args:
        ispypsa_inputs_path: Path specifying where the files are located.

    Returns: pd.DataFrame
    """
    custom_constraint_lhs_values = _combine_custom_constraints_tables(
        ispypsa_inputs_path, _CUSTOM_CONSTRAINT_LHS_FILES
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
