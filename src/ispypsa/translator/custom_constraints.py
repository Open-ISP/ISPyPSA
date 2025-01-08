from pathlib import Path

import pandas as pd

from ispypsa.translator.helpers import annuitised_investment_costs
from ispypsa.translator.mappings import (
    _CUSTOM_CONSTRAINT_ADDITIONAL_LINES,
    _CUSTOM_CONSTRAINT_ATTRIBUTES,
    _CUSTOM_CONSTRAINT_LHS_FILES,
    _CUSTOM_CONSTRAINT_RHS_FILES,
)


def _translate_custom_constraints_tables(
    ispypsa_inputs_path: Path | str, files: list[str]
):
    """Combines a set of data tables into a single data table, renaming the columns so
    they are consistent.

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
    """Combines all tables specifying the rhs variables needed for custom
    constraints into a single pd.Dataframe formatting the data so the rhs
    can be represented by PyPSA line components.

    Args:
        ispypsa_inputs_path: Path specifying where the files are located.
        expansion_on: bool,
        wacc: float,
        asset_lifetime: int

    Returns: pd.DataFrame
    """
    custom_constraints_additional_variables = _translate_custom_constraints_tables(
        ispypsa_inputs_path, _CUSTOM_CONSTRAINT_ADDITIONAL_LINES
    )

    custom_constraints_additional_variables = (
        custom_constraints_additional_variables.rename(
            columns={"variable_name": "name"}
        )
    )

    custom_constraints_additional_variables["p_nom"] = 0.0

    custom_constraints_additional_variables["bus"] = "bus_for_custom_constraint_gens"

    if expansion_on:
        custom_constraints_additional_variables["p_nom_extendable"] = True
        custom_constraints_additional_variables["capital_cost"] = (
            custom_constraints_additional_variables[
                "capital_cost"
            ].apply(lambda x: annuitised_investment_costs(x, wacc, asset_lifetime))
        )
    else:
        custom_constraints_additional_variables["p_nom_extendable"] = False
        custom_constraints_additional_variables["capital_cost"] = 0.0

    return custom_constraints_additional_variables


def _translate_custom_constraint_rhs(ispypsa_inputs_path: Path | str):
    """Combines all tables specifying the rhs values of custom constraints into a single
    pd.Dataframe.

    Args:
        ispypsa_inputs_path: Path specifying where the files are located.

    Returns: pd.DataFrame
    """
    custom_constraint_rhs_values = _translate_custom_constraints_tables(
        ispypsa_inputs_path, _CUSTOM_CONSTRAINT_RHS_FILES
    )
    return custom_constraint_rhs_values


def _translate_custom_constraint_lhs(ispypsa_inputs_path: Path | str):
    """Combines all tables specifying the lhs values of custom constraints into a single
    pd.Dataframe.

    Args:
        ispypsa_inputs_path: Path specifying where the files are located.

    Returns: pd.DataFrame
    """
    custom_constraint_lhs_values = _translate_custom_constraints_tables(
        ispypsa_inputs_path, _CUSTOM_CONSTRAINT_LHS_FILES
    )
    return custom_constraint_lhs_values
