from pathlib import Path

import pandas as pd

from ispypsa.translator.mappings import (
    _POLICY_CONSTRAINT_ID_TO_ATTRIBUTE_TYPE,
    _POLICY_CONSTRAINT_ID_TO_METRIC,
    _REGION_TO_BUS,
)


def _translate_custom_constraints_policy_lhs(
    custom_policy_constraint_tables: list[pd.DataFrame],
) -> pd.DataFrame:
    """Combines all tables specifying the lhs values of custom constraints into a single
    pd.Dataframe.

    Args:
        custom_constraint_lhs_tables: list of pd.DataFrames in `ISPyPSA` with policy-related constraints

    Returns: pd.DataFrame with constraint name, financial year, list of eligible buses, attribute, and metric
     List of eligible buses includes ispypsa sub-region ids and REZ ids
     Attribute is the type of constraint (e.g. "p", "p_nom")
     Metric will be used for crafting the equation depending on if the rhs value is a percent or a value
    ** Note: currently financial year is rounded to the earlier year (2025_26 -> 2025)
    """

    custom_constraint_lhs_values = []

    for table in custom_policy_constraint_tables:
        # Create new column which concatenates policy_id column and FY column
        if "policy_id" in table.columns and "FY" in table.columns:
            table["constraint_name"] = (
                table["policy_id"] + "_" + table["FY"].str[-2:].astype(str)
            )

        if "region_id" in table.columns:
            table["bus"] = table["region_id"].map(_REGION_TO_BUS)

        table["metric"] = table["policy_id"].map(_POLICY_CONSTRAINT_ID_TO_METRIC)
        table["attribute"] = table["policy_id"].map(
            _POLICY_CONSTRAINT_ID_TO_ATTRIBUTE_TYPE
        )

        # Identify the value column
        value_col = next(
            (
                col
                for col in set(_POLICY_CONSTRAINT_ID_TO_METRIC.values())
                if col in table.columns
            ),
            None,
        )
        table = table[table[value_col].fillna(0.0) != 0.0]

        table.loc[:, "FY"] = table["FY"].str[-2:].astype(int) + 2000
        custom_constraint_lhs_values.append(
            table[["constraint_name", "FY", "attribute", "metric", "bus"]]
        )

    custom_constraint_lhs_values = pd.concat(custom_constraint_lhs_values)
    custom_constraint_lhs_values["FY"] = custom_constraint_lhs_values["FY"].astype(
        "int64"
    )

    return custom_constraint_lhs_values


def _translate_custom_constraints_policy_rhs(
    custom_policy_constraint_tables: list[pd.DataFrame],
) -> pd.DataFrame:
    """Combines all tables specifying the rhs values of custom constraints into a single
    pd.Dataframe.
    Args:
        custom_policy_constraint_tables:  list of pd.DataFrames in `ISPyPSA` detailing
            policy constraint tables
    Returns: pd.DataFrame with constraint name and rhs value
    """

    custom_constraint_rhs_values = []
    for table in custom_policy_constraint_tables:
        # Create new column which concatenates policy_id column and FY column
        if "policy_id" in table.columns and "FY" in table.columns:
            table["constraint_name"] = (
                table["policy_id"] + "_" + table["FY"].str[-2:].astype(str)
            )

        for column in set(_POLICY_CONSTRAINT_ID_TO_METRIC.values()):
            if column in table.columns:
                # Identify valid (non-null, non-zero) rows
                valid_rows = table[column].notnull() & (table[column] != 0)

                # Drop invalid rows
                table = table.loc[valid_rows, :]

                # Assign values using .loc[row_indexer, col_indexer]
                table.loc[:, "rhs"] = table.loc[:, column]

        cols_to_keep = ["constraint_name", "rhs"]
        table = table.loc[:, cols_to_keep]

        custom_constraint_rhs_values.append(table)
    custom_constraint_rhs_values = pd.concat(custom_constraint_rhs_values)

    return custom_constraint_rhs_values
