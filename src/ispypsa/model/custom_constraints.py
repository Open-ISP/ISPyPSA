import logging
from pathlib import Path

import linopy
import pandas as pd
import pypsa


def get_variables(model: linopy.Model, component_name: str, component_type: str):
    var = None
    if component_type == "generator_capacity":
        var = model.variables.Generator_p_nom.at[f"{component_name}"]
    elif component_type == "line_flow":
        var = model.variables.Line_s.loc[:, f"{component_name}"]
    elif component_type in ["generator_output"]:
        var = model.variables.Generator_p.loc[:, f"{component_name}"]
    elif component_type in ["load_consumption"]:
        logging.warning(
            f"load_consumption component {component_name} not added to custom constraint. "
            f"Load variables not implemented."
        )
    elif component_type in ["battery_output"]:
        logging.warning(
            f"battery_output component {component_name} not added to custom constraint. "
            f"Battery variables not implemented."
        )
    else:
        raise ValueError(f"{component_type} is not defined.")
    return var


def add_custom_constraints(network: pypsa.Network, path_pypsa_inputs: Path):
    """Adds constrains defined in `custom_constraints_lhs.csv` and
    `custom_constraints_rhs.csv` in =the `path_to_pypsa_inputs` directory
    to the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        path_pypsa_inputs: `pathlib.Path` that points to the directory containing
            PyPSA inputs

    Returns: None
    """
    lhs = pd.read_csv(path_pypsa_inputs / Path("custom_constraints_lhs.csv"))
    rhs = pd.read_csv(path_pypsa_inputs / Path("custom_constraints_rhs.csv"))

    for index, row in rhs.iterrows():
        constraint_name = row["constraint_name"]
        constraint_lhs = lhs[lhs["constraint_name"] == constraint_name].copy()
        variables = constraint_lhs.apply(
            lambda row: get_variables(
                network.model, row["variable_name"], row["component_type"]
            ),
            axis=1,
        )
        retrieved_vars = ~variables.isna()
        variables = variables.loc[retrieved_vars]
        coefficients = constraint_lhs.loc[retrieved_vars, "coefficient"]
        x = tuple(zip(coefficients, variables))
        linear_expression = network.model.linexpr(*x)
        network.model.add_constraints(
            linear_expression <= row["rhs"], name=constraint_name
        )
