import logging
from pathlib import Path

import linopy
import pandas as pd
import pypsa


def get_variables(
    model: linopy.Model, component_name: str, component_type: str, attribute_type: str
):
    """Retrieves variable objects from a linopy model based on a component name and
    type.

    Args:
        model: The `linopy.Model` object
        component_name: str, the name given to the component when added by ISPyPSA to
            the `pypsa.Network`.
        component_type: str, the type of variable, should be one of
            'Generator', 'Line', 'Load', or 'Storage'
        attribute_type: str, the type of variable, should be one of
            'p', 'p_nom', or 's'

    Returns: linopy.variables.Variable

    """
    var = None
    if component_type == "Generator" and attribute_type == "p_nom":
        var = model.variables.Generator_p_nom.at[f"{component_name}"]
    elif component_type == "Line" and attribute_type == "s":
        var = model.variables.Line_s.loc[:, f"{component_name}"]
    elif component_type == "Generator" and attribute_type == "p":
        var = model.variables.Generator_p.loc[:, f"{component_name}"]
    elif component_type == "Load" and attribute_type == "p":
        logging.info(
            f"Load component {component_name} not added to custom constraint. "
            f"Load variables not implemented."
        )
    elif component_type == "Storage" and attribute_type == "p":
        logging.info(
            f"Storage component {component_name} not added to custom constraint. "
            f"Storage variables not implemented."
        )
    else:
        raise ValueError(f"{component_type} and {attribute_type} is not defined.")
    return var


def add_custom_constraints(network: pypsa.Network, path_pypsa_inputs: Path):
    """Adds constrains defined in `custom_constraints_lhs.csv` and
    `custom_constraints_rhs.csv` in the `path_to_pypsa_inputs` directory
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

        # Retrieve the variable objects needed on the constraint lhs from the linopy
        # model used by the pypsa.Network
        variables = constraint_lhs.apply(
            lambda row: get_variables(
                network.model, row["variable_name"], row["component"], row["attribute"]
            ),
            axis=1,
        )

        # Some variables may not be present in the modeled so these a filtered out.
        # variables that couldn't be found are logged in get_variables so this doesn't
        # result in 'silent failure'.
        retrieved_vars = ~variables.isna()
        variables = variables.loc[retrieved_vars]
        coefficients = constraint_lhs.loc[retrieved_vars, "coefficient"]

        x = tuple(zip(coefficients, variables))
        linear_expression = network.model.linexpr(*x)
        network.model.add_constraints(
            linear_expression <= row["rhs"], name=constraint_name
        )
