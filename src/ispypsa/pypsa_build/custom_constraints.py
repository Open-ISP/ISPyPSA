import logging
from pathlib import Path

import linopy
import pandas as pd
import pypsa

logger = logging.getLogger(__name__)


def _get_variables(
    model: linopy.Model, component_name: str, component_type: str, attribute_type: str
):
    """Retrieves variable objects from a linopy model based on a component name and
    type.

    Args:
        model: The `linopy.Model` object
        component_name: str, the name given to the component when added by ISPyPSA to
            the `pypsa.Network`.
        component_type: str, the type of variable, should be one of
            'Generator', 'Link', 'Load', or 'Storage
        attribute_type: str, the type of variable, should be one of
            'p' or 'p_nom'

    Returns: linopy.variables.Variable

    """
    var = None
    if component_type == "Generator" and attribute_type == "p_nom":
        var = model.variables.Generator_p_nom.at[f"{component_name}"]
    elif component_type == "Link" and attribute_type == "p":
        var = model.variables.Link_p.loc[:, f"{component_name}"]
    elif component_type == "Link" and attribute_type == "p_nom":
        var = model.variables.Link_p_nom.at[f"{component_name}"]
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


def _add_custom_constraints(
    network: pypsa.Network,
    custom_constraints_rhs: pd.DataFrame,
    custom_constraints_lhs: pd.DataFrame,
):
    """Adds constrains defined in `custom_constraints_lhs.csv` and
    `custom_constraints_rhs.csv` in the `path_to_pypsa_inputs` directory
    to the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        custom_constraints_rhs: `pd.DataFrame` specifying custom constraint RHS values,
            has two columns 'constraint_name' and 'rhs'.
        custom_constraints_lhs: `pd.DataFrame` specifying custom constraint LHS values.
            The DataFrame has five columns 'constraint_name', 'variable_name',
            'component', 'attribute', and 'coefficient'. The 'component' specifies
            whether the LHS variable belongs to a `PyPSA` 'Bus', 'Generator', 'Link',
            etc. The 'variable_name' specifies the name of the `PyPSA` component, and
            the 'attribute' specifies the attribute of the component that the variable
            belongs to i.e. 'p_nom', 's_nom', etc.

    Returns: None
    """
    lhs = custom_constraints_lhs
    rhs = custom_constraints_rhs

    for index, row in rhs.iterrows():
        constraint_name = row["constraint_name"]
        constraint_lhs = lhs[lhs["constraint_name"] == constraint_name].copy()

        # Retrieve the variable objects needed on the constraint lhs from the linopy
        # model used by the pypsa.Network
        model_variables = constraint_lhs.apply(
            lambda lhs_var: _get_variables(
                network.model,
                lhs_var["variable_name"],
                lhs_var["component"],
                lhs_var["attribute"],
            ),
            axis=1,
        )

        # Some variables may not be present in the modeled so these a filtered out.
        # variables that couldn't be found are logged in _get_variables so this doesn't
        # result in 'silent failure'.
        retrieved_vars = ~model_variables.isna()
        model_variables = model_variables.loc[retrieved_vars]
        coefficients = constraint_lhs.loc[retrieved_vars, "coefficient"]

        x = tuple(zip(coefficients, model_variables))
        linear_expression = network.model.linexpr(*x)
        if row["constraint_type"] == "<=":
            network.model.add_constraints(
                linear_expression <= row["rhs"], name=constraint_name
            )
        elif row["constraint_type"] == ">=":
            network.model.add_constraints(
                linear_expression >= row["rhs"], name=constraint_name
            )
        elif row["constraint_type"] == "==":
            network.model.add_constraints(
                linear_expression == row["rhs"], name=constraint_name
            )
        else:
            raise ValueError(
                f"{row['constraint_type']} is not a valid constraint type."
            )


def _add_custom_constraints_with_temporal_scope(
    network: pypsa.Network,
    custom_constraints_rhs: pd.DataFrame,
    custom_constraints_lhs: pd.DataFrame,
    timeslice_snapshots: pd.DataFrame,
):
    """Adds the new-format custom constraints to the `pypsa.Network`.

    Each RHS row creates one linopy constraint scoped to the row's
    investment_period and timeslice: time-indexed LHS variables are
    restricted to the snapshots inside that scope. NaN investment_period or
    timeslice means unrestricted in that dimension (e.g. the expansion-limit
    constraints, which only involve p_nom variables, carry NaN in both).

    LHS terms referencing components not in the model (e.g. generators while
    generator translation for the new format is pending) are skipped with a
    log line; constraint instances with no snapshots in scope or no terms in
    the model are skipped silently — the translator logs the timeslices that
    never apply.

    Args:
        network: The `pypsa.Network` object
        custom_constraints_rhs: `pd.DataFrame` with columns constraint_name,
            investment_period, timeslice, rhs, and constraint_type.
        custom_constraints_lhs: `pd.DataFrame` with columns constraint_name,
            investment_period, variable_name, component, attribute, and
            coefficient. A NaN investment_period term applies to every
            instance of its constraint.
        timeslice_snapshots: `pd.DataFrame` mapping timeslice_ids to the
            snapshots they are active at (columns timeslice_id,
            investment_periods, snapshots).

    Returns: None
    """
    timeslice_labels = _timeslice_constraint_labels(timeslice_snapshots)
    for row in custom_constraints_rhs.itertuples():
        snapshot_subset = _constraint_snapshot_subset(
            row, timeslice_labels, network.snapshots
        )
        if snapshot_subset is not None and len(snapshot_subset) == 0:
            continue
        terms = _select_lhs_terms(custom_constraints_lhs, row)
        expression = _build_constraint_expression(network.model, terms, snapshot_subset)
        if expression is None:
            continue
        _add_constraint_for_rhs_row(network.model, expression, row)


def _timeslice_constraint_labels(
    timeslice_snapshots: pd.DataFrame,
) -> dict[str, list[tuple]]:
    """The (investment_period, snapshot) labels each timeslice is active at.

    I/O Example:
        timeslice_id=qld_peak_demand, investment_periods=2025,
        snapshots=2025-01-13 12:00
        -> {"qld_peak_demand": [(2025, Timestamp("2025-01-13 12:00"))]}
    """
    mapping = timeslice_snapshots.copy()
    mapping["snapshots"] = pd.to_datetime(mapping["snapshots"])
    return {
        timeslice_id: list(zip(rows["investment_periods"], rows["snapshots"]))
        for timeslice_id, rows in mapping.groupby("timeslice_id")
    }


def _constraint_snapshot_subset(
    rhs_row, timeslice_labels: dict[str, list[tuple]], snapshots: pd.MultiIndex
) -> list[tuple] | None:
    """The snapshot labels an RHS row's constraint is restricted to, or None
    when the constraint is unrestricted (NaN timeslice and investment_period).

    I/O Example:
        timeslice=qld_peak_demand, investment_period=2025,
        timeslice_labels={"qld_peak_demand": [(2025, t1), (2030, t2)]}
        -> [(2025, t1)]

        timeslice=NaN, investment_period=2025 -> all 2025 snapshots
        timeslice=NaN, investment_period=NaN  -> None
    """
    if not pd.isna(rhs_row.timeslice):
        labels = timeslice_labels.get(rhs_row.timeslice, [])
        if not pd.isna(rhs_row.investment_period):
            period = int(rhs_row.investment_period)
            labels = [label for label in labels if label[0] == period]
        return labels
    if not pd.isna(rhs_row.investment_period):
        period = int(rhs_row.investment_period)
        return list(snapshots[snapshots.get_level_values(0) == period])
    return None


def _select_lhs_terms(custom_constraints_lhs: pd.DataFrame, rhs_row) -> pd.DataFrame:
    """The LHS terms belonging to an RHS row's constraint instance: terms for
    the same constraint whose investment_period matches the row's or is NaN
    (applies to every instance).

    I/O Example:
        lhs:
            constraint_name  investment_period  variable_name
            SWQLD1           2025               KINGASF1
            SWQLD1           2030               KINGASF1
            SWQLD1           ,                  SWQLD1_exp_2025

        rhs_row (SWQLD1, investment_period=2025) selects rows 1 and 3.
    """
    terms = custom_constraints_lhs[
        custom_constraints_lhs["constraint_name"] == rhs_row.constraint_name
    ]
    if pd.isna(rhs_row.investment_period):
        return terms
    return terms[
        terms["investment_period"].isna()
        | (terms["investment_period"] == rhs_row.investment_period)
    ]


def _build_constraint_expression(
    model: linopy.Model, terms: pd.DataFrame, snapshot_subset: list[tuple] | None
):
    """Builds the linear expression sum(coefficient * variable) for a
    constraint instance, restricting time-indexed variables to the snapshot
    subset. Returns None when no term's variable is in the model."""
    expression_terms = []
    for term in terms.itertuples():
        variable = _get_variable_with_snapshot_subset(
            model, term.variable_name, term.component, term.attribute, snapshot_subset
        )
        if variable is not None:
            expression_terms.append((term.coefficient, variable))
    if not expression_terms:
        return None
    return model.linexpr(*expression_terms)


def _get_variable_with_snapshot_subset(
    model: linopy.Model,
    component_name: str,
    component_type: str,
    attribute_type: str,
    snapshot_subset: list[tuple] | None,
):
    """Retrieves a variable like _get_variables, restricting time-indexed
    ('p') variables to the snapshot subset. Components not in the model are
    skipped with a log line rather than raising — new-format constraints
    legitimately reference generators and batteries before those components
    are translated."""
    try:
        variable = _get_variables(model, component_name, component_type, attribute_type)
    except KeyError:
        logger.info(
            f"{component_type} {component_name} not in model, "
            f"custom constraint term skipped."
        )
        return None
    if variable is not None and attribute_type == "p" and snapshot_subset is not None:
        variable = variable.loc[snapshot_subset]
    return variable


def _add_constraint_for_rhs_row(model: linopy.Model, expression, rhs_row) -> None:
    """Adds one constraint, named uniquely for the RHS row's temporal scope.

    I/O Example:
        (SWQLD1, 2025, qld_peak_demand) -> "SWQLD1_2025_qld_peak_demand"
        (CQ-NQ_expansion_limit, NaN, NaN) -> "CQ-NQ_expansion_limit"
    """
    name_parts = [rhs_row.constraint_name]
    if not pd.isna(rhs_row.investment_period):
        name_parts.append(str(int(rhs_row.investment_period)))
    if not pd.isna(rhs_row.timeslice):
        name_parts.append(rhs_row.timeslice)
    name = "_".join(name_parts)
    if rhs_row.constraint_type == "<=":
        model.add_constraints(expression <= rhs_row.rhs, name=name)
    elif rhs_row.constraint_type == ">=":
        model.add_constraints(expression >= rhs_row.rhs, name=name)
    elif rhs_row.constraint_type == "==":
        model.add_constraints(expression == rhs_row.rhs, name=name)
    else:
        raise ValueError(f"{rhs_row.constraint_type} is not a valid constraint type.")
