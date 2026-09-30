import logging
from pathlib import Path

import linopy
import pandas as pd
import pypsa


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


# The linopy model variables each supported (component, attribute) LHS term
# sums, with the sign each enters at. A Storage "p" term is net dispatch:
# discharging counts positive and charging negative, matching PLEXOS's paired
# battery Generation (+c) and Load (-c) Coefficients. Load "p_set" terms aren't
# listed because they are demand data, not variables — see _sum_load_terms.
_TERM_TO_MODEL_VARIABLES = pd.DataFrame(
    [
        ("Link", "p", "Link-p", 1.0),
        ("Link", "p_nom", "Link-p_nom", 1.0),
        ("Generator", "p", "Generator-p", 1.0),
        ("Generator", "p_nom", "Generator-p_nom", 1.0),
        ("Storage", "p", "StorageUnit-p_dispatch", 1.0),
        ("Storage", "p", "StorageUnit-p_store", -1.0),
    ],
    columns=["component", "attribute", "model_variable", "sign"],
)

_LOAD_COMPONENT = "Load"
_LOAD_ATTRIBUTE = "p_set"


def _add_custom_constraints_with_temporal_scope(
    network: pypsa.Network,
    custom_constraints_rhs: pd.DataFrame,
    custom_constraints_lhs: pd.DataFrame,
    timeslice_snapshots: pd.DataFrame,
) -> None:
    """Adds the new-format custom constraints to the network's linopy model.

    A linopy constraint is created for each unique constraint_name,
    investment_period and timeslice combination in custom_constraints_rhs, and
    applied to the snapshots corresponding to its investment_period and
    timeslice:

    - When the timeslice is named, the constraint applies at that timeslice's
      snapshots in its investment_period, as listed in timeslice_snapshots.
    - When the timeslice is blank, it acts as a fallback: the constraint
      applies at the snapshots in its investment_period that none of the same
      constraint's named timeslices cover.
    - A blank investment_period means every investment period, and only
      occurs on fallback rows (custom_constraints_rhs schema). The expansion-limit
      constraints are the case in practice: blank in both, with a p_nom-only
      LHS that has no time dimension.

    A linopy constraint's LHS terms are the custom_constraints_lhs rows with
    its constraint_name and the same investment_period, blank matching blank.
    The translator resolves every dated input to explicit investment periods,
    so a blank investment_period only occurs on constraints that apply
    regardless of investment period (the expansion limits). Load terms are demand data rather than variables:
    coefficient x p_set becomes a per-snapshot constant, which linopy moves to
    the right-hand side.

    Implementation note (linopy): flow and storage-dispatch variables take a
    value at every snapshot, so a constraint containing one is enforced
    separately at each snapshot it applies at (SWQLD1_2025_qld_peak_demand at
    01:00 and again at 02:00). Capacity (p_nom) variables take a single value,
    so a constraint made only of them, with no load terms, is enforced once.

    A linopy constraint whose investment_period and timeslice select no
    snapshots isn't created. It's expected under snapshot aggregation, and the
    translator warns about each timeslice with no snapshots in a constraint's
    investment period
    (ispypsa.translator.timeslices._log_referenced_timeslices_without_snapshots).

    Unsupported (component, attribute) terms raise, as does a constraint with
    no LHS terms on model variables, since either would silently weaken or
    drop the constraint.

    The three input tables' contracts are declared in
    src/ispypsa/validation/schemas/pypsa_friendly_tables
    (custom_constraints_rhs.yaml, custom_constraints_lhs.yaml and
    timeslice_snapshots.yaml).

    I/O Example:
        custom_constraints_rhs:
            constraint_name        investment_period  timeslice        rhs   constraint_type
            SWQLD1                 2025               qld_peak_demand  3000  <=
            SWQLD1                 2025               ,                3500  <=   # fallback: off-peak
            CQ-NQ_expansion_limit  ,                  ,                1000  <=

        custom_constraints_lhs:
            constraint_name        investment_period  variable_name     component  attribute  coefficient
            SWQLD1                 2025               NSW-QLD_existing  Link       p          0.84
            SWQLD1                 2025               load_SQ           Load       p_set      -0.33
            CQ-NQ_expansion_limit  ,                  CQ-NQ_exp_2025    Link       p_nom      1.0

        timeslice_snapshots:
            timeslice        investment_periods  snapshots
            qld_peak_demand  2025                2025-01-01 01:00
            qld_peak_demand  2025                2025-01-01 02:00

        network.snapshots:
            period  timestep
            2025    2025-01-01 00:00   # no timeslice
            2025    2025-01-01 01:00   # qld_peak_demand
            2025    2025-01-01 02:00   # qld_peak_demand
            2025    2025-01-01 03:00   # no timeslice

        adds constraints (one row per snapshot t in scope, where time-indexed):
            SWQLD1_2025_qld_peak_demand, at 01:00 and 02:00:
                0.84 Link-p[t, NSW-QLD_existing] <= 3000 + 0.33 p_set[t, load_SQ]
            SWQLD1_2025 (fallback), at 00:00 and 03:00:
                0.84 Link-p[t, NSW-QLD_existing] <= 3500 + 0.33 p_set[t, load_SQ]
            CQ-NQ_expansion_limit, no time dimension:
                Link-p_nom[CQ-NQ_exp_2025] <= 1000
    """
    _raise_on_unsupported_terms(custom_constraints_lhs)
    for rhs_row in custom_constraints_rhs.itertuples():
        scope = _linopy_constraint_snapshots(
            rhs_row, custom_constraints_rhs, timeslice_snapshots, network.snapshots
        )
        if scope.empty:
            continue
        name = _linopy_constraint_name(rhs_row)
        terms = _select_lhs_terms(custom_constraints_lhs, rhs_row)
        expression = _build_lhs_expression(network, terms, scope, name)
        _add_linopy_constraint(network.model, expression, rhs_row, name)


def _raise_on_unsupported_terms(custom_constraints_lhs: pd.DataFrame) -> None:
    """Raises when an LHS term's (component, attribute) has no model mapping.

    I/O Example:
        custom_constraints_lhs:
            constraint_name  variable_name  component  attribute
            SWQLD1           NSW-QLD        Link       p
            SWQLD1           SQ             Bus        p          # unsupported

        raises ValueError: "Custom constraint LHS terms with unsupported
        (component, attribute): [('Bus', 'p')]"
    """
    supported = set(
        zip(
            _TERM_TO_MODEL_VARIABLES["component"], _TERM_TO_MODEL_VARIABLES["attribute"]
        )
    ) | {(_LOAD_COMPONENT, _LOAD_ATTRIBUTE)}
    lhs = custom_constraints_lhs
    unsupported = sorted(set(zip(lhs["component"], lhs["attribute"])) - supported)
    if unsupported:
        raise ValueError(
            f"Custom constraint LHS terms with unsupported "
            f"(component, attribute): {unsupported}"
        )


def _linopy_constraint_snapshots(
    rhs_row: tuple,
    custom_constraints_rhs: pd.DataFrame,
    timeslice_snapshots: pd.DataFrame,
    snapshots: pd.MultiIndex,
) -> pd.MultiIndex:
    """The snapshots a linopy constraint applies at: its timeslice's snapshots
    in its investment_period when the timeslice is named, otherwise its
    fallback snapshots.

    rhs_row is one row of custom_constraints_rhs from itertuples(), so its
    columns are attributes (rhs_row.timeslice).

    I/O Example:
        custom_constraints_rhs:
            constraint_name  investment_period  timeslice
            SWQLD1           2025               qld_peak_demand
            SWQLD1           2025               ,                  # fallback

        timeslice_snapshots:
            timeslice        investment_periods  snapshots
            qld_peak_demand  2025                2025-01-01 01:00

        snapshots:
            period  timestep
            2025    2025-01-01 00:00
            2025    2025-01-01 01:00

        rhs_row:
            constraint_name  investment_period  timeslice
            SWQLD1           2025               qld_peak_demand

        returns:
            investment_periods  snapshots
            2025                2025-01-01 01:00

        rhs_row:
            constraint_name  investment_period  timeslice
            SWQLD1           2025               ,                  # fallback

        returns:
            period  timestep
            2025    2025-01-01 00:00
    """
    if pd.isna(rhs_row.timeslice):
        return _fallback_snapshots(
            rhs_row, custom_constraints_rhs, timeslice_snapshots, snapshots
        )
    return _timeslice_snapshots_in_period(
        timeslice_snapshots, [rhs_row.timeslice], rhs_row.investment_period
    )


def _fallback_snapshots(
    rhs_row: tuple,
    custom_constraints_rhs: pd.DataFrame,
    timeslice_snapshots: pd.DataFrame,
    snapshots: pd.MultiIndex,
) -> pd.MultiIndex:
    """The snapshots in a fallback constraint's investment_period (every
    investment period when blank) that none of its constraint_name's named
    timeslices in that investment period cover.

    I/O Example:
        rhs_row:
            constraint_name  investment_period  timeslice
            SWQLD1           2025               ,

        custom_constraints_rhs:
            constraint_name  investment_period  timeslice
            SWQLD1           2025               qld_peak_demand
            SWQLD1           2025               ,                  # rhs_row itself

        timeslice_snapshots:
            timeslice        investment_periods  snapshots
            qld_peak_demand  2025                2025-01-01 01:00

        snapshots:
            period  timestep
            2025    2025-01-01 00:00
            2025    2025-01-01 01:00   # covered by SWQLD1's qld_peak_demand
            2030    2030-01-01 00:00   # other investment period

        returns:
            period  timestep
            2025    2025-01-01 00:00
    """
    in_period = _snapshots_in_period(snapshots, rhs_row.investment_period)
    named = _timeslices_named_in_period(custom_constraints_rhs, rhs_row)
    covered = _timeslice_snapshots_in_period(
        timeslice_snapshots, named, rhs_row.investment_period
    )
    return in_period[~in_period.isin(covered)]


def _timeslices_named_in_period(
    custom_constraints_rhs: pd.DataFrame, rhs_row: tuple
) -> list[str]:
    """The timeslices named on rhs_row's constraint_name in its
    investment_period. None for a row with a blank investment_period: only
    fallback rows leave it blank (custom_constraints_rhs schema), and a blank
    investment_period matches nothing here.

    I/O Example:
        custom_constraints_rhs:
            constraint_name  investment_period  timeslice
            SWQLD1           2025               qld_peak_demand
            SWQLD1           2025               ,                    # the fallback itself
            SWQLD1           2030               qld_summer_typical   # other investment period
            SWQLD2           2025               qld_summer_typical   # other constraint

        rhs_row:
            constraint_name  investment_period  timeslice
            SWQLD1           2025               ,

        returns: ["qld_peak_demand"]
    """
    rhs = custom_constraints_rhs
    same_constraint = rhs["constraint_name"] == rhs_row.constraint_name
    same_period = rhs["investment_period"] == rhs_row.investment_period
    return list(rhs.loc[same_constraint & same_period, "timeslice"].dropna())


def _timeslice_snapshots_in_period(
    timeslice_snapshots: pd.DataFrame,
    timeslices: list[str],
    investment_period: float,
) -> pd.MultiIndex:
    """The snapshots in one investment_period at which any of the given
    timeslices is active, as an (investment_periods, snapshots) index. A blank
    (NaN) investment_period matches no snapshots.

    I/O Example:
        timeslice_snapshots:
            timeslice           investment_periods  snapshots
            qld_peak_demand     2025                2025-01-01 01:00
            qld_summer_typical  2025                2025-01-01 02:00
            qld_peak_demand     2030                2030-01-01 01:00   # other investment period
            nsw_peak_demand     2025                2025-01-01 01:00   # not listed

        timeslices: ["qld_peak_demand", "qld_summer_typical"]
        investment_period: 2025

        returns:
            investment_periods  snapshots
            2025                2025-01-01 01:00
            2025                2025-01-01 02:00
    """
    in_scope = timeslice_snapshots["timeslice"].isin(timeslices) & (
        timeslice_snapshots["investment_periods"] == investment_period
    )
    selected = timeslice_snapshots.loc[in_scope]
    return pd.MultiIndex.from_arrays(
        [selected["investment_periods"], pd.to_datetime(selected["snapshots"])]
    )


def _snapshots_in_period(
    snapshots: pd.MultiIndex, investment_period: float
) -> pd.MultiIndex:
    """The snapshots in one investment period, or all of them when it's blank
    (NaN).

    I/O Example:
        snapshots:
            period  timestep
            2025    2025-01-01 00:00
            2030    2030-01-01 00:00

        investment_period: 2025

        returns:
            period  timestep
            2025    2025-01-01 00:00

        investment_period: NaN

        returns:
            period  timestep
            2025    2025-01-01 00:00
            2030    2030-01-01 00:00
    """
    if pd.isna(investment_period):
        return snapshots
    return snapshots[snapshots.get_level_values("period") == investment_period]


def _select_lhs_terms(
    custom_constraints_lhs: pd.DataFrame, rhs_row: tuple
) -> pd.DataFrame:
    """The LHS terms of a linopy constraint: those with its constraint_name and
    the same investment_period, a blank investment_period matching only a
    blank one.

    I/O Example:
        custom_constraints_lhs:
            constraint_name          investment_period  variable_name
            SWQLD1                   2025               KINGASF1
            SWQLD1                   2030               KINGASF1        # other investment period: dropped
            SWQLD2                   2025               KINGASF1        # other constraint: dropped
            CQ-NQ_expansion_limit    ,                  CQ-NQ_exp_2025

        rhs_row:
            constraint_name  investment_period
            SWQLD1           2025

        returns:
            constraint_name  investment_period  variable_name
            SWQLD1           2025               KINGASF1

        rhs_row:
            constraint_name        investment_period
            CQ-NQ_expansion_limit  ,

        returns:
            constraint_name          investment_period  variable_name
            CQ-NQ_expansion_limit    ,                  CQ-NQ_exp_2025
    """
    lhs = custom_constraints_lhs
    same_constraint = lhs["constraint_name"] == rhs_row.constraint_name
    same_period = (lhs["investment_period"] == rhs_row.investment_period) | (
        lhs["investment_period"].isna() & pd.isna(rhs_row.investment_period)
    )
    return lhs[same_constraint & same_period]


def _build_lhs_expression(
    network: pypsa.Network, terms: pd.DataFrame, scope: pd.MultiIndex, name: str
) -> linopy.LinearExpression:
    """Builds sum(coefficient x variable) over a linopy constraint's variable
    terms, plus its load terms as a constant (which linopy moves to the RHS).

    I/O Example:
        terms:
            variable_name     component  attribute  coefficient
            NSW-QLD_existing  Link       p          0.84
            load_SQ           Load       p_set      -0.33

        scope:
            investment_periods  snapshots
            2025                2025-01-01 01:00

        name: "SWQLD1_2025_qld_peak_demand"   # only used in the error message

        returns, at each snapshot t in scope:
            0.84 Link-p[t, NSW-QLD_existing] - 0.33 p_set[t, load_SQ]
    """
    is_load = terms["component"] == _LOAD_COMPONENT
    variable_terms = _expand_terms_to_model_variables(terms[~is_load])
    _raise_if_no_variable_terms(variable_terms, name)
    expression = _sum_variable_terms(network.model, variable_terms, scope)
    return expression + _sum_load_terms(network, terms[is_load], scope)


def _expand_terms_to_model_variables(terms: pd.DataFrame) -> pd.DataFrame:
    """Maps each variable term to the model variable(s) it sums, folding each
    variable's sign into the coefficient.

    Every term's (component, attribute) must be in _TERM_TO_MODEL_VARIABLES,
    so load terms are split off before this is called. The orchestrator
    guarantees the rest with _raise_on_unsupported_terms; an unmatched term
    would be dropped by the merge.

    I/O Example:
        terms:
            variable_name    component  attribute  coefficient
            Q8 Battery - 2h  Storage    p          0.43

        returns:
            variable_name    model_variable          coefficient
            Q8 Battery - 2h  StorageUnit-p_dispatch  0.43
            Q8 Battery - 2h  StorageUnit-p_store     -0.43
    """
    expanded = terms.merge(_TERM_TO_MODEL_VARIABLES, on=["component", "attribute"])
    expanded["coefficient"] = expanded["coefficient"] * expanded["sign"]
    return expanded.loc[:, ["variable_name", "model_variable", "coefficient"]]


def _raise_if_no_variable_terms(variable_terms: pd.DataFrame, name: str) -> None:
    """Raises when a linopy constraint has no LHS terms on model variables, as
    when no LHS rows match its constraint_name and investment_period. Its LHS
    would be empty.

    I/O Example:
        variable_terms:
            variable_name  model_variable  coefficient   # no rows

        name: "SWQLD1_2025_qld_peak_demand"

        raises ValueError: "Custom constraint SWQLD1_2025_qld_peak_demand has
        no LHS terms on model variables."
    """
    if variable_terms.empty:
        raise ValueError(
            f"Custom constraint {name} has no LHS terms on model variables."
        )


def _sum_variable_terms(
    model: linopy.Model, variable_terms: pd.DataFrame, scope: pd.MultiIndex
) -> linopy.LinearExpression:
    """Sums coefficient x variable over the terms, restricting time-indexed
    variables to the snapshots in scope.

    I/O Example:
        variable_terms:
            variable_name     model_variable  coefficient
            NSW-QLD_existing  Link-p          0.84
            NSW-QLD_exp_2025  Link-p_nom      -1.0

        scope:
            investment_periods  snapshots
            2025                2025-01-01 01:00

        returns, at each snapshot t in scope:
            0.84 Link-p[t, NSW-QLD_existing] - 1.0 Link-p_nom[NSW-QLD_exp_2025]
    """
    expressions = [
        term.coefficient
        * _select_model_variable(model, term.model_variable, term.variable_name, scope)
        for term in variable_terms.itertuples()
    ]
    return linopy.merge(expressions, cls=linopy.LinearExpression)


def _select_model_variable(
    model: linopy.Model, model_variable: str, component_name: str, scope: pd.MultiIndex
) -> linopy.Variable:
    """One component's model variable, restricted to the snapshots in scope when
    it's time-indexed.

    I/O Example:
        scope:
            investment_periods  snapshots
            2025                2025-01-01 01:00

        model_variable: "Link-p", component_name: "NSW-QLD_existing"
        returns: Link-p[(2025, 2025-01-01 01:00), NSW-QLD_existing]

        model_variable: "Link-p_nom", component_name: "NSW-QLD_exp_2025"
        returns: Link-p_nom[NSW-QLD_exp_2025]   # no snapshot dimension
    """
    variable = model.variables[model_variable].sel(name=component_name)
    if "snapshot" in variable.dims:
        variable = variable.sel(snapshot=scope)
    return variable


def _sum_load_terms(
    network: pypsa.Network, load_terms: pd.DataFrame, scope: pd.MultiIndex
) -> pd.Series | float:
    """Sums coefficient x p_set over the load terms at each snapshot in scope.
    Returns 0 when there are none, so a p_nom-only constraint doesn't gain a
    snapshot dimension.

    I/O Example:
        load_terms:
            variable_name  coefficient
            load_SQ        -0.33

        scope:
            investment_periods  snapshots
            2025                2025-01-01 01:00
            2025                2025-01-01 02:00

        network.loads_t.p_set:
            period  timestep          load_SQ
            2025    2025-01-01 00:00  5000      # not in scope
            2025    2025-01-01 01:00  6000
            2025    2025-01-01 02:00  7000

        returns (a Series whose index is named "snapshot"):
            snapshot                  value
            (2025, 2025-01-01 01:00)  -1980
            (2025, 2025-01-01 02:00)  -2310
    """
    # A scalar 0 adds nothing and, unlike a series, doesn't give the
    # expression a snapshot dimension.
    if load_terms.empty:
        return 0.0
    # Demand table: one row per snapshot in scope, one column per load term
    # (in load_terms' order, repeating a load listed twice).
    p_set = network.loads_t.p_set.loc[scope, load_terms["variable_name"]]
    # Scale each column by its term's coefficient. to_numpy() multiplies by
    # position, so duplicate load columns each get their own coefficient;
    # then sum across the loads to one value per snapshot.
    offset = (p_set * load_terms["coefficient"].to_numpy()).sum(axis=1)
    # linopy turns the index into a dimension named after it. Unnamed, it
    # becomes "dim_0" when the expression has no snapshot dimension of its
    # own (a p_nom-only constraint with a load term), so name it "snapshot".
    offset.index.name = "snapshot"
    return offset


def _linopy_constraint_name(rhs_row: tuple) -> str:
    """A name unique to a linopy constraint, built from its constraint_name
    and whichever of investment_period and timeslice it has.

    I/O Example (rhs_row -> returns):
        constraint_name        investment_period  timeslice
        SWQLD1                 2025               qld_peak_demand  -> "SWQLD1_2025_qld_peak_demand"
        SWQLD1                 2025               ,                -> "SWQLD1_2025"
        CQ-NQ_expansion_limit  ,                  ,                -> "CQ-NQ_expansion_limit"
    """
    name_parts = [rhs_row.constraint_name]
    if pd.notna(rhs_row.investment_period):
        name_parts.append(str(int(rhs_row.investment_period)))
    if pd.notna(rhs_row.timeslice):
        name_parts.append(rhs_row.timeslice)
    return "_".join(name_parts)


def _add_linopy_constraint(
    model: linopy.Model,
    expression: linopy.LinearExpression,
    rhs_row: tuple,
    name: str,
) -> None:
    """Adds expression <constraint_type> rhs to the model under the given name.

    I/O Example:
        expression: 0.84 Link-p[t, NSW-QLD_existing]

        rhs_row:
            rhs   constraint_type
            3000  <=

        name: "SWQLD1_2025"

        adds to model: SWQLD1_2025: 0.84 Link-p[t, NSW-QLD_existing] <= 3000
    """
    model.add_constraints(expression, rhs_row.constraint_type, rhs_row.rhs, name=name)
