"""Translate the new-format custom-constraint tables into PyPSA friendly form.

This module sits in the translator stage alongside ispypsa.translator.network.
It turns the templated custom-constraint tables (PLEXOS-derived group
constraints such as SWQLD1) into the LHS/RHS tables pypsa_build applies as
linopy constraints, and adds the endogenous expansion-limit constraints, with
their constraint-relaxation generators, that cap how much capacity the model
can build for each expandable network element.

The inputs are the three custom-constraint tables — custom_constraints (one
row per constraint), custom_constraints_lhs (one row per term per date_from)
and custom_constraints_rhs (one row per timeslice per date_from):

    custom_constraints:            custom_constraints_rhs:
        constraint_id  direction      constraint_id  timeslice        rhs   date_from
        SWQLD1         <=             SWQLD1         qld_peak_demand  3000
                                      SWQLD1         qld_peak_demand  2500  2027-07-01T00:00:00

    custom_constraints_lhs:
        constraint_id  term_type         variable_name  coefficient  date_from
        SWQLD1         link_flow         NSW-QLD        0.84
        SWQLD1         generator_output  KINGASF1       0.14

plus network_expansion_options and network_transmission_path_expansion_costs
(the unified expansion tables, see ispypsa.translator.network) and the PyPSA
friendly links table (existing plus expansion links).

The outputs are the PyPSA friendly custom_constraints_rhs (one row per
constraint, investment period and timeslice), custom_constraints_lhs (one row
per constraint, investment period and term) and custom_constraints_generators
(one row per relaxable constraint and investment period):

    custom_constraints_rhs:
        constraint_name          investment_period  timeslice        rhs   constraint_type
        SWQLD1                   2026               qld_peak_demand  3000  <=
        SWQLD1                   2028               qld_peak_demand  2500  <=
        NSW-QLD_expansion_limit                                      1000  <=
        SWQLD1_expansion_limit                                       400   <=

    custom_constraints_lhs (2028 rows mirror 2026):
        constraint_name          investment_period  variable_name     component  attribute  coefficient
        SWQLD1                   2026               NSW-QLD_existing  Link       p          0.84
        SWQLD1                   2026               NSW-QLD_exp_2026  Link       p          0.84
        SWQLD1                   2026               KINGASF1          Generator  p          0.14
        SWQLD1                   2026               SWQLD1_exp_2026   Generator  p_nom      -1.0
        NSW-QLD_expansion_limit                     NSW-QLD_exp_2026  Link       p_nom      1.0
        SWQLD1_expansion_limit                      SWQLD1_exp_2026   Generator  p_nom      1.0

    custom_constraints_generators (abridged):
        name             isp_name  bus                             p_nom  p_nom_extendable  build_year  capital_cost
        SWQLD1_exp_2026  SWQLD1    bus_for_custom_constraint_gens  0.0    True              2026        annuitise(100000)

A blank investment_period means the row applies in every period. A named
timeslice scopes the RHS to the snapshots inside that timeslice's windows
(the timeslices table); a blank timeslice is the constraint's fallback,
applying at the snapshots none of its named-timeslice rows cover, so a
constraint with only named rows does not bind outside them. This module
passes timeslice through untouched — resolving it to snapshots is
pypsa_build's job when the constraints are applied.

The pipeline runs as follows. The date_from column of the LHS and RHS tables
is resolved into one row per investment period: for each period, each group
(a constraint's term, or a constraint's timeslice) keeps the row active at
the period's start — the latest date_from on or before it, with no-date_from
rows as the baseline. The RHS gains each constraint's sense from
custom_constraints as constraint_type. LHS term_types map to the PyPSA
component and attribute their variable belongs to, and each link_flow term
is expanded from its path_id to every link the model has on that path — the
existing link and each expansion link — so flow through new builds counts
towards the constraint too; terms for paths not in the model are dropped.
Because date resolution can leave a constraint with terms but no limit (or a
limit but no terms) in some periods, the two tables are then reconciled
period by period: a constraint is kept only in the periods where it has both
LHS terms and an RHS row, and the one-sided periods are dropped and logged.

Constraint relaxation comes next, gated by the config's
rez_transmission_expansion flag. Each constraint that has a
constraint_relaxation expansion option gets one extendable dummy generator
per investment period at the option's annualised cost. The generator's p_nom
enters the parent constraint's LHS, in each period the constraint binds in,
with a sign chosen by the constraint's direction so that building it always
loosens the constraint: subtracted from a "<=" it raises the cap, added to a
">=" it lowers the floor. Finally the expansion-limit constraints cap the total
p_nom built across each expandable element's per-period components: for a
path the cap is max(forward, reverse) of its option, matching the per-unit
ratings ispypsa.translator.network gives its expansion links; for a
relaxation it is the option's allowed_expansion. Both tables are then
finalised — constraint_id becomes constraint_name and duplicate constraint
names are rejected.

Input integrity is the table schemas' job, not this module's. The rules the
pipeline relies on without re-checking — unique input rows, a direction for
every constraint with RHS values, the LHS and RHS naming the same
constraints, every LHS variable_name naming a component in the table its
term_type refers to, no constraint_relaxation option on an "=" constraint,
and a cost for every expandable element in every investment period — are
declared in src/ispypsa/validation/schemas (custom_constraints*.yaml,
network_expansion_options.yaml and
network_transmission_path_expansion_costs.yaml).

Reference detail:

- direction to constraint_type: "<=" and ">=" pass through, "=" becomes "==".
- relaxation generator coefficient: -1.0 on a "<=" constraint, +1.0 on a ">=".
- term_type to component/attribute lives in ispypsa.translator.mappings
  (_CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE and _..._ATTRIBUTE_TYPE).
- Expansion-limit constraints are named "<expansion_id>_expansion_limit" so a
  relaxation cap doesn't collide with the constraint it relaxes.
- Dropped rows: link_flow terms whose path is not in the model (logged); per
  investment period, RHS rows of a constraint with no LHS terms in that
  period and LHS terms of a constraint with no RHS row in that period (both
  logged); relaxation options and costs for constraints not in the model, or
  all of them when rez_transmission_expansion is off; date_from rows that
  only start after every investment period.
"""

import logging

import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.helpers import _resolve_wildcards
from ispypsa.translator.mappings import (
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE,
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE,
)
from ispypsa.translator.network import (
    _keep_rows_for_expansion_ids,
    _pair_forward_and_reverse_options,
    _prepare_expansion_costs,
    _resolve_expansion_options,
)

logger = logging.getLogger(__name__)

_LHS_COLUMNS = [
    "constraint_name",
    "investment_period",
    "variable_name",
    "component",
    "attribute",
    "coefficient",
]

_RHS_COLUMNS = [
    "constraint_name",
    "investment_period",
    "timeslice",
    "rhs",
    "constraint_type",
]

_GENERATOR_COLUMNS = [
    "name",
    "isp_name",
    "bus",
    "p_nom",
    "p_nom_extendable",
    "build_year",
    "lifetime",
    "capital_cost",
]

_DIRECTION_TO_CONSTRAINT_TYPE = {"<=": "<=", ">=": ">=", "=": "=="}

# The LHS sign that lets a relaxation generator's p_nom loosen a constraint:
# subtracting it from a "<=" raises the cap, adding it to a ">=" lowers the
# floor. No single sign loosens an "==", so the network_expansion_options
# schema forbids relaxing one and it is absent here.
_CONSTRAINT_TYPE_TO_RELAXATION_COEFFICIENT = {"<=": -1.0, ">=": 1.0}

# Working column orders before constraint_id is renamed to constraint_name.
_INTERNAL_LHS_COLUMNS = ["constraint_id"] + [
    c for c in _LHS_COLUMNS if c != "constraint_name"
]
_INTERNAL_RHS_COLUMNS = ["constraint_id"] + [
    c for c in _RHS_COLUMNS if c != "constraint_name"
]


def _concat_non_empty(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    """Concatenates the non-empty frames (concatenating all-empty frames is
    deprecated by pandas), returning a header-only frame when all are empty.

    I/O Example:
        [empty, 2-row frame, empty], columns -> the 2-row frame
        [empty, empty], columns              -> header-only frame with columns
    """
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=columns)
    return pd.concat(non_empty, ignore_index=True)


def _translate_custom_constraints_from_network_tables(
    ispypsa_tables: dict[str, pd.DataFrame],
    links: pd.DataFrame,
    config: ModelConfig,
) -> dict[str, pd.DataFrame]:
    """Translates the custom-constraint tables and builds the endogenous
    expansion-limit constraints.

    Consumes the custom_constraints, custom_constraints_lhs,
    custom_constraints_rhs, network_expansion_options and
    network_transmission_path_expansion_costs tables, plus the PyPSA friendly
    links table from ispypsa.translator.network (existing plus expansion
    links). Generator, storage and load terms pass through with their IASR
    IDs as variable_names, unchecked: the custom_constraints_lhs schema ties
    every variable_name to its component table.

    I/O Example (config: investment periods 2026 and 2028):
        ispypsa_tables["custom_constraints"]:
            constraint_id  direction
            SWQLD1         <=

        ispypsa_tables["custom_constraints_rhs"]:
            constraint_id  timeslice        rhs   date_from
            SWQLD1         qld_peak_demand  3000

        ispypsa_tables["custom_constraints_lhs"]:
            constraint_id  term_type         variable_name  coefficient  date_from
            SWQLD1         link_flow         NSW-QLD        0.84
            SWQLD1         generator_output  KINGASF1       0.14

        ispypsa_tables["network_expansion_options"]:
            expansion_id  expansion_type         allowed_expansion  expansion_option
            NSW-QLD       forward                1000               Option 1
            NSW-QLD       reverse                900                Option 1
            SWQLD1        constraint_relaxation  400                Option 2

        ispypsa_tables["network_transmission_path_expansion_costs"]:
            expansion_id  year  cost
            NSW-QLD       2026  500000
            SWQLD1        2026  100000

        links:
            isp_name  name              p_nom_extendable
            NSW-QLD   NSW-QLD_existing  False
            NSW-QLD   NSW-QLD_exp_2026  True

        returns["custom_constraints_rhs"]:
            constraint_name          investment_period  timeslice        rhs   constraint_type
            SWQLD1                   2026               qld_peak_demand  3000  <=
            SWQLD1                   2028               qld_peak_demand  3000  <=
            NSW-QLD_expansion_limit                                      1000  <=
            SWQLD1_expansion_limit                                       400   <=

        returns["custom_constraints_lhs"] (2028 rows mirror 2026):
            constraint_name          investment_period  variable_name     component  attribute  coefficient
            SWQLD1                   2026               NSW-QLD_existing  Link       p          0.84
            SWQLD1                   2026               NSW-QLD_exp_2026  Link       p          0.84
            SWQLD1                   2026               KINGASF1          Generator  p          0.14
            SWQLD1                   2026               SWQLD1_exp_2026   Generator  p_nom      -1.0
            NSW-QLD_expansion_limit                     NSW-QLD_exp_2026  Link       p_nom      1.0
            SWQLD1_expansion_limit                      SWQLD1_exp_2026   Generator  p_nom      1.0

        returns["custom_constraints_generators"] (abridged):
            name             isp_name  bus                             p_nom  build_year  capital_cost
            SWQLD1_exp_2026  SWQLD1    bus_for_custom_constraint_gens  0.0    2026        annuitise(100000)
    """
    period_starts = _investment_period_start_dates(
        config.temporal.capacity_expansion.investment_periods,
        config.temporal.year_type,
    )
    rhs = _resolve_values_active_at_period_starts(
        ispypsa_tables["custom_constraints_rhs"],
        ["constraint_id", "timeslice"],
        period_starts,
    )
    rhs = _add_constraint_type(rhs, ispypsa_tables["custom_constraints"])
    lhs = _resolve_values_active_at_period_starts(
        ispypsa_tables["custom_constraints_lhs"],
        ["constraint_id", "term_type", "variable_name"],
        period_starts,
    )
    lhs = _add_component_and_attribute(lhs)
    lhs = _expand_link_flow_terms(lhs, links)
    lhs, rhs = _drop_one_sided_constraint_periods(lhs, rhs)

    relaxation_generators = _create_constraint_relaxation_generators(
        ispypsa_tables, sorted(set(rhs["constraint_id"])), config
    )
    relaxation_generator_lhs = _relaxation_generator_lhs_terms(
        relaxation_generators, rhs
    )
    path_caps = _resolve_path_expansion_caps(
        ispypsa_tables["network_expansion_options"], links
    )
    expansion_limit_lhs, expansion_limit_rhs = _create_expansion_limit_constraints(
        links, relaxation_generators, path_caps
    )

    lhs = _concat_non_empty(
        [lhs, relaxation_generator_lhs, expansion_limit_lhs], _INTERNAL_LHS_COLUMNS
    )
    rhs = _concat_non_empty([rhs, expansion_limit_rhs], _INTERNAL_RHS_COLUMNS)
    lhs, rhs = _finalise_lhs_and_rhs(lhs, rhs)
    relaxation_generators = _finalise_generators(relaxation_generators)
    return {
        "custom_constraints_lhs": lhs,
        "custom_constraints_rhs": rhs,
        "custom_constraints_generators": relaxation_generators,
    }


def _investment_period_start_dates(
    investment_periods: list[int], year_type: str
) -> dict[int, pd.Timestamp]:
    """The datetime each investment period starts at.

    I/O Example:
        [2030], "fy"       -> {2030: 2029-07-01}  # FY ending nomenclature
        [2030], "calendar" -> {2030: 2030-01-01}
    """
    if year_type == "fy":
        return {p: pd.Timestamp(year=p - 1, month=7, day=1) for p in investment_periods}
    return {p: pd.Timestamp(year=p, month=1, day=1) for p in investment_periods}


def _resolve_values_active_at_period_starts(
    table: pd.DataFrame,
    group_columns: list[str],
    period_starts: dict[int, pd.Timestamp],
) -> pd.DataFrame:
    """Resolves date_from-varying rows into one row per investment period.

    For each period, each group keeps the row active at the period's start:
    the latest date_from on or before it, with no-date_from rows acting as
    the baseline. A group whose earliest date_from is after a period's start
    contributes no row for that period.

    I/O Example:
        table (group_columns=["constraint_id", "timeslice"]):
            constraint_id  timeslice        rhs   date_from
            SWQLD1         qld_peak_demand  3000
            SWQLD1         qld_peak_demand  2500  2032-12-01T00:00:00

        period_starts={2030: 2029-07-01, 2035: 2034-07-01} returns:
            constraint_id  timeslice        rhs   investment_period
            SWQLD1         qld_peak_demand  3000  2030
            SWQLD1         qld_peak_demand  2500  2035  # 2032 value held from period start
    """
    table = table.copy()
    table["date_from"] = pd.to_datetime(table["date_from"]).fillna(pd.Timestamp.min)
    resolved = []
    for period, start in period_starts.items():
        active = table[table["date_from"] <= start]
        active = active.sort_values("date_from")
        active = active.groupby(group_columns, dropna=False).tail(1).copy()
        active["investment_period"] = period
        resolved.append(active)
    return pd.concat(resolved, ignore_index=True).drop(columns="date_from")


def _add_constraint_type(
    rhs: pd.DataFrame, custom_constraints: pd.DataFrame
) -> pd.DataFrame:
    """Adds each constraint's sense as constraint_type ("=" becomes "==",
    matching the vocabulary pypsa_build applies constraints with).

    I/O Example:
        rhs:
            constraint_id  timeslice        rhs   investment_period
            SWQLD1         qld_peak_demand  3000  2026
            NQ1            qld_peak_demand  2650  2026

        custom_constraints:
            constraint_id  direction
            SWQLD1         <=
            NQ1            =

        returns:
            constraint_id  timeslice        rhs   investment_period  constraint_type
            SWQLD1         qld_peak_demand  3000  2026               <=
            NQ1            qld_peak_demand  2650  2026               ==
    """
    rhs = rhs.merge(custom_constraints, on="constraint_id", how="left")
    rhs["constraint_type"] = rhs["direction"].map(_DIRECTION_TO_CONSTRAINT_TYPE)
    return rhs.drop(columns="direction")


def _add_component_and_attribute(lhs: pd.DataFrame) -> pd.DataFrame:
    """Maps each term_type to the PyPSA component and attribute its variable
    belongs to.

    I/O Example:
        lhs:
            constraint_id  term_type         variable_name    coefficient  investment_period
            SWQLD1         link_flow         NSW-QLD          0.84         2026
            SWQLD1         generator_output  KINGASF1         0.14         2026
            SWQLD1         storage_output    Q8 Battery - 2h  0.43         2026

        returns:
            constraint_id  variable_name    coefficient  investment_period  component  attribute
            SWQLD1         NSW-QLD          0.84         2026               Link       p
            SWQLD1         KINGASF1         0.14         2026               Generator  p
            SWQLD1         Q8 Battery - 2h  0.43         2026               Storage    p
    """
    lhs = lhs.copy()
    lhs["component"] = lhs["term_type"].map(
        _CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE
    )
    lhs["attribute"] = lhs["term_type"].map(
        _CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE
    )
    _raise_on_unmapped_term_types(lhs)
    return lhs.drop(columns="term_type")


def _raise_on_unmapped_term_types(lhs: pd.DataFrame) -> None:
    """Raise if any term_type has no component mapping — silently dropping a
    term would weaken its constraint."""
    unmapped = lhs.loc[lhs["component"].isna(), "term_type"]
    if not unmapped.empty:
        raise ValueError(
            f"Custom constraint LHS term_types with no component mapping: "
            f"{sorted(set(unmapped))}"
        )


def _expand_link_flow_terms(lhs: pd.DataFrame, links: pd.DataFrame) -> pd.DataFrame:
    """Replaces each link term's path_id with the model's link names — the
    existing link plus each expansion link — one term per link. Terms for
    paths not in the model are dropped and logged.

    I/O Example:
        lhs:
            constraint_id  variable_name  component  coefficient
            SWQLD1         NSW-QLD        Link       0.84
            SWQLD1         KINGASF1       Generator  0.14

        links (isp_name -> name): NSW-QLD -> NSW-QLD_existing, NSW-QLD_exp_2030

        returns:
            constraint_id  variable_name     component  coefficient
            SWQLD1         KINGASF1          Generator  0.14
            SWQLD1         NSW-QLD_existing  Link       0.84
            SWQLD1         NSW-QLD_exp_2030  Link       0.84
    """
    link_terms = lhs[lhs["component"] == "Link"]
    other_terms = lhs[lhs["component"] != "Link"]
    _log_link_terms_not_in_model(link_terms, links)
    expanded = link_terms.merge(
        links.loc[:, ["isp_name", "name"]], left_on="variable_name", right_on="isp_name"
    )
    expanded = expanded.drop(columns=["variable_name", "isp_name"])
    expanded = expanded.rename(columns={"name": "variable_name"})
    return pd.concat([other_terms, expanded], ignore_index=True)


def _log_link_terms_not_in_model(link_terms: pd.DataFrame, links: pd.DataFrame) -> None:
    """Logs the link_flow terms whose path has no link in the model (they are
    dropped by the merge in _expand_link_flow_terms)."""
    missing = set(link_terms["variable_name"]) - set(links["isp_name"])
    if missing:
        logger.info(
            f"Custom constraint link_flow terms dropped (paths not in model): "
            f"{sorted(missing)}"
        )


def _drop_one_sided_constraint_periods(
    lhs: pd.DataFrame, rhs: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Keeps each constraint only in the investment periods where it has both
    LHS terms and an RHS row, dropping (and logging) the one-sided periods.

    A period is one-sided when one side's date_from starts later than the
    other's, or when every LHS term was dropped because its path is not in
    the model. Either way the constraint can't be applied in that period.

    I/O Example:
        lhs (abridged):                       rhs (abridged):
            constraint_id  investment_period     constraint_id  investment_period
            SWQLD1         2028                  SWQLD1         2026
            NQ1            2026                  SWQLD1         2028
            NQ1            2028                  NQ1            2026

        returns:
            lhs without its NQ1 2028 term (no RHS row that period; logged)
            rhs without its SWQLD1 2026 row (no LHS terms that period; logged)
    """
    keys = ["constraint_id", "investment_period"]
    lhs_periods = lhs.loc[:, keys].drop_duplicates()
    rhs_periods = rhs.loc[:, keys].drop_duplicates()
    _log_one_sided_periods(
        rhs_periods, lhs_periods, "RHS rows dropped (no LHS terms in that period)"
    )
    _log_one_sided_periods(
        lhs_periods, rhs_periods, "LHS terms dropped (no RHS row in that period)"
    )
    two_sided = lhs_periods.merge(rhs_periods, on=keys)
    return lhs.merge(two_sided, on=keys), rhs.merge(two_sided, on=keys)


def _log_one_sided_periods(
    present: pd.DataFrame, required: pd.DataFrame, message: str
) -> None:
    """Logs the (constraint_id, investment_period) pairs in present that have
    no partner in required — the pairs _drop_one_sided_constraint_periods is
    about to drop."""
    unmatched = present.merge(required, how="left", indicator=True)
    unmatched = unmatched[unmatched["_merge"] == "left_only"]
    if not unmatched.empty:
        pairs = zip(unmatched["constraint_id"], unmatched["investment_period"])
        logger.info(
            f"Custom constraint {message}: {sorted((c, int(p)) for c, p in pairs)}"
        )


def _create_constraint_relaxation_generators(
    ispypsa_tables: dict[str, pd.DataFrame],
    constraint_ids: list[str],
    config: ModelConfig,
) -> pd.DataFrame:
    """Builds one extendable dummy generator per relaxable constraint and
    investment period, with the selected expansion option's annualised cost.

    The generators' p_nom enters the parent constraint's LHS with a sign set
    by the constraint's direction (see _relaxation_generator_lhs_terms), so
    building them relaxes the constraint at the option's cost; total
    relaxation is capped at the option's allowed_expansion by the
    expansion-limit constraints.

    Options and costs may use blank key cells as wildcards: a blank
    expansion_id means "every constraint" — here, every constraint in the
    model (constraint_ids) — and a blank cost year means "every investment
    period", so a single blank-id row gives all constraints the same
    relaxation option or cost, and a blank-year cost row is a static cost
    across the periods. Every constraint with an option has a cost in every
    investment period (the costs schema's coverage rule), so joining the two
    gives exactly one generator per option and period. If the config's
    rez_transmission_expansion flag is off, no relaxation generators are
    built at all.

    I/O Example (blank cells are wildcards):
        ispypsa_tables["network_expansion_options"]:
            expansion_id  expansion_type         allowed_expansion  expansion_option
            SWQLD1        constraint_relaxation  500                Option 2
                          constraint_relaxation  200                Default

        ispypsa_tables["network_transmission_path_expansion_costs"]:
            expansion_id  year  cost
            SWQLD1        2030  100000
                                80000    # every constraint, every period

        constraint_ids = ["SWQLD1", "NQ1"]

        config: rez_transmission_expansion = True, investment_periods = [2030, 2040]

        returns:
            name             isp_name  bus                             p_nom  p_nom_extendable  build_year  lifetime  capital_cost       allowed_expansion
            SWQLD1_exp_2030  SWQLD1    bus_for_custom_constraint_gens  0.0    True              2030        inf       annuitise(100000)  500
            SWQLD1_exp_2040  SWQLD1    bus_for_custom_constraint_gens  0.0    True              2040        inf       annuitise(80000)   500
            NQ1_exp_2030     NQ1       bus_for_custom_constraint_gens  0.0    True              2030        inf       annuitise(80000)   200
            NQ1_exp_2040     NQ1       bus_for_custom_constraint_gens  0.0    True              2040        inf       annuitise(80000)   200
    """
    if not config.network.rez_transmission_expansion:
        return pd.DataFrame(columns=_GENERATOR_COLUMNS + ["allowed_expansion"])
    relaxations = _resolve_relaxation_options(
        ispypsa_tables["network_expansion_options"], constraint_ids
    )
    costs = _prepare_expansion_costs(
        ispypsa_tables["network_transmission_path_expansion_costs"],
        constraint_ids,
        config.temporal.capacity_expansion.investment_periods,
        config.wacc,
        config.network.annuitisation_lifetime,
    )
    generators = costs.merge(
        relaxations.loc[:, ["expansion_id", "allowed_expansion"]], on="expansion_id"
    )
    return _format_relaxation_generators(generators)


def _resolve_relaxation_options(
    options: pd.DataFrame, constraint_ids: list[str]
) -> pd.DataFrame:
    """Resolves the expansion-options wildcards to one constraint_relaxation
    row per constraint in the model that has an option.

    The physical forward/reverse rows are set aside first (they become
    expansion links in ispypsa.translator.network); a blank expansion_type
    covers constraint_relaxation too, so it is kept. Options for constraints
    not in the model are dropped, then _resolve_wildcards fans blank cells out
    against the model's constraints, most specific row winning.

    I/O Example (blank cells are wildcards):
        options:
            expansion_id  expansion_type         allowed_expansion  expansion_option
            CQ-NQ         forward                1000               BigLine   # physical: set aside
            SWQLD1        constraint_relaxation  400                Relax
                          constraint_relaxation  200                Default   # blank id: every constraint

        constraint_ids = ["SWQLD1", "SWV1"]

        returns:
            expansion_id  expansion_type         allowed_expansion  expansion_option
            SWQLD1        constraint_relaxation  400                Relax
            SWV1          constraint_relaxation  200                Default
    """
    expansion_type = options["expansion_type"]
    options = options[
        expansion_type.isna() | (expansion_type == "constraint_relaxation")
    ]
    options = _keep_rows_for_expansion_ids(options, constraint_ids)
    allowed_values = {
        "expansion_id": constraint_ids,
        "expansion_type": ["constraint_relaxation"],
    }
    return _resolve_wildcards(
        options, allowed_values, ["allowed_expansion", "expansion_option"]
    )


def _format_relaxation_generators(generators: pd.DataFrame) -> pd.DataFrame:
    """Adds the PyPSA generator attributes shared by all relaxation generators.

    I/O Example:
        generators:
            expansion_id  year  capital_cost  allowed_expansion
            SWQLD1        2030  8140          500
            SWQLD1        2040  7900          500

        returns:
            name             isp_name  bus                             p_nom  p_nom_extendable  build_year  lifetime  capital_cost  allowed_expansion
            SWQLD1_exp_2030  SWQLD1    bus_for_custom_constraint_gens  0.0    True              2030        inf       8140          500
            SWQLD1_exp_2040  SWQLD1    bus_for_custom_constraint_gens  0.0    True              2040        inf       7900          500
    """
    generators = generators.rename(columns={"expansion_id": "isp_name"})
    generators["name"] = (
        generators["isp_name"] + "_exp_" + generators["year"].astype(str)
    )
    generators["bus"] = "bus_for_custom_constraint_gens"
    generators["p_nom"] = 0.0
    generators["p_nom_extendable"] = True
    generators["build_year"] = generators["year"]
    generators["lifetime"] = np.inf
    return generators.loc[:, _GENERATOR_COLUMNS + ["allowed_expansion"]]


def _relaxation_generator_lhs_terms(
    relaxation_generators: pd.DataFrame, rhs: pd.DataFrame
) -> pd.DataFrame:
    """LHS terms letting each relaxation generator's capacity loosen its
    parent constraint.

    A term exists for each period the parent constraint has an RHS row in,
    and only for generators already built by that period — capacity built in
    a later period can't relax an earlier period's constraint. Each term's
    sign follows the parent constraint's direction (see
    _relaxation_coefficients) so that building capacity always loosens the
    constraint.

    I/O Example:
        relaxation_generators:
            name             isp_name  build_year
            SWQLD1_exp_2030  SWQLD1    2030
            SWQLD1_exp_2040  SWQLD1    2040
            NQ1_exp_2030     NQ1       2030

        rhs (abridged):
            constraint_id  investment_period  constraint_type
            SWQLD1         2030               <=
            SWQLD1         2040               <=
            NQ1            2040               >=      # NQ1 has no 2030 row

        returns:
            constraint_id  investment_period  variable_name    component  attribute  coefficient
            SWQLD1         2030               SWQLD1_exp_2030  Generator  p_nom      -1.0
            SWQLD1         2040               SWQLD1_exp_2030  Generator  p_nom      -1.0
            SWQLD1         2040               SWQLD1_exp_2040  Generator  p_nom      -1.0
            NQ1            2040               NQ1_exp_2030     Generator  p_nom      1.0   # ">=": added, lowering the floor
    """
    constraint_periods = rhs.loc[:, ["constraint_id", "investment_period"]]
    terms = constraint_periods.drop_duplicates().merge(
        relaxation_generators, left_on="constraint_id", right_on="isp_name"
    )
    terms = terms[terms["build_year"] <= terms["investment_period"]].copy()
    terms = terms.rename(columns={"name": "variable_name"})
    terms["component"] = "Generator"
    terms["attribute"] = "p_nom"
    terms["coefficient"] = _relaxation_coefficients(terms["constraint_id"], rhs)
    columns = [c for c in _LHS_COLUMNS if c != "constraint_name"] + ["constraint_id"]
    return terms.loc[:, columns]


def _relaxation_coefficients(constraint_ids: pd.Series, rhs: pd.DataFrame) -> pd.Series:
    """The LHS coefficient that lets a relaxation generator's p_nom loosen
    each constraint: -1.0 on a "<=" (subtracting from the LHS raises the cap)
    and +1.0 on a ">=" (adding to the LHS lowers the floor). No single-signed
    term can loosen an "==", so the network_expansion_options schema forbids
    relaxing one.

    I/O Example:
        constraint_ids: SWQLD1, SWQLD1, NQ1

        rhs (abridged):
            constraint_id  constraint_type
            SWQLD1         <=
            NQ1            >=

        returns: -1.0, -1.0, 1.0
    """
    constraint_type = rhs.drop_duplicates("constraint_id").set_index("constraint_id")[
        "constraint_type"
    ]
    return constraint_ids.map(constraint_type).map(
        _CONSTRAINT_TYPE_TO_RELAXATION_COEFFICIENT
    )


def _resolve_path_expansion_caps(
    options: pd.DataFrame, links: pd.DataFrame
) -> pd.DataFrame:
    """The capacity cap for each path with expansion links in the model:
    max(forward, reverse) of its resolved expansion option.

    Each expansion link's p_max_pu and p_min_pu are the option's forward and
    reverse capacities per unit of that max (see
    ispypsa.translator.network._build_expansion_links), so capping the p_nom
    built across a path's expansion links at the max delivers the option's
    full capacity in both directions. The paths with extendable links are the
    enabled elements the options wildcards resolve against — the same set
    ispypsa.translator.network resolved them for.

    I/O Example:
        options:
            expansion_id  expansion_type  allowed_expansion  expansion_option
            CQ-NQ         forward         800                BigLine
            CQ-NQ         reverse         1000               BigLine

        links:
            isp_name  name            p_nom_extendable
            CQ-NQ     CQ-NQ_existing  False
            CQ-NQ     CQ-NQ_exp_2030  True

        returns:
            expansion_id  allowed_expansion
            CQ-NQ         1000
    """
    expandable_ids = sorted(set(links.loc[links["p_nom_extendable"], "isp_name"]))
    options = _resolve_expansion_options(options, expandable_ids)
    options = _pair_forward_and_reverse_options(options)
    caps = options.loc[:, ["expansion_id"]].copy()
    caps["allowed_expansion"] = options[["forward_capacity", "reverse_capacity"]].max(
        axis=1
    )
    return caps


def _create_expansion_limit_constraints(
    links: pd.DataFrame,
    relaxation_generators: pd.DataFrame,
    path_caps: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Caps the total capacity built across each expandable element's
    per-period components at the selected option's capacity.

    For physical paths the cap is path_caps' max(forward, reverse); for
    constraint relaxations it is the option's allowed_expansion carried on the
    relaxation generators. The constraints have no investment_period or
    timeslice — they apply to the p_nom variables globally. Names get an
    "_expansion_limit" suffix so a relaxation cap doesn't collide with the
    constraint it relaxes.

    I/O Example:
        links:
            isp_name  name            p_nom_extendable
            CQ-NQ     CQ-NQ_existing  False
            CQ-NQ     CQ-NQ_exp_2030  True
            CQ-NQ     CQ-NQ_exp_2040  True

        relaxation_generators:
            name             isp_name  allowed_expansion
            SWQLD1_exp_2030  SWQLD1    500

        path_caps:
            expansion_id  allowed_expansion
            CQ-NQ         1000

        returns lhs:
            constraint_id           variable_name    component  attribute  coefficient  investment_period
            CQ-NQ_expansion_limit   CQ-NQ_exp_2030   Link       p_nom      1.0          NaN
            CQ-NQ_expansion_limit   CQ-NQ_exp_2040   Link       p_nom      1.0          NaN
            SWQLD1_expansion_limit  SWQLD1_exp_2030  Generator  p_nom      1.0          NaN

        and rhs:
            constraint_id           rhs   constraint_type  investment_period  timeslice
            CQ-NQ_expansion_limit   1000  <=               NaN                NaN
            SWQLD1_expansion_limit  500   <=               NaN                NaN
    """
    lhs = pd.concat(
        [
            _expansion_limit_lhs(links[links["p_nom_extendable"]], "Link"),
            _expansion_limit_lhs(relaxation_generators, "Generator"),
        ],
        ignore_index=True,
    )
    relaxation_caps = relaxation_generators.loc[:, ["isp_name", "allowed_expansion"]]
    relaxation_caps = relaxation_caps.rename(columns={"isp_name": "expansion_id"})
    caps = pd.concat([path_caps, relaxation_caps.drop_duplicates()], ignore_index=True)
    rhs = _expansion_limit_rhs(caps)
    lhs["constraint_id"] = lhs["constraint_id"] + "_expansion_limit"
    rhs["constraint_id"] = rhs["constraint_id"] + "_expansion_limit"
    return lhs, rhs


def _expansion_limit_lhs(components: pd.DataFrame, component_type: str) -> pd.DataFrame:
    """One LHS term per expandable component, summing p_nom across the
    investment periods of its parent element.

    I/O Example:
        components: name=CQ-NQ_exp_2030, isp_name=CQ-NQ; component_type="Link"
        -> constraint_id=CQ-NQ, variable_name=CQ-NQ_exp_2030, component=Link,
           attribute=p_nom, coefficient=1.0, investment_period=NaN
    """
    lhs = components.loc[:, ["isp_name", "name"]].copy()
    lhs = lhs.rename(columns={"isp_name": "constraint_id", "name": "variable_name"})
    lhs["component"] = component_type
    lhs["attribute"] = "p_nom"
    lhs["coefficient"] = 1.0
    lhs["investment_period"] = np.nan
    return lhs


def _expansion_limit_rhs(caps: pd.DataFrame) -> pd.DataFrame:
    """One RHS row per expandable element, capping its components' total p_nom
    at the element's allowed_expansion.

    I/O Example:
        caps:
            expansion_id  allowed_expansion
            CQ-NQ         1000
            SWQLD1        500

        returns:
            constraint_id  rhs   constraint_type  investment_period  timeslice
            CQ-NQ          1000  <=               NaN                NaN
            SWQLD1         500   <=               NaN                NaN
    """
    rhs = caps.rename(columns={"expansion_id": "constraint_id"}).copy()
    rhs["rhs"] = rhs["allowed_expansion"]
    rhs["constraint_type"] = "<="
    rhs["investment_period"] = np.nan
    rhs["timeslice"] = np.nan
    columns = [c for c in _RHS_COLUMNS if c != "constraint_name"] + ["constraint_id"]
    return rhs.loc[:, columns]


def _finalise_lhs_and_rhs(
    lhs: pd.DataFrame, rhs: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Renames constraint_id to constraint_name, rejects duplicate constraint
    names, and sets the final PyPSA friendly column orders.

    I/O Example:
        lhs: constraint_id=SWQLD1, ...  rhs: constraint_id=SWQLD1, ...
        -> lhs.columns == _LHS_COLUMNS, rhs.columns == _RHS_COLUMNS, both
           keyed by constraint_name=SWQLD1
    """
    lhs = lhs.rename(columns={"constraint_id": "constraint_name"})
    rhs = rhs.rename(columns={"constraint_id": "constraint_name"})
    _raise_on_duplicate_rhs_rows(rhs)
    return (
        lhs.loc[:, _LHS_COLUMNS].reset_index(drop=True),
        rhs.loc[:, _RHS_COLUMNS].reset_index(drop=True),
    )


def _raise_on_duplicate_rhs_rows(rhs: pd.DataFrame) -> None:
    """Raise on duplicate (constraint, period, timeslice) RHS rows — pypsa_build
    would create two constraints with the same name."""
    keys = ["constraint_name", "investment_period", "timeslice"]
    duplicates = rhs[rhs.duplicated(subset=keys, keep=False)]
    if not duplicates.empty:
        raise ValueError(
            f"Duplicate custom constraint RHS rows for: "
            f"{sorted(set(duplicates['constraint_name']))}"
        )


def _finalise_generators(relaxation_generators: pd.DataFrame) -> pd.DataFrame:
    """Drops the allowed_expansion working column carried for the
    expansion-limit RHS, leaving the PyPSA generator columns.

    I/O Example:
        columns [*_GENERATOR_COLUMNS, allowed_expansion] -> _GENERATOR_COLUMNS
    """
    return relaxation_generators.loc[:, _GENERATOR_COLUMNS].reset_index(drop=True)
