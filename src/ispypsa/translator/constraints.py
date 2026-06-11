"""Translates the new-format custom-constraint tables into PyPSA friendly form.

Consumes ``custom_constraints`` (constraint senses), ``custom_constraints_lhs``
(terms) and ``custom_constraints_rhs`` (limit values), plus the unified
network expansion tables for constraint-relaxation generators and
expansion-limit constraints.

The PyPSA friendly outputs carry two temporal columns the old format lacked:

- ``investment_period``: time-varying inputs (``date_from``) are resolved by
  holding, for each investment period, the value active at the period's
  start. Rows with a NaN investment_period apply in every period.
- ``timeslice`` (RHS only): the constraint binds only at snapshots inside the
  timeslice's windows (see ispypsa.translator.timeslices). NaN means the
  limit applies at all snapshots.
"""

import logging

import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.mappings import (
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_ATTRIBUTE_TYPE,
    _CUSTOM_CONSTRAINT_TERM_TYPE_TO_COMPONENT_TYPE,
)
from ispypsa.translator.network import (
    _pivot_physical_expansion_options,
    _prepare_expansion_costs,
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

# Working column orders before constraint_id is renamed to constraint_name.
_INTERNAL_LHS_COLUMNS = ["constraint_id"] + [
    c for c in _LHS_COLUMNS if c != "constraint_name"
]
_INTERNAL_RHS_COLUMNS = ["constraint_id"] + [
    c for c in _RHS_COLUMNS if c != "constraint_name"
]


def _raise_on_duplicate_input_rows(
    table: pd.DataFrame, keys: list[str], label: str
) -> None:
    """Raise on input rows sharing the same key — duplicates would otherwise
    silently collapse to one arbitrary row during date_from resolution."""
    duplicates = table[table.duplicated(subset=keys, keep=False)]
    if not duplicates.empty:
        raise ValueError(
            f"Duplicate custom constraint {label} rows for: "
            f"{sorted(set(duplicates['constraint_id']))}"
        )


def _concat_non_empty(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    """Concatenates the non-empty frames (concatenating all-empty frames is
    deprecated by pandas), returning a header-only frame when all are empty."""
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=columns)
    return pd.concat(non_empty, ignore_index=True)


def _translate_custom_constraints_from_network_tables(
    ispypsa_tables: dict[str, pd.DataFrame],
    links: pd.DataFrame,
    config: ModelConfig,
    generators: pd.DataFrame | None = None,
    batteries: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Translates the custom-constraint tables and builds the endogenous
    expansion-limit constraints.

    Generator and battery terms pass through with their IASR IDs as
    variable_names: pypsa_build skips (and logs) terms whose components are
    not in the model. The ``generators`` and ``batteries`` arguments are
    accepted so the templated generator/battery tables can be used to map
    those IDs to model component names once generator templating lands; they
    are currently unused.

    Args:
        ispypsa_tables: dictionary of new-format `ISPyPSA` input tables;
            consumes custom_constraints, custom_constraints_lhs,
            custom_constraints_rhs, network_expansion_options and
            network_transmission_path_expansion_costs.
        links: PyPSA friendly links table from
            ispypsa.translator.network (existing plus expansion links).
        config: `ispypsa.config.ModelConfig` object.
        generators: templated generator table (future name-mapping hook).
        batteries: templated battery table (future name-mapping hook).

    Returns: dict with the PyPSA friendly custom_constraints_lhs,
        custom_constraints_rhs and custom_constraints_generators tables.
    """
    _raise_on_duplicate_input_rows(
        ispypsa_tables["custom_constraints_rhs"],
        ["constraint_id", "timeslice", "date_from"],
        "RHS",
    )
    _raise_on_duplicate_input_rows(
        ispypsa_tables["custom_constraints_lhs"],
        ["constraint_id", "term_type", "variable_name", "date_from"],
        "LHS",
    )
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
    rhs = _drop_rhs_without_lhs_terms(rhs, lhs)

    relaxation_generators = _create_constraint_relaxation_generators(
        ispypsa_tables, set(rhs["constraint_id"]), config
    )
    relaxation_generator_lhs = _relaxation_generator_lhs_terms(
        relaxation_generators,
        config.temporal.capacity_expansion.investment_periods,
    )
    expansion_limit_lhs, expansion_limit_rhs = _create_expansion_limit_constraints(
        links, relaxation_generators, ispypsa_tables["network_expansion_options"]
    )

    lhs = _concat_non_empty(
        [lhs, relaxation_generator_lhs, expansion_limit_lhs], _INTERNAL_LHS_COLUMNS
    )
    rhs = _concat_non_empty([rhs, expansion_limit_rhs], _INTERNAL_RHS_COLUMNS)
    lhs, rhs = _finalise_lhs_and_rhs(lhs, rhs)
    return {
        "custom_constraints_lhs": lhs,
        "custom_constraints_rhs": rhs,
        "custom_constraints_generators": _finalise_generators(relaxation_generators),
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
        rhs: constraint_id=SWQLD1, rhs=3000
        custom_constraints: constraint_id=SWQLD1, direction="<="
        -> constraint_id=SWQLD1, rhs=3000, constraint_type="<="
    """
    rhs = rhs.merge(custom_constraints, on="constraint_id", how="left")
    rhs["constraint_type"] = rhs["direction"].map(_DIRECTION_TO_CONSTRAINT_TYPE)
    _raise_on_missing_constraint_type(rhs)
    return rhs.drop(columns="direction")


def _raise_on_missing_constraint_type(rhs: pd.DataFrame) -> None:
    """Raise if any RHS row's constraint has no (or an unmapped) sense — the
    constraint can't be applied without one."""
    missing = rhs.loc[rhs["constraint_type"].isna(), "constraint_id"]
    if not missing.empty:
        raise ValueError(
            f"Custom constraints with RHS values but no direction in the "
            f"custom_constraints table: {sorted(set(missing))}"
        )


def _add_component_and_attribute(lhs: pd.DataFrame) -> pd.DataFrame:
    """Maps each term_type to the PyPSA component and attribute its variable
    belongs to.

    I/O Example:
        term_type=generator_output -> component=Generator, attribute=p
        term_type=link_flow        -> component=Link,      attribute=p
        term_type=storage_output   -> component=Storage,   attribute=p
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
    missing = set(link_terms["variable_name"]) - set(links["isp_name"])
    if missing:
        logger.info(
            f"Custom constraint link_flow terms dropped (paths not in model): "
            f"{sorted(missing)}"
        )


def _drop_rhs_without_lhs_terms(rhs: pd.DataFrame, lhs: pd.DataFrame) -> pd.DataFrame:
    """Drops (and logs) RHS rows for constraints left with no LHS terms — a
    constraint with an empty LHS can't be applied."""
    without_lhs = set(rhs["constraint_id"]) - set(lhs["constraint_id"])
    if without_lhs:
        logger.info(
            f"Custom constraints dropped (no LHS terms in model): {sorted(without_lhs)}"
        )
    return rhs[~rhs["constraint_id"].isin(without_lhs)]


def _create_constraint_relaxation_generators(
    ispypsa_tables: dict[str, pd.DataFrame],
    constraint_ids: set[str],
    config: ModelConfig,
) -> pd.DataFrame:
    """Builds one extendable dummy generator per relaxable constraint and
    investment period, with the selected expansion option's annualised cost.

    The generators' p_nom enters the parent constraint's LHS with coefficient
    -1.0 (see _relaxation_generator_lhs_terms), so building them relaxes the
    constraint at the option's cost; total relaxation is capped at the
    option's allowed_expansion by the expansion-limit constraints.

    I/O Example:
        network_expansion_options:
            expansion_id  expansion_type         allowed_expansion
            SWQLD1        constraint_relaxation  500

        network_transmission_path_expansion_costs:
            expansion_id  year  cost
            SWQLD1        2030  100000

        constraint_ids={"SWQLD1"}, investment_periods=[2030] returns (abridged):
            name             isp_name  bus                            p_nom  build_year
            SWQLD1_exp_2030  SWQLD1    bus_for_custom_constraint_gens 0.0    2030
    """
    if not config.network.rez_transmission_expansion:
        return pd.DataFrame(columns=_GENERATOR_COLUMNS + ["allowed_expansion"])
    options = ispypsa_tables["network_expansion_options"]
    relaxations = options[options["expansion_type"] == "constraint_relaxation"]
    relaxations = _drop_relaxations_without_constraints(relaxations, constraint_ids)
    costs = _prepare_expansion_costs(
        ispypsa_tables["network_transmission_path_expansion_costs"],
        config.temporal.capacity_expansion.investment_periods,
        config.temporal.year_type,
        config.wacc,
        config.network.annuitisation_lifetime,
    )
    generators = costs.merge(
        relaxations.loc[:, ["expansion_id", "allowed_expansion"]], on="expansion_id"
    )
    return _format_relaxation_generators(generators)


def _drop_relaxations_without_constraints(
    relaxations: pd.DataFrame, constraint_ids: set[str]
) -> pd.DataFrame:
    """Drops (and logs) relaxation options whose constraint is not in the
    model — there is nothing for them to relax."""
    missing = set(relaxations["expansion_id"]) - constraint_ids
    if missing:
        logger.info(
            f"Constraint relaxation expansion options dropped (their constraints "
            f"are not in the model): {sorted(missing)}"
        )
    return relaxations[~relaxations["expansion_id"].isin(missing)]


def _format_relaxation_generators(generators: pd.DataFrame) -> pd.DataFrame:
    """Adds the PyPSA generator attributes shared by all relaxation generators.

    I/O Example:
        expansion_id=SWQLD1, year=2030, capital_cost=8140, allowed_expansion=500
        -> name=SWQLD1_exp_2030, isp_name=SWQLD1, p_nom=0.0,
           p_nom_extendable=True, build_year=2030, lifetime=inf
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
    relaxation_generators: pd.DataFrame, investment_periods: list[int]
) -> pd.DataFrame:
    """LHS terms subtracting each relaxation generator's capacity from its
    parent constraint.

    Terms are per investment period and only include generators already built
    by that period — capacity built in a later period can't relax an earlier
    period's constraint.

    I/O Example:
        relaxation_generators:
            name             isp_name  build_year
            SWQLD1_exp_2030  SWQLD1    2030
            SWQLD1_exp_2040  SWQLD1    2040

        investment_periods=[2030, 2040] returns:
            constraint_id  investment_period  variable_name    component  attribute  coefficient
            SWQLD1         2030               SWQLD1_exp_2030  Generator  p_nom      -1.0
            SWQLD1         2040               SWQLD1_exp_2030  Generator  p_nom      -1.0
            SWQLD1         2040               SWQLD1_exp_2040  Generator  p_nom      -1.0
    """
    terms = []
    for period in investment_periods:
        built = relaxation_generators[
            relaxation_generators["build_year"] <= period
        ].copy()
        built["investment_period"] = period
        terms.append(built)
    terms = pd.concat(terms, ignore_index=True)
    terms = terms.rename(columns={"isp_name": "constraint_id", "name": "variable_name"})
    terms["component"] = "Generator"
    terms["attribute"] = "p_nom"
    terms["coefficient"] = -1.0
    columns = [c for c in _LHS_COLUMNS if c != "constraint_name"] + ["constraint_id"]
    return terms.loc[:, columns]


def _create_expansion_limit_constraints(
    links: pd.DataFrame,
    relaxation_generators: pd.DataFrame,
    expansion_options: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Caps the total capacity built across each expandable element's
    per-period components at the selected option's capacity.

    For physical paths the cap is the option's forward capacity (reverse
    capacity scales with it through each expansion link's p_min_pu); for
    constraint relaxations it is the option's allowed_expansion. The
    constraints have no investment_period or timeslice — they apply to the
    p_nom variables globally. Names get an "_expansion_limit" suffix so a
    relaxation cap doesn't collide with the constraint it relaxes.

    I/O Example:
        links (extendable): CQ-NQ_exp_2030, CQ-NQ_exp_2040 (isp_name CQ-NQ)
        expansion_options: CQ-NQ forward 1000 / reverse 900

        returns lhs:
            constraint_id          variable_name   component  attribute  coefficient  investment_period
            CQ-NQ_expansion_limit  CQ-NQ_exp_2030  Link       p_nom      1.0          NaN
            CQ-NQ_expansion_limit  CQ-NQ_exp_2040  Link       p_nom      1.0          NaN

        and rhs:
            constraint_id          rhs   constraint_type  investment_period  timeslice
            CQ-NQ_expansion_limit  1000  <=               NaN                NaN
    """
    lhs = pd.concat(
        [
            _expansion_limit_lhs(links[links["p_nom_extendable"]], "Link"),
            _expansion_limit_lhs(relaxation_generators, "Generator"),
        ],
        ignore_index=True,
    )
    rhs = _expansion_limit_rhs(expansion_options, set(lhs["constraint_id"]))
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


def _expansion_limit_rhs(
    expansion_options: pd.DataFrame, expandable_ids: set[str]
) -> pd.DataFrame:
    """One RHS row per expandable element with components in the model: the
    selected option's forward capacity (physical paths) or allowed_expansion
    (constraint relaxations).

    I/O Example:
        expansion_options:
            expansion_id  expansion_type         allowed_expansion
            CQ-NQ         forward                1000
            CQ-NQ         reverse                900
            SWQLD1        constraint_relaxation  500

        expandable_ids={"CQ-NQ", "SWQLD1"} returns:
            constraint_id  rhs   constraint_type  investment_period  timeslice
            CQ-NQ          1000  <=               NaN                NaN
            SWQLD1         500   <=               NaN                NaN
    """
    caps = expansion_options[
        expansion_options["expansion_type"].isin(["forward", "constraint_relaxation"])
    ]
    caps = caps[caps["expansion_id"].isin(expandable_ids)]
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
    """Suffixes expansion-limit names, validates the pairing, and sets the
    final PyPSA friendly column orders."""
    lhs = lhs.rename(columns={"constraint_id": "constraint_name"})
    rhs = rhs.rename(columns={"constraint_id": "constraint_name"})
    _raise_on_duplicate_rhs_rows(rhs)
    _raise_on_unpaired_constraints(lhs, rhs)
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


def _raise_on_unpaired_constraints(lhs: pd.DataFrame, rhs: pd.DataFrame) -> None:
    """Raise if any constraint appears on only one side — a one-sided
    constraint can't be applied."""
    lhs_names = set(lhs["constraint_name"])
    rhs_names = set(rhs["constraint_name"])
    if lhs_names != rhs_names:
        raise ValueError(
            f"Custom constraints with LHS terms but no RHS: "
            f"{sorted(lhs_names - rhs_names)}; with RHS but no LHS terms: "
            f"{sorted(rhs_names - lhs_names)}"
        )


def _finalise_generators(relaxation_generators: pd.DataFrame) -> pd.DataFrame:
    """Drops the allowed_expansion working column carried for the
    expansion-limit RHS, leaving the PyPSA generator columns."""
    return relaxation_generators.loc[:, _GENERATOR_COLUMNS].reset_index(drop=True)
