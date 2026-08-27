import logging

import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.helpers import _period_start, _resolve_wildcards
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


def _translate_custom_constraints(
    ispypsa_tables: dict[str, pd.DataFrame],
    links: pd.DataFrame,
    generators: pd.DataFrame,
    storage: pd.DataFrame,
    demand_nodes: pd.DataFrame,
    config: ModelConfig,
) -> dict[str, pd.DataFrame]:
    """Translates the custom-constraint tables and appends the endogenous
    expansion-limit constraints, returning them in PyPSA friendly form.

    Three constraint blocks are produced in sequence and assembled here.
    _translate_constraint_tables turns the user-authored constraint tables
    into per-investment-period LHS/RHS rows. _create_constraint_relaxations
    adds extendable dummy generators that let the model buy relaxation of
    those constraints, with the LHS terms wiring them in.
    _create_expansion_limit_constraints caps the total capacity built for
    each expandable element — expansion links and relaxation generators
    alike. The blocks are concatenated and finalised once: constraint_id
    becomes constraint_name, and duplicate (constraint, period, timeslice)
    RHS rows across the assembled set raise a ValueError, since pypsa_build
    would build two constraints with the same name from them. Relaxation
    resolves against the assembled RHS,
    so a future producer of constraint blocks (e.g. new entrant build limits)
    should append before the relaxation step if its constraints are to be
    relaxable.

    Input integrity is the table schemas' job, not this module's. The rules the
    pipeline relies on without re-checking — unique input rows, a direction for
    every constraint with RHS values, the LHS and RHS naming the same
    constraints, every LHS variable_name matching an ID in one of the input
    tables for its term_type (e.g. a generator_output term names a generator
    in generators_existing_planned or generators_new_entrant), no
    constraint_relaxation option on an "=" constraint, and a cost for every
    expandable element in every investment period — are declared in
    src/ispypsa/validation/schemas (custom_constraints*.yaml,
    network_expansion_options.yaml and
    network_transmission_path_expansion_costs.yaml).

    How time-varying inputs land on the investment periods — the forward
    quantisation of date_from, build_year and lifetime — is described on
    _translate_constraint_tables.

    In the output tables, a blank investment_period means the row applies in
    every period. A named timeslice scopes the RHS to the snapshots inside that
    timeslice's windows (the timeslices table); a blank timeslice is the
    constraint's fallback, applying at the snapshots none of its named-timeslice
    rows cover, so a constraint with only named rows does not bind outside them.
    Timeslice values pass through untouched — resolving them to snapshots is
    pypsa_build's job when the constraints are applied.

    I/O Example (config: investment periods 2026 and 2028):
        ispypsa_tables["custom_constraints"]:
            constraint_id  direction
            SWQLD1         <=

        ispypsa_tables["custom_constraints_rhs"]:
            constraint_id  timeslice        rhs   date_from
            SWQLD1         qld_peak_demand  3000

        ispypsa_tables["custom_constraints_lhs"]:
            constraint_id  term_type         variable_name    coefficient  date_from
            SWQLD1         link_flow         NSW-QLD          0.84
            SWQLD1         generator_output  KINGASF1         0.14
            SWQLD1         generator_output  N2 Solar         0.5
            SWQLD1         storage_output    Q8 Battery - 2h  0.43
            SWQLD1         load              SQ               -0.33

        ispypsa_tables["network_expansion_options"]:
            expansion_id  expansion_type         allowed_expansion  expansion_option
            NSW-QLD       forward                1000               Option 1
            NSW-QLD       reverse                900                Option 1
            SWQLD1        constraint_relaxation  400                Option 2

        ispypsa_tables["network_transmission_path_expansion_costs"]:
            expansion_id  year  cost
            NSW-QLD       2026  500000
            NSW-QLD       2028  500000
            SWQLD1        2026  100000
            SWQLD1        2028  80000

        links:
            isp_name  name              p_nom_extendable  build_year  lifetime
            NSW-QLD   NSW-QLD_existing  False             2025        inf
            NSW-QLD   NSW-QLD_exp_2026  True              2026        inf

        generators (isp_name = name for existing units; a new entrant's ID
        maps to each of its per-build-year components):
            isp_name  name           build_year  lifetime
            KINGASF1  KINGASF1       2025        inf       # inf: never retires
            N2 Solar  N2 Solar_2026  2026        30
            N2 Solar  N2 Solar_2028  2028        30

        storage:
            isp_name         name             build_year  lifetime
            Q8 Battery - 2h  Q8 Battery - 2h  2025        inf
            SQ BESS          SQ BESS_2026     2026        20
            SQ BESS          SQ BESS_2028     2028        20

        demand_nodes:
            name
            SQ

        returns["custom_constraints_rhs"]:
            constraint_name          investment_period  timeslice        rhs   constraint_type
            SWQLD1                   2026               qld_peak_demand  3000  <=
            SWQLD1                   2028               qld_peak_demand  3000  <=
            NSW-QLD_expansion_limit                                      1000  <=
            SWQLD1_expansion_limit                                       400   <=

        returns["custom_constraints_lhs"] (2028 rows mirror 2026, and also
        pick up N2 Solar_2028 once it is built):
            constraint_name          investment_period  variable_name     component  attribute  coefficient
            SWQLD1                   2026               NSW-QLD_existing  Link       p          0.84
            SWQLD1                   2026               NSW-QLD_exp_2026  Link       p          0.84
            SWQLD1                   2026               KINGASF1          Generator  p          0.14
            SWQLD1                   2026               N2 Solar_2026     Generator  p          0.5   # N2 Solar_2028 enters in 2028 only
            SWQLD1                   2026               Q8 Battery - 2h   Storage    p          0.43
            SWQLD1                   2026               load_SQ           Load       p_set      -0.33
            SWQLD1                   2026               SWQLD1_exp_2026   Generator  p_nom      -1.0
            SWQLD1                   2028               SWQLD1_exp_2028   Generator  p_nom      -1.0  # relaxation accumulates
            NSW-QLD_expansion_limit                     NSW-QLD_exp_2026  Link       p_nom      1.0
            SWQLD1_expansion_limit                      SWQLD1_exp_2026   Generator  p_nom      1.0
            SWQLD1_expansion_limit                      SWQLD1_exp_2028   Generator  p_nom      1.0

        returns["custom_constraints_generators"] (abridged):
            name             isp_name  bus                             p_nom  build_year  capital_cost
            SWQLD1_exp_2026  SWQLD1    bus_for_custom_constraint_gens  0.0    2026        annuitise(100000)
            SWQLD1_exp_2028  SWQLD1    bus_for_custom_constraint_gens  0.0    2028        annuitise(80000)
    """
    lhs, rhs = _translate_constraint_tables(
        ispypsa_tables, links, generators, storage, demand_nodes, config
    )
    relaxation_generators, relaxation_lhs, relaxation_caps = (
        _create_constraint_relaxations(
            ispypsa_tables["network_expansion_options"],
            ispypsa_tables["network_transmission_path_expansion_costs"],
            rhs,
            config,
        )
    )
    expansion_limit_lhs, expansion_limit_rhs = _create_expansion_limit_constraints(
        ispypsa_tables["network_expansion_options"],
        links,
        relaxation_generators,
        relaxation_caps,
    )
    lhs = _concat_non_empty(
        [lhs, relaxation_lhs, expansion_limit_lhs], _INTERNAL_LHS_COLUMNS
    )
    rhs = _concat_non_empty([rhs, expansion_limit_rhs], _INTERNAL_RHS_COLUMNS)
    lhs, rhs = _finalise_lhs_and_rhs(lhs, rhs)
    return {
        "custom_constraints_lhs": lhs,
        "custom_constraints_rhs": rhs,
        "custom_constraints_generators": relaxation_generators,
    }


def _translate_constraint_tables(
    ispypsa_tables: dict[str, pd.DataFrame],
    links: pd.DataFrame,
    generators: pd.DataFrame,
    storage: pd.DataFrame,
    demand_nodes: pd.DataFrame,
    config: ModelConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Translates the ISPyPSA custom-constraint tables into one LHS term
    and one RHS row per constraint, investment period and (RHS only)
    timeslice, still keyed by constraint_id.

    Everything time-varying is quantised forward onto the investment-period
    sequence, mirroring how PyPSA's multi-investment-period optimisation
    treats the components themselves. A dated LHS or RHS value takes effect
    at the first period whose start falls on or after its date_from (the
    boundary is inclusive), so a change landing mid-period defers to the
    next period rather than reaching back into the one it landed in. A term
    is kept in exactly the periods PyPSA activates its component —
    build_year <= period < build_year + lifetime, compared against the
    period labels — so a component built between two periods joins the
    constraint at the next label, one retiring between two periods leaves at
    the next label, and the constraint never counts capacity or dispatch the
    model doesn't have.

    The translation steps:

        - the LHS and RHS values active during each investment period are
        resolved per the date_from quantisation above, blank date_from rows
        acting as the earliest values.
        - term_type values are mapped to PyPSA component and attribute
        combinations.
        - every LHS term must resolve to a component in the model. A term naming a
        component the configured model doesn't contain (e.g. a link_flow term when
        regional_granularity is single_region builds no links, or a load term when
        the granularity doesn't make its sub-region a demand node) raises, since
        applying the constraint without the term would silently alter it.
        - LHS terms are expanded from their input IDs into one term per matching
        model component (the constraint-variable-name to PyPSA-model-name mapping
        built from the links, generators, storage and demand_nodes tables): a
        link_flow term covers its path's existing link
        and each expansion link, a term on a new entrant generator or storage unit
        covers each of its per-build-year components, and a load term maps to
        the "load_<bus>" Load component at its demand node — a data term whose
        p_set attribute pypsa_build resolves from the demand trace, not an
        optimisation variable.
        - each term is then dropped in the investment periods outside its
        component's in-service window, per the activity quantisation above.
        Existing components' build years precede the horizon, an infinite
        lifetime never retires, and load terms carry neither year — all of
        these apply in every period.
        - LHS and RHS rows are dropped in investment periods where the constraint does
        not have both LHS terms and an RHS value. This happens when date_from coverage
        differs between the two sides (including a side whose earliest date_from falls
        after a period's start).

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

        links:
            isp_name  name              p_nom_extendable  build_year  lifetime
            NSW-QLD   NSW-QLD_existing  False             2025        inf
            NSW-QLD   NSW-QLD_exp_2026  True              2026        inf

        generators:
            isp_name  name           build_year  lifetime
            KINGASF1  KINGASF1       2025        3         # retires 2028: out of service from the 2028 period
            N2 Solar  N2 Solar_2026  2026        30
            N2 Solar  N2 Solar_2028  2028        30

        storage:
            isp_name         name             build_year  lifetime
            Q8 Battery - 2h  Q8 Battery - 2h  2025        inf
            SQ BESS          SQ BESS_2026     2026        20
            SQ BESS          SQ BESS_2028     2028        20

        demand_nodes:
            name
            SQ

        returns lhs:
            constraint_id  investment_period  variable_name     component  attribute  coefficient
            SWQLD1         2026               NSW-QLD_existing  Link       p          0.84
            SWQLD1         2026               NSW-QLD_exp_2026  Link       p          0.84
            SWQLD1         2026               KINGASF1          Generator  p          0.14
            SWQLD1         2028               NSW-QLD_existing  Link       p          0.84
            SWQLD1         2028               NSW-QLD_exp_2026  Link       p          0.84  # KINGASF1 retired: no 2028 term

        returns rhs:
            constraint_id  investment_period  timeslice        rhs   constraint_type
            SWQLD1         2026               qld_peak_demand  3000  <=
            SWQLD1         2028               qld_peak_demand  3000  <=
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
    variable_name_mapping = _map_constraint_variables_to_pypsa_names(
        links, generators, storage, demand_nodes
    )
    _raise_on_terms_not_in_model(lhs, variable_name_mapping)
    lhs = _expand_terms_to_model_components(lhs, variable_name_mapping)
    lhs = _drop_terms_before_build_year(lhs)
    lhs = _drop_terms_from_retirement_year(lhs)
    return _drop_one_sided_constraint_periods(lhs, rhs)


def _investment_period_start_dates(
    investment_periods: list[int], year_type: str
) -> dict[int, pd.Timestamp]:
    """The datetime each investment period starts at — the same financial or
    calendar year boundary the snapshots use (see _period_start in
    ispypsa.translator.helpers).

    I/O Example:
        [2030], "fy"       -> {2030: 2029-07-01}  # FY ending nomenclature
        [2030], "calendar" -> {2030: 2030-01-01}
    """
    return {p: _period_start(year_type, p) for p in investment_periods}


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


def _map_constraint_variables_to_pypsa_names(
    links: pd.DataFrame,
    generators: pd.DataFrame,
    storage: pd.DataFrame,
    demand_nodes: pd.DataFrame,
) -> pd.DataFrame:
    """Builds the mapping from the names the custom-constraint tables refer to
    model elements by (constraint_variable_name) to the names of the PyPSA
    components built for them (pypsa_model_name), one row per component.

    The links, generators and storage frames already carry both sides as their
    isp_name (the element's un-suffixed ISP-level ID) and name columns; this
    function relabels them into the constraints module's vocabulary. A
    constraint variable name identifies a whole element, so one name can map
    to several components: a path's name (e.g. NSW-QLD, with no _existing or
    _exp_<year> suffix) covers the path's existing link and each of its
    expansion links, and a new entrant generator or storage unit's ID covers
    each of its per-build-year components. An existing unit's name maps to
    itself. Each row also carries its component's in-service window as
    build_year and retirement_year — the latter computed as build_year +
    lifetime, the year PyPSA deactivates the component, so a component with
    an infinite lifetime never retires. _drop_terms_before_build_year and
    _drop_terms_from_retirement_year use the pair to scope terms to the
    periods the component is in service; Load rows have neither year —
    demand is not built.

    Demand nodes — the buses with demand attached, not all buses — appear as
    Load rows: a load term resolves to the Load component pypsa_build attaches
    to its node's demand trace, named "load_<bus>" (see
    ispypsa.pypsa_build.buses). Which buses carry demand follows the regional
    granularity (sub-region buses, region buses, or the single NEM bus); REZ
    buses never do (see ispypsa.translator.buses).

    I/O Example:
        links:
            isp_name  name              build_year  lifetime
            NSW-QLD   NSW-QLD_existing  2029        inf
            NSW-QLD   NSW-QLD_exp_2030  2030        inf

        generators:
            isp_name  name           build_year  lifetime
            KINGASF1  KINGASF1       2029        11        # closes 2040
            N2 Solar  N2 Solar_2030  2030        30
            N2 Solar  N2 Solar_2040  2040        30

        storage:
            isp_name         name             build_year  lifetime
            Q8 Battery - 2h  Q8 Battery - 2h  2029        inf
            SQ BESS          SQ BESS_2030     2030        20
            SQ BESS          SQ BESS_2040     2040        20

        demand_nodes:
            name
            SQ

        returns:
            constraint_variable_name  pypsa_model_name  component  build_year  retirement_year
            NSW-QLD                   NSW-QLD_existing  Link       2029        inf
            NSW-QLD                   NSW-QLD_exp_2030  Link       2030        inf
            KINGASF1                  KINGASF1          Generator  2029        2040
            N2 Solar                  N2 Solar_2030     Generator  2030        2060
            N2 Solar                  N2 Solar_2040     Generator  2040        2070
            Q8 Battery - 2h           Q8 Battery - 2h   Storage    2029        inf
            SQ BESS                   SQ BESS_2030      Storage    2030        2050
            SQ BESS                   SQ BESS_2040      Storage    2040        2060
            SQ                        load_SQ           Load                                  # data term: never built, always present
    """
    frames = [
        _component_mapping_rows(links, "Link"),
        _component_mapping_rows(generators, "Generator"),
        _component_mapping_rows(storage, "Storage"),
        pd.DataFrame(
            {
                "constraint_variable_name": demand_nodes["name"],
                "pypsa_model_name": "load_" + demand_nodes["name"],
                "component": "Load",
                "build_year": np.nan,
                "retirement_year": np.nan,
            }
        ),
    ]
    return _concat_non_empty(
        frames,
        [
            "constraint_variable_name",
            "pypsa_model_name",
            "component",
            "build_year",
            "retirement_year",
        ],
    )


def _component_mapping_rows(
    components: pd.DataFrame, component_type: str
) -> pd.DataFrame:
    """One mapping row per component, carrying its in-service window as
    build_year and retirement_year — build_year + lifetime, the year PyPSA
    deactivates the component, so an infinite lifetime never retires.

    I/O Example (component_type="Generator"):
        components:
            isp_name  name      build_year  lifetime
            KINGASF1  KINGASF1  2029        11
            LOYYB     LOYYB     2029        inf

        returns:
            constraint_variable_name  pypsa_model_name  component  build_year  retirement_year
            KINGASF1                  KINGASF1          Generator  2029        2040
            LOYYB                     LOYYB             Generator  2029        inf
    """
    rows = components.loc[:, ["isp_name", "name", "build_year"]].rename(
        columns={"isp_name": "constraint_variable_name", "name": "pypsa_model_name"}
    )
    rows["component"] = component_type
    rows["retirement_year"] = components["build_year"] + components["lifetime"]
    return rows


def _raise_on_terms_not_in_model(
    lhs: pd.DataFrame, variable_name_mapping: pd.DataFrame
) -> None:
    """Raises when a term references a component with no match in the model —
    e.g. a link_flow term when regional_granularity is single_region builds no
    links. Applying the constraint without the term would silently weaken it,
    so the run halts instead.

    I/O Example:
        lhs:
            constraint_id  variable_name  component
            SWQLD1         NSW-QLD        Link
            SWQLD1         KINGASF1       Generator

        variable_name_mapping:
            constraint_variable_name  pypsa_model_name  component
            NSW-QLD                   NSW-QLD_existing  Link

        raises:
            "... components not in the model, as (constraint_id,
            variable_name): [('SWQLD1', 'KINGASF1')]"
    """
    ids = variable_name_mapping.loc[
        :, ["component", "constraint_variable_name"]
    ].drop_duplicates()
    matched = lhs.merge(
        ids,
        how="left",
        left_on=["component", "variable_name"],
        right_on=["component", "constraint_variable_name"],
    )
    missing = matched[matched["constraint_variable_name"].isna()]
    if not missing.empty:
        pairs = sorted(set(zip(missing["constraint_id"], missing["variable_name"])))
        raise ValueError(
            f"Custom constraint LHS terms reference components not in the "
            f"model, as (constraint_id, variable_name): {pairs}"
        )


def _expand_terms_to_model_components(
    lhs: pd.DataFrame, variable_name_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Replaces each term's constraint variable name (in the variable_name column) with
    the PyPSA model component names it covers, one term per component: a link_flow term
    covers its path's existing and expansion links, and a term on a new entrant
    generator or storage unit covers each of its per-build-year components.

    I/O Example:
        lhs:
            constraint_id  variable_name  component  coefficient
            SWQLD1         NSW-QLD        Link       0.84
            SWQLD1         KINGASF1       Generator  0.14
            SWQLD1         N2 Solar       Generator  0.5

        variable_name_mapping:
            constraint_variable_name  pypsa_model_name  component  build_year  retirement_year
            NSW-QLD                   NSW-QLD_existing  Link       2029        inf
            NSW-QLD                   NSW-QLD_exp_2030  Link       2030        inf
            KINGASF1                  KINGASF1          Generator  2029        2040
            N2 Solar                  N2 Solar_2030     Generator  2030        2060
            N2 Solar                  N2 Solar_2040     Generator  2040        2070

        returns:
            constraint_id  variable_name     component  coefficient  build_year  retirement_year
            SWQLD1         NSW-QLD_existing  Link       0.84         2029        inf
            SWQLD1         NSW-QLD_exp_2030  Link       0.84         2030        inf
            SWQLD1         KINGASF1          Generator  0.14         2029        2040  # existing unit: unchanged
            SWQLD1         N2 Solar_2030     Generator  0.5          2030        2060  # new entrant: one term
            SWQLD1         N2 Solar_2040     Generator  0.5          2040        2070  # per build year
    """
    expanded = lhs.merge(
        variable_name_mapping,
        left_on=["component", "variable_name"],
        right_on=["component", "constraint_variable_name"],
    )
    expanded = expanded.drop(columns=["variable_name", "constraint_variable_name"])
    return expanded.rename(columns={"pypsa_model_name": "variable_name"})


def _drop_terms_before_build_year(lhs: pd.DataFrame) -> pd.DataFrame:
    """Drops each term in the investment periods before its component's build
    year, then drops the build_year column.

    A component contributes nothing to a period before it is built — PyPSA
    fixes its dispatch (p) to zero there, and its capacity (p_nom) is a single
    horizon-wide variable that would otherwise let capacity built for a later
    period alter an earlier period's constraint. Terms with no build_year
    (load data terms) apply in every period.

    I/O Example (columns abridged):
        lhs:
            constraint_id  investment_period  variable_name  build_year
            SWQLD1         2026               N2 Solar_2026  2026
            SWQLD1         2026               N2 Solar_2028  2028        # not built in 2026: dropped
            SWQLD1         2028               N2 Solar_2028  2028
            SWQLD1         2026               load_SQ                    # no build year: kept

        returns:
            constraint_id  investment_period  variable_name
            SWQLD1         2026               N2 Solar_2026
            SWQLD1         2028               N2 Solar_2028
            SWQLD1         2026               load_SQ
    """
    built = lhs["build_year"].isna() | (lhs["build_year"] <= lhs["investment_period"])
    return lhs[built].drop(columns="build_year")


def _drop_terms_from_retirement_year(lhs: pd.DataFrame) -> pd.DataFrame:
    """Drops each term in the investment periods from its component's
    retirement year onward, then drops the retirement_year column. The
    retirement year is build_year + lifetime and the bound is exclusive,
    matching when PyPSA deactivates the component (it is in service for
    build_year <= period < build_year + lifetime).

    A retired component's dispatch (p) is zero, and its capacity (p_nom) is a
    single horizon-wide variable that would otherwise keep counting capacity
    after it has left the system. An infinite lifetime gives an inf
    retirement year and load data terms have none — both apply in every
    period.

    I/O Example (columns abridged):
        lhs:
            constraint_id  investment_period  variable_name  retirement_year
            SWQLD1         2026               KINGASF1       2028
            SWQLD1         2028               KINGASF1       2028             # retired: dropped
            SWQLD1         2026               load_SQ                         # no retirement year: kept
            SWQLD1         2028               load_SQ

        returns:
            constraint_id  investment_period  variable_name
            SWQLD1         2026               KINGASF1
            SWQLD1         2026               load_SQ
            SWQLD1         2028               load_SQ
    """
    in_service = lhs["retirement_year"].isna() | (
        lhs["investment_period"] < lhs["retirement_year"]
    )
    return lhs[in_service].drop(columns="retirement_year")


def _drop_one_sided_constraint_periods(
    lhs: pd.DataFrame, rhs: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Keeps each constraint only in the investment periods where it has both
    LHS terms and an RHS row, dropping (and logging) the one-sided periods.

    A period is one-sided when one side's date_from coverage starts later
    than the other's, leaving the constraint with terms but no limit (or a
    limit but no terms) in the earlier periods, where it can't be applied.

    I/O Example:
        lhs (abridged):
            constraint_id  investment_period
            SWQLD1         2028
            NQ1            2026
            NQ1            2028

        rhs (abridged):
            constraint_id  investment_period
            SWQLD1         2026
            SWQLD1         2028
            NQ1            2026

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


def _create_constraint_relaxations(
    options: pd.DataFrame,
    expansion_costs: pd.DataFrame,
    rhs: pd.DataFrame,
    config: ModelConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Creates the dummy generators that let PyPSA invest in relaxing
    constraints with a constraint_relaxation expansion option, the LHS terms
    wiring them into their constraints, and the caps on how much can be built.

        - Constraint relaxation is gated by the config's rez_transmission_expansion
        flag: with it off, no dummy generators or relaxation expansion-limit
        constraints are created.
        - For rows in network_expansion_options with an expansion_type of
        constraint_relaxation, dummy generator capacity is added to the LHS of the
        constraint named by the expansion_id column.
        - One dummy generator is created per relaxation option per investment period.
        A period's constraint LHS carries every generator built up to that period, so
        the relaxation available accumulates across the horizon.
        - The generators' capital_cost is the option's cost from
        network_transmission_path_expansion_costs, annuitised with the config's wacc
        and annuitisation_lifetime.
        - For <= constraints the generator LHS terms are negative and for >= they
        are positive, so building capacity always loosens the constraint.
        - Each option's allowed_expansion is returned as its cap, for
        _create_expansion_limit_constraints to bound the total relaxation built.

    I/O Example (investment periods 2026 and 2028; rez_transmission_expansion on):
        options:
            expansion_id  expansion_type         allowed_expansion  expansion_option
            SWQLD1        constraint_relaxation  400                Option 2

        expansion_costs:
            expansion_id  year  cost
            SWQLD1        2026  100000
            SWQLD1        2028  80000

        rhs (abridged):
            constraint_id  investment_period  constraint_type
            SWQLD1         2026               <=
            SWQLD1         2028               <=

        returns generators (abridged):
            name             isp_name  build_year  capital_cost
            SWQLD1_exp_2026  SWQLD1    2026        annuitise(100000)
            SWQLD1_exp_2028  SWQLD1    2028        annuitise(80000)

        returns lhs terms:
            constraint_id  investment_period  variable_name    component  attribute  coefficient
            SWQLD1         2026               SWQLD1_exp_2026  Generator  p_nom      -1.0
            SWQLD1         2028               SWQLD1_exp_2026  Generator  p_nom      -1.0
            SWQLD1         2028               SWQLD1_exp_2028  Generator  p_nom      -1.0

        returns caps:
            expansion_id  allowed_expansion
            SWQLD1        400
    """
    relaxations = _resolve_relaxation_options(
        options, sorted(set(rhs["constraint_id"])), config
    )
    relaxation_generators = _create_constraint_relaxation_generators(
        relaxations, expansion_costs, config
    )
    relaxation_lhs = _relaxation_generator_lhs_terms(relaxation_generators, rhs)
    caps = relaxations.loc[:, ["expansion_id", "allowed_expansion"]]
    return relaxation_generators, relaxation_lhs, caps


def _create_constraint_relaxation_generators(
    relaxations: pd.DataFrame,
    expansion_costs: pd.DataFrame,
    config: ModelConfig,
) -> pd.DataFrame:
    """Builds one extendable dummy generator per relaxable constraint and
    investment period, with the selected expansion option's annualised cost.

    The generators' p_nom enters the parent constraint's LHS with a sign set
    by the constraint's direction (see _relaxation_generator_lhs_terms), so
    building them relaxes the constraint at the option's cost; total
    relaxation is capped at the option's allowed_expansion by the
    expansion-limit constraints.

    The costs table may use blank key cells as wildcards: a blank
    expansion_id is a table-wide default cost and a blank year a static cost
    across the investment periods (see _prepare_expansion_costs). Every
    relaxable constraint has a cost in every investment period (the costs
    schema's coverage rule), so there is exactly one generator per relaxation
    and period.

    I/O Example (blank cells are wildcards):
        relaxations (from _resolve_relaxation_options):
            expansion_id  expansion_type         allowed_expansion  expansion_option
            SWQLD1        constraint_relaxation  500                Option 2
            NQ1           constraint_relaxation  200                Default

        expansion_costs:
            expansion_id  year  cost
            SWQLD1        2030  100000
                                80000    # every constraint, every period

        config: investment_periods = [2030, 2040]

        returns:
            name             isp_name  bus                             p_nom  p_nom_extendable  build_year  lifetime  capital_cost
            SWQLD1_exp_2030  SWQLD1    bus_for_custom_constraint_gens  0.0    True              2030        inf       annuitise(100000)
            SWQLD1_exp_2040  SWQLD1    bus_for_custom_constraint_gens  0.0    True              2040        inf       annuitise(80000)
            NQ1_exp_2030     NQ1       bus_for_custom_constraint_gens  0.0    True              2030        inf       annuitise(80000)
            NQ1_exp_2040     NQ1       bus_for_custom_constraint_gens  0.0    True              2040        inf       annuitise(80000)
    """
    costs = _prepare_expansion_costs(
        expansion_costs,
        sorted(set(relaxations["expansion_id"])),
        config.temporal.capacity_expansion.investment_periods,
        config.wacc,
        config.network.annuitisation_lifetime,
    )
    return _format_relaxation_generators(costs)


def _resolve_relaxation_options(
    options: pd.DataFrame, constraint_ids: list[str], config: ModelConfig
) -> pd.DataFrame:
    """Resolves the expansion-options wildcards to one constraint_relaxation
    row per constraint in the model that has an option — or to no rows at all
    when the config's rez_transmission_expansion flag is off.

    The physical forward/reverse rows are set aside first (they become
    expansion links in ispypsa.translator.network); a blank expansion_type
    covers constraint_relaxation too, so it is kept. Options for constraints
    not in the model are dropped, then _resolve_wildcards fans blank cells out
    against the model's constraints, most specific row winning. The resolved
    rows drive both the relaxation generators and, through allowed_expansion,
    the expansion-limit caps.

    I/O Example (blank cells are wildcards; rez_transmission_expansion on):
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
    if not config.network.rez_transmission_expansion:
        return pd.DataFrame(columns=options.columns)
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
            expansion_id  year  capital_cost
            SWQLD1        2030  8140
            SWQLD1        2040  7900

        returns:
            name             isp_name  bus                             p_nom  p_nom_extendable  build_year  lifetime  capital_cost
            SWQLD1_exp_2030  SWQLD1    bus_for_custom_constraint_gens  0.0    True              2030        inf       8140
            SWQLD1_exp_2040  SWQLD1    bus_for_custom_constraint_gens  0.0    True              2040        inf       7900
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
    return generators.loc[:, _GENERATOR_COLUMNS].reset_index(drop=True)


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
    constraint. The function works from any generators frame carrying name,
    isp_name (the parent constraint) and build_year, against any RHS carrying
    constraint_id, investment_period and constraint_type — it isn't specific
    to where either came from.

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
    options: pd.DataFrame,
    links: pd.DataFrame,
    relaxation_generators: pd.DataFrame,
    relaxation_caps: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Caps the total capacity built across each expandable element's
    per-period components at the selected option's capacity.

    The components are the paths' expansion links and the constraints'
    relaxation generators. A path's cap is max(forward, reverse) of its
    resolved expansion option (see _resolve_path_expansion_caps); a
    relaxation's cap is its option's allowed_expansion, passed in as
    relaxation_caps. Each cap becomes an RHS row with constraint_type "<="
    and no investment_period or timeslice — it applies to the p_nom variables
    globally — and each component contributes a coefficient 1.0 p_nom LHS
    term. Names get an "_expansion_limit" suffix so a relaxation cap doesn't
    collide with the constraint it relaxes. The underlying builders
    (_expansion_limit_lhs and _expansion_limit_rhs) take any (element,
    component) and (element, cap) rows — they aren't specific to paths or
    relaxations.

    I/O Example:
        options:
            expansion_id  expansion_type  allowed_expansion  expansion_option
            CQ-NQ         forward         800                BigLine
            CQ-NQ         reverse         1000               BigLine

        links:
            isp_name  name            p_nom_extendable
            CQ-NQ     CQ-NQ_existing  False
            CQ-NQ     CQ-NQ_exp_2030  True
            CQ-NQ     CQ-NQ_exp_2040  True

        relaxation_generators:
            name             isp_name
            SWQLD1_exp_2030  SWQLD1

        relaxation_caps:
            expansion_id  allowed_expansion
            SWQLD1        500

        returns lhs:
            constraint_id           variable_name    component  attribute  coefficient  investment_period
            CQ-NQ_expansion_limit   CQ-NQ_exp_2030   Link       p_nom      1.0          NaN
            CQ-NQ_expansion_limit   CQ-NQ_exp_2040   Link       p_nom      1.0          NaN
            SWQLD1_expansion_limit  SWQLD1_exp_2030  Generator  p_nom      1.0          NaN

        and rhs (CQ-NQ capped at max(forward, reverse)):
            constraint_id           rhs   constraint_type  investment_period  timeslice
            CQ-NQ_expansion_limit   1000  <=               NaN                NaN
            SWQLD1_expansion_limit  500   <=               NaN                NaN
    """
    path_caps = _resolve_path_expansion_caps(options, links)
    lhs = pd.concat(
        [
            _expansion_limit_lhs(links[links["p_nom_extendable"]], "Link"),
            _expansion_limit_lhs(relaxation_generators, "Generator"),
        ],
        ignore_index=True,
    )
    caps = pd.concat([path_caps, relaxation_caps], ignore_index=True)
    rhs = _expansion_limit_rhs(caps)
    lhs["constraint_id"] = lhs["constraint_id"] + "_expansion_limit"
    rhs["constraint_id"] = rhs["constraint_id"] + "_expansion_limit"
    return lhs, rhs


def _expansion_limit_lhs(components: pd.DataFrame, component_type: str) -> pd.DataFrame:
    """One LHS term per expandable component, summing p_nom across the
    investment periods of its parent element.

    I/O Example (component_type="Link"):
        components:
            isp_name  name
            CQ-NQ     CQ-NQ_exp_2030
            CQ-NQ     CQ-NQ_exp_2040

        returns:
            constraint_id  variable_name   component  attribute  coefficient  investment_period
            CQ-NQ          CQ-NQ_exp_2030  Link       p_nom      1.0          NaN
            CQ-NQ          CQ-NQ_exp_2040  Link       p_nom      1.0          NaN
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
    """Renames constraint_id to constraint_name, raises on duplicate
    (constraint, period, timeslice) RHS rows, and sets the final PyPSA
    friendly column orders.

    I/O Example (non-key columns abridged — see _LHS_COLUMNS and _RHS_COLUMNS):
        lhs:
            constraint_id  variable_name     coefficient
            SWQLD1         NSW-QLD_existing  0.84

        rhs:
            constraint_id  timeslice        rhs
            SWQLD1         qld_peak_demand  3000

        returns lhs:
            constraint_name  variable_name     coefficient
            SWQLD1           NSW-QLD_existing  0.84

        returns rhs:
            constraint_name  timeslice        rhs
            SWQLD1           qld_peak_demand  3000
    """
    lhs = lhs.rename(columns={"constraint_id": "constraint_name"})
    rhs = rhs.rename(columns={"constraint_id": "constraint_name"})
    _raise_on_duplicate_rhs_rows(rhs)
    return (
        lhs.loc[:, _LHS_COLUMNS].reset_index(drop=True),
        rhs.loc[:, _RHS_COLUMNS].reset_index(drop=True),
    )


def _raise_on_duplicate_rhs_rows(rhs: pd.DataFrame) -> None:
    """Raise on duplicate (constraint, period, timeslice) RHS rows. A final check to make
    sure the constraint translation process hasn't created constraints with overlapping
    names."""
    keys = ["constraint_name", "investment_period", "timeslice"]
    duplicates = rhs[rhs.duplicated(subset=keys, keep=False)]
    if not duplicates.empty:
        raise ValueError(
            f"Duplicate custom constraint RHS rows for: "
            f"{sorted(set(duplicates['constraint_name']))}"
        )
