import re

import numpy as np
import pandas as pd
import pytest

from ispypsa.templater.custom_constraints_from_plexos import (
    _match_to_templated_unit_names,
    _plexos_extract_dir,
    empty_custom_constraint_tables,
    template_custom_constraints_from_plexos,
)

_EMPTY_EXISTING_PLANNED = pd.DataFrame(columns=["name"])
_EMPTY_GENERATORS_NEW_ENTRANT = pd.DataFrame(columns=["name", "geo_id"])
_EMPTY_STORAGE_NEW_ENTRANT = pd.DataFrame(columns=["name", "technology", "geo_id"])


def _write_extract(extract_dir, constraints, lhs_terms, rhs_values):
    """Save three DataFrames as the CSVs that make up a PLEXOS extract."""
    extract_dir.mkdir()
    constraints.to_csv(extract_dir / "constraints.csv", index=False)
    lhs_terms.to_csv(extract_dir / "lhs_terms.csv", index=False)
    rhs_values.to_csv(extract_dir / "rhs_values.csv", index=False)


def _assert_terms_of_constraint(lhs, constraint_id, expected):
    """Compare one constraint's rows of the LHS table with expected, ignoring
    row order."""
    sort_columns = ["term_type", "variable_name", "coefficient"]
    terms = lhs[lhs["constraint_id"] == constraint_id]
    pd.testing.assert_frame_equal(
        terms.sort_values(sort_columns).reset_index(drop=True),
        expected.sort_values(sort_columns).reset_index(drop=True),
        check_dtype=False,
    )


# --- 1. Pass 1: translating the extract ---


def test_translates_the_plexos_extract_into_the_three_tables(
    tmp_path, csv_str_to_df, caplog
):
    # Four constraints. SWQLD1 is the workhorse and holds most of the LHS
    # cases; the other three each make one point:
    #   NET1                two PLEXOS lines that are the same ISPyPSA path
    #   NSA1                a constraint whose only term is dropped
    #   CNSW-SNW South GPG  a constraint name with no ExportGroup_ prefix
    constraints = csv_str_to_df("""
        constraint_name,     property,            value
        ExportGroup_SWQLD1,  Sense,               -1.0
        ExportGroup_SWQLD1,  Penalty Price,       -1.0
        ExportGroup_SWQLD1,  Include in LT Plan,  -1.0
        ExportGroup_NET1,    Sense,               1.0
        ExportGroup_NSA1,    Sense,               -1.0
        CNSW-SNW South GPG,  Sense,               0.0
    """)
    lhs_terms = csv_str_to_df("""
        constraint_name,     parent_class,  parent_name,                             property,                         value,  date_from
        ExportGroup_SWQLD1,  Generator,     BW01,                                    Generation Sent Out Coefficient,  0.5,
        ExportGroup_SWQLD1,  Generator,     BW01,                                    Generation Sent Out Coefficient,  0.6,    2030-07-01T00:00:00
        ExportGroup_SWQLD1,  Generator,     SHGEN01,                                 Generation Sent Out Coefficient,  0.3,
        ExportGroup_SWQLD1,  Generator,     SWQLD1_Linear Augmentation,              Installed Capacity Coefficient,   -1.0,
        ExportGroup_SWQLD1,  Battery,       Tarong BESS,                             Generation Coefficient,           0.14,
        ExportGroup_SWQLD1,  Battery,       Tarong BESS,                             Load Coefficient,                 -0.14,
        ExportGroup_SWQLD1,  Battery,       SWQLD1 Battery - 2h,                     Generation Coefficient,           1.0,
        ExportGroup_SWQLD1,  Battery,       SWQLD1 Battery - 2h,                     Load Coefficient,                 -1.0,
        ExportGroup_SWQLD1,  Line,          NSW1-QLD1,                               Flow Coefficient,                 0.8,
        ExportGroup_SWQLD1,  Node,          CNSW,                                    Load Coefficient,                 1.0,
        ExportGroup_SWQLD1,  Purchaser,     Q1 to NQ Flexible Electrolyser,          Load Coefficient,                 -1.0,
        ExportGroup_NET1,    Line,          Marinus,                                 Flow Coefficient,                 1.0,
        ExportGroup_NET1,    Line,          T-V-MNSP1,                               Flow Coefficient,                 1.0,
        ExportGroup_NSA1,    Generator,     SA Hydrogen Turbine,                     Generation Sent Out Coefficient,  1.0,
        CNSW-SNW South GPG,  Generator,     CNSW SAT - Distributed Resources Area1,  Generation Sent Out Coefficient,  0.4,
    """)
    rhs_values = csv_str_to_df("""
        constraint_name,     value,   date_from,            tags
        ExportGroup_SWQLD1,  3000.0,  ,                     QLD Hot Day
        ExportGroup_SWQLD1,  3100.0,  2030-07-01T00:00:00,  QLD Hot Day
        ExportGroup_SWQLD1,  2950.0,  ,                     QLD Typical Summer
        ExportGroup_SWQLD1,  2900.0,  ,                     QLD Winter
        ExportGroup_NET1,    1200.0,  ,                     TAS Winter
        ExportGroup_NSA1,    800.0,   ,                     SA Hot Day
        CNSW-SNW South GPG,  500.0,   ,                     NSW Typical Summer
    """)
    _write_extract(tmp_path / "plexos", constraints, lhs_terms, rhs_values)
    # The units ISPyPSA models. Everything is existing or planned plant; the
    # new-entrant tables are empty, so pass 2 has nothing to add and the LHS
    # below is the pass-1 translation alone.
    generators_existing_planned = csv_str_to_df("""
        name
        BW01
        CNSW SAT - Distributed Resources
    """)
    storage_existing_planned = csv_str_to_df("""
        name
        SHGEN01
        Tarong BESS
    """)

    with caplog.at_level("INFO"):
        tables = template_custom_constraints_from_plexos(
            generators_existing_planned,
            _EMPTY_GENERATORS_NEW_ENTRANT,
            storage_existing_planned,
            _EMPTY_STORAGE_NEW_ENTRANT,
            iasr_workbook_version="not used, plexos_extract_dir is given",
            plexos_extract_dir=tmp_path / "plexos",
        )

    assert set(tables) == {
        "custom_constraints",
        "custom_constraints_lhs",
        "custom_constraints_rhs",
    }

    # custom_constraints: one row per constraint, from its Sense row alone
    # (Penalty Price and Include in LT Plan are ignored).
    #   - the ExportGroup_ prefix is stripped; CNSW-SNW South GPG has no
    #     prefix and keeps its name
    #   - Sense -1 -> "<=", 1 -> ">=", 0 -> "="
    #   - NSA1 is listed even though it ends up with no LHS terms
    expected_constraints = csv_str_to_df("""
        constraint_id,       direction
        SWQLD1,              <=
        NET1,                >=
        NSA1,                <=
        CNSW-SNW South GPG,  =
    """)
    pd.testing.assert_frame_equal(tables["custom_constraints"], expected_constraints)

    # custom_constraints_lhs. Of the 15 input rows, 8 come through.
    #
    # Rows that are kept:
    #   - BW01 (both rows): a templated generator, so generator_output. Its two
    #     rows are one coefficient that steps from 0.5 to 0.6 on 2030-07-01, and
    #     date_from is carried through so both stay.
    #   - SHGEN01: a PLEXOS Generator, but templated in
    #     storage_existing_planned, so its term becomes storage_output.
    #   - Tarong BESS, Generation Coefficient: Battery -> storage_output.
    #   - NSW1-QLD1: a Line -> link_flow, named by the path_id NSW-QLD.
    #   - CNSW: a Node -> load, keeping its name.
    #   - Marinus and T-V-MNSP1: two PLEXOS lines that are both the ISPyPSA path
    #     TAS-SEV. They become two identical TAS-SEV rows, and the duplicate is
    #     removed, leaving one.
    #   - CNSW SAT - Distributed Resources Area1: the " Area1" suffix is
    #     removed, which gives the templated generator's name.
    #
    # Rows that are dropped:
    #   - SWQLD1_Linear Augmentation: an Installed Capacity Coefficient row is a
    #     PLEXOS constraint relaxation, not an operational term.
    #   - Tarong BESS, Load Coefficient: the negative pair of the Generation
    #     Coefficient row that was kept.
    #   - SWQLD1 Battery - 2h (both rows): the Load row as above; the
    #     Generation row because the battery is named for the constraint and
    #     matches no templated unit.
    #   - Q1 to NQ Flexible Electrolyser: Purchasers are not modelled.
    #   - SA Hydrogen Turbine: in neither templated table. It was NSA1's only
    #     term, so NSA1 has no LHS rows.
    expected_lhs = csv_str_to_df("""
        constraint_id,       term_type,         variable_name,                     coefficient,  date_from
        SWQLD1,              generator_output,  BW01,                              0.5,
        SWQLD1,              generator_output,  BW01,                              0.6,          2030-07-01T00:00:00
        SWQLD1,              storage_output,    SHGEN01,                           0.3,
        SWQLD1,              storage_output,    Tarong BESS,                       0.14,
        SWQLD1,              link_flow,         NSW-QLD,                           0.8,
        SWQLD1,              load,              CNSW,                              1.0,
        NET1,                link_flow,         TAS-SEV,                           1.0,
        CNSW-SNW South GPG,  generator_output,  CNSW SAT - Distributed Resources,  0.4,
    """)
    lhs_sort = ["constraint_id", "term_type", "variable_name", "coefficient"]
    pd.testing.assert_frame_equal(
        tables["custom_constraints_lhs"].sort_values(lhs_sort).reset_index(drop=True),
        expected_lhs.sort_values(lhs_sort).reset_index(drop=True),
        check_dtype=False,
    )

    # custom_constraints_rhs: one row per input row, in the same order.
    #   - the tag's region is kept, lower-cased, and its suffix becomes the
    #     canonical timeslice name: Hot Day -> peak_demand,
    #     Typical Summer -> summer_typical, Winter -> winter_reference
    #   - SWQLD1's peak-demand limit steps from 3000 to 3100 on 2030-07-01;
    #     date_from is carried through so both rows stay
    #   - NSA1 keeps its RHS row even though it has no LHS terms
    expected_rhs = csv_str_to_df("""
        constraint_id,       timeslice,             rhs,     date_from
        SWQLD1,              qld_peak_demand,       3000.0,
        SWQLD1,              qld_peak_demand,       3100.0,  2030-07-01T00:00:00
        SWQLD1,              qld_summer_typical,    2950.0,
        SWQLD1,              qld_winter_reference,  2900.0,
        NET1,                tas_winter_reference,  1200.0,
        NSA1,                sa_peak_demand,        800.0,
        CNSW-SNW South GPG,  nsw_summer_typical,    500.0,
    """)
    pd.testing.assert_frame_equal(
        tables["custom_constraints_rhs"], expected_rhs, check_dtype=False
    )

    # The log, in full. One line for each kind of dropped row and each
    # translation described above; the two that loosen or empty a constraint
    # (units with no match, NSA1 left with no terms) are WARNINGs, the rest
    # INFO. The rename line lists every name that changed, lines included.
    # The "duplicate" line is the second of NET1's two TAS-SEV rows (from
    # Marinus and T-V-MNSP1) being removed. It is not about batteries: none
    # were added, as the "Injected no new-entrant batteries" line says.
    assert caplog.messages == [
        "Creating a custom-constraints template from the PLEXOS extract",
        "Dropped 1 LHS rows for excluded parent classes ['Purchaser']: "
        "parents=['Q1 to NQ Flexible Electrolyser']",
        "Dropped 1 constraint relaxation LHS rows "
        "(PLEXOS Installed Capacity Coefficient): ['SWQLD1_Linear Augmentation']",
        "Dropped 2 battery Load Coefficient LHS rows "
        "(negative pairs of the kept Generation Coefficient rows)",
        "Applied 4 PLEXOS->ISPyPSA LHS name renames: "
        "['CNSW SAT - Distributed Resources Area1 -> "
        "CNSW SAT - Distributed Resources', 'Marinus -> TAS-SEV', "
        "'NSW1-QLD1 -> NSW-QLD', 'T-V-MNSP1 -> TAS-SEV']",
        "Dropped 2 LHS term rows (2 distinct units) with no ISPyPSA unit match: "
        "['NSA1: SA Hydrogen Turbine', 'SWQLD1: SWQLD1 Battery - 2h']",
        "Changed the term_type of 1 LHS term rows to match the table their unit "
        "is templated in: ['SHGEN01: generator_output -> storage_output']",
        "Injected no new-entrant batteries",
        "Dropped 1 duplicate LHS term rows, keeping the first of each: "
        "['NET1: TAS-SEV']",
        "Custom constraints left with no LHS terms: ['NSA1']",
    ]
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == [
        "Dropped 2 LHS term rows (2 distinct units) with no ISPyPSA unit match: "
        "['NSA1: SA Hydrogen Turbine', 'SWQLD1: SWQLD1 Battery - 2h']",
        "Custom constraints left with no LHS terms: ['NSA1']",
    ]


# --- 2. Pass 2: adding new-entrant batteries ---


def test_adds_new_entrant_batteries_at_triggered_locations(
    tmp_path, csv_str_to_df, caplog
):
    # The rule under test. A new-entrant generator or storage unit that is in
    # a constraint after pass 1 "triggers" its location (its geo_id). Pass 2
    # then adds a storage_output term for every new-entrant battery at that
    # location. The added batteries take the coefficients, over time, of the
    # new-entrant batteries already in the constraint at that location, or
    # 1.0 if there are none.
    #
    # Each constraint below is one case of that rule and is named after it.
    #
    # The constraints and rhs_values files are left header-only, which does not
    # change the LHS table asserted here. The three output tables are built
    # separately: constraints.csv only feeds custom_constraints, and
    # rhs_values.csv only feeds custom_constraints_rhs. Nothing checks that a
    # constraint with LHS terms also has a Sense row, so the LHS rows below
    # translate the same with or without one.
    #
    # The battery Load Coefficient rows a real extract pairs with
    # each Generation Coefficient row are left out too (the pass-1 test covers
    # them).
    constraints = csv_str_to_df("""
        constraint_name,  property,  value
    """)
    lhs_terms = csv_str_to_df("""
        constraint_name,            parent_class,  parent_name,            property,                         value,  date_from
        generator_triggers,         Generator,     Q8 Solar,               Generation Sent Out Coefficient,  0.4,
        sibling_coefficient,        Generator,     Q8 Solar,               Generation Sent Out Coefficient,  0.14,
        sibling_coefficient,        Battery,       Q8 Battery - 2h,        Generation Coefficient,           0.43,
        battery_only_trigger,       Battery,       Q8 Battery - 2h,        Generation Coefficient,           0.5,
        time_varying_profile,       Battery,       Q8 Battery - 2h,        Generation Coefficient,           0.78,
        time_varying_profile,       Battery,       Q8 Battery - 2h,        Generation Coefficient,           0.0,    2031-11-30T00:00:00
        pumped_hydro_triggers,      Battery,       T1 Pumped Hydro - 24h,  Generation Coefficient,           0.9,
        two_locations,              Battery,       Q8 Battery - 2h,        Generation Coefficient,           0.43,
        two_locations,              Generator,     T1 Wind,                Generation Sent Out Coefficient,  0.6,
        existing_units_only,        Generator,     BW01,                   Generation Sent Out Coefficient,  0.5,
        existing_units_only,        Battery,       Tarong BESS,            Generation Coefficient,           0.14,
        location_with_no_battery,   Generator,     N3 Wind,                Generation Sent Out Coefficient,  0.6,
    """)
    rhs_values = csv_str_to_df("""
        constraint_name,  value,  date_from,  tags
    """)
    _write_extract(tmp_path / "plexos", constraints, lhs_terms, rhs_values)
    # Three locations:
    #   Q8  one generator, two batteries (2h and 4h)
    #   T1  one generator, one battery and one pumped hydro unit
    #   N3  one generator, no storage
    generators_existing_planned = csv_str_to_df("""
        name
        BW01
    """)
    generators_new_entrant = csv_str_to_df("""
        name,      geo_id
        Q8 Solar,  Q8
        T1 Wind,   T1
        N3 Wind,   N3
    """)
    storage_existing_planned = csv_str_to_df("""
        name
        Tarong BESS
    """)
    storage_new_entrant = csv_str_to_df("""
        name,                   technology,                      geo_id
        Q8 Battery - 2h,        Battery Storage (2hrs storage),  Q8
        Q8 Battery - 4h,        Battery Storage (4hrs storage),  Q8
        T1 Battery - 2h,        Battery Storage (2hrs storage),  T1
        T1 Pumped Hydro - 24h,  Pumped Hydro (24hrs storage),    T1
    """)

    with caplog.at_level("INFO"):
        tables = template_custom_constraints_from_plexos(
            generators_existing_planned,
            generators_new_entrant,
            storage_existing_planned,
            storage_new_entrant,
            iasr_workbook_version="not used, plexos_extract_dir is given",
            plexos_extract_dir=tmp_path / "plexos",
        )
    lhs = tables["custom_constraints_lhs"]

    # generator_triggers: Q8 Solar is a new-entrant generator at Q8, so both Q8
    # batteries are added. The constraint had no Q8 battery to copy a
    # coefficient from, so they take the default 1.0 (and a warning is logged).
    expected = csv_str_to_df("""
        constraint_id,       term_type,         variable_name,    coefficient,  date_from
        generator_triggers,  generator_output,  Q8 Solar,         0.4,
        generator_triggers,  storage_output,    Q8 Battery - 2h,  1.0,
        generator_triggers,  storage_output,    Q8 Battery - 4h,  1.0,
    """)
    _assert_terms_of_constraint(lhs, "generator_triggers", expected)

    # sibling_coefficient: PLEXOS lists the Q8 2h battery at 0.43 and leaves
    # the 4h out. The 4h battery is added at the 2h battery's 0.43, not at 1.0
    # and not at Q8 Solar's 0.14: only batteries supply the coefficient. The
    # 2h battery is already in the constraint, so it is not added again.
    expected = csv_str_to_df("""
        constraint_id,        term_type,         variable_name,    coefficient,  date_from
        sibling_coefficient,  generator_output,  Q8 Solar,         0.14,
        sibling_coefficient,  storage_output,    Q8 Battery - 2h,  0.43,
        sibling_coefficient,  storage_output,    Q8 Battery - 4h,  0.43,
    """)
    _assert_terms_of_constraint(lhs, "sibling_coefficient", expected)

    # battery_only_trigger: no generator in the constraint. The Q8 2h battery
    # triggers Q8 on its own, so the 4h battery is added at the 2h's 0.5.
    expected = csv_str_to_df("""
        constraint_id,         term_type,       variable_name,    coefficient,  date_from
        battery_only_trigger,  storage_output,  Q8 Battery - 2h,  0.5,
        battery_only_trigger,  storage_output,  Q8 Battery - 4h,  0.5,
    """)
    _assert_terms_of_constraint(lhs, "battery_only_trigger", expected)

    # time_varying_profile: the Q8 2h battery's coefficient steps from 0.78 to
    # 0.0 on 2031-11-30. The 4h battery is added with the same two rows.
    expected = csv_str_to_df("""
        constraint_id,         term_type,       variable_name,    coefficient,  date_from
        time_varying_profile,  storage_output,  Q8 Battery - 2h,  0.78,
        time_varying_profile,  storage_output,  Q8 Battery - 2h,  0.0,          2031-11-30T00:00:00
        time_varying_profile,  storage_output,  Q8 Battery - 4h,  0.78,
        time_varying_profile,  storage_output,  Q8 Battery - 4h,  0.0,          2031-11-30T00:00:00
    """)
    _assert_terms_of_constraint(lhs, "time_varying_profile", expected)

    # pumped_hydro_triggers: T1 Pumped Hydro - 24h is new-entrant storage at
    # T1, so it triggers T1 and the T1 battery is added. Pumped hydro is not a
    # battery, so its 0.9 is not copied (the T1 battery takes the default 1.0,
    # with a warning), and no pumped hydro unit is ever added by pass 2.
    expected = csv_str_to_df("""
        constraint_id,          term_type,       variable_name,          coefficient,  date_from
        pumped_hydro_triggers,  storage_output,  T1 Pumped Hydro - 24h,  0.9,
        pumped_hydro_triggers,  storage_output,  T1 Battery - 2h,        1.0,
    """)
    _assert_terms_of_constraint(lhs, "pumped_hydro_triggers", expected)

    # two_locations: the Q8 2h battery triggers Q8 and T1 Wind triggers T1.
    # Each location is handled on its own: the Q8 4h battery takes Q8's 0.43,
    # while the T1 battery has no T1 battery to copy from and takes 1.0 (with
    # a warning). T1 Pumped Hydro - 24h is at T1 but is not added.
    expected = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,    coefficient,  date_from
        two_locations,  storage_output,    Q8 Battery - 2h,  0.43,
        two_locations,  storage_output,    Q8 Battery - 4h,  0.43,
        two_locations,  generator_output,  T1 Wind,          0.6,
        two_locations,  storage_output,    T1 Battery - 2h,  1.0,
    """)
    _assert_terms_of_constraint(lhs, "two_locations", expected)

    # existing_units_only: BW01 and Tarong BESS are existing plant. They have
    # no location, trigger nothing, and the constraint is left as pass 1 made
    # it.
    expected = csv_str_to_df("""
        constraint_id,        term_type,         variable_name,  coefficient,  date_from
        existing_units_only,  generator_output,  BW01,           0.5,
        existing_units_only,  storage_output,    Tarong BESS,    0.14,
    """)
    _assert_terms_of_constraint(lhs, "existing_units_only", expected)

    # location_with_no_battery: N3 Wind triggers N3, but there are no
    # new-entrant batteries at N3, so nothing is added (and nothing is warned
    # about, since no battery was given a default coefficient).
    expected = csv_str_to_df("""
        constraint_id,             term_type,         variable_name,  coefficient,  date_from
        location_with_no_battery,  generator_output,  N3 Wind,        0.6,
    """)
    _assert_terms_of_constraint(lhs, "location_with_no_battery", expected)

    # The log, in full.
    #   - "Injected 9 ... rows across 6 constraints (3 distinct batteries)"
    #     counts the rows pass 2 added: generator_triggers 2,
    #     sibling_coefficient 1, battery_only_trigger 1, time_varying_profile 2,
    #     pumped_hydro_triggers 1, two_locations 2. The three batteries are
    #     Q8 Battery - 2h, Q8 Battery - 4h and T1 Battery - 2h.
    #   - The WARNING names the three (constraint, location) pairs whose
    #     batteries were given the default 1.0.
    #   - There is no "duplicate" line: the Q8 2h battery was already in four
    #     of the constraints and was not added to them again.
    assert caplog.messages == [
        "Creating a custom-constraints template from the PLEXOS extract",
        "Injected 9 new-entrant battery storage_output rows "
        "across 6 constraints (3 distinct batteries)",
        "New-entrant batteries injected with default coefficient 1.0 "
        "(no surviving sibling to copy from): ['generator_triggers: Q8', "
        "'pumped_hydro_triggers: T1', 'two_locations: T1']",
    ]
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == [
        "New-entrant batteries injected with default coefficient 1.0 "
        "(no surviving sibling to copy from): ['generator_triggers: Q8', "
        "'pumped_hydro_triggers: T1', 'two_locations: T1']",
    ]


# --- 3. A clean extract ---


def test_logs_only_progress_lines_when_nothing_is_dropped_or_changed(
    tmp_path, csv_str_to_df, caplog
):
    # The negative case for every drop, rename, term_type change and warning
    # asserted in the two tests above: one constraint whose single term names a
    # templated generator exactly.
    constraints = csv_str_to_df("""
        constraint_name,     property,  value
        ExportGroup_SWQLD1,  Sense,     -1.0
    """)
    lhs_terms = csv_str_to_df("""
        constraint_name,     parent_class,  parent_name,  property,                         value,  date_from
        ExportGroup_SWQLD1,  Generator,     BW01,         Generation Sent Out Coefficient,  0.5,
    """)
    rhs_values = csv_str_to_df("""
        constraint_name,     value,   date_from,  tags
        ExportGroup_SWQLD1,  3000.0,  ,           QLD Hot Day
    """)
    _write_extract(tmp_path / "plexos", constraints, lhs_terms, rhs_values)
    generators_existing_planned = csv_str_to_df("""
        name
        BW01
    """)

    with caplog.at_level("INFO"):
        template_custom_constraints_from_plexos(
            generators_existing_planned,
            _EMPTY_GENERATORS_NEW_ENTRANT,
            _EMPTY_EXISTING_PLANNED,
            _EMPTY_STORAGE_NEW_ENTRANT,
            iasr_workbook_version="not used, plexos_extract_dir is given",
            plexos_extract_dir=tmp_path / "plexos",
        )

    assert caplog.messages == [
        "Creating a custom-constraints template from the PLEXOS extract",
        "Injected no new-entrant batteries",
    ]


# --- 4. Extract rows that raise ---

# A valid one-constraint extract, as raw CSV text with the full set of columns
# the real extract has. Each case below appends its bad rows to one of the
# three files.
_VALID_EXTRACT = {
    "constraints.csv": (
        "constraint_name,property,value,date_from,date_to,tags\n"
        "ExportGroup_SWQLD1,Sense,-1.0,,,\n"
    ),
    "lhs_terms.csv": (
        "constraint_name,parent_class,parent_name,property,value,date_from,date_to,tags\n"
        "ExportGroup_SWQLD1,Generator,BW01,Generation Sent Out Coefficient,0.5,,,\n"
    ),
    "rhs_values.csv": (
        "constraint_name,value,date_from,date_to,tags\n"
        "ExportGroup_SWQLD1,3000.0,,,QLD Hot Day\n"
    ),
}


@pytest.mark.parametrize(
    "file_name, bad_rows, error_type, message",
    [
        # date_to is dropped from the output, so a populated one would be lost.
        pytest.param(
            "constraints.csv",
            "ExportGroup_SWQLD1,Sense,-1.0,,2030-07-01T00:00:00,\n",
            ValueError,
            "PLEXOS extract 'constraints' has non-empty date_to values",
            id="date_to_in_constraints",
        ),
        pytest.param(
            "lhs_terms.csv",
            "ExportGroup_SWQLD1,Generator,BW01,Generation Sent Out Coefficient,0.6,,2030-07-01T00:00:00,\n",
            ValueError,
            "PLEXOS extract 'lhs_terms' has non-empty date_to values",
            id="date_to_in_lhs_terms",
        ),
        pytest.param(
            "rhs_values.csv",
            "ExportGroup_SWQLD1,3100.0,,2030-07-01T00:00:00,QLD Hot Day\n",
            ValueError,
            "PLEXOS extract 'rhs_values' has non-empty date_to values",
            id="date_to_in_rhs_values",
        ),
        # Sense must be -1, 0 or 1.
        pytest.param(
            "constraints.csv",
            "ExportGroup_NQ1,Sense,2.0,,,\n",
            ValueError,
            "PLEXOS Sense values with no direction mapping",
            id="unknown_sense",
        ),
        # A battery Load Coefficient row is only dropped as the pair of a
        # Generation Coefficient row. Lone BESS has no such pair, so dropping
        # its Load row would remove the battery from the constraint.
        pytest.param(
            "lhs_terms.csv",
            "ExportGroup_SWQLD1,Battery,Lone BESS,Load Coefficient,-1.0,,,\n",
            ValueError,
            "Battery Load Coefficient rows with no Generation Coefficient pair "
            "(dropping them would lose the battery's only LHS term): "
            "['SWQLD1: Lone BESS']",
            id="battery_load_row_without_generation_pair",
        ),
        # A (parent_class, property) pair with no term_type.
        pytest.param(
            "lhs_terms.csv",
            "ExportGroup_SWQLD1,Generator,BW01,Units Generating Coefficient,1.0,,,\n",
            ValueError,
            "PLEXOS (parent_class, property) pairs with no term_type mapping: "
            "[('Generator', 'Units Generating Coefficient')]",
            id="unknown_class_and_property_pair",
        ),
        # A line with no entry in the line -> path_id table.
        pytest.param(
            "lhs_terms.csv",
            "ExportGroup_SWQLD1,Line,MADE-UP-LINE,Flow Coefficient,1.0,,,\n",
            ValueError,
            "PLEXOS line names not in _LINE_TO_PATH_ID -- add a mapping: "
            "['MADE-UP-LINE']",
            id="unknown_line",
        ),
        # An RHS tag whose suffix is not Hot Day, Typical Summer or Winter.
        pytest.param(
            "rhs_values.csv",
            "ExportGroup_SWQLD1,3000.0,,,QLD Mild Spring\n",
            KeyError,
            "Mild Spring",
            id="unknown_rhs_tag",
        ),
        # Pass 2 copies one coefficient profile per location onto the batteries
        # it adds. The Q8 2h and 8h batteries disagree (0.43 and 0.5), so there
        # is no single profile to copy.
        pytest.param(
            "lhs_terms.csv",
            "ExportGroup_SWQLD1,Battery,Q8 Battery - 2h,Generation Coefficient,0.43,,,\n"
            "ExportGroup_SWQLD1,Battery,Q8 Battery - 8h,Generation Coefficient,0.5,,,\n",
            ValueError,
            "Surviving new-entrant batteries at a (constraint, location) do not "
            "share one (date_from, coefficient) profile: ['SWQLD1: Q8']",
            id="batteries_at_a_location_disagree_on_coefficient",
        ),
        # The two batteries agree at the start (0.78), but only the 2h steps to
        # 0.0 in 2031. Their profiles differ, so this raises too; otherwise the
        # 2031 step would be copied onto the 8h battery.
        pytest.param(
            "lhs_terms.csv",
            "ExportGroup_SWQLD1,Battery,Q8 Battery - 2h,Generation Coefficient,0.78,,,\n"
            "ExportGroup_SWQLD1,Battery,Q8 Battery - 2h,Generation Coefficient,0.0,2031-11-30T00:00:00,,\n"
            "ExportGroup_SWQLD1,Battery,Q8 Battery - 8h,Generation Coefficient,0.78,,,\n",
            ValueError,
            "Surviving new-entrant batteries at a (constraint, location) do not "
            "share one (date_from, coefficient) profile: ['SWQLD1: Q8']",
            id="batteries_at_a_location_disagree_on_dates",
        ),
    ],
)
def test_raises_on_extract_rows_it_cannot_translate(
    tmp_path, csv_str_to_df, file_name, bad_rows, error_type, message
):
    extract = {**_VALID_EXTRACT, file_name: _VALID_EXTRACT[file_name] + bad_rows}
    for name, text in extract.items():
        (tmp_path / name).write_text(text)
    generators_existing_planned = csv_str_to_df("""
        name
        BW01
    """)
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        Q8 Battery - 2h,  Battery Storage (2hrs storage),  Q8
        Q8 Battery - 8h,  Battery Storage (8hrs storage),  Q8
    """)

    with pytest.raises(error_type, match=re.escape(message)):
        template_custom_constraints_from_plexos(
            generators_existing_planned,
            _EMPTY_GENERATORS_NEW_ENTRANT,
            _EMPTY_EXISTING_PLANNED,
            storage_new_entrant,
            iasr_workbook_version="not used, plexos_extract_dir is given",
            plexos_extract_dir=tmp_path,
        )


def test_valid_extract_used_by_the_raises_test_does_not_raise(tmp_path, csv_str_to_df):
    # Guards the test above: with no bad rows appended, the same extract and
    # unit tables translate cleanly, so each raise is caused by its bad rows.
    for name, text in _VALID_EXTRACT.items():
        (tmp_path / name).write_text(text)
    generators_existing_planned = csv_str_to_df("""
        name
        BW01
    """)
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        Q8 Battery - 2h,  Battery Storage (2hrs storage),  Q8
        Q8 Battery - 8h,  Battery Storage (8hrs storage),  Q8
    """)

    tables = template_custom_constraints_from_plexos(
        generators_existing_planned,
        _EMPTY_GENERATORS_NEW_ENTRANT,
        _EMPTY_EXISTING_PLANNED,
        storage_new_entrant,
        iasr_workbook_version="not used, plexos_extract_dir is given",
        plexos_extract_dir=tmp_path,
    )

    expected_lhs = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  BW01,           0.5,
    """)
    pd.testing.assert_frame_equal(
        tables["custom_constraints_lhs"], expected_lhs, check_dtype=False
    )


# --- 5. empty_custom_constraint_tables ---


def test_empty_custom_constraint_tables(csv_str_to_df):
    # The header-only stand-ins for the three tables, used at the granularities
    # where custom constraints are not templated. Each has the same columns as
    # the populated table of the same name in the tests above, and no rows.
    tables = empty_custom_constraint_tables()

    assert set(tables) == {
        "custom_constraints",
        "custom_constraints_lhs",
        "custom_constraints_rhs",
    }
    expected_constraints = csv_str_to_df("""
        constraint_id,  direction
    """)
    pd.testing.assert_frame_equal(
        tables["custom_constraints"], expected_constraints, check_dtype=False
    )
    expected_lhs = csv_str_to_df("""
        constraint_id,  term_type,  variable_name,  coefficient,  date_from
    """)
    pd.testing.assert_frame_equal(
        tables["custom_constraints_lhs"], expected_lhs, check_dtype=False
    )
    expected_rhs = csv_str_to_df("""
        constraint_id,  timeslice,  rhs,  date_from
    """)
    pd.testing.assert_frame_equal(
        tables["custom_constraints_rhs"], expected_rhs, check_dtype=False
    )


# --- 6. Helpers tested directly ---


def test_match_to_templated_unit_names_renames_then_matches():
    # The naming rules, one PLEXOS name per case. The result lines up with
    # plexos_names row for row, and the comment on each expected value says
    # which rule produced it.
    plexos_names = pd.Series(
        [
            "DN1_SAT_Dubbo",
            "DN3 Marulan Battery - 2h",
            "DN10_SAT_Elsewhere",
            "OTHER_SAT_X",
            "CNSW SAT - Distributed Resources Area1",
            "CNSW V2G Area12",
            "Areaway",
            "BW01",
            "Q2_SAT_North QLD Clean Energy Hub",
            "SWQLD1 Battery - 2h",
            "UNKNOWN",
        ]
    )
    unit_names = {
        "DREZ_SAT_Dubbo",
        "DREZ Marulan Battery - 2h",
        "DN10_SAT_Elsewhere",
        "OTHER_SAT_X",
        "CNSW SAT - Distributed Resources",
        "CNSW V2G",
        "Areaway",
        "BW01",
        "Q2_SAT_North Qld Clean Energy Hub",
        "SQ Battery - 2h",
    }

    result = _match_to_templated_unit_names(plexos_names, unit_names)

    expected = pd.Series(
        [
            "DREZ_SAT_Dubbo",  # DN1 prefix followed by "_" -> DREZ
            "DREZ Marulan Battery - 2h",  # DN3 prefix followed by " " -> DREZ
            "DN10_SAT_Elsewhere",  # DN10 is a different prefix: not renamed
            "OTHER_SAT_X",  # prefix is not DN1 or DN3: not renamed
            "CNSW SAT - Distributed Resources",  # " Area1" suffix removed
            "CNSW V2G",  # " Area12": any area number is removed
            "Areaway",  # "Area" inside a word is not the suffix
            "BW01",  # already a templated name
            "Q2_SAT_North Qld Clean Energy Hub",  # matched ignoring case
            np.nan,  # constraint-scoped battery: no rename, so no unit
            np.nan,  # no templated unit of this name
        ]
    )
    pd.testing.assert_series_equal(result, expected)


def test_match_to_templated_unit_names_prefers_the_exact_spelling():
    # Two templated units differ only by capitalisation. A PLEXOS name that is
    # exactly one of them keeps that spelling, whichever of the two the
    # ignoring-case match would have picked.
    plexos_names = pd.Series(["North QLD Hub", "North Qld Hub"])
    unit_names = {"North QLD Hub", "North Qld Hub"}

    result = _match_to_templated_unit_names(plexos_names, unit_names)

    expected = pd.Series(["North QLD Hub", "North Qld Hub"])
    pd.testing.assert_series_equal(result, expected)


def test_plexos_extract_dir_resolves_to_shipped_extract():
    """The default extract directory points at the package's shipped 7.5 CSVs.

    Production callers leave plexos_extract_dir unset, so the templater
    resolves the extract through importlib.resources. This guards that the
    three CSVs ship with the package and stay discoverable.
    """
    extract_dir = _plexos_extract_dir("7.5")

    assert (extract_dir / "constraints.csv").is_file()
    assert (extract_dir / "lhs_terms.csv").is_file()
    assert (extract_dir / "rhs_values.csv").is_file()
