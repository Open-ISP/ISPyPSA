"""Tests for ``custom_constraints_from_plexos``.

The file opens with an end-to-end test of the public
``template_custom_constraints_from_plexos`` orchestrator against a small but
representative PLEXOS extract, asserting the full content of all three output
tables. The remaining tests cover each private helper in isolation.

A second end-to-end test -- the workbook-vs-PLEXOS reconciliation that runs the
real production logic against a workbook-derived fixture -- lives in
``test_custom_constraints_validation.py``.
"""

import numpy as np
import pandas as pd
import pytest

from ispypsa.templater.custom_constraints_from_plexos import (
    _add_term_type_column,
    _add_variable_name_column,
    _assert_no_date_to,
    _batteries_by_location,
    _battery_rows_for_triggers,
    _battery_to_location,
    _build_custom_constraints,
    _build_custom_constraints_lhs,
    _build_custom_constraints_rhs,
    _dedupe_lhs_terms,
    _drop_battery_load_coefficient_rows,
    _drop_constraint_relaxation_terms,
    _drop_excluded_classes,
    _drop_unresolved_terms,
    _inject_iasr_new_entrant_batteries,
    _line_names_to_path_ids,
    _location_battery_pairs,
    _log_injected_batteries,
    _match_to_templated_unit_names,
    _new_entrant_to_location,
    _plexos_extract_dir,
    _set_unit_term_types_from_templated_tables,
    _surviving_battery_coefficients,
    _tag_to_timeslice,
    _triggered_locations_per_constraint,
    _unit_term_types,
    _warn_on_constraints_missing_lhs,
    _warn_on_default_battery_coefficients,
    template_custom_constraints_from_plexos,
)

# --- template_custom_constraints_from_plexos (end-to-end) ---


def _write_plexos_extract(plexos_dir):
    """Write a small but representative PLEXOS extract for the end-to-end test.

    Exercises every translation path: all three senses, each
    (parent_class, property) -> term_type pair, the DN1 -> DREZ battery
    rename, the three dropped-row categories (Purchaser, Installed Capacity
    Coefficient, battery Load Coefficient), an unmatched generator that
    gets dropped, and a PLEXOS generator (SHGEN01) that ISPyPSA templates as
    storage.
    """
    plexos_dir.mkdir()
    (plexos_dir / "constraints.csv").write_text(
        "constraint_name,property,value,date_from,date_to,tags\n"
        "ExportGroup_SWQLD1,Sense,-1.0,,,\n"
        "ExportGroup_SWQLD1,Penalty Price,-1.0,,,\n"
        "ExportGroup_SWQLD1,Include in LT Plan,-1.0,,,\n"
        "ExportGroup_NET1,Sense,1.0,,,\n"
        "ExportGroup_EQ,Sense,0.0,,,\n"
    )
    (plexos_dir / "lhs_terms.csv").write_text(
        "constraint_name,parent_class,parent_name,property,value,date_from,date_to,tags\n"
        "ExportGroup_SWQLD1,Generator,BW01,Generation Sent Out Coefficient,0.5,,,\n"
        "ExportGroup_SWQLD1,Generator,Q8_SAT_Brisbane,Generation Sent Out Coefficient,0.4,,,\n"
        "ExportGroup_SWQLD1,Generator,SA Hydrogen Turbine,Generation Sent Out Coefficient,1.0,,,\n"
        "ExportGroup_SWQLD1,Generator,SWQLD1_Linear Augmentation,Installed Capacity Coefficient,-1.0,,,\n"
        "ExportGroup_SWQLD1,Battery,DN1 Dubbo Battery - 2h,Generation Coefficient,1.0,,,\n"
        "ExportGroup_SWQLD1,Battery,DN1 Dubbo Battery - 2h,Load Coefficient,-1.0,,,\n"
        "ExportGroup_SWQLD1,Line,NSW1-QLD1,Flow Coefficient,0.8,,,\n"
        "ExportGroup_SWQLD1,Node,CNSW,Load Coefficient,1.0,,,\n"
        "ExportGroup_SWQLD1,Purchaser,Q1 to NQ Flexible Electrolyser,Load Coefficient,-1.0,,,\n"
        "ExportGroup_NET1,Generator,T1_Wind,Generation Sent Out Coefficient,0.6,,,\n"
        "ExportGroup_NET1,Generator,SHGEN01,Generation Sent Out Coefficient,0.3,,,\n"
        "ExportGroup_EQ,Node,NQ,Load Coefficient,1.0,,,\n"
    )
    (plexos_dir / "rhs_values.csv").write_text(
        "constraint_name,value,date_from,date_to,tags\n"
        "ExportGroup_SWQLD1,3000.0,,,QLD Hot Day\n"
        "ExportGroup_SWQLD1,2900.0,,,QLD Winter\n"
        "ExportGroup_NET1,1200.0,,,TAS Winter\n"
        "ExportGroup_EQ,500.0,,,QLD Typical Summer\n"
    )


def test_template_custom_constraints_from_plexos_end_to_end(
    tmp_path, csv_str_to_df, caplog
):
    plexos_dir = tmp_path / "plexos"
    _write_plexos_extract(plexos_dir)
    generators_existing_planned = csv_str_to_df("""
        name
        BW01
    """)
    generators_new_entrant = csv_str_to_df("""
        name,             geo_id
        Q8_SAT_Brisbane,  Q8
        T1_Wind,          T1
    """)
    storage_existing_planned = csv_str_to_df("""
        name
        SHGEN01
    """)
    storage_new_entrant = csv_str_to_df("""
        name,                     technology,                      geo_id
        Q8 Battery - 2h,          Battery Storage (2hrs storage),  Q8
        Q8 Battery - 4h,          Battery Storage (4hrs storage),  Q8
        DREZ Dubbo Battery - 2h,  Battery Storage (2hrs storage),  DN1
        T1 Battery - 2h,          Battery Storage (2hrs storage),  T1
        T1 Pumped Hydro - 10h,    Pumped Hydro (10hrs storage),    T1
    """)

    with caplog.at_level("INFO"):
        out = template_custom_constraints_from_plexos(
            generators_existing_planned,
            generators_new_entrant,
            storage_existing_planned,
            storage_new_entrant,
            iasr_workbook_version="ignored",
            plexos_extract_dir=plexos_dir,
        )

    assert set(out) == {
        "custom_constraints",
        "custom_constraints_lhs",
        "custom_constraints_rhs",
    }

    # constraints: sense -> direction, ExportGroup_ prefix stripped.
    expected_constraints = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         <=
        NET1,           >=
        EQ,             =
    """)
    pd.testing.assert_frame_equal(
        out["custom_constraints"].sort_values("constraint_id").reset_index(drop=True),
        expected_constraints.sort_values("constraint_id").reset_index(drop=True),
    )

    # LHS: pass-1 PLEXOS translation + pass-2 new-entrant battery injection.
    # Pass 1 keeps BW01 (existing) and Q8_SAT_Brisbane (new entrant); renames
    # DN1 Dubbo Battery -> DREZ; translates the NSW1-QLD1 line and CNSW node;
    # drops the Purchaser, Installed Capacity Coefficient and battery Load
    # Coefficient rows, plus the unmatched SA Hydrogen Turbine. Pass 2 injects
    # the Q8 batteries (triggered by Q8_SAT_Brisbane in SWQLD1), the T1
    # battery (triggered by T1_Wind in NET1) but not T1 Pumped Hydro - 10h,
    # which is new-entrant storage but not a battery, and re-injects the
    # surviving DREZ Dubbo battery (its own DN1 trigger; deduped against
    # pass 1). SHGEN01 is a PLEXOS generator listed in storage_existing_planned,
    # so its term is storage_output. EQ carries only its NQ load term.
    expected_lhs = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,            coefficient,  date_from
        EQ,             load,              NQ,                       1.0,
        NET1,           generator_output,  T1_Wind,                  0.6,
        NET1,           storage_output,    SHGEN01,                  0.3,
        NET1,           storage_output,    T1 Battery - 2h,          1.0,
        SWQLD1,         generator_output,  BW01,                     0.5,
        SWQLD1,         generator_output,  Q8_SAT_Brisbane,          0.4,
        SWQLD1,         link_flow,         NSW-QLD,                  0.8,
        SWQLD1,         load,              CNSW,                     1.0,
        SWQLD1,         storage_output,    DREZ Dubbo Battery - 2h,  1.0,
        SWQLD1,         storage_output,    Q8 Battery - 2h,          1.0,
        SWQLD1,         storage_output,    Q8 Battery - 4h,          1.0,
    """)
    lhs_sort = ["constraint_id", "term_type", "variable_name"]
    pd.testing.assert_frame_equal(
        out["custom_constraints_lhs"].sort_values(lhs_sort).reset_index(drop=True),
        expected_lhs.sort_values(lhs_sort).reset_index(drop=True),
        check_dtype=False,
    )

    # RHS: regional tags -> region-prefixed canonical timeslices.
    expected_rhs = csv_str_to_df("""
        constraint_id,  timeslice,             rhs,     date_from
        EQ,             qld_summer_typical,    500.0,
        NET1,           tas_winter_reference,  1200.0,
        SWQLD1,         qld_peak_demand,       3000.0,
        SWQLD1,         qld_winter_reference,  2900.0,
    """)
    rhs_sort = ["constraint_id", "timeslice"]
    pd.testing.assert_frame_equal(
        out["custom_constraints_rhs"].sort_values(rhs_sort).reset_index(drop=True),
        expected_rhs.sort_values(rhs_sort).reset_index(drop=True),
        check_dtype=False,
    )

    # End-to-end side effects: dropping the unmatched generator loosens the
    # constraint (WARNING); the term_type change and the pass-2 injection are
    # summarised (INFO).
    assert "SWQLD1: SA Hydrogen Turbine" in caplog.text
    assert "['SHGEN01: generator_output -> storage_output']" in caplog.text
    assert "Injected 4 new-entrant battery storage_output rows" in caplog.text


# --- _assert_no_date_to ---


def test_assert_no_date_to_passes_when_all_empty(csv_str_to_df):
    df = csv_str_to_df("""
        constraint_name,  property,  value,  date_to
        X,                Sense,     -1.0,
    """)

    _assert_no_date_to(df, "test")  # no raise


def test_assert_no_date_to_raises_when_any_populated(csv_str_to_df):
    df = csv_str_to_df("""
        constraint_name,  property,  value,  date_to
        X,                Sense,     -1.0,
        X,                Sense,     -1.0,   2030-01-01T00:00:00
    """)

    with pytest.raises(ValueError, match="non-empty date_to"):
        _assert_no_date_to(df, "test")


# --- _unit_term_types ---


_EMPTY_NAMES = pd.DataFrame(columns=["name"])


def test_unit_term_types_maps_each_table_to_its_term_type(csv_str_to_df):
    generators_existing_planned = csv_str_to_df("""
        name
        BW01
        BW02
    """)
    generators_new_entrant = csv_str_to_df("""
        name
        Q8_SAT_Brisbane
    """)
    storage_existing_planned = csv_str_to_df("""
        name
        SHGEN01
    """)
    storage_new_entrant = csv_str_to_df("""
        name
        Q8 Battery - 2h
    """)

    result = _unit_term_types(
        generators_existing_planned,
        generators_new_entrant,
        storage_existing_planned,
        storage_new_entrant,
    )

    assert result == {
        "BW01": "generator_output",
        "BW02": "generator_output",
        "Q8_SAT_Brisbane": "generator_output",
        "SHGEN01": "storage_output",
        "Q8 Battery - 2h": "storage_output",
    }


def test_unit_term_types_handles_empty_generator_tables(csv_str_to_df):
    storage_existing_planned = csv_str_to_df("""
        name
        SHGEN01
    """)
    storage_new_entrant = csv_str_to_df("""
        name
        Q8 Battery - 2h
    """)

    result = _unit_term_types(
        _EMPTY_NAMES, _EMPTY_NAMES, storage_existing_planned, storage_new_entrant
    )

    assert result == {"SHGEN01": "storage_output", "Q8 Battery - 2h": "storage_output"}


def test_unit_term_types_handles_empty_storage_tables(csv_str_to_df):
    generators_existing_planned = csv_str_to_df("""
        name
        BW01
    """)
    generators_new_entrant = csv_str_to_df("""
        name
        Q8_SAT_Brisbane
    """)

    result = _unit_term_types(
        generators_existing_planned, generators_new_entrant, _EMPTY_NAMES, _EMPTY_NAMES
    )

    assert result == {"BW01": "generator_output", "Q8_SAT_Brisbane": "generator_output"}


def test_unit_term_types_handles_all_empty():
    result = _unit_term_types(_EMPTY_NAMES, _EMPTY_NAMES, _EMPTY_NAMES, _EMPTY_NAMES)

    assert result == {}


# --- _build_custom_constraints ---


def test_build_custom_constraints_filters_sense_and_strips_prefix(csv_str_to_df):
    constraints = csv_str_to_df("""
        constraint_name,      property,            value
        ExportGroup_SWQLD1,   Sense,               -1.0
        ExportGroup_SWQLD1,   Penalty Price,       -1.0
        ExportGroup_SWQLD1,   Include in LT Plan,  -1.0
        CNSW-SNW South GPG,   Sense,                1.0
        CNSW-SNW South GPG,   Penalty Price,       -1.0
        CNSW-SNW South GPG,   Include in LT Plan,  -1.0
        ExportGroup_EQ,       Sense,                0.0
        ExportGroup_EQ,       Penalty Price,       -1.0
        ExportGroup_EQ,       Include in LT Plan,  -1.0
    """)

    result = _build_custom_constraints(constraints)

    expected = csv_str_to_df("""
        constraint_id,        direction
        SWQLD1,               <=
        CNSW-SNW South GPG,   >=
        EQ,                   =
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_build_custom_constraints_raises_on_unmapped_sense(csv_str_to_df):
    # A sense outside {-1, 0, 1} is a new PLEXOS encoding the templater
    # doesn't know -- it must raise, not pass through as a NaN direction.
    constraints = csv_str_to_df("""
        constraint_name,      property,  value
        ExportGroup_SWQLD1,   Sense,     2.0
    """)

    with pytest.raises(ValueError, match="no direction mapping"):
        _build_custom_constraints(constraints)


# --- _drop_excluded_classes (Purchaser) ---


def test_drop_excluded_classes_drops_purchaser_and_logs(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        parent_class,  parent_name
        Generator,     KINGASF1
        Purchaser,     Some Electrolyser
        Generator,     BW01
    """)

    with caplog.at_level("INFO"):
        result = _drop_excluded_classes(lhs)

    expected = csv_str_to_df("""
        parent_class,  parent_name
        Generator,     KINGASF1
        Generator,     BW01
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
    assert "Dropped 1 LHS rows" in caplog.text
    assert "Purchaser" in caplog.text
    assert "Some Electrolyser" in caplog.text


def test_drop_excluded_classes_no_log_when_none_excluded(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        parent_class,  parent_name
        Generator,     KINGASF1
    """)

    with caplog.at_level("INFO"):
        _drop_excluded_classes(lhs)

    assert "Dropped" not in caplog.text


# --- _drop_constraint_relaxation_terms ---


def test_drop_constraint_relaxation_terms_drops_and_logs(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        parent_class,  parent_name,                   property
        Generator,     KINGASF1,                      Generation Sent Out Coefficient
        Generator,     SWQLD1_Linear Augmentation,   Installed Capacity Coefficient
    """)

    with caplog.at_level("INFO"):
        result = _drop_constraint_relaxation_terms(lhs)

    expected = csv_str_to_df("""
        parent_class,  parent_name,  property
        Generator,     KINGASF1,     Generation Sent Out Coefficient
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
    assert "Dropped 1 constraint relaxation LHS rows" in caplog.text
    assert "SWQLD1_Linear Augmentation" in caplog.text


def test_drop_constraint_relaxation_terms_no_log_when_none(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        parent_class,  parent_name,  property
        Generator,     KINGASF1,     Generation Sent Out Coefficient
    """)

    with caplog.at_level("INFO"):
        _drop_constraint_relaxation_terms(lhs)

    assert "Dropped" not in caplog.text


# --- _drop_battery_load_coefficient_rows ---


def test_drop_battery_load_coefficient_rows_drops_only_battery_load(
    csv_str_to_df, caplog
):
    lhs = csv_str_to_df("""
        constraint_name,    parent_class,  parent_name,   property
        ExportGroup_SWQLD1, Battery,       Tarong BESS,  Generation Coefficient
        ExportGroup_SWQLD1, Battery,       Tarong BESS,  Load Coefficient
        ExportGroup_SWQLD1, Node,          NSA,           Load Coefficient
    """)

    with caplog.at_level("INFO"):
        result = _drop_battery_load_coefficient_rows(lhs)

    expected = csv_str_to_df("""
        constraint_name,    parent_class,  parent_name,   property
        ExportGroup_SWQLD1, Battery,       Tarong BESS,  Generation Coefficient
        ExportGroup_SWQLD1, Node,          NSA,           Load Coefficient
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
    assert (
        "Dropped 1 battery Load Coefficient LHS rows "
        "(negative pairs of the kept Generation Coefficient rows)"
    ) in caplog.text


def test_drop_battery_load_coefficient_rows_no_log_when_none(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        constraint_name,    parent_class,  parent_name,  property
        ExportGroup_SWQLD1, Battery,       Tarong BESS,  Generation Coefficient
    """)

    with caplog.at_level("INFO"):
        _drop_battery_load_coefficient_rows(lhs)

    assert "Dropped" not in caplog.text


def test_drop_battery_load_coefficient_rows_raises_on_unpaired_load(csv_str_to_df):
    # A battery Load row with no Generation pair is the battery's only LHS
    # term -- dropping it would silently loosen the constraint, so it raises.
    lhs = csv_str_to_df("""
        constraint_name,    parent_class,  parent_name,  property
        ExportGroup_SWQLD1, Battery,       Lone BESS,    Load Coefficient
        ExportGroup_SWQLD1, Battery,       Tarong BESS,  Generation Coefficient
        ExportGroup_SWQLD1, Battery,       Tarong BESS,  Load Coefficient
    """)

    with pytest.raises(ValueError, match="no Generation Coefficient pair"):
        _drop_battery_load_coefficient_rows(lhs)


# --- _add_term_type_column + _raise_on_unmapped_term_type ---


def test_add_term_type_column_maps_known_pairs(csv_str_to_df):
    lhs = csv_str_to_df("""
        parent_class,  property
        Generator,     Generation Sent Out Coefficient
        Battery,       Generation Coefficient
        Line,          Flow Coefficient
        Node,          Load Coefficient
    """)

    result = _add_term_type_column(lhs)

    expected = csv_str_to_df("""
        parent_class,  property,                            term_type
        Generator,     Generation Sent Out Coefficient,  generator_output
        Battery,       Generation Coefficient,             storage_output
        Line,          Flow Coefficient,                   link_flow
        Node,          Load Coefficient,                   load
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_add_term_type_column_raises_on_unknown_pair(csv_str_to_df):
    lhs = csv_str_to_df("""
        parent_class,  property
        Storage,       MagicProperty
    """)

    with pytest.raises(ValueError, match="no term_type mapping"):
        _add_term_type_column(lhs)


# --- _match_to_templated_unit_names ---


def test_match_to_templated_unit_names_renames_then_matches():
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
            "DREZ_SAT_Dubbo",  # DN1 prefix, then "_"
            "DREZ Marulan Battery - 2h",  # DN3 prefix, then " "
            "DN10_SAT_Elsewhere",  # DN10 is a different first token: not renamed
            "OTHER_SAT_X",  # prefix not one of DN1/DN3
            "CNSW SAT - Distributed Resources",  # Area suffix stripped
            "CNSW V2G",  # multi-digit Area number
            "Areaway",  # "Area" inside another word
            "BW01",  # neither rule applies
            "Q2_SAT_North Qld Clean Energy Hub",  # matched ignoring case
            np.nan,  # constraint-scoped: no rename, so no templated unit
            np.nan,  # no templated unit
        ]
    )
    pd.testing.assert_series_equal(result, expected)


def test_match_to_templated_unit_names_prefers_the_exact_spelling():
    # Two templated units differ only by capitalisation. A PLEXOS name that is
    # exactly one of them keeps that spelling, whichever the lower-cased
    # lookup would have picked.
    plexos_names = pd.Series(["North QLD Hub", "North Qld Hub"])
    unit_names = {"North QLD Hub", "North Qld Hub"}

    result = _match_to_templated_unit_names(plexos_names, unit_names)

    expected = pd.Series(["North QLD Hub", "North Qld Hub"])
    pd.testing.assert_series_equal(result, expected)


def test_match_to_templated_unit_names_no_templated_units():
    plexos_names = pd.Series(["BW01", "UNKNOWN"])

    result = _match_to_templated_unit_names(plexos_names, set())

    expected = pd.Series([np.nan, np.nan], dtype=object)
    pd.testing.assert_series_equal(result, expected)


def test_match_to_templated_unit_names_no_names():
    plexos_names = pd.Series([], dtype=object)

    result = _match_to_templated_unit_names(plexos_names, {"BW01"})

    expected = pd.Series([], dtype=object)
    pd.testing.assert_series_equal(result, expected)


# --- _line_names_to_path_ids ---


def test_line_names_to_path_ids_known():
    result = _line_names_to_path_ids(pd.Series(["NSW1-QLD1", "SQ-CQ"]))

    expected = pd.Series(["NSW-QLD", "SQ-CQ"])
    pd.testing.assert_series_equal(result, expected)


def test_line_names_to_path_ids_raises_for_unknown():
    line_names = pd.Series(["NSW1-QLD1", "MADE-UP-LINE"])

    with pytest.raises(
        ValueError,
        match=r"not in _LINE_TO_PATH_ID -- add a mapping: \['MADE-UP-LINE'\]",
    ):
        _line_names_to_path_ids(line_names)


# --- _add_variable_name_column (orchestration) + rename logging ---


def test_add_variable_name_column_names_each_parent_class(csv_str_to_df):
    lhs = csv_str_to_df("""
        parent_class,  parent_name
        Generator,     DN1_SAT_Dubbo
        Generator,     SA Hydrogen Turbine
        Battery,       DN1 Dubbo Battery - 4h
        Battery,       SWQLD1 Battery - 2h
        Line,          NSW1-QLD1
        Node,          CNSW
    """)
    unit_names = {"DREZ_SAT_Dubbo", "DREZ Dubbo Battery - 4h"}

    result = _add_variable_name_column(lhs, unit_names)

    # Generators and batteries are renamed and matched to unit_names; SA
    # Hydrogen Turbine and the constraint-scoped SWQLD1 battery match nothing,
    # so their variable_name is blank. The line goes through the path_id table
    # and the node keeps its name.
    expected = csv_str_to_df("""
        parent_class,  parent_name,             variable_name
        Generator,     DN1_SAT_Dubbo,           DREZ_SAT_Dubbo
        Generator,     SA Hydrogen Turbine,
        Battery,       DN1 Dubbo Battery - 4h,  DREZ Dubbo Battery - 4h
        Battery,       SWQLD1 Battery - 2h,
        Line,          NSW1-QLD1,               NSW-QLD
        Node,          CNSW,                    CNSW
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_add_variable_name_column_raises_on_unexpected_parent_class(csv_str_to_df):
    lhs = csv_str_to_df("""
        parent_class,  parent_name
        Generator,     BW01
        Storage,       X
    """)

    with pytest.raises(
        ValueError, match=r"Unexpected LHS parent_class values: \['Storage'\]"
    ):
        _add_variable_name_column(lhs, {"BW01"})


def test_add_variable_name_column_empty_lhs(csv_str_to_df):
    lhs = pd.DataFrame(columns=["parent_class", "parent_name"], dtype=object)

    result = _add_variable_name_column(lhs, {"BW01"})

    expected = csv_str_to_df("""
        parent_class,  parent_name,  variable_name
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_add_variable_name_column_logs_renames(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        parent_class,  parent_name
        Generator,     DN1_SAT_Dubbo
        Generator,     BW01
    """)
    unit_names = {"DREZ_SAT_Dubbo", "BW01"}

    with caplog.at_level("INFO"):
        result = _add_variable_name_column(lhs, unit_names)

    expected = csv_str_to_df("""
        parent_class,  parent_name,    variable_name
        Generator,     DN1_SAT_Dubbo,  DREZ_SAT_Dubbo
        Generator,     BW01,           BW01
    """)
    pd.testing.assert_frame_equal(result, expected)
    assert (
        "Applied 1 PLEXOS->ISPyPSA LHS name renames: "
        "['DN1_SAT_Dubbo -> DREZ_SAT_Dubbo']"
    ) in caplog.text


def test_add_variable_name_column_no_log_when_no_renames(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        parent_class,  parent_name
        Generator,     BW01
    """)

    with caplog.at_level("INFO"):
        _add_variable_name_column(lhs, {"BW01"})

    assert "renames" not in caplog.text


# --- _drop_unresolved_terms ---


def test_drop_unresolved_terms_drops_nan_and_warns(csv_str_to_df, caplog):
    lhs = pd.DataFrame(
        {
            "constraint_name": ["ExportGroup_NSA1", "ExportGroup_NSA1"],
            "parent_name": ["SA Hydrogen Turbine", "BW01"],
            "variable_name": [None, "BW01"],
        }
    )

    with caplog.at_level("WARNING"):
        result = _drop_unresolved_terms(lhs)

    expected = csv_str_to_df("""
        constraint_name,   parent_name,  variable_name
        ExportGroup_NSA1,  BW01,         BW01
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
    assert "Dropped 1 LHS term rows" in caplog.text
    assert "NSA1: SA Hydrogen Turbine" in caplog.text


def test_drop_unresolved_terms_no_warn_when_all_resolved(csv_str_to_df, caplog):
    lhs = pd.DataFrame(
        {
            "constraint_name": ["ExportGroup_NSA1"],
            "parent_name": ["BW01"],
            "variable_name": ["BW01"],
        }
    )

    with caplog.at_level("WARNING"):
        _drop_unresolved_terms(lhs)

    assert "Dropped" not in caplog.text


# --- _warn_on_constraints_missing_lhs ---


def test_warn_on_constraints_missing_lhs_warns_when_missing(csv_str_to_df, caplog):
    constraints = csv_str_to_df("""
        constraint_id,  direction
        A,              <=
        B,              <=
    """)
    lhs = csv_str_to_df("""
        constraint_id,  term_type
        A,              generator_output
    """)

    with caplog.at_level("WARNING"):
        _warn_on_constraints_missing_lhs(constraints, lhs)

    assert "Custom constraints left with no LHS terms" in caplog.text
    assert "'B'" in caplog.text


def test_warn_on_constraints_missing_lhs_no_warn_when_complete(csv_str_to_df, caplog):
    constraints = csv_str_to_df("""
        constraint_id,  direction
        A,              <=
    """)
    lhs = csv_str_to_df("""
        constraint_id,  term_type
        A,              generator_output
    """)

    with caplog.at_level("WARNING"):
        _warn_on_constraints_missing_lhs(constraints, lhs)

    assert "no LHS terms" not in caplog.text


# --- _build_custom_constraints_lhs (helper wiring) ---


def test_build_custom_constraints_lhs_wires_filters_renames_and_drops(csv_str_to_df):
    lhs_terms = csv_str_to_df("""
        constraint_name,     parent_class,  parent_name,           property,                            value,  date_from
        ExportGroup_SWQLD1,  Battery,       SWQLD1 Battery - 2h, Generation Coefficient,            1.0,
        ExportGroup_SWQLD1,  Battery,       SWQLD1 Battery - 2h, Load Coefficient,                  -1.0,
        ExportGroup_SWQLD1,  Generator,     BW01,                  Generation Sent Out Coefficient,  0.5,
        ExportGroup_SWQLD1,  Generator,     Q8_SAT_Brisbane,       Generation Sent Out Coefficient,  0.4,
        ExportGroup_SWQLD1,  Generator,     UNKNOWN,               Generation Sent Out Coefficient,  1.0,
        ExportGroup_SWQLD1,  Purchaser,     Some Electrolyser,    Load Coefficient,                   -1.0,
        ExportGroup_SWQLD1,  Generator,     SWQLD1_Linear Augmentation, Installed Capacity Coefficient, -1.0,
    """)
    unit_term_types = {
        "BW01": "generator_output",
        "Q8_SAT_Brisbane": "generator_output",
        "Q8 Battery - 2h": "storage_output",
    }
    generators_new_entrant = csv_str_to_df("""
        name,             geo_id
        Q8_SAT_Brisbane,  Q8
    """)
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        Q8 Battery - 2h,  Battery Storage (2hrs storage),  Q8
    """)

    result = _build_custom_constraints_lhs(
        lhs_terms, unit_term_types, generators_new_entrant, storage_new_entrant
    )

    # Purchaser dropped, Installed Capacity dropped, Battery Load Coefficient
    # dropped, SWQLD1 constraint-scoped Battery dropped (no templated unit),
    # UNKNOWN dropped (no templated unit). BW01 + Q8_SAT_Brisbane survive pass 1;
    # pass 2 injects Q8 Battery - 2h because Q8_SAT_Brisbane triggers REZ Q8.
    expected = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,      coefficient,  date_from
        SWQLD1,         generator_output,  BW01,               0.5,
        SWQLD1,         generator_output,  Q8_SAT_Brisbane,    0.4,
        SWQLD1,         storage_output,    Q8 Battery - 2h, 1.0,
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["term_type", "variable_name"]).reset_index(drop=True),
        expected.sort_values(["term_type", "variable_name"]).reset_index(drop=True),
        check_dtype=False,
    )


# --- _set_unit_term_types_from_templated_tables ---


def test_set_unit_term_types_from_templated_tables_follows_templated_table(
    csv_str_to_df, caplog
):
    lhs = csv_str_to_df("""
        constraint_name,  term_type,         variable_name
        X,                generator_output,  BW01
        X,                generator_output,  SHGEN01
        X,                generator_output,  SHGEN02
        Y,                generator_output,  SHGEN01
        X,                storage_output,    Q8 Battery - 2h
        X,                link_flow,         NSW-QLD
        X,                load,              CNSW
    """)
    unit_term_types = {
        "BW01": "generator_output",
        "SHGEN01": "storage_output",
        "SHGEN02": "storage_output",
        "Q8 Battery - 2h": "storage_output",
    }

    with caplog.at_level("INFO"):
        result = _set_unit_term_types_from_templated_tables(lhs, unit_term_types)

    # SHGEN01 and SHGEN02 are generator terms on units templated as storage, so
    # all three of their rows become storage_output. The link and load terms
    # aren't unit terms and keep their types.
    expected = csv_str_to_df("""
        constraint_name,  term_type,         variable_name
        X,                generator_output,  BW01
        X,                storage_output,    SHGEN01
        X,                storage_output,    SHGEN02
        Y,                storage_output,    SHGEN01
        X,                storage_output,    Q8 Battery - 2h
        X,                link_flow,         NSW-QLD
        X,                load,              CNSW
    """)
    pd.testing.assert_frame_equal(result, expected)
    assert (
        "Changed the term_type of 3 LHS term rows to match the table their unit "
        "is templated in: ['SHGEN01: generator_output -> storage_output', "
        "'SHGEN02: generator_output -> storage_output']"
    ) in caplog.text


def test_set_unit_term_types_from_templated_tables_no_log_when_types_agree(
    csv_str_to_df, caplog
):
    lhs = csv_str_to_df("""
        constraint_name,  term_type,         variable_name
        X,                generator_output,  BW01
        X,                storage_output,    Q8 Battery - 2h
        X,                load,              CNSW
    """)
    unit_term_types = {"BW01": "generator_output", "Q8 Battery - 2h": "storage_output"}

    with caplog.at_level("INFO"):
        result = _set_unit_term_types_from_templated_tables(lhs, unit_term_types)

    pd.testing.assert_frame_equal(result, lhs)
    assert "Changed the term_type" not in caplog.text


def test_set_unit_term_types_from_templated_tables_empty_lhs(csv_str_to_df):
    lhs = pd.DataFrame(columns=["constraint_name", "term_type", "variable_name"])

    result = _set_unit_term_types_from_templated_tables(
        lhs, {"BW01": "generator_output"}
    )

    expected = csv_str_to_df("""
        constraint_name,  term_type,  variable_name
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _tag_to_timeslice ---


@pytest.mark.parametrize(
    "tag, expected",
    [
        ("QLD Hot Day", "qld_peak_demand"),
        ("VIC Typical Summer", "vic_summer_typical"),
        ("TAS Winter", "tas_winter_reference"),
        ("NSW Hot Day", "nsw_peak_demand"),
        ("SA Winter", "sa_winter_reference"),
    ],
)
def test_tag_to_timeslice(tag, expected):
    assert _tag_to_timeslice(tag) == expected


def test_tag_to_timeslice_raises_on_unknown_suffix():
    with pytest.raises(KeyError):
        _tag_to_timeslice("QLD Mild Spring")


# --- _build_custom_constraints_rhs ---


def test_build_custom_constraints_rhs_maps_to_region_prefixed_canonical_timeslices(
    csv_str_to_df,
):
    rhs = csv_str_to_df("""
        constraint_name,      value,    date_from,  tags
        ExportGroup_SWQLD1,   3000.0,             ,  QLD Hot Day
        ExportGroup_SWQLD1,   3000.0,             ,  QLD Typical Summer
        ExportGroup_SWQLD1,   2900.0,             ,  QLD Winter
        ExportGroup_NET1,     1200.0,             ,  TAS Winter
    """)

    result = _build_custom_constraints_rhs(rhs)

    expected = csv_str_to_df("""
        constraint_id,  timeslice,             rhs,    date_from
        SWQLD1,         qld_peak_demand,       3000.0,
        SWQLD1,         qld_summer_typical,    3000.0,
        SWQLD1,         qld_winter_reference,  2900.0,
        NET1,           tas_winter_reference,  1200.0,
    """)
    pd.testing.assert_frame_equal(result, expected)


# --- _new_entrant_to_location ---


_EMPTY_GENERATORS_NEW_ENTRANT = pd.DataFrame(columns=["name", "geo_id"])
_EMPTY_STORAGE_NEW_ENTRANT = pd.DataFrame(columns=["name", "technology", "geo_id"])


def test_new_entrant_to_location_covers_generators_and_storage(csv_str_to_df):
    generators_new_entrant = csv_str_to_df("""
        name,      geo_id
        Q8 Solar,  Q8
        CSA Coal,  CSA
    """)
    storage_new_entrant = csv_str_to_df("""
        name,                   technology,                      geo_id
        Q1 Battery - 2h,        Battery Storage (2hrs storage),  Q1
        Q1 Pumped Hydro - 10h,  Pumped Hydro (10hrs storage),    Q1
    """)

    result = _new_entrant_to_location(generators_new_entrant, storage_new_entrant)

    assert result == {
        "Q8 Solar": "Q8",
        "CSA Coal": "CSA",
        "Q1 Battery - 2h": "Q1",
        "Q1 Pumped Hydro - 10h": "Q1",
    }


def test_new_entrant_to_location_empty_generators(csv_str_to_df):
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        Q1 Battery - 2h,  Battery Storage (2hrs storage),  Q1
    """)

    result = _new_entrant_to_location(
        _EMPTY_GENERATORS_NEW_ENTRANT, storage_new_entrant
    )

    assert result == {"Q1 Battery - 2h": "Q1"}


def test_new_entrant_to_location_empty_storage(csv_str_to_df):
    generators_new_entrant = csv_str_to_df("""
        name,      geo_id
        Q8 Solar,  Q8
    """)

    result = _new_entrant_to_location(
        generators_new_entrant, _EMPTY_STORAGE_NEW_ENTRANT
    )

    assert result == {"Q8 Solar": "Q8"}


def test_new_entrant_to_location_both_empty():
    result = _new_entrant_to_location(
        _EMPTY_GENERATORS_NEW_ENTRANT, _EMPTY_STORAGE_NEW_ENTRANT
    )

    assert result == {}


# --- _batteries_by_location ---


def test_batteries_by_location_groups_batteries_by_geo_id(csv_str_to_df):
    storage_new_entrant = csv_str_to_df("""
        name,                   technology,                      geo_id
        Q8 Battery - 2h,        Battery Storage (2hrs storage),  Q8
        Q8 Battery - 4h,        Battery Storage (4hrs storage),  Q8
        CSA Battery - 2h,       Battery Storage (2hrs storage),  CSA
        Q8 Pumped Hydro - 10h,  Pumped Hydro (10hrs storage),    Q8
    """)

    result = _batteries_by_location(storage_new_entrant)

    # Battery rows grouped by geo_id; the pumped hydro row is excluded; each
    # location's list is sorted.
    assert result == {
        "Q8": ["Q8 Battery - 2h", "Q8 Battery - 4h"],
        "CSA": ["CSA Battery - 2h"],
    }


def test_batteries_by_location_empty_input():
    result = _batteries_by_location(_EMPTY_STORAGE_NEW_ENTRANT)

    assert result == {}


# --- _triggered_locations_per_constraint ---


def test_triggered_locations_per_constraint_dedupes_and_drops_unknown(csv_str_to_df):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,         variable_name
        SWQLD1,         generator_output,  Q8 Solar
        SWQLD1,         generator_output,  Q8 Wind
        SWQLD1,         generator_output,  Tarong BESS
        SWQLD1,         link_flow,         NSW-QLD
        NQ1,            generator_output,  Q1 Solar
        NET1,           storage_output,    T1 Battery - 2h
    """)
    unit_to_location = {
        "Q8 Solar": "Q8",
        "Q8 Wind": "Q8",
        "Q1 Solar": "Q1",
        "T1 Battery - 2h": "T1",
    }

    result = _triggered_locations_per_constraint(lhs, unit_to_location)

    # Two SWQLD1 generators in Q8 dedupe to one row; Tarong BESS (not in
    # unit_to_location) drops; link_flow ignored; NET1's surviving battery
    # triggers T1 on its own.
    expected = csv_str_to_df("""
        constraint_id,  location
        SWQLD1,         Q8
        NQ1,            Q1
        NET1,           T1
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["constraint_id", "location"]).reset_index(drop=True),
        expected.sort_values(["constraint_id", "location"]).reset_index(drop=True),
    )


def test_triggered_locations_per_constraint_no_unit_terms(csv_str_to_df):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,   variable_name
        SWQLD1,         link_flow,   NSW-QLD
    """)
    unit_to_location = {"Q8 Solar": "Q8"}

    result = _triggered_locations_per_constraint(lhs, unit_to_location)

    expected = pd.DataFrame(columns=["constraint_id", "location"])
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _battery_rows_for_triggers ---


_EMPTY_COEFFICIENTS = pd.DataFrame(
    columns=["constraint_id", "location", "coefficient", "date_from"]
)


def test_battery_rows_for_triggers_defaults_to_one_without_profile(csv_str_to_df):
    triggered = csv_str_to_df("""
        constraint_id,  location
        SWQLD1,         Q8
        NQ1,            Q1
    """)
    batteries_by_location = {
        "Q8": ["Q8 Battery - 2h", "Q8 Battery - 4h"],
        "Q1": ["Q1 Battery - 2h"],
        "Q99": ["unused"],
    }

    result = _battery_rows_for_triggers(
        triggered, batteries_by_location, _EMPTY_COEFFICIENTS
    )

    # Q8 trigger -> 2 batteries injected; Q1 trigger -> 1 battery. Locations
    # with no triggering generators (Q99) are ignored. With no surviving-battery
    # profile, coefficient defaults to 1.0 and date_from is empty.
    expected = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        SWQLD1,         storage_output,  Q8 Battery - 2h,  1.0,
        SWQLD1,         storage_output,  Q8 Battery - 4h,  1.0,
        NQ1,            storage_output,  Q1 Battery - 2h,  1.0,
    """)
    sort_cols = ["constraint_id", "variable_name"]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_cols).reset_index(drop=True),
        expected.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_battery_rows_for_triggers_copies_location_profile(csv_str_to_df):
    triggered = csv_str_to_df("""
        constraint_id,  location
        SWQLD1,         Q8
        WNV1,           V7
        NQ1,            Q1
    """)
    batteries_by_location = {
        "Q8": ["Q8 Battery - 2h", "Q8 Battery - 4h"],
        "V7": ["V7 Battery - 4h"],
        "Q1": ["Q1 Battery - 2h"],
    }
    # SWQLD1/Q8 carries a flat 0.43; WNV1/V7 is time-varying (0.78 then 0.0);
    # NQ1/Q1 has no profile so its battery falls back to 1.0.
    coefficients = csv_str_to_df("""
        constraint_id,  location,  coefficient,  date_from
        SWQLD1,         Q8,        0.43,
        WNV1,           V7,        0.78,
        WNV1,           V7,        0.0,          2031-11-30T00:00:00
    """)

    result = _battery_rows_for_triggers(triggered, batteries_by_location, coefficients)

    # Both Q8 batteries inherit 0.43; the V7 battery is emitted once per profile
    # row (0.78 until 2031-11-30, then 0.0 from 2031-11-30); Q1's defaults to 1.0.
    expected = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        SWQLD1,         storage_output,  Q8 Battery - 2h,  0.43,
        SWQLD1,         storage_output,  Q8 Battery - 4h,  0.43,
        WNV1,           storage_output,  V7 Battery - 4h,  0.78,
        WNV1,           storage_output,  V7 Battery - 4h,  0.0,          2031-11-30T00:00:00
        NQ1,            storage_output,  Q1 Battery - 2h,  1.0,
    """)
    sort_cols = ["constraint_id", "variable_name", "coefficient"]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_cols).reset_index(drop=True),
        expected.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_battery_rows_for_triggers_missing_location_yields_no_rows():
    triggered = pd.DataFrame({"constraint_id": ["X"], "location": ["MISSING"]})
    batteries_by_location = {"Q1": ["Q1 Battery - 2h"]}

    result = _battery_rows_for_triggers(
        triggered, batteries_by_location, _EMPTY_COEFFICIENTS
    )

    expected = pd.DataFrame(
        columns=[
            "constraint_id",
            "term_type",
            "variable_name",
            "coefficient",
            "date_from",
        ]
    )
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _inject_iasr_new_entrant_batteries (end-to-end of pass 2) ---


def test_inject_iasr_new_entrant_batteries_appends_for_triggered_locations(
    csv_str_to_df,
):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  Q8 Solar,       0.4,
        SWQLD1,         link_flow,         NSW-QLD,        0.8,
    """)
    generators_new_entrant = csv_str_to_df("""
        name,      geo_id
        Q8 Solar,  Q8
    """)
    storage_new_entrant = csv_str_to_df("""
        name,                   technology,                      geo_id
        Q8 Battery - 2h,        Battery Storage (2hrs storage),  Q8
        Q8 Battery - 4h,        Battery Storage (4hrs storage),  Q8
        Q8 Pumped Hydro - 10h,  Pumped Hydro (10hrs storage),    Q8
    """)

    result = _inject_iasr_new_entrant_batteries(
        lhs, generators_new_entrant, storage_new_entrant
    )

    # Pass-1 rows preserved; pass-2 appends Q8 Battery - 2h and 4h for SWQLD1.
    # Q8 Pumped Hydro - 10h is new-entrant storage at Q8 but not a battery, so
    # it isn't injected.
    expected = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,    coefficient,  date_from
        SWQLD1,         generator_output,  Q8 Solar,         0.4,
        SWQLD1,         link_flow,         NSW-QLD,          0.8,
        SWQLD1,         storage_output,    Q8 Battery - 2h,  1.0,
        SWQLD1,         storage_output,    Q8 Battery - 4h,  1.0,
    """)
    sort_cols = ["term_type", "variable_name"]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_cols).reset_index(drop=True),
        expected.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_inject_iasr_new_entrant_batteries_no_trigger_no_injection(csv_str_to_df):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         link_flow,         NSW-QLD,        0.8,
    """)
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        Q8 Battery - 2h,  Battery Storage (2hrs storage),  Q8
    """)

    result = _inject_iasr_new_entrant_batteries(
        lhs, _EMPTY_GENERATORS_NEW_ENTRANT, storage_new_entrant
    )

    # No surviving generator term -> no triggers -> no injection.
    pd.testing.assert_frame_equal(result, lhs)


def test_inject_iasr_new_entrant_batteries_logs_summary(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  Q8 Solar,      0.4,
    """)
    generators_new_entrant = csv_str_to_df("""
        name,      geo_id
        Q8 Solar,  Q8
    """)
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        Q8 Battery - 2h,  Battery Storage (2hrs storage),  Q8
    """)

    with caplog.at_level("INFO"):
        _inject_iasr_new_entrant_batteries(
            lhs, generators_new_entrant, storage_new_entrant
        )

    assert "Injected 1 new-entrant battery storage_output rows" in caplog.text


# --- _log_injected_batteries (empty input) ---


def test_log_injected_batteries_empty_input_logs_zero_message(caplog):
    empty = pd.DataFrame(
        columns=[
            "constraint_id",
            "term_type",
            "variable_name",
            "coefficient",
            "date_from",
        ]
    )

    with caplog.at_level("INFO"):
        _log_injected_batteries(empty)

    assert "Injected no new-entrant batteries" in caplog.text


# --- _dedupe_lhs_terms ---


def test_dedupe_lhs_terms_keeps_first_on_full_key(csv_str_to_df):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        SWQLD1,         storage_output,  Q8 Battery - 2h,  0.5,
        SWQLD1,         storage_output,  Q8 Battery - 2h,  1.0,
    """)

    result = _dedupe_lhs_terms(lhs)

    # Same (constraint, term, variable, date_from) -> keep first row (0.5).
    expected = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        SWQLD1,         storage_output,  Q8 Battery - 2h,  0.5,
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_dedupe_lhs_terms_preserves_time_varying_rows(csv_str_to_df):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        SWQLD1,         storage_output,  Q8 Battery - 2h,  0.33,
        SWQLD1,         storage_output,  Q8 Battery - 2h,  0.34,         2028-12-01T00:00:00
        SWQLD1,         storage_output,  Q8 Battery - 2h,  0.25,         2029-11-30T00:00:00
    """)

    result = _dedupe_lhs_terms(lhs)

    # All three rows survive: distinct date_from values are part of the key.
    pd.testing.assert_frame_equal(result.reset_index(drop=True), lhs)


def test_dedupe_lhs_terms_logs_when_dedupes(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        SWQLD1,         storage_output,  Q8 Battery - 2h, 0.5,
        SWQLD1,         storage_output,  Q8 Battery - 2h, 1.0,
    """)

    with caplog.at_level("INFO"):
        _dedupe_lhs_terms(lhs)

    assert "Deduped 1 overlapping LHS rows" in caplog.text


def test_dedupe_lhs_terms_no_log_when_nothing_to_dedupe(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        SWQLD1,         storage_output,  Q8 Battery - 2h, 0.5,
        SWQLD1,         generator_output, Q8 Solar,         0.4,
    """)

    with caplog.at_level("INFO"):
        _dedupe_lhs_terms(lhs)

    assert "Deduped" not in caplog.text


# --- _plexos_extract_dir ---


def test_plexos_extract_dir_resolves_to_shipped_extract():
    """The default extract directory points at the package's shipped 7.5 CSVs.

    Production callers leave ``plexos_extract_dir`` unset, so the templater
    resolves the extract through ``importlib.resources``. This guards that the
    three CSVs ship with the package and stay discoverable.
    """
    extract_dir = _plexos_extract_dir("7.5")

    assert (extract_dir / "constraints.csv").is_file()
    assert (extract_dir / "lhs_terms.csv").is_file()
    assert (extract_dir / "rhs_values.csv").is_file()


# --- _battery_to_location ---


def test_battery_to_location_maps_batteries_only(csv_str_to_df):
    storage_new_entrant = csv_str_to_df("""
        name,                   technology,                      geo_id
        Q8 Battery - 2h,        Battery Storage (2hrs storage),  Q8
        CSA Battery - 2h,       Battery Storage (2hrs storage),  CSA
        Q8 Pumped Hydro - 10h,  Pumped Hydro (10hrs storage),    Q8
    """)

    result = _battery_to_location(storage_new_entrant)

    # The pumped hydro row is excluded.
    assert result == {"Q8 Battery - 2h": "Q8", "CSA Battery - 2h": "CSA"}


# --- _location_battery_pairs ---


def test_location_battery_pairs_flattens(csv_str_to_df):
    result = _location_battery_pairs(
        {"Q8": ["Q8 Battery - 2h", "Q8 Battery - 4h"], "Q1": ["Q1 Battery - 2h"]}
    )

    expected = csv_str_to_df("""
        location,  variable_name
        Q8,        Q8 Battery - 2h
        Q8,        Q8 Battery - 4h
        Q1,        Q1 Battery - 2h
    """)
    sort_cols = ["location", "variable_name"]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_cols).reset_index(drop=True),
        expected.sort_values(sort_cols).reset_index(drop=True),
    )


# --- _surviving_battery_coefficients ---


def test_surviving_battery_coefficients_profiles_per_location(csv_str_to_df):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,    coefficient,  date_from
        SWQLD1,         generator_output,  Q8 Solar,         0.43,
        SWQLD1,         storage_output,    Q8 Battery - 2h,  0.43,
        SWQLD1,         storage_output,    Q8 Battery - 8h,  0.43,
        SWQLD1,         storage_output,    Tarong BESS,      0.14,
        SWQLD1,         storage_output,    Q8 Pumped Hydro - 10h,  0.9,
        WNV1,           storage_output,    V7 Battery - 2h,  0.78,
        WNV1,           storage_output,    V7 Battery - 2h,  0.0,          2031-11-30T00:00:00
    """)
    storage_new_entrant = csv_str_to_df("""
        name,                   technology,                      geo_id
        Q8 Battery - 2h,        Battery Storage (2hrs storage),  Q8
        Q8 Battery - 8h,        Battery Storage (8hrs storage),  Q8
        Q8 Pumped Hydro - 10h,  Pumped Hydro (10hrs storage),    Q8
        V7 Battery - 2h,        Battery Storage (2hrs storage),  V7
    """)

    result = _surviving_battery_coefficients(lhs, storage_new_entrant)

    # Q8's two surviving siblings collapse to one 0.43 profile row; the
    # generator term, the existing Tarong BESS (not a new-entrant battery) and
    # Q8 Pumped Hydro - 10h (new-entrant storage, but not a battery, so its 0.9
    # is not part of Q8's battery profile) are ignored; V7 keeps both
    # time-varying rows.
    expected = csv_str_to_df("""
        constraint_id,  location,  coefficient,  date_from
        SWQLD1,         Q8,        0.43,
        WNV1,           V7,        0.78,
        WNV1,           V7,        0.0,          2031-11-30T00:00:00
    """)
    sort_cols = ["constraint_id", "location", "coefficient"]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_cols).reset_index(drop=True),
        expected.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_surviving_battery_coefficients_raises_on_inconsistent_siblings(csv_str_to_df):
    # Two surviving Q8 batteries disagree on coefficient at the same date_from,
    # so the per-location copy onto the omitted durations would be ambiguous --
    # the load-bearing "siblings agree" assumption is violated and must raise.
    lhs = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        SWQLD1,         storage_output,  Q8 Battery - 2h,  0.43,
        SWQLD1,         storage_output,  Q8 Battery - 8h,  0.50,
    """)
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        Q8 Battery - 2h,  Battery Storage (2hrs storage),  Q8
        Q8 Battery - 8h,  Battery Storage (8hrs storage),  Q8
    """)

    with pytest.raises(ValueError, match="share one"):
        _surviving_battery_coefficients(lhs, storage_new_entrant)


def test_surviving_battery_coefficients_raises_on_divergent_date_sets(csv_str_to_df):
    # The siblings agree wherever both have a row, but the 2h battery is
    # time-varying while the 8h is constant. Copying the union profile would
    # silently graft the 2031 step onto the 8h battery (the injected extra-date
    # row survives deduping because nothing matches its date_from), so the
    # whole-profile comparison must raise.
    lhs = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        WNV1,           storage_output,  V7 Battery - 2h,  0.78,
        WNV1,           storage_output,  V7 Battery - 2h,  0.0,          2031-11-30T00:00:00
        WNV1,           storage_output,  V7 Battery - 8h,  0.78,
    """)
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        V7 Battery - 2h,  Battery Storage (2hrs storage),  V7
        V7 Battery - 8h,  Battery Storage (8hrs storage),  V7
    """)

    with pytest.raises(ValueError, match="share one"):
        _surviving_battery_coefficients(lhs, storage_new_entrant)


# --- _build_custom_constraints_lhs: injected battery inherits sibling coeff ---


def test_build_custom_constraints_lhs_injected_battery_inherits_sibling_coefficient(
    csv_str_to_df,
):
    # PLEXOS includes Q8 Battery - 2h at 0.43 (survives pass 1) but omits the
    # 4h duration; a Q8 new-entrant generator triggers the injection. The
    # injected 4h battery must inherit the 0.43 its 2h sibling carries, not the
    # 1.0 default -- this is the regression the coefficient-profile copy fixes.
    lhs_terms = csv_str_to_df("""
        constraint_name,     parent_class,  parent_name,      property,                         value,  date_from
        ExportGroup_SWQLD1,  Generator,     Q8_SAT_Brisbane,  Generation Sent Out Coefficient,  0.43,
        ExportGroup_SWQLD1,  Battery,       Q8 Battery - 2h,  Generation Coefficient,           0.43,
    """)
    unit_term_types = {
        "Q8_SAT_Brisbane": "generator_output",
        "Q8 Battery - 2h": "storage_output",
        "Q8 Battery - 4h": "storage_output",
    }
    generators_new_entrant = csv_str_to_df("""
        name,             geo_id
        Q8_SAT_Brisbane,  Q8
    """)
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        Q8 Battery - 2h,  Battery Storage (2hrs storage),  Q8
        Q8 Battery - 4h,  Battery Storage (4hrs storage),  Q8
    """)

    result = _build_custom_constraints_lhs(
        lhs_terms, unit_term_types, generators_new_entrant, storage_new_entrant
    )

    # Surviving 2h (0.43) is deduped against its pass-2 re-injection; the
    # omitted 4h is injected at the inherited 0.43, not 1.0.
    expected = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,    coefficient,  date_from
        SWQLD1,         generator_output,  Q8_SAT_Brisbane,  0.43,
        SWQLD1,         storage_output,    Q8 Battery - 2h,  0.43,
        SWQLD1,         storage_output,    Q8 Battery - 4h,  0.43,
    """)
    sort_cols = ["term_type", "variable_name"]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_cols).reset_index(drop=True),
        expected.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_build_custom_constraints_lhs_battery_only_survivor_triggers_injection(
    csv_str_to_df,
):
    # No new-entrant generator participates: the surviving 2h battery alone
    # triggers Q8, so the PLEXOS-omitted 4h duration is still injected, at the
    # 2h sibling's coefficient.
    lhs_terms = csv_str_to_df("""
        constraint_name,     parent_class,  parent_name,      property,                value,  date_from
        ExportGroup_SWQLD1,  Battery,       Q8 Battery - 2h,  Generation Coefficient,  0.43,
        ExportGroup_SWQLD1,  Battery,       Q8 Battery - 2h,  Load Coefficient,        -0.43,
    """)
    unit_term_types = {
        "Q8 Battery - 2h": "storage_output",
        "Q8 Battery - 4h": "storage_output",
    }
    storage_new_entrant = csv_str_to_df("""
        name,             technology,                      geo_id
        Q8 Battery - 2h,  Battery Storage (2hrs storage),  Q8
        Q8 Battery - 4h,  Battery Storage (4hrs storage),  Q8
    """)

    result = _build_custom_constraints_lhs(
        lhs_terms, unit_term_types, _EMPTY_GENERATORS_NEW_ENTRANT, storage_new_entrant
    )

    expected = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,    coefficient,  date_from
        SWQLD1,         storage_output,  Q8 Battery - 2h,  0.43,
        SWQLD1,         storage_output,  Q8 Battery - 4h,  0.43,
    """)
    sort_cols = ["term_type", "variable_name"]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_cols).reset_index(drop=True),
        expected.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


# --- _warn_on_default_battery_coefficients ---


def test_warn_on_default_battery_coefficients_warns_for_fallback(csv_str_to_df, caplog):
    triggered = csv_str_to_df("""
        constraint_id,  location
        SWQLD1,         Q8
        WV1,            V3
        NQ1,            S99
    """)
    # SWQLD1/Q8 has a surviving-sibling profile; WV1/V3 does not (-> default);
    # NQ1/S99 has no batteries to inject at all, so it must not be reported.
    coefficients = csv_str_to_df("""
        constraint_id,  location,  coefficient,  date_from
        SWQLD1,         Q8,        0.43,
    """)
    batteries_by_location = {"Q8": ["Q8 Battery - 4h"], "V3": ["V3 Battery - 4h"]}

    with caplog.at_level("WARNING"):
        _warn_on_default_battery_coefficients(
            triggered, coefficients, batteries_by_location
        )

    assert (
        "New-entrant batteries injected with default coefficient 1.0 "
        "(no surviving sibling to copy from): ['WV1: V3']"
    ) in caplog.text


def test_warn_on_default_battery_coefficients_silent_when_all_have_profiles(
    csv_str_to_df, caplog
):
    triggered = csv_str_to_df("""
        constraint_id,  location
        SWQLD1,         Q8
    """)
    coefficients = csv_str_to_df("""
        constraint_id,  location,  coefficient,  date_from
        SWQLD1,         Q8,        0.43,
    """)
    batteries_by_location = {"Q8": ["Q8 Battery - 4h"]}

    with caplog.at_level("WARNING"):
        _warn_on_default_battery_coefficients(
            triggered, coefficients, batteries_by_location
        )

    assert "default coefficient" not in caplog.text
