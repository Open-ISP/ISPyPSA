"""Tests for ``custom_constraints_from_plexos``.

The file opens with an end-to-end test of the public
``template_custom_constraints_from_plexos`` orchestrator against a small but
representative PLEXOS extract, asserting the full content of all three output
tables. The remaining tests cover each private helper in isolation.

A second end-to-end test -- the workbook-vs-PLEXOS reconciliation that runs the
real production logic against a workbook-derived fixture -- lives in
``test_custom_constraints_validation.py``.
"""

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
    _generator_to_location,
    _iasr_id_choices,
    _inject_iasr_new_entrant_batteries,
    _is_battery_row,
    _line_variable_name,
    _location_battery_pairs,
    _log_injected_batteries,
    _match_unit_name,
    _pick_location,
    _plexos_extract_dir,
    _rename_battery_name,
    _rename_first_token,
    _rename_generator_name,
    _resolve_variable_name,
    _strip_area_suffix,
    _surviving_battery_coefficients,
    _tag_to_timeslice,
    _triggered_locations_per_constraint,
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
    Coefficient, battery Load Coefficient), and an unmatched generator that
    gets dropped.
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
    iasr_tables = {
        "existing_committed_anticipated_additional_generator_summary": csv_str_to_df("""
            IASR ID / DLT names
            BW01
        """),
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,      Sub-region,  REZ ID,  Technology Type
            Q8_SAT_Brisbane,          SQ,          Q8,      Solar
            Q8 Battery - 2h,          SQ,          Q8,      Battery Storage (2hrs storage)
            Q8 Battery - 4h,          SQ,          Q8,      Battery Storage (4hrs storage)
            DREZ Dubbo Battery - 2h,  CNSW,        DN1,     Battery Storage (2hrs storage)
            T1_Wind,                  TAS,         T1,      Wind
            T1 Battery - 2h,          TAS,         T1,      Battery Storage (2hrs storage)
        """),
    }

    with caplog.at_level("INFO"):
        out = template_custom_constraints_from_plexos(
            iasr_tables, iasr_workbook_version="ignored", plexos_extract_dir=plexos_dir
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
    # battery (triggered by T1_Wind in NET1), and re-injects the surviving
    # DREZ Dubbo battery (its own DN1 trigger; deduped against pass 1).
    # EQ carries only its NQ load term.
    expected_lhs = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,            coefficient,  date_from
        EQ,             load,              NQ,                       1.0,
        NET1,           generator_output,  T1_Wind,                  0.6,
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
    # constraint (WARNING); the pass-2 injection is summarised (INFO).
    assert "SWQLD1: SA Hydrogen Turbine" in caplog.text
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


# --- _iasr_id_choices ---


def test_iasr_id_choices_unions_and_dedupes(csv_str_to_df):
    iasr = {
        "existing_committed_anticipated_additional_generator_summary": csv_str_to_df("""
            IASR ID / DLT names
            BW01
            BW02
            SHARED
        """),
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names
            Q1 Battery - 2h
            SHARED
        """),
    }

    result = _iasr_id_choices(iasr)

    assert result == {"BW01", "BW02", "SHARED", "Q1 Battery - 2h"}


def test_iasr_id_choices_handles_empty_existing(csv_str_to_df):
    iasr = {
        "existing_committed_anticipated_additional_generator_summary": pd.DataFrame(
            columns=["IASR ID / DLT names"]
        ),
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names
            ONLY_NEW
        """),
    }

    assert _iasr_id_choices(iasr) == {"ONLY_NEW"}


def test_iasr_id_choices_handles_empty_new_entrants(csv_str_to_df):
    iasr = {
        "existing_committed_anticipated_additional_generator_summary": csv_str_to_df("""
            IASR ID / DLT names
            ONLY_EXISTING
        """),
        "new_entrants_summary": pd.DataFrame(columns=["IASR ID / DLT names"]),
    }

    assert _iasr_id_choices(iasr) == {"ONLY_EXISTING"}


def test_iasr_id_choices_handles_both_empty():
    iasr = {
        "existing_committed_anticipated_additional_generator_summary": pd.DataFrame(
            columns=["IASR ID / DLT names"]
        ),
        "new_entrants_summary": pd.DataFrame(columns=["IASR ID / DLT names"]),
    }

    assert _iasr_id_choices(iasr) == set()


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


# --- _rename_first_token ---


@pytest.mark.parametrize(
    "name, delimiter, mapping, expected",
    [
        ("DN1_SAT_Dubbo", "_", {"DN1": "DREZ"}, "DREZ_SAT_Dubbo"),
        ("DN3 Marulan Battery - 2h", " ", {"DN3": "DREZ"}, "DREZ Marulan Battery - 2h"),
        ("BW01", "_", {"DN1": "DREZ"}, "BW01"),  # no delimiter -> unchanged
        ("KSP1", "_", {}, "KSP1"),  # empty map
        ("OTHER_SAT_X", "_", {"DN1": "DREZ"}, "OTHER_SAT_X"),  # token not in map
    ],
)
def test_rename_first_token(name, delimiter, mapping, expected):
    assert _rename_first_token(name, delimiter, mapping) == expected


# --- _strip_area_suffix ---


@pytest.mark.parametrize(
    "name, expected",
    [
        ("PV CNSW Area1", "PV CNSW"),
        ("CNSW SAT - Distributed Resources Area1", "CNSW SAT - Distributed Resources"),
        ("CNSW V2G Area12", "CNSW V2G"),  # multi-digit Area
        ("BW01", "BW01"),  # no Area suffix
        ("Areaway", "Areaway"),  # 'Area' inside other word -> untouched
    ],
)
def test_strip_area_suffix(name, expected):
    assert _strip_area_suffix(name) == expected


# --- _rename_generator_name / _rename_battery_name ---


@pytest.mark.parametrize(
    "name, expected",
    [
        ("DN1_SAT_Dubbo", "DREZ_SAT_Dubbo"),  # prefix rename
        ("DN3_WH_Marulan", "DREZ_WH_Marulan"),
        (
            "CNSW SAT - Distributed Resources Area1",
            "CNSW SAT - Distributed Resources",
        ),  # Area strip
        ("BW01", "BW01"),  # neither rule applies
    ],
)
def test_rename_generator_name(name, expected):
    assert _rename_generator_name(name) == expected


@pytest.mark.parametrize(
    "name, expected",
    [
        # DN1/DN3 -> DREZ: one-to-one naming rename retained
        ("DN1 Dubbo Battery - 4h", "DREZ Dubbo Battery - 4h"),
        ("DN3 Marulan Battery - 2h", "DREZ Marulan Battery - 2h"),
        # Constraint-scoped prefixes: no rename (drop through, re-injected by pass 2)
        ("SWQLD1 Battery - 2h", "SWQLD1 Battery - 2h"),
        ("MN1 Battery - 8h", "MN1 Battery - 8h"),
        # Area suffix is always stripped
        (
            "CNSW Battery - Distributed Resources Area1",
            "CNSW Battery - Distributed Resources",
        ),
        # Plant name passes through unchanged
        ("Tarong BESS", "Tarong BESS"),
    ],
)
def test_rename_battery_name(name, expected):
    assert _rename_battery_name(name) == expected


# --- _match_unit_name ---


def test_match_unit_name_exact():
    iasr_ids = {"BW01", "BW02"}
    lower = {n.lower(): n for n in iasr_ids}
    assert _match_unit_name("BW01", iasr_ids, lower) == "BW01"


def test_match_unit_name_case_insensitive():
    iasr_ids = {"Q2_SAT_North Qld Clean Energy Hub"}
    lower = {n.lower(): n for n in iasr_ids}
    assert (
        _match_unit_name("Q2_SAT_North QLD Clean Energy Hub", iasr_ids, lower)
        == "Q2_SAT_North Qld Clean Energy Hub"
    )


def test_match_unit_name_returns_none_on_miss():
    iasr_ids = {"BW01"}
    lower = {n.lower(): n for n in iasr_ids}
    assert _match_unit_name("UNKNOWN", iasr_ids, lower) is None


# --- _line_variable_name ---


def test_line_variable_name_known():
    assert _line_variable_name("NSW1-QLD1") == "NSW-QLD"
    assert _line_variable_name("SQ-CQ") == "SQ-CQ"


def test_line_variable_name_raises_for_unknown():
    with pytest.raises(ValueError, match="_LINE_TO_PATH_ID"):
        _line_variable_name("MADE-UP-LINE")


# --- _resolve_variable_name (per-class dispatch) ---


def test_resolve_variable_name_generator_matches():
    iasr_ids = {"BW01"}
    lower = {n.lower(): n for n in iasr_ids}
    assert _resolve_variable_name("Generator", "BW01", iasr_ids, lower) == "BW01"


def test_resolve_variable_name_generator_no_match_returns_none():
    iasr_ids = {"BW01"}
    lower = {n.lower(): n for n in iasr_ids}
    assert _resolve_variable_name("Generator", "UNKNOWN", iasr_ids, lower) is None


def test_resolve_variable_name_battery_applies_rename():
    iasr_ids = {"DREZ Dubbo Battery - 4h"}
    lower = {n.lower(): n for n in iasr_ids}
    assert (
        _resolve_variable_name("Battery", "DN1 Dubbo Battery - 4h", iasr_ids, lower)
        == "DREZ Dubbo Battery - 4h"
    )


def test_resolve_variable_name_battery_constraint_scoped_returns_none():
    """Constraint-scoped battery names (no rename rule) don't match IASR."""
    iasr_ids = {"SQ Battery - 2h"}
    lower = {n.lower(): n for n in iasr_ids}
    assert (
        _resolve_variable_name("Battery", "SWQLD1 Battery - 2h", iasr_ids, lower)
        is None
    )


def test_resolve_variable_name_line_uses_path_table():
    assert _resolve_variable_name("Line", "NSW1-QLD1", set(), {}) == "NSW-QLD"


def test_resolve_variable_name_node_passes_through():
    assert _resolve_variable_name("Node", "CNSW", set(), {}) == "CNSW"


def test_resolve_variable_name_unknown_class_raises():
    with pytest.raises(ValueError, match="Unexpected LHS parent_class"):
        _resolve_variable_name("Storage", "X", set(), {})


# --- _add_variable_name_column (orchestration) + rename logging ---


def test_add_variable_name_column_logs_renames(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        parent_class,  parent_name
        Generator,     DN1_SAT_Dubbo
        Generator,     BW01
    """)
    iasr_ids = {"DREZ_SAT_Dubbo", "BW01"}

    with caplog.at_level("INFO"):
        result = _add_variable_name_column(lhs, iasr_ids)

    expected = csv_str_to_df("""
        parent_class,  parent_name,    variable_name
        Generator,     DN1_SAT_Dubbo,  DREZ_SAT_Dubbo
        Generator,     BW01,           BW01
    """)
    pd.testing.assert_frame_equal(result, expected)
    assert "Applied 1 PLEXOS->ISPyPSA LHS name renames" in caplog.text
    assert "DN1_SAT_Dubbo -> DREZ_SAT_Dubbo" in caplog.text


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
    iasr_ids = {"BW01", "Q8_SAT_Brisbane", "Q8 Battery - 2h"}
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,         Technology Type
        Q8_SAT_Brisbane,          SQ,          Q8,              Solar
        Q8 Battery - 2h,       SQ,          Q8,              Battery Storage (2hrs storage)
    """)

    result = _build_custom_constraints_lhs(lhs_terms, iasr_ids, new_entrants)

    # Purchaser dropped, Installed Capacity dropped, Battery Load Coefficient
    # dropped, SWQLD1 constraint-scoped Battery dropped (no IASR match),
    # UNKNOWN dropped (no IASR match). BW01 + Q8_SAT_Brisbane survive pass 1;
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


# --- _is_battery_row ---


def test_is_battery_row(csv_str_to_df):
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Technology Type
        Q1 Battery - 2h,       Battery Storage (2hrs storage)
        NQ Battery - Dist,     Distributed Resources Batteries
        Q1 Wind,                 Wind
        N1 Pumped Hydro - 24h,Pumped Hydro (24hrs storage)
        Q1 Solar Thermal,       Solar Thermal (16hrs storage)
    """)

    result = _is_battery_row(new_entrants)

    # Battery + Distributed Resources Batteries match; others (incl. pumped
    # hydro and solar thermal storage) do not.
    assert list(result) == [True, True, False, False, False]


# --- _pick_location ---


@pytest.mark.parametrize(
    "rez_id, sub_region, expected",
    [
        ("Q8", "SQ", "Q8"),  # REZ ID populated -> REZ ID
        ("Not Applicable", "SQ", "SQ"),  # 'Not Applicable' -> Sub-region
        (None, "SQ", "SQ"),  # NaN/None -> Sub-region
    ],
)
def test_pick_location(rez_id, sub_region, expected):
    row = pd.Series({"REZ ID": rez_id, "Sub-region": sub_region})
    assert _pick_location(row) == expected


# --- _generator_to_location ---


def test_generator_to_location_uses_rez_id_when_populated(csv_str_to_df):
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,         Technology Type
        Q8 Solar,                SQ,          Q8,              Solar
        CSA Coal,                CSA,         Not Applicable, Black Coal
        Q1 Battery - 2h,       NQ,          Q1,              Battery Storage (2hrs storage)
    """)

    result = _generator_to_location(new_entrants)

    # Battery row excluded; REZ ID used when populated, Sub-region as fallback.
    assert result == {"Q8 Solar": "Q8", "CSA Coal": "CSA"}


def test_generator_to_location_empty_input(csv_str_to_df):
    new_entrants = pd.DataFrame(
        columns=["IASR ID / DLT names", "Sub-region", "REZ ID", "Technology Type"]
    )

    result = _generator_to_location(new_entrants)

    assert result == {}


# --- _batteries_by_location ---


def test_batteries_by_location_groups_by_rez_or_sub_region(csv_str_to_df):
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,    Sub-region,  REZ ID,         Technology Type
        Q8 Battery - 2h,         SQ,          Q8,              Battery Storage (2hrs storage)
        Q8 Battery - 4h,         SQ,          Q8,              Battery Storage (4hrs storage)
        CSA Battery - 2h,        CSA,         Not Applicable, Battery Storage (2hrs storage)
        Q8 Solar,                  SQ,          Q8,              Solar
    """)

    result = _batteries_by_location(new_entrants)

    # Battery rows grouped by REZ ID (when populated) / Sub-region (when not);
    # non-battery rows excluded; per-key list is sorted.
    assert result == {
        "Q8": ["Q8 Battery - 2h", "Q8 Battery - 4h"],
        "CSA": ["CSA Battery - 2h"],
    }


def test_batteries_by_location_empty_input():
    new_entrants = pd.DataFrame(
        columns=["IASR ID / DLT names", "Sub-region", "REZ ID", "Technology Type"]
    )

    result = _batteries_by_location(new_entrants)

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
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,  Technology Type
        Q8 Solar,             SQ,          Q8,      Solar
        Q8 Battery - 2h,      SQ,          Q8,      Battery Storage (2hrs storage)
        Q8 Battery - 4h,      SQ,          Q8,      Battery Storage (4hrs storage)
    """)

    result = _inject_iasr_new_entrant_batteries(lhs, new_entrants)

    # Pass-1 rows preserved; pass-2 appends Q8 Battery - 2h and 4h for SWQLD1.
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
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,  Technology Type
        Q8 Battery - 2h,       SQ,          Q8,       Battery Storage (2hrs storage)
    """)

    result = _inject_iasr_new_entrant_batteries(lhs, new_entrants)

    # No surviving generator term -> no triggers -> no injection.
    pd.testing.assert_frame_equal(result, lhs)


def test_inject_iasr_new_entrant_batteries_logs_summary(csv_str_to_df, caplog):
    lhs = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  Q8 Solar,      0.4,
    """)
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,  Technology Type
        Q8 Solar,                SQ,          Q8,       Solar
        Q8 Battery - 2h,       SQ,          Q8,       Battery Storage (2hrs storage)
    """)

    with caplog.at_level("INFO"):
        _inject_iasr_new_entrant_batteries(lhs, new_entrants)

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
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,          Technology Type
        Q8 Battery - 2h,      SQ,          Q8,              Battery Storage (2hrs storage)
        CSA Battery - 2h,     CSA,         Not Applicable,  Battery Storage (2hrs storage)
        Q8 Solar,             SQ,          Q8,              Solar
    """)

    result = _battery_to_location(new_entrants)

    # Non-battery rows excluded; REZ ID used when populated, Sub-region as fallback.
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
        WNV1,           storage_output,    V7 Battery - 2h,  0.78,
        WNV1,           storage_output,    V7 Battery - 2h,  0.0,          2031-11-30T00:00:00
    """)
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,  Technology Type
        Q8 Battery - 2h,      SQ,          Q8,      Battery Storage (2hrs storage)
        Q8 Battery - 8h,      SQ,          Q8,      Battery Storage (8hrs storage)
        V7 Battery - 2h,      SEV,         V7,      Battery Storage (2hrs storage)
    """)

    result = _surviving_battery_coefficients(lhs, new_entrants)

    # Q8's two surviving siblings collapse to one 0.43 profile row; the
    # generator term and the existing Tarong BESS (not a new-entrant battery)
    # are ignored; V7 keeps both time-varying rows.
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
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,  Technology Type
        Q8 Battery - 2h,      SQ,          Q8,      Battery Storage (2hrs storage)
        Q8 Battery - 8h,      SQ,          Q8,      Battery Storage (8hrs storage)
    """)

    with pytest.raises(ValueError, match="share one"):
        _surviving_battery_coefficients(lhs, new_entrants)


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
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,  Technology Type
        V7 Battery - 2h,      SEV,         V7,      Battery Storage (2hrs storage)
        V7 Battery - 8h,      SEV,         V7,      Battery Storage (8hrs storage)
    """)

    with pytest.raises(ValueError, match="share one"):
        _surviving_battery_coefficients(lhs, new_entrants)


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
    iasr_ids = {"Q8_SAT_Brisbane", "Q8 Battery - 2h"}
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,  Technology Type
        Q8_SAT_Brisbane,      SQ,          Q8,      Solar
        Q8 Battery - 2h,      SQ,          Q8,      Battery Storage (2hrs storage)
        Q8 Battery - 4h,      SQ,          Q8,      Battery Storage (4hrs storage)
    """)

    result = _build_custom_constraints_lhs(lhs_terms, iasr_ids, new_entrants)

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
    iasr_ids = {"Q8 Battery - 2h"}
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region,  REZ ID,  Technology Type
        Q8 Battery - 2h,      SQ,          Q8,      Battery Storage (2hrs storage)
        Q8 Battery - 4h,      SQ,          Q8,      Battery Storage (4hrs storage)
    """)

    result = _build_custom_constraints_lhs(lhs_terms, iasr_ids, new_entrants)

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
