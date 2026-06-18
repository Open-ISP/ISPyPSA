"""Unit tests for the pure helpers of ``scripts/extract_plexos_constraints.py``.

The query and database-level functions need a loaded PLEXOS database and are
not unit-tested here; they are covered instead by the script's own structural-
invariant checks, which run against the real model on every extraction.
"""

import pandas as pd
import pytest
from extract_plexos_constraints import (
    _assert_every_constraint_has_lhs,
    _assert_every_constraint_has_rhs,
    _assert_merge_is_one_to_one,
    _assert_no_duplicate_data_points,
    _assert_no_unresolved_references,
    _assert_one_sense_per_constraint,
    _assert_one_tag_per_rhs,
    _assert_parent_classes_are_known,
    _assert_properties_are_known,
    _assert_sense_values_are_valid,
    _assert_tags_only_on_rhs,
    _assert_values_are_numeric,
    _check_constraints_resolved,
    _merge_into_long_table,
    _placeholders,
    _split_into_tables,
    _validate_constraint_rows,
)


@pytest.fixture
def valid_rows(csv_str_to_df):
    """A minimal long table that satisfies every structural invariant."""
    return csv_str_to_df("""
        constraint_name, parent_class, parent_name, property,                          value, date_from, date_to, tags
        C1,              Generator,    GEN_A,       Generation__Sent__Out__Coefficient, 0.5,   ,          ,
        C1,              System,       NEM,         RHS,                                100,   ,          ,        Hot__Day
        C1,              System,       NEM,         Sense,                              -1,    ,          ,
    """)


# --- _placeholders ---


def test_placeholders():
    assert _placeholders(1) == "?"
    assert _placeholders(3) == "?,?,?"


# --- _merge_into_long_table ---


def test_merge_into_long_table(csv_str_to_df):
    constraints = csv_str_to_df("""
        constraint_object_id, constraint_name
        20,                   C1
    """)
    memberships = csv_str_to_df("""
        membership_id, constraint_object_id, parent_class, parent_name
        500,           20,                   Generator,    GEN_A
        501,           20,                   System,       NEM
    """)
    data_points = csv_str_to_df("""
        data_id, membership_id, property, value
        900,     500,           Coef,     0.5
        903,     500,           Coef,     0.6
        901,     501,           RHS,      100
    """)
    dates = csv_str_to_df("""
        data_id, date_from,           date_to
        903,     2030-01-01T00:00:00,
    """)
    tags = csv_str_to_df("""
        data_id, tags
        901,     Hot__Day
    """)

    result = _merge_into_long_table(constraints, memberships, data_points, dates, tags)

    # The undated coefficient (900) sorts before the dated one (903) because
    # the merge sorts date_from with na_position="first".
    expected = csv_str_to_df("""
        constraint_name, parent_class, parent_name, property, value, date_from,           date_to, tags
        C1,              Generator,    GEN_A,       Coef,     0.5,   ,                     ,
        C1,              Generator,    GEN_A,       Coef,     0.6,   2030-01-01T00:00:00,  ,
        C1,              System,       NEM,         RHS,      100,   ,                     ,        Hot__Day
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _split_into_tables ---


def test_split_into_tables(csv_str_to_df):
    rows = csv_str_to_df("""
        constraint_name, parent_class, parent_name, property,        value, date_from, date_to, tags
        C1,              Generator,    GEN_A,       Coef,           0.5,   ,          ,
        C1,              System,       NEM,         RHS,            100,   ,          ,        Hot__Day
        C1,              System,       NEM,         Sense,          -1,    ,          ,
        C1,              System,       NEM,         Penalty__Price, -1,    ,          ,
    """)

    constraints, lhs, rhs = _split_into_tables(rows)

    expected_constraints = csv_str_to_df("""
        constraint_name, property,        value, date_from, date_to, tags
        C1,              Sense,           -1,    ,          ,
        C1,              Penalty__Price,  -1,    ,          ,
    """)
    pd.testing.assert_frame_equal(constraints, expected_constraints, check_dtype=False)

    expected_lhs = csv_str_to_df("""
        constraint_name, parent_class, parent_name, property, value, date_from, date_to, tags
        C1,              Generator,    GEN_A,       Coef,     0.5,   ,          ,
    """)
    pd.testing.assert_frame_equal(lhs, expected_lhs, check_dtype=False)

    expected_rhs = csv_str_to_df("""
        constraint_name, value, date_from, date_to, tags
        C1,              100,   ,          ,        Hot__Day
    """)
    pd.testing.assert_frame_equal(rhs, expected_rhs, check_dtype=False)


# --- _check_constraints_resolved ---


def test_check_constraints_resolved_accepts_resolved_names():
    constraints = pd.DataFrame({"constraint_name": ["C1", "C2"]})
    _check_constraints_resolved(constraints, ["C1", "C2"])  # must not raise


def test_check_constraints_resolved_raises_on_missing_name():
    constraints = pd.DataFrame({"constraint_name": ["C1"]})
    with pytest.raises(ValueError, match="not found"):
        _check_constraints_resolved(constraints, ["C1", "C2"])


def test_check_constraints_resolved_raises_on_name_collision():
    constraints = pd.DataFrame({"constraint_name": ["C1", "C1"]})
    with pytest.raises(ValueError, match="matching more than one"):
        _check_constraints_resolved(constraints, ["C1"])


# --- _validate_constraint_rows (orchestrator) ---


def test_validate_constraint_rows_accepts_a_valid_table(valid_rows):
    _validate_constraint_rows(valid_rows)  # must not raise


def test_validate_constraint_rows_raises_on_a_broken_table(csv_str_to_df):
    # C1 has a Sense and an LHS term but no RHS row.
    rows = csv_str_to_df("""
        constraint_name, parent_class, parent_name, property,                          value, date_from, date_to, tags
        C1,              Generator,    GEN_A,       Generation__Sent__Out__Coefficient, 0.5,   ,          ,
        C1,              System,       NEM,         Sense,                              -1,    ,          ,
    """)
    with pytest.raises(ValueError, match="no RHS"):
        _validate_constraint_rows(rows)


# --- individual structural invariants: each must fire on a broken table ---


def test_assert_no_unresolved_references_raises_on_null(csv_str_to_df):
    # The second row's property did not resolve (a broken PLEXOS reference).
    rows = csv_str_to_df("""
        constraint_name, parent_class, parent_name, property
        C1,              System,       NEM,         Sense
        C1,              Generator,    GEN_A,
    """)
    with pytest.raises(ValueError, match="unresolved"):
        _assert_no_unresolved_references(rows)


def test_assert_one_sense_per_constraint_raises_on_two_senses(csv_str_to_df):
    rows = csv_str_to_df("""
        constraint_name, property
        C1,              Sense
        C1,              Sense
    """)
    with pytest.raises(ValueError, match="exactly one Sense"):
        _assert_one_sense_per_constraint(rows)


def test_assert_every_constraint_has_rhs_raises_when_missing(csv_str_to_df):
    rows = csv_str_to_df("""
        constraint_name, property
        C1,              Sense
    """)
    with pytest.raises(ValueError, match="no RHS"):
        _assert_every_constraint_has_rhs(rows)


def test_assert_every_constraint_has_lhs_raises_when_missing(csv_str_to_df):
    rows = csv_str_to_df("""
        constraint_name, parent_class
        C1,              System
    """)
    with pytest.raises(ValueError, match="no LHS"):
        _assert_every_constraint_has_lhs(rows)


def test_assert_properties_are_known_raises_on_unknown():
    rows = pd.DataFrame({"property": ["RHS", "Bogus Property"]})
    with pytest.raises(ValueError, match="Unrecognised PLEXOS properties"):
        _assert_properties_are_known(rows)


def test_assert_parent_classes_are_known_raises_on_unknown():
    rows = pd.DataFrame({"parent_class": ["Generator", "Bogus Class"]})
    with pytest.raises(ValueError, match="Unrecognised participant classes"):
        _assert_parent_classes_are_known(rows)


def test_assert_sense_values_are_valid_raises_on_bad_value():
    rows = pd.DataFrame({"property": ["Sense"], "value": [5]})
    with pytest.raises(ValueError, match="Unexpected Sense values"):
        _assert_sense_values_are_valid(rows)


def test_assert_values_are_numeric_raises_on_null():
    rows = pd.DataFrame({"property": ["RHS", "Sense"], "value": [100, None]})
    with pytest.raises(ValueError, match="null value"):
        _assert_values_are_numeric(rows)


def test_assert_values_are_numeric_raises_on_non_numeric():
    rows = pd.DataFrame({"property": ["RHS", "Sense"], "value": [100, "not a number"]})
    with pytest.raises(ValueError, match="Non-numeric"):
        _assert_values_are_numeric(rows)


def test_assert_tags_only_on_rhs_raises_when_tag_on_other_row(csv_str_to_df):
    rows = csv_str_to_df("""
        property, tags
        RHS,      Hot__Day
        Coef,     Winter
    """)
    with pytest.raises(ValueError, match="Tags found on non-RHS"):
        _assert_tags_only_on_rhs(rows)


def test_assert_one_tag_per_rhs_raises_when_untagged(csv_str_to_df):
    rows = csv_str_to_df("""
        property, tags
        RHS,
    """)
    with pytest.raises(ValueError, match="not scoped to exactly one timeslice"):
        _assert_one_tag_per_rhs(rows)


def test_assert_one_tag_per_rhs_raises_when_multiple_tags(csv_str_to_df):
    rows = csv_str_to_df("""
        property, tags
        RHS,      Hot__Day|Winter
    """)
    with pytest.raises(ValueError, match="not scoped to exactly one timeslice"):
        _assert_one_tag_per_rhs(rows)


def test_assert_no_duplicate_data_points_raises_on_duplicate(csv_str_to_df):
    rows = csv_str_to_df("""
        constraint_name, parent_class, parent_name, property, date_from, date_to, tags
        C1,              Generator,    GEN_A,       Coef,     ,          ,
        C1,              Generator,    GEN_A,       Coef,     ,          ,
    """)
    with pytest.raises(ValueError, match="Duplicate data points"):
        _assert_no_duplicate_data_points(rows)


# --- _assert_merge_is_one_to_one ---


def test_assert_merge_is_one_to_one_accepts_equal_lengths():
    data_points = pd.DataFrame({"data_id": [1, 2, 3]})
    rows = pd.DataFrame({"value": [1, 2, 3]})
    _assert_merge_is_one_to_one(data_points, rows)  # must not raise


def test_assert_merge_is_one_to_one_raises_on_row_count_mismatch():
    data_points = pd.DataFrame({"data_id": [1, 2, 3]})
    rows = pd.DataFrame({"value": [1, 2, 3, 4]})
    with pytest.raises(ValueError, match="Merge changed the row count"):
        _assert_merge_is_one_to_one(data_points, rows)
