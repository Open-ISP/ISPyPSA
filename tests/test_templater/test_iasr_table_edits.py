import re
from pathlib import Path

import pandas as pd
import pytest

from ispypsa.data_fetch import read_csvs
from ispypsa.templater.iasr_table_edits import (
    _apply_edits,
    _apply_edits_to_iasr_tables,
)

# --- _apply_edits_to_iasr_tables ---


def test_every_7_5_edit_matches_the_frozen_7_5_cache(caplog):
    iasr_tables = read_csvs(Path("tests", "test_workbook_table_cache", "7.5"))

    with caplog.at_level("WARNING"):
        _apply_edits_to_iasr_tables(iasr_tables, "7.5")

    # An edit that matches nothing logs a warning - signalling a table or an edit
    # detail has changed unexpectedly
    assert caplog.messages == []


# --- _apply_edits ---


def test_replace_edit_changes_matching_cells_in_every_row(csv_str_to_df, caplog):
    iasr_tables = {
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,     Regional build cost zone
            NSA Pumped Hydro - 10h,  CSA
            NSA Pumped Hydro - 24h,  CSA
            SQ Pumped Hydro - 24h,   SQ
        """)
    }
    edits = [
        {
            "table": "new_entrants_summary",
            "column": "Regional build cost zone",
            # no "where" field
            "replace": {"CSA": "NSA"},
            "reason": "Build cost zone typo'd as CSA.",
        }
    ]

    with caplog.at_level("INFO"):
        result = _apply_edits(iasr_tables, edits)

    expected = csv_str_to_df("""
        IASR ID / DLT names,     Regional build cost zone
        NSA Pumped Hydro - 10h,  NSA
        NSA Pumped Hydro - 24h,  NSA
        SQ Pumped Hydro - 24h,   SQ
    """)
    pd.testing.assert_frame_equal(result["new_entrants_summary"], expected)

    # The SQ row isn't changed, so it isn't counted.
    assert (
        "IASR TABLE EDIT: 'new_entrants_summary' column 'Regional build cost zone': "
        "{'CSA': 'NSA'} (cells changed: 2). Build cost zone typo'd as CSA."
    ) in caplog.text


def test_replace_edit_with_where_changes_only_named_rows(csv_str_to_df, caplog):
    iasr_tables = {
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,     Regional build cost zone
            NSA Pumped Hydro - 10h,  CSA
            CSA Pumped Hydro - 10h,  CSA
        """)
    }
    edits = [
        {
            "table": "new_entrants_summary",
            "column": "Regional build cost zone",
            "where": {"IASR ID / DLT names": ["NSA Pumped Hydro - 10h"]},
            "replace": {"CSA": "NSA"},
            "reason": "Build cost zone typo'd as CSA.",
        }
    ]

    with caplog.at_level("INFO"):
        result = _apply_edits(iasr_tables, edits)

    expected = csv_str_to_df("""
        IASR ID / DLT names,     Regional build cost zone
        NSA Pumped Hydro - 10h,  NSA
        CSA Pumped Hydro - 10h,  CSA
    """)
    pd.testing.assert_frame_equal(result["new_entrants_summary"], expected)

    # The CSA Pumped Hydro row isn't named in where, so its CSA isn't counted.
    assert (
        "IASR TABLE EDIT: 'new_entrants_summary' column 'Regional build cost zone': "
        "{'CSA': 'NSA'} (cells changed: 1). Build cost zone typo'd as CSA."
    ) in caplog.text


def test_replace_edit_with_two_where_columns_changes_rows_matching_both(
    csv_str_to_df,
):
    iasr_tables = {
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,  Technology Type,  REZ ID,  Sub-region
            N13 Wind,             Wind,             N13,     CNSW
            N13 Solar,            Solar,            N13,     CNSW
            N3 Wind,              Wind,             N3,      CNSW
        """)
    }
    edits = [
        {
            "table": "new_entrants_summary",
            "column": "Sub-region",
            "where": {
                "REZ ID": ["N13"],
                "Technology Type": ["Wind"],
            },
            "replace": {"CNSW": "SNSW"},
            "reason": "Mis-labelled sub-region.",
        }
    ]

    result = _apply_edits(iasr_tables, edits)

    # Only N13 Wind matches both where columns: N13 Solar's technology isn't listed,
    # and N3 Wind's REZ isn't.
    expected = csv_str_to_df("""
        IASR ID / DLT names,  Technology Type,  REZ ID,  Sub-region
        N13 Wind,             Wind,             N13,     SNSW
        N13 Solar,            Solar,            N13,     CNSW
        N3 Wind,              Wind,             N3,      CNSW
    """)
    pd.testing.assert_frame_equal(result["new_entrants_summary"], expected)


def test_later_edits_build_on_earlier_edits_to_the_same_table(csv_str_to_df):
    iasr_tables = {
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,  Sub-regon
            N13 Wind,             CNSW
        """)
    }
    edits = [
        {
            "table": "new_entrants_summary",
            "rename_columns": {"Sub-regon": "Sub-region"},
            "reason": "Typo in column header.",
        },
        {
            "table": "new_entrants_summary",
            "column": "Sub-region",
            "replace": {"CNSW": "SNSW"},
            "reason": "Mis-labelled sub-region.",
        },
    ]

    result = _apply_edits(iasr_tables, edits)

    # The replace edit uses the header the rename edit has just fixed.
    expected = csv_str_to_df("""
        IASR ID / DLT names,  Sub-region
        N13 Wind,             SNSW
    """)
    pd.testing.assert_frame_equal(result["new_entrants_summary"], expected)


def test_rename_columns_edit_skips_headers_not_in_the_table(csv_str_to_df, caplog):
    iasr_tables = {
        "flow_path_transfer_capability": csv_str_to_df("""
            Flow Paths,  Forward_Winter refernce,  Reverse_Winter reference
            CQ-NQ,       1400,                     1200
        """)
    }
    edits = [
        {
            "table": "flow_path_transfer_capability",
            "rename_columns": {
                "Forward_Winter refernce": "Forward_Winter reference",
                "Reverse_Winter refernce": "Reverse_Winter reference",  # typo not in table
            },
            "reason": "Typo in column headers ('refernce').",
        }
    ]

    with caplog.at_level("INFO"):
        result = _apply_edits(iasr_tables, edits)

    expected = csv_str_to_df("""
        Flow Paths,  Forward_Winter reference,  Reverse_Winter reference
        CQ-NQ,       1400,                      1200
    """)
    pd.testing.assert_frame_equal(result["flow_path_transfer_capability"], expected)

    assert (
        "IASR TABLE EDIT: 'flow_path_transfer_capability' columns renamed: "
        "{'Forward_Winter refernce': 'Forward_Winter reference'} "
        "(headers found: 1 of 2). Typo in column headers ('refernce')."
    ) in caplog.text


def test_replace_edit_that_matches_nothing_warns(csv_str_to_df, caplog):
    iasr_tables = {
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,     Regional build cost zone
            NSA Pumped Hydro - 10h,  NSA
        """)
    }
    edits = [
        {
            "table": "new_entrants_summary",
            "column": "Regional build cost zone",
            "replace": {"CSA": "NSA"},
            "reason": "Build cost zone typo'd as CSA.",
        }
    ]

    with caplog.at_level("WARNING"):
        _apply_edits(iasr_tables, edits)

    assert (
        "IASR TABLE EDIT (not applied): 'new_entrants_summary' column "
        "'Regional build cost zone': {'CSA': 'NSA'} matched nothing."
    ) in caplog.text


def test_rename_columns_edit_that_matches_nothing_warns(csv_str_to_df, caplog):
    iasr_tables = {
        "flow_path_transfer_capability": csv_str_to_df("""
            Flow Paths,  Forward_Winter reference
            CQ-NQ,       1400
        """)
    }
    edits = [
        {
            "table": "flow_path_transfer_capability",
            "rename_columns": {
                "Forward_Winter refernce": "Forward_Winter reference"
            },  # typo not in table
            "reason": "Typo in column headers ('refernce').",
        }
    ]

    with caplog.at_level("WARNING"):
        _apply_edits(iasr_tables, edits)

    assert (
        "IASR TABLE EDIT (not applied): 'flow_path_transfer_capability' column "
        "renames {'Forward_Winter refernce': 'Forward_Winter reference'} "
        "matched nothing."
    ) in caplog.text


def test_edits_for_missing_tables_raise(csv_str_to_df):
    iasr_tables = {
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,  Sub-region
            N13 Wind,             CNSW
        """)
    }
    edits = [
        {
            "table": "technology_specific_lcf",
            "column": "BOTN - Cethana",
            "replace": {"100": 1.0},
            "reason": "Typed in as a percentage.",
        },
        {
            "table": "new_entrants_summary",
            "column": "Sub-region",
            "replace": {"CNSW": "SNSW"},
            "reason": "Mis-labelled sub-region.",
        },
        {
            "table": "flow_path_transfer_capabilty",
            "rename_columns": {"Forward_Winter refernce": "Forward_Winter reference"},
            "reason": "Typo in column headers ('refernce').",
        },
    ]

    with pytest.raises(
        ValueError,
        match=re.escape(
            "IASR table edits defined for missing tables: "
            "['flow_path_transfer_capabilty', 'technology_specific_lcf']."
        ),
    ):
        _apply_edits(iasr_tables, edits)


def test_replace_edit_on_missing_columns_raises(csv_str_to_df):
    iasr_tables = {
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,  Sub-region
            N13 Wind,             CNSW
        """)
    }
    edits = [
        {
            "table": "new_entrants_summary",
            "column": "Sub region",
            "where": {"REZ ID": ["N13"]},
            # column and col named in ``where`` (REZ ID) both named in the raise
            "replace": {"CNSW": "SNSW"},
            "reason": "Mis-labelled sub-region.",
        }
    ]

    with pytest.raises(
        ValueError,
        match=re.escape(
            "IASR table edit on 'new_entrants_summary' defined for missing columns: "
            "['REZ ID', 'Sub region']."
        ),
    ):
        _apply_edits(iasr_tables, edits)


@pytest.mark.parametrize(
    "edit, expected_error",
    [
        pytest.param(
            {
                "table": "new_entrants_summary",
                "column": "Sub-region",
                "replce": {"CNSW": "SNSW"},
                "reason": "Mis-labelled sub-region.",
            },
            "Invalid IASR table edit 2: no 'rename_columns' or 'replace' key "
            "(exactly one required); unexpected keys ['replce'].",
            id="misspelt edit type",
        ),
        pytest.param(
            {
                "table": "new_entrants_summary",
                "column": "Sub-region",
                "replace": {"CNSW": "SNSW"},
                "rename_columns": {"Sub-region": "Subregion"},
                "reason": "Mis-labelled sub-region.",
            },
            "Invalid IASR table edit 2: both 'rename_columns' and 'replace' keys "
            "(exactly one required).",
            id="both edit types",
        ),
        pytest.param(
            {
                "table": "new_entrants_summary",
                "column": "Sub-region",
                "whre": {"REZ ID": ["N13"]},
                "replace": {"CNSW": "SNSW"},
            },
            "Invalid IASR table edit 2 (replace): missing keys ['reason']; "
            "unexpected keys ['whre'].",
            id="missing and misspelt keys",
        ),
        pytest.param(
            {
                "table": "new_entrants_summary",
                "where": {"REZ ID": ["N13"]},
                "rename_columns": {"Sub-region": "Subregion"},
                "reason": "Mis-labelled sub-region.",
            },
            "Invalid IASR table edit 2 (rename_columns): unexpected keys ['where'].",
            id="key the edit type doesn't use",
        ),
    ],
)
def test_invalid_edit_raises(csv_str_to_df, edit, expected_error):
    iasr_tables = {
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,  Sub-region
            N13 Wind,             CNSW
        """)
    }
    valid_edit = {
        "table": "new_entrants_summary",
        "column": "Sub-region",
        "replace": {"CNSW": "SNSW"},
        "reason": "Mis-labelled sub-region.",
    }

    # The invalid edit comes second, so its message calls it edit 2.
    with pytest.raises(ValueError, match=re.escape(expected_error)):
        _apply_edits(iasr_tables, [valid_edit, edit])
