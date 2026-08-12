# tests/test_translator/test_helpers.py
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from ispypsa.translator.helpers import (
    _add_investment_periods_as_build_years,
    _get_financial_year_int_from_string,
    _resolve_wildcards,
)


def test_get_financial_year_int_from_string():
    """Test financial year string translation to integer."""
    # Standard financial year format
    assert _get_financial_year_int_from_string("2023_24", "test", "fy") == 2024
    assert _get_financial_year_int_from_string("2023_24_extra", "test", "fy") == 2024
    assert _get_financial_year_int_from_string("2099_00", "test", "fy") == 2100

    # Error cases
    with pytest.raises(ValueError, match="Invalid financial year string"):
        _get_financial_year_int_from_string("invalid", "test", "fy")

    with pytest.raises(ValueError, match="Unknown year_type"):
        _get_financial_year_int_from_string("2023_24", "test", "unknown")

    with pytest.raises(NotImplementedError, match="Calendar years are not implemented"):
        _get_financial_year_int_from_string("2023", "test", "calendar")


def test_add_investment_periods_as_build_years(csv_str_to_df):
    """Test adding investment periods as build years to a DataFrame."""
    # Input DataFrame
    input_df_csv = """
    generator_name,  technology,  capacity_mw
    Gen1,            Solar,       100
    Gen2,            Wind,        200
    """
    input_df = csv_str_to_df(input_df_csv)

    # Investment periods
    investment_periods = [2020, 2025, 2030]

    # Call the function
    result = _add_investment_periods_as_build_years(input_df, investment_periods)

    # Expected result
    expected_csv = """
    generator_name,  technology,  capacity_mw,  build_year
    Gen1,            Solar,       100,          2020
    Gen1,            Solar,       100,          2025
    Gen1,            Solar,       100,          2030
    Gen2,            Wind,        200,          2020
    Gen2,            Wind,        200,          2025
    Gen2,            Wind,        200,          2030
    """
    expected = csv_str_to_df(expected_csv)

    # Sort both DataFrames for consistent comparison
    pd.testing.assert_frame_equal(
        result.sort_values(["generator_name", "build_year"]).reset_index(drop=True),
        expected.sort_values(["generator_name", "build_year"]).reset_index(drop=True),
    )


def test_add_investment_periods_as_build_years_empty(csv_str_to_df):
    """Test adding investment periods to an empty DataFrame."""
    # Empty input DataFrame
    input_df = pd.DataFrame(columns=["generator_name", "technology", "capacity_mw"])

    # Investment periods
    investment_periods = [2020, 2025, 2030]

    # Call the function
    result = _add_investment_periods_as_build_years(input_df, investment_periods)

    # Expected result - empty DataFrame with build_year column
    expected = pd.DataFrame(
        columns=["generator_name", "technology", "capacity_mw", "build_year"]
    )

    # Compare DataFrames
    assert result.empty
    assert "build_year" in result.columns
    assert list(result.columns) == list(expected.columns)


def test_resolve_wildcards_expands_a_blank_key_to_every_allowed_value(csv_str_to_df):
    """A blank key cell is a wildcard expanded to every allowed value; a column
    not in allowed_values (timeslice) rides along unchanged."""
    table = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    peak,       1200
        N1-CNSW,  ,           ,
    """)

    result = _resolve_wildcards(
        table,
        {"path_id": ["CQ-NQ", "N1-CNSW"], "direction": ["forward", "reverse"]},
        ["capacity"],
    )

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    peak,       1200
        N1-CNSW,  forward,    ,
        N1-CNSW,  reverse,    ,
    """)
    assert_frame_equal(
        result.sort_values(["path_id", "direction"]).reset_index(drop=True),
        expected.sort_values(["path_id", "direction"]).reset_index(drop=True),
        check_dtype=False,
    )


def test_resolve_wildcards_keeps_the_most_specific_row(csv_str_to_df):
    """Where an expanded wildcard row and a specific row land on the same key,
    the one that used fewer wildcards wins."""
    table = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1200
        ,         ,           ,           500
    """)

    result = _resolve_wildcards(
        table,
        {"path_id": ["CQ-NQ", "Q1-NQ"], "direction": ["forward", "reverse"]},
        ["capacity"],
    )

    # CQ-NQ/forward keeps its specific 1200; every other combination takes the
    # global default 500.
    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1200
        CQ-NQ,    reverse,    ,           500
        Q1-NQ,    forward,    ,           500
        Q1-NQ,    reverse,    ,           500
    """)
    assert_frame_equal(
        result.sort_values(["path_id", "direction"]).reset_index(drop=True),
        expected.sort_values(["path_id", "direction"]).reset_index(drop=True),
        check_dtype=False,
    )


def test_resolve_wildcards_raises_on_out_of_allowed_values(csv_str_to_df):
    """A filled value outside the allowed set is bad input data — callers filter
    designed selections out before resolving — so resolution halts the run. The
    validation layer will catch this class of input upstream once it lands (the
    allowed values mirror the schemas' allowed_values / allowed_values_from);
    the raise stays on as the resolver's backstop invariant."""
    table = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2026,  100
        CQ-NQ,         2025,  90
    """)

    with pytest.raises(
        ValueError,
        match=r"year contains values outside the allowed set: \[2025\]",
    ):
        _resolve_wildcards(
            table, {"expansion_id": ["CQ-NQ"], "year": [2026, 2028]}, ["cost"]
        )


def test_resolve_wildcards_empty(csv_str_to_df):
    """An empty table resolves to an empty table with the same columns."""
    table = pd.DataFrame(columns=["path_id", "direction", "timeslice", "capacity"])

    result = _resolve_wildcards(
        table,
        {"path_id": ["CQ-NQ"], "direction": ["forward", "reverse"]},
        ["capacity"],
    )

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
    """)
    assert_frame_equal(result, expected, check_dtype=False)
