# tests/test_translator/test_helpers.py
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from ispypsa.translator.helpers import (
    _add_investment_periods_as_build_years,
    _get_financial_year_int_from_string,
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
