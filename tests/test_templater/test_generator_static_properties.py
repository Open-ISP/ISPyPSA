from pathlib import Path

import pandas as pd
import pytest

from ispypsa.data_fetch import read_csvs
from ispypsa.templater.lists import _MINIMUM_REQUIRED_GENERATOR_COLUMNS
from ispypsa.templater.mappings import (
    _ECAA_GENERATOR_NEW_COLUMN_MAPPING,
    _ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP,
    _NEW_ENTRANT_GENERATOR_NEW_COLUMN_MAPPING,
    _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP,
)
from ispypsa.templater.static_ecaa_generator_properties import (
    _add_closure_year_column,
    _template_ecaa_generators_static_properties,
)
from ispypsa.templater.static_new_generator_properties import (
    _template_new_generators_static_properties,
)


def test_static_ecaa_generator_templater(workbook_table_cache_test_path: Path):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    df = _template_ecaa_generators_static_properties(iasr_tables)
    for static_property_col in _ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP.keys():
        if static_property_col in _MINIMUM_REQUIRED_GENERATOR_COLUMNS:
            if (
                "new_col_name"
                in _ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP[static_property_col].keys()
            ):
                static_property_col = _ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP[
                    static_property_col
                ]["new_col_name"]

            if "date" not in static_property_col:
                assert all(
                    df[static_property_col].apply(
                        lambda x: True if not isinstance(x, str) else False
                    )
                )
    assert set(df["status"]) == set(
        ("Existing", "Committed", "Anticipated", "Additional projects")
    )

    # check that columns present are all required columns:
    for column in df.columns:
        assert column in _MINIMUM_REQUIRED_GENERATOR_COLUMNS

    # checks that all entries in "generator" col are strings
    assert all(df.generator.apply(lambda x: True if isinstance(x, str) else False))
    # checks that all entries in "generator" col are unique
    assert len(df.generator.unique()) == len(df.generator)

    where_solar, where_wind = (
        df["technology_type"].str.contains("solar", case=False),
        df["technology_type"].str.contains("wind", case=False),
    )
    for where_tech in (where_solar, where_wind):
        tech_df = df.loc[where_tech, :]
        assert all(tech_df["minimum_load_mw"] == 0.0)
        assert all(tech_df["heat_rate_gj/mwh"] == 0.0)


def test_static_new_generator_templater(workbook_table_cache_test_path: Path):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    df = _template_new_generators_static_properties(iasr_tables)
    for static_property_col in _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP.keys():
        if static_property_col in _MINIMUM_REQUIRED_GENERATOR_COLUMNS:
            # checks few updated column names first
            if (
                "new_col_name"
                in _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP[static_property_col].keys()
            ):
                static_property_col = _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP[
                    static_property_col
                ]["new_col_name"]
            # checks that no strings (mappings) remain in each mapped column
            assert all(
                df[static_property_col].apply(
                    lambda x: True if not isinstance(x, str) else False
                )
            )

    # checks that all entries in "status" col are "New Entrant" only
    assert set(df["status"]) == set(["New Entrant"])

    for column in df.columns:
        assert column in _MINIMUM_REQUIRED_GENERATOR_COLUMNS

    # checks that all entries in "generator" col are strings
    assert all(df.generator.apply(lambda x: True if isinstance(x, str) else False))
    # check that all entries in "generator" col are unique
    assert len(df.generator.unique()) == len(df.generator)

    # checks that values that should be always set to zero are zero:
    where_solar, where_wind, where_ocgt, where_h2 = (
        df["generator"].str.contains("solar", case=False),
        df["generator"].str.contains("wind", case=False),
        df["generator"].str.contains("ocgt", case=False),
        df["generator"].str.contains("hydrogen", case=False),
    )
    zero_tests = {
        "minimum_stable_level_%": (
            where_solar,
            where_wind,
            where_ocgt,
            where_h2,
        ),
        "vom_$/mwh_sent_out": (
            where_solar,
            where_wind,
            where_h2,
        ),
        "heat_rate_gj/mwh": (where_solar, where_wind),
    }
    for zero_col_name, technology_dfs in zero_tests.items():
        for where_tech in technology_dfs:
            tech_df = df.loc[where_tech, :]
            assert all(tech_df[zero_col_name] == 0.0)


def test_add_closure_year_column(csv_str_to_df):
    """Test the _add_closure_year_column function with various scenarios."""
    # Setup test data
    ecaa_generators_csv = """
    generator,                 technology_type,    region_id,    closure_year
    Bayswater_1,               Coal,               NSW,          Bayswater_1
    Liddell_1,                 Coal,               NSW,          Liddell_1
    Eraring_1,                 Coal,               NSW,          Eraring_1
    Newport_Gas,               CCGT,               VIC,          Newport_Gas
    New_Generator_No_Closure,  Wind,               QLD,          New_Generator_No_Closure
    """
    ecaa_generators = csv_str_to_df(ecaa_generators_csv)

    closure_years_csv = """
    generator,                 expected_closure_year_calendar_year, duid
    Bayswater_1,               2035,                                BA01
    Bayswater_1,               2036,                                BA02
    Liddell_1,                 2023,                                LD01
    Eraring_1,                 2025,                                ER01
    Newport_Gas_,              2040,                                NP01
    """
    closure_years = csv_str_to_df(closure_years_csv)

    # Execute function
    result = _add_closure_year_column(ecaa_generators, closure_years)

    # Expected result
    expected_csv = """
    generator,                 technology_type,    region_id,    closure_year
    Bayswater_1,               Coal,               NSW,          2035
    Liddell_1,                 Coal,               NSW,          2023
    Eraring_1,                 Coal,               NSW,          2025
    Newport_Gas,               CCGT,               VIC,          2040
    New_Generator_No_Closure,  Wind,               QLD,          -1
    """
    expected = csv_str_to_df(expected_csv)
    expected = expected.fillna(pd.NA)

    pd.testing.assert_frame_equal(
        result.sort_values("generator").reset_index(drop=True),
        expected.sort_values("generator").reset_index(drop=True),
        check_dtype=False,
    )


def test_add_closure_year_column_empty_ecaa_df(csv_str_to_df):
    """Test edge cases for the _add_closure_year_column function."""

    # Setup test data
    ecaa_generators_csv = """
    generator,                 technology_type,    region_id,    closure_year
    Gen_A,                     Coal,               NSW,          Gen_A
    Gen_B,                     Coal,               NSW,          Gen_B
    """
    ecaa_generators = csv_str_to_df(ecaa_generators_csv)

    # Case 1a: Empty closure years dataframe
    empty_closure = pd.DataFrame(
        columns=["generator", "duid", "expected_closure_year_calendar_year"]
    )

    # Execute function
    result = _add_closure_year_column(ecaa_generators, empty_closure)

    # Expected result
    expected_csv = """
    generator,                 technology_type,    region_id,    closure_year
    Gen_A,                     Coal,               NSW,          -1
    Gen_B,                     Coal,               NSW,          -1
    """
    expected = csv_str_to_df(expected_csv)
    expected = expected.fillna(pd.NA)

    pd.testing.assert_frame_equal(
        result.sort_values("generator").reset_index(drop=True),
        expected.sort_values("generator").reset_index(drop=True),
        check_dtype=False,
    )
