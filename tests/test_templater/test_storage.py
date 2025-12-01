import logging
from pathlib import Path

import pandas as pd
import pytest

from ispypsa.data_fetch import read_csvs
from ispypsa.templater.lists import _MINIMUM_REQUIRED_BATTERY_COLUMNS
from ispypsa.templater.mappings import (
    _ECAA_STORAGE_STATIC_PROPERTY_TABLE_MAP,
    _NEW_ENTRANT_STORAGE_STATIC_PROPERTY_TABLE_MAP,
)
from ispypsa.templater.storage import (
    _add_and_clean_rez_ids,
    _add_closure_year_column,
    _add_isp_resource_type_column,
    _calculate_and_merge_tech_specific_lcfs,
    _calculate_storage_duration_hours,
    _process_and_merge_connection_cost,
    _process_and_merge_opex,
    _restructure_battery_property_table,
    _template_battery_properties,
)


def test_battery_templater(workbook_table_cache_test_path: Path):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    ecaa_batteries, new_entrant_batteries = _template_battery_properties(iasr_tables)

    for ecaa_property_col in _ECAA_STORAGE_STATIC_PROPERTY_TABLE_MAP.keys():
        if (
            "new_col_name"
            in _ECAA_STORAGE_STATIC_PROPERTY_TABLE_MAP[ecaa_property_col].keys()
        ):
            ecaa_property_col = _ECAA_STORAGE_STATIC_PROPERTY_TABLE_MAP[
                ecaa_property_col
            ]["new_col_name"]

        if (
            ecaa_property_col in _MINIMUM_REQUIRED_BATTERY_COLUMNS
            and "date" not in ecaa_property_col
        ):
            assert all(
                ecaa_batteries[ecaa_property_col].apply(
                    lambda x: True if not isinstance(x, str) else False
                )
            )

    for (
        new_entrant_property_col
    ) in _NEW_ENTRANT_STORAGE_STATIC_PROPERTY_TABLE_MAP.keys():
        if (
            "new_col_name"
            in _NEW_ENTRANT_STORAGE_STATIC_PROPERTY_TABLE_MAP[
                new_entrant_property_col
            ].keys()
        ):
            new_entrant_property_col = _NEW_ENTRANT_STORAGE_STATIC_PROPERTY_TABLE_MAP[
                new_entrant_property_col
            ]["new_col_name"]

        if (
            new_entrant_property_col in _MINIMUM_REQUIRED_BATTERY_COLUMNS
            and "date" not in new_entrant_property_col
        ):
            assert all(
                new_entrant_batteries[new_entrant_property_col].apply(
                    lambda x: True if not isinstance(x, str) else False
                )
            )

    # limited test CSV contains only "Existing" ECAA battery
    assert all(ecaa_batteries["status"] == "Existing")
    assert all(new_entrant_batteries["status"] == "New Entrant")

    all_columns = (
        ecaa_batteries.columns.tolist() + new_entrant_batteries.columns.tolist()
    )
    for column in all_columns:
        assert column in _MINIMUM_REQUIRED_BATTERY_COLUMNS


def test_merge_and_set_battery_static_properties_string_handling(
    workbook_table_cache_test_path: Path,
):
    """Test that string values in numeric columns are properly handled when merging static properties."""
    iasr_tables = read_csvs(workbook_table_cache_test_path)

    # Get the original data
    ecaa_batteries, new_entrant_batteries = _template_battery_properties(iasr_tables)

    # Check that string values were properly handled in static property columns
    for df in [ecaa_batteries, new_entrant_batteries]:
        numeric_cols = df.select_dtypes(include=["number"]).columns
        for col in numeric_cols:
            if col in df.columns and "date" not in col:
                # Verify no string values remain in numeric columns
                assert not df[col].apply(lambda x: isinstance(x, str)).any(), (
                    f"Column {col} contains string values"
                )


def test_add_closure_year_column(csv_str_to_df):
    """Test the _add_closure_year_column function with various scenarios."""
    # Setup test data
    ecaa_batteries_csv = """
    storage_name,              technology_type,    region_id,    closure_year
    Existing__Battery,         Battery__Storage,   NSW,          Existing__Battery
    New_Battery_No_Closure,    Battery__Storage,   QLD,          New_Battery_No_Closure
    """
    ecaa_batteries = csv_str_to_df(ecaa_batteries_csv)

    closure_years_csv = """
    Generator__name,        DUID,       Expected__Closure__Year__(Calendar__year)
    Existing__Battery,      EB01,       2035
    Existing__Battery,      EB02,       2036
    """
    closure_years = csv_str_to_df(closure_years_csv)

    # Execute function
    result = _add_closure_year_column(ecaa_batteries, closure_years)

    # Expected result
    expected_csv = """
    storage_name,              technology_type,    region_id,    closure_year
    Existing__Battery,         Battery__Storage,   NSW,          2035
    New_Battery_No_Closure,    Battery__Storage,   QLD,          -1
    """
    expected = csv_str_to_df(expected_csv)
    expected = expected.fillna(pd.NA)

    pd.testing.assert_frame_equal(
        result.sort_values("storage_name").reset_index(drop=True),
        expected.sort_values("storage_name").reset_index(drop=True),
        check_dtype=False,
    )


def test_add_closure_year_column_empty_ecaa_df(csv_str_to_df):
    """Test edge cases for the _add_closure_year_column function."""

    # Setup test data
    ecaa_batteries_csv = """
    storage_name,              technology_type,    region_id,    closure_year
    Battery_A,                 Battery__Storage,   NSW,          Battery_A
    Battery_B,                 Battery__Storage,   SA,           Battery_B
    """
    ecaa_batteries = csv_str_to_df(ecaa_batteries_csv)

    # Case 1a: Empty closure years dataframe
    empty_closure = pd.DataFrame(
        columns=["Generator name", "DUID", "Expected Closure Year (Calendar year)"]
    )

    # Execute function
    result = _add_closure_year_column(ecaa_batteries, empty_closure)

    # Expected result
    expected_csv = """
    storage_name,                 technology_type,    region_id,    closure_year
    Battery_A,                    Battery__Storage,   NSW,          -1
    Battery_B,                    Battery__Storage,   SA,           -1
    """
    expected = csv_str_to_df(expected_csv)
    expected = expected.fillna(pd.NA)

    pd.testing.assert_frame_equal(
        result.sort_values("storage_name").reset_index(drop=True),
        expected.sort_values("storage_name").reset_index(drop=True),
        check_dtype=False,
    )


def test_restructure_battery_property_table(
    csv_str_to_df, workbook_table_cache_test_path: Path
):
    """Test the _restructure_battery_property_table function."""

    # grab test table:
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    battery_properties = iasr_tables["battery_properties"]

    # Execute function
    result = _restructure_battery_property_table(battery_properties)

    # Expected result - restructured with batteries as rows
    expected_csv = """
    storage_name,                       Maximum__power, Energy__capacity,   Charge__efficiency__(utility),  Discharge__efficiency__(utility),   Round__trip__efficiency__(utility),     Annual__degradation__(utility)
    Battery__storage__(1hr__storage),   1.0,            1.0,                91.7,                           91.7,                               84.0,                                     1.8
    Battery__storage__(2hrs__storage),  1.0,            2.0,                91.7,                           91.7,                               84.0,                                     1.8
    Battery__storage__(4hrs__storage),  1.0,            4.0,                92.2,                           92.2,                               85.0,                                     1.8
    Battery__storage__(8hrs__storage),  1.0,            8.0,                91.1,                           91.1,                               83.0,                                     1.8
    """
    expected = csv_str_to_df(expected_csv)

    # Assert results match expected
    pd.testing.assert_frame_equal(
        result.sort_values("storage_name").reset_index(drop=True),
        expected.sort_values("storage_name").reset_index(drop=True),
    )


def test_calculate_storage_duration_hours_edge_cases(csv_str_to_df, caplog):
    """Test edge cases for the _calculate_storage_duration_hours function."""
    # Case 1: Empty DataFrame
    empty_df = pd.DataFrame(
        columns=["storage_name", "maximum_capacity_mw", "energy_capacity_mwh"]
    )
    result_empty = _calculate_storage_duration_hours(empty_df)
    assert result_empty.empty, "Empty DataFrame should return empty DataFrame"

    # Case 2: Missing required columns
    missing_columns_df = csv_str_to_df("""
    storage_name,        some_other_column
    Battery_A,           value
    """)
    with pytest.raises(KeyError):
        _calculate_storage_duration_hours(missing_columns_df)

    # Case 3: All rows have missing values
    all_missing_df = csv_str_to_df("""
    storage_name,        maximum_capacity_mw,  energy_capacity_mwh
    Battery_A,           ,
    Battery_B,           ,                     100
    Battery_C,           100,
    """)

    # Capture logs to verify warnings are issued
    with caplog.at_level(logging.WARNING):
        result_all_missing = _calculate_storage_duration_hours(all_missing_df)

    # Check that warnings were logged for each row
    assert "Battery_A" in caplog.text
    assert "Battery_B" in caplog.text
    assert "Battery_C" in caplog.text
    assert "missing maximum_capacity_mw or energy_capacity_mwh value" in caplog.text

    # Result should be empty as all rows had missing values (missing values get dropped)
    assert result_all_missing.empty

    # Case 4: Non-numeric values in numeric columns
    non_numeric_df = csv_str_to_df("""
    storage_name,        maximum_capacity_mw,  energy_capacity_mwh
    Battery_A,           text,                 400
    Battery_B,           100,                  text
    """)

    # Clear previous logs
    caplog.clear()

    with caplog.at_level(logging.WARNING):
        result_non_numeric = _calculate_storage_duration_hours(non_numeric_df)

    # Check that warnings were logged
    assert "Battery_A" in caplog.text
    assert "Battery_B" in caplog.text

    # Result should be empty as all rows had invalid values
    assert result_non_numeric.empty

    # Case 5: Mixed valid and invalid rows
    mixed_df = csv_str_to_df("""
    storage_name,        maximum_capacity_mw,  energy_capacity_mwh
    Battery_Valid,       100,                  400
    Battery_Invalid,     ,                     100
    Battery_Zero,        0,                    200
    """)

    result_mixed = _calculate_storage_duration_hours(mixed_df)

    # Expected result - only valid rows and zero capacity row should remain
    expected_csv = """
    storage_name,        maximum_capacity_mw,  energy_capacity_mwh,  storage_duration_hours
    Battery_Valid,       100,                  400,                  4.0
    Battery_Zero,        0,                    200,                  0.0
    """
    expected = csv_str_to_df(expected_csv)

    # Assert results match expected
    pd.testing.assert_frame_equal(
        result_mixed.sort_values("storage_name").reset_index(drop=True),
        expected.sort_values("storage_name").reset_index(drop=True),
        check_dtype=False,
    )


def test_add_isp_resource_type_column(csv_str_to_df):
    """Test the _add_isp_resource_type_column function."""
    # Setup test data - include different capitalisations too
    storage_df_csv = """
    storage_name,            isp_resource_type
    Battery_A,               All__Battery__storage__(2hrs__storage)
    Battery_B,               All__Battery__Storage__(1hr__storage)
    Non_Battery,             Not__Matching__String__Pattern
    """
    storage_df = csv_str_to_df(storage_df_csv)

    # Execute function
    result = _add_isp_resource_type_column(storage_df)

    # Expected result - should extract duration and format consistently
    expected_csv = """
    storage_name,            isp_resource_type
    Battery_A,               Battery__Storage__2h
    Battery_B,               Battery__Storage__1h
    Non_Battery,             None
    """
    expected = csv_str_to_df(expected_csv)

    # Make nan values from result and expected the same type of nan:
    result = result.fillna(pd.NA)
    expected = expected.fillna(pd.NA)

    # Assert results match expected
    pd.testing.assert_frame_equal(
        result.sort_values("storage_name").reset_index(drop=True),
        expected.sort_values("storage_name").reset_index(drop=True),
    )


def test_add_and_clean_rez_ids(csv_str_to_df):
    """Test the _add_and_clean_rez_ids function."""
    # Setup test data
    storage_df_csv = """
    storage_name,       region_id,     sub_region_id,        rez_location
    Battery_NSW,        NSW,           NNSW,                 North__West__NSW
    Battery_QLD,        QLD,           SQ,                   Darling__Downs
    Battery_Non_REZ,    NSW,           NNSW,
    """
    storage_df = csv_str_to_df(storage_df_csv)

    rez_df_csv = """
    ID, Name,               NEM__Region,     NTNDP__Zone,     ISP__Sub-region,     Regional__Cost__Zones
    N1, North__West__NSW,   NSW,             NNS,             NNSW,                Medium
    Q8, Darling__Downs,     QLD,             SWQ,             SQ,                  Low
    N2, New__England,       NSW,             NNS,             NNSW,                Low
    """
    rez_df = csv_str_to_df(rez_df_csv)

    # Execute function
    result = _add_and_clean_rez_ids(storage_df, "rez_id", rez_df)
    result = result.fillna(pd.NA)

    # Expected result - should add rez_id column
    expected_csv = """
    storage_name,    region_id,     sub_region_id,        rez_location,     rez_id
    Battery_NSW,     NSW,           NNSW,                 N1,               N1
    Battery_QLD,     QLD,           SQ,                   Q8,               Q8
    Battery_Non_REZ, NSW,           NNSW,
    """
    expected = csv_str_to_df(expected_csv)
    expected = expected.fillna(pd.NA)

    # Assert results match expected
    pd.testing.assert_frame_equal(
        result.sort_values("storage_name").reset_index(drop=True),
        expected.sort_values("storage_name").reset_index(drop=True),
    )


def test_add_and_clean_rez_ids_special_names(csv_str_to_df):
    """Test _add_and_clean_rez_ids with special REZ name mappings."""
    # Setup test data with special REZ names that should be standardized
    storage_df_csv = """
    storage_name,       region_id,     sub_region_id,        rez_location
    Battery_TAS_NE,     TAS,           TAS,                  North__East__Tasmania__Coast
    Battery_TAS_NW,     TAS,           TAS,                  North__West__Tasmania__Coast
    Battery_VIC,        VIC,           VIC,                  Portland__Coast
    """
    storage_df = csv_str_to_df(storage_df_csv)

    rez_df_csv = """
    ID, Name,                   NEM__Region
    T1, North__Tasmania__Coast, TAS
    V2, Southern__Ocean,        VIC
    """
    rez_df = csv_str_to_df(rez_df_csv)

    # Execute function
    result = _add_and_clean_rez_ids(storage_df, "rez_id", rez_df)
    result = result.fillna(pd.NA)

    # Expected result - should standardize names and map to correct REZ IDs for all columns
    # with "rez" or "region_id" in the name (where REZ names are present)
    expected_csv = """
    storage_name,    region_id,     sub_region_id,        rez_location,     rez_id
    Battery_TAS_NE,  TAS,           TAS,                  T1,               T1
    Battery_TAS_NW,  TAS,           TAS,                  T1,               T1
    Battery_VIC,     VIC,           VIC,                  V2,               V2
    """
    expected = csv_str_to_df(expected_csv)
    expected = expected.fillna(pd.NA)

    # Assert results match expected
    pd.testing.assert_frame_equal(
        result.sort_values("storage_name").reset_index(drop=True),
        expected.sort_values("storage_name").reset_index(drop=True),
    )


def test_process_and_merge_opex(csv_str_to_df, workbook_table_cache_test_path: Path):
    """Test the _process_and_merge_opex function with various scenarios."""
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    new_entrant_storage_df = csv_str_to_df("""
    storage_name,                       technology_type,                    region_id,     sub_region_id,      fom_$/kw/annum
    Battery__Storage__(2hrs__storage),  Battery__Storage__(2hrs__storage),  NSW,           NNSW,               NSW__Low
    Battery__Storage__(1hr__storage),   Battery__Storage__(1hr__storage),   QLD,           SQ,                 QLD__Low
    """)

    new_entrant_fixed_opex = iasr_tables["fixed_opex_new_entrants"]

    table_attrs = dict(
        table="fixed_opex_new_entrants",
        table_lookup="Generator",
        table_col_prefix="Fixed OPEX ($/kW sent out/year)",
    )

    # Execute function for fixed O&M costs
    result_df, result_col = _process_and_merge_opex(
        new_entrant_storage_df.copy(),
        new_entrant_fixed_opex,
        "fom_$/kw/annum",
        table_attrs,
    )

    # Expected result for fixed O&M costs
    expected_fixed_csv = """
    storage_name,                       technology_type,                    region_id,     sub_region_id,      fom_$/kw/annum
    Battery__Storage__(2hrs__storage),  Battery__Storage__(2hrs__storage),  NSW,           NNSW,               10.799929999999998
    Battery__Storage__(1hr__storage),   Battery__Storage__(1hr__storage),   QLD,           SQ,                 7.592029999999999
    """
    expected_fixed = csv_str_to_df(expected_fixed_csv)

    # Assert fixed O&M results match expected
    pd.testing.assert_frame_equal(
        result_df.sort_values("storage_name").reset_index(drop=True),
        expected_fixed.sort_values("storage_name").reset_index(drop=True),
        check_exact=False,
        check_dtype=False,
    )
    assert result_col == "fom_$/kw/annum"

    # Test edge case: Empty storage dataframe
    empty_df = pd.DataFrame(
        columns=["storage_name", "technology_type", "fom_$/kw/annum"]
    )
    result_empty_df, result_empty_col = _process_and_merge_opex(
        empty_df, new_entrant_fixed_opex, "fom_$/kw/annum", table_attrs
    )
    assert result_empty_df.empty
    assert result_empty_col == "fom_$/kw/annum"

    # Test edge case: Empty opex table
    empty_opex = pd.DataFrame(
        columns=["Generator", "Fixed OPEX ($/kW sent out/year)_NSW Low"]
    )
    result_empty_opex_df, result_empty_opex_col = _process_and_merge_opex(
        new_entrant_storage_df.copy(), empty_opex, "fom_$/kw/annum", table_attrs
    )
    # Should return original dataframe with values unchanged
    assert "fom_$/kw/annum" in result_empty_opex_df.columns
    assert result_empty_opex_col == "fom_$/kw/annum"


def test_calculate_and_merge_tech_specific_lcfs(
    csv_str_to_df, workbook_table_cache_test_path: Path
):
    """Test the _calculate_and_merge_tech_specific_lcfs function."""

    iasr_tables = read_csvs(workbook_table_cache_test_path)

    # Setup test data
    storage_df_csv = """
    storage_name,                       technology_type,                     technology_specific_lcf_%
    Battery__Storage__(2hrs__storage),  Battery__Storage__(2hrs__storage),   NSW__Low
    Battery__Storage__(1hr__storage),   Battery__Storage__(1hr__storage),    QLD__Low
    """
    storage_df = csv_str_to_df(storage_df_csv)

    # Execute function
    result = _calculate_and_merge_tech_specific_lcfs(
        storage_df, iasr_tables, "technology_specific_lcf_%"
    )

    # Expected result
    expected_csv = """
    storage_name,                       technology_type,                     technology_specific_lcf_%
    Battery__Storage__(1hr__storage),   Battery__Storage__(1hr__storage),    100.0
    Battery__Storage__(2hrs__storage),  Battery__Storage__(2hrs__storage),   100.0
    """
    expected = csv_str_to_df(expected_csv)

    # Assert results match expected
    pd.testing.assert_frame_equal(
        result.sort_values("storage_name").reset_index(drop=True),
        expected.sort_values("storage_name").reset_index(drop=True),
        check_dtype=False,
        check_exact=False,  # allow for floating point errors
        rtol=1e-2,
    )

    # Test edge case: Empty storage dataframe
    empty_df = pd.DataFrame(
        columns=["storage_name", "technology_type", "technology_specific_lcf_%"]
    )
    result_empty = _calculate_and_merge_tech_specific_lcfs(
        empty_df, iasr_tables, "technology_specific_lcf_%"
    )
    assert result_empty.empty
    assert "technology_specific_lcf_%" in result_empty.columns

    # Test edge case: Missing location mapping
    missing_location_csv = """
    storage_name,                       technology_type,                     technology_specific_lcf_%
    Battery__Storage__(2hrs__storage),  Battery__Storage__(2hrs__storage),   Nonexistant__Region
    """
    missing_location_df = csv_str_to_df(missing_location_csv)

    result_missing = _calculate_and_merge_tech_specific_lcfs(
        missing_location_df, iasr_tables, "technology_specific_lcf_%"
    )

    # Expected result for missing location - should use default value of 1.0
    expected_missing_csv = """
    storage_name,                       technology_type,                     technology_specific_lcf_%
    Battery__Storage__(2hrs__storage),  Battery__Storage__(2hrs__storage),
    """
    expected_missing = csv_str_to_df(expected_missing_csv)
    # try to match nan types:
    expected_missing = expected_missing.fillna("replace_with_nan")
    expected_missing = expected_missing.replace("replace_with_nan", pd.NA)

    result_missing = result_missing.fillna("replace_with_nan")
    result_missing = result_missing.replace("replace_with_nan", pd.NA)

    pd.testing.assert_frame_equal(
        result_missing.sort_values("storage_name").reset_index(drop=True),
        expected_missing.sort_values("storage_name").reset_index(drop=True),
        check_dtype=False,
    )


def test_process_and_merge_connection_cost(csv_str_to_df):
    """Test the _process_and_merge_connection_cost function."""
    # Setup test data
    storage_df_csv = """
    storage_name,                        connection_cost_rez/_region_id,    connection_cost_technology
    Battery__Storage__(2hrs__storage),   NSW,                               2hr__Battery__Storage
    Battery__Storage__(1hr__storage),    QLD,                               1hr__Battery__Storage
    """
    storage_df = csv_str_to_df(storage_df_csv)

    connection_costs_csv = """
    Region,    1__hr__Battery__Storage,     2__hr__Battery__Storage
    NSW,       50,                          55
    QLD,       45,                          48
    """
    connection_costs_table = csv_str_to_df(connection_costs_csv)

    # Execute function
    result = _process_and_merge_connection_cost(storage_df, connection_costs_table)

    # Expected result - connection costs should be in $/MW (multiplied by 1000)
    expected_csv = """
    storage_name,                        connection_cost_rez/_region_id,    connection_cost_technology,      connection_cost_$/mw
    Battery__Storage__(2hrs__storage),   NSW,                               2hr__Battery__Storage,           55000.0
    Battery__Storage__(1hr__storage),    QLD,                               1hr__Battery__Storage,           45000.0
    """
    expected = csv_str_to_df(expected_csv)

    # Assert results match expected
    pd.testing.assert_frame_equal(
        result.sort_values("storage_name").reset_index(drop=True),
        expected.sort_values("storage_name").reset_index(drop=True),
        check_dtype=False,
    )

    # Test edge case: Empty storage dataframe
    empty_df = pd.DataFrame(
        columns=[
            "storage_name",
            "technology_type",
            "connection_cost_rez/_region_id",
            "connection_cost_technology",
        ]
    )
    result_empty = _process_and_merge_connection_cost(empty_df, connection_costs_table)
    assert result_empty.empty
    assert "connection_cost_$/mw" in result_empty.columns

    # Test edge case: Missing connection costs that should raise ValueError. Use the same storage_df to test.
    # Create a connection costs table that will result in NaN values after merging
    incomplete_costs_csv = """
    Region,    1__hr__Battery__Storage
    QLD,       50
    """
    incomplete_costs = csv_str_to_df(incomplete_costs_csv)

    with pytest.raises(
        ValueError,
        match=r"Missing connection costs for the following batteries: \['Battery Storage \(2hrs storage\)'\]",
    ):
        _process_and_merge_connection_cost(storage_df, incomplete_costs)
