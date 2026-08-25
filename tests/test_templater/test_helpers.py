import pandas as pd
import pytest

from ispypsa.templater.helpers import (
    _apply_known_value_replacement,
    _assert_table_valid,
    _build_geo_region_lookup,
    _derive_phes_symmetric_efficiency,
    _get_property_value_map,
    _group_properties_by_source,
    _is_battery_row,
    _is_pumped_hydro_row,
    _is_storage_row,
    _is_subregion_geo_id,
    _looks_like_financial_year,
    _manual_remove_footnotes_from_generator_names,
    _map_geo_id_to_granularity,
    _pick_location,
    _required_property_columns,
    _rez_name_to_id_mapping,
    _set_geo_id,
    _snakecase_string,
    _standardise_storage_capitalisation,
    _strip_all_text_after_numeric_value,
    _where_any_substring_appears,
)

snakecase_test_cases = {
    # Single word cases
    "word": "word",
    "Word": "word",
    # CamelCase variations
    "CamelCaseWord": "camel_case_word",
    "HTTPResponseCode": "http_response_code",
    "JSONDataFormat": "json_data_format",
    # Acronyms
    "NEM Region": "nem_region",
    # Mixed cases and symbols
    "snake_case_word": "snake_case_word",
    "Already_snake_case": "already_snake_case",
    "wordWith123Numbers": "word_with_123_numbers",
    "123numberedWords": "123numbered_words",
    "Word_with-Mixed_Cases-and_dashes": "word_with_mixed_cases_and_dashes",
    "MergedWord_with-Mixed_Cases-and_dashes": "merged_word_with_mixed_cases_and_dashes",
    # Special characters and whitespace
    " words  With   spaces ": "words_with_spaces",
    # Empty strings and unusual cases
    "": "",
    " ": "",
    # Duplicates and delimiters
    "Multiple___Underscores": "multiple_underscores",
    "multiple--dashes": "multiple_dashes",
    # Non-printable or control characters
    "line\nbreaks\tand\ttabs": "line_breaks_and_tabs",
    # Columns with units
    "FOM ($/kW/annum)": "fom_$/kw/annum",
    "VOM ($/MWh sent-out)": "vom_$/mwh_sent_out",
    "Capacity (MW)": "capacity_mw",
    # Columns with years
    "Mean time to repair_Partial outage_Post 2022": "mean_time_to_repair_partial_outage_post_2022",
    "2022-23": "2022_23",
    # String with commas
    "Existing, Committed and Anticipated batteries": "existing_committed_and_anticipated_batteries",
}


@pytest.mark.parametrize(
    "input,expected", [(k, v) for k, v in snakecase_test_cases.items()]
)
def test_snakecase(input: str, expected: str):
    processed_input = _snakecase_string(input)
    assert processed_input == expected


def test_where_any_substring_appears():
    test_input = [
        "Wind",
        "wind",
        "OCGT",
        "All Solar PV",
        "Hydroelectric",
        "Solar thermal",
    ]
    output = _where_any_substring_appears(
        pd.Series(test_input), ["solar", "wind", "hydro"]
    )
    assert (output == [True, True, False, True, True, True]).all()
    output_2 = _where_any_substring_appears(pd.Series(test_input), ["solar"])
    assert (output_2 == [False, False, False, True, False, True]).all()


def test_strip_all_text_after_numeric_value_series():
    """Test stripping text after numeric values in pandas Series."""
    # Test with Series containing various numeric formats
    test_series = pd.Series(
        [
            "100 MW capacity",
            "1,500 units available",
            "2.5 percent increase",
            "3,000.50 total cost",
            "No numeric value here",
            "500",  # Just a number
            "",  # Empty string
            "123.45 some text 678",  # Multiple numbers
            "+1,234.56 positive value",
            "100MW",  # No space between number and text
            "2.5%",  # No space, percentage
            "1,000units",  # No space, with comma
            # Negative numbers
            "-100 MW",
            "-1,234.56 units",
            "-500",
            # Edge cases
            "++123 invalid",  # Invalid: multiple plus signs
            "1.2.3 multiple dots",  # Partially valid: extracts "1.2"
            "1,23 wrong comma placement",  # Partially valid: extracts "1"
            "...123",  # Invalid: starts with dots
            "+-123",  # Invalid: plus and minus together
            ",,,123",  # Invalid: starts with commas
        ]
    )

    result = _strip_all_text_after_numeric_value(test_series)

    expected = pd.Series(
        [
            "100",
            "1,500",
            "2.5",
            "3,000.50",
            "No numeric value here",
            "500",
            "",
            "123.45",
            "+1,234.56",
            "100",  # Should now work without space
            "2.5",  # Should now work without space
            "1,000",  # Should now work without space
            # Negative numbers
            "-100",
            "-1,234.56",
            "-500",
            # Edge cases
            "++123 invalid",  # Remains unchanged (invalid format)
            "1.2",  # Extracts valid number at start
            "1",  # Extracts valid number at start
            "...123",  # Remains unchanged (invalid format)
            "+-123",  # Remains unchanged (invalid format)
            ",,,123",  # Remains unchanged (invalid format)
        ]
    )

    pd.testing.assert_series_equal(result, expected)


def test_strip_all_text_after_numeric_value_non_object_dtype():
    """Test that non-object dtype Series are returned unchanged."""
    # Test with numeric Series (non-object dtype)
    numeric_series = pd.Series([1, 2, 3, 4, 5])
    result = _strip_all_text_after_numeric_value(numeric_series)
    pd.testing.assert_series_equal(result, numeric_series)

    # Test with float Series
    float_series = pd.Series([1.5, 2.7, 3.9])
    result = _strip_all_text_after_numeric_value(float_series)
    pd.testing.assert_series_equal(result, float_series)


def test_standardise_storage_capitalisation():
    """Test standardisation of 'storage' capitalisation in generator names."""

    # Create a test series with various storage naming patterns
    test_series = pd.Series(
        [
            # Battery storage cases (should have capital S)
            "Battery storage",
            "Battery Storage",
            # Duration-related storage cases (should have lowercase s)
            "2hrs Storage",
            "4 hrs Storage",
            "1hr storage",
            "8 hr Storage",
            "12hrs storage",
            "24 hr storage",
            # Edge cases
            "Battery Storage with 2hrs storage",  # Mixed case
            "2hrs Storage Battery Storage",  # Mixed case, different order
            "Storage 4hrs",  # Non-standard format
            "Battery Storage 4hrs",  # Ambiguous case - no change
            "4hrs Battery Storage",  # Ambiguous case - no change
            "Other Storage Technology",  # Non-battery storage
            "StorageSystem",  # No space
            "",  # Empty string
        ]
    )

    # Apply the function
    result = _standardise_storage_capitalisation(test_series)

    # Expected results
    expected = pd.Series(
        [
            # Battery storage cases
            "Battery Storage",
            "Battery Storage",
            # Duration-related storage cases
            "2hrs storage",
            "4 hrs storage",
            "1hr storage",
            "8 hr storage",
            "12hrs storage",
            "24 hr storage",
            # Edge cases
            "Battery Storage with 2hrs storage",  # Both patterns preserved correctly
            "2hrs storage Battery Storage",  # Both patterns preserved correctly
            "Storage 4hrs",
            "Battery Storage 4hrs",  # Preserved as battery name
            "4hrs Battery Storage",  # Preserved as battery name
            "Other Storage Technology",
            "StorageSystem",  # Unchanged (no space)
            "",  # Empty string unchanged
        ]
    )

    # Compare results
    pd.testing.assert_series_equal(result, expected)


def test_manual_remove_footnotes_from_generator_names_column_names():
    """Test that footnotes are removed from column names."""
    # Create a test DataFrame with footnotes in column names
    df = pd.DataFrame(
        {
            "Small OCGT2": [1, 2, 3],
            "Pumped Hydro3 (8 hrs storage)": [4, 5, 6],
            "Normal Column": [7, 8, 9],
        }
    )

    # Apply the function
    result = _manual_remove_footnotes_from_generator_names(df)

    # Expected result
    expected = pd.DataFrame(
        {
            "Small OCGT": [1, 2, 3],
            "Pumped Hydro (8 hrs storage)": [4, 5, 6],
            "Normal Column": [7, 8, 9],
        }
    )

    # Check that the column names are correctly renamed
    pd.testing.assert_frame_equal(result, expected)


def test_manual_remove_footnotes_from_generator_names():
    """Test handling of mixed cases - footnotes in both column names and cell values."""
    # Create a test DataFrame with footnotes in both column names and cell values
    df = pd.DataFrame(
        {
            "Small OCGT2": ["Small OCGT2", "Pumped Hydro3 (8 hrs storage)"],
            "Pumped Hydro3 (8 hrs storage)": ["Normal Generator", "Small OCGT2"],
        }
    )

    # Apply the function
    result = _manual_remove_footnotes_from_generator_names(df)

    # Expected result
    expected = pd.DataFrame(
        {
            "Small OCGT": ["Small OCGT", "Pumped Hydro (8 hrs storage)"],
            "Pumped Hydro (8 hrs storage)": ["Normal Generator", "Small OCGT"],
        }
    )

    # Check that both column names and cell values are correctly handled
    pd.testing.assert_frame_equal(result, expected)


def test_rez_name_to_id_mapping():
    """Test the REZ name to ID mapping functionality."""

    # Create sample input data
    series = pd.Series(
        [
            "North East Tasmania Coast",  # Should be standardized to "North Tasmania Coast"
            "North West Tasmania Coast",  # Should be standardized to "North Tasmania Coast"
            "North Tasmania Coast",  # Already correct
            "Portland Coast",  # Should be standardized to "Southern Ocean"
            "Southern Ocean",  # Already correct
            "Central NSW Tablelands",  # Regular REZ
            "Victoria Non-REZ",  # Non-REZ that should be mapped to V0
            "New South Wales Non-REZ",  # Non-REZ that should be mapped to N0
            "Unknown REZ",  # Should be fuzzy matched if possible
        ]
    )

    # Create sample REZ table
    renewable_energy_zones = pd.DataFrame(
        {
            "ID": ["T1", "V2", "N3"],
            "Name": [
                "North Tasmania Coast",
                "Southern Ocean",
                "Central NSW Tablelands",
            ],
        }
    )

    # Apply the function
    result = _rez_name_to_id_mapping(series, "test_column", renewable_energy_zones)

    # Expected result after mapping
    expected = pd.Series(
        [
            "T1",  # North East Tasmania Coast -> North Tasmania Coast -> T1
            "T1",  # North West Tasmania Coast -> North Tasmania Coast -> T1
            "T1",  # North Tasmania Coast -> T1
            "V2",  # Portland Coast -> Southern Ocean -> V2
            "V2",  # Southern Ocean -> V2
            "N3",  # Central NSW Tablelands -> N3
            "V0",  # Victoria Non-REZ -> V0
            "N0",  # New South Wales Non-REZ -> N0
            "Unknown REZ",  # Should remain as is if fuzzy matching threshold not met
        ]
    )

    # Check the result
    pd.testing.assert_series_equal(result, expected)


def test_rez_name_to_id_mapping_non_rez_addition():
    """Test that non-REZ entries are correctly added to the REZ table."""

    # Create sample input data with only non-REZ entries
    series = pd.Series(["Victoria Non-REZ", "New South Wales Non-REZ"])

    # Create empty REZ table
    renewable_energy_zones = pd.DataFrame({"ID": [], "Name": []})

    # Apply the function
    result = _rez_name_to_id_mapping(series, "test_column", renewable_energy_zones)

    # Expected result after mapping
    expected = pd.Series(["V0", "N0"])

    # Check the result
    pd.testing.assert_series_equal(result, expected)

    # Also verify that the function doesn't modify the original DataFrame
    assert len(renewable_energy_zones) == 0


def test_rez_name_to_id_mapping_empty_input():
    """Test handling of empty input series."""

    # Create empty input series
    series = pd.Series([], dtype=object)

    # Create sample REZ table
    renewable_energy_zones = pd.DataFrame(
        {
            "ID": ["T1", "V2", "N3"],
            "Name": [
                "North Tasmania Coast",
                "Southern Ocean",
                "Central NSW Tablelands",
            ],
        }
    )

    # Apply the function
    result = _rez_name_to_id_mapping(series, "test_column", renewable_energy_zones)

    # Expected result - empty series
    expected = pd.Series([], dtype=object)

    # Check the result
    pd.testing.assert_series_equal(result, expected)


def test_looks_like_financial_year_matches_only_canonical_formats():
    assert _looks_like_financial_year("2024-25") is True
    assert _looks_like_financial_year("2025-26") is True
    assert _looks_like_financial_year("2024-2025") is False
    assert _looks_like_financial_year("24-25") is False
    assert _looks_like_financial_year("Status") is False
    assert _looks_like_financial_year("Flow path") is False


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


# --- _is_battery_row ---


def test_is_battery_row(csv_str_to_df):
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,    Technology Type
        Q1 Battery - 2h,        Battery Storage (2hrs storage)
        NQ Battery - Dist,      Distributed Resources Batteries
        Q1 Wind,                Wind
        N1 Pumped Hydro - 24h,  Pumped Hydro (24hrs storage)
        Q1 Solar Thermal,       Solar Thermal (16hrs storage)
    """)

    result = _is_battery_row(new_entrants)

    # Battery + Distributed Resources Batteries match; others (incl. pumped
    # hydro and solar thermal storage) do not.
    assert list(result) == [True, True, False, False, False]


# --- _is_pumped_hydro_row ---


def test_is_pumped_hydro_row(csv_str_to_df):
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,    Technology Type
        Q1 Battery - 2h,        Battery Storage (2hrs storage)
        NQ Battery - Dist,      Distributed Resources Batteries
        Q1 Wind,                Wind
        N1 Pumped Hydro - 24h,  Pumped Hydro (24hrs storage)
        Q1 Solar Thermal,       Solar Thermal (16hrs storage)
    """)

    result = _is_pumped_hydro_row(new_entrants)

    # Pumped Hydro resources match; Batter* and other storage do not.
    assert list(result) == [False, False, False, True, False]


# --- _is_storage_row ---


def test_is_storage_row(csv_str_to_df):
    new_entrants = csv_str_to_df("""
        IASR ID / DLT names,    Technology
        Q1 Battery - 2h,        Battery Storage (2hrs storage)
        NQ Battery - Dist,      Distributed Resources Batteries
        Q1 Wind,                Wind
        N1 Pumped Hydro - 24h,  Pumped Hydro (24hrs storage)
        Q1 Solar Thermal,       Solar Thermal (16hrs storage)
    """)

    result = _is_storage_row(new_entrants, col_to_check="Technology")

    # Battery,  Distributed Resources Batteries and Pumped Hydro all match.
    # Solar thermal still does not.
    assert list(result) == [True, True, False, True, False]


# --- _derive_phes_symmetric_efficiency ---


def test_derive_phes_symmetric_efficiency(csv_str_to_df):
    # A single round-trip efficiency becomes equal charge and discharge legs, each its
    # square root: sqrt(0.81) = 0.9 -> 90.0%.
    phes = csv_str_to_df("""
        name,                  round_trip_efficiency
        NQ Pumped Hydro - 24h, 81.0
    """)

    result = _derive_phes_symmetric_efficiency(phes)

    expected = csv_str_to_df("""
        name,                  round_trip_efficiency, efficiency_charge, efficiency_discharge
        NQ Pumped Hydro - 24h, 81.0,                  90.0,              90.0
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False, rtol=1e-6)


# --- _set_geo_id ---


def test_set_geo_id(csv_str_to_df):
    new_entrants = csv_str_to_df("""
        technology,                     REZ ID,         Sub-region
        Wind,                           N3,             CNSW
        OCGT (small GT),                Not Applicable, NQ
    """)

    result = _set_geo_id(new_entrants)

    expected = csv_str_to_df("""
        technology,                     REZ ID,         Sub-region, geo_id
        Wind,                           N3,             CNSW,       N3
        OCGT (small GT),                Not Applicable, NQ,         NQ
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_set_geo_id_empty_input(csv_str_to_df):
    new_entrants = pd.DataFrame(columns=["technology", "REZ ID", "Sub-region"])

    result = _set_geo_id(new_entrants)

    expected = csv_str_to_df("""
        technology, REZ ID, Sub-region, geo_id
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _build_geo_region_lookup ---


def test_build_geo_region_lookup(csv_str_to_df):
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        NQ,      subregion,  QLD
        CNSW,    subregion,  NSW
        Q1,      rez,        QLD
    """)

    result = _build_geo_region_lookup(sub_regional_geography)

    # sub-regions and REZs map to their region; regions map to themselves.
    assert result == {
        "NQ": "QLD",
        "CNSW": "NSW",
        "Q1": "QLD",
        "QLD": "QLD",
        "NSW": "NSW",
    }


# --- _map_geo_id_to_granularity ---


def test_map_geo_id_to_granularity_sub_regions(csv_str_to_df):
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        SNW,     subregion,  NSW
        Q1,      rez,        QLD
    """)
    geo_id = pd.Series(["CNSW", "SNW", "Q1"])

    result = _map_geo_id_to_granularity(geo_id, "sub_regions", sub_regional_geography)

    expected = pd.Series(["CNSW", "SNW", "Q1"])
    pd.testing.assert_series_equal(result, expected)


def test_map_geo_id_to_granularity_nem_regions(csv_str_to_df):
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        SNW,     subregion,  NSW
        Q1,      rez,        QLD
    """)
    geo_id = pd.Series(["CNSW", "SNW", "Q1"])

    result = _map_geo_id_to_granularity(geo_id, "nem_regions", sub_regional_geography)

    expected = pd.Series(["NSW", "NSW", "Q1"])
    pd.testing.assert_series_equal(result, expected)


def test_map_geo_id_to_granularity_single_region(csv_str_to_df):
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        Q1,      rez,        QLD
    """)
    geo_id = pd.Series(["CNSW", "Q1"])

    result = _map_geo_id_to_granularity(geo_id, "single_region", sub_regional_geography)

    expected = pd.Series(["NEM", "Q1"])
    pd.testing.assert_series_equal(result, expected)


# --- _is_subregion_geo_id ---


def test_is_subregion_geo_id(csv_str_to_df):
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type
        CNSW,    subregion
        Q1,      rez
    """)
    geo_id = pd.Series(["CNSW", "Q1"])

    result = _is_subregion_geo_id(geo_id, sub_regional_geography)

    expected = pd.Series([True, False])
    pd.testing.assert_series_equal(result, expected)


# --- _apply_known_value_replacement ---


def test_apply_known_value_replacement(csv_str_to_df):
    maximum_capacity = csv_str_to_df("""
        IASR ID,   Installed capacity (MW)
        KiataWF1,  30.0
        BW01,      660.0
    """)
    iasr_tables = {
        "maximum_capacity": maximum_capacity,
        "some_other_table": pd.DataFrame({"col": [1]}),
    }

    correction = dict(
        table_name="maximum_capacity",
        column="IASR ID",
        replacements={"KiataWF1": "KIATAWF1"},
    )
    result = _apply_known_value_replacement(iasr_tables, correction)

    expected = csv_str_to_df("""
        IASR ID,   Installed capacity (MW)
        KIATAWF1,  30.0
        BW01,      660.0
    """)
    pd.testing.assert_frame_equal(result["maximum_capacity"], expected)

    # Other tables pass through untouched; input dict itself isn't mutated.
    assert result["some_other_table"] is iasr_tables["some_other_table"]
    unmutated = csv_str_to_df("""
        IASR ID,   Installed capacity (MW)
        KiataWF1,  30.0
        BW01,      660.0
    """)
    pd.testing.assert_frame_equal(iasr_tables["maximum_capacity"], unmutated)


# --- _group_properties_by_source ---


def test_group_properties_by_source():
    # Two properties sharing a (table, key_col) source are grouped together, each
    # keeping its original attrs dict unchanged; two properties from the same table
    # but with different key_cols are independent.
    property_map = {
        "storage_hours": {
            "table": "battery_properties",
            "key_col": "Technology",
            "value_col": "Energy capacity_Hours",
        },
        "efficiency_charge": {
            "table": "battery_properties",
            "key_col": "Technology",
            "value_col": "Charge efficiency_%",
        },
        "lifetime_technical": {
            "table": "lead_time_and_project_life",
            "key_col": "Technology",
            "value_col": "Technical life (years)",
        },
        "different_key_col": {
            "table": "lead_time_and_project_life",
            "key_col": "Alternate Technology",
            "value_col": "Test",
        },
    }

    result = _group_properties_by_source(property_map)

    expected = {
        ("battery_properties", "Technology"): {
            "storage_hours": property_map["storage_hours"],
            "efficiency_charge": property_map["efficiency_charge"],
        },
        ("lead_time_and_project_life", "Technology"): {
            "lifetime_technical": property_map["lifetime_technical"],
        },
        ("lead_time_and_project_life", "Alternate Technology"): {
            "different_key_col": property_map["different_key_col"]
        },
    }
    assert result == expected


# --- _required_property_columns ---


def test_required_property_columns():
    # Two properties sharing a source - both properties' value_col/key_col are
    # collected into one set.
    props = {
        "storage_hours": {
            "table": "battery_properties",
            "key_col": "Technology",
            "value_col": "Storage hours",
        },
        "degradation_annual": {
            "table": "battery_properties",
            "key_col": "Technology",
            "value_col": "Variable value",
        },
    }

    result = _required_property_columns(props)

    assert result == {"Technology", "Storage hours", "Variable value"}


# --- _get_property_value_map ---


def test_get_property_value_map_numeric_with_scale(csv_str_to_df):
    table = csv_str_to_df("""
        Technology,  Base value
        Wind,        2.0
        CCGT,        5.0
    """)
    attrs = {"key_col": "Technology", "value_col": "Base value", "scale": 1000.0}

    result = _get_property_value_map(table, attrs)

    expected = pd.Series(
        [2000.0, 5000.0], index=pd.Index(["Wind", "CCGT"], name="Technology")
    )
    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_get_property_value_map_numeric_false_skips_coercion_and_scale(csv_str_to_df):
    # commissioning_date-style column: passed through as-is, no numeric coercion.
    table = csv_str_to_df("""
        IASR ID,  Commissioning date
        BW01,     2028-12-01
    """)
    attrs = {"key_col": "IASR ID", "value_col": "Commissioning date", "numeric": False}

    result = _get_property_value_map(table, attrs)

    expected = pd.Series(["2028-12-01"], index=pd.Index(["BW01"], name="IASR ID"))
    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_get_property_value_map_raises_on_typo(csv_str_to_df):
    table = csv_str_to_df("""
        IASR ID,  Heat rate (GJ/MWh)
        BW01,     not_a_number
    """)
    attrs = {"key_col": "IASR ID", "value_col": "Heat rate (GJ/MWh)"}

    with pytest.raises(ValueError, match=r'Unable to parse string "not_a_number"'):
        _get_property_value_map(table, attrs)


# --- _assert_table_valid ---


def test_assert_table_valid_passes(csv_str_to_df):
    # Table has both required columns and at least one row - no error raised.
    table = csv_str_to_df("""
        Technology,  Base value
        Wind,        20.0
    """)
    # should not raise
    _assert_table_valid(
        table, "fixed_opex_new_entrants", {"Technology", "Base value"}, "'fom'"
    )


def test_assert_table_valid_raises_missing_columns(csv_str_to_df):
    # Table is missing a required column -> raise, naming the table and the column.
    table = csv_str_to_df("""
        Technology,  Base value
        Wind,        20.0
    """)

    with pytest.raises(
        ValueError,
        match=r"'fixed_opex_new_entrants' table missing required columns: "
        r"\['Storage hours'\]",
    ):
        _assert_table_valid(
            table,
            table_name="fixed_opex_new_entrants",
            required_cols={"Technology", "Storage hours"},
            merge_desc="'fom'",
        )


def test_assert_table_valid_raises_empty_table():
    # Table has both required columns but no rows -> raise, naming what would
    # have been merged.
    table = pd.DataFrame(columns=["Technology", "Base value"])

    with pytest.raises(
        ValueError,
        match=r"'fixed_opex_new_entrants' table is empty - cannot merge 'fom'",
    ):
        _assert_table_valid(
            table,
            table_name="fixed_opex_new_entrants",
            required_cols={"Technology", "Base value"},
            merge_desc="'fom'",
        )
