import pandas as pd
import pytest

from ispypsa.templater.helpers import (
    _manual_remove_footnotes_from_generator_names,
    _rez_name_to_id_mapping,
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
