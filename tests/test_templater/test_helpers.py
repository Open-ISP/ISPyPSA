import pandas as pd
import pytest

from ispypsa.templater.helpers import (
    _snakecase_string,
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
