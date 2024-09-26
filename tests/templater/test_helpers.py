import pandas as pd
import pytest

from ispypsa.templater.helpers import (
    _fuzzy_match_names,
    _snakecase_string,
)
from ispypsa.templater.nodes import _NEM_REGION_IDS


def test_fuzzy_matching() -> None:
    region_typos = pd.Series(
        ["New South Walks", "Coinsland", "North Australia", "Bigtoria", "Radmania"]
    )
    expected = pd.Series(
        ["New South Wales", "Queensland", "South Australia", "Victoria", "Tasmania"]
    )
    test_results = _fuzzy_match_names(region_typos, _NEM_REGION_IDS.keys())
    assert (test_results == expected).all()


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
}


@pytest.mark.parametrize(
    "input,expected", [(k, v) for k, v in snakecase_test_cases.items()]
)
def test_snakecase(input: str, expected: str):
    processed_input = _snakecase_string(input)
    assert processed_input == expected
