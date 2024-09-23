from pathlib import Path

import pandas as pd
import pytest
import logging

from ispypsa.templater.helpers import (
    ModelConfigOptionError,
    _fuzzy_match_names,
    _snakecase_string,
)
from ispypsa.templater.nodes import _NEM_REGION_IDS, template_nodes


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
    # Columns with years
    "Mean time to repair_Partial outage_Post 2022": "mean_time_to_repair_partial_outage_post_2022",
}


@pytest.mark.parametrize(
    "input,expected", [(k, v) for k, v in snakecase_test_cases.items()]
)
def test_snakecase(input: str, expected: str):
    processed_input = _snakecase_string(input)
    assert processed_input == expected


def test_node_templater_regional(workbook_table_cache_test_path: Path):
    node_template = template_nodes(workbook_table_cache_test_path, "regional")
    assert node_template.index.name == "node_id"
    assert set(node_template.regional_reference_node_voltage_kv) == set((132,))
    assert set(node_template.index) == set(("QLD", "VIC"))
    assert set(node_template.isp_sub_region) == set(("Gas Town", "Bullet Farm"))
    assert not node_template.substation_longitude.empty
    assert not node_template.substation_latitude.empty


def test_node_templater_sub_regional(workbook_table_cache_test_path: Path):
    node_template = template_nodes(workbook_table_cache_test_path, "sub_regional")
    assert node_template.index.name == "node_id"
    assert set(node_template.sub_region_reference_node_voltage_kv) == set((132,))
    assert set(node_template.index) == set(("GT", "BF"))
    assert set(node_template.nem_region_id) == set(("QLD", "VIC"))
    assert not node_template.substation_longitude.empty
    assert not node_template.substation_latitude.empty


def test_node_templater_single_region(workbook_table_cache_test_path: Path):
    node_template = template_nodes(workbook_table_cache_test_path, "single_region")
    assert node_template.index.name == "node_id"
    assert set(node_template.regional_reference_node_voltage_kv) == set((66,))
    assert set(node_template.index) == set(("NEM",))
    assert set(node_template.nem_region) == set(("Victoria",))
    assert not node_template.substation_longitude.empty
    assert not node_template.substation_latitude.empty


def test_invalid_granularity(workbook_table_cache_test_path: Path):
    with pytest.raises(ModelConfigOptionError):
        template_nodes(workbook_table_cache_test_path, granularity="Wastelands")


def test_no_substation_coordinates(workbook_table_cache_test_path: Path, mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        "ispypsa.templater.nodes._request_transmission_substation_coordinates",
        return_value=pd.DataFrame(({})).T,
    )
    node_template = template_nodes(workbook_table_cache_test_path, "sub_regional")
    assert node_template.index.name == "node_id"
    assert set(node_template.sub_region_reference_node_voltage_kv) == set((132,))
    assert set(node_template.index) == set(("GT", "BF"))
    assert set(node_template.nem_region_id) == set(("QLD", "VIC"))
    assert len(node_template) == 2
    assert len(node_template.columns) == 6


def test_substation_coordinate_http_error(
    workbook_table_cache_test_path: Path, requests_mock, caplog
):
    url = "https://services.ga.gov.au/gis/services/Foundation_Electricity_Infrastructure/MapServer/WFSServer"
    requests_mock.get(url, status_code=404)
    # Run the test and expect an HTTPError
    with caplog.at_level(logging.WARNING):
        template_nodes(workbook_table_cache_test_path)
    assert "Failed to fetch substation coordinates" in caplog.text
    assert "Network node data will be templated without coordinate data" in caplog.text
