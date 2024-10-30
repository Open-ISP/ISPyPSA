import logging
from pathlib import Path

import pandas as pd
import pytest

from ispypsa.config.validators import ModelConfigOptionError
from ispypsa.templater.nodes import template_nodes


def test_node_templater_regional(workbook_table_cache_test_path: Path):
    node_template = template_nodes(workbook_table_cache_test_path, "regional")
    assert node_template.index.name == "node_id"
    assert set(node_template.regional_reference_node_voltage_kv) == set((132,))
    assert set(node_template.index) == set(("QLD", "VIC"))
    assert set(node_template.isp_sub_region) == set(("South Queensland", "Victoria"))
    assert not node_template.substation_longitude.empty
    assert not node_template.substation_latitude.empty


def test_node_templater_sub_regional(workbook_table_cache_test_path: Path):
    node_template = template_nodes(workbook_table_cache_test_path, "sub_regional")
    assert node_template.index.name == "node_id"
    assert set(node_template.sub_region_reference_node_voltage_kv) == set((132,))
    assert set(node_template.index) == set(("SQ", "VIC"))
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
    assert set(node_template.index) == set(("SQ", "VIC"))
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
