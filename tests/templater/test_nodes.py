import logging
from pathlib import Path

import pandas as pd

from ispypsa.templater.nodes import (
    template_nodes,
    template_region_to_single_sub_region_mapping,
    template_sub_regions_to_nem_regions_mapping,
)


def test_node_templater_nem_regions(workbook_table_cache_test_path: Path):
    node_template = template_nodes(
        workbook_table_cache_test_path,
        regional_granularity="nem_regions",
        rez_nodes="attached_to_parent_node",
    )
    assert node_template.index.name == "node_id"
    assert set(node_template.regional_reference_node_voltage_kv) == set((132,))
    assert set(node_template.index) == set(("QLD", "VIC"))
    assert set(node_template.type) == set(("nem_region",))
    assert not node_template.substation_longitude.empty
    assert not node_template.substation_latitude.empty


def test_node_templater_sub_regions(workbook_table_cache_test_path: Path):
    node_template = template_nodes(
        workbook_table_cache_test_path,
        regional_granularity="sub_regions",
        rez_nodes="attached_to_parent_node",
    )
    assert node_template.index.name == "node_id"
    assert set(node_template.sub_region_reference_node_voltage_kv) == set((132,))
    assert set(node_template.index) == set(("SQ", "VIC"))
    assert set(node_template.type) == set(("sub_region",))
    assert not node_template.substation_longitude.empty
    assert not node_template.substation_latitude.empty


def test_node_templater_sub_regions_with_rezs(workbook_table_cache_test_path: Path):
    node_template = template_nodes(
        workbook_table_cache_test_path,
        regional_granularity="sub_regions",
        rez_nodes="discrete_nodes",
    )
    assert node_template.index.name == "node_id"
    assert set(node_template.index) == set(("SQ", "VIC", "Q1", "Q2"))
    assert set(node_template.type) == set(("sub_region", "rez"))


def test_node_templater_single_region(workbook_table_cache_test_path: Path):
    node_template = template_nodes(
        workbook_table_cache_test_path,
        regional_granularity="single_region",
        rez_nodes="attached_to_parent_node",
    )
    assert node_template.index.name == "node_id"
    assert set(node_template.regional_reference_node_voltage_kv) == set((66,))
    assert set(node_template.index) == set(("NEM",))
    assert not node_template.substation_longitude.empty
    assert not node_template.substation_latitude.empty


def test_no_substation_coordinates(workbook_table_cache_test_path: Path, mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        "ispypsa.templater.nodes._request_transmission_substation_coordinates",
        return_value=pd.DataFrame(({})).T,
    )
    node_template = template_nodes(
        workbook_table_cache_test_path,
        regional_granularity="sub_regions",
        rez_nodes="attached_to_parent_node",
    )
    assert node_template.index.name == "node_id"
    assert set(node_template.sub_region_reference_node_voltage_kv) == set((132,))
    assert set(node_template.index) == set(("SQ", "VIC"))
    assert len(node_template) == 2
    assert len(node_template.columns) == 4


def test_substation_coordinate_http_error(
    workbook_table_cache_test_path: Path, requests_mock, caplog
):
    url = "https://services.ga.gov.au/gis/services/Foundation_Electricity_Infrastructure/MapServer/WFSServer"
    requests_mock.get(url, status_code=404)
    # Run the test and expect an HTTPError
    with caplog.at_level(logging.WARNING):
        template_nodes(
            workbook_table_cache_test_path,
            rez_nodes="attached_to_parent_node",
        )
    assert "Failed to fetch substation coordinates" in caplog.text
    assert "Network node data will be templated without coordinate data" in caplog.text


def test_sub_regions_to_nem_regions_mapping(workbook_table_cache_test_path: Path):
    mapping = template_sub_regions_to_nem_regions_mapping(
        workbook_table_cache_test_path
    )
    assert set(mapping.index) == set(("SQ", "VIC"))
    assert mapping.at["SQ", "nem_region_id"] == "QLD"
    assert mapping.at["VIC", "nem_region_id"] == "VIC"


def test_region_to_single_sub_region_mapping(workbook_table_cache_test_path: Path):
    mapping = template_region_to_single_sub_region_mapping(
        workbook_table_cache_test_path
    )
    assert set(mapping.index) == set(("QLD", "VIC"))
    assert mapping.at["QLD", "isp_sub_region_id"] == "SQ"
    assert mapping.at["VIC", "isp_sub_region_id"] == "VIC"
