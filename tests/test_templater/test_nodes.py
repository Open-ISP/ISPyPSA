import logging
from pathlib import Path

import pandas as pd
import pytest
import requests

from ispypsa.templater.nodes import (
    _get_reference_node_locations,
    _request_transmission_substation_coordinates,
    _template_regions,
    _template_sub_regions,
)


def test_node_templater_nem_regions(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path("regional_reference_nodes.csv")
    regional_reference_nodes = pd.read_csv(filepath)
    regional_template = _template_regions(regional_reference_nodes)
    assert set(regional_template.nem_region_id) == set(("QLD", "VIC"))
    assert set(regional_template.isp_sub_region_id) == set(("SQ", "VIC"))
    assert set(regional_template.regional_reference_node) == set(
        ("Prominent Hill", "Barcaldine")
    )
    assert set(regional_template.regional_reference_node_voltage_kv) == set((132,))
    # When coordinates are available, we have 6 columns, otherwise 4
    assert len(regional_template.columns) in (4, 6)


def test_templater_sub_regions(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path("sub_regional_reference_nodes.csv")
    sub_regional_reference_nodes = pd.read_csv(filepath)
    sub_regions_template = _template_sub_regions(sub_regional_reference_nodes)
    assert set(sub_regions_template.isp_sub_region_id) == set(("SQ", "VIC"))
    assert set(sub_regions_template.nem_region_id) == set(("QLD", "VIC"))
    assert set(sub_regions_template.sub_region_reference_node) == set(
        ("Prominent Hill", "Barcaldine")
    )
    assert set(sub_regions_template.sub_region_reference_node_voltage_kv) == set((132,))
    # When coordinates are available, we have 6 columns, otherwise 4
    assert len(sub_regions_template.columns) in (4, 6)


def test_templater_sub_regions_mapping_only(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path("sub_regional_reference_nodes.csv")
    sub_regional_reference_nodes = pd.read_csv(filepath)
    sub_regions_template = _template_sub_regions(
        sub_regional_reference_nodes, mapping_only=True
    )
    assert set(sub_regions_template.isp_sub_region_id) == set(("SQ", "VIC"))
    assert set(sub_regions_template.nem_region_id) == set(("QLD", "VIC"))
    assert len(sub_regions_template.columns) == 2


def test_no_substation_coordinates(workbook_table_cache_test_path: Path, mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        "ispypsa.templater.nodes._request_transmission_substation_coordinates",
        return_value=pd.DataFrame(({})).T,
    )
    filepath = workbook_table_cache_test_path / Path("sub_regional_reference_nodes.csv")
    sub_regional_reference_nodes = pd.read_csv(filepath)
    sub_regions_template = _template_sub_regions(sub_regional_reference_nodes)
    assert set(sub_regions_template.isp_sub_region_id) == set(("SQ", "VIC"))
    assert set(sub_regions_template.nem_region_id) == set(("QLD", "VIC"))
    assert set(sub_regions_template.sub_region_reference_node) == set(
        ("Prominent Hill", "Barcaldine")
    )
    assert set(sub_regions_template.sub_region_reference_node_voltage_kv) == set((132,))
    # When coordinates are available, we have 6 columns, otherwise 4
    assert len(sub_regions_template.columns) in (4, 6)


def test_substation_coordinate_http_error(requests_mock, caplog):
    url = "https://services.ga.gov.au/gis/services/National_Electricity_Infrastructure/MapServer/WFSServer"
    requests_mock.get(url, status_code=404)

    with caplog.at_level(logging.WARNING):
        result = _request_transmission_substation_coordinates()

    assert (
        "Failed to fetch substation coordinates. HTTP Status code: 404." in caplog.text
    )
    assert result.empty


def test_substation_coordinate_request_exception(mocker, caplog):
    mocker.patch(
        "requests.get",
        side_effect=requests.exceptions.RequestException("Connection error"),
    )

    with caplog.at_level(logging.ERROR):
        result = _request_transmission_substation_coordinates()

    assert (
        "Error requesting substation coordinate data:\nConnection error." in caplog.text
    )
    assert result.empty


def test_substation_coordinate_empty_result_warning(mocker, caplog):
    # Mock a successful request that returns data with no valid coordinates
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    # Multiple members to ensure xmltodict returns a list
    mock_response.content = b"""<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" xmlns:esri="http://www.esri.com" xmlns:gml="http://www.opengis.net/gml">
        <wfs:member>
            <esri:Electricity_Transmission_Substations>
                <esri:SUBSTATION_NAME>Test1</esri:SUBSTATION_NAME>
            </esri:Electricity_Transmission_Substations>
        </wfs:member>
        <wfs:member>
            <esri:Electricity_Transmission_Substations>
                <esri:SUBSTATION_NAME></esri:SUBSTATION_NAME>
            </esri:Electricity_Transmission_Substations>
        </wfs:member>
    </wfs:FeatureCollection>"""
    mocker.patch("requests.get", return_value=mock_response)

    with caplog.at_level(logging.WARNING):
        result = _request_transmission_substation_coordinates()

    assert "Could not get substation coordinate data." in caplog.text
    assert "Network node data will be templated without coordinate data." in caplog.text
    assert result.empty


def test_get_reference_node_locations_with_coordinates(mocker):
    # Test the function with valid substation coordinates
    reference_nodes = pd.DataFrame(
        {
            "sub_region_reference_node": ["Test Node 1", "Test Node 2"],
            "other_column": ["A", "B"],
        }
    )

    substation_coords = pd.DataFrame(
        {
            "substation_latitude": [-35.0, -34.0],
            "substation_longitude": [150.0, 151.0],
        },
        index=["Test Node 1", "Test Node 2"],
    )

    mocker.patch(
        "ispypsa.templater.nodes._request_transmission_substation_coordinates",
        return_value=substation_coords,
    )

    result = _get_reference_node_locations(reference_nodes)

    assert "substation_latitude" in result.columns
    assert "substation_longitude" in result.columns
    assert len(result) == 2
    assert result["substation_latitude"].iloc[0] == -35.0
    assert result["substation_longitude"].iloc[0] == 150.0


def test_get_reference_node_locations_without_coordinates(mocker):
    # Test the function when no coordinates are available
    reference_nodes = pd.DataFrame(
        {
            "reference_node": ["Test Node 1", "Test Node 2"],
            "other_column": ["A", "B"],
        }
    )

    mocker.patch(
        "ispypsa.templater.nodes._request_transmission_substation_coordinates",
        return_value=pd.DataFrame(),
    )

    result = _get_reference_node_locations(reference_nodes)

    # Should return the same dataframe without coordinate columns
    assert "substation_latitude" not in result.columns
    assert "substation_longitude" not in result.columns
    assert len(result) == 2
    assert list(result.columns) == list(reference_nodes.columns)
