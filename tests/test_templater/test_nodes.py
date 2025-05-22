import logging
from pathlib import Path

import pandas as pd

from ispypsa.templater.nodes import _template_regions, _template_sub_regions


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
    # assert not regional_template.substation_longitude.empty
    # assert not regional_template.substation_latitude.empty
    assert len(regional_template.columns) == 4


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
    # assert not sub_regions_template.substation_longitude.empty
    # assert not sub_regions_template.substation_latitude.empty
    assert len(sub_regions_template.columns) == 4


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
    assert len(sub_regions_template.columns) == 4


# def test_substation_coordinate_http_error(
#     workbook_table_cache_test_path: Path, requests_mock, caplog
# ):
#     url = "https://services.ga.gov.au/gis/services/Foundation_Electricity_Infrastructure/MapServer/WFSServer"
#     requests_mock.get(url, status_code=404)
#     # Run the test and expect an HTTPError
#     with caplog.at_level(logging.WARNING):
#         filepath = workbook_table_cache_test_path / Path(
#             "sub_regional_reference_nodes.csv"
#         )
#         sub_regional_reference_nodes = pd.read_csv(filepath)
#         sub_regions_template = _template_sub_regions(sub_regional_reference_nodes)
#     assert "Failed to fetch substation coordinates" in caplog.text
#     assert "Network node data will be templated without coordinate data" in caplog.text
