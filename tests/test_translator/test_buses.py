import pandas as pd

from ispypsa.translator.buses import (
    _create_single_region_bus,
    _translate_isp_sub_regions_to_buses,
    _translate_nem_regions_to_buses,
    _translate_rezs_to_buses,
)


def test_translate_isp_sub_regions_to_buses():
    isp_sub_regions = pd.DataFrame(
        columns=["isp_sub_region_id", "nem_region_id"],
        data=[["CNSW", "NSW"], ["SNSW", "NSW"]],
    )
    expected_buses = pd.DataFrame(
        columns=["name"], data=[["CNSW"], ["SNSW"]]
    ).set_index("name")
    buses = _translate_isp_sub_regions_to_buses(isp_sub_regions)
    pd.testing.assert_frame_equal(buses, expected_buses)


def test_translate_nem_regions_to_buses():
    nem_regions = pd.DataFrame(
        columns=["nem_region_id", "isp_sub_region_id"],
        data=[["NSW", "CNSW"], ["VIC", "VIC"]],
    )
    expected_buses = pd.DataFrame(columns=["name"], data=[["NSW"], ["VIC"]]).set_index(
        "name"
    )
    buses = _translate_nem_regions_to_buses(nem_regions)
    pd.testing.assert_frame_equal(buses, expected_buses)


def test_create_single_region_bus():
    expected_buses = pd.DataFrame(columns=["name"], data=[["NEM"]]).set_index("name")
    buses = _create_single_region_bus()
    pd.testing.assert_frame_equal(buses, expected_buses)


def test_translate_rezs_to_buses():
    rezs = pd.DataFrame(
        columns=["rez_id", "isp_sub_region_id"], data=[["X", "CNSW"], ["Y", "SNSW"]]
    )
    expected_buses = pd.DataFrame(columns=["name"], data=[["X"], ["Y"]]).set_index(
        "name"
    )
    buses = _translate_rezs_to_buses(rezs)
    pd.testing.assert_frame_equal(buses, expected_buses)
