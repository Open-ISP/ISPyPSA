"""Tests _build_node_to_geography_mapping under the new table format
(use_new_table_format=true), where the regions and zones mapping has a node
column.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into test_helpers.py and
drop the flag fixture.
"""

from unittest.mock import patch

import pytest

from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.results.helpers import _build_node_to_geography_mapping


@pytest.fixture
def new_table_format():
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        yield


def _nem_regions_mapping(csv_str_to_df):
    """The mapping at nem_regions granularity: no node has a sub-region, and
    only N3 is a REZ."""
    return csv_str_to_df("""
        node,  nem_region_id,  isp_sub_region_id,  rez_id
        NSW,   NSW,            ,
        TAS,   TAS,            ,
        N3,    NSW,            ,                   N3
    """)


def test_maps_nodes_to_nem_regions(csv_str_to_df, new_table_format):
    mapping = _nem_regions_mapping(csv_str_to_df)

    result = _build_node_to_geography_mapping(mapping, "nem_region_id")

    assert result == {"NSW": "NSW", "TAS": "TAS", "N3": "NSW"}


def test_leaves_out_nodes_blank_at_the_level(csv_str_to_df, new_table_format):
    mapping = _nem_regions_mapping(csv_str_to_df)

    result = _build_node_to_geography_mapping(mapping, "rez_id")

    # NSW and TAS are not REZs, so they are left out rather than mapped to NaN.
    assert result == {"N3": "N3"}


def test_maps_no_node_to_a_level_blank_in_every_row(csv_str_to_df, new_table_format):
    mapping = _nem_regions_mapping(csv_str_to_df)

    result = _build_node_to_geography_mapping(mapping, "isp_sub_region_id")

    assert result == {}


def test_mapping_read_back_from_csv(csv_str_to_df, tmp_path, new_table_format):
    # At single_region granularity the sub-region column is blank in every
    # row, so read_csvs reads it back as numbers. The plot tasks build their
    # lookups from the saved file.
    mapping = csv_str_to_df("""
        node,  nem_region_id,  isp_sub_region_id,  rez_id
        NEM,   NEM,            ,
        N3,    NEM,            ,                   N3
    """)
    write_csvs({"regions_and_zones_mapping": mapping}, tmp_path)
    saved = read_csvs(tmp_path)["regions_and_zones_mapping"]

    result = {
        level: _build_node_to_geography_mapping(saved, level)
        for level in ["nem_region_id", "isp_sub_region_id", "rez_id"]
    }

    assert result == {
        "nem_region_id": {"NEM": "NEM", "N3": "NEM"},
        "isp_sub_region_id": {},
        "rez_id": {"N3": "N3"},
    }
