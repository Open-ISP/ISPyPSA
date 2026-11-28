"""Tests extract_regions_and_zones_mapping under the new table format
(use_new_table_format=true), where it is built from network_geography.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into test_extract.py and
drop the flag fixture.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from ispypsa.results.extract import extract_regions_and_zones_mapping


@pytest.fixture
def new_table_format():
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        yield


def test_mapping_at_sub_regions_granularity(csv_str_to_df, new_table_format):
    # REZs are in network_geography whether or not they are discrete nodes,
    # so they always have a row.
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        NNSW,    subregion,  NSW,        NNSW
        TAS,     subregion,  TAS,        TAS
        N3,      rez,        NSW,        CNSW
    """)

    result = extract_regions_and_zones_mapping({"network_geography": network_geography})

    expected = csv_str_to_df("""
        node,  nem_region_id,  isp_sub_region_id,  rez_id
        CNSW,  NSW,            CNSW,
        NNSW,  NSW,            NNSW,
        TAS,   TAS,            TAS,
        N3,    NSW,            CNSW,               N3
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_mapping_at_nem_regions_granularity(csv_str_to_df, new_table_format):
    # network_geography has no subregion_id column at this granularity, so no
    # node has a sub-region, TAS included.
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,  region_id
        NSW,     region,    NSW
        TAS,     region,    TAS
        N3,      rez,       NSW
    """)

    result = extract_regions_and_zones_mapping({"network_geography": network_geography})

    expected = csv_str_to_df("""
        node,  nem_region_id,  isp_sub_region_id,  rez_id
        NSW,   NSW,            ,
        TAS,   TAS,            ,
        N3,    NSW,            ,                   N3
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_mapping_at_single_region_granularity(csv_str_to_df, new_table_format):
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,  region_id
        NEM,     region,    NEM
        N3,      rez,       NEM
    """)

    result = extract_regions_and_zones_mapping({"network_geography": network_geography})

    # The region_id of every node is NEM, which is not a region to total by,
    # so no node has a region.
    expected = csv_str_to_df("""
        node,  nem_region_id,  isp_sub_region_id,  rez_id
        NEM,   ,               ,
        N3,    ,               ,                   N3
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_mapping_of_a_region_filtered_template(csv_str_to_df, new_table_format):
    # Filtered by filter_by_isp_sub_regions to CNSW: network_geography keeps
    # CNSW and its REZ.
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        N3,      rez,        NSW,        CNSW
    """)

    result = extract_regions_and_zones_mapping({"network_geography": network_geography})

    expected = csv_str_to_df("""
        node,  nem_region_id,  isp_sub_region_id,  rez_id
        CNSW,  NSW,            CNSW,
        N3,    NSW,            CNSW,               N3
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_mapping_with_an_empty_network_geography(csv_str_to_df, new_table_format):
    network_geography = pd.DataFrame(columns=["geo_id", "geo_type", "region_id"])

    result = extract_regions_and_zones_mapping({"network_geography": network_geography})

    expected = csv_str_to_df("""
        node,  nem_region_id,  isp_sub_region_id,  rez_id
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
