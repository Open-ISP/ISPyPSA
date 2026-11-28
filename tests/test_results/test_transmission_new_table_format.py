"""Tests the transmission results under the new table format
(use_new_table_format=true): the geographic flows with the regions and zones
mapping built from network_geography.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into test_transmission.py
and drop the flag fixture.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from ispypsa.results.extract import extract_regions_and_zones_mapping
from ispypsa.results.transmission import (
    extract_isp_sub_region_transmission_flows,
    extract_nem_region_transmission_flows,
    extract_rez_transmission_flows,
)

_GEOGRAPHIC_FLOW_COLUMNS = (
    "investment_period,  timestep,  imports_mw,  exports_mw,  net_imports_mw"
)


@pytest.fixture
def new_table_format():
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        yield


# --- geographic flows with the new-format mapping ---


def _geographic_flows(link_flows: pd.DataFrame, ispypsa_tables: dict) -> dict:
    mapping = extract_regions_and_zones_mapping(ispypsa_tables)
    return {
        "nem_region": extract_nem_region_transmission_flows(link_flows, mapping),
        "sub_region": extract_isp_sub_region_transmission_flows(link_flows, mapping),
        "rez": extract_rez_transmission_flows(link_flows, mapping),
    }


def test_geographic_flows_at_sub_regions_granularity(csv_str_to_df, new_table_format):
    ispypsa_tables = {
        "network_geography": csv_str_to_df("""
            geo_id,  geo_type,   region_id,  subregion_id
            CNSW,    subregion,  NSW,        CNSW
            NNSW,    subregion,  NSW,        NNSW
            SQ,      subregion,  QLD,        SQ
            N3,      rez,        NSW,        CNSW
        """),
    }
    link_flows = csv_str_to_df("""
        investment_period,  timestep,  from_node,  to_node,  flow_mw,  isp_name
        2025,               0,         CNSW,       NNSW,     100,      CNSW-NNSW
        2025,               0,         NNSW,       SQ,       40,       NNSW-SQ
        2025,               0,         N3,         CNSW,     300,      N3-CNSW
    """)

    result = _geographic_flows(link_flows, ispypsa_tables)

    # Only NNSW-SQ crosses a region boundary.
    expected = csv_str_to_df(f"""
        nem_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        NSW,            2025,  0,  0,  40,  -40
        QLD,            2025,  0,  40, 0,   40
    """)
    pd.testing.assert_frame_equal(result["nem_region"], expected)
    # N3-CNSW is inside CNSW.
    expected = csv_str_to_df(f"""
        isp_sub_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        CNSW,               2025,  0,  0,    100,  -100
        NNSW,               2025,  0,  100,  40,   60
        SQ,                 2025,  0,  40,   0,    40
    """)
    pd.testing.assert_frame_equal(result["sub_region"], expected)
    expected = csv_str_to_df(f"""
        rez_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        N3,      2025,  0,  0,  300,  -300
    """)
    pd.testing.assert_frame_equal(result["rez"], expected)


def test_geographic_flows_at_nem_regions_granularity(csv_str_to_df, new_table_format):
    ispypsa_tables = {
        "network_geography": csv_str_to_df("""
            geo_id,  geo_type,  region_id
            NSW,     region,    NSW
            QLD,     region,    QLD
            N3,      rez,       NSW
        """),
    }
    link_flows = csv_str_to_df("""
        investment_period,  timestep,  from_node,  to_node,  flow_mw,  isp_name
        2025,               0,         NSW,        QLD,      40,       NSW-QLD
        2025,               0,         N3,         NSW,      300,      N3-NSW
    """)

    result = _geographic_flows(link_flows, ispypsa_tables)

    # N3-NSW is inside NSW.
    expected = csv_str_to_df(f"""
        nem_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        NSW,            2025,  0,  0,  40,  -40
        QLD,            2025,  0,  40, 0,   40
    """)
    pd.testing.assert_frame_equal(result["nem_region"], expected)
    # The model has no sub-region nodes, so there are no sub-region flows.
    expected = csv_str_to_df(f"""
        isp_sub_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
    """)
    pd.testing.assert_frame_equal(result["sub_region"], expected, check_dtype=False)
    expected = csv_str_to_df(f"""
        rez_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        N3,      2025,  0,  0,  300,  -300
    """)
    pd.testing.assert_frame_equal(result["rez"], expected)


def test_geographic_flows_at_single_region_granularity(csv_str_to_df, new_table_format):
    ispypsa_tables = {
        "network_geography": csv_str_to_df("""
            geo_id,  geo_type,  region_id
            NEM,     region,    NEM
            N3,      rez,       NEM
        """),
    }
    link_flows = csv_str_to_df("""
        investment_period,  timestep,  from_node,  to_node,  flow_mw,  isp_name
        2025,               0,         N3,         NEM,      300,      N3-NEM
    """)

    result = _geographic_flows(link_flows, ispypsa_tables)

    # Both ends are in the one region, NEM, and there are no sub-regions.
    expected = csv_str_to_df(f"""
        nem_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
    """)
    pd.testing.assert_frame_equal(result["nem_region"], expected, check_dtype=False)
    expected = csv_str_to_df(f"""
        isp_sub_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
    """)
    pd.testing.assert_frame_equal(result["sub_region"], expected, check_dtype=False)
    expected = csv_str_to_df(f"""
        rez_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        N3,      2025,  0,  0,  300,  -300
    """)
    pd.testing.assert_frame_equal(result["rez"], expected)
