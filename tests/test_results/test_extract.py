"""Tests for extract module functions."""

import pandas as pd

from ispypsa.results.extract import extract_regions_and_zones_mapping


def test_extract_regions_and_zones_mapping_no_sub_regions():
    """Test extraction when sub_regions table is not present."""
    ispypsa_tables = {}

    result = extract_regions_and_zones_mapping(ispypsa_tables)

    assert result.empty
    assert list(result.columns) == ["nem_region_id", "isp_sub_region_id", "rez_id"]


def test_extract_regions_and_zones_mapping_without_rez(csv_str_to_df):
    """Test extraction when only sub_regions is present (no REZ table)."""
    sub_regions_csv = """
    nem_region_id,  isp_sub_region_id
    NSW1,           CNSW
    NSW1,           NNSW
    QLD1,           SEQ
    VIC1,           CVIC
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
    }

    result = extract_regions_and_zones_mapping(ispypsa_tables)

    expected_csv = """
    nem_region_id,  isp_sub_region_id
    NSW1,           CNSW
    NSW1,           NNSW
    QLD1,           SEQ
    VIC1,           CVIC
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(["nem_region_id", "isp_sub_region_id"]).reset_index(
            drop=True
        ),
        expected.sort_values(["nem_region_id", "isp_sub_region_id"]).reset_index(
            drop=True
        ),
    )


def test_extract_regions_and_zones_mapping_with_rez(csv_str_to_df):
    """Test extraction when both sub_regions and renewable_energy_zones are present."""
    sub_regions_csv = """
    nem_region_id,  isp_sub_region_id
    NSW1,           CNSW
    NSW1,           NNSW
    QLD1,           SEQ
    VIC1,           CVIC
    """

    rez_csv = """
    rez_id,  isp_sub_region_id
    N1,      CNSW
    N2,      CNSW
    N3,      NNSW
    Q1,      SEQ
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
        "renewable_energy_zones": csv_str_to_df(rez_csv),
    }

    result = extract_regions_and_zones_mapping(ispypsa_tables)

    # Expected: sub-regions with their REZs merged in
    # CNSW has N1 and N2, NNSW has N3, SEQ has Q1, CVIC has no REZ (NaN)
    expected_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW1,           CNSW,               N1
    NSW1,           CNSW,               N2
    NSW1,           NNSW,               N3
    QLD1,           SEQ,                Q1
    VIC1,           CVIC,               NaN
    """
    expected = csv_str_to_df(expected_csv)

    pd.testing.assert_frame_equal(
        result.sort_values(
            ["nem_region_id", "isp_sub_region_id", "rez_id"]
        ).reset_index(drop=True),
        expected.sort_values(
            ["nem_region_id", "isp_sub_region_id", "rez_id"]
        ).reset_index(drop=True),
    )
