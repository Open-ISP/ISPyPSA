import pandas as pd
import pytest

from ispypsa.results import extract_regions_and_zones_mapping


def test_extract_regions_and_zones_mapping_basic(csv_str_to_df):
    """Test basic mapping extraction with sub-regions and REZs."""

    # Create test data
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    CNSW,               NSW
    NNSW,               NSW
    VIC,                VIC
    """

    renewable_energy_zones_csv = """
    rez_id,                        isp_sub_region_id
    Central-West__Orana__REZ,      CNSW
    New__England__REZ,             NNSW
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
        "renewable_energy_zones": csv_str_to_df(renewable_energy_zones_csv),
    }

    # Call the function
    result = extract_regions_and_zones_mapping(ispypsa_tables)

    # Expected output
    expected_csv = """
    nem_region_id,  isp_sub_region_id,  rez_id
    NSW,            CNSW,               Central-West__Orana__REZ
    NSW,            NNSW,               New__England__REZ
    VIC,            VIC,                NaN
    """
    expected = csv_str_to_df(expected_csv)

    # Sort both DataFrames for comparison
    result_sorted = result.sort_values(["isp_sub_region_id", "rez_id"]).reset_index(
        drop=True
    )
    expected_sorted = expected.sort_values(["isp_sub_region_id", "rez_id"]).reset_index(
        drop=True
    )

    # Compare
    pd.testing.assert_frame_equal(result_sorted, expected_sorted)


def test_extract_regions_and_zones_mapping_no_rezs(csv_str_to_df):
    """Test mapping extraction when there are no REZs."""

    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    CNSW,               NSW
    VIC,                VIC
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
    }

    # Call the function
    result = extract_regions_and_zones_mapping(ispypsa_tables)

    # Expected output - no rez_id column when there are no REZs
    expected_csv = """
    nem_region_id,  isp_sub_region_id
    NSW,            CNSW
    VIC,            VIC
    """
    expected = csv_str_to_df(expected_csv)

    # Sort both DataFrames for comparison
    result_sorted = result.sort_values("isp_sub_region_id").reset_index(drop=True)
    expected_sorted = expected.sort_values("isp_sub_region_id").reset_index(drop=True)

    # Compare
    pd.testing.assert_frame_equal(result_sorted, expected_sorted)


def test_extract_regions_and_zones_mapping_empty_tables():
    """Test mapping extraction with empty tables."""

    ispypsa_tables = {}

    # Call the function
    result = extract_regions_and_zones_mapping(ispypsa_tables)

    # Should return empty DataFrame with correct columns
    assert result.empty
    assert list(result.columns) == ["nem_region_id", "isp_sub_region_id", "rez_id"]


def test_extract_regions_and_zones_mapping_with_sample_fixture(sample_ispypsa_tables):
    """Test mapping extraction using the sample_ispypsa_tables fixture."""

    # Call the function
    result = extract_regions_and_zones_mapping(sample_ispypsa_tables)

    # Verify the result has the expected columns
    assert list(result.columns) == ["nem_region_id", "isp_sub_region_id", "rez_id"]

    # Verify we have the expected number of rows (2 sub-regions)
    assert len(result) == 2

    # Verify all sub-regions are present
    assert set(result["isp_sub_region_id"]) == {"CNSW", "NNSW"}

    # Verify all REZs are present (note: csv_str_to_df replaces __ with spaces)
    assert set(result["rez_id"].dropna()) == {
        "Central-West Orana REZ",
        "New England REZ",
    }

    # Verify nem_region_id is correct for all rows
    assert all(result["nem_region_id"] == "NSW1")
