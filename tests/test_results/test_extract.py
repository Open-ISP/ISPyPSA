"""Tests for extract module functions."""

from unittest.mock import MagicMock, patch

import pandas as pd

from ispypsa.results.extract import (
    RESULTS_FILES,
    extract_regions_and_zones_mapping,
    extract_tabular_results,
)


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


def test_extract_tabular_results_includes_all_expected_files():
    """Test that RESULTS_FILES includes all expected result types."""
    assert "regions_and_zones_mapping" in RESULTS_FILES
    assert "transmission_expansion" in RESULTS_FILES
    assert "transmission_flows" in RESULTS_FILES
    assert "rez_transmission_flows" in RESULTS_FILES
    assert "isp_sub_region_transmission_flows" in RESULTS_FILES
    assert "nem_region_transmission_flows" in RESULTS_FILES
    assert "generator_dispatch" in RESULTS_FILES
    assert "demand" in RESULTS_FILES


def test_extract_tabular_results(csv_str_to_df):
    """Test extract_tabular_results returns all expected keys."""
    # Setup mock return values for all extract functions
    mock_flows = MagicMock(return_value=pd.DataFrame({"flow": [1, 2, 3]}))
    mock_expansion = MagicMock(return_value=pd.DataFrame({"expansion": [1, 2]}))
    mock_dispatch = MagicMock(return_value=pd.DataFrame({"dispatch": [1]}))
    mock_demand = MagicMock(return_value=pd.DataFrame({"demand": [1]}))
    mock_rez_flows = MagicMock(return_value=pd.DataFrame({"rez_flow": [1]}))
    mock_sub_flows = MagicMock(return_value=pd.DataFrame({"sub_flow": [1]}))
    mock_nem_flows = MagicMock(return_value=pd.DataFrame({"nem_flow": [1]}))

    # Create mock RESULTS_FILES dictionary
    mock_results_files = {
        "regions_and_zones_mapping": MagicMock(),
        "transmission_expansion": mock_expansion,
        "transmission_flows": mock_flows,
        "rez_transmission_flows": mock_rez_flows,
        "isp_sub_region_transmission_flows": mock_sub_flows,
        "nem_region_transmission_flows": mock_nem_flows,
        "generator_dispatch": mock_dispatch,
        "demand": mock_demand,
    }

    # Setup ispypsa_tables with sub_regions for regions_and_zones_mapping
    sub_regions_csv = """
    nem_region_id,  isp_sub_region_id
    NSW1,           CNSW
    """
    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
    }

    # Create mock network
    network = MagicMock()

    # Patch RESULTS_FILES
    with patch.dict(
        "ispypsa.results.extract.RESULTS_FILES",
        mock_results_files,
        clear=True,
    ):
        # Execute
        result = extract_tabular_results(network, ispypsa_tables)

    # Verify all expected keys are present
    assert "regions_and_zones_mapping" in result
    assert "transmission_flows" in result
    assert "transmission_expansion" in result
    assert "generator_dispatch" in result
    assert "demand" in result
    assert "rez_transmission_flows" in result
    assert "isp_sub_region_transmission_flows" in result
    assert "nem_region_transmission_flows" in result
