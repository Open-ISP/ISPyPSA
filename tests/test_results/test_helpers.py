import pytest

from ispypsa.results.helpers import _build_node_to_geography_mapping


def test_build_node_to_geography_mapping(csv_str_to_df):
    """Test node to geography mapping for regions, subregions, and REZs."""

    # 1. Prepare Input Data
    mapping_csv = """
    nem_region_id, isp_sub_region_id, rez_id
    NSW1,          CNSW,              N1
    NSW1,          NNSW,              N2
    QLD1,          SEQ,
    """
    # Note: Empty value for SEQ's rez_id is handled as NaN by csv_str_to_df/pandas
    mapping_df = csv_str_to_df(mapping_csv)

    # 2. Test Region Mapping
    # Should map subregions -> regions AND REZs -> regions
    result_region = _build_node_to_geography_mapping(mapping_df, "nem_region_id")
    expected_region = {
        "CNSW": "NSW1",
        "NNSW": "NSW1",
        "SEQ": "QLD1",
        "N1": "NSW1",
        "N2": "NSW1",
    }
    assert result_region == expected_region

    # 3. Test Subregion Mapping
    # Should map subregions -> subregions AND REZs -> subregions
    result_subregion = _build_node_to_geography_mapping(mapping_df, "isp_sub_region_id")
    expected_subregion = {
        "CNSW": "CNSW",
        "NNSW": "NNSW",
        "SEQ": "SEQ",
        "N1": "CNSW",
        "N2": "NNSW",
    }
    assert result_subregion == expected_subregion

    # 4. Test REZ Mapping
    # Should map REZs -> REZs only
    result_rez = _build_node_to_geography_mapping(mapping_df, "rez_id")
    expected_rez = {"N1": "N1", "N2": "N2"}
    assert result_rez == expected_rez

    # 5. Test Invalid Geography Level
    with pytest.raises(ValueError, match="Unknown geography_level"):
        _build_node_to_geography_mapping(mapping_df, "invalid_level")


def test_build_node_to_geography_mapping_rez_without_rez_column(csv_str_to_df):
    """Test REZ mapping when rez_id column doesn't exist returns empty dict."""
    # Mapping without rez_id column
    mapping_csv = """
    nem_region_id, isp_sub_region_id
    NSW1,          CNSW
    NSW1,          NNSW
    QLD1,          SEQ
    """
    mapping_df = csv_str_to_df(mapping_csv)

    # When requesting rez_id mapping but column doesn't exist, should return empty dict
    result = _build_node_to_geography_mapping(mapping_df, "rez_id")
    assert result == {}
