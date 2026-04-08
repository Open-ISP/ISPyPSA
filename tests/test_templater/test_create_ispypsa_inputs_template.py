from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ispypsa.data_fetch import read_csvs
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    list_templater_output_files,
    load_manually_extracted_tables,
)


def test_create_ispypsa_inputs_template_sub_regions(
    workbook_table_cache_test_path: Path,
):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    manual_tables = load_manually_extracted_tables("6.0")
    template_tables = create_ispypsa_inputs_template(
        "Step Change", "sub_regions", iasr_tables, manual_tables
    )

    for table in list_templater_output_files("sub_regions"):
        assert table in template_tables.keys()

    assert "neregions" not in template_tables.keys()

    assert "sub_region_reference_node" in template_tables["sub_regions"].columns
    assert (
        "sub_region_reference_node_voltage_kv" in template_tables["sub_regions"].columns
    )

    assert "NNSW" in template_tables["flow_paths"]["node_from"].values


def test_create_ispypsa_inputs_template_regions(workbook_table_cache_test_path: Path):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    manual_tables = load_manually_extracted_tables("6.0")
    template_tables = create_ispypsa_inputs_template(
        "Step Change", "nem_regions", iasr_tables, manual_tables
    )

    for table in list_templater_output_files("nem_regions"):
        assert table in template_tables.keys()

    assert "sub_region_reference_node" not in template_tables["sub_regions"].columns
    assert (
        "sub_region_reference_node_voltage_kv"
        not in template_tables["sub_regions"].columns
    )

    assert "NNSW" in template_tables["flow_paths"]["node_from"].values


def test_create_ispypsa_inputs_template_single_regions(
    workbook_table_cache_test_path: Path,
):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    manual_tables = load_manually_extracted_tables("6.0")
    template_tables = create_ispypsa_inputs_template(
        "Step Change", "single_region", iasr_tables, manual_tables
    )

    for table in list_templater_output_files("single_region"):
        assert table in template_tables.keys()

    assert "sub_region_reference_node" not in template_tables["sub_regions"].columns
    assert (
        "sub_region_reference_node_voltage_kv"
        not in template_tables["sub_regions"].columns
    )


# NOTE: The tests above use a disk-based fixture (workbook_table_cache_test_path) and check
# general properties rather than comparing full DataFrames. The test below uses inline
# csv_str_to_df data and a full DataFrame comparison. The inconsistency in approach needs
# to be considered and resolved before the feature flag is removed.
def test_create_ispypsa_inputs_template_new_format(csv_str_to_df):
    sub_regional_reference_nodes = csv_str_to_df("""
        NEM region,  ISP sub-region,                        Sub-regional reference node
        Queensland,  Northern Queensland (NQ),              Ross 275 kV
        New South Wales,  Central New South Wales (CNSW),   Wellington 330 kV
    """)
    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,               NEM region,  ISP sub-region
        Q1,   Far North QLD,      QLD,         NQ
        N3,   Central-West Orana, NSW,         CNSW
    """)
    expected = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        NQ,      subregion,  QLD,        NQ
        CNSW,    subregion,  NSW,        CNSW
        Q1,      rez,        QLD,        NQ
        N3,      rez,        NSW,        CNSW
    """)

    with patch(
        "ispypsa.templater.create_template.FEATURE_FLAGS",
        {"use_new_table_format": True},
    ):
        result = create_ispypsa_inputs_template(
            scenario="Step Change",
            regional_granularity="sub_regions",
            iasr_tables={
                "sub_regional_reference_nodes": sub_regional_reference_nodes,
                "renewable_energy_zones": renewable_energy_zones,
            },
            manually_extracted_tables={},
        )

    assert list(result.keys()) == ["network_geography"]
    pd.testing.assert_frame_equal(
        result["network_geography"].reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_create_ispypsa_inputs_template_new_format_unsupported_granularity():
    with patch(
        "ispypsa.templater.create_template.FEATURE_FLAGS",
        {"use_new_table_format": True},
    ):
        with pytest.raises(NotImplementedError):
            create_ispypsa_inputs_template(
                scenario="Step Change",
                regional_granularity="nem_regions",
                iasr_tables={},
                manually_extracted_tables={},
            )
