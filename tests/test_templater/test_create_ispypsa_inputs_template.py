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


# NOTE: This is an integration test — its job is to verify that create_ispypsa_inputs_template
# routes iasr_tables into the right templater functions and returns each output under the
# right key. We intentionally depart from the assert_frame_equal convention and only check
# presence, column schema, and row count: full DataFrame content is exercised by the
# per-module templater tests.
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
    flow_path_transfer_capability = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        CQ-NQ,       1200,  1200,  1400,  1440,  1440,  1910
    """)
    initial_transmission_limits = csv_str_to_df("""
        REZ ID,  REZ transmission network limit_Peak demand,  REZ transmission network limit_Summer typical,  REZ transmission network limit_Winter reference
        Q1,      750,  750,  750
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
                "flow_path_transfer_capability": flow_path_transfer_capability,
                "initial_transmission_limits": initial_transmission_limits,
            },
            manually_extracted_tables={},
        )

    assert "network_geography" in result
    geography = result["network_geography"]
    assert set(geography.columns) == {"geo_id", "geo_type", "region_id", "subregion_id"}
    assert len(geography) == 4  # 2 subregions + 2 REZs

    assert "network_transmission_paths" in result
    paths = result["network_transmission_paths"]
    assert set(paths.columns) == {"path_id", "geo_from", "geo_to", "carrier"}
    assert len(paths) == 3  # 1 flow path + 2 REZ connections

    assert "network_transmission_path_limits" in result
    limits = result["network_transmission_path_limits"]
    assert set(limits.columns) == {"path_id", "direction", "timeslice", "capacity"}
    # CQ-NQ: 6 (2 directions x 3 timeslices). Q1-NQ: 6 (REZ mirrored to both directions).
    # N3-CNSW: 1 (absent from initial_transmission_limits, collapsed).
    assert len(limits) == 13


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
