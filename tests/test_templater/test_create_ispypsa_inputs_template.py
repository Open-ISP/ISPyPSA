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
from ispypsa.templater.network_expansion import (
    _FLOW_PATH_FORWARD_MW_COL,
    _FLOW_PATH_REVERSE_MW_COL,
)

_FP_AUG_OPTION_COLS = [
    "Flow path",
    "Option name",
    _FLOW_PATH_FORWARD_MW_COL,
    _FLOW_PATH_REVERSE_MW_COL,
]


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
    # SNW is included alongside NQ and CNSW so the parallel-path scenario below
    # (CNSW-SNW corridor) has valid endpoints in the geography.
    sub_regional_reference_nodes = csv_str_to_df("""
        NEM region,       ISP sub-region,                        Sub-regional reference node
        Queensland,       Northern Queensland (NQ),              Ross 275 kV
        New South Wales,  Central New South Wales (CNSW),        Wellington 330 kV
        New South Wales,  Southern New South Wales (SNW),        Lower Tumut 330 kV
    """)
    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,               NEM region,  ISP sub-region
        Q1,   Far North QLD,      QLD,         NQ
        N3,   Central-West Orana, NSW,         CNSW
    """)
    # CNSW-SNW (NTH) and CNSW-SNW (STH) are the two existing siblings of the
    # CNSW-SNW corridor. The augmentation key CNSW-SNW (un-suffixed) below has
    # no exact match here, which triggers _new_parallel_path_rows to inject a
    # new path row.
    flow_path_transfer_capability = csv_str_to_df("""
        Flow Paths,      Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        CQ-NQ,           1200,  1200,  1400,  1440,  1440,  1910
        CNSW-SNW (NTH),  900,   900,   900,   900,   900,   900
        CNSW-SNW (STH),  800,   800,   800,   800,   800,   800
    """)
    initial_transmission_limits = csv_str_to_df("""
        REZ ID,  REZ transmission network limit_Peak demand,  REZ transmission network limit_Summer typical,  REZ transmission network limit_Winter reference
        Q1,      750,  750,  750
    """)
    # Minimal expansion inputs covering one flow path and one REZ state. The extractors
    # tolerate missing path/state tables, so we only supply CQ-NQ and NSW here.
    flow_path_aug_options_cq_nq = pd.DataFrame(
        [("CQ-NQ", "CQ-NQ Option 1", 1000, 1000)],
        columns=_FP_AUG_OPTION_COLS,
    )
    flow_path_aug_costs_cq_nq = csv_str_to_df("""
        Flow path,  Option,          2024-25,  2025-26
        CQ-NQ,      CQ-NQ Option 1,  1000000,  1010000
    """)
    # Augmentation key CNSW-SNW has no exact match in flow_path_transfer_capability
    # — exercises _new_parallel_path_rows end-to-end.
    flow_path_aug_options_cnsw_snw = pd.DataFrame(
        [("CNSW-SNW", "CNSW-SNW Option 1", 1500, 1500)],
        columns=_FP_AUG_OPTION_COLS,
    )
    flow_path_aug_costs_cnsw_snw = csv_str_to_df("""
        Flow path,  Option,             2024-25,  2025-26
        CNSW-SNW,   CNSW-SNW Option 1,  800000,   810000
    """)
    rez_aug_options_nsw = csv_str_to_df("""
        REZ / constraint ID,  Option,    Additional network capacity (MW),  Additional import capacity (MW)
        N3,                   Option 1,  1500,                              1500
    """)
    rez_aug_costs_nsw = csv_str_to_df("""
        REZ / Constraint ID,  Option,    2024-25,  2025-26
        N3,                   Option 1,  750000,   760000
    """)

    connection_cost_forecast_wind_and_solar = csv_str_to_df("""
        REZ__ID,  Scenario,     2024-25
        Q1,       Step__Change, 73000000
    """)
    connection_costs_for_wind_and_solar = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
        Q1,       400
    """)
    efficient_level_of_system_strength_cost = csv_str_to_df("""
        label,  2024-25
        IBR,    10
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
                "flow_path_augmentation_options_CQ-NQ": flow_path_aug_options_cq_nq,
                "flow_path_augmentation_costs_step_change_CQ-NQ": flow_path_aug_costs_cq_nq,
                "flow_path_augmentation_options_CNSW-SNW": flow_path_aug_options_cnsw_snw,
                "flow_path_augmentation_costs_step_change_CNSW-SNW": flow_path_aug_costs_cnsw_snw,
                "rez_augmentation_options_NSW": rez_aug_options_nsw,
                "rez_augmentation_costs_step_change_NSW": rez_aug_costs_nsw,
                "connection_cost_forecast_wind_and_solar": connection_cost_forecast_wind_and_solar,
                "connection_costs_for_wind_and_solar": connection_costs_for_wind_and_solar,
                "efficient_level_of_system_strength_cost": efficient_level_of_system_strength_cost,
            },
            manually_extracted_tables={},
        )

    geography = result["network_geography"]
    assert set(geography.columns) == {"geo_id", "geo_type", "region_id", "subregion_id"}
    # 3 subregions (NQ + CNSW + SNW) + 2 REZs.
    assert len(geography) == 5

    paths = result["network_transmission_paths"]
    assert set(paths.columns) == {"path_id", "geo_from", "geo_to", "carrier"}
    # CQ-NQ + 2 REZ connections (Q1-NQ, N3-CNSW) + CNSW-SNW_NTH + CNSW-SNW_STH
    # + CNSW-SNW (injected by _new_parallel_path_rows).
    assert len(paths) == 6
    # Specifically pin the new parallel-path row — a regression in the
    # _append_new_parallel_paths wiring would fail this assertion.
    assert "CNSW-SNW" in set(paths["path_id"])

    limits = result["network_transmission_path_limits"]
    assert set(limits.columns) == {"path_id", "direction", "timeslice", "capacity"}
    # 3 flow paths × 6 (CQ-NQ + 2 CNSW-SNW siblings) + Q1-NQ × 6 (REZ mirrored)
    # + N3-CNSW × 1 (collapsed, absent from initial_transmission_limits)
    # + CNSW-SNW × 6 zero-capacity rows from _new_parallel_path_rows.
    assert len(limits) == 31

    expansion_options = result["network_expansion_options"]
    assert set(expansion_options.columns) == {
        "expansion_id",
        "expansion_type",
        "allowed_expansion",
        "expansion_option",
    }
    # CQ-NQ + N3-CNSW + CNSW-SNW, each emitted as forward + reverse.
    assert len(expansion_options) == 6

    expansion_costs = result["network_transmission_path_expansion_costs"]
    assert set(expansion_costs.columns) == {"expansion_id", "year", "cost"}
    # 3 expansion_ids x 2 years
    assert len(expansion_costs) == 6

    assert "costs_connection" in result
    costs_connection = result["costs_connection"]
    assert set(costs_connection.columns) == {
        "geo_id",
        "technology",
        "year",
        "connection_cost",
        "system_strength_cost",
    }
    # costs_connection key present with correct columns; currently
    # generators_new_entrant is placeholder (empty) so no VRE rows are produced
    # yet (but no errors either).
    assert costs_connection.empty


def test_create_ispypsa_inputs_template_new_format_nem_regions(csv_str_to_df):
    sub_regional_reference_nodes = csv_str_to_df("""
        NEM region,       ISP sub-region,                  Sub-regional reference node
        Queensland,       Northern Queensland (NQ),        Ross 275 kV
        Queensland,       Central Queensland (CQ),         Stanwell 275 kV
        Queensland,       Southern Queensland (SQ),        South Pine 275 kV
        New South Wales,  Central New South Wales (CNSW),  Wellington 330 kV
        New South Wales,  Northern NSW (NNSW),             Armidale 330 kV
    """)
    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,               NEM region,  ISP sub-region
        Q1,   Far North QLD,      QLD,         NQ
        N3,   Central-West Orana, NSW,         CNSW
    """)
    flow_path_transfer_capability = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        CQ-NQ,       1200,  1200,  1400,  1440,  1440,  1910
        NNSW-SQ,     950,   950,   950,   1450,  1450,  1450
    """)
    initial_transmission_limits = csv_str_to_df("""
        REZ ID,  REZ transmission network limit_Peak demand,  REZ transmission network limit_Summer typical,  REZ transmission network limit_Winter reference
        Q1,      750,  750,  750
    """)
    # Augmentation tables for both an intra-region path (CQ-NQ, dropped) and a
    # cross-region path (NNSW-SQ, re-keyed to NSW-QLD). REZ N3 augmentation
    # exercises automatic REZ→region remapping (N3-CNSW → N3-NSW).
    flow_path_aug_options_cq_nq = pd.DataFrame(
        [("CQ-NQ", "CQ-NQ Option 1", 1000, 1000)],
        columns=_FP_AUG_OPTION_COLS,
    )
    flow_path_aug_options_nnsw_sq = pd.DataFrame(
        [("NNSW-SQ", "NNSW-SQ Option 1", 500, 600)],
        columns=_FP_AUG_OPTION_COLS,
    )
    flow_path_aug_costs_cq_nq = csv_str_to_df("""
        Flow path,  Option,          2024-25,  2025-26
        CQ-NQ,      CQ-NQ Option 1,  1000000,  1010000
    """)
    flow_path_aug_costs_nnsw_sq = csv_str_to_df("""
        Flow path,  Option,            2024-25,  2025-26
        NNSW-SQ,    NNSW-SQ Option 1,  600000,   610000
    """)
    rez_aug_options_nsw = csv_str_to_df("""
        REZ / constraint ID,  Option,    Additional network capacity (MW),  Additional import capacity (MW)
        N3,                   Option 1,  1500,                              1500
    """)
    rez_aug_costs_nsw = csv_str_to_df("""
        REZ / Constraint ID,  Option,    2024-25,  2025-26
        N3,                   Option 1,  750000,   760000
    """)
    connection_cost_forecast_wind_and_solar = csv_str_to_df("""
        REZ__ID,    Scenario,       2024-25
        Q1,         Step__Change,   73000000
    """)
    connection_costs_for_wind_and_solar = csv_str_to_df("""
        REZ__ID,    Connection__capacity__(MVA)
        Q1,         400
    """)
    efficient_level_of_system_strength_cost = csv_str_to_df("""
        label,  2024-25
        IBR,    10
    """)

    with patch(
        "ispypsa.templater.create_template.FEATURE_FLAGS",
        {"use_new_table_format": True},
    ):
        result = create_ispypsa_inputs_template(
            scenario="Step Change",
            regional_granularity="nem_regions",
            iasr_tables={
                "sub_regional_reference_nodes": sub_regional_reference_nodes,
                "renewable_energy_zones": renewable_energy_zones,
                "flow_path_transfer_capability": flow_path_transfer_capability,
                "initial_transmission_limits": initial_transmission_limits,
                "flow_path_augmentation_options_CQ-NQ": flow_path_aug_options_cq_nq,
                "flow_path_augmentation_options_NNSW-SQ": flow_path_aug_options_nnsw_sq,
                "flow_path_augmentation_costs_step_change_CQ-NQ": flow_path_aug_costs_cq_nq,
                "flow_path_augmentation_costs_step_change_NNSW-SQ": flow_path_aug_costs_nnsw_sq,
                "rez_augmentation_options_NSW": rez_aug_options_nsw,
                "rez_augmentation_costs_step_change_NSW": rez_aug_costs_nsw,
                "connection_cost_forecast_wind_and_solar": connection_cost_forecast_wind_and_solar,
                "connection_costs_for_wind_and_solar": connection_costs_for_wind_and_solar,
                "efficient_level_of_system_strength_cost": efficient_level_of_system_strength_cost,
            },
            manually_extracted_tables={},
        )

    geography = result["network_geography"]
    assert set(geography.columns) == {"geo_id", "geo_type", "region_id"}
    assert len(geography) == 4  # 2 unique NEM regions (QLD, NSW) + 2 REZs

    paths = result["network_transmission_paths"]
    assert set(paths.columns) == {"path_id", "geo_from", "geo_to", "carrier"}

    # CQ-NQ (intra-QLD) dropped; NNSW-SQ (cross-region) kept; 2 REZ paths kept.
    assert len(paths) == 3

    limits = result["network_transmission_path_limits"]
    assert set(limits.columns) == {"path_id", "direction", "timeslice", "capacity"}
    # NNSW-SQ -> NSW-QLD: 6 rows. Q1-QLD: 6 rows. N3-NSW: 1 collapsed row.
    assert len(limits) == 13

    expansion_options = result["network_expansion_options"]
    # CQ-NQ augmentation dropped (intra-QLD); NNSW-SQ re-keyed to NSW-QLD;
    # N3 REZ augmentation re-keyed via geography to N3-NSW. Both physical paths
    # emit forward + reverse, so 4 rows total.
    assert set(expansion_options["expansion_id"]) == {"NSW-QLD", "N3-NSW"}
    assert len(expansion_options) == 4

    expansion_costs = result["network_transmission_path_expansion_costs"]
    assert set(expansion_costs["expansion_id"]) == {"NSW-QLD", "N3-NSW"}
    # 2 expansion_ids x 2 years
    assert len(expansion_costs) == 4
    costs_connection = result["costs_connection"]
    assert set(costs_connection.columns) == {
        "geo_id",
        "technology",
        "year",
        "connection_cost",
        "system_strength_cost",
    }
    assert costs_connection.empty


def test_create_ispypsa_inputs_template_new_format_single_region(csv_str_to_df):
    sub_regional_reference_nodes = csv_str_to_df("""
        NEM region,       ISP sub-region,                  Sub-regional reference node
        Queensland,       Northern Queensland (NQ),        Ross 275 kV
        New South Wales,  Central New South Wales (CNSW),  Wellington 330 kV
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
    # CQ-NQ augmentation present in inputs; expected to be dropped entirely at
    # single_region. REZ N3 augmentation should still flow through with
    # expansion_id = N3-NEM.
    flow_path_aug_options_cq_nq = pd.DataFrame(
        [("CQ-NQ", "CQ-NQ Option 1", 1000, 1000)],
        columns=_FP_AUG_OPTION_COLS,
    )
    flow_path_aug_costs_cq_nq = csv_str_to_df("""
        Flow path,  Option,          2024-25,  2025-26
        CQ-NQ,      CQ-NQ Option 1,  1000000,  1010000
    """)
    rez_aug_options_nsw = csv_str_to_df("""
        REZ / constraint ID,  Option,    Additional network capacity (MW),  Additional import capacity (MW)
        N3,                   Option 1,  1500,                              1500
    """)
    rez_aug_costs_nsw = csv_str_to_df("""
        REZ / Constraint ID,  Option,    2024-25,  2025-26
        N3,                   Option 1,  750000,   760000
    """)
    connection_cost_forecast_wind_and_solar = csv_str_to_df("""
        REZ__ID,    Scenario,       2024-25
        Q1,         Step__Change,   73000000
    """)
    connection_costs_for_wind_and_solar = csv_str_to_df("""
        REZ__ID,    Connection__capacity__(MVA)
        Q1,         400
    """)
    efficient_level_of_system_strength_cost = csv_str_to_df("""
        label,  2024-25
        IBR,    10
    """)

    with patch(
        "ispypsa.templater.create_template.FEATURE_FLAGS",
        {"use_new_table_format": True},
    ):
        result = create_ispypsa_inputs_template(
            scenario="Step Change",
            regional_granularity="single_region",
            iasr_tables={
                "sub_regional_reference_nodes": sub_regional_reference_nodes,
                "renewable_energy_zones": renewable_energy_zones,
                "flow_path_transfer_capability": flow_path_transfer_capability,
                "initial_transmission_limits": initial_transmission_limits,
                "flow_path_augmentation_options_CQ-NQ": flow_path_aug_options_cq_nq,
                "flow_path_augmentation_costs_step_change_CQ-NQ": flow_path_aug_costs_cq_nq,
                "rez_augmentation_options_NSW": rez_aug_options_nsw,
                "rez_augmentation_costs_step_change_NSW": rez_aug_costs_nsw,
                "connection_cost_forecast_wind_and_solar": connection_cost_forecast_wind_and_solar,
                "connection_costs_for_wind_and_solar": connection_costs_for_wind_and_solar,
                "efficient_level_of_system_strength_cost": efficient_level_of_system_strength_cost,
            },
            manually_extracted_tables={},
        )

    geography = result["network_geography"]
    assert set(geography.columns) == {"geo_id", "geo_type", "region_id"}
    assert len(geography) == 3  # 1 NEM row + 2 REZs

    paths = result["network_transmission_paths"]
    assert set(paths.columns) == {"path_id", "geo_from", "geo_to", "carrier"}
    # CQ-NQ flow path dropped; only the two REZ-to-NEM paths remain.
    assert len(paths) == 2

    limits = result["network_transmission_path_limits"]
    assert set(limits.columns) == {"path_id", "direction", "timeslice", "capacity"}
    # Q1-NEM: 6 rows. N3-NEM: 1 collapsed row.
    assert len(limits) == 7

    expansion_options = result["network_expansion_options"]
    # CQ-NQ augmentation dropped entirely (no flow paths exist at single_region).
    # N3 REZ augmentation flows through with expansion_id = N3-NEM, emitting
    # forward + reverse rows.
    assert set(expansion_options["expansion_id"]) == {"N3-NEM"}
    assert len(expansion_options) == 2

    expansion_costs = result["network_transmission_path_expansion_costs"]
    assert set(expansion_costs["expansion_id"]) == {"N3-NEM"}
    # 1 expansion_id x 2 years
    assert len(expansion_costs) == 2
    connection_costs = result["costs_connection"]
    assert set(connection_costs.columns) == {
        "geo_id",
        "technology",
        "year",
        "connection_cost",
        "system_strength_cost",
    }
    assert connection_costs.empty
