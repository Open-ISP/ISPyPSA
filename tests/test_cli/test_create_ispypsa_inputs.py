"""Optimized tests for create_ispypsa_inputs CLI task.

Tests cover all original functionality but minimize the number of times
create_ispypsa_inputs is run by combining related test scenarios.

Coverage:
- Core functionality: fresh run, up-to-date detection, and triggers (combined)
- CLI flags and dependency chain execution (combined)
- Config path variations (combined)
- Single task mode failure
- New-format end-to-end row counts and structural relationships (per granularity)
"""

import shutil
import time
from pathlib import Path

import pandas as pd
import pytest
import yaml

from ispypsa.templater import list_templater_output_files

from .cli_test_helpers import (
    assert_task_ran,
    assert_task_up_to_date,
    create_config_with_missing_cache,
    get_file_timestamps,
    mock_config,
    mock_workbook_file,
    modify_config_value,
    prepare_test_cache,
    prepare_test_cache_new_format,
    run_cli_command,
    run_extensive,
    verify_output_files,
)


def test_create_ispypsa_inputs_task(
    mock_config,
    prepare_test_cache,
    tmp_path,
    run_cli_command,
    monkeypatch,
    run_extensive,
):
    """Test fresh run, up-to-date detection, config_changed, and various triggers.

    Combines:
    - test_fresh_run_creates_all_outputs
    - test_up_to_date_skips_execution
    - test_config_changed_irrelevant_does_not_trigger
    - test_config_changed_relevant_triggers_rerun
    - test_dependency_modified_triggers_rerun (extensive only)
    - test_missing_some_target_files_triggers_rerun (extensive only)

    Runs: 4 (fresh + up-to-date + 2 config_changed), or 6 with extensive
    """
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # Test fresh run
    cache_dir = tmp_path / "cache"
    assert cache_dir.exists()
    cache_files = list(cache_dir.glob("*.csv"))
    assert len(cache_files) > 50

    # Run command
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0, result.stdout

    # Check outputs created
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs"
    assert output_dir.exists()
    expected_file_names = list_templater_output_files("sub_regions")
    verify_output_files(output_dir, expected_file_names)

    # Check log and config files
    log_file = tmp_path / "run_dir" / "test_run" / "ISPyPSA.log"
    assert log_file.exists()
    saved_config = tmp_path / "run_dir" / "test_run" / "test_config.yaml"
    assert saved_config.exists()

    # Get timestamps from first run
    first_run_timestamps = get_file_timestamps(output_dir)
    time.sleep(0.1)

    # Test up-to-date detection - second run
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0
    assert_task_up_to_date(result.stdout, "create_ispypsa_inputs")

    # Verify timestamps haven't changed (files weren't regenerated)
    second_run_timestamps = get_file_timestamps(output_dir)
    assert first_run_timestamps == second_run_timestamps

    # Test config_changed: irrelevant change should NOT trigger rerun
    time.sleep(0.1)
    modify_config_value(mock_config, "solver", "gurobi")
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0
    assert_task_up_to_date(result.stdout, "create_ispypsa_inputs")

    # Test config_changed: relevant change SHOULD trigger rerun
    time.sleep(0.1)
    modify_config_value(mock_config, "scenario", "Progressive Change")
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    # Get new timestamps after config change rerun
    config_change_timestamps = get_file_timestamps(output_dir)

    if not run_extensive:
        return

    # Test dependency modification trigger (extensive only)
    time.sleep(0.1)
    cache_file = tmp_path / "cache" / "existing_generators_summary.csv"
    df = pd.read_csv(cache_file)
    if len(df) > 0:
        new_row = df.iloc[0].copy()
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    df.to_csv(cache_file, index=False)

    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    # Verify timestamps changed
    new_timestamps = get_file_timestamps(output_dir)
    assert new_timestamps != config_change_timestamps
    for filename, old_time in config_change_timestamps.items():
        assert new_timestamps[filename] > old_time

    # Test missing single file trigger (extensive only)
    target_file = output_dir / "ecaa_generators.csv"
    target_file.unlink()

    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_ispypsa_inputs")
    assert target_file.exists()
    assert target_file.stat().st_size > 0


def test_cli_flags_and_dependency_chain(
    mock_config, prepare_test_cache, tmp_path, run_cli_command, monkeypatch
):
    """Test CLI flags and dependency chain execution.

    Combines:
    - test_force_execution_with_always_flag
    - test_dependency_chain_execution

    Runs: 3 (initial + force + dependency chain)
    """
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # Test 1: Force execution with -a flag
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs"
    original_timestamps = get_file_timestamps(output_dir)
    time.sleep(0.1)

    result = run_cli_command([f"config={mock_config}", "-a", "create_ispypsa_inputs"])
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    new_timestamps = get_file_timestamps(output_dir)
    assert new_timestamps != original_timestamps

    # Test 2: Dependency chain execution (clean state)
    # Remove everything to test dependency chain
    if (tmp_path / "run_dir").exists():
        shutil.rmtree(tmp_path / "run_dir")
    cache_dir = tmp_path / "cache"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)

    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0
    assert_task_ran(result.stdout, "cache_required_iasr_workbook_tables")
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs"
    expected_file_names = list_templater_output_files("sub_regions")
    verify_output_files(output_dir, expected_file_names)


def test_config_path_variations(
    mock_config,
    prepare_test_cache,
    tmp_path,
    run_cli_command,
    monkeypatch,
    run_extensive,
):
    """Test different config path scenarios.

    Combines:
    - test_relative_config_path
    - test_absolute_config_path

    Runs: 2 (relative + absolute) - extensive only
    """
    if not run_extensive:
        pytest.skip("Skipped unless ISPYPSA_RUN_EXTENSIVE=1")

    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # Test 1: Relative config path
    subdir = tmp_path / "configs"
    subdir.mkdir()
    config_name = Path(mock_config).name
    new_config = subdir / config_name
    shutil.copy2(mock_config, new_config)

    with open(new_config, "r") as f:
        config_data = yaml.safe_load(f)

    config_data["paths"]["parsed_traces_directory"] = str(
        Path(config_data["paths"]["parsed_traces_directory"]).absolute()
    )
    config_data["paths"]["parsed_workbook_cache"] = "../cache"
    config_data["paths"]["workbook_path"] = "../dummy.xlsx"
    config_data["paths"]["run_directory"] = "../run_dir"

    with open(new_config, "w") as f:
        yaml.dump(config_data, f)

    result = run_cli_command(
        [f"config={config_name}", "create_ispypsa_inputs"], cwd=str(subdir)
    )
    assert result.returncode == 0, (
        f"Command failed with return code {result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs"
    expected_file_names = list_templater_output_files("sub_regions")
    verify_output_files(output_dir, expected_file_names)

    # Test 2: Absolute config path (from different working directory)
    # Clean outputs to ensure it runs fresh
    if output_dir.exists():
        shutil.rmtree(output_dir)

    working_dir = tmp_path / "working"
    working_dir.mkdir()

    result = run_cli_command(
        [f"config={new_config.absolute()}", "create_ispypsa_inputs"],
        cwd=str(working_dir),
    )
    assert result.returncode == 0
    verify_output_files(output_dir, expected_file_names)


def test_single_task_mode_fails_without_deps(mock_config, tmp_path, run_cli_command):
    """Test that -s flag prevents dependency execution and fails when deps missing.

    This test runs quickly as it fails immediately.

    Runs: 1 (fails quickly)
    """
    config_path = create_config_with_missing_cache(mock_config, tmp_path)
    result = run_cli_command([f"config={config_path}", "create_ispypsa_inputs", "-s"])
    assert result.returncode != 0


# ── New-format end-to-end row counts ───────────────────────────────────────────
#
# Drives the new-format templater via the CLI for each regional granularity
# against a frozen 7.5 cache committed at tests/test_workbook_table_cache/7.5/.
# Per output table the assertions blend two kinds:
#
# - **Principled** — row counts derived from named entity constants below
#   (e.g. paths = flow paths visible at granularity + REZs + parallel
#   injections), plus referential-integrity invariants (every limit row
#   references a path that exists; every cost row references an option). When
#   these fire, the named constant tells you which structural quantity moved.
# - **Drift-detection** — pinned counts where a clean formula would just be
#   templater logic in disguise. Currently only the expansion-cost row count
#   falls in this bucket (per-option year coverage varies). Refresh by
#   rerunning the test and pasting the failure value into the dict below.
#
# AEMO updates to the 7.5 workbook should force a deliberate review of every
# constant in this block.

# Entity counts derived from the input cache.
_NUM_SUBREGIONS_75 = 15
_NUM_REGIONS_75 = 5
_NUM_REZS_75 = 47
_NUM_FLOW_PATHS_75 = 18

# REZs whose row in `initial_transmission_limits` has all-NaN limit columns.
# Each collapses to a single placeholder row in network_transmission_path_limits
# instead of the usual `NUM_TIMESLICES × NUM_DIRECTIONS` rows.
_NUM_REZS_WITHOUT_LIMITS_75 = 15

# Flow paths in the 7.5 input whose augmentation key has no exact match in
# `flow_path_transfer_capability` and that therefore get a synthetic
# parallel-path row injected. Sub_regions only — at coarser granularities
# the parent path collapses, taking the injection with it.
_NUM_PARALLEL_PATHS_INJECTED_75_SUB_REGIONS = 1

# Flow paths whose endpoints sit in different NEM regions and therefore
# survive the nem_regions collapse. (The remaining 11 of 18 are intra-region.)
_NUM_INTER_REGIONAL_FLOW_PATHS_75 = 7

# REZ-only constraint relaxations (granularity-independent — they live on REZs
# and so survive every granularity collapse). One per REZ that has at least
# one augmentation option, capped at 8 in the 7.5 inputs.
_NUM_REZ_CONSTRAINT_OPTIONS_75 = 8

# Number of distinct flow-path expansion options surviving at each granularity
# (each emits a forward + reverse row in network_expansion_options, so they
# contribute 2× to the row count and 1× to the unique-id count).
_NUM_FLOW_PATH_OPTIONS_AT_GRANULARITY_75 = {
    "sub_regions": 43,
    "nem_regions": 34,
    "single_region": 31,
}

_NUM_TIMESLICES = 3
_NUM_DIRECTIONS = 2
_LIMIT_ROWS_PER_FULL_PATH = _NUM_TIMESLICES * _NUM_DIRECTIONS

_GEOS_PER_GRANULARITY_75 = {
    "sub_regions": _NUM_SUBREGIONS_75,
    "nem_regions": _NUM_REGIONS_75,
    "single_region": 1,
}

_PATHS_PER_GRANULARITY_75 = {
    "sub_regions": (
        _NUM_FLOW_PATHS_75 + _NUM_REZS_75 + _NUM_PARALLEL_PATHS_INJECTED_75_SUB_REGIONS
    ),
    "nem_regions": _NUM_INTER_REGIONAL_FLOW_PATHS_75 + _NUM_REZS_75,
    "single_region": _NUM_REZS_75,
}

# Drift-detection only — per-option year coverage varies.
_EXPECTED_EXPANSION_COST_ROWS_75 = {
    "sub_regions": 1581,
    "nem_regions": 1302,
    "single_region": 1209,
}


def _write_new_format_config(tmp_path, mock_workbook_file, granularity):
    """Write a 7.5 config tuned to drive the new-format templater for `granularity`."""
    config_path = tmp_path / f"new_format_config_{granularity}.yaml"
    config_content = {
        "scenario": "Step Change",
        "wacc": 0.07,
        "discount_rate": 0.05,
        "network": {
            "transmission_expansion": True,
            "rez_transmission_expansion": True,
            "annuitisation_lifetime": 30,
            "nodes": {"regional_granularity": granularity, "rezs": "discrete_nodes"},
            "transmission_expansion_limit_override": None,
            "rez_connection_expansion_limit_override": None,
            "rez_to_sub_region_transmission_default_limit": 1e6,
        },
        "temporal": {
            "year_type": "fy",
            "range": {"start_year": 2025, "end_year": 2026},
            "capacity_expansion": {
                "resolution_min": 30,
                "reference_year_cycle": [2018],
                "investment_periods": [2025],
                "aggregation": {"representative_weeks": [0]},
            },
            "operational": {
                "resolution_min": 30,
                "reference_year_cycle": [2018],
                "horizon": 336,
                "overlap": 48,
                "aggregation": {"representative_weeks": [0]},
            },
        },
        "unserved_energy": {"cost": 10000.0, "max_per_node": 1e5},
        "solver": "highs",
        "iasr_workbook_version": "7.5",
        "paths": {
            "ispypsa_run_name": "test_run",
            "parsed_traces_directory": "tests/trace_data",
            "parsed_workbook_cache": str(tmp_path / "cache"),
            "workbook_path": str(mock_workbook_file),
            "run_directory": str(tmp_path / "run_dir"),
        },
        "trace_data": {"dataset_type": "example", "dateset_year": 2024},
    }
    with open(config_path, "w") as f:
        yaml.dump(config_content, f)
    return config_path


@pytest.mark.parametrize("granularity", ["sub_regions", "nem_regions", "single_region"])
def test_create_ispypsa_inputs_new_format(
    granularity,
    tmp_path,
    mock_workbook_file,
    prepare_test_cache_new_format,
    run_cli_command,
    monkeypatch,
):
    monkeypatch.setenv("ISPYPSA_USE_NEW_TABLE_FORMAT", "true")
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    config_path = _write_new_format_config(tmp_path, mock_workbook_file, granularity)

    result = run_cli_command([f"config={config_path}", "create_ispypsa_inputs"])
    assert result.returncode == 0, (
        f"CLI failed for granularity={granularity}.\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs"
    geo = pd.read_csv(output_dir / "network_geography.csv")
    paths = pd.read_csv(output_dir / "network_transmission_paths.csv")
    limits = pd.read_csv(output_dir / "network_transmission_path_limits.csv")
    options = pd.read_csv(output_dir / "network_expansion_options.csv")
    costs = pd.read_csv(output_dir / "network_transmission_path_expansion_costs.csv")

    # network_geography — one row per (sub-)region or REZ; geo_ids are unique.
    assert len(geo) == _GEOS_PER_GRANULARITY_75[granularity] + _NUM_REZS_75
    assert geo["geo_id"].is_unique

    # network_transmission_paths — derived from named structural quantities:
    # sub_regions = flow paths + REZ connections + parallel-path injections;
    # nem_regions drops intra-region flow paths; single_region drops them all.
    expected_paths = _PATHS_PER_GRANULARITY_75[granularity]
    assert len(paths) == expected_paths
    assert paths["path_id"].is_unique
    # Every geo participates in at least one path (no isolated nodes).
    assert set(paths["geo_from"]) | set(paths["geo_to"]) == set(geo["geo_id"])

    # network_transmission_path_limits — paths with full limit data emit
    # NUM_TIMESLICES × NUM_DIRECTIONS rows; the REZs whose initial-limit row is
    # all-NaN collapse to a single placeholder row.
    expected_limits = (
        expected_paths - _NUM_REZS_WITHOUT_LIMITS_75
    ) * _LIMIT_ROWS_PER_FULL_PATH + _NUM_REZS_WITHOUT_LIMITS_75
    assert len(limits) == expected_limits
    # Every path has at least one limit row; no orphan limits.
    assert set(limits["path_id"]) == set(paths["path_id"])

    # network_expansion_options — each surviving flow-path option emits paired
    # forward + reverse rows sharing one expansion_id; REZ constraint
    # relaxations contribute one row (and one id) each.
    num_fp_options = _NUM_FLOW_PATH_OPTIONS_AT_GRANULARITY_75[granularity]
    assert len(options) == 2 * num_fp_options + _NUM_REZ_CONSTRAINT_OPTIONS_75
    assert (
        options["expansion_id"].nunique()
        == num_fp_options + _NUM_REZ_CONSTRAINT_OPTIONS_75
    )

    # network_transmission_path_expansion_costs — every option has cost rows;
    # no orphan costs. Total row count is drift-only (per-option year coverage
    # is uneven).
    assert set(costs["expansion_id"]) == set(options["expansion_id"])
    assert len(costs) == _EXPECTED_EXPANSION_COST_ROWS_75[granularity]
