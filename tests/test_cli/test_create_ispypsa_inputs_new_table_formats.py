"""End-to-end CLI tests for the new-format templater (use_new_table_format=true).

Mirrors test_create_ispypsa_inputs.py for the new format. Lives separately so
that when the feature flag goes away and 6.0 is dropped, the test
restructuring becomes a sequence of file operations rather than a diff inside
test bodies.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge this file into the sibling
test_create_ispypsa_inputs.py and delete it.

Coverage:
- Mechanism: fresh run, up-to-date detection, config_changed (irrelevant +
  relevant), plus the extensive-only dependency-modified + missing-target-file
  triggers.
- Content: per-granularity row counts and structural relationships against a
  frozen 7.5 cache.

Handover steps (when 6.0 support is dropped):

1. Port the format-agnostic tests from the legacy file. None of these have
   new-format coverage yet; all three need to be rewritten here, using
   mock_config_new_format / prepare_test_cache_new_format in place of their
   6.0 equivalents:
   - test_cli_flags_and_dependency_chain (covers cache + run_dir rebuild
     from scratch — a code path nothing in this file currently exercises)
   - test_single_task_mode_fails_without_deps (`-s` flag failure semantics)
   - test_config_path_variations (relative + absolute config path handling,
     extensive-only)

2. Delete the legacy file and rename this one:
       git rm tests/test_cli/test_create_ispypsa_inputs.py
       git mv tests/test_cli/test_create_ispypsa_inputs_new_table_formats.py \\
              tests/test_cli/test_create_ispypsa_inputs.py

3. Collapse the helpers. cli_test_helpers.py keeps the format-agnostic
   infrastructure (run_cli_command, assert_task_ran, build_mock_config, the
   shared cache-prep helper, etc.); the 6.0-only fixtures are removed and the
   new-format ones move in under their unsuffixed names:
   - delete `mock_config` and `prepare_test_cache` from cli_test_helpers.py
   - move `mock_config_new_format` → `mock_config` and update
     `build_mock_config` to default to version="7.5"
   - move `prepare_test_cache_new_format` → `prepare_test_cache`
   - git rm cli_test_helpers_new_table_formats.py
   - update fixture references in this file accordingly
"""

import time

import pandas as pd
import pytest

from .cli_test_helpers import (
    assert_task_ran,
    assert_task_up_to_date,
    build_mock_config,
    get_file_timestamps,
    mock_workbook_file,
    modify_config_value,
    run_cli_command,
    run_extensive,
    verify_output_files,
)
from .cli_test_helpers_new_table_formats import (
    mock_config_new_format,
    prepare_test_cache_new_format,
)

_NEW_FORMAT_OUTPUTS = [
    "network_geography",
    "network_transmission_paths",
    "network_transmission_path_limits",
    "network_expansion_options",
    "network_transmission_path_expansion_costs",
    "costs_connection",
]

# Custom constraints are templated only at sub_regions (coarser granularities
# collapse the entities they reference). Detailed content lives in
# test_custom_constraints_from_plexos.py; here we check the CLI writes them at
# sub_regions and omits them otherwise.
_CUSTOM_CONSTRAINT_OUTPUTS = [
    "custom_constraints",
    "custom_constraints_lhs",
    "custom_constraints_rhs",
]


def test_create_ispypsa_inputs_task_new_format(
    mock_config_new_format,
    prepare_test_cache_new_format,
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
    monkeypatch.setenv("ISPYPSA_USE_NEW_TABLE_FORMAT", "true")
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # Test fresh run
    cache_dir = tmp_path / "cache"
    assert cache_dir.exists()
    cache_files = list(cache_dir.glob("*.csv"))
    assert len(cache_files) > 50

    # Run command
    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_ispypsa_inputs"]
    )
    assert result.returncode == 0, result.stdout

    # Check outputs created
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs"
    assert output_dir.exists()
    verify_output_files(output_dir, _NEW_FORMAT_OUTPUTS)

    # Check log and config files
    log_file = tmp_path / "run_dir" / "test_run" / "ISPyPSA.log"
    assert log_file.exists()
    saved_config = tmp_path / "run_dir" / "test_run" / "test_config.yaml"
    assert saved_config.exists()

    # Get timestamps from first run
    first_run_timestamps = get_file_timestamps(output_dir)
    time.sleep(0.1)

    # Test up-to-date detection - second run
    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_ispypsa_inputs"]
    )
    assert result.returncode == 0
    assert_task_up_to_date(result.stdout, "create_ispypsa_inputs")

    # Verify timestamps haven't changed (files weren't regenerated)
    second_run_timestamps = get_file_timestamps(output_dir)
    assert first_run_timestamps == second_run_timestamps

    # Test config_changed: irrelevant change should NOT trigger rerun
    time.sleep(0.1)
    modify_config_value(mock_config_new_format, "solver", "gurobi")
    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_ispypsa_inputs"]
    )
    assert result.returncode == 0
    assert_task_up_to_date(result.stdout, "create_ispypsa_inputs")

    # Test config_changed: relevant change SHOULD trigger rerun. Switching
    # regional_granularity is watched in the new-format templater branch.
    time.sleep(0.1)
    modify_config_value(
        mock_config_new_format, "network.nodes.regional_granularity", "nem_regions"
    )
    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_ispypsa_inputs"]
    )
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    # Get new timestamps after config change rerun
    config_change_timestamps = get_file_timestamps(output_dir)

    if not run_extensive:
        return

    # Test dependency modification trigger (extensive only)
    time.sleep(0.1)
    cache_file = tmp_path / "cache" / "sub_regional_reference_nodes.csv"
    df = pd.read_csv(cache_file)
    if len(df) > 0:
        new_row = df.iloc[0].copy()
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    df.to_csv(cache_file, index=False)

    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_ispypsa_inputs"]
    )
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    # Verify timestamps changed
    new_timestamps = get_file_timestamps(output_dir)
    assert new_timestamps != config_change_timestamps
    for filename, old_time in config_change_timestamps.items():
        assert new_timestamps[filename] > old_time

    # Test missing single file trigger (extensive only)
    target_file = output_dir / "network_geography.csv"
    target_file.unlink()

    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_ispypsa_inputs"]
    )
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_ispypsa_inputs")
    assert target_file.exists()
    assert target_file.stat().st_size > 0


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

    config_path = build_mock_config(
        tmp_path, mock_workbook_file, version="7.5", granularity=granularity
    )

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

    # custom_constraints — written only at sub_regions. Detailed content is
    # covered by test_custom_constraints_from_plexos.py; here we assert the CLI
    # emits the three tables at sub_regions with no orphan LHS/RHS rows, and
    # omits them entirely at coarser granularities.
    if granularity == "sub_regions":
        verify_output_files(output_dir, _CUSTOM_CONSTRAINT_OUTPUTS)
        constraints = pd.read_csv(output_dir / "custom_constraints.csv")
        lhs = pd.read_csv(output_dir / "custom_constraints_lhs.csv")
        rhs = pd.read_csv(output_dir / "custom_constraints_rhs.csv")
        constraint_ids = set(constraints["constraint_id"])
        assert set(lhs["constraint_id"]) <= constraint_ids
        assert set(rhs["constraint_id"]) <= constraint_ids
    else:
        for name in _CUSTOM_CONSTRAINT_OUTPUTS:
            assert not (output_dir / f"{name}.csv").exists()
