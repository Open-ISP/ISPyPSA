"""Optimized tests for create_ispypsa_inputs CLI task.

Tests cover all original functionality but minimize the number of times
create_ispypsa_inputs is run by combining related test scenarios.

Coverage:
- Core functionality: fresh run, up-to-date detection, and triggers (combined)
- CLI flags and dependency chain execution (combined)
- Config path variations (combined)
- Single task mode failure
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
    prepare_test_cache,
    run_cli_command,
    verify_output_files,
)


def test_create_ispypsa_inputs_task(
    mock_config, prepare_test_cache, tmp_path, run_cli_command
):
    """Test fresh run, up-to-date detection, and various triggers.

    Combines:
    - test_fresh_run_creates_all_outputs
    - test_up_to_date_skips_execution
    - test_dependency_modified_triggers_rerun
    - test_missing_some_target_files_triggers_rerun

    Runs: 4 (fresh + up-to-date + dependency mod + missing file)
    """
    # Test fresh run
    cache_dir = tmp_path / "cache"
    assert cache_dir.exists()
    cache_files = list(cache_dir.glob("*.csv"))
    assert len(cache_files) > 50

    # Run command
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0, result.stdout

    # Check outputs created
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
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

    # Test dependency modification trigger
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
    assert new_timestamps != second_run_timestamps
    for filename, old_time in second_run_timestamps.items():
        assert new_timestamps[filename] > old_time

    # Test missing single file trigger
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

    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
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

    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    expected_file_names = list_templater_output_files("sub_regions")
    verify_output_files(output_dir, expected_file_names)


def test_config_path_variations(
    mock_config, prepare_test_cache, tmp_path, run_cli_command
):
    """Test different config path scenarios.

    Combines:
    - test_relative_config_path
    - test_absolute_config_path

    Runs: 2 (relative + absolute)
    """
    # Test 1: Relative config path
    subdir = tmp_path / "configs"
    subdir.mkdir()
    config_name = Path(mock_config).name
    new_config = subdir / config_name
    shutil.copy2(mock_config, new_config)

    with open(new_config, "r") as f:
        config_data = yaml.safe_load(f)

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

    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    expected_file_names = list_templater_output_files("sub_regions")
    verify_output_files(output_dir, expected_file_names)

    # Test 2: Absolute config path (from different working directory)
    # Clean outputs to ensure it runs fresh
    if output_dir.exists():
        shutil.rmtree(output_dir)

    working_dir = tmp_path / "working"
    working_dir.mkdir()

    result = run_cli_command(
        [f"config={mock_config}", "create_ispypsa_inputs"], cwd=str(working_dir)
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
