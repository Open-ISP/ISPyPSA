"""Tests for create_ispypsa_inputs CLI task.

Tests cover:
- Fresh run with no previous outputs
- Up-to-date detection when outputs exist
- Dependency modification detection
- Missing target files scenarios
- Dependency chain execution
- CLI parameter variations
- Path resolution from different directories
"""

import shutil
import time
from pathlib import Path

import pytest
import yaml

from .cli_test_helpers import (
    assert_task_ran,
    assert_task_up_to_date,
    create_config_with_missing_cache,
    get_expected_output_files,
    get_file_timestamps,
    mock_config,
    mock_workbook_file,
    prepare_test_cache,
    run_cli_command,
    verify_output_files,
)


def test_fresh_run_creates_all_outputs(
    mock_config, prepare_test_cache, tmp_path, run_cli_command
):
    """Test running create_ispypsa_inputs with no existing outputs."""
    # The cache is already prepared, so the cache task should be up-to-date

    # Check cache files exist (prepared by prepare_test_cache)
    cache_dir = tmp_path / "cache"
    assert cache_dir.exists()
    cache_files = list(cache_dir.glob("*.csv"))
    assert len(cache_files) > 50  # Should have many CSV files

    # Run command
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])

    # Assert success
    assert result.returncode == 0

    # Check ISPyPSA inputs created
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    assert output_dir.exists()

    # Verify expected output files were created using list_templater_output_files
    expected_file_names = get_expected_output_files(
        "sub_regions"
    )  # Use granularity from config
    verify_output_files(output_dir, expected_file_names)

    # Check log file was created
    log_file = tmp_path / "run_dir" / "test_run" / "ISPyPSA.log"
    assert log_file.exists()

    # Check config was saved
    saved_config = tmp_path / "run_dir" / "test_run" / "test_config.yaml"
    assert saved_config.exists()


def test_up_to_date_skips_execution(
    mock_config, prepare_test_cache, tmp_path, run_cli_command
):
    """Test that task is skipped when outputs exist and deps unchanged."""
    # First run to create outputs
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Run again - should be up-to-date
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Check that task was marked as up-to-date
    assert_task_up_to_date(result.stdout, "create_ispypsa_inputs")

    # Verify no files were modified (timestamps should be the same)
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    first_run_timestamps = get_file_timestamps(output_dir)

    # Sleep briefly to ensure any file writes would have different timestamps
    time.sleep(0.1)

    # Run once more
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Check timestamps haven't changed
    second_run_timestamps = get_file_timestamps(output_dir)
    assert first_run_timestamps == second_run_timestamps


def test_dependency_modified_triggers_rerun(
    mock_config, prepare_test_cache, tmp_path, run_cli_command
):
    """Test that modifying cache files triggers task rerun."""
    # First run
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Get original timestamps
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    original_timestamps = get_file_timestamps(output_dir)

    # Sleep briefly to ensure timestamp difference
    time.sleep(0.1)

    # Modify a cache file by actually changing its content
    cache_file = tmp_path / "cache" / "existing_generators_summary.csv"
    assert cache_file.exists()

    # Read the file and modify it slightly but keep it valid CSV
    import pandas as pd

    df = pd.read_csv(cache_file)
    # Add a new dummy row to actually change the content
    if len(df) > 0:
        # Duplicate the first row to change the file content
        new_row = df.iloc[0].copy()
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    df.to_csv(cache_file, index=False)

    # Run again
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Check task was run (not up-to-date)
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    # Check files were regenerated (new timestamps)
    new_timestamps = get_file_timestamps(output_dir)
    assert new_timestamps != original_timestamps

    # All files should have newer timestamps
    for filename, old_time in original_timestamps.items():
        assert new_timestamps[filename] > old_time


def test_missing_some_target_files_triggers_rerun(
    mock_config, prepare_test_cache, tmp_path, run_cli_command
):
    """Test that deleting some target files triggers task rerun."""
    # First run
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Delete one target file
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    target_file = output_dir / "ecaa_generators.csv"
    assert target_file.exists()
    target_file.unlink()

    # Run again
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Check task was run
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    # Check deleted file was recreated
    assert target_file.exists()
    assert target_file.stat().st_size > 0


def test_missing_all_target_files_triggers_rerun(
    mock_config, prepare_test_cache, tmp_path, run_cli_command
):
    """Test that deleting all target files triggers task rerun."""
    # First run
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Delete entire output directory
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    assert output_dir.exists()
    shutil.rmtree(output_dir)

    # Run again
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Check task was run
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    # Check directory and files were recreated
    expected_file_names = get_expected_output_files("sub_regions")
    verify_output_files(output_dir, expected_file_names)


def test_dependency_chain_execution(
    mock_config, tmp_path, run_cli_command, monkeypatch
):
    """Test that dependencies are automatically run when needed."""
    # Set environment variable to mock cache building in subprocess
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # Ensure clean state - no cache files exist
    cache_dir = tmp_path / "cache"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)

    # Run create_ispypsa_inputs directly (should trigger dependencies)
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Check that both cache and ispypsa_inputs tasks ran
    assert_task_ran(result.stdout, "cache_required_iasr_workbook_tables")
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    # Check outputs were created
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    expected_file_names = get_expected_output_files("sub_regions")
    verify_output_files(output_dir, expected_file_names)


def test_force_execution_with_always_flag(
    mock_config, prepare_test_cache, tmp_path, run_cli_command, monkeypatch
):
    """Test that -a flag forces execution even when up-to-date."""
    # Set environment variable to mock cache building in subprocess
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # First run
    result = run_cli_command([f"config={mock_config}", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Get original timestamps
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    original_timestamps = get_file_timestamps(output_dir)

    time.sleep(0.1)

    # Run with -a flag to force execution
    result = run_cli_command([f"config={mock_config}", "-a", "create_ispypsa_inputs"])
    assert result.returncode == 0

    # Check task was run (not skipped)
    assert_task_ran(result.stdout, "create_ispypsa_inputs")

    # Check files have new timestamps
    new_timestamps = get_file_timestamps(output_dir)
    assert new_timestamps != original_timestamps


def test_single_task_mode_fails_without_deps(mock_config, tmp_path, run_cli_command):
    """Test that -s flag prevents dependency execution and fails when deps missing."""
    # Create config with missing cache directory
    config_path = create_config_with_missing_cache(mock_config, tmp_path)

    # Run with -s flag (single task, no dependencies)
    result = run_cli_command([f"config={config_path}", "create_ispypsa_inputs", "-s"])

    # Should fail because cache files don't exist
    assert result.returncode != 0


def test_relative_config_path(
    mock_config, prepare_test_cache, tmp_path, run_cli_command
):
    """Test using relative path for config file."""
    # Copy config to a subdirectory
    subdir = tmp_path / "configs"
    subdir.mkdir()
    config_name = Path(mock_config).name
    new_config = subdir / config_name
    shutil.copy2(mock_config, new_config)

    # Update paths in config to be relative to subdir
    with open(new_config, "r") as f:
        config_data = yaml.safe_load(f)

    # Make paths relative
    config_data["paths"]["parsed_workbook_cache"] = "../cache"
    config_data["paths"]["workbook_path"] = "../dummy.xlsx"
    config_data["paths"]["run_directory"] = "../run_dir"

    with open(new_config, "w") as f:
        yaml.dump(config_data, f)

    # Run from the subdirectory with relative config path
    result = run_cli_command(
        [f"config={config_name}", "create_ispypsa_inputs"], cwd=str(subdir)
    )
    assert result.returncode == 0

    # Check outputs were created in the right place
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    expected_file_names = get_expected_output_files("sub_regions")
    verify_output_files(output_dir, expected_file_names)


def test_absolute_config_path(
    mock_config, prepare_test_cache, tmp_path, run_cli_command
):
    """Test using absolute path for config file."""
    # Run from a different directory with absolute config path
    working_dir = tmp_path / "working"
    working_dir.mkdir()

    result = run_cli_command(
        [f"config={mock_config}", "create_ispypsa_inputs"], cwd=str(working_dir)
    )
    assert result.returncode == 0

    # Check outputs were created according to config paths
    output_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "tables"
    expected_file_names = get_expected_output_files("sub_regions")
    verify_output_files(output_dir, expected_file_names)
