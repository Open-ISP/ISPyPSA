"""Tests for create_and_run_operational_model CLI task.

Tests core functionality: fresh run, up-to-date detection, and triggers using
the optimized testing pattern that minimizes the number of task runs.

Optimization: Combines related test scenarios into a single test to reduce
subprocess overhead and total execution time.
"""

import time
from pathlib import Path

import pandas as pd
import pytest

from .cli_test_helpers import (
    assert_task_ran,
    assert_task_up_to_date,
    get_file_timestamps,
    mock_config,
    mock_workbook_file,
    modify_config_value,
    prepare_test_cache,
    run_cli_command,
    run_extensive,
    verify_output_files,
)


def test_core_functionality_and_triggers(
    mock_config,
    prepare_test_cache,
    tmp_path,
    run_cli_command,
    monkeypatch,
    run_extensive,
):
    """Test create_and_run_operational_model: fresh run, up-to-date detection, and triggers.

    Combines:
    - Fresh run with model solve
    - Up-to-date detection
    - Config file modification trigger (extensive only)
    - Dependency modification trigger (extensive only)
    - Missing single file trigger (extensive only)

    Runs: ~3 (prerequisites + fresh + up-to-date), or ~6 with extensive
    """
    # Set environment variable to mock cache building in subprocess
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # Run prerequisite tasks to generate operational timeseries
    result = run_cli_command([f"config={mock_config}", "create_operational_timeseries"])
    assert result.returncode == 0, (
        f"Prerequisite failed: {result.stdout}\n{result.stderr}"
    )

    # Test fresh run
    result = run_cli_command(
        [f"config={mock_config}", "create_and_run_operational_model"]
    )
    assert result.returncode == 0, (
        f"Command failed with return code {result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    assert_task_ran(result.stdout, "create_and_run_operational_model")

    # Check operational model outputs created
    output_dir = tmp_path / "run_dir" / "test_run" / "outputs"
    assert output_dir.exists(), f"Output directory does not exist: {output_dir}"

    # Verify key output files exist
    expected_files = [
        "operational",
    ]
    verify_output_files(output_dir, expected_files, extension="nc")

    # Get timestamps from first run
    first_run_timestamps = get_file_timestamps(output_dir)
    time.sleep(0.1)

    # Test up-to-date detection - second run
    result = run_cli_command(
        [f"config={mock_config}", "create_and_run_operational_model"]
    )
    assert result.returncode == 0
    assert_task_up_to_date(result.stdout, "create_and_run_operational_model")

    # Verify timestamps haven't changed (files weren't regenerated)
    second_run_timestamps = get_file_timestamps(output_dir)
    assert first_run_timestamps == second_run_timestamps, (
        "Files were regenerated when they shouldn't have been"
    )

    if not run_extensive:
        return

    # Test config file modification triggers rerun (extensive only)
    time.sleep(0.1)
    modify_config_value(mock_config, "discount_rate", 0.06)
    result = run_cli_command(
        [f"config={mock_config}", "create_and_run_operational_model"]
    )
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_and_run_operational_model")

    # Test dependency modification trigger (extensive only)
    time.sleep(0.1)

    # Modify a PyPSA-friendly input file (dependency)
    pypsa_dir = tmp_path / "run_dir" / "test_run" / "pypsa_friendly"
    generators_file = pypsa_dir / "generators.csv"

    # Add a dummy row to trigger regeneration
    df = pd.read_csv(generators_file)
    if len(df) > 0:
        m = df["p_nom"] > 0.0
        df.loc[m, ["p_nom"]] = df.loc[:, ["p_nom"]] + 1.0
    df.to_csv(generators_file, index=False)

    result = run_cli_command(
        [f"config={mock_config}", "create_and_run_operational_model"]
    )
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_and_run_operational_model")

    # Verify timestamps changed
    new_timestamps = get_file_timestamps(output_dir)
    assert new_timestamps != second_run_timestamps, (
        "Files were not regenerated after dependency modification"
    )
    for filename, old_time in second_run_timestamps.items():
        assert new_timestamps[filename] > old_time, f"{filename} was not regenerated"

    # Test missing single file trigger (extensive only)
    target_file = output_dir / "operational.nc"
    target_file.unlink()

    result = run_cli_command(
        [f"config={mock_config}", "create_and_run_operational_model"]
    )
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_and_run_operational_model")
    assert target_file.exists(), "Missing file was not regenerated"
