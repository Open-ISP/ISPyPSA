"""Tests for create_operational_timeseries CLI task.

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
    prepare_test_cache,
    run_cli_command,
    verify_output_files,
)


def test_core_functionality_and_triggers(
    mock_config, prepare_test_cache, tmp_path, run_cli_command, monkeypatch
):
    """Test create_operational_timeseries: fresh run, up-to-date detection, and triggers.

    Combines:
    - Fresh run with timeseries generation
    - Up-to-date detection
    - Dependency modification trigger
    - Missing single file trigger

    Runs: ~5 (prerequisites + fresh + up-to-date + dependency mod + missing file)
    """
    # Set environment variable to mock cache building in subprocess
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # Run prerequisite task to generate PyPSA-friendly inputs
    # result = run_cli_command([f"config={mock_config}", "create_pypsa_friendly_inputs"])
    # assert result.returncode == 0, f"Prerequisite failed: {result.stdout}\n{result.stderr}"

    # Test fresh run
    result = run_cli_command([f"config={mock_config}", "create_operational_timeseries"])
    assert result.returncode == 0, (
        f"Command failed with return code {result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    assert_task_ran(result.stdout, "create_operational_timeseries")

    # Check operational timeseries outputs created
    output_dir = (
        tmp_path / "run_dir" / "test_run" / "pypsa_friendly" / "operational_timeseries"
    )
    assert output_dir.exists(), f"Output directory does not exist: {output_dir}"

    # Verify key subdirectories exist
    expected_dirs = ["demand_traces", "solar_traces", "wind_traces"]
    for dir_name in expected_dirs:
        dir_path = output_dir / dir_name
        assert dir_path.exists(), f"Expected directory {dir_name} not found"
        # Check that directory contains at least one parquet file
        parquet_files = list(dir_path.glob("*.parquet"))
        assert len(parquet_files) > 0, f"No parquet files found in {dir_name}"

    # Get timestamps from first run - collect all parquet files recursively
    def get_all_parquet_timestamps(base_dir):
        """Get timestamps for all parquet files in subdirectories."""
        timestamps = {}
        for parquet_file in base_dir.rglob("*.parquet"):
            # Use relative path as key for consistency
            rel_path = parquet_file.relative_to(base_dir)
            timestamps[str(rel_path)] = parquet_file.stat().st_mtime
        return timestamps

    first_run_timestamps = get_all_parquet_timestamps(output_dir)
    time.sleep(0.1)

    # Test up-to-date detection - second run
    result = run_cli_command([f"config={mock_config}", "create_operational_timeseries"])
    assert result.returncode == 0
    assert_task_up_to_date(result.stdout, "create_operational_timeseries")

    # Verify timestamps haven't changed (files weren't regenerated)
    second_run_timestamps = get_all_parquet_timestamps(output_dir)
    assert first_run_timestamps == second_run_timestamps, (
        "Files were regenerated when they shouldn't have been"
    )

    # Test dependency modification trigger
    time.sleep(0.1)

    # Modify an ISPyPSA input file (dependency)
    ispypsa_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs"
    generators_file = ispypsa_dir / "ecaa_generators.csv"

    # Add a dummy row to trigger regeneration
    df = pd.read_csv(generators_file)
    if len(df) > 0:
        new_row = df.iloc[0].copy()
        new_row["isp_name"] = "Dummy Generator OT Test"
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    df.to_csv(generators_file, index=False)

    result = run_cli_command([f"config={mock_config}", "create_operational_timeseries"])
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_operational_timeseries")

    # Verify timestamps changed
    new_timestamps = get_all_parquet_timestamps(output_dir)
    assert new_timestamps != second_run_timestamps, (
        "Files were not regenerated after dependency modification"
    )
    for filename, old_time in second_run_timestamps.items():
        assert new_timestamps[filename] > old_time, f"{filename} was not regenerated"

    # Test missing single file trigger
    # Remove one of the parquet files
    demand_dir = output_dir / "demand_traces"
    parquet_files = list(demand_dir.glob("*.parquet"))
    assert len(parquet_files) > 0, "No parquet files to delete"
    target_file = parquet_files[0]
    target_file.unlink()

    result = run_cli_command([f"config={mock_config}", "create_operational_timeseries"])
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_operational_timeseries")
    assert target_file.exists(), "Missing file was not regenerated"
