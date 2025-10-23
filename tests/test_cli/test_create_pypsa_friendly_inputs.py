"""Tests for create_pypsa_friendly_inputs CLI task.

Tests core functionality: fresh run, up-to-date detection, and triggers using
the optimized testing pattern that minimizes the number of task runs.

Optimization: Combines related test scenarios into a single test to reduce
subprocess overhead and total execution time.
"""

import shutil
import time
from pathlib import Path

import pandas as pd
import pytest
import yaml

from ispypsa.translator.create_pypsa_friendly_inputs import _BASE_TRANSLATOR_OUTPUTS

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
    """Test create_pypsa_friendly_inputs: fresh run, up-to-date detection, and triggers.

    Combines:
    - Fresh run with trace data processing
    - Up-to-date detection
    - Dependency modification trigger
    - Missing single file trigger

    Runs: 4 (fresh + up-to-date + dependency mod + missing file)
    """
    # Set environment variable to mock cache building in subprocess
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # Test fresh run
    result = run_cli_command([f"config={mock_config}", "create_pypsa_friendly_inputs"])
    assert result.returncode == 0, (
        f"Command failed with return code {result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    # Check PyPSA-friendly outputs created
    output_dir = tmp_path / "run_dir" / "test_run" / "pypsa_friendly"
    assert output_dir.exists()

    # Verify expected PyPSA files were created
    verify_output_files(output_dir, _BASE_TRANSLATOR_OUTPUTS)

    # Check capacity expansion timeseries were created
    timeseries_dir = output_dir / "capacity_expansion_timeseries"
    assert timeseries_dir.exists()

    # Verify trace data directories and files
    demand_dir = timeseries_dir / "demand_traces"
    solar_dir = timeseries_dir / "solar_traces"
    wind_dir = timeseries_dir / "wind_traces"

    assert demand_dir.exists()
    assert solar_dir.exists()
    assert wind_dir.exists()

    # Check that trace files were created for our dummy data
    expected_demand_files = [f"{region}.parquet" for region in ["NNSW", "SQ"]]
    for file in expected_demand_files:
        assert (demand_dir / file).exists(), f"Missing demand file: {file}"

    assert (solar_dir / "Tamworth Solar Farm.parquet").exists()
    assert (wind_dir / "Wambo Wind Farm.parquet").exists()

    # Get timestamps from first run
    first_run_timestamps = get_file_timestamps(output_dir)
    time.sleep(0.1)

    # Test up-to-date detection - second run
    result = run_cli_command([f"config={mock_config}", "create_pypsa_friendly_inputs"])
    assert result.returncode == 0
    assert_task_up_to_date(result.stdout, "create_pypsa_friendly_inputs")

    # Verify timestamps haven't changed (files weren't regenerated)
    second_run_timestamps = get_file_timestamps(output_dir)
    assert first_run_timestamps == second_run_timestamps

    # Test dependency modification trigger
    time.sleep(0.1)

    # Modify an ISPyPSA input file (dependency)
    ispypsa_input_dir = tmp_path / "run_dir" / "test_run" / "ispypsa_inputs"
    generators_file = ispypsa_input_dir / "ecaa_generators.csv"

    # Add a dummy row to trigger regeneration
    df = pd.read_csv(generators_file)
    if len(df) > 0:
        new_row = df.iloc[0].copy()
        new_row["generator"] = "Dummy Generator"
        new_row["fuel_type"] = "Black Coal"
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    df.to_csv(generators_file, index=False)

    result = run_cli_command([f"config={mock_config}", "create_pypsa_friendly_inputs"])
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_pypsa_friendly_inputs")

    # Verify timestamps changed
    new_timestamps = get_file_timestamps(output_dir)
    assert new_timestamps != second_run_timestamps
    for filename, old_time in second_run_timestamps.items():
        assert new_timestamps[filename] > old_time

    # Test missing single file trigger
    target_file = output_dir / "buses.csv"
    target_file.unlink()

    result = run_cli_command([f"config={mock_config}", "create_pypsa_friendly_inputs"])
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_pypsa_friendly_inputs")
    assert target_file.exists()
    assert target_file.stat().st_size > 0
