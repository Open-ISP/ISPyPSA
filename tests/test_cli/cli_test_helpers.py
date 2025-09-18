"""Helper functions and fixtures for CLI testing.

This module provides reusable components for testing ISPyPSA CLI tasks including:
- Mock configurations
- Test data preparation
- CLI command execution
- File timestamp utilities
- Task output parsing
"""

import os
import shutil
import subprocess
import time
from pathlib import Path

import pandas as pd
import pytest
import yaml


@pytest.fixture
def prepare_test_cache(tmp_path, mock_workbook_file):
    """Prepare test cache by copying from test_workbook_table_cache.

    Creates a complete cache directory with all required CSV files,
    ensuring timestamps are newer than the workbook file so the cache
    task considers itself up-to-date.

    Args:
        tmp_path: pytest temporary directory
        mock_workbook_file: Path to mock Excel workbook

    Returns:
        Path: Path to the prepared cache directory
    """
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Copy all CSV files from tests/test_workbook_table_cache to cache_dir
    test_cache_dir = Path(__file__).parent.parent / "test_workbook_table_cache"

    for csv_file in test_cache_dir.glob("*.csv"):
        shutil.copy2(csv_file, cache_dir / csv_file.name)

    # Make sure cache files are newer than the workbook file so they're considered up-to-date
    current_time = time.time()
    for csv_file in cache_dir.glob("*.csv"):
        os.utime(csv_file, (current_time, current_time))

    # Make workbook older than cache files
    os.utime(
        mock_workbook_file, (current_time - 3600, current_time - 3600)
    )  # 1 hour older

    return cache_dir


@pytest.fixture
def mock_workbook_file(tmp_path):
    """Create a minimal Excel workbook that passes basic validation.

    Args:
        tmp_path: pytest temporary directory

    Returns:
        Path: Path to the created workbook file
    """
    workbook_path = tmp_path / "dummy.xlsx"

    # Create a minimal Excel file with required sheets
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        # Add a dummy sheet
        pd.DataFrame({"A": [1, 2, 3]}).to_excel(
            writer, sheet_name="Sheet1", index=False
        )

    return workbook_path


@pytest.fixture
def mock_config(tmp_path, mock_workbook_file):
    """Create a minimal valid ISPyPSA configuration.

    Args:
        tmp_path: pytest temporary directory
        mock_workbook_file: Path to mock Excel workbook

    Returns:
        Path: Path to the created config YAML file
    """
    config_path = tmp_path / "test_config.yaml"
    config_content = {
        "scenario": "Step Change",
        "wacc": 0.07,
        "discount_rate": 0.05,
        "network": {
            "transmission_expansion": True,
            "rez_transmission_expansion": True,
            "annuitisation_lifetime": 30,
            "nodes": {"regional_granularity": "sub_regions", "rezs": "discrete_nodes"},
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
        "iasr_workbook_version": "6.0",
        "paths": {
            "ispypsa_run_name": "test_run",
            "parsed_traces_directory": "tests/trace_data",
            "parsed_workbook_cache": str(tmp_path / "cache"),
            "workbook_path": str(mock_workbook_file),
            "run_directory": str(tmp_path / "run_dir"),
        },
        "filter_by_isp_sub_regions": ["NNSW", "SQ"],

    }
    # Write YAML config
    with open(config_path, "w") as f:
        yaml.dump(config_content, f)

    return config_path


@pytest.fixture
def run_cli_command():
    """Fixture to run CLI commands and capture output.

    Returns:
        function: A function that takes CLI args and returns subprocess result
    """

    def _run(args, cwd=None):
        """Run ispypsa CLI command with given arguments.

        Args:
            args: List of command line arguments (excluding 'uv run ispypsa')
            cwd: Working directory to run command from

        Returns:
            subprocess.CompletedProcess: Result with returncode, stdout, stderr
        """
        cmd = ["uv", "run", "ispypsa"] + args
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
        return result

    return _run


def touch_file_with_timestamp(file_path, timestamp):
    """Set file modification time to specific timestamp.

    Args:
        file_path: Path to file to modify
        timestamp: Unix timestamp to set
    """
    file_path.touch()
    os.utime(file_path, (timestamp, timestamp))


def get_file_timestamps(directory):
    """Get dict of filename: mtime for all files in directory.

    Args:
        directory: Path to directory to scan

    Returns:
        dict: Mapping of filename to modification time
    """
    return {f.name: f.stat().st_mtime for f in directory.glob("*") if f.is_file()}


def assert_task_ran(output, task_name):
    """Assert that a task was executed (not skipped).

    Args:
        output: CLI stdout output
        task_name: Name of task to check

    Raises:
        AssertionError: If task was not executed
    """
    # Parse doit output format - task names are prefixed with . when run
    # Note: doit uses variable spacing, so check for patterns with different spacing
    patterns = [f". {task_name}", f".  {task_name}", f"R {task_name}"]
    task_executed = any(pattern in output for pattern in patterns)
    assert task_executed, f"Task '{task_name}' was not executed. Output: {output}"


def assert_task_up_to_date(output, task_name):
    """Assert that a task was skipped as up-to-date.

    Args:
        output: CLI stdout output
        task_name: Name of task to check

    Raises:
        AssertionError: If task was not skipped
    """
    assert f"-- {task_name}" in output, (
        f"Task '{task_name}' was not skipped as up-to-date. Output: {output}"
    )


def verify_output_files(output_dir, expected_files, extension="csv"):
    """Verify that expected output files exist and are non-empty.

    Args:
        output_dir: Path to directory containing output files
        expected_files: List of expected file names
        extension: file type extension, csv by default

    Raises:
        AssertionError: If files don't exist or are empty
    """
    # Check directory exists
    assert output_dir.exists(), f"Output directory {output_dir} does not exist"

    # Check expected files exist and are non-empty
    for file_name in expected_files:
        file_name = file_name + "." + extension
        file_path = output_dir / file_name
        assert file_path.exists(), f"Expected file {file_name} not found"
        assert file_path.stat().st_size > 0, f"File {file_name} is empty"


def create_config_with_granularity(tmp_path, mock_workbook_file, granularity):
    """Create a config with specific regional granularity.

    Args:
        tmp_path: pytest temporary directory
        mock_workbook_file: Path to mock Excel workbook
        granularity: Regional granularity ("sub_regions", "nem_regions", "single_region")

    Returns:
        Path: Path to the created config file
    """
    config_path = tmp_path / f"config_{granularity}.yaml"
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
        "iasr_workbook_version": "6.0",
        "paths": {
            "ispypsa_run_name": "test_run",
            "parsed_traces_directory": "NOT_SET_FOR_TESTING",
            "parsed_workbook_cache": str(tmp_path / "cache"),
            "workbook_path": str(mock_workbook_file),
            "run_directory": str(tmp_path / "run_dir"),
        },
        "filter_by_nem_regions": ["NSW"],
    }

    with open(config_path, "w") as f:
        yaml.dump(config_content, f)

    return config_path


def modify_cache_file_timestamp(cache_dir, filename):
    """Modify the timestamp of a specific cache file to trigger dependency detection.

    Args:
        cache_dir: Path to cache directory
        filename: Name of cache file to modify

    Returns:
        Path: Path to the modified file
    """
    cache_file = cache_dir / filename
    if cache_file.exists():
        cache_file.touch()  # Update modification time
    return cache_file


def clean_output_directory(output_dir):
    """Remove all files from an output directory.

    Args:
        output_dir: Path to directory to clean
    """
    if output_dir.exists():
        for file in output_dir.glob("*"):
            if file.is_file():
                file.unlink()
            elif file.is_dir():
                shutil.rmtree(file)


def create_config_with_missing_cache(base_config_path, tmp_path):
    """Create a config with missing cache directory for testing dependency failures.

    Args:
        base_config_path: Path to base config to modify
        tmp_path: pytest temporary directory

    Returns:
        Path: Path to the modified config file
    """
    with open(base_config_path, "r") as f:
        config_data = yaml.safe_load(f)

    # Point to a cache directory that doesn't exist
    config_data["paths"]["parsed_workbook_cache"] = str(tmp_path / "nonexistent_cache")

    # Write modified config
    config_path = tmp_path / "no_cache_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)

    return config_path
