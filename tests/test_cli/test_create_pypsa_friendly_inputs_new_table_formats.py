"""Tests for the create_pypsa_friendly_inputs CLI task under the new table format.

Mirrors test_create_pypsa_friendly_inputs.py (fresh run, up-to-date detection,
and triggers) for the new-format translator path: network and custom-constraint
tables only, no generators/batteries and no timeseries directory.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into
test_create_pypsa_friendly_inputs.py when the legacy format is removed.
"""

import time

import pandas as pd

from ispypsa.translator.create_pypsa_friendly import _NEW_FORMAT_TRANSLATOR_OUTPUTS

from .cli_test_helpers import (
    assert_task_ran,
    assert_task_up_to_date,
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


def test_create_pypsa_friendly_inputs_task_new_format(
    mock_config_new_format,
    prepare_test_cache_new_format,
    tmp_path,
    run_cli_command,
    monkeypatch,
    run_extensive,
):
    """Test fresh run, up-to-date detection, and triggers for the new format.

    Combines:
    - Fresh run creates the new-format translator outputs (and no timeseries)
    - Up-to-date detection
    - Config file modification trigger (extensive only)
    - Dependency modification trigger (extensive only)
    - Missing single file trigger (extensive only)

    Runs: 2 (fresh + up-to-date), or 5 with extensive
    """
    monkeypatch.setenv("ISPYPSA_USE_NEW_TABLE_FORMAT", "true")
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    # Test fresh run (doit also runs the upstream create_ispypsa_inputs task).
    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_pypsa_friendly_inputs"]
    )
    assert result.returncode == 0, (
        f"Command failed with return code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    output_dir = tmp_path / "run_dir" / "test_run" / "pypsa_friendly"
    assert output_dir.exists()
    verify_output_files(output_dir, _NEW_FORMAT_TRANSLATOR_OUTPUTS)

    # No generators/batteries and no timeseries until generator translation
    # lands for the new format.
    assert not (output_dir / "generators.csv").exists()
    assert not (output_dir / "batteries.csv").exists()
    assert not (output_dir / "capacity_expansion_timeseries").exists()

    # Spot-check the wiring of the outputs (content is covered by the
    # translator unit tests). 15 sub-regions + 47 REZs at 7.5.
    buses = pd.read_csv(output_dir / "buses.csv")
    assert len(buses) == 62

    links = pd.read_csv(output_dir / "links.csv")
    limits = pd.read_csv(output_dir / "link_timeslice_limits.csv")
    assert set(limits["name"]) <= set(links["name"])

    lhs = pd.read_csv(output_dir / "custom_constraints_lhs.csv")
    rhs = pd.read_csv(output_dir / "custom_constraints_rhs.csv")
    assert len(rhs) > 0
    assert set(lhs["constraint_name"]) == set(rhs["constraint_name"])

    mapping = pd.read_csv(output_dir / "timeslice_snapshots.csv")
    snapshots = pd.read_csv(output_dir / "snapshots.csv")
    assert len(mapping) > 0
    assert set(mapping["snapshots"]) <= set(snapshots["snapshots"])

    # Get timestamps from first run
    first_run_timestamps = get_file_timestamps(output_dir)
    time.sleep(0.1)

    # Test up-to-date detection - second run
    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_pypsa_friendly_inputs"]
    )
    assert result.returncode == 0
    assert_task_up_to_date(result.stdout, "create_pypsa_friendly_inputs")

    second_run_timestamps = get_file_timestamps(output_dir)
    assert first_run_timestamps == second_run_timestamps

    if not run_extensive:
        return

    # Test config file modification triggers rerun (extensive only)
    time.sleep(0.1)
    modify_config_value(mock_config_new_format, "discount_rate", 0.06)
    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_pypsa_friendly_inputs"]
    )
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_pypsa_friendly_inputs")

    # Test dependency modification trigger (extensive only): edit a templated
    # input the translator consumes.
    time.sleep(0.1)
    limits_file = (
        tmp_path
        / "run_dir"
        / "test_run"
        / "ispypsa_inputs"
        / "network_transmission_path_limits.csv"
    )
    df = pd.read_csv(limits_file)
    df["capacity"] = df["capacity"] + 1.0
    df.to_csv(limits_file, index=False)

    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_pypsa_friendly_inputs"]
    )
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_pypsa_friendly_inputs")

    config_change_timestamps = get_file_timestamps(output_dir)

    # Test missing single file trigger (extensive only)
    target_file = output_dir / "buses.csv"
    target_file.unlink()

    result = run_cli_command(
        [f"config={mock_config_new_format}", "create_pypsa_friendly_inputs"]
    )
    assert result.returncode == 0
    assert_task_ran(result.stdout, "create_pypsa_friendly_inputs")
    assert target_file.exists()
    assert target_file.stat().st_size > 0
