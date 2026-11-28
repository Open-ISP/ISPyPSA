"""Runs the capacity expansion model through the CLI with tsam segments turned
on, to check the segmented snapshots and traces make it through the model build
and the solve."""

import pandas as pd
import pypsa

from .cli_test_helpers import (
    mock_config,
    mock_workbook_file,
    modify_config_value,
    prepare_test_cache,
    run_cli_command,
)


def test_capacity_expansion_model_solves_on_tsam_segments(
    mock_config,
    prepare_test_cache,
    tmp_path,
    run_cli_command,
    monkeypatch,
):
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")
    modify_config_value(
        mock_config,
        "temporal.capacity_expansion.aggregation.tsam",
        {"features": "demand", "segments": {"n_segments_per_day": 6}},
    )

    result = run_cli_command([f"config={mock_config}", "create_pypsa_friendly_inputs"])
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"

    # The mock config models one representative week at half-hourly resolution:
    # 7 days of 48 snapshots. At 6 segments a day that is 42 segments covering
    # 168 hours.
    run_dir = tmp_path / "run_dir" / "test_run"
    snapshots = pd.read_csv(run_dir / "pypsa_friendly" / "snapshots.csv")
    assert len(snapshots) == 42
    assert snapshots["stores"].sum() == 168

    # The snapshot map sits beside the snapshots, with a row for each of the 336
    # half-hours naming one of the 42 segments.
    snapshot_map = pd.read_csv(run_dir / "pypsa_friendly" / "snapshot_map.csv")
    assert list(snapshot_map.columns) == [
        "investment_periods",
        "original_snapshots",
        "snapshots",
    ]
    assert len(snapshot_map) == 336
    assert snapshot_map["snapshots"].nunique() == 42

    result = run_cli_command(
        [f"config={mock_config}", "create_and_run_capacity_expansion_model"]
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"

    network = pypsa.Network(run_dir / "outputs" / "capacity_expansion.nc")
    assert len(network.snapshots) == 42
    assert network.snapshot_weightings["stores"].sum() == 168
    assert network.generators_t.p.shape[0] == 42

    # The results say how long each segment the model was solved on lasts, so
    # the per-timestep results can be turned into energy.
    timesteps = pd.read_csv(
        run_dir / "outputs" / "capacity_expansion_tables" / "timesteps.csv"
    )
    assert list(timesteps.columns) == [
        "investment_period",
        "timestep",
        "model_year",
        "duration_hours",
        "hours_represented",
    ]
    assert len(timesteps) == 42
    # The lengths are the ones the network was solved with.
    assert list(timesteps["duration_hours"]) == list(
        network.snapshot_weightings["stores"]
    )
