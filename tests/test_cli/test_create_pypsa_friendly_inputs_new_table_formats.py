"""End-to-end CLI tests for the new-format translator and model build
(use_new_table_format=true), run against the frozen 7.5 cache.

Only the network and demand are translated from the new tables so far, so
these tests check that the tables flow through every task and the networks
build. The models are not solved: the results extraction needs generator
tables.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into
test_create_pypsa_friendly_inputs.py, test_create_and_run_capacity_expansion_model.py
and test_create_and_run_operational_model.py when the legacy format is removed.
"""

import time
from unittest.mock import patch

import pandas as pd
import pypsa
import pytest

from ispypsa.translator import list_translator_output_files

from .cli_test_helpers import (
    assert_task_ran,
    assert_task_up_to_date,
    build_mock_config,
    get_file_timestamps,
    mock_workbook_file,
    run_cli_command,
    verify_output_files,
)
from .cli_test_helpers_new_table_formats import (
    prepare_test_cache_new_format,
)


@pytest.fixture
def mock_config_filtered_to_traces(tmp_path, mock_workbook_file):
    """A 7.5 config filtered to the sub-regions with a demand trace in
    tests/trace_data (CNSW, NNSW and SQ), as every sub-region in the model
    needs one. The config models financial years 2025 and 2026 with reference
    year 2018, which those traces cover."""
    return build_mock_config(
        tmp_path,
        mock_workbook_file,
        version="7.5",
        filter_by_isp_sub_regions=["CNSW", "NNSW", "SQ"],
    )


def _expected_output_files() -> list[str]:
    """The translator's declared output set under the new-format flag.

    FEATURE_FLAGS is read at import, so the ISPYPSA_USE_NEW_TABLE_FORMAT env var
    the tests set only reaches the CLI subprocess; the in-process call needs
    the flag patched.
    """
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        return list_translator_output_files()


def test_create_pypsa_friendly_inputs_task_new_format(
    mock_config_filtered_to_traces,
    prepare_test_cache_new_format,
    tmp_path,
    run_cli_command,
    monkeypatch,
):
    """Fresh run creates the new-format translator outputs; a second run is
    up to date."""
    monkeypatch.setenv("ISPYPSA_USE_NEW_TABLE_FORMAT", "true")
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")

    result = run_cli_command(
        [f"config={mock_config_filtered_to_traces}", "create_pypsa_friendly_inputs"]
    )
    assert result.returncode == 0, (
        f"Command failed with return code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    output_dir = tmp_path / "run_dir" / "test_run" / "pypsa_friendly"
    verify_output_files(output_dir, _expected_output_files())

    # Wiring only: the content of each table is covered by the translator
    # unit tests. REZs are discrete nodes in the test config, so every
    # geography gets a bus.
    network_geography = pd.read_csv(
        tmp_path / "run_dir" / "test_run" / "ispypsa_inputs" / "network_geography.csv"
    )
    buses = pd.read_csv(output_dir / "buses.csv")
    assert len(buses) == len(network_geography)

    links = pd.read_csv(output_dir / "links.csv")
    limits = pd.read_csv(output_dir / "link_timeslice_limits.csv")
    existing_links = set(links.loc[~links["p_nom_extendable"], "name"])
    assert set(limits["name"]) == existing_links

    # Generators aren't translated from the new tables yet, so the only ones
    # are the unserved energy generators: one at each of the three
    # sub-regions the config is filtered to.
    sub_regions = network_geography.loc[
        network_geography["geo_type"] == "subregion", "geo_id"
    ]
    assert set(sub_regions) == {"CNSW", "NNSW", "SQ"}
    generators = pd.read_csv(output_dir / "generators.csv")
    assert set(generators["carrier"]) == {"Unserved Energy"}
    assert set(generators["bus"]) == set(sub_regions)

    # A demand trace is saved for each sub-region, with a row per snapshot.
    demand_traces_dir = output_dir / "capacity_expansion_timeseries" / "demand_traces"
    demand_trace_files = {file.name for file in demand_traces_dir.glob("*")}
    assert demand_trace_files == set(sub_regions + ".parquet")

    # The templated custom constraints aren't translated yet (their terms
    # need generators and storage), so only the expansion limits are
    # built: one for each path with expansion links.
    lhs = pd.read_csv(output_dir / "custom_constraints_lhs.csv")
    rhs = pd.read_csv(output_dir / "custom_constraints_rhs.csv")
    expandable_paths = set(links.loc[links["p_nom_extendable"], "isp_name"])
    expected_constraints = {f"{path}_expansion_limit" for path in expandable_paths}
    assert set(rhs["constraint_name"]) == expected_constraints
    assert set(lhs["constraint_name"]) == expected_constraints

    # Every snapshot has a timeslice for each of the five regions.
    snapshots = pd.read_csv(output_dir / "snapshots.csv")
    timeslice_snapshots = pd.read_csv(output_dir / "timeslice_snapshots.csv")
    assert len(timeslice_snapshots) == 5 * len(snapshots)
    assert set(timeslice_snapshots["snapshots"]) == set(snapshots["snapshots"])

    demand_trace = pd.read_parquet(demand_traces_dir / "CNSW.parquet")
    assert list(demand_trace.columns) == ["investment_periods", "snapshots", "p_set"]
    assert len(demand_trace) == len(snapshots)

    first_run_timestamps = get_file_timestamps(output_dir)
    time.sleep(0.1)

    result = run_cli_command(
        [f"config={mock_config_filtered_to_traces}", "create_pypsa_friendly_inputs"]
    )
    assert result.returncode == 0
    assert_task_up_to_date(result.stdout, "create_pypsa_friendly_inputs")
    assert get_file_timestamps(output_dir) == first_run_timestamps


def test_build_capacity_expansion_and_operational_models_new_format(
    mock_config_filtered_to_traces,
    prepare_test_cache_new_format,
    tmp_path,
    run_cli_command,
    monkeypatch,
):
    """Both model tasks build their network from the new-format tables, with
    the per-timeslice link limits and the demand applied at each phase's
    snapshots."""
    monkeypatch.setenv("ISPYPSA_USE_NEW_TABLE_FORMAT", "true")
    monkeypatch.setenv("ISPYPSA_TEST_MOCK_CACHE", "true")
    run_dir = tmp_path / "run_dir" / "test_run"

    for task in [
        "create_and_run_capacity_expansion_model",
        "create_operational_timeseries",
        "create_and_run_operational_model",
    ]:
        result = run_cli_command(
            [f"config={mock_config_filtered_to_traces}", "run_optimisation=False", task]
        )
        assert result.returncode == 0, (
            f"{task} failed with return code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
        assert_task_ran(result.stdout, task)

    links = pd.read_csv(run_dir / "pypsa_friendly" / "links.csv")
    network = pypsa.Network(run_dir / "outputs" / "capacity_expansion.nc")
    assert set(network.links.index) == set(links["name"])

    # A load at each sub-region, with demand at every snapshot, and an
    # unserved energy generator beside it.
    network_geography = pd.read_csv(
        run_dir / "ispypsa_inputs" / "network_geography.csv"
    )
    sub_regions = network_geography.loc[
        network_geography["geo_type"] == "subregion", "geo_id"
    ]
    assert set(network.loads.index) == set("load_" + sub_regions)
    assert set(network.loads_t.p_set.columns) == set(network.loads.index)
    assert set(network.generators.index) == set("unserved_energy_" + sub_regions)

    # Every existing link is given a limit at every snapshot, but a saved
    # network only keeps a series that differs from the attribute's default
    # (p_max_pu 1.0, p_min_pu 0.0) at some snapshot. So the links with a
    # saved series are the ones with a non-default limit that applies in the
    # model: a fallback, or a timeslice the snapshots fall in.
    limits = pd.read_csv(run_dir / "pypsa_friendly" / "link_timeslice_limits.csv")
    timeslice_snapshots = pd.read_csv(
        run_dir / "pypsa_friendly" / "timeslice_snapshots.csv"
    )
    applies = limits["timeslice"].isna() | limits["timeslice"].isin(
        timeslice_snapshots["timeslice"]
    )
    p_max_pu = limits[applies & (limits["attribute"] == "p_max_pu")]
    p_min_pu = limits[applies & (limits["attribute"] == "p_min_pu")]
    links_below_full_forward = p_max_pu.loc[p_max_pu["value"] != 1.0, "name"]
    links_with_reverse_flow = p_min_pu.loc[p_min_pu["value"] != 0.0, "name"]
    assert links_below_full_forward.nunique() > 0
    assert links_with_reverse_flow.nunique() > 0
    assert len(network.links_t.p_max_pu.columns) == links_below_full_forward.nunique()
    assert len(network.links_t.p_min_pu.columns) == links_with_reverse_flow.nunique()

    # The operational phase gets its own snapshots and timeslice mapping.
    operational_snapshots = pd.read_csv(
        run_dir / "pypsa_friendly" / "operational_snapshots.csv"
    )
    operational_timeslice_snapshots = pd.read_csv(
        run_dir / "pypsa_friendly" / "operational_timeslice_snapshots.csv"
    )
    assert len(operational_timeslice_snapshots) == 5 * len(operational_snapshots)

    # It also gets its own demand traces, with a row per operational snapshot.
    # The operational network is only saved once solved, so the update of its
    # loads is covered by test_demand_new_table_format.py.
    demand_traces_dir = (
        run_dir / "pypsa_friendly" / "operational_timeseries" / "demand_traces"
    )
    demand_trace_files = {file.name for file in demand_traces_dir.glob("*")}
    assert demand_trace_files == set(sub_regions + ".parquet")
    demand_trace = pd.read_parquet(demand_traces_dir / "CNSW.parquet")
    assert len(demand_trace) == len(operational_snapshots)
