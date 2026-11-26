"""New-format-specific fixtures for CLI tests.

Lives separate from cli_test_helpers.py so that when use_new_table_format
becomes the only path, this file is renamed back to cli_test_helpers.py
(after the legacy file is removed) without needing to disentangle anything.
Format-agnostic infrastructure (run_cli_command, assert_task_ran, the
mock_workbook_file fixture, etc.) stays in the sibling shared module.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge this file into the sibling
cli_test_helpers.py and delete it.
"""

from pathlib import Path

import pandas as pd
import pytest

from .cli_test_helpers import (
    _populate_test_cache,
    build_mock_config,
)

_TESTS_DIRECTORY = Path(__file__).parent.parent


@pytest.fixture
def prepare_test_cache_new_format(tmp_path, mock_workbook_file):
    """Prepare a 7.5 new-format test cache from test_workbook_table_cache/7.5."""
    return _populate_test_cache(tmp_path, mock_workbook_file, "7.5")


@pytest.fixture
def mock_config_new_format(tmp_path, mock_workbook_file):
    """Create a 7.5 ISPyPSA configuration tuned for the new-format templater.

    Defaults to sub_regions granularity. Tests that need a different
    granularity should call `build_mock_config` directly with explicit kwargs
    rather than overriding via this fixture.
    """
    return build_mock_config(
        tmp_path,
        mock_workbook_file,
        version="7.5",
        parsed_traces_directory=_create_demand_traces_for_every_sub_region(tmp_path),
    )


def _create_demand_traces_for_every_sub_region(tmp_path):
    """Creates a parsed trace directory with a demand trace for every ISP
    sub-region in the 7.5 test cache, and returns its path.

    The new-format path has no region filter yet, so the model has every
    sub-region and each needs a demand trace, while tests/trace_data has
    traces for only CNSW, NNSW and SQ. Every sub-region is given a copy of the
    CNSW trace for reference year 2018, cut down to the financial years the
    mock config models (2025 and 2026).

    FEATURE_FLAG_CLEANUP[use_new_table_format]: delete once the region
    filters work on the new tables, and filter the mock config to the
    sub-regions with a trace in tests/trace_data instead.
    """
    cnsw_trace = pd.read_parquet(
        _TESTS_DIRECTORY
        / "trace_data"
        / "isp_2024"
        / "demand"
        / "Step_Change_RefYear2018_CNSW_POE50_OPSO_MODELLING.parquet"
    )
    in_modelled_years = (cnsw_trace["datetime"] > "2024-07-01") & (
        cnsw_trace["datetime"] <= "2026-07-01"
    )
    cnsw_trace = cnsw_trace[in_modelled_years].drop(columns="subregion")
    sub_regions = pd.read_csv(
        _TESTS_DIRECTORY
        / "test_workbook_table_cache"
        / "7.5"
        / "sub_regional_reference_nodes.csv"
    )
    # e.g. "Northern Queensland (NQ)" -> "NQ"
    sub_region_ids = sub_regions["ISP sub-region"].str.extract(
        r"\(([A-Z]+)\)", expand=False
    )
    traces = cnsw_trace.merge(sub_region_ids.rename("subregion"), how="cross")
    demand_directory = tmp_path / "trace_data" / "isp_2024" / "demand"
    demand_directory.mkdir(parents=True)
    traces.to_parquet(demand_directory / "demand.parquet", index=False)
    return tmp_path / "trace_data"
