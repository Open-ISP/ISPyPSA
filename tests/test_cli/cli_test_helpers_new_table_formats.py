"""New-format-specific fixtures for CLI tests.

Lives separate from cli_test_helpers.py so that when use_new_table_format
becomes the only path, this file is renamed back to cli_test_helpers.py
(after the legacy file is removed) without needing to disentangle anything.
Format-agnostic infrastructure (run_cli_command, assert_task_ran, the
mock_workbook_file fixture, etc.) stays in the sibling shared module.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge this file into the sibling
cli_test_helpers.py and delete it.
"""

import pytest

from .cli_test_helpers import (
    _populate_test_cache,
    build_mock_config,
)


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
    return build_mock_config(tmp_path, mock_workbook_file, version="7.5")
