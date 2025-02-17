from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def workbook_table_cache_test_path():
    return Path("tests", "test_workbook_table_cache")
