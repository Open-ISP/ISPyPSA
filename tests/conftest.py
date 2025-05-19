import io
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture(scope="module")
def workbook_table_cache_test_path():
    return Path("tests", "test_workbook_table_cache")


@pytest.fixture
def csv_str_to_df():
    def func(csv_str, **kwargs):
        """Helper function to convert a CSV string to a DataFrame."""
        # Remove spaces and tabs that have been included for readability.
        csv_str = csv_str.replace(" ", "").replace("\t", "")
        return pd.read_csv(io.StringIO(csv_str), **kwargs)

    return func
