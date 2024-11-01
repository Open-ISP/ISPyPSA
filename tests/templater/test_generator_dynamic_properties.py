from pathlib import Path

from ispypsa.templater.dynamic_generator_properties import (
    template_generator_dynamic_properties,
)
from ispypsa.templater.lists import _ISP_SCENARIOS


def test_generator_dynamic_properties_templater(workbook_table_cache_test_path: Path):
    for scenario in _ISP_SCENARIOS:
        mapped_dfs = template_generator_dynamic_properties(
            workbook_table_cache_test_path, scenario
        )
        for key, df in mapped_dfs.items():
            if "price" in key:
                assert all("$/gj" in col for col in df.columns[1:])
                assert all(df.iloc[:, 1:].dtypes != "object")
                assert all(df.notna())
            elif "outage" in key:
                assert all(df.iloc[:, 1:].dtypes != "object")
                assert all(df.notna())
            elif "ratings" in key:
                assert all(df.iloc[:, 3:].dtypes != "object")
                assert all(df.notna())
