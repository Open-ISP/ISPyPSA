from pathlib import Path

from ispypsa.data_fetch import read_csvs
from ispypsa.templater.dynamic_generator_properties import (
    _template_generator_dynamic_properties,
)
from ispypsa.templater.lists import _ISP_SCENARIOS


def test_generator_dynamic_properties_templater(workbook_table_cache_test_path: Path):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    for scenario in _ISP_SCENARIOS:
        mapped_dfs = _template_generator_dynamic_properties(iasr_tables, scenario)
        for key, df in [
            item for item in mapped_dfs.items() if item[0] != "liquid_fuel_prices"
        ]:
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
        liquid_fuel = mapped_dfs["liquid_fuel_prices"]
        assert all("$/gj" in ind for ind in liquid_fuel.index)
        assert all(liquid_fuel.notna())
        assert liquid_fuel.dtypes != "object"
