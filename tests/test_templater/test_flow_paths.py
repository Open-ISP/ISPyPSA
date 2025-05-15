from pathlib import Path

import pandas as pd

from ispypsa.templater import load_manually_extracted_tables
from ispypsa.templater.flow_paths import (
    _template_regional_interconnectors,
    _template_sub_regional_flow_path_costs,
    _template_sub_regional_flow_paths,
)


def test_flow_paths_templater_regional(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path(
        "interconnector_transfer_capability.csv"
    )
    interconnector_capabilities = pd.read_csv(filepath)
    flow_paths_template = _template_regional_interconnectors(
        interconnector_capabilities
    )
    assert all(
        [
            True
            for carrier in flow_paths_template.carrier
            if (carrier == "AC" or carrier == "DC")
        ]
    )
    assert len(flow_paths_template[flow_paths_template.carrier == "DC"]) == 3
    assert all(
        [
            True
            for dtype in flow_paths_template[
                [col for col in flow_paths_template.columns if "mw" in col]
            ].dtypes
            if dtype is int
        ]
    )
    assert all(
        [
            True
            for name in ("QNI", "Terranora", "Heywood", "Murraylink", "Basslink")
            if name in flow_paths_template.flow_path
        ]
    )
    assert len(flow_paths_template) == 6
    assert len(flow_paths_template.columns) == 5


def test_flow_paths_templater_sub_regional(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path(
        "flow_path_transfer_capability.csv"
    )
    flow_path_transfer_capability = pd.read_csv(filepath)
    manual_tables = load_manually_extracted_tables("6.0")
    flow_paths_template = _template_sub_regional_flow_paths(
        flow_path_transfer_capability, manual_tables["transmission_expansion_costs"]
    )
    assert all(
        [
            True
            for carrier in flow_paths_template.carrier
            if (carrier == "AC" or carrier == "DC")
        ]
    )
    assert len(flow_paths_template[flow_paths_template.carrier == "DC"]) == 3
    assert all(
        [
            True
            for dtype in flow_paths_template[
                [col for col in flow_paths_template.columns if "mw" in col]
            ].dtypes
            if dtype is int
        ]
    )
    assert all(
        [
            True
            for name in ("QNI", "Terranora", "Heywood", "Murraylink", "Basslink")
            if name in flow_paths_template.flow_path
        ]
    )
    assert len(flow_paths_template) == 14
    assert len(flow_paths_template.columns) == 6
