from pathlib import Path

import numpy as np
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
    assert all([carrier == "AC" for carrier in flow_paths_template.carrier])
    assert all(
        [
            dtype == np.int64
            for dtype in flow_paths_template[
                [col for col in flow_paths_template.columns if "mw" in col]
            ].dtypes
        ]
    )
    assert len(flow_paths_template) == 6
    assert len(flow_paths_template.columns) == 5


def test_flow_paths_templater_sub_regional(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path(
        "flow_path_transfer_capability.csv"
    )
    flow_path_transfer_capability = pd.read_csv(filepath)
    flow_paths_template = _template_sub_regional_flow_paths(
        flow_path_transfer_capability
    )
    assert all([carrier == "AC" for carrier in flow_paths_template.carrier])
    assert all(
        [
            dtype == np.int64
            for dtype in flow_paths_template[
                [col for col in flow_paths_template.columns if "mw" in col]
            ].dtypes
        ]
    )
    assert len(flow_paths_template) == 12
    assert len(flow_paths_template.columns) == 6
