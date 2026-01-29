from pathlib import Path

import pandas as pd

from ispypsa.templater.nodes import (
    _template_regions,
    _template_sub_regions,
)


def test_node_templater_nem_regions(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path("regional_reference_nodes.csv")
    regional_reference_nodes = pd.read_csv(filepath)
    regional_template = _template_regions(regional_reference_nodes)
    assert set(regional_template.nem_region_id) == set(("QLD", "NSW"))
    assert set(regional_template.isp_sub_region_id) == set(("SQ", "SNW"))
    assert set(regional_template.regional_reference_node) == set(
        ("South Pine", "Sydney West")
    )
    assert set(regional_template.regional_reference_node_voltage_kv) == set((275, 330))
    assert len(regional_template.columns) == 4


def test_templater_sub_regions(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path("sub_regional_reference_nodes.csv")
    sub_regional_reference_nodes = pd.read_csv(filepath)
    sub_regions_template = _template_sub_regions(sub_regional_reference_nodes)
    assert set(sub_regions_template.isp_sub_region_id) == set(("NNSW", "SQ"))
    assert set(sub_regions_template.nem_region_id) == set(("NSW", "QLD"))
    assert set(sub_regions_template.sub_region_reference_node) == set(
        ("South Pine", "Armidale")
    )
    assert set(sub_regions_template.sub_region_reference_node_voltage_kv) == set(
        (275, 330)
    )
    assert len(sub_regions_template.columns) == 4


def test_templater_sub_regions_mapping_only(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path("sub_regional_reference_nodes.csv")
    sub_regional_reference_nodes = pd.read_csv(filepath)
    sub_regions_template = _template_sub_regions(
        sub_regional_reference_nodes, mapping_only=True
    )
    assert set(sub_regions_template.isp_sub_region_id) == set(("SQ", "NNSW"))
    assert set(sub_regions_template.nem_region_id) == set(("QLD", "NSW"))
    assert len(sub_regions_template.columns) == 2
