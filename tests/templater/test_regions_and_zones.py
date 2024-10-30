from pathlib import Path

from ispypsa.templater.regions_and_zones import template_region_and_zone_mapping


def test_node_templater_regional(workbook_table_cache_test_path: Path):
    node_template = template_region_and_zone_mapping(workbook_table_cache_test_path)
    assert node_template.index.name == "rez_id"
    assert set(node_template.index) == set(("Q1", "Q2"))
    assert set(node_template.isp_sub_region_id) == set(("NQ", "NQ"))
    assert set(node_template.nem_region_id) == set(("QLD", "QLD"))
