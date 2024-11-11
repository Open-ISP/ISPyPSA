from pathlib import Path

from ispypsa.templater.renewable_energy_zones import (
    template_renewable_energy_zone_locations,
)


def test_renewable_energy_zones_locations(workbook_table_cache_test_path: Path):
    node_template = template_renewable_energy_zone_locations(
        workbook_table_cache_test_path
    )
    assert node_template.index.name == "rez_id"
    assert set(node_template.index) == set(("Q1", "Q2"))
    assert set(node_template.isp_sub_region_id) == set(("NQ", "NQ"))
    assert set(node_template.nem_region_id) == set(("QLD", "QLD"))
