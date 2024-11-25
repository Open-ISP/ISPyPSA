from pathlib import Path


from ispypsa.templater.flow_paths import template_flow_paths


def test_flow_paths_templater_regional(workbook_table_cache_test_path: Path):
    flow_paths_template = template_flow_paths(
        workbook_table_cache_test_path, "nem_regions"
    )
    assert flow_paths_template.index.name == "flow_path_name"
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
            if name in flow_paths_template.index
        ]
    )
    assert len(flow_paths_template) == 6


def test_flow_paths_templater_sub_regional(workbook_table_cache_test_path: Path):
    flow_paths_template = template_flow_paths(
        workbook_table_cache_test_path, "sub_regions"
    )
    assert flow_paths_template.index.name == "flow_path_name"
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
            if name in flow_paths_template.index
        ]
    )
    assert len(flow_paths_template) == 14


def test_flow_paths_templater_single_region(workbook_table_cache_test_path: Path):
    flow_paths_template = template_flow_paths(
        workbook_table_cache_test_path, "single_region"
    )
    assert flow_paths_template.empty
