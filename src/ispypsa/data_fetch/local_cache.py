from pathlib import Path

from isp_workbook_parser import Parser
from ..templater.mappings import _GENERATOR_PROPERTIES


_GENERATOR_PROPERTY_TABLES = [
    table_name
    for key, val in _GENERATOR_PROPERTIES.items()
    for table_name in [key + "_" + gen_type for gen_type in val]
]


REQUIRED_TABLES = [
    "sub_regional_reference_nodes",
    "regional_topology_representation",
    "regional_reference_nodes",
    "flow_path_transfer_capability",
    "interconnector_transfer_capability",
    "existing_generators_summary",
    "committed_generators_summary",
    "anticipated_projects_summary",
    "batteries_summary",
    "additional_projects_summary",
    "new_entrants_summary",
    "expected_closure_years",
    "coal_minimum_stable_level",
] + _GENERATOR_PROPERTY_TABLES


def build_local_cache(cache_path: Path | str, workbook_path: Path | str) -> None:
    """Uses `isp-workbook-parser` to build a local cache of parsed workbook CSVs

    Args:
        cache_path: Path that should be created for the local cache
        workbook_path: Path to an ISP Assumptions Workbook that is supported by
            `isp-workbook-parser`
    """
    workbook = Parser(Path(workbook_path))
    tables_to_get = REQUIRED_TABLES
    workbook.save_tables(cache_path, tables=tables_to_get)
    return None
