from pathlib import Path

from isp_workbook_parser import Parser

REQUIRED_TABLES = [
    "sub_regional_reference_nodes",
    "existing_generator_summary",
    "regional_topology_representation",
    "regional_reference_nodes",
]


def build_local_cache(cache_path: Path | str, workbook_path: Path | str) -> None:
    """Uses `isp-workbook-parser` to build a local cache of parsed workbook CSVs

    Args:
        cache_path: Path that should be created for the local cache
        workbook_path: Path to an ISP Assumptions Workbook that is supported by
            `isp-workbook-parser`
    """
    if not (cache := Path(cache_path)).exists():
        cache.mkdir(parents=True)
    workbook = Parser(Path(workbook_path))
    tables_to_get = REQUIRED_TABLES
    workbook.save_tables(cache_path, tables=tables_to_get)
    return None
