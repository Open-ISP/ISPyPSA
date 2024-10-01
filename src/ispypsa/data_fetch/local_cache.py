from pathlib import Path

from isp_workbook_parser import Parser

_ALL_GENERATOR_TYPES = [
    "existing_generators",
    "committed_generators",
    "anticipated_projects",
    "additional_projects",
    "new_entrants",
    "existing_committed_and_anticipated_batteries",
]

_CONDENSED_GENERATOR_TYPES = [
    "existing_committed_anticipated_additional_generators",
    "new_entrants",
]


_GENERATOR_PROPERTIES = {
    "maximum_capacity": _ALL_GENERATOR_TYPES,
    "seasonal_ratings": _ALL_GENERATOR_TYPES,
    "maintenance": ["existing_generators"],
    "fixed_opex": _CONDENSED_GENERATOR_TYPES,
    "variable_opex": _CONDENSED_GENERATOR_TYPES,
    "marginal_loss_factors": _ALL_GENERATOR_TYPES,
    "auxiliary_load": _CONDENSED_GENERATOR_TYPES,
}

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
    "existing_generator_summary",
    "committed_generator_summary",
    "anticipated_projects_summary",
    "batteries_summary",
    "additional_projects_summary",
    "new_entrants_summary",
    "expected_closure_years",
] + _GENERATOR_PROPERTY_TABLES


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
