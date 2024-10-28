from pathlib import Path
import yaml

from ispypsa.data_fetch.local_cache import REQUIRED_TABLES, build_local_cache
from ispypsa.templater.nodes import template_nodes
from ispypsa.templater.flow_paths import template_flow_paths
from ispypsa.templater.generators import _template_ecaa_generators

_PARSED_WORKBOOK_CACHE = Path("model_inputs", "workbook_table_cache")
_TEMPLATE_DIRECTORY = Path("model_inputs", "template")
_CONFIG_PATH = Path("model_inputs", "ispypsa_config.yaml")


def build_parsed_workbook_cache(cache_location: Path) -> None:
    if not cache_location.exists():
        cache_location.mkdir(parents=True)
    workbook_path = list(
        Path(Path.cwd().parent, "isp-workbook-parser", "workbooks", "6.0").glob(
            "*.xlsx"
        )
    ).pop()
    build_local_cache(cache_location, workbook_path)


def create_template_from_config(
    config_location: Path, workbook_cache_location: Path, template_location: Path
) -> None:
    with open(config_location, "r") as file:
        config = yaml.safe_load(file)
    if not template_location.exists():
        template_location.mkdir(parents=True)
    node_template = template_nodes(
        workbook_cache_location, config["network"]["granularity"]
    )
    flow_path_template = template_flow_paths(
        workbook_cache_location, config["network"]["granularity"]
    )
    ecaa_generators_template = _template_ecaa_generators(workbook_cache_location)
    if node_template is not None:
        node_template.to_csv(Path(template_location, "node_template.csv"))
    if flow_path_template is not None:
        flow_path_template.to_csv(Path(template_location, "flow_paths_template.csv"))
    if ecaa_generators_template is not None:
        ecaa_generators_template.to_csv(
            Path(template_location, "ecaa_generators_template.csv")
        )


def task_cache_required_tables():
    return {
        "actions": [(build_parsed_workbook_cache, [_PARSED_WORKBOOK_CACHE])],
        "targets": [
            Path(_PARSED_WORKBOOK_CACHE, table + ".csv") for table in REQUIRED_TABLES
        ],
        # force doit to always mark the task as up-to-date (unless target removed)
        # N.B. this will NOT run if target exists but has been modified
        "uptodate": [True],
    }


def task_create_template():
    return {
        "actions": [
            (
                create_template_from_config,
                [_CONFIG_PATH, _PARSED_WORKBOOK_CACHE, _TEMPLATE_DIRECTORY],
            )
        ],
        "file_dep": [_CONFIG_PATH]
        + [Path(_PARSED_WORKBOOK_CACHE, table + ".csv") for table in REQUIRED_TABLES],
        "targets": [
            Path(_TEMPLATE_DIRECTORY, "node_template.csv"),
            Path(_TEMPLATE_DIRECTORY, "flow_paths_template.csv"),
            Path(_TEMPLATE_DIRECTORY, "ecaa_generators_template.csv"),
        ],
    }
