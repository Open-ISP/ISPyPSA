from pathlib import Path

import yaml

from ispypsa.data_fetch.local_cache import REQUIRED_TABLES, build_local_cache
from ispypsa.logging import configure_logging
from ispypsa.templater.flow_paths import template_flow_paths
from ispypsa.templater.nodes import (
    template_nodes,
    template_regional_sub_regional_mapping,
)
from ispypsa.templater.static_generator_properties import (
    _template_ecaa_generators_static_properties,
)
from ispypsa.templater.dynamic_generator_properties import (
    template_generator_dynamic_properties,
)
from ispypsa.config.validators import validate_config
from ispypsa.templater.renewable_energy_zones import (
    template_renewable_energy_zone_locations,
)
from ispypsa.templater.regions_and_zones import template_region_and_zone_mapping

_PARSED_WORKBOOK_CACHE = Path("model_data", "workbook_table_cache")
_ISPYPSA_INPUTS_DIRECTORY = Path("model_data", "ispypsa_inputs")
_CONFIG_PATH = Path("model_data", "ispypsa_inputs", "ispypsa_config.yaml")

configure_logging()


def build_parsed_workbook_cache(cache_location: Path) -> None:
    if not cache_location.exists():
        cache_location.mkdir(parents=True)
    workbook_path = list(
        Path(Path.cwd().parent, "isp-workbook-parser", "workbooks", "6.0").glob(
            "*.xlsx"
        )
    ).pop()
    build_local_cache(cache_location, workbook_path)


def create_ispypsa_inputs_from_config(
    config_location: Path, workbook_cache_location: Path, template_location: Path
) -> None:
    with open(config_location, "r") as file:
        config = yaml.safe_load(file)
    validate_config(config)
    if not template_location.exists():
        template_location.mkdir(parents=True)

    node_template = template_nodes(
        workbook_cache_location, config["network"]["granularity"]
    )

    if config["network"]["granularity"] == "regional":
        regional_sub_regional_mapping = template_regional_sub_regional_mapping(
            workbook_cache_location
        )
        regional_sub_regional_mapping.to_csv(
            Path(template_location, "regional_sub_regional_mapping.csv")
        )

    renewable_energy_zone_locations = template_renewable_energy_zone_locations(
        workbook_cache_location
    )

    region_and_zone_mapping_template = template_region_and_zone_mapping(
        workbook_cache_location
    )
    flow_path_template = template_flow_paths(
        workbook_cache_location, config["network"]["granularity"]
    )
    ecaa_generators_template = _template_ecaa_generators_static_properties(
        workbook_cache_location
    )
    dynamic_generator_property_templates = template_generator_dynamic_properties(
        workbook_cache_location, config["scenario"]
    )
    if node_template is not None:
        node_template.to_csv(Path(template_location, "nodes.csv"))
    if renewable_energy_zone_locations is not None:
        renewable_energy_zone_locations.to_csv(
            Path(template_location, "renewable_energy_zone_locations.csv")
        )
    if flow_path_template is not None:
        flow_path_template.to_csv(Path(template_location, "flow_paths.csv"))
    if ecaa_generators_template is not None:
        ecaa_generators_template.to_csv(Path(template_location, "ecaa_generators.csv"))
    if dynamic_generator_property_templates is not None:
        for gen_property in dynamic_generator_property_templates.keys():
            dynamic_generator_property_templates[gen_property].to_csv(
                Path(template_location, f"{gen_property}.csv")
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


def task_create_ispypsa_inputs():
    return {
        "actions": [
            (
                create_ispypsa_inputs_from_config,
                [_CONFIG_PATH, _PARSED_WORKBOOK_CACHE, _ISPYPSA_INPUTS_DIRECTORY],
            )
        ],
        "file_dep": [_CONFIG_PATH]
        + [Path(_PARSED_WORKBOOK_CACHE, table + ".csv") for table in REQUIRED_TABLES],
        "targets": [
            Path(_ISPYPSA_INPUTS_DIRECTORY, "nodes.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "regional_sub_regional_mapping.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "flow_paths.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "ecaa_generators.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "coal_prices.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "gas_prices.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "liquid_fuel_prices.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "full_outage_forecasts.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "partial_outage_forecasts.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "seasonal_ratings.csv"),
            Path(_ISPYPSA_INPUTS_DIRECTORY, "renewable_energy_zone_locations.csv"),
            Path(_TEMPLATE_DIRECTORY, "region_and_zone_mapping.csv"),
        ],
    }
