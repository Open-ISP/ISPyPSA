import logging
from pathlib import Path
from shutil import rmtree

from isp_trace_parser import construct_reference_year_mapping

from ispypsa.config import load_config
from ispypsa.data_fetch.local_cache import REQUIRED_TABLES, build_local_cache
from ispypsa.logging import configure_logging
from ispypsa.model import (
    add_buses_to_network,
    add_carriers_to_network,
    add_ecaa_generators_to_network,
    add_lines_to_network,
    initialise_network,
    run,
    save_results,
)
from ispypsa.templater.dynamic_generator_properties import (
    template_generator_dynamic_properties,
)
from ispypsa.templater.flow_paths import template_flow_paths
from ispypsa.templater.nodes import (
    template_nem_region_to_single_sub_region_mapping,
    template_nodes,
    template_sub_regions_to_nem_regions_mapping,
)
from ispypsa.templater.renewable_energy_zones import (
    template_renewable_energy_zones,
)
from ispypsa.templater.static_generator_properties import (
    _template_ecaa_generators_static_properties,
)
from ispypsa.translator.buses import (
    _translate_buses_demand_timeseries,
    _translate_nodes_to_buses,
)
from ispypsa.translator.generators import (
    _translate_ecaa_generators,
    _translate_generator_timeseries,
)
from ispypsa.translator.lines import translate_flow_paths_to_lines

_PARSED_WORKBOOK_CACHE = Path("model_data", "workbook_table_cache")
_ISPYPSA_INPUT_TABLES_DIRECTORY = Path("model_data", "ispypsa_inputs", "tables")
_PYPSA_FRIENDLY_DIRECTORY = Path("model_data", "pypsa_friendly")
_PARSED_TRACE_DIRECTORY = Path("/home/abi/isp-traces/parsed-traces/")
_CONFIG_PATH = Path("model_data", "ispypsa_inputs", "ispypsa_config.yaml")
_PYPSA_OUTPUTS_DIRECTORY = Path("model_data", "outputs")

configure_logging()


def create_or_clean_task_output_folder(output_folder: Path) -> None:
    if not output_folder.exists():
        output_folder.mkdir(parents=True)
    else:
        logging.info(f"Deleting previous outputs in {output_folder}")
        for item in output_folder.iterdir():
            if item.is_dir():
                rmtree(item)
            elif item.is_file():
                item.unlink()


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
    config = load_config(config_location)
    create_or_clean_task_output_folder(template_location)
    node_template = template_nodes(
        workbook_cache_location, config.network.nodes.regional_granularity
    )
    renewable_energy_zone_location_mapping = template_renewable_energy_zones(
        workbook_cache_location, location_mapping_only=True
    )
    sub_regions_to_nem_regions_mapping = template_sub_regions_to_nem_regions_mapping(
        workbook_cache_location
    )
    nem_region_to_single_sub_region_mapping = (
        template_nem_region_to_single_sub_region_mapping(workbook_cache_location)
    )
    flow_path_template = template_flow_paths(
        workbook_cache_location, config.network.nodes.regional_granularity
    )
    ecaa_generators_template = _template_ecaa_generators_static_properties(
        workbook_cache_location
    )
    dynamic_generator_property_templates = template_generator_dynamic_properties(
        workbook_cache_location, config.scenario
    )
    if node_template is not None:
        node_template.to_csv(Path(template_location, "nodes.csv"))
    if renewable_energy_zone_location_mapping is not None:
        renewable_energy_zone_location_mapping.to_csv(
            Path(template_location, "mapping_renewable_energy_zone_locations.csv")
        )
    if sub_regions_to_nem_regions_mapping is not None:
        sub_regions_to_nem_regions_mapping.to_csv(
            Path(template_location, "mapping_sub_regions_to_nem_regions.csv")
        )
    if nem_region_to_single_sub_region_mapping is not None:
        nem_region_to_single_sub_region_mapping.to_csv(
            Path(template_location, "mapping_nem_region_to_single_sub_region.csv")
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


def create_pypsa_inputs_from_config_and_ispypsa_inputs(
    config_location: Path,
    ispypsa_inputs_location: Path,
    trace_data_path: Path,
    pypsa_inputs_location: Path,
) -> None:
    config = load_config(config_location)
    create_or_clean_task_output_folder(pypsa_inputs_location)
    pypsa_inputs = {}
    pypsa_inputs["generators"] = _translate_ecaa_generators(
        ispypsa_inputs_location, config.network.nodes.regional_granularity
    )
    pypsa_inputs["buses"] = _translate_nodes_to_buses(
        ispypsa_inputs_location,
    )
    pypsa_inputs["lines"] = translate_flow_paths_to_lines(
        ispypsa_inputs_location,
    )
    for name, table in pypsa_inputs.items():
        table.to_csv(Path(pypsa_inputs_location, f"{name}.csv"))
    reference_year_mapping = construct_reference_year_mapping(
        start_year=config.traces.start_year,
        end_year=config.traces.end_year,
        reference_years=config.traces.reference_year_cycle,
    )
    _translate_generator_timeseries(
        ispypsa_inputs_location,
        trace_data_path,
        pypsa_inputs_location,
        generator_type="solar",
        reference_year_mapping=reference_year_mapping,
        year_type=config.traces.year_type,
    )
    _translate_generator_timeseries(
        ispypsa_inputs_location,
        trace_data_path,
        pypsa_inputs_location,
        generator_type="wind",
        reference_year_mapping=reference_year_mapping,
        year_type=config.traces.year_type,
    )
    _translate_buses_demand_timeseries(
        ispypsa_inputs_location,
        trace_data_path,
        pypsa_inputs_location,
        scenario=config.scenario,
        regional_granularity=config.network.nodes.regional_granularity,
        reference_year_mapping=reference_year_mapping,
        year_type=config.traces.year_type,
    )


def create_and_run_pypsa_model(
    config_location: Path, pypsa_inputs_location: Path, pypsa_outputs_location: Path
) -> None:
    config = load_config(config_location)
    create_or_clean_task_output_folder(pypsa_outputs_location)
    network = initialise_network(
        config.traces.start_year,
        config.traces.end_year,
        config.traces.year_type,
        config.operational_temporal_resolution_min,
    )
    add_carriers_to_network(network, pypsa_inputs_location)
    add_buses_to_network(network, pypsa_inputs_location)
    add_lines_to_network(network, pypsa_inputs_location)
    add_ecaa_generators_to_network(network, pypsa_inputs_location)
    run(network, solver_name=config.solver)
    save_results(network, pypsa_outputs_location)


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
                [_CONFIG_PATH, _PARSED_WORKBOOK_CACHE, _ISPYPSA_INPUT_TABLES_DIRECTORY],
            )
        ],
        "file_dep": [_CONFIG_PATH]
        + [Path(_PARSED_WORKBOOK_CACHE, table + ".csv") for table in REQUIRED_TABLES],
        "targets": [
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "nodes.csv"),
            Path(
                _ISPYPSA_INPUT_TABLES_DIRECTORY,
                "mapping_renewable_energy_zone_locations.csv",
            ),
            Path(
                _ISPYPSA_INPUT_TABLES_DIRECTORY,
                "mapping_sub_regions_to_nem_regions.csv",
            ),
            Path(
                _ISPYPSA_INPUT_TABLES_DIRECTORY,
                "mapping_nem_region_to_single_sub_region.csv",
            ),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "flow_paths.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "ecaa_generators.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "coal_prices.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "gas_prices.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "liquid_fuel_prices.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "full_outage_forecasts.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "partial_outage_forecasts.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "seasonal_ratings.csv"),
        ],
    }


def task_create_pypsa_inputs():
    return {
        "actions": [
            (
                create_pypsa_inputs_from_config_and_ispypsa_inputs,
                [
                    _CONFIG_PATH,
                    _ISPYPSA_INPUT_TABLES_DIRECTORY,
                    _PARSED_TRACE_DIRECTORY,
                    _PYPSA_FRIENDLY_DIRECTORY,
                ],
            )
        ],
        "file_dep": [
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "nodes.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "flow_paths.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "ecaa_generators.csv"),
        ],
        "targets": [Path(_PYPSA_FRIENDLY_DIRECTORY, "generators.csv")],
    }


def task_create_and_run_pypsa_model():
    return {
        "actions": [
            (
                create_and_run_pypsa_model,
                [_CONFIG_PATH, _PYPSA_FRIENDLY_DIRECTORY, _PYPSA_OUTPUTS_DIRECTORY],
            )
        ],
        "file_dep": [
            Path(_PYPSA_FRIENDLY_DIRECTORY, "buses.csv"),
            Path(_PYPSA_FRIENDLY_DIRECTORY, "lines.csv"),
            Path(_PYPSA_FRIENDLY_DIRECTORY, "generators.csv"),
        ],
        "targets": [Path(_PYPSA_OUTPUTS_DIRECTORY, "network.hdf5")],
    }
