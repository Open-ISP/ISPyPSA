import logging
from pathlib import Path
from shutil import rmtree

import pandas as pd
from isp_trace_parser import construct_reference_year_mapping

from ispypsa.config import load_config
from ispypsa.config.validators import ModelConfig
from ispypsa.data_fetch.local_cache import REQUIRED_TABLES, build_local_cache
from ispypsa.logging import configure_logging
from ispypsa.model import (
    add_buses_for_custom_constraints,
    add_buses_to_network,
    add_carriers_to_network,
    add_custom_constraint_generators_to_network,
    add_custom_constraints,
    add_ecaa_generators_to_network,
    add_lines_to_network,
    initialise_network,
    run,
    save_results,
)
from ispypsa.templater.dynamic_generator_properties import (
    template_generator_dynamic_properties,
)
from ispypsa.templater.energy_policy_targets import (
    template_powering_australia_plan,
    template_renewable_generation_targets,
    template_renewable_share_targets,
    template_technology_capacity_targets,
)
from ispypsa.templater.flow_paths import template_flow_paths
from ispypsa.templater.manual_tables import template_manually_extracted_tables
from ispypsa.templater.nodes import (
    template_nem_region_to_single_sub_region_mapping,
    template_nodes,
    template_sub_regions_to_nem_regions_mapping,
)
from ispypsa.templater.renewable_energy_zones import (
    template_renewable_energy_zones_locations,
    template_rez_build_limits,
)
from ispypsa.templater.static_ecaa_generator_properties import (
    template_ecaa_generators_static_properties,
)
from ispypsa.templater.static_new_generator_properties import (
    template_new_generators_static_properties,
)
from ispypsa.translator.buses import (
    _translate_buses_demand_timeseries,
    _translate_nodes_to_buses,
)
from ispypsa.translator.custom_constraints import (
    _translate_custom_constraint_lhs,
    _translate_custom_constraint_rhs,
    _translate_custom_constraints_generators,
)
from ispypsa.translator.generators import (
    _translate_ecaa_generators,
    _translate_generator_timeseries,
)
from ispypsa.translator.lines import translate_flow_paths_to_lines
from ispypsa.translator.renewable_energy_zones import (
    translate_renewable_energy_zone_build_limits_to_flow_paths,
)
from ispypsa.translator.snapshot import create_complete_snapshot_index
from ispypsa.translator.temporal_filters import filter_snapshot

root_folder = Path("ispypsa_runs")

##### MODIFY FOR DIFFERENT MODEL RUN ####################################################
_CONFIG_PATH = root_folder / Path(
    "development", "ispypsa_inputs", "ispypsa_config.yaml"
)
#########################################################################################

config = load_config(_CONFIG_PATH)
run_folder = Path(root_folder, config.ispypsa_run_name)
_PARSED_WORKBOOK_CACHE = root_folder / Path("workbook_table_cache")
_ISPYPSA_INPUT_TABLES_DIRECTORY = Path(run_folder, "ispypsa_inputs", "tables")
_PYPSA_FRIENDLY_DIRECTORY = Path(run_folder, "pypsa_friendly")
_PARSED_TRACE_DIRECTORY = Path(config.temporal.path_to_parsed_traces)
_PYPSA_OUTPUTS_DIRECTORY = Path(run_folder, "outputs")

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


def build_parsed_workbook_cache(config: ModelConfig, cache_location: Path) -> None:
    if not cache_location.exists():
        cache_location.mkdir(parents=True)
    version = config.iasr_workbook_version
    workbook_path = list(
        Path(Path.cwd().parent, "isp-workbook-parser", "workbooks", version).glob(
            "*.xlsx"
        )
    ).pop()
    build_local_cache(cache_location, workbook_path, version)


def create_ispypsa_inputs_from_config(
    config: ModelConfig, workbook_cache_location: Path, template_location: Path
) -> None:
    create_or_clean_task_output_folder(template_location)
    node_template = template_nodes(
        workbook_cache_location, config.network.nodes.regional_granularity
    )
    renewable_energy_zone_location_mapping = template_renewable_energy_zones_locations(
        workbook_cache_location, location_mapping_only=True
    )
    renewable_energy_zone_build_limits = template_rez_build_limits(
        workbook_cache_location, config.network.rez_transmission_expansion
    )
    sub_regions_to_nem_regions_mapping = template_sub_regions_to_nem_regions_mapping(
        workbook_cache_location
    )
    nem_region_to_single_sub_region_mapping = (
        template_nem_region_to_single_sub_region_mapping(workbook_cache_location)
    )
    flow_path_template = template_flow_paths(
        workbook_cache_location,
        config.iasr_workbook_version,
        config.network.nodes.regional_granularity,
    )
    ecaa_generators_template = template_ecaa_generators_static_properties(
        workbook_cache_location
    )
    new_entrant_generators_template = template_new_generators_static_properties(
        workbook_cache_location
    )
    dynamic_generator_property_templates = template_generator_dynamic_properties(
        workbook_cache_location, config.scenario
    )
    manually_extracted_tables = template_manually_extracted_tables(
        config.iasr_workbook_version
    )
    renewable_share_targets_template = template_renewable_share_targets(
        workbook_cache_location
    )
    powering_australia_plan_template = template_powering_australia_plan(
        workbook_cache_location, config.scenario
    )
    technology_capacity_targets_template = template_technology_capacity_targets(
        workbook_cache_location
    )
    renewable_generation_targets_template = template_renewable_generation_targets(
        workbook_cache_location
    )

    if node_template is not None:
        node_template.to_csv(Path(template_location, "nodes.csv"))
    if renewable_energy_zone_location_mapping is not None:
        renewable_energy_zone_location_mapping.to_csv(
            Path(template_location, "mapping_renewable_energy_zone_locations.csv")
        )
    if renewable_energy_zone_build_limits is not None:
        renewable_energy_zone_build_limits.to_csv(
            Path(template_location, "renewable_energy_zone_build_limits.csv")
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
    if new_entrant_generators_template is not None:
        new_entrant_generators_template.to_csv(
            Path(template_location, "new_entrant_generators.csv")
        )
    if dynamic_generator_property_templates is not None:
        for gen_property in dynamic_generator_property_templates.keys():
            dynamic_generator_property_templates[gen_property].to_csv(
                Path(template_location, f"{gen_property}.csv")
            )
    for name, table in manually_extracted_tables.items():
        table.to_csv(Path(template_location, name))
    if renewable_share_targets_template is not None:
        renewable_share_targets_template.to_csv(
            Path(template_location, "renewable_share_targets.csv")
        )
    if powering_australia_plan_template is not None:
        powering_australia_plan_template.to_csv(
            Path(template_location, "powering_australia_plan.csv")
        )
    if technology_capacity_targets_template is not None:
        technology_capacity_targets_template.to_csv(
            Path(template_location, "technology_capacity_targets.csv")
        )
    if renewable_generation_targets_template is not None:
        renewable_generation_targets_template.to_csv(
            Path(template_location, "renewable_generation_targets.csv")
        )


def create_pypsa_inputs_from_config_and_ispypsa_inputs(
    config: ModelConfig,
    ispypsa_inputs_location: Path,
    trace_data_path: Path,
    pypsa_inputs_location: Path,
) -> None:
    create_or_clean_task_output_folder(pypsa_inputs_location)
    pypsa_inputs = {}
    snapshot = create_complete_snapshot_index(
        start_year=config.temporal.start_year,
        end_year=config.temporal.end_year,
        operational_temporal_resolution_min=config.temporal.operational_temporal_resolution_min,
        year_type=config.temporal.year_type,
    )
    pypsa_inputs["snapshot"] = filter_snapshot(
        config=config.temporal, snapshot=snapshot
    )
    pypsa_inputs["generators"] = _translate_ecaa_generators(
        ispypsa_inputs_location, config.network.nodes.regional_granularity
    )
    pypsa_inputs["buses"] = _translate_nodes_to_buses(
        ispypsa_inputs_location,
    )
    lines_interregional_or_subregional = translate_flow_paths_to_lines(
        ispypsa_inputs_location,
        config.network.transmission_expansion,
        config.wacc,
        config.network.annuitisation_lifetime,
    )
    lines_rez_to_region_or_subregion = (
        translate_renewable_energy_zone_build_limits_to_flow_paths(
            ispypsa_inputs_location,
            config.network.rez_transmission_expansion,
            config.wacc,
            config.network.annuitisation_lifetime,
            config.network.rez_to_sub_region_transmission_default_limit,
        )
    )
    pypsa_inputs["lines"] = pd.concat(
        [lines_interregional_or_subregional, lines_rez_to_region_or_subregion]
    )
    pypsa_inputs["custom_constraints_lhs"] = _translate_custom_constraint_lhs(
        ispypsa_inputs_location
    )
    pypsa_inputs["custom_constraints_rhs"] = _translate_custom_constraint_rhs(
        ispypsa_inputs_location
    )
    pypsa_inputs["custom_constraints_generators"] = (
        _translate_custom_constraints_generators(
            ispypsa_inputs_location,
            config.network.rez_transmission_expansion,
            config.wacc,
            config.network.annuitisation_lifetime,
        )
    )
    for name, table in pypsa_inputs.items():
        table.to_csv(Path(pypsa_inputs_location, f"{name}.csv"))
    reference_year_mapping = construct_reference_year_mapping(
        start_year=config.temporal.start_year,
        end_year=config.temporal.end_year,
        reference_years=config.temporal.reference_year_cycle,
    )
    _translate_generator_timeseries(
        ispypsa_inputs_location,
        trace_data_path,
        pypsa_inputs_location,
        generator_type="solar",
        reference_year_mapping=reference_year_mapping,
        year_type=config.temporal.year_type,
        snapshot=pypsa_inputs["snapshot"],
    )
    _translate_generator_timeseries(
        ispypsa_inputs_location,
        trace_data_path,
        pypsa_inputs_location,
        generator_type="wind",
        reference_year_mapping=reference_year_mapping,
        year_type=config.temporal.year_type,
        snapshot=pypsa_inputs["snapshot"],
    )
    _translate_buses_demand_timeseries(
        ispypsa_inputs_location,
        trace_data_path,
        pypsa_inputs_location,
        scenario=config.scenario,
        regional_granularity=config.network.nodes.regional_granularity,
        reference_year_mapping=reference_year_mapping,
        year_type=config.temporal.year_type,
        snapshot=pypsa_inputs["snapshot"],
    )


def create_and_run_pypsa_model(
    config: ModelConfig, pypsa_inputs_location: Path, pypsa_outputs_location: Path
) -> None:
    network = initialise_network(pypsa_inputs_location)
    add_carriers_to_network(network, pypsa_inputs_location)
    add_buses_to_network(network, pypsa_inputs_location)
    add_buses_for_custom_constraints(network)
    add_lines_to_network(network, pypsa_inputs_location)
    add_custom_constraint_generators_to_network(network, pypsa_inputs_location)
    add_ecaa_generators_to_network(network, pypsa_inputs_location)
    network.optimize.create_model()
    add_custom_constraints(network, pypsa_inputs_location)
    run(network, solver_name=config.solver)
    save_results(network, pypsa_outputs_location)


def task_cache_required_tables():
    return {
        "actions": [(build_parsed_workbook_cache, [config, _PARSED_WORKBOOK_CACHE])],
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
                [config, _PARSED_WORKBOOK_CACHE, _ISPYPSA_INPUT_TABLES_DIRECTORY],
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
                "renewable_energy_zone_build_limits.csv",
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
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "new_entrant_generators.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "coal_prices.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "gas_prices.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "liquid_fuel_prices.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "full_outage_forecasts.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "partial_outage_forecasts.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "seasonal_ratings.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "closure_years.csv"),
            Path(
                _ISPYPSA_INPUT_TABLES_DIRECTORY,
                "rez_group_constraints_expansion_costs.csv",
            ),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "rez_group_constraints_lhs.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "rez_group_constraints_rhs.csv"),
            Path(
                _ISPYPSA_INPUT_TABLES_DIRECTORY,
                "rez_transmission_limit_constraints_expansion_costs.csv",
            ),
            Path(
                _ISPYPSA_INPUT_TABLES_DIRECTORY,
                "rez_transmission_limit_constraints_lhs.csv",
            ),
            Path(
                _ISPYPSA_INPUT_TABLES_DIRECTORY,
                "rez_transmission_limit_constraints_rhs.csv",
            ),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "renewable_share_targets.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "powering_australia_plan.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "technology_capacity_targets.csv"),
            Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "renewable_generation_targets.csv"),
        ],
    }


# def task_create_pypsa_inputs():
#     return {
#         "actions": [
#             (
#                 create_pypsa_inputs_from_config_and_ispypsa_inputs,
#                 [
#                     config,
#                     _ISPYPSA_INPUT_TABLES_DIRECTORY,
#                     _PARSED_TRACE_DIRECTORY,
#                     _PYPSA_FRIENDLY_DIRECTORY,
#                 ],
#             )
#         ],
#         "file_dep": [
#             Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "nodes.csv"),
#             Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "flow_paths.csv"),
#             Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "ecaa_generators.csv"),
#             Path(
#                 _ISPYPSA_INPUT_TABLES_DIRECTORY,
#                 "rez_group_constraints_expansion_costs.csv",
#             ),
#             Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "rez_group_constraints_lhs.csv"),
#             Path(_ISPYPSA_INPUT_TABLES_DIRECTORY, "rez_group_constraints_rhs.csv"),
#             Path(
#                 _ISPYPSA_INPUT_TABLES_DIRECTORY,
#                 "rez_transmission_limit_constraints_expansion_costs.csv",
#             ),
#             Path(
#                 _ISPYPSA_INPUT_TABLES_DIRECTORY,
#                 "rez_transmission_limit_constraints_lhs.csv",
#             ),
#             Path(
#                 _ISPYPSA_INPUT_TABLES_DIRECTORY,
#                 "rez_transmission_limit_constraints_rhs.csv",
#             ),
#         ],
#         "targets": [
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "snapshot.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "buses.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "lines.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "generators.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "custom_constraints_lhs.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "custom_constraints_rhs.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "custom_constraints_generators.csv"),
#         ],
#     }


# def task_create_and_run_pypsa_model():
#     return {
#         "actions": [
#             (
#                 create_and_run_pypsa_model,
#                 [config, _PYPSA_FRIENDLY_DIRECTORY, _PYPSA_OUTPUTS_DIRECTORY],
#             )
#         ],
#         "file_dep": [
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "snapshot.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "buses.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "lines.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "generators.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "custom_constraints_lhs.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "custom_constraints_rhs.csv"),
#             Path(_PYPSA_FRIENDLY_DIRECTORY, "custom_constraints_generators.csv"),
#         ],
#         "targets": [Path(_PYPSA_OUTPUTS_DIRECTORY, "network.hdf5")],
#     }
