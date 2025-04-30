import logging
from pathlib import Path
from shutil import rmtree

import pypsa

from ispypsa.config import ModelConfig, load_config
from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.iasr_table_caching import build_local_cache, list_cache_files
from ispypsa.logging import configure_logging
from ispypsa.model import build_pypsa_network, save_results, update_network_timeseries
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    list_templater_output_files,
    load_manually_extracted_tables,
)
from ispypsa.translator import (
    create_pypsa_friendly_dynamic_marginal_costs,
    create_pypsa_friendly_inputs,
    create_pypsa_friendly_snapshots,
    create_pypsa_friendly_timeseries_inputs,
    list_translator_output_files,
)

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
_CAPACITY_EXPANSION_TIMESERIES_LOCATION = Path(
    _PYPSA_FRIENDLY_DIRECTORY, "capacity_expansion_timeseries"
)
_OPERATIONAL_TIMESERIES_LOCATION = Path(
    _PYPSA_FRIENDLY_DIRECTORY, "operational_timeseries"
)
_PARSED_TRACE_DIRECTORY = Path(config.temporal.path_to_parsed_traces)
_PYPSA_OUTPUTS_DIRECTORY = Path(run_folder, "outputs")

local_cache_files = list_cache_files(_PARSED_WORKBOOK_CACHE)

ispypsa_input_files = list_templater_output_files(
    config.network.nodes.regional_granularity, _ISPYPSA_INPUT_TABLES_DIRECTORY
)

pypsa_friendly_input_files = list_translator_output_files(_PYPSA_FRIENDLY_DIRECTORY)

capacity_expansion_output_file = Path(
    _PYPSA_OUTPUTS_DIRECTORY, f"{config.ispypsa_run_name}.h5"
)
operational_output_file = Path(
    _PYPSA_OUTPUTS_DIRECTORY, f"{config.ispypsa_run_name}_operational.h5"
)


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

    iasr_tables = read_csvs(workbook_cache_location)

    manually_extracted_tables = load_manually_extracted_tables(
        config.iasr_workbook_version
    )

    template = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manually_extracted_tables,
    )
    write_csvs(template, template_location)


def create_pypsa_inputs_from_config_and_ispypsa_inputs(
    config: ModelConfig,
    ispypsa_inputs_location: Path,
    trace_data_path: Path,
    pypsa_inputs_location: Path,
) -> None:
    create_or_clean_task_output_folder(pypsa_inputs_location)

    ispypsa_tables = read_csvs(ispypsa_inputs_location)
    pypsa_tables = create_pypsa_friendly_inputs(config, ispypsa_tables)
    write_csvs(pypsa_tables, pypsa_inputs_location)

    # Create capacity expansion timeseries
    create_pypsa_friendly_timeseries_inputs(
        config,
        "capacity_expansion",
        ispypsa_tables,
        pypsa_tables["snapshots"],
        trace_data_path,
        _CAPACITY_EXPANSION_TIMESERIES_LOCATION,
    )
    create_pypsa_friendly_dynamic_marginal_costs(
        ispypsa_tables,
        pypsa_tables["generators"],
        pypsa_tables["snapshots"],
        pypsa_inputs_location,
    )


def create_and_run_capacity_expansion_model(
    config: ModelConfig,
    pypsa_friendly_inputs_location: Path,
    pypsa_outputs_location: Path,
) -> None:
    create_or_clean_task_output_folder(pypsa_outputs_location)

    pypsa_friendly_input_tables = read_csvs(pypsa_friendly_inputs_location)

    network = build_pypsa_network(
        pypsa_friendly_input_tables,
        path_to_pypsa_friendly_timeseries_data=_CAPACITY_EXPANSION_TIMESERIES_LOCATION,
    )
    # Never use network.optimize() as this will remove custom constraints.
    network.optimize.solve_model(solver_name=config.solver)

    # Save results (exports network to HDF5)
    save_results(network, pypsa_outputs_location, config.ispypsa_run_name)


def run_operational_model(
    config: ModelConfig,
    ispypsa_inputs_location: Path,
    pypsa_friendly_inputs_location: Path,
    trace_data_path: Path,
    pypsa_outputs_location: Path,
) -> None:
    # Load tables
    ispypsa_tables = read_csvs(ispypsa_inputs_location)
    pypsa_friendly_input_tables = read_csvs(pypsa_friendly_inputs_location)

    # Create operational snapshots
    operational_snapshots = create_pypsa_friendly_snapshots(config, "operational")

    # Create operational timeseries
    create_pypsa_friendly_timeseries_inputs(
        config,
        "operational",
        ispypsa_tables,
        operational_snapshots,
        trace_data_path,
        _OPERATIONAL_TIMESERIES_LOCATION,
    )

    # Load the capacity expansion network
    network = pypsa.Network(capacity_expansion_output_file)

    # Update network timeseries
    update_network_timeseries(
        network,
        pypsa_friendly_input_tables,
        operational_snapshots,
        _OPERATIONAL_TIMESERIES_LOCATION,
    )

    # Fix optimal capacities from capacity expansion
    network.optimize.fix_optimal_capacities()

    # Never use network.optimize() as this will remove custom constraints.
    network.optimize.optimize_with_rolling_horizon(
        horizon=config.temporal.operational.horizon,
        overlap=config.temporal.operational.overlap,
    )

    # Save operational results
    save_results(
        network, pypsa_outputs_location, config.ispypsa_run_name + "_operational"
    )


def task_cache_required_tables():
    return {
        "actions": [(build_parsed_workbook_cache, [config, _PARSED_WORKBOOK_CACHE])],
        "targets": local_cache_files,
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
        "file_dep": local_cache_files,
        "targets": ispypsa_input_files,
    }


def task_create_pypsa_inputs():
    return {
        "actions": [
            (
                create_pypsa_inputs_from_config_and_ispypsa_inputs,
                [
                    config,
                    _ISPYPSA_INPUT_TABLES_DIRECTORY,
                    _PARSED_TRACE_DIRECTORY,
                    _PYPSA_FRIENDLY_DIRECTORY,
                ],
            )
        ],
        "file_dep": ispypsa_input_files,
        "targets": pypsa_friendly_input_files,
    }


def task_run_capacity_expansion():
    return {
        "actions": [
            (
                create_and_run_capacity_expansion_model,
                [config, _PYPSA_FRIENDLY_DIRECTORY, _PYPSA_OUTPUTS_DIRECTORY],
            )
        ],
        "file_dep": pypsa_friendly_input_files,
        "targets": [capacity_expansion_output_file],
    }


def task_run_operational_model():
    return {
        "actions": [
            (
                run_operational_model,
                [
                    config,
                    _ISPYPSA_INPUT_TABLES_DIRECTORY,
                    _PYPSA_FRIENDLY_DIRECTORY,
                    _PARSED_TRACE_DIRECTORY,
                    _PYPSA_OUTPUTS_DIRECTORY,
                ],
            )
        ],
        "file_dep": [capacity_expansion_output_file] + pypsa_friendly_input_files,
        "targets": [operational_output_file],
    }
