import argparse
import logging
import os
from pathlib import Path
from shutil import rmtree

import pypsa
from doit import create_after

from ispypsa.config import ModelConfig, load_config
from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.iasr_table_caching import build_local_cache, list_cache_files
from ispypsa.logging import configure_logging
from ispypsa.model import build_pypsa_network, update_network_timeseries
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    list_templater_output_files,
    load_manually_extracted_tables,
)
from ispypsa.translator import (
    create_pypsa_friendly_inputs,
    create_pypsa_friendly_snapshots,
    create_pypsa_friendly_timeseries_inputs,
    list_timeseries_files,
    list_translator_output_files,
)


# Parse command line arguments for config path
def get_config_path():
    # Check if config path was passed via environment variable (from ispypsa CLI)
    env_config_path = os.environ.get("ISPYPSA_CONFIG_PATH")
    if env_config_path:
        return Path(env_config_path)

    # Otherwise parse command line arguments
    parser = argparse.ArgumentParser(description="Run ISPyPSA workflow tasks")
    parser.add_argument(
        "--config",
        type=str,
        default="ispypsa_runs/development/ispypsa_inputs/ispypsa_config.yaml",
        help="Path to the ISPyPSA configuration file (default: ispypsa_runs/development/ispypsa_inputs/ispypsa_config.yaml)",
    )

    # Only parse known args to avoid conflicts with dodo command line args
    args, unknown = parser.parse_known_args()
    config_path = Path(args.config)

    # If running via ispypsa CLI and we have an original working directory,
    # resolve relative config paths relative to the original directory
    original_cwd = os.environ.get("ISPYPSA_ORIGINAL_CWD")
    if original_cwd and not config_path.is_absolute():
        config_path = Path(original_cwd) / config_path

    return config_path


_CONFIG_PATH = get_config_path()
config = load_config(_CONFIG_PATH)


# Load base paths from config, resolving relative paths correctly
def resolve_path(path_str):
    """Resolve a path string, handling relative paths correctly when called via ispypsa
    CLI.
    """
    path = Path(path_str)
    if not path.is_absolute():
        original_cwd = os.environ.get("ISPYPSA_ORIGINAL_CWD")
        if original_cwd:
            # Resolve relative to original working directory
            path = Path(original_cwd) / path
    return path


_PARSED_WORKBOOK_CACHE = resolve_path(config.paths.parsed_workbook_cache)
_PARSED_TRACE_DIRECTORY = resolve_path(config.paths.parsed_traces_directory)
_WORKBOOK_PATH = resolve_path(config.paths.workbook_path)
_RUN_DIRECTORY = resolve_path(config.paths.run_directory) / config.ispypsa_run_name

# Construct full paths from base paths
_ISPYPSA_INPUT_TABLES_DIRECTORY = _RUN_DIRECTORY / "ispypsa_inputs" / "tables"
_PYPSA_FRIENDLY_DIRECTORY = _RUN_DIRECTORY / "pypsa_friendly"
_CAPACITY_EXPANSION_TIMESERIES_LOCATION = (
    _PYPSA_FRIENDLY_DIRECTORY / "capacity_expansion_timeseries"
)
_OPERATIONAL_TIMESERIES_LOCATION = _PYPSA_FRIENDLY_DIRECTORY / "operational_timeseries"
_PYPSA_OUTPUTS_DIRECTORY = _RUN_DIRECTORY / "outputs"

local_cache_files = list_cache_files(_PARSED_WORKBOOK_CACHE)

ispypsa_input_files = list_templater_output_files(
    config.network.nodes.regional_granularity, _ISPYPSA_INPUT_TABLES_DIRECTORY
)

pypsa_friendly_input_files = list_translator_output_files(_PYPSA_FRIENDLY_DIRECTORY)

capacity_expansion_pypsa_file = Path(
    _PYPSA_OUTPUTS_DIRECTORY, f"{config.ispypsa_run_name}_capacity_expansion.nc"
)
operational_pypsa_file = Path(
    _PYPSA_OUTPUTS_DIRECTORY, f"{config.ispypsa_run_name}_operational.nc"
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


def build_parsed_workbook_cache() -> None:
    version = config.iasr_workbook_version

    # Verify the workbook file exists
    if not _PARSED_WORKBOOK_CACHE.exists():
        raise FileNotFoundError(f"Workbook file not found: {_PARSED_WORKBOOK_CACHE}")

    # Verify it's an Excel file
    if _PARSED_WORKBOOK_CACHE.suffix.lower() != ".xlsx":
        raise ValueError(
            f"Workbook path must point to a .xlsx file, got: {_PARSED_WORKBOOK_CACHE}"
        )

    build_local_cache(_PARSED_WORKBOOK_CACHE, _PARSED_WORKBOOK_CACHE, version)


def create_ispypsa_inputs_from_config() -> None:
    create_or_clean_task_output_folder(_ISPYPSA_INPUT_TABLES_DIRECTORY)

    iasr_tables = read_csvs(_PARSED_WORKBOOK_CACHE)

    manually_extracted_tables = load_manually_extracted_tables(
        config.iasr_workbook_version
    )

    template = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manually_extracted_tables,
    )
    write_csvs(template, _ISPYPSA_INPUT_TABLES_DIRECTORY)


def create_pypsa_inputs_for_capacity_expansion_model() -> None:
    create_or_clean_task_output_folder(_PYPSA_FRIENDLY_DIRECTORY)

    ispypsa_tables = read_csvs(_ISPYPSA_INPUT_TABLES_DIRECTORY)
    pypsa_tables = create_pypsa_friendly_inputs(config, ispypsa_tables)
    write_csvs(pypsa_tables, _PYPSA_FRIENDLY_DIRECTORY)

    # Create capacity expansion timeseries
    create_pypsa_friendly_timeseries_inputs(
        config,
        "capacity_expansion",
        ispypsa_tables,
        pypsa_tables["snapshots"],
        _PARSED_TRACE_DIRECTORY,
        _CAPACITY_EXPANSION_TIMESERIES_LOCATION,
    )


def create_and_run_capacity_expansion_model(
    dont_run: bool = False,
) -> None:
    create_or_clean_task_output_folder(capacity_expansion_pypsa_file.parent)

    pypsa_friendly_input_tables = read_csvs(_PYPSA_FRIENDLY_DIRECTORY)

    network = build_pypsa_network(
        pypsa_friendly_input_tables,
        path_to_pypsa_friendly_timeseries_data=_CAPACITY_EXPANSION_TIMESERIES_LOCATION,
    )

    if not dont_run:
        # Never use network.optimize() as this will remove custom constraints.
        network.optimize.solve_model(solver_name=config.solver)

    network.export_to_netcdf(capacity_expansion_pypsa_file)


def create_operational_timeseries() -> None:
    """Create operational timeseries inputs."""
    # Load tables
    ispypsa_tables = read_csvs(_ISPYPSA_INPUT_TABLES_DIRECTORY)

    # Create operational snapshots
    operational_snapshots = create_pypsa_friendly_snapshots(config, "operational")

    # Create operational timeseries
    create_pypsa_friendly_timeseries_inputs(
        config,
        "operational",
        ispypsa_tables,
        operational_snapshots,
        _PARSED_TRACE_DIRECTORY,
        _OPERATIONAL_TIMESERIES_LOCATION,
    )


def create_and_run_operational_model(
    dont_run: bool = False,
) -> None:
    """Create PyPSA network object for operational model."""
    # Load tables
    pypsa_friendly_input_tables = read_csvs(_PYPSA_FRIENDLY_DIRECTORY)

    # Create operational snapshots (needed for update_network_timeseries)
    operational_snapshots = create_pypsa_friendly_snapshots(config, "operational")

    # Load the capacity expansion network
    network = pypsa.Network(capacity_expansion_pypsa_file)

    # Update network timeseries
    update_network_timeseries(
        network,
        pypsa_friendly_input_tables,
        operational_snapshots,
        _OPERATIONAL_TIMESERIES_LOCATION,
    )

    # Fix optimal capacities from capacity expansion
    network.optimize.fix_optimal_capacities()

    if not dont_run:
        # Never use network.optimize() as this will remove custom constraints.
        network.optimize.optimize_with_rolling_horizon(
            horizon=config.temporal.operational.horizon,
            overlap=config.temporal.operational.overlap,
        )

    # Save the network for operational optimization
    network.export_to_hdf5(operational_pypsa_file)


def task_cache_required_iasr_workbook_tables():
    return {
        "actions": [build_parsed_workbook_cache],
        "targets": local_cache_files,
        # force doit to always mark the task as up-to-date (unless target removed)
        # N.B. this will NOT run if target exists but has been modified
        "uptodate": [True],
    }


def task_create_ispypsa_inputs():
    return {
        "actions": [create_ispypsa_inputs_from_config],
        "file_dep": local_cache_files,
        "targets": ispypsa_input_files,
    }


def task_create_pypsa_friendly_inputs():
    def check_targets():
        targets = pypsa_friendly_input_files + list_timeseries_files(
            config,
            read_csvs(_ISPYPSA_INPUT_TABLES_DIRECTORY),
            _CAPACITY_EXPANSION_TIMESERIES_LOCATION,
        )

        for target in targets:
            if not target.exists():
                return False

        return True

    return {
        "actions": [create_pypsa_inputs_for_capacity_expansion_model],
        "file_dep": ispypsa_input_files,
        "uptodate": [check_targets],
    }


@create_after(executed="create_pypsa_friendly_inputs")
def task_create_and_run_capacity_expansion_model():
    deps = pypsa_friendly_input_files + list_timeseries_files(
        config,
        read_csvs(_ISPYPSA_INPUT_TABLES_DIRECTORY),
        _CAPACITY_EXPANSION_TIMESERIES_LOCATION,
    )

    return {
        "actions": [create_and_run_capacity_expansion_model],
        "params": [
            {
                "name": "dont_run",
                "default": False,
            }
        ],
        "task_dep": ["create_pypsa_friendly_inputs"],
        "file_dep": deps,
        "targets": [capacity_expansion_pypsa_file],
    }


@create_after(executed="create_and_run_capacity_expansion_model")
def task_create_operational_timeseries():
    def check_targets():
        targets = pypsa_friendly_input_files + list_timeseries_files(
            config,
            read_csvs(_ISPYPSA_INPUT_TABLES_DIRECTORY),
            _OPERATIONAL_TIMESERIES_LOCATION,
        )

        for target in targets:
            if not target.exists():
                return False

        return True

    return {
        "actions": [create_operational_timeseries],
        "file_dep": ispypsa_input_files,
        "uptodate": [check_targets],
    }


@create_after(executed="create_operational_timeseries")
def task_create_and_run_operational_model():
    deps = pypsa_friendly_input_files + list_timeseries_files(
        config,
        read_csvs(_ISPYPSA_INPUT_TABLES_DIRECTORY),
        _OPERATIONAL_TIMESERIES_LOCATION,
    )

    return {
        "actions": [create_and_run_operational_model],
        "file_dep": deps,
        "targets": [operational_pypsa_file],
    }
