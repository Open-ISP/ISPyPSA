import logging
import os
import shutil
from pathlib import Path
from shutil import copy2, rmtree

import pypsa
from doit import create_after, get_var

from ispypsa.config import load_config
from ispypsa.data_fetch import (
    fetch_trace_data,
    fetch_workbook,
    read_csvs,
    write_csvs,
)
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

config_path = get_var("config", None)

if config_path:
    config = load_config(Path(config_path))
else:
    config = None


def check_config_present():
    if not config:
        raise ValueError(
            "Config path required for task execution. Use config parameter:\n"
            "  doit config=path/to/config.yaml TASK\n"
            "  ispypsa config=path/to/config.yaml TASK"
        )


def get_run_directory():
    """Get run directory path."""
    return Path(config.paths.run_directory) / config.paths.ispypsa_run_name


def get_parsed_workbook_cache():
    """Get parsed workbook cache path."""
    return Path(config.paths.parsed_workbook_cache)


def get_parsed_trace_directory():
    """Get parsed trace directory path with isp_{year} appended."""
    base_path = Path(config.paths.parsed_traces_directory)
    year_suffix = f"isp_{config.trace_data.dataset_year}"
    return base_path / year_suffix


def get_ispypsa_input_directory():
    """Get ISPyPSA input tables directory path."""
    return get_run_directory() / "ispypsa_inputs"


def get_ispypsa_input_tables_directory():
    """Get ISPyPSA input tables directory path."""
    return get_ispypsa_input_directory()


def get_pypsa_friendly_directory():
    """Get PyPSA friendly directory path."""
    return get_run_directory() / "pypsa_friendly"


def get_capacity_expansion_timeseries_location():
    """Get capacity expansion timeseries location path."""
    return get_pypsa_friendly_directory() / "capacity_expansion_timeseries"


def get_operational_timeseries_location():
    """Get operational timeseries location path."""
    return get_pypsa_friendly_directory() / "operational_timeseries"


def get_pypsa_outputs_directory():
    """Get PyPSA outputs directory path."""
    return get_run_directory() / "outputs"


def return_empty_list_if_no_config(func):
    def wrapper(*args, **kwargs):
        if not config:
            return []
        else:
            return func(*args, **kwargs)

    return wrapper


@return_empty_list_if_no_config
def get_config_save_path():
    """Get config save file path."""
    config_file_path = Path(config_path)
    run_dir = get_run_directory()
    config_copy_path = run_dir / config_file_path.name
    return config_copy_path


@return_empty_list_if_no_config
def get_capacity_expansion_pypsa_file():
    """Get capacity expansion PyPSA file path."""
    return get_pypsa_outputs_directory() / "capacity_expansion.nc"


@return_empty_list_if_no_config
def get_operational_pypsa_file():
    """Get operational PyPSA file path."""
    return get_pypsa_outputs_directory() / "operational.nc"


@return_empty_list_if_no_config
def get_local_cache_files():
    """Get list of local cache files."""
    return list_cache_files(get_parsed_workbook_cache())


@return_empty_list_if_no_config
def get_workbook_path():
    """Get workbook file path."""
    return Path(config.paths.workbook_path)


@return_empty_list_if_no_config
def get_ispypsa_input_files():
    """Get list of ISPyPSA input files."""
    check_config_present()
    return list_templater_output_files(
        config.network.nodes.regional_granularity, get_ispypsa_input_tables_directory()
    )


@return_empty_list_if_no_config
def get_pypsa_friendly_input_files():
    """Get list of PyPSA friendly input files."""
    return list_translator_output_files(get_pypsa_friendly_directory())


@return_empty_list_if_no_config
def get_capacity_expansion_timeseries_files():
    """Get list of capacity expansion timeseries files."""
    check_config_present()
    ispypsa_tables = read_csvs(get_ispypsa_input_tables_directory())
    return list_timeseries_files(
        config, ispypsa_tables, get_capacity_expansion_timeseries_location()
    )


@return_empty_list_if_no_config
def get_operational_timeseries_files():
    """Get list of operational timeseries files."""
    check_config_present()
    ispypsa_tables = read_csvs(get_ispypsa_input_tables_directory())
    return list_timeseries_files(
        config, ispypsa_tables, get_operational_timeseries_location()
    )


def configure_logging_for_run() -> None:
    """Configure logging to use run directory."""
    run_dir = get_run_directory()
    # Ensure run directory exists before configuring logging
    run_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = run_dir / "ISPyPSA.log"
    configure_logging(log_file=str(log_file_path))


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
    check_config_present()
    configure_logging_for_run()
    parsed_workbook_cache = get_parsed_workbook_cache()
    workbook_path = get_workbook_path()
    version = config.iasr_workbook_version

    # Check if we're in test mode and should skip actual workbook parsing
    if os.environ.get("ISPYPSA_TEST_MOCK_CACHE", "").lower() == "true":
        # In test mode, just ensure cache directory exists and copy pre-existing files
        parsed_workbook_cache.mkdir(parents=True, exist_ok=True)
        # Copy any existing test cache files if they don't already exist
        test_cache_dir = (
            Path(__file__).parent.parent.parent.parent
            / "tests"
            / "test_workbook_table_cache"
        )
        if test_cache_dir.exists():
            for csv_file in test_cache_dir.glob("*.csv"):
                target_file = parsed_workbook_cache / csv_file.name
                if not target_file.exists():
                    shutil.copy2(csv_file, target_file)
        return

    # Verify the workbook file exists
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook file not found: {workbook_path}")

    # Verify it's an Excel file
    if workbook_path.suffix.lower() != ".xlsx":
        raise ValueError(
            f"Workbook path must point to a .xlsx file, got: {workbook_path}"
        )

    build_local_cache(parsed_workbook_cache, workbook_path, version)


def download_workbook_from_config() -> None:
    """Download ISP workbook from manifest.

    Can be run with direct parameters or with config file:
    - Direct: ispypsa workbook_version=6.0 workbook_path=path/to/workbook.xlsx download_workbook
    - Config: ispypsa config=config.yaml download_workbook
    """
    # Check if direct parameters are provided
    direct_version = get_var("workbook_version", None)
    direct_path = get_var("workbook_path", None)

    if direct_version and direct_path:
        # Use direct parameters (no config needed)
        version = direct_version
        workbook_path = Path(direct_path)
        # Simple logging setup for direct mode
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    else:
        # Use config file
        check_config_present()
        configure_logging_for_run()
        workbook_path = get_workbook_path()
        version = config.iasr_workbook_version

    # Validate path
    if not str(workbook_path).endswith(".xlsx"):
        raise ValueError(f"workbook_path must end with .xlsx, got: {workbook_path}")

    # Log start
    logging.info(f"Downloading ISP workbook version {version} to {workbook_path}")

    # Download (fetch_workbook handles directory creation and silently overwrites)
    fetch_workbook(version, workbook_path)

    # Log completion
    logging.info("Workbook download completed successfully")


def download_trace_data_from_config() -> None:
    """Download trace data from manifest.

    Can be run with direct parameters or with config file:
    - Direct: ispypsa save_directory=path/to/traces trace_dataset_type=example download_trace_data
    - Config: ispypsa config=config.yaml download_trace_data
    """
    # Check if direct save_directory parameter is provided
    direct_save_dir = get_var("save_directory", None)

    if direct_save_dir:
        # Use direct parameters (no config needed)
        trace_dir = Path(direct_save_dir)
        # Get parameters from command line with defaults
        dataset_type = get_var("trace_dataset_type", "example")
        dataset_year = int(get_var("trace_dataset_year", "2024"))
        # Simple logging setup for direct mode
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    else:
        # Use config file
        check_config_present()
        configure_logging_for_run()
        trace_dir = get_parsed_trace_directory()
        # Get parameters from config, with command line override
        dataset_type = get_var("trace_dataset_type", config.trace_data.dataset_type)
        dataset_year = int(
            get_var("trace_dataset_year", config.trace_data.dataset_year)
        )

    # Validate dataset_type
    if dataset_type not in ["full", "example"]:
        raise ValueError(
            f"trace_dataset_type must be 'full' or 'example', got: {dataset_type}"
        )

    # Log start
    logging.info(
        f"Downloading {dataset_type} trace data for {dataset_year} to {trace_dir}"
    )

    # Download (fetch_trace_data handles directory creation)
    # Note: Partial downloads from network errors will be left as-is
    fetch_trace_data(dataset_type, dataset_year, trace_dir)

    # Log completion
    logging.info("Trace data download completed successfully")


def save_config_file() -> None:
    """Save a copy of the configuration file to the run directory."""
    check_config_present()
    config_file_path = Path(config_path)
    run_dir = get_run_directory()

    # Ensure run directory exists
    run_dir.mkdir(parents=True, exist_ok=True)

    # Copy config file to run directory
    config_copy_path = run_dir / config_file_path.name
    copy2(config_file_path, config_copy_path)


def create_ispypsa_inputs_from_config() -> None:
    check_config_present()
    configure_logging_for_run()
    input_tables_dir = get_ispypsa_input_tables_directory()
    parsed_workbook_cache = get_parsed_workbook_cache()

    create_or_clean_task_output_folder(input_tables_dir)

    iasr_tables = read_csvs(parsed_workbook_cache)

    manually_extracted_tables = load_manually_extracted_tables(
        config.iasr_workbook_version
    )

    template = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manually_extracted_tables,
        filter_to_nem_regions=config.filter_by_nem_regions,
        filter_to_isp_sub_regions=config.filter_by_isp_sub_regions,
    )
    write_csvs(template, input_tables_dir)


def create_pypsa_inputs_for_capacity_expansion_model() -> None:
    check_config_present()
    configure_logging_for_run()
    pypsa_friendly_dir = get_pypsa_friendly_directory()
    input_tables_dir = get_ispypsa_input_tables_directory()
    parsed_trace_dir = get_parsed_trace_directory()
    capacity_expansion_timeseries_location = (
        get_capacity_expansion_timeseries_location()
    )

    create_or_clean_task_output_folder(pypsa_friendly_dir)

    ispypsa_tables = read_csvs(input_tables_dir)
    pypsa_tables = create_pypsa_friendly_inputs(config, ispypsa_tables)
    write_csvs(pypsa_tables, pypsa_friendly_dir)

    # Create capacity expansion timeseries
    create_pypsa_friendly_timeseries_inputs(
        config,
        "capacity_expansion",
        ispypsa_tables,
        pypsa_tables["snapshots"],
        parsed_trace_dir,
        capacity_expansion_timeseries_location,
    )


def create_and_run_capacity_expansion_model() -> None:
    check_config_present()
    configure_logging_for_run()
    capacity_expansion_pypsa_file = get_capacity_expansion_pypsa_file()
    pypsa_friendly_dir = get_pypsa_friendly_directory()
    capacity_expansion_timeseries_location = (
        get_capacity_expansion_timeseries_location()
    )

    # Get dont_run flag from doit variables
    dont_run = get_var("dont_run_capacity_expansion", "False") == "True"

    create_or_clean_task_output_folder(capacity_expansion_pypsa_file.parent)

    pypsa_friendly_input_tables = read_csvs(pypsa_friendly_dir)

    network = build_pypsa_network(
        pypsa_friendly_input_tables,
        capacity_expansion_timeseries_location,
    )

    if not dont_run:
        # Never use network.optimize() as this will remove custom constraints.
        network.optimize.solve_model(solver_name=config.solver)

    network.export_to_netcdf(capacity_expansion_pypsa_file)


def create_operational_timeseries() -> None:
    """Create operational timeseries inputs."""
    check_config_present()
    configure_logging_for_run()
    input_tables_dir = get_ispypsa_input_tables_directory()
    parsed_trace_dir = get_parsed_trace_directory()
    operational_timeseries_location = get_operational_timeseries_location()

    # Load tables
    ispypsa_tables = read_csvs(input_tables_dir)

    # Create operational snapshots
    operational_snapshots = create_pypsa_friendly_snapshots(config, "operational")

    # Create operational timeseries
    create_pypsa_friendly_timeseries_inputs(
        config,
        "operational",
        ispypsa_tables,
        operational_snapshots,
        parsed_trace_dir,
        operational_timeseries_location,
    )


def create_and_run_operational_model() -> None:
    """Create PyPSA network object for operational model."""
    check_config_present()
    configure_logging_for_run()
    pypsa_friendly_dir = get_pypsa_friendly_directory()
    capacity_expansion_pypsa_file = get_capacity_expansion_pypsa_file()
    operational_timeseries_location = get_operational_timeseries_location()
    operational_pypsa_file = get_operational_pypsa_file()

    # Get dont_run flag from doit variables
    dont_run = get_var("dont_run_operational", "False") == "True"

    # Load tables
    pypsa_friendly_input_tables = read_csvs(pypsa_friendly_dir)

    # Create operational snapshots (needed for update_network_timeseries)
    operational_snapshots = create_pypsa_friendly_snapshots(config, "operational")

    # Load the capacity expansion network
    network = pypsa.Network(capacity_expansion_pypsa_file)

    # Update network timeseries
    update_network_timeseries(
        network,
        pypsa_friendly_input_tables,
        operational_snapshots,
        operational_timeseries_location,
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
    network.export_to_netcdf(operational_pypsa_file)


def remove_deps_and_targets_if_no_config(func):
    """Using this as a decorator on the tasks stops them throwing errors when
    ispypsa or doit is called on the CLI without a config specified. For example,
    when calling "ispypsa list"
    """

    def wrapper(*args, **kwargs):
        # Check config availability when the task is evaluated, not at import time
        if not config:
            task = func(*args, **kwargs)
            return {"actions": task["actions"]}
        else:
            return func(*args, **kwargs)

    return wrapper


@remove_deps_and_targets_if_no_config
def task_save_config():
    """Save configuration file to run directory."""
    return {
        "actions": [save_config_file],
        "targets": [get_config_save_path()],
        "uptodate": [False],  # Always run this task
    }


@remove_deps_and_targets_if_no_config
def task_cache_required_iasr_workbook_tables():
    return {
        "actions": [build_parsed_workbook_cache],
        "targets": get_local_cache_files(),
        "task_dep": ["save_config"],
        # force doit to always mark the task as up-to-date (unless target removed)
        # N.B. this will NOT run if target exists but has been modified
        "uptodate": [True],
    }


@remove_deps_and_targets_if_no_config
def task_create_ispypsa_inputs():
    return {
        "actions": [create_ispypsa_inputs_from_config],
        "file_dep": get_local_cache_files(),
        "targets": get_ispypsa_input_files(),
    }


@remove_deps_and_targets_if_no_config
def task_create_pypsa_friendly_inputs():
    def check_targets():
        targets = (
            get_pypsa_friendly_input_files() + get_capacity_expansion_timeseries_files()
        )

        for target in targets:
            if not target.exists():
                return False

        return True

    return {
        "actions": [create_pypsa_inputs_for_capacity_expansion_model],
        "file_dep": get_ispypsa_input_files(),
        "uptodate": [check_targets],
    }


@create_after(executed="create_pypsa_friendly_inputs")
@remove_deps_and_targets_if_no_config
def task_create_and_run_capacity_expansion_model():
    capacity_expansion_deps = (
        get_pypsa_friendly_input_files() + get_capacity_expansion_timeseries_files()
    )

    return {
        "actions": [(create_and_run_capacity_expansion_model,)],
        "task_dep": ["create_pypsa_friendly_inputs"],
        "file_dep": capacity_expansion_deps,
        "targets": [get_capacity_expansion_pypsa_file()],
    }


@create_after(executed="create_and_run_capacity_expansion_model")
@remove_deps_and_targets_if_no_config
def task_create_operational_timeseries():
    def check_targets():
        targets = get_pypsa_friendly_input_files() + get_operational_timeseries_files()

        for target in targets:
            if not target.exists():
                return False

        return True

    return {
        "actions": [create_operational_timeseries],
        "file_dep": get_ispypsa_input_files(),
        "uptodate": [check_targets],
    }


@create_after(executed="create_operational_timeseries")
@remove_deps_and_targets_if_no_config
def task_create_and_run_operational_model():
    operational_deps = (
        get_pypsa_friendly_input_files() + get_operational_timeseries_files()
    )
    return {
        "actions": [create_and_run_operational_model],
        "file_dep": operational_deps,
        "targets": [get_operational_pypsa_file()],
    }


@remove_deps_and_targets_if_no_config
def task_download_workbook():
    """Download ISP workbook file from data repository."""
    return {
        "actions": [download_workbook_from_config],
        "targets": [get_workbook_path()],
        "uptodate": [False],  # Always allow re-download
    }


@remove_deps_and_targets_if_no_config
def task_download_trace_data():
    """Download trace data from data repository."""
    return {
        "actions": [download_trace_data_from_config],
        "targets": [],  # No targets to track
        "uptodate": [False],  # Always allow re-download
    }
