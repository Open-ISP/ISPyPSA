from pathlib import Path

from ispypsa.config import load_config
from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.logging import configure_logging
from ispypsa.model import build_pypsa_network, save_results
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    load_manually_extracted_tables,
)
from ispypsa.translator import (
    create_pypsa_friendly_inputs,
)

# Define root folder for modelling files.
root_folder = Path("ispypsa_runs")

# Load model config.
config_path = root_folder / Path("development/ispypsa_inputs/ispypsa_config.yaml")
config = load_config(config_path)

# Define input/output data storage directories.
run_folder = Path(root_folder, config.ispypsa_run_name)
parsed_workbook_cache = root_folder / Path("workbook_table_cache")
parsed_traces_directory = Path(config.temporal.path_to_parsed_traces)
ispypsa_input_tables_directory = Path(run_folder, "ispypsa_inputs", "tables")
pypsa_friendly_inputs_location = Path(run_folder, "pypsa_friendly")
pypsa_outputs_directory = Path(run_folder, "outputs")

configure_logging()

# Load ISP IASR data tables.
iasr_tables = read_csvs(parsed_workbook_cache)
manually_extracted_tables = load_manually_extracted_tables(config.iasr_workbook_version)

# Create ISPyPSA inputs from IASR tables.
ispypsa_tables = create_ispypsa_inputs_template(
    config.scenario,
    config.network.nodes.regional_granularity,
    iasr_tables,
    manually_extracted_tables,
)
write_csvs(ispypsa_tables, ispypsa_input_tables_directory)

# Suggested stage of user interaction:
# At this stage of the workflow the user can modify ispypsa input files, either
# manually or programmatically, to run alternative scenarios using the template
# generated from the chosen ISP scenario.

# Translate ISPyPSA format to a PyPSA friendly format.
pypsa_friendly_input_tables = create_pypsa_friendly_inputs(config, ispypsa_tables)
write_csvs(pypsa_friendly_input_tables, pypsa_friendly_inputs_location)

create_pypsa_friendly_timeseries_inputs(
    config,
    ispypsa_tables,
    snapshots,
    parsed_traces_directory,
    pypsa_friendly_investment_timeseries_location,
)

# Build a PyPSA network object.
network = build_pypsa_network(
    pypsa_friendly_input_tables,
    path_to_pypsa_friendly_timeseries_data=pypsa_friendly_investment_timeseries_location,
)

# Solve for least cost operation/expansion
network.optimize.solve_model(solver_name=config.solver)

# Save results.
save_results(network, pypsa_outputs_directory, config.ispypsa_run_name)

# Operational modelling extension
operational_snapshots = create_pypsa_friendly_snapshots(config.temporal.operational)

create_pypsa_friendly_timeseries_inputs(
    config,
    ispypsa_tables,
    operational_snapshots,
    parsed_traces_directory,
    pypsa_friendly_operational_timeseries_location,
)

update_network_timeseries(
    network,
    pypsa_friendly_input_tables,
    operational_snapshots,
    pypsa_friendly_operational_timeseries_location,
)

network.fix_optimal_capacities()

network.optimize_with_rolling_horizon(
    operational_snapshots,
    horizon=config.temporal.operational.horizon,
    overlap=config.temporal.operational.overlap,
)

save_results(network, pypsa_outputs_directory, config.ispypsa_run_name + "_operational")
