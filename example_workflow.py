from pathlib import Path

from isp_trace_parser import construct_reference_year_mapping

from ispypsa.config import load_config
from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.logging import configure_logging
from ispypsa.model import build_pypsa_network, save_results
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    load_manually_extracted_tables,
)
from ispypsa.translator import (
    create_pypsa_friendly_bus_demand_timeseries,
    create_pypsa_friendly_existing_generator_timeseries,
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

reference_year_mapping = construct_reference_year_mapping(
    start_year=config.temporal.start_year,
    end_year=config.temporal.end_year,
    reference_years=config.temporal.reference_year_cycle,
)
create_pypsa_friendly_existing_generator_timeseries(
    ispypsa_tables["ecaa_generators"],
    parsed_traces_directory,
    pypsa_friendly_inputs_location,
    generator_types=["solar", "wind"],
    reference_year_mapping=reference_year_mapping,
    year_type=config.temporal.year_type,
    snapshots=pypsa_friendly_input_tables["snapshots"],
)
create_pypsa_friendly_bus_demand_timeseries(
    ispypsa_tables["sub_regions"],
    parsed_traces_directory,
    pypsa_friendly_inputs_location,
    scenario=config.scenario,
    regional_granularity=config.network.nodes.regional_granularity,
    reference_year_mapping=reference_year_mapping,
    year_type=config.temporal.year_type,
    snapshot=pypsa_friendly_input_tables["snapshots"],
)

# Build a PyPSA network object.
network = build_pypsa_network(
    pypsa_friendly_input_tables,
    path_to_pypsa_friendly_timeseries_data=pypsa_friendly_inputs_location,
)

# Solve for least cost operation/expansion
network.optimize.solve_model(solver_name=config.solver)

# Save results.
save_results(network, pypsa_outputs_directory, config.ispypsa_run_name)
