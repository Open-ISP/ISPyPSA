from pathlib import Path

from isp_trace_parser import construct_reference_year_mapping

from ispypsa.config import load_config
from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.iasr_table_caching import build_local_cache
from ispypsa.logging import configure_logging
from ispypsa.model import (
    build_pypsa_network,
    save_pypsa_network,
    update_network_timeseries,
)
from ispypsa.plotting import (
    create_capacity_expansion_plot_suite,
    create_operational_plot_suite,
    generate_results_website,
    save_plots,
)
from ispypsa.results import (
    extract_regions_and_zones_mapping,
    extract_tabular_results,
)
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    load_manually_extracted_tables,
)
from ispypsa.translator import (
    create_pypsa_friendly_dynamic_marginal_costs,
    create_pypsa_friendly_inputs,
    create_pypsa_friendly_snapshots,
    create_pypsa_friendly_timeseries_inputs,
)

# Load model config.
config_path = Path("ispypsa_config.yaml")
config = load_config(config_path)

# Load base paths from config
parsed_workbook_cache = Path(config.paths.parsed_workbook_cache)
parsed_traces_directory = (
    Path(config.paths.parsed_traces_directory) / f"isp_{config.trace_data.dataset_year}"
)
workbook_path = Path(config.paths.workbook_path)
run_directory = Path(config.paths.run_directory)

# Construct full paths from base paths
ispypsa_input_tables_directory = (
    run_directory / config.paths.ispypsa_run_name / "ispypsa_inputs"
)
pypsa_friendly_inputs_location = (
    run_directory / config.paths.ispypsa_run_name / "pypsa_friendly"
)
capacity_expansion_timeseries_location = (
    pypsa_friendly_inputs_location / "capacity_expansion_timeseries"
)
operational_timeseries_location = (
    pypsa_friendly_inputs_location / "operational_timeseries"
)
pypsa_outputs_directory = run_directory / config.paths.ispypsa_run_name / "outputs"
capacity_expansion_tabular_results_directory = (
    pypsa_outputs_directory / "capacity_expansion_tables"
)
capacity_expansion_plot_results_directory = (
    pypsa_outputs_directory / "capacity_expansion_plots"
)
operational_tabular_results_directory = pypsa_outputs_directory / "operational_tables"
operational_plot_results_directory = pypsa_outputs_directory / "operational_plots"

# Create output directories if they don't exist
parsed_workbook_cache.mkdir(parents=True, exist_ok=True)
ispypsa_input_tables_directory.mkdir(parents=True, exist_ok=True)
pypsa_friendly_inputs_location.mkdir(parents=True, exist_ok=True)
capacity_expansion_timeseries_location.mkdir(parents=True, exist_ok=True)
operational_timeseries_location.mkdir(parents=True, exist_ok=True)
pypsa_outputs_directory.mkdir(parents=True, exist_ok=True)
capacity_expansion_tabular_results_directory.mkdir(parents=True, exist_ok=True)
capacity_expansion_plot_results_directory.mkdir(parents=True, exist_ok=True)
operational_tabular_results_directory.mkdir(parents=True, exist_ok=True)
operational_plot_results_directory.mkdir(parents=True, exist_ok=True)

configure_logging()

# Build the local cache from the workbook
build_local_cache(parsed_workbook_cache, workbook_path, config.iasr_workbook_version)

# Load ISP IASR data tables.
iasr_tables = read_csvs(parsed_workbook_cache)
manually_extracted_tables = load_manually_extracted_tables(config.iasr_workbook_version)

# Create ISPyPSA inputs from IASR tables.
ispypsa_tables = create_ispypsa_inputs_template(
    config.scenario,
    config.network.nodes.regional_granularity,
    iasr_tables,
    manually_extracted_tables,
    config.filter_by_nem_regions,
    config.filter_by_isp_sub_regions,
)
write_csvs(ispypsa_tables, ispypsa_input_tables_directory)

# Suggested stage of user interaction:
# At this stage of the workflow the user can modify ispypsa input files, either
# manually or programmatically, to run alternative scenarios using the template
# generated from the chosen ISP scenario.

# Translate ISPyPSA format to a PyPSA friendly format.
pypsa_friendly_input_tables = create_pypsa_friendly_inputs(config, ispypsa_tables)

# Create timeseries inputs and snapshots
pypsa_friendly_input_tables["snapshots"] = create_pypsa_friendly_timeseries_inputs(
    config,
    "capacity_expansion",
    ispypsa_tables,
    pypsa_friendly_input_tables["generators"],
    parsed_traces_directory,
    capacity_expansion_timeseries_location,
)

write_csvs(pypsa_friendly_input_tables, pypsa_friendly_inputs_location)

# Build a PyPSA network object.
network = build_pypsa_network(
    pypsa_friendly_input_tables,
    capacity_expansion_timeseries_location,
)

# Solve for least cost operation/expansion
# Never use network.optimize() as this will remove custom constraints.
network.optimize.solve_model(solver_name=config.solver)

# Save capacity expansion results
save_pypsa_network(network, pypsa_outputs_directory, "capacity_expansion")
capacity_expansion_results = extract_tabular_results(network, ispypsa_tables)
capacity_expansion_results["regions_and_zones_mapping"] = (
    extract_regions_and_zones_mapping(ispypsa_tables)
)
write_csvs(capacity_expansion_results, capacity_expansion_tabular_results_directory)

# Create and save capacity expansion plots
capacity_expansion_plots = create_capacity_expansion_plot_suite(
    capacity_expansion_results
)
save_plots(capacity_expansion_plots, capacity_expansion_plot_results_directory)

# Generate capacity expansion results website
generate_results_website(
    capacity_expansion_plots,
    capacity_expansion_plot_results_directory,
    pypsa_outputs_directory,
    output_filename="capacity_expansion_results_viewer.html",
    subtitle="Capacity Expansion Analysis",
    regions_and_zones_mapping=capacity_expansion_results["regions_and_zones_mapping"],
)

# Operational modelling extension
operational_snapshots = create_pypsa_friendly_timeseries_inputs(
    config,
    "operational",
    ispypsa_tables,
    pypsa_friendly_input_tables["generators"],
    parsed_traces_directory,
    operational_timeseries_location,
)

write_csvs(
    {"operational_snapshots": operational_snapshots}, pypsa_friendly_inputs_location
)

update_network_timeseries(
    network,
    pypsa_friendly_input_tables,
    operational_snapshots,
    operational_timeseries_location,
)

network.optimize.fix_optimal_capacities()

# Never use network.optimize() as this will remove custom constraints.
network.optimize.optimize_with_rolling_horizon(
    horizon=config.temporal.operational.horizon,
    overlap=config.temporal.operational.overlap,
)

save_pypsa_network(network, pypsa_outputs_directory, "operational")

# Extract and save operational results
operational_results = extract_tabular_results(network, ispypsa_tables)
operational_results["regions_and_zones_mapping"] = extract_regions_and_zones_mapping(
    ispypsa_tables
)
write_csvs(operational_results, operational_tabular_results_directory)

# Create and save operational plots
operational_plots = create_operational_plot_suite(operational_results)
save_plots(operational_plots, operational_plot_results_directory)

# Generate operational results website
generate_results_website(
    operational_plots,
    operational_plot_results_directory,
    pypsa_outputs_directory,
    output_filename="operational_results_viewer.html",
    subtitle="Operational Analysis",
    regions_and_zones_mapping=operational_results["regions_and_zones_mapping"],
)
