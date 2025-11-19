"""Example of using Plotly interactive plotting functions."""

from pathlib import Path

import pypsa

from ispypsa.data_fetch import read_csvs
from ispypsa.plotting import create_capacity_expansion_plot_suite, save_plots
from ispypsa.results import (
    extract_regions_and_zones_mapping,
    extract_tabular_capacity_expansion_results,
)

# Load network
print("Loading network...")
network = pypsa.Network()
network.import_from_netcdf("ispypsa_runs/development/outputs/capacity_expansion.nc")

# Load ISPyPSA tables
print("Loading ISPyPSA tables...")
ispypsa_tables = read_csvs(Path("ispypsa_runs/development/ispypsa_inputs"))

# Extract all results (includes dispatch, demand, transmission flows, etc.)
print("Extracting results...")
results = extract_tabular_capacity_expansion_results(network, ispypsa_tables)
results["regions_and_zones_mapping"] = extract_regions_and_zones_mapping(ispypsa_tables)

# Create all plots (including interactive Plotly dispatch plots)
print("\nCreating plots...")
plots = create_capacity_expansion_plot_suite(results)

# Save plots (all saved as interactive HTML)
print(f"\nSaving {len(plots)} plots...")
output_dir = Path("ispypsa_runs/development/outputs/capacity_expansion_plots")
save_plots(plots, output_dir)

print(f"\nDone! Saved {len(plots)} plots to {output_dir}")
print("All plots saved as interactive HTML files.")
print("\nOpen the HTML files in a browser to view interactive plots.")
