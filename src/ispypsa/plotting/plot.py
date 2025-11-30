from pathlib import Path

import pandas as pd

from ispypsa.plotting.generation import (
    plot_dispatch,
    plot_generation_capacity_expansion,
)
from ispypsa.plotting.transmission import (
    plot_aggregate_transmission_capacity,
    plot_flows,
    plot_regional_capacity_expansion,
)


def flatten_dict_with_file_paths_as_keys(
    nested_dict, parent_path=None
) -> dict[Path, dict[str, pd.DataFrame]]:
    """
    Flatten a nested dictionary converting the dictionary structure to a directory structure and file name.

    Args:
        nested_dict: The nested dictionary to flatten
        parent_path: The base Path object (used for recursion)

    Returns:
        A flat dictionary with Path objects as keys and dict values containing "plot" and "data"
    """
    items = []

    for key, value in nested_dict.items():
        # Create new path by joining parent and current key
        if parent_path is None:
            new_path = Path(key)
        else:
            new_path = parent_path / key

        # Check if this is a leaf node (has "plot" and "data" keys)
        if isinstance(value, dict) and "plot" in value and "data" in value:
            # Extract just the plot and data for this leaf
            items.append(
                (
                    new_path.with_suffix(".html"),
                    {"plot": value["plot"], "data": value["data"]},
                )
            )
            # Continue processing other keys in this dict (for mixed leaf/nested structures)
            for sub_key, sub_value in value.items():
                if sub_key not in ("plot", "data") and isinstance(sub_value, dict):
                    items.extend(
                        flatten_dict_with_file_paths_as_keys(
                            {sub_key: sub_value}, new_path
                        ).items()
                    )
        elif isinstance(value, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_dict_with_file_paths_as_keys(value, new_path).items())

    return dict(items)


def create_plot_suite(
    results: dict[str, pd.DataFrame],
) -> dict[Path, dict]:
    """Create a suite of plots for ISPyPSA modelling results.

    Works for both capacity expansion and operational model results.

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.data_fetch import read_csvs
        >>> from ispypsa.results import extract_tabular_results
        >>> from ispypsa.plotting import create_plot_suite, save_plots

        Extract tabular results from the solved network.
        >>> ispypsa_tables = read_csvs(Path("ispypsa_inputs"))
        >>> results = extract_tabular_results(network, ispypsa_tables)

        Create the plot suite from the results.
        >>> plots = create_plot_suite(results)

        Save the plots to disk.
        >>> save_plots(plots, Path("outputs/plots"))

    Args:
        results: A dictionary of tabular results from the ISPyPSA model. Should contain:
            - transmission_expansion
            - transmission_flows
            - nem_region_transmission_flows
            - isp_sub_region_transmission_flows
            - regions_and_zones_mapping
            - generator_dispatch
            - generation_expansion
            - demand

    Returns:
        A dictionary of plots with the file path as the key and the plot as the value.
    """
    nem_region_flows = results.get("nem_region_transmission_flows", pd.DataFrame())
    isp_sub_region_flows = results.get(
        "isp_sub_region_transmission_flows", pd.DataFrame()
    )

    plots = {
        "transmission": {
            "aggregate_capacity": plot_aggregate_transmission_capacity(
                results["transmission_expansion"], results["regions_and_zones_mapping"]
            ),
            "flows": plot_flows(
                results["transmission_flows"], results["transmission_expansion"]
            ),
            "regional_expansion": plot_regional_capacity_expansion(
                results["transmission_expansion"], results["regions_and_zones_mapping"]
            ),
        },
        "generation": plot_generation_capacity_expansion(
            results["generation_expansion"], results["regions_and_zones_mapping"]
        ),
        "dispatch": {
            "system": plot_dispatch(
                results["generator_dispatch"],
                results["demand"],
            ),
            "regional": plot_dispatch(
                results["generator_dispatch"],
                results["demand"],
                results["regions_and_zones_mapping"],
                "nem_region_id",
                nem_region_flows,
            ),
            "sub_regional": plot_dispatch(
                results["generator_dispatch"],
                results["demand"],
                results["regions_and_zones_mapping"],
                "isp_sub_region_id",
                isp_sub_region_flows,
            ),
        },
    }
    return flatten_dict_with_file_paths_as_keys(plots)


def save_plots(charts: dict[Path, dict], base_path: Path) -> None:
    """Save a suite of Plotly plots and their underlying data to the plots directory.

    All plots are saved as interactive HTML files with accompanying CSV data files.

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.plotting import create_plot_suite, save_plots

        Create plots from results (see `create_plot_suite` for how to get results).
        >>> plots = create_plot_suite(results)

        Save all plots to a directory.
        >>> save_plots(plots, Path("outputs/capacity_expansion_plots"))
        # Creates HTML files like:
        # outputs/capacity_expansion_plots/transmission/aggregate_capacity.html
        # outputs/capacity_expansion_plots/transmission/aggregate_capacity.csv
        # outputs/capacity_expansion_plots/generation.html
        # outputs/capacity_expansion_plots/generation.csv

    Args:
        charts: A dictionary with file paths as keys and dicts with "plot" and "data" as values.
        base_path: The path to the directory where the plots are saved.

    Returns:
        None
    """
    for path, content in charts.items():
        plot = content["plot"]

        # Save Plotly chart as HTML
        html_path = base_path / path
        html_path.parent.mkdir(parents=True, exist_ok=True)

        # Save the underlying data (CSV)
        csv_path = html_path.with_suffix(".csv")
        content["data"].to_csv(csv_path, index=False)

        # Save the plot (HTML)
        plot.write_html(html_path)
