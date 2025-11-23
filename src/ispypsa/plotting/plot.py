from pathlib import Path

import pandas as pd

from ispypsa.plotting.generation import (
    plot_node_level_dispatch,
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
            items.append((new_path.with_suffix(".html"), value))
        elif isinstance(value, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_dict_with_file_paths_as_keys(value, new_path).items())

    return dict(items)


def create_capacity_expansion_plot_suite(
    results: dict[str, pd.DataFrame],
) -> dict[Path, dict]:
    """Create a suite of plots for the ISPyPSA capacity expansion modelling results.

    Args:
        results: A dictionary of results from the ISPyPSA model. Should contain each of the following results tables:
        - transmission_expansion
        - transmission_flows
        - nem_region_transmission_flows
        - isp_sub_region_transmission_flows
        - regions_and_zones_mapping
        - generator_dispatch
        - demand

    Returns:
        A dictionary of plots with the file path as the key and the plot as the value.
    """

    # Get transmission flows at appropriate geographic levels
    nem_region_flows = results.get("nem_region_transmission_flows", pd.DataFrame())
    isp_sub_region_flows = results.get(
        "isp_sub_region_transmission_flows", pd.DataFrame()
    )

    # The plots dict creates a mapping between plot file paths/names and a plotting function used to create the plot.
    # The dictionary structure of plots is converted to a directory structure by flatten_dict_with_paths, with the last
    # key in the dictionary structure becoming the file name (.html is added for Plotly plots).
    plots = {
        "transmission": {
            "aggregate_transmission_capacity": plot_aggregate_transmission_capacity(
                results["transmission_expansion"], results["regions_and_zones_mapping"]
            ),
            "flows": plot_flows(
                results["transmission_flows"], results["transmission_expansion"]
            ),
            "regional_expansion": plot_regional_capacity_expansion(
                results["transmission_expansion"], results["regions_and_zones_mapping"]
            ),
        },
        "dispatch": {
            "nem_region_id": plot_node_level_dispatch(
                results["generator_dispatch"],
                results["demand"],
                results["regions_and_zones_mapping"],
                "nem_region_id",
                nem_region_flows,
            ),
            "isp_sub_region_id": plot_node_level_dispatch(
                results["generator_dispatch"],
                results["demand"],
                results["regions_and_zones_mapping"],
                "isp_sub_region_id",
                isp_sub_region_flows,
            ),
        },
    }
    plots = flatten_dict_with_file_paths_as_keys(plots)
    return plots


def create_operational_plot_suite(
    results: dict[str, pd.DataFrame],
) -> dict[Path, dict]:
    """Create a suite of plots for the ISPyPSA operational modelling results.

    Args:
        results: A dictionary of results from the ISPyPSA model. Should contain each of the following results tables:
        - transmission_expansion (from capacity expansion model, for capacity limits)
        - transmission_flows
        - nem_region_transmission_flows (for regional dispatch plots)
        - isp_sub_region_transmission_flows (for sub-regional dispatch plots)
        - regions_and_zones_mapping
        - generator_dispatch
        - demand

    Returns:
        A dictionary of plots with the file path as the key and the plot as the value.
    """

    # Get transmission flows at appropriate geographic levels
    nem_region_flows = results.get("nem_region_transmission_flows", pd.DataFrame())
    isp_sub_region_flows = results.get(
        "isp_sub_region_transmission_flows", pd.DataFrame()
    )

    # The plots dict creates a mapping between plot file paths/names and a plotting function used to create the plot.
    # The dictionary structure of plots is converted to a directory structure by flatten_dict_with_paths, with the last
    # key in the dictionary structure becoming the file name (.html is added for Plotly plots).
    # Note: Operational results use transmission expansion data from the capacity expansion model for capacity limits.
    plots = {
        "transmission": {
            "flows": plot_flows(
                results["transmission_flows"], results["transmission_expansion"]
            ),
        },
        "dispatch": {
            "regional": plot_node_level_dispatch(
                results["generator_dispatch"],
                results["demand"],
                results["regions_and_zones_mapping"],
                "nem_region_id",
                nem_region_flows,
            ),
            "sub_regional": plot_node_level_dispatch(
                results["generator_dispatch"],
                results["demand"],
                results["regions_and_zones_mapping"],
                "isp_sub_region_id",
                isp_sub_region_flows,
            ),
        },
    }
    plots = flatten_dict_with_file_paths_as_keys(plots)
    return plots


def save_plots(charts: dict[Path, dict], base_path: Path) -> None:
    """Save a suite of Plotly plots and their underlying data to the plots directory.

    All plots are saved as interactive HTML files.

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
