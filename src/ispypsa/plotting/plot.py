from pathlib import Path
from typing import Callable

import pandas as pd
import pygal

from ispypsa.plotting.transmission import plot_aggregate_transmission_capacity

"""
The PLOTS dict creates a mapping between plot file paths/names and a plotting function used to create the plot. The
dictionary structure of PLOTS is converted to a directory structure by flatten_dict_with_paths, with the last key in
the dictionary structure becoming the file name (.svg is added).
"""
CAPACITY_EXPANSION_PLOTS = {
    "transmission": {
        "aggregate_transmission_capacity": plot_aggregate_transmission_capacity,
    }
}


def flatten_dict_with_file_paths_as_keys(
    nested_dict, parent_path=None
) -> dict[Path, Callable]:
    """
    Flatten a nested dictionary converting the dictionary structure to a directory structure and file name.

    Args:
        nested_dict: The nested dictionary to flatten
        parent_path: The base Path object (used for recursion)

    Returns:
        A flat dictionary with Path objects as keys
    """
    items = []

    for key, value in nested_dict.items():
        # Create new path by joining parent and current key
        if parent_path is None:
            new_path = Path(key)
        else:
            new_path = parent_path / key

        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_dict_with_file_paths_as_keys(value, new_path).items())
        else:
            # Add non-dict values to the result
            items.append((new_path.with_suffix(".svg"), value))

    return dict(items)


CAPACITY_EXPANSION_PLOTS = flatten_dict_with_file_paths_as_keys(
    CAPACITY_EXPANSION_PLOTS
)


def create_capacity_expansion_plot_suite(
    results: dict[str, pd.DataFrame],
) -> dict[Path, pygal.Graph]:
    """Create a suite of plots for the ISPyPSA capacity expansion modelling results.

    Args:
        results: A dictionary of results from the ISPyPSA model. Should conatain each of the following results tables:
        - transmission_expansion

    Returns:
        A dictionary of plots with the file path as the key and the plot as the value.
    """
    plots = {}

    for path, plotting_function in CAPACITY_EXPANSION_PLOTS.items():
        plots[path] = plotting_function(results)

    return plots


def list_capacity_expansion_plot_files(plots_directory: Path) -> list[Path]:
    """List all the capacity expansion plot files in the plots directory.

    Args:
        plots_directory: The directory where the plots are saved.

    Returns:
        A list of the plot file paths.
    """
    return [plots_directory / path for path in CAPACITY_EXPANSION_PLOTS.keys()]


def save_plots(charts: dict[Path, pygal.Graph], base_path: Path) -> None:
    """Save a suite of plots to the plots directory.

    Args:
        charts: A dictionary of plots with the file path as the key and the plot as the value.
        base_path: The path to the directory where the plots are saved.

    Returns:
        None
    """
    for path, graph in charts.items():
        path = base_path / path
        path.parent.mkdir(parents=True, exist_ok=True)
        graph.render_to_file(path)
