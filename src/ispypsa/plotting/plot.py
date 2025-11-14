from pathlib import Path
from typing import Callable

import pandas as pd
import pygal

from ispypsa.plotting.transmission import plot_aggregate_transmission_capacity

PLOTS = {
    "transmission": {
        "aggregate_transmission_capacity": plot_aggregate_transmission_capacity,
    }
}

flattened_plots = {}


def flatten_dict_with_paths(nested_dict, parent_path=None):
    """
    Flatten a nested dictionary using Path objects as keys.

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
            items.extend(flatten_dict_with_paths(value, new_path).items())
        else:
            # Add non-dict values to the result
            items.append((new_path.with_suffix(".svg"), value))

    return dict(items)


PLOTS = flatten_dict_with_paths(PLOTS)


def create_plot_suite(results: dict[str, pd.DataFrame]) -> dict[Path, pygal.Graph]:
    plots = {}

    for path, plotting_function in PLOTS.items():
        plots[path] = plotting_function(results)

    return plots


def list_capacity_expansion_plot_files(plots_directory: Path) -> list[Path]:
    return [plots_directory / path for path in PLOTS.keys()]


def save_plots(charts: dict[Path, pygal.Graph], base_path: Path) -> None:
    for path, graph in charts.items():
        path = base_path / path
        path.parent.mkdir(parents=True, exist_ok=True)
        graph.render_to_file(path)
