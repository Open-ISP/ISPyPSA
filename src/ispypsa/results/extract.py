from pathlib import Path

import pandas as pd
import pypsa

from ispypsa.results.transmission import extract_transmission_expansion_results

"""
The CAPACITY_EXPANSION_RESULTS_FILES dictionary creates a mapping between the result file name and the function used to extract the results.
"""
CAPACITY_EXPANSION_RESULTS_FILES = {
    "transmission_expansion": extract_transmission_expansion_results
}


def extract_tabular_capacity_expansion_results(
    network: pypsa.Network,
) -> dict[str : pd.DataFrame]:
    """Extract the capacity expansion results from the PyPSA network and return a dictionary of results.

    Args:
        network: The PyPSA network object.

    Returns:
        A dictionary of results with the file name as the key and the results as the value.
    """
    results = {}
    for file, function in CAPACITY_EXPANSION_RESULTS_FILES.items():
        results[file] = function(network)

    return results


def list_capacity_expansion_results_files(results_directory: Path) -> list[Path]:
    """List all the capacity expansion results files, with full file paths.

    Args:
        results_directory: The directory where the results are saved.

    Returns:
        A list of the result file paths.

    """
    return [
        results_directory / Path(file).with_suffix(".csv")
        for file in CAPACITY_EXPANSION_RESULTS_FILES.keys()
    ]
