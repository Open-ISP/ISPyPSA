from pathlib import Path

import pandas as pd
import pypsa

from ispypsa.results.transmission import extract_transmission_expansion_results

CAPACITY_EXPANSION_RESULTS_FILES = [
    "transmission_expansion",
]


def extract_capacity_expansion_results(
    network: pypsa.Network,
) -> dict[str : pd.DataFrame]:
    results = {
        "transmission_expansion": extract_transmission_expansion_results(network)
    }

    return results


def list_capacity_expansion_results_files(results_directory: Path) -> list[Path]:
    return [
        results_directory / Path(file).with_suffix(".csv")
        for file in CAPACITY_EXPANSION_RESULTS_FILES
    ]
