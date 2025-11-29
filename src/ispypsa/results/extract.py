from pathlib import Path

import pandas as pd
import pypsa

from ispypsa.results.generation import (
    extract_demand,
    extract_generation_expansion_results,
    extract_generator_dispatch,
)
from ispypsa.results.transmission import (
    _extract_raw_link_flows,
    extract_isp_sub_region_transmission_flows,
    extract_nem_region_transmission_flows,
    extract_rez_transmission_flows,
    extract_transmission_expansion_results,
    extract_transmission_flows,
)


def extract_regions_and_zones_mapping(
    ispypsa_tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Create a reference table mapping REZs and sub-regions to NEM regions.

    This function creates a mapping showing the geographic hierarchy of zones and regions,
    which can be used for regional analysis and plotting.

    Args:
        ispypsa_tables: Dictionary of ISPyPSA input tables containing at minimum:
            - "sub_regions": Table with isp_sub_region_id and nem_region_id columns
            - "renewable_energy_zones": Table with rez_id and isp_sub_region_id columns

    Returns:
        A DataFrame with columns:
            - nem_region_id: The NEM region identifier
            - isp_sub_region_id: The sub-region identifier
            - rez_id: The REZ identifier (NaN for sub-regions without REZs)
    """
    if "sub_regions" not in ispypsa_tables:
        return pd.DataFrame(columns=["nem_region_id", "isp_sub_region_id", "rez_id"])

    # Start with sub-regions
    mapping = ispypsa_tables["sub_regions"][
        ["nem_region_id", "isp_sub_region_id"]
    ].copy()

    # Add REZs if available
    if "renewable_energy_zones" in ispypsa_tables:
        # Merge to add rez_id column
        mapping = mapping.merge(
            ispypsa_tables["renewable_energy_zones"][["rez_id", "isp_sub_region_id"]],
            on="isp_sub_region_id",
            how="left",
        )

    return mapping


"""
The RESULTS_FILES dictionary creates a mapping between the result file name and the function used to extract the results.
"""
RESULTS_FILES = {
    "regions_and_zones_mapping": extract_regions_and_zones_mapping,
    "transmission_expansion": extract_transmission_expansion_results,
    "transmission_flows": extract_transmission_flows,
    "rez_transmission_flows": extract_rez_transmission_flows,
    "isp_sub_region_transmission_flows": extract_isp_sub_region_transmission_flows,
    "nem_region_transmission_flows": extract_nem_region_transmission_flows,
    "generation_expansion": extract_generation_expansion_results,
    "generator_dispatch": extract_generator_dispatch,
    "demand": extract_demand,
}


def extract_tabular_results(
    network: pypsa.Network,
    ispypsa_tables: dict[str, pd.DataFrame],
) -> dict[str : pd.DataFrame]:
    """Extract the results from the PyPSA network and return a dictionary of results.

    Extracts generation expansion, transmission expansion, dispatch, demand, and
    transmission flow results from the solved PyPSA network.

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.data_fetch import read_csvs, write_csvs
        >>> from ispypsa.results import extract_tabular_results

        Load ISPyPSA input tables (needed for regions mapping).
        >>> ispypsa_tables = read_csvs(Path("ispypsa_inputs"))

        After solving the network, extract the results.
        >>> network.optimize.solve_model(solver_name="highs")
        >>> results = extract_tabular_results(network, ispypsa_tables)

        Access specific result tables.
        >>> generation_expansion = results["generation_expansion"]
        >>> transmission_flows = results["transmission_flows"]

        Write results to CSV files.
        >>> write_csvs(results, Path("outputs/results"))

    Args:
        network: The PyPSA network object.
        ispypsa_tables: Dictionary of ISPyPSA input tables (needed for regions mapping).

    Returns:
        A dictionary of results with the file name as the key and the results as the value.
    """

    # Functions that require link_flows and regions_mapping parameters
    geographic_transmission_functions = {
        "rez_transmission_flows",
        "isp_sub_region_transmission_flows",
        "nem_region_transmission_flows",
    }

    results = {}

    # Extract regions and zones mapping to be used in other functions that require it.
    results["regions_and_zones_mapping"] = extract_regions_and_zones_mapping(
        ispypsa_tables
    )

    # Extract first transmission flows to be used in other functions that require it.
    results["transmission_flows"] = extract_transmission_flows(network)

    for file, function in RESULTS_FILES.items():
        if file in ["transmission_flows", "regions_and_zones_mapping"]:
            continue

        if file in geographic_transmission_functions:
            results[file] = function(
                results["transmission_flows"], results["regions_and_zones_mapping"]
            )
        else:
            results[file] = function(network)

    return results


def list_results_files(results_directory: Path) -> list[Path]:
    """List all the results files, with full file paths.

    Args:
        results_directory: The directory where the results are saved.

    Returns:
        A list of the result file paths.

    """
    return [
        results_directory / Path(file).with_suffix(".csv")
        for file in RESULTS_FILES.keys()
    ]
