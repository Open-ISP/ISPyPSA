from pathlib import Path

import pandas as pd
import pypsa

from ispypsa.results.generation import extract_demand, extract_generator_dispatch
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
The CAPACITY_EXPANSION_RESULTS_FILES dictionary creates a mapping between the result file name and the function used to extract the results.
"""
CAPACITY_EXPANSION_RESULTS_FILES = {
    "regions_and_zones_mapping": extract_regions_and_zones_mapping,
    "transmission_expansion": extract_transmission_expansion_results,
    "transmission_flows": extract_transmission_flows,
    "rez_transmission_flows": extract_rez_transmission_flows,
    "isp_sub_region_transmission_flows": extract_isp_sub_region_transmission_flows,
    "nem_region_transmission_flows": extract_nem_region_transmission_flows,
    "generator_dispatch": extract_generator_dispatch,
    "demand": extract_demand,
}


"""
The OPERATIONAL_RESULTS_FILES dictionary creates a mapping between the result file name and the function used to extract the results.
Operational results include transmission expansion data (from the capacity expansion model) to provide capacity limits for flow plots.
"""
OPERATIONAL_RESULTS_FILES = {
    "transmission_expansion": extract_transmission_expansion_results,
    "transmission_flows": extract_transmission_flows,
    "rez_transmission_flows": extract_rez_transmission_flows,
    "isp_sub_region_transmission_flows": extract_isp_sub_region_transmission_flows,
    "nem_region_transmission_flows": extract_nem_region_transmission_flows,
    "generator_dispatch": extract_generator_dispatch,
    "demand": extract_demand,
}


def extract_tabular_capacity_expansion_results(
    network: pypsa.Network,
    ispypsa_tables: dict[str, pd.DataFrame],
) -> dict[str : pd.DataFrame]:
    """Extract the capacity expansion results from the PyPSA network and return a dictionary of results.

    Args:
        network: The PyPSA network object.
        ispypsa_tables: Dictionary of ISPyPSA input tables (optional, needed for regions mapping).

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

    for file, function in CAPACITY_EXPANSION_RESULTS_FILES.items():
        if file in ["transmission_flows", "regions_and_zones_mapping"]:
            continue

        if file in geographic_transmission_functions:
            results[file] = function(
                results["transmission_flows"], results["regions_and_zones_mapping"]
            )
        else:
            results[file] = function(network)

    return results


def extract_tabular_operational_results(
    network: pypsa.Network,
    ispypsa_tables: dict[str, pd.DataFrame],
) -> dict[str : pd.DataFrame]:
    """Extract the operational results from the PyPSA network and return a dictionary of results.

    Args:
        network: The PyPSA network object.
        ispypsa_tables: Dictionary of ISPyPSA input tables (optional, needed for regions mapping).

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

    for file, function in OPERATIONAL_RESULTS_FILES.items():
        if file in ["transmission_flows", "regions_and_zones_mapping"]:
            continue

        if file in geographic_transmission_functions:
            results[file] = function(
                results["transmission_flows"], results["regions_and_zones_mapping"]
            )
        else:
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


def list_operational_results_files(results_directory: Path) -> list[Path]:
    """List all the operational results files, with full file paths.

    Args:
        results_directory: The directory where the results are saved.

    Returns:
        A list of the result file paths.

    """
    return [
        results_directory / Path(file).with_suffix(".csv")
        for file in OPERATIONAL_RESULTS_FILES.keys()
    ]
