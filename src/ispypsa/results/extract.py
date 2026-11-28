from pathlib import Path

import pandas as pd
import pypsa

from ispypsa.feature_flags import FEATURE_FLAGS
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
    """Maps the model's nodes to the NEM regions, ISP sub-regions and REZs they
    are or sit inside.

    The results name the node (bus) each link, generator and load is at. The
    regional results and plots total them by NEM region, sub-region or REZ,
    and this table says which region, sub-region and REZ each node is or sits
    inside.

    Under the new table format the nodes are the geographies in
    network_geography, which already gives each one's region and sub-region
    at the granularity the model is built at. A REZ has those of the node it
    connects to: at "nem_regions" granularity the REZ N3 has the region NSW
    and no sub-region, so a flow from N3 to NSW is not counted as leaving a
    sub-region the model doesn't have.

    Under the old table format the table lists every sub-region and REZ from
    sub_regions and renewable_energy_zones, with no node column: a node is
    matched by its id in the sub-region or REZ column.

    Examples:
        >>> from pathlib import Path
        >>> from ispypsa.data_fetch import read_csvs
        >>> from ispypsa.results import extract_regions_and_zones_mapping

        >>> ispypsa_tables = read_csvs(Path("ispypsa_inputs"))
        >>> mapping = extract_regions_and_zones_mapping(ispypsa_tables)

    Args:
        ispypsa_tables: The ISPyPSA input tables. The new table format reads
            network_geography, the old format sub_regions and
            renewable_energy_zones.

    Returns:
        New table format: a table with one row per node and the columns:

            - node: the node's geo_id, which is also its bus name.
            - nem_region_id: network_geography's region_id. Blank at
              "single_region" granularity, where that is "NEM" for every
              node: the whole NEM is not a region to total by.
            - isp_sub_region_id: network_geography's subregion_id. Blank at
              "nem_regions" and "single_region" granularity, where the table
              has no subregion_id.
            - rez_id: the node's own id for a REZ, blank otherwise.

        Old table format: a table with one row per sub-region and REZ, and
        the columns nem_region_id, isp_sub_region_id and rez_id (blank for a
        sub-region with no REZ).
    """
    # FEATURE_FLAG_CLEANUP[use_new_table_format]: keep the first branch,
    # dedented, and delete the legacy body below it.
    if FEATURE_FLAGS["use_new_table_format"]:
        # Reindexed: subregion_id is only there at sub_regions granularity.
        geography = ispypsa_tables["network_geography"].reindex(
            columns=["geo_id", "geo_type", "region_id", "subregion_id"]
        )
        is_rez = geography["geo_type"] == "rez"
        # At single_region granularity every node's region_id is "NEM".
        is_in_a_region = geography["region_id"] != "NEM"
        return pd.DataFrame(
            {
                "node": geography["geo_id"],
                "nem_region_id": geography["region_id"].where(is_in_a_region),
                "isp_sub_region_id": geography["subregion_id"],
                "rez_id": geography["geo_id"].where(is_rez),
            }
        )

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
