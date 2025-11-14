import pandas as pd
import pypsa


def extract_transmission_expansion_results(network: pypsa.Network) -> pd.DataFrame:
    """Extract transmission expansion results from PyPSA network and rename columns according to ISP conventions.

    Existing capacity is reported with a build year of 0.

    Examples:

    >>> extract_transmission_expansion_results(network)
    isp_name, node_from, node_to, build_year, forward_direction_nominal_capacity_mw, reverse_direction_nominal_capacity_mw
    A-B, A, B, 0, 100, 100
    A-B, A, B, 2026, 300, 300
    A-B, A, B, 2027, 400, 400

    Args:
        network: PyPSA network object

    Returns:
        pd.DataFrame: Transmission expansion results in ISP format. Columns: isp_name, node_from, node_to, build_year,
        forward_direction_nominal_capacity_mw, and reverse_direction_nominal_capacity_mw.

    """

    results = network.links

    # results = results[results["p_nom_opt"] > 0].copy()

    columns_to_rename = {
        "bus0": "node_from",
        "bus1": "node_to",
    }

    results = results.rename(columns=columns_to_rename)

    results["forward_direction_nominal_capacity_mw"] = results["p_nom_opt"]
    results["reverse_direction_nominal_capacity_mw"] = (
        results["p_nom_opt"] * results["p_min_pu"]
    )

    cols_to_keep = [
        "isp_name",
        "isp_type",
        "node_from",
        "node_to",
        "build_year",
        "forward_direction_nominal_capacity_mw",
        "reverse_direction_nominal_capacity_mw",
    ]

    results = results.loc[:, cols_to_keep].reset_index(drop=True)

    return results
