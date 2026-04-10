import pandas as pd

_HVDC_PATH_IDS = {"NNSW-SQ_Terranora", "WNV-CSA_Murraylink", "TAS-SEV"}


def _template_network_transmission_paths(
    flow_path_transfer_capability: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
) -> pd.DataFrame:
    """Creates the network_transmission_paths topology table.

    Args:
        flow_path_transfer_capability: IASR flow path transfer capability table.
        renewable_energy_zones: IASR renewable energy zones table.

    Returns:
        DataFrame with columns: path_id, geo_from, geo_to, carrier.
    """
    flow_path_rows = _extract_flow_path_rows(flow_path_transfer_capability)
    rez_connection_rows = _extract_rez_connection_rows(renewable_energy_zones)
    return pd.concat([flow_path_rows, rez_connection_rows], ignore_index=True)


def _extract_flow_path_rows(
    flow_path_transfer_capability: pd.DataFrame,
) -> pd.DataFrame:
    topology = _parse_flow_path_topology(flow_path_transfer_capability["Flow Paths"])
    topology["carrier"] = topology["path_id"].map(
        lambda pid: "DC" if pid in _HVDC_PATH_IDS else "AC"
    )
    return topology[["path_id", "geo_from", "geo_to", "carrier"]]


def _parse_flow_path_topology(name_series: pd.Series) -> pd.DataFrame:
    parsed = name_series.str.strip().str.extract(
        # e.g. "NNSW-SQ (Terranora)" or "CNSW-SNW-NTH"
        r"^(?P<geo_from>[A-Z]+)"  # uppercase code, e.g. "NNSW"
        r"\s*[-\u2013\u2014\u00ad]+\s*"  # dash/en-dash/em-dash separator
        r"(?P<geo_to>[A-Z]+)"  # uppercase code, e.g. "SQ"
        r"\s*(?P<suffix>.*)"  # optional suffix, e.g. "(Terranora)" or "-NTH"
    )
    parsed["suffix"] = parsed["suffix"].apply(_clean_suffix)
    parsed["path_id"] = parsed.apply(
        lambda row: _build_path_id(row["geo_from"], row["geo_to"], row["suffix"]),
        axis=1,
    )
    return parsed.drop(columns=["suffix"])


def _clean_suffix(suffix: str) -> str:
    # Handles two suffix patterns:
    #   parenthesized: "(Terranora)" -> "Terranora"
    #   dash-separated: "-NTH" -> "NTH"
    suffix = suffix.strip()
    if not suffix:
        return ""
    return suffix.strip("()").lstrip("-").strip()


def _build_path_id(geo_from: str, geo_to: str, suffix: str) -> str:
    path_id = f"{geo_from}-{geo_to}"
    if suffix:
        path_id = f"{path_id}_{suffix}"
    return path_id


def _extract_rez_connection_rows(
    renewable_energy_zones: pd.DataFrame,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "path_id": renewable_energy_zones["ID"]
            + "-"
            + renewable_energy_zones["ISP sub-region"],
            "geo_from": renewable_energy_zones["ID"],
            "geo_to": renewable_energy_zones["ISP sub-region"],
            "carrier": "AC",
        }
    )
