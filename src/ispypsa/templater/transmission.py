import pandas as pd

_HVDC_PATH_IDS = {"NNSW-SQ_Terranora", "WNV-CSA_Murraylink", "TAS-SEV"}

# Flow path capacity columns are renamed to direction__timeslice format;
# _melt_flow_path_capacity splits on '__' to produce separate direction and timeslice columns.
_FLOW_PATH_COLUMN_RENAMES = {
    "Forward direction capability approximation (MW)_Peak demand": "forward__peak_demand",
    "Forward direction capability approximation (MW)_Summer typical": "forward__summer_typical",
    "Forward direction capability approximation (MW)_Winter reference": "forward__winter_reference",
    "Reverse direction capability approximation (MW)_Peak demand": "reverse__peak_demand",
    "Reverse direction capability approximation (MW)_Summer typical": "reverse__summer_typical",
    "Reverse direction capability approximation (MW)_Winter reference": "reverse__winter_reference",
}

# REZ limits have no direction — they are symmetric. _duplicate_for_both_directions
# adds forward/reverse rows explicitly downstream.
_REZ_COLUMN_RENAMES = {
    "REZ transmission network limit_Peak demand": "peak_demand",
    "REZ transmission network limit_Summer typical": "summer_typical",
    "REZ transmission network limit_Winter reference": "winter_reference",
}


def _template_network_transmission(
    flow_path_transfer_capability: pd.DataFrame,
    initial_transmission_limits: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Creates the network_transmission_paths and network_transmission_path_limits tables.

    Args:
        flow_path_transfer_capability: IASR flow path transfer capability table.
        initial_transmission_limits: IASR initial transmission limits table
            for REZ transmission network limits.
        renewable_energy_zones: IASR renewable energy zones table.

    Returns:
        Tuple of (network_transmission_paths, network_transmission_path_limits).
    """
    topology = _parse_flow_path_topology(flow_path_transfer_capability["Flow Paths"])
    flow_paths = _build_flow_paths(topology)
    flow_limits = _extract_flow_path_limits(
        flow_path_transfer_capability, topology["path_id"]
    )
    rez_paths = _extract_rez_connection_rows(renewable_energy_zones)
    rez_limits = _extract_rez_limits(initial_transmission_limits, rez_paths)

    paths = pd.concat([flow_paths, rez_paths], ignore_index=True)
    limits = pd.concat([flow_limits, rez_limits], ignore_index=True)
    limits = _collapse_paths_with_no_limits(limits)
    return paths, limits


# --- Flow path extraction ---


def _build_flow_paths(topology: pd.DataFrame) -> pd.DataFrame:
    carrier = topology["path_id"].map(
        lambda pid: "DC" if pid in _HVDC_PATH_IDS else "AC"
    )
    return topology.assign(carrier=carrier)[
        ["path_id", "geo_from", "geo_to", "carrier"]
    ]


def _extract_flow_path_limits(
    flow_path_transfer_capability: pd.DataFrame,
    path_ids: pd.Series,
) -> pd.DataFrame:
    df = flow_path_transfer_capability.copy()
    df.columns = _fix_column_typos(df.columns)
    df["path_id"] = path_ids.values
    return _melt_flow_path_capacity(df, _FLOW_PATH_COLUMN_RENAMES)


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


# --- REZ extraction ---


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


def _extract_rez_limits(
    initial_transmission_limits: pd.DataFrame,
    rez_connection_rows: pd.DataFrame,
) -> pd.DataFrame:
    # Left-merge: REZs absent from initial_transmission_limits get NaN rows
    # (collapsed later by _collapse_paths_with_no_limits).
    joined = rez_connection_rows[["path_id", "geo_from"]].merge(
        initial_transmission_limits,
        left_on="geo_from",
        right_on="REZ ID",
        how="left",
    )
    limits = _melt_rez_capacity(joined, _REZ_COLUMN_RENAMES)
    return _duplicate_for_both_directions(limits)


# --- Shared helpers ---


def _fix_column_typos(columns: pd.Index) -> pd.Index:
    # IASR workbook v7.5 has "refernce" instead of "reference" in some columns.
    return columns.str.replace("refernce", "reference", regex=False)


def _melt_flow_path_capacity(
    df: pd.DataFrame,
    column_renames: dict[str, str],
) -> pd.DataFrame:
    renamed = df.rename(columns=column_renames)[
        ["path_id"] + list(column_renames.values())
    ]
    melted = renamed.melt(
        id_vars="path_id", var_name="dir_timeslice", value_name="capacity"
    )
    melted["capacity"] = pd.to_numeric(melted["capacity"], errors="coerce")
    melted["direction"] = melted["dir_timeslice"].str.split("__").str[0]
    melted["timeslice"] = melted["dir_timeslice"].str.split("__").str[1]
    return melted[["path_id", "direction", "timeslice", "capacity"]]


def _melt_rez_capacity(
    df: pd.DataFrame,
    column_renames: dict[str, str],
) -> pd.DataFrame:
    renamed = df.rename(columns=column_renames)[
        ["path_id"] + list(column_renames.values())
    ]
    melted = renamed.melt(
        id_vars="path_id", var_name="timeslice", value_name="capacity"
    )
    melted["capacity"] = pd.to_numeric(melted["capacity"], errors="coerce")
    return melted[["path_id", "timeslice", "capacity"]]


def _duplicate_for_both_directions(limits: pd.DataFrame) -> pd.DataFrame:
    # REZ limits are symmetric — emit identical rows for both forward and reverse directions.
    forward = limits.assign(direction="forward")
    reverse = limits.assign(direction="reverse")
    combined = pd.concat([forward, reverse], ignore_index=True)
    return combined[["path_id", "direction", "timeslice", "capacity"]]


def _collapse_paths_with_no_limits(limits: pd.DataFrame) -> pd.DataFrame:
    # Paths with all-NaN capacity collapse to one row (path_id only, other fields NaN)
    # so the output is concise. Translator fills in a default capacity for these.
    paths_with_data = limits.dropna(subset=["capacity"])["path_id"].unique()
    has_data = limits["path_id"].isin(paths_with_data)
    collapsed = pd.DataFrame(
        {"path_id": limits.loc[~has_data, "path_id"].unique()}
    ).reindex(columns=limits.columns)
    return pd.concat([limits[has_data], collapsed], ignore_index=True)
