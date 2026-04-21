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

    I/O Example:
        Column names are abbreviated below; real IASR names are in
        ``_FLOW_PATH_COLUMN_RENAMES`` and ``_REZ_COLUMN_RENAMES``.

        Inputs:

            flow_path_transfer_capability:
                Flow Paths,  Fwd_Peak,  Fwd_Sum,  Fwd_Win,  Rev_Peak,  Rev_Sum,  Rev_Win
                CQ-NQ,       1200,      1200,     1400,     1440,      1440,     1910
                MN-SA,       ,          ,         ,         ,          ,

            initial_transmission_limits:
                REZ ID,  Peak,  Sum,  Win
                Q1,      750,   750,  750

            renewable_energy_zones:
                ID,  Name,           NEM region,  ISP sub-region
                Q1,  Far North,      QLD,         NQ
                N1,  Hunter Valley,  NSW,         CNSW

        Outputs:

            paths:
                path_id   geo_from  geo_to  carrier
                CQ-NQ     CQ        NQ      AC
                MN-SA     MN        SA      AC
                Q1-NQ     Q1        NQ      AC
                N1-CNSW   N1        CNSW    AC

            limits:
                path_id   direction  timeslice         capacity
                CQ-NQ     forward    peak_demand       1200       # flow path with values: 6 rows
                ...
                Q1-NQ     forward    peak_demand       750        # REZ with values: 6 rows, symmetric
                ...
                MN-SA     (NaN)      (NaN)             (NaN)      # flow path all blank -> collapsed
                N1-CNSW   (NaN)      (NaN)             (NaN)      # REZ absent from initial_transmission_limits -> collapsed
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
    """Adds the carrier column (AC/DC) to parsed flow path topology.

    I/O Example:
        topology:
            path_id             geo_from  geo_to
            CQ-NQ               CQ        NQ
            TAS-SEV             TAS       SEV
            NNSW-SQ_Terranora   NNSW      SQ

        returns:
            path_id             geo_from  geo_to  carrier
            CQ-NQ               CQ        NQ      AC
            TAS-SEV             TAS       SEV     DC    # known HVDC (in _HVDC_PATH_IDS)
            NNSW-SQ_Terranora   NNSW      SQ      DC    # known HVDC
    """
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
    """Attaches precomputed path_ids to the IASR flow path table and melts to long format.

    I/O Example:
        Column names abbreviated.

        flow_path_transfer_capability:
            Flow Paths,  Fwd_Peak,  Fwd_Sum,  Fwd_Win,  Rev_Peak,  Rev_Sum,  Rev_Win
            CQ-NQ,       1200,      1200,     1400,     1440,      1440,     1910

        path_ids:
            0  CQ-NQ

        returns:
            path_id  direction  timeslice         capacity
            CQ-NQ    forward    peak_demand       1200
            CQ-NQ    forward    summer_typical    1200
            CQ-NQ    forward    winter_reference  1400
            CQ-NQ    reverse    peak_demand       1440
            CQ-NQ    reverse    summer_typical    1440
            CQ-NQ    reverse    winter_reference  1910
    """
    df = flow_path_transfer_capability.copy()
    df.columns = _fix_column_typos(df.columns)
    df["path_id"] = path_ids.values
    return _melt_flow_path_capacity(df, _FLOW_PATH_COLUMN_RENAMES)


def _parse_flow_path_topology(name_series: pd.Series) -> pd.DataFrame:
    """Parses IASR flow path name strings into geo_from, geo_to, and path_id columns.

    I/O Example:
        name_series:
            0  CQ-NQ
            1  NNSW-SQ (Terranora)
            2  CNSW-SNW-NTH

        returns:
            geo_from  geo_to  path_id
            CQ        NQ      CQ-NQ
            NNSW      SQ      NNSW-SQ_Terranora      # parenthesized suffix
            CNSW      SNW     CNSW-SNW_NTH           # dash-separated suffix
    """
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
    """Strips parentheses or leading dash from a parallel-path suffix.

    I/O Example:
        "(Terranora)"  -> "Terranora"      # parenthesized
        "-NTH"         -> "NTH"            # dash-separated
        ""             -> ""               # no suffix
    """
    suffix = suffix.strip()
    if not suffix:
        return ""
    return suffix.strip("()").lstrip("-").strip()


def _build_path_id(geo_from: str, geo_to: str, suffix: str) -> str:
    """Joins geo codes (and optional suffix) into a path_id.

    I/O Example:
        ("CQ", "NQ", "")           -> "CQ-NQ"
        ("NNSW", "SQ", "Terranora") -> "NNSW-SQ_Terranora"
    """
    path_id = f"{geo_from}-{geo_to}"
    if suffix:
        path_id = f"{path_id}_{suffix}"
    return path_id


# --- REZ extraction ---


def _extract_rez_connection_rows(
    renewable_energy_zones: pd.DataFrame,
) -> pd.DataFrame:
    """Builds one transmission path row per REZ, connecting it to its parent subregion.

    I/O Example:
        renewable_energy_zones:
            ID,  Name,       NEM region,  ISP sub-region
            Q1,  Far North,  QLD,         NQ
            N1,  Hunter,     NSW,         CNSW

        returns:
            path_id   geo_from  geo_to  carrier
            Q1-NQ     Q1        NQ      AC
            N1-CNSW   N1        CNSW    AC
    """
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
    """Produces long-format REZ limits, mirrored across both directions.

    Uses a left-merge so REZs absent from initial_transmission_limits still produce
    rows (filled with NaN) — collapsed later by _collapse_paths_with_no_limits.

    I/O Example:
        Column names abbreviated.

        initial_transmission_limits:
            REZ ID,  Peak,  Sum,  Win
            Q1,      750,   750,  750

        rez_connection_rows:
            path_id   geo_from  geo_to  carrier
            Q1-NQ     Q1        NQ      AC
            N1-CNSW   N1        CNSW    AC             # N1 absent from initial_transmission_limits

        returns:
            path_id   direction  timeslice         capacity
            Q1-NQ     forward    peak_demand       750
            Q1-NQ     forward    summer_typical    750
            Q1-NQ     forward    winter_reference  750
            Q1-NQ     reverse    peak_demand       750
            Q1-NQ     reverse    summer_typical    750
            Q1-NQ     reverse    winter_reference  750
            N1-CNSW   forward    peak_demand       (NaN)   # absent -> NaN rows
            ... (6 NaN rows total for N1-CNSW)
    """
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
    """Patches known IASR workbook v7.5 column-name typos.

    I/O Example:
        Index(['...Winter refernce', 'Flow Paths'])
            -> Index(['...Winter reference', 'Flow Paths'])
    """
    return columns.str.replace("refernce", "reference", regex=False)


def _melt_flow_path_capacity(
    df: pd.DataFrame,
    column_renames: dict[str, str],
) -> pd.DataFrame:
    """Melts wide 'direction__timeslice' capacity columns into long format.

    I/O Example:
        Column names abbreviated (real names in _FLOW_PATH_COLUMN_RENAMES keys).

        df:
            path_id  Fwd_Peak  Fwd_Sum  Fwd_Win  Rev_Peak  Rev_Sum  Rev_Win
            CQ-NQ    1200      1200     1400     1440      1440     1910

        column_renames:
            {"Fwd_Peak": "forward__peak_demand", ..., "Rev_Win": "reverse__winter_reference"}

        returns:
            path_id  direction  timeslice         capacity
            CQ-NQ    forward    peak_demand       1200
            CQ-NQ    forward    summer_typical    1200
            CQ-NQ    forward    winter_reference  1400
            CQ-NQ    reverse    peak_demand       1440
            CQ-NQ    reverse    summer_typical    1440
            CQ-NQ    reverse    winter_reference  1910
    """
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
    """Melts wide timeslice capacity columns into long format (no direction).

    I/O Example:
        Column names abbreviated (real names in _REZ_COLUMN_RENAMES keys).

        df:
            path_id  Peak  Sum  Win
            Q1-NQ    750   750  750

        column_renames:
            {"Peak": "peak_demand", "Sum": "summer_typical", "Win": "winter_reference"}

        returns:
            path_id  timeslice         capacity
            Q1-NQ    peak_demand       750
            Q1-NQ    summer_typical    750
            Q1-NQ    winter_reference  750
    """
    renamed = df.rename(columns=column_renames)[
        ["path_id"] + list(column_renames.values())
    ]
    melted = renamed.melt(
        id_vars="path_id", var_name="timeslice", value_name="capacity"
    )
    melted["capacity"] = pd.to_numeric(melted["capacity"], errors="coerce")
    return melted[["path_id", "timeslice", "capacity"]]


def _duplicate_for_both_directions(limits: pd.DataFrame) -> pd.DataFrame:
    """Mirrors each row into a forward and a reverse entry (REZ limits are symmetric).

    I/O Example:
        limits:
            path_id  timeslice         capacity
            Q1-NQ    peak_demand       750
            Q1-NQ    summer_typical    750

        returns:
            path_id  direction  timeslice         capacity
            Q1-NQ    forward    peak_demand       750
            Q1-NQ    forward    summer_typical    750
            Q1-NQ    reverse    peak_demand       750
            Q1-NQ    reverse    summer_typical    750
    """
    forward = limits.assign(direction="forward")
    reverse = limits.assign(direction="reverse")
    combined = pd.concat([forward, reverse], ignore_index=True)
    return combined[["path_id", "direction", "timeslice", "capacity"]]


def _collapse_paths_with_no_limits(limits: pd.DataFrame) -> pd.DataFrame:
    """Collapses paths with all-NaN capacity into a single path_id-only row.

    A path with even one non-NaN capacity value is kept in full, including its
    NaN rows. The translator applies a default capacity to the collapsed rows.

    I/O Example:
        limits:
            path_id   direction  timeslice         capacity
            CQ-NQ     forward    peak_demand       1200
            CQ-NQ     reverse    peak_demand       1440
            MN-SA     forward    peak_demand       (NaN)
            MN-SA     reverse    peak_demand       (NaN)

        returns:
            path_id   direction  timeslice         capacity
            CQ-NQ     forward    peak_demand       1200       # has data: kept
            CQ-NQ     reverse    peak_demand       1440
            MN-SA     (NaN)      (NaN)             (NaN)      # all-NaN: collapsed to one row
    """
    paths_with_data = limits.dropna(subset=["capacity"])["path_id"].unique()
    has_data = limits["path_id"].isin(paths_with_data)
    collapsed = pd.DataFrame(
        {"path_id": limits.loc[~has_data, "path_id"].unique()}
    ).reindex(columns=limits.columns)
    return pd.concat([limits[has_data], collapsed], ignore_index=True)
