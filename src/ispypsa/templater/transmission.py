import logging

import pandas as pd

from ispypsa.templater.mappings import _CANONICAL_TIMESLICES, _SINGLE_REGION_ID

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
    sub_regional_geography: pd.DataFrame,
    regional_granularity: str,
    flow_path_options: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Creates the network_transmission_paths and network_transmission_path_limits tables.

    Sub-regional paths and limits are built first, then each limit's timeslice is
    prefixed with the NEM region whose demand condition sets it (see
    ``_add_region_to_timeslices`` and Open-ISP/ISPyPSA#109). If a coarser
    granularity is requested, the result is aggregated to that level. Finally,
    augmentation keys without an existing path (new parallel corridors) are appended.

    Args:
        flow_path_transfer_capability: IASR flow path transfer capability table.
        initial_transmission_limits: IASR initial transmission limits table
            for REZ transmission network limits.
        renewable_energy_zones: IASR renewable energy zones table.
        sub_regional_geography: the sub_regional ``network_geography`` table —
            its geo_id -> region_id mapping (covering sub-regions and REZs) drives
            both the timeslice region prefix and the cross-region flow-path
            identification used when aggregating to a coarser granularity.
        regional_granularity: one of "sub_regions", "nem_regions", or
            "single_region".
        flow_path_options: granularity-filtered dict of flow-path augmentation
            options, keyed by path_id. Keys without an existing path become
            new zero-capacity parallel corridors (see ``_append_new_parallel_paths``).
            ``None`` (the default) means no augmentation data — used by tests that
            don't exercise parallel-path behaviour.

    Returns:
        Tuple of (network_transmission_paths, network_transmission_path_limits).

    I/O Example:
        Real IASR column names are in ``_FLOW_PATH_COLUMN_RENAMES`` and
        ``_REZ_COLUMN_RENAMES``; abbreviated names are used here. Timeslices are
        region-prefixed: the forward limit carries the destination region's demand
        condition, the reverse limit the origin region's (see
        ``_add_region_to_timeslices``).

        Inputs:

            flow_path_transfer_capability:
                Flow Paths,  Fwd_Peak,  Fwd_Sum,  Fwd_Win,  Rev_Peak,  Rev_Sum,  Rev_Win
                CQ-NQ,       1200,      1200,     1400,     1440,      1440,     1910
                NNSW-SQ,     950,       950,      950,      1450,      1450,     1450
                MN-SA,       (NaN),     (NaN),    (NaN),    (NaN),     (NaN),    (NaN)

            initial_transmission_limits:
                REZ ID,  Peak,  Sum,  Win
                Q1,      750,   750,  750

            renewable_energy_zones:
                ID,  Name,           NEM region,  ISP sub-region
                Q1,  Far North,      QLD,         NQ
                N1,  Hunter Valley,  NSW,         CNSW

        regional_granularity = "sub_regions":

            returns paths:
                path_id   geo_from  geo_to  carrier
                CQ-NQ     CQ        NQ      AC
                NNSW-SQ   NNSW      SQ      AC
                MN-SA     MN        SA      AC
                Q1-NQ     Q1        NQ      AC
                N1-CNSW   N1        CNSW    AC

            returns limits (one forward + one reverse row shown per path; 6 rows each):
                path_id   direction  timeslice            capacity
                CQ-NQ     forward    qld_peak_demand      1200      # into NQ (QLD)
                CQ-NQ     reverse    qld_peak_demand      1440      # into CQ (QLD): intra-region
                NNSW-SQ   forward    qld_peak_demand      950       # into SQ (QLD)
                NNSW-SQ   reverse    nsw_peak_demand      1450      # into NNSW (NSW)
                Q1-NQ     forward    qld_peak_demand      750       # REZ in QLD, symmetric
                Q1-NQ     reverse    qld_peak_demand      750
                MN-SA     (NaN)      (NaN)                (NaN)     # all-blank flow path -> collapsed
                N1-CNSW   (NaN)      (NaN)                (NaN)     # REZ absent from limits -> collapsed

        regional_granularity = "nem_regions":

            returns paths:
                path_id   geo_from  geo_to  carrier
                NSW-QLD   NSW       QLD     AC                  # was NNSW-SQ (cross-region)
                Q1-QLD    Q1        QLD     AC                  # REZ geo_to retargeted
                N1-NSW    N1        NSW     AC                  # REZ geo_to retargeted
                # CQ-NQ dropped (intra-QLD)

            returns limits (prefixes ride through the path-id re-keying unchanged):
                path_id   direction  timeslice            capacity
                NSW-QLD   forward    qld_peak_demand      950
                NSW-QLD   reverse    nsw_peak_demand      1450
                Q1-QLD    forward    qld_peak_demand      750
                N1-NSW    (NaN)      (NaN)                (NaN)    # collapsed row preserved

        regional_granularity = "single_region":

            returns paths:
                path_id  geo_from  geo_to  carrier
                Q1-NEM   Q1        NEM     AC
                N1-NEM   N1        NEM     AC
                # All inter-subregional flow paths dropped; only REZ paths remain.

            returns limits (REZ keeps its real region prefix despite geo_to -> NEM):
                path_id  direction  timeslice            capacity
                Q1-NEM   forward    qld_peak_demand      750
                N1-NEM   (NaN)      (NaN)                (NaN)

        When ``flow_path_options`` contains keys without a matching path_id (e.g.
        ``CNSW-SNW`` when only ``CNSW-SNW_NTH``/``_STH`` exist), those corridors are
        appended as zero-capacity parallel paths — see ``_append_new_parallel_paths``
        for an example.
    """
    topology = _parse_flow_path_topology(flow_path_transfer_capability["Flow Paths"])
    flow_paths = _add_flow_path_carrier(topology)
    flow_limits = _extract_flow_path_limits(
        flow_path_transfer_capability, topology["path_id"]
    )
    rez_paths = _extract_rez_connection_rows(renewable_energy_zones)
    rez_limits = _extract_rez_limits(initial_transmission_limits, rez_paths)

    paths = pd.concat([flow_paths, rez_paths], ignore_index=True)
    limits = pd.concat([flow_limits, rez_limits], ignore_index=True)
    limits = _collapse_paths_with_no_limits(limits)
    region_lookup = _build_geo_region_lookup(sub_regional_geography)
    limits = _add_region_to_timeslices(limits, paths, region_lookup)
    paths, limits = _aggregate_to_granularity(
        paths,
        limits,
        regional_granularity,
        renewable_energy_zones,
        region_lookup,
    )
    return _append_new_parallel_paths(
        paths, limits, flow_path_options or {}, region_lookup
    )


def _aggregate_to_granularity(
    paths: pd.DataFrame,
    limits: pd.DataFrame,
    regional_granularity: str,
    renewable_energy_zones: pd.DataFrame,
    region_lookup: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Dispatches to the appropriate aggregation step for the chosen granularity."""
    if regional_granularity == "sub_regions":
        return paths, limits
    rez_ids = set(renewable_energy_zones["ID"])
    if regional_granularity == "single_region":
        return _aggregate_to_single_region(paths, limits, rez_ids)
    if regional_granularity == "nem_regions":
        return _aggregate_to_nem_regions(paths, limits, region_lookup, rez_ids)
    raise ValueError(f"Unknown regional_granularity: {regional_granularity!r}")


# --- Region-prefixed timeslices ---


def _build_geo_region_lookup(sub_regional_geography: pd.DataFrame) -> dict[str, str]:
    """Maps every geo (sub-region, REZ, or NEM region) to its NEM region id.

    ``sub_regional_geography`` already lists both sub-regions and REZs against their
    ``region_id``. NEM regions are added as identities so that geos which are
    already regions — after granularity aggregation, or on new parallel corridors —
    resolve to themselves.

    I/O Example:
        sub_regional_geography:
            geo_id  geo_type   region_id
            NQ      subregion  QLD
            CNSW    subregion  NSW
            Q1      rez        QLD

        returns:
            {"NQ": "QLD", "CNSW": "NSW", "Q1": "QLD", "QLD": "QLD", "NSW": "NSW"}
    """
    lookup = dict(
        zip(sub_regional_geography["geo_id"], sub_regional_geography["region_id"])
    )
    for region in set(sub_regional_geography["region_id"]):
        lookup[region] = region
    return lookup


def _add_region_to_timeslices(
    limits: pd.DataFrame,
    paths: pd.DataFrame,
    region_lookup: dict[str, str],
) -> pd.DataFrame:
    """Prefixes each timeslice with the NEM region whose demand condition sets it.

    PLEXOS tags a path's forward (geo_from -> geo_to) limit with the *destination*
    region's demand condition and its reverse limit with the *origin* region's — the
    receiving region's load is what tightens the limit (Open-ISP/ISPyPSA#109). The
    region id is lowercased and prefixed onto the canonical timeslice so the value
    matches the custom-constraints vocabulary (``qld_peak_demand`` etc.). Collapsed
    rows (NaN timeslice) are left untouched.

    Applied at the sub-regional level, before any granularity aggregation, so that a
    REZ — whose forward and reverse endpoints sit in the same region — stays
    symmetric even when single_region later retargets its geo_to to ``NEM``.

    I/O Example:
        limits:
            path_id  direction  timeslice         capacity
            NNSW-SQ  forward    peak_demand       950        # into SQ (QLD)
            NNSW-SQ  reverse    peak_demand       1450       # into NNSW (NSW)
            Q1-NQ    reverse    summer_typical    750        # REZ Q1 in QLD
            SA-VIC   (NaN)      (NaN)             (NaN)      # collapsed: untouched

        paths:
            path_id  geo_from  geo_to
            NNSW-SQ  NNSW      SQ
            Q1-NQ    Q1        NQ
            SA-VIC   SA        VIC

        region_lookup: {"NNSW": "NSW", "SQ": "QLD", "Q1": "QLD", "NQ": "QLD", ...}

        returns:
            path_id  direction  timeslice            capacity
            NNSW-SQ  forward    qld_peak_demand      950
            NNSW-SQ  reverse    nsw_peak_demand      1450
            Q1-NQ    reverse    qld_summer_typical   750
            SA-VIC   (NaN)      (NaN)                (NaN)

        Raises ValueError if any path geo is absent from ``region_lookup`` (every
        endpoint must resolve to a region, else its prefix would be NaN).
    """
    _assert_geos_have_regions(paths, region_lookup)
    merged = limits.merge(
        paths[["path_id", "geo_from", "geo_to"]], on="path_id", how="left"
    )
    tagged = merged["timeslice"].notna()
    if not tagged.any():
        return merged[["path_id", "direction", "timeslice", "capacity"]]
    endpoint = merged["geo_to"].where(
        merged["direction"] == "forward", merged["geo_from"]
    )
    lower_region = {geo: region.lower() for geo, region in region_lookup.items()}
    merged.loc[tagged, "timeslice"] = (
        endpoint[tagged].map(lower_region) + "_" + merged.loc[tagged, "timeslice"]
    )
    return merged[["path_id", "direction", "timeslice", "capacity"]]


# --- Flow path extraction ---


def _add_flow_path_carrier(topology: pd.DataFrame) -> pd.DataFrame:
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
        Inputs:

            flow_path_transfer_capability:
                Flow Paths,  Fwd_Peak,  Fwd_Sum,  Fwd_Win,  Rev_Peak,  Rev_Sum,  Rev_Win
                CQ-NQ,       1200,      1200,     1400,     1440,      1440,     1910

            path_ids:
                CQ-NQ

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
    melted = _melt_flow_path_capacity(df, _FLOW_PATH_COLUMN_RENAMES)
    _log_flow_paths_with_no_capacity_data(melted)
    return melted


def _parse_flow_path_topology(name_series: pd.Series) -> pd.DataFrame:
    """Parses IASR flow path name strings into geo_from, geo_to, and path_id columns.

    I/O Example:
        name_series:
            CQ-NQ
            NNSW-SQ (Terranora)
            CNSW-SNW-NTH

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
        Inputs:

            initial_transmission_limits:
                REZ ID,  Peak,  Sum,  Win
                Q1,      750,   750,  750

            rez_connection_rows:
                path_id   geo_from  geo_to  carrier
                Q1-NQ     Q1        NQ      AC
                N1-CNSW   N1        CNSW    AC          # N1 absent from limits

        returns:
            path_id   direction  timeslice         capacity
            Q1-NQ     forward    peak_demand       750
            Q1-NQ     forward    summer_typical    750
            Q1-NQ     forward    winter_reference  750
            Q1-NQ     reverse    peak_demand       750
            Q1-NQ     reverse    summer_typical    750
            Q1-NQ     reverse    winter_reference  750
            N1-CNSW   forward    peak_demand       (NaN)    # 6 NaN rows for N1-CNSW
            N1-CNSW   forward    summer_typical    (NaN)
            N1-CNSW   forward    winter_reference  (NaN)
            N1-CNSW   reverse    peak_demand       (NaN)
            N1-CNSW   reverse    summer_typical    (NaN)
            N1-CNSW   reverse    winter_reference  (NaN)
    """
    _log_rez_paths_absent_from_limits(rez_connection_rows, initial_transmission_limits)
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
        Real column names are in ``_FLOW_PATH_COLUMN_RENAMES``; abbreviated here.

        Inputs:

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
        Real column names are in ``_REZ_COLUMN_RENAMES``; abbreviated here.

        Inputs:

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


# --- Logging of missing data ---


def _log_flow_paths_with_no_capacity_data(limits: pd.DataFrame) -> None:
    """Logs flow paths whose IASR capacity row was entirely blank/non-numeric.

    These paths will be collapsed to a single path_id-only row downstream and
    receive a default capacity from the translator.
    """
    paths_with_data = limits.dropna(subset=["capacity"])["path_id"].unique()
    missing = sorted(set(limits["path_id"]) - set(paths_with_data))
    if missing:
        logging.warning(
            f"Flow paths with no capacity data in IASR table "
            f"(default will be applied downstream): {missing}"
        )


def _log_rez_paths_absent_from_limits(
    rez_connection_rows: pd.DataFrame,
    initial_transmission_limits: pd.DataFrame,
) -> None:
    """Logs REZ paths whose REZ ID has no row in initial_transmission_limits.

    These paths get NaN capacity rows from the left-merge, are collapsed by
    ``_collapse_paths_with_no_limits``, and receive a default downstream.
    """
    rez_with_limits = set(initial_transmission_limits["REZ ID"])
    missing = sorted(set(rez_connection_rows["geo_from"]) - rez_with_limits)
    if missing:
        logging.warning(
            f"REZs absent from initial_transmission_limits "
            f"(default will be applied downstream): {missing}"
        )


# --- Granularity aggregation ---


def _aggregate_to_nem_regions(
    paths: pd.DataFrame,
    limits: pd.DataFrame,
    region_lookup: dict[str, str],
    rez_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filters/re-keys paths and limits to NEM-region geos.

    Sub-region flow paths that don't cross a NEM region boundary are dropped.
    REZ paths are kept; their geo_to switches from sub-region to NEM region.
    Path IDs are rebuilt from the new geo_from/geo_to (suffixes preserved).

    I/O Example:
        Inputs:

            paths:
                path_id            geo_from  geo_to  carrier
                CQ-NQ              CQ        NQ      AC        # both QLD -> dropped
                NNSW-SQ            NNSW      SQ      AC        # NSW -> QLD -> kept
                NNSW-SQ_Terranora  NNSW      SQ      DC        # parallel, kept
                Q1-NQ              Q1        NQ      AC        # REZ, geo_to retargeted
                N1-CNSW            N1        CNSW    AC        # REZ, geo_to retargeted

            limits (already region-prefixed by ``_add_region_to_timeslices``):
                path_id   direction  timeslice        capacity
                CQ-NQ     forward    qld_peak_demand  1200
                NNSW-SQ   forward    qld_peak_demand  950
                Q1-NQ     forward    qld_peak_demand  750
                N1-CNSW   (NaN)      (NaN)            (NaN)

            region_lookup:
                {"NQ": "QLD", "CQ": "QLD", "NNSW": "NSW", "SQ": "QLD",
                 "Q1": "QLD", "N1": "NSW"}

            rez_ids:
                {"Q1", "N1"}

        returns paths:
            path_id            geo_from  geo_to  carrier
            NSW-QLD            NSW       QLD     AC
            NSW-QLD_Terranora  NSW       QLD     DC
            Q1-QLD             Q1        QLD     AC
            N1-NSW             N1        NSW     AC

        returns limits (path_ids re-keyed; the timeslice prefix is untouched):
            path_id  direction  timeslice        capacity
            NSW-QLD  forward    qld_peak_demand  950             # CQ-NQ row dropped
            Q1-QLD   forward    qld_peak_demand  750
            N1-NSW   (NaN)      (NaN)            (NaN)           # collapsed row preserved through rename

        Geo resolution is already validated upstream by ``_add_region_to_timeslices``
        (run before aggregation), so every geo here is known to map to a region.
    """
    flow_paths = paths[~paths["geo_from"].isin(rez_ids)]
    rez_paths = paths[paths["geo_from"].isin(rez_ids)]
    flow_paths = _filter_to_cross_region_flow_paths(flow_paths, region_lookup)
    new_flow_paths, flow_renames = _remap_flow_paths_to_regions(
        flow_paths, region_lookup
    )
    new_rez_paths, rez_renames = _remap_rez_paths(
        rez_paths, rez_paths["geo_to"].map(region_lookup)
    )
    new_paths = pd.concat([new_flow_paths, new_rez_paths], ignore_index=True)
    rename_map = {**flow_renames, **rez_renames}
    new_limits = _remap_limit_path_ids(limits, rename_map)
    return new_paths, new_limits


def _aggregate_to_single_region(
    paths: pd.DataFrame,
    limits: pd.DataFrame,
    rez_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Drops inter-subregional flow paths; re-keys REZ paths to a single NEM geo.

    I/O Example:
        Inputs:

            paths:
                path_id   geo_from  geo_to  carrier
                CQ-NQ     CQ        NQ      AC          # flow path -> dropped
                NNSW-SQ   NNSW      SQ      AC          # flow path -> dropped
                Q1-NQ     Q1        NQ      AC          # REZ -> retargeted
                N1-CNSW   N1        CNSW    AC          # REZ -> retargeted

            limits (already region-prefixed by ``_add_region_to_timeslices``):
                path_id  direction  timeslice        capacity
                CQ-NQ    forward    qld_peak_demand  1200
                Q1-NQ    forward    qld_peak_demand  750

            rez_ids:
                {"Q1", "N1"}

        returns paths:
            path_id  geo_from  geo_to  carrier
            Q1-NEM   Q1        NEM     AC
            N1-NEM   N1        NEM     AC

        returns limits (REZ keeps its own qld prefix though geo_to is now NEM):
            path_id  direction  timeslice        capacity
            Q1-NEM   forward    qld_peak_demand  750            # CQ-NQ row dropped
    """
    rez_paths = paths[paths["geo_from"].isin(rez_ids)]
    new_rez_paths, rename_map = _remap_rez_paths(rez_paths, _SINGLE_REGION_ID)
    new_limits = _remap_limit_path_ids(limits, rename_map)
    return new_rez_paths, new_limits


def _assert_geos_have_regions(
    paths: pd.DataFrame,
    region_lookup: dict[str, str],
) -> None:
    """Raises ValueError if any path endpoint is absent from ``region_lookup``.

    Every ``geo_from`` and ``geo_to`` — sub-region, REZ id, or already-aggregated
    region — must resolve to a NEM region, because the timeslice region prefix and
    the cross-region path re-keying both look the endpoint up. A missing geo would
    otherwise produce a NaN prefix or corrupt the output path IDs silently.

    I/O Example:
        paths:
            path_id  geo_from  geo_to
            CQ-NQ    CQ        NQ
            MN-SA    MN        SA            # MN, SA missing from region_lookup
            Q1-NQ    Q1        NQ

        region_lookup:
            {"CQ": "QLD", "NQ": "QLD", "Q1": "QLD"}

        raises:
            ValueError: Path geos missing from sub_regional_geography: ['MN', 'SA']
    """
    geos = pd.concat([paths["geo_from"], paths["geo_to"]]).unique()
    missing = sorted(set(geos) - set(region_lookup.keys()))
    if missing:
        raise ValueError(f"Path geos missing from sub_regional_geography: {missing}")


def _filter_to_cross_region_flow_paths(
    flow_paths: pd.DataFrame, region_lookup: dict[str, str]
) -> pd.DataFrame:
    """Keeps only flow paths whose endpoints sit in different NEM regions.

    I/O Example:
        Inputs:

            flow_paths:
                path_id   geo_from  geo_to  carrier
                CQ-NQ     CQ        NQ      AC          # both QLD -> dropped
                NNSW-SQ   NNSW      SQ      AC          # NSW -> QLD -> kept

            region_lookup:
                {"CQ": "QLD", "NQ": "QLD", "NNSW": "NSW", "SQ": "QLD"}

        returns:
            path_id  geo_from  geo_to  carrier
            NNSW-SQ  NNSW      SQ      AC
    """
    region_from = flow_paths["geo_from"].map(region_lookup)
    region_to = flow_paths["geo_to"].map(region_lookup)
    return flow_paths[region_from != region_to]


def _remap_flow_paths_to_regions(
    flow_paths: pd.DataFrame, region_lookup: dict[str, str]
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Re-keys cross-region flow paths to NEM-region geos and returns the rename map.

    I/O Example:
        Inputs:

            flow_paths:
                path_id            geo_from  geo_to  carrier
                NNSW-SQ            NNSW      SQ      AC
                NNSW-SQ_Terranora  NNSW      SQ      DC

            region_lookup:
                {"NNSW": "NSW", "SQ": "QLD"}

        returns paths:
            path_id            geo_from  geo_to  carrier
            NSW-QLD            NSW       QLD     AC
            NSW-QLD_Terranora  NSW       QLD     DC

        returns rename_map:
            {"NNSW-SQ": "NSW-QLD", "NNSW-SQ_Terranora": "NSW-QLD_Terranora"}
    """
    new = flow_paths.copy()
    new["geo_from"] = new["geo_from"].map(region_lookup)
    new["geo_to"] = new["geo_to"].map(region_lookup)
    new_path_ids = _rebuild_path_ids(
        flow_paths["path_id"], new["geo_from"], new["geo_to"]
    )
    rename_map = dict(zip(flow_paths["path_id"], new_path_ids))
    new["path_id"] = new_path_ids.values
    return new[["path_id", "geo_from", "geo_to", "carrier"]], rename_map


def _rebuild_path_ids(
    old_path_ids: pd.Series, new_from: pd.Series, new_to: pd.Series
) -> pd.Series:
    """Rebuilds path IDs as new_from-new_to, preserving any '_suffix' from the old ID.

    I/O Example:
        The three input Series are aligned by row; each row below shows one
        triple of (old_path_ids, new_from, new_to) and the corresponding output.

        Inputs (per-row):

            old_path_ids       new_from  new_to
            NNSW-SQ            NSW       QLD
            NNSW-SQ_Terranora  NSW       QLD
            CNSW-SNW_NTH       NSW       NSW

        returns:
            NSW-QLD
            NSW-QLD_Terranora
            NSW-NSW_NTH
    """
    suffix = old_path_ids.str.split("_", n=1).str[1].fillna("")
    base = new_from.astype(str) + "-" + new_to.astype(str)
    return (base + "_" + suffix).where(suffix != "", base)


def _remap_rez_paths(
    rez_paths: pd.DataFrame, new_geo_to
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Switches each REZ path's geo_to and rebuilds path_id accordingly.

    ``new_geo_to`` may be a Series (per-row replacement, used for nem_regions)
    or a scalar string (broadcast, used for single_region).

    I/O Example:
        rez_paths:
            path_id   geo_from  geo_to  carrier
            Q1-NQ     Q1        NQ      AC
            N1-CNSW   N1        CNSW    AC

        new_geo_to = pd.Series(["QLD", "NSW"]):

            returns paths:
                path_id  geo_from  geo_to  carrier
                Q1-QLD   Q1        QLD     AC
                N1-NSW   N1        NSW     AC

            returns rename_map:
                {"Q1-NQ": "Q1-QLD", "N1-CNSW": "N1-NSW"}

        new_geo_to = "NEM":

            returns paths:
                path_id  geo_from  geo_to  carrier
                Q1-NEM   Q1        NEM     AC
                N1-NEM   N1        NEM     AC

            returns rename_map:
                {"Q1-NQ": "Q1-NEM", "N1-CNSW": "N1-NEM"}
    """
    new = rez_paths.copy()
    new["geo_to"] = new_geo_to
    new_path_ids = new["geo_from"] + "-" + new["geo_to"].astype(str)
    rename_map = dict(zip(rez_paths["path_id"], new_path_ids))
    new["path_id"] = new_path_ids.values
    return new[["path_id", "geo_from", "geo_to", "carrier"]], rename_map


def _remap_limit_path_ids(
    limits: pd.DataFrame, rename_map: dict[str, str]
) -> pd.DataFrame:
    """Drops limit rows whose path_id isn't in rename_map; re-keys the rest.

    I/O Example:
        Inputs:

            limits:
                path_id  direction  timeslice        capacity
                CQ-NQ    forward    qld_peak_demand  1200          # not in rename_map -> dropped
                NNSW-SQ  forward    qld_peak_demand  950
                Q1-NQ    forward    qld_peak_demand  750

            rename_map:
                {"NNSW-SQ": "NSW-QLD", "Q1-NQ": "Q1-QLD"}

        returns:
            path_id  direction  timeslice        capacity
            NSW-QLD  forward    qld_peak_demand  950
            Q1-QLD   forward    qld_peak_demand  750
    """
    kept = limits[limits["path_id"].isin(rename_map.keys())].copy()
    kept["path_id"] = kept["path_id"].map(rename_map)
    return kept.reset_index(drop=True)


# --- Augmentation-driven new parallel corridors ---

_NEW_PATH_DIRECTIONS = ("forward", "reverse")


def _append_new_parallel_paths(
    paths: pd.DataFrame,
    limits: pd.DataFrame,
    flow_path_options: dict[str, pd.DataFrame],
    region_lookup: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Appends topology + limit rows for augmentation corridors not already in the path table.

    Some flow-path augmentation tables in the IASR are keyed at corridor level
    (e.g. ``flow_path_augmentation_options_CNSW-SNW``), not at the level of an
    individual physical path. The corridor's existing topology is split across
    parallel suffixed paths (``CNSW-SNW_NTH``, ``CNSW-SNW_STH``), so the un-suffixed
    key has no matching ``path_id`` in the base topology — but it still describes
    a real transmission expansion option.

    Without injecting a synthetic path for that un-suffixed key, the expansion
    orchestrator misclassifies the option. ``_build_options_table`` discriminates
    physical paths from constraint groups by membership in
    ``network_transmission_paths``, so an unmatched key would emit a single
    ``constraint_relaxation`` row instead of forward+reverse expansion rows, and
    the translator would build a phantom constraint rather than an expandable Link.

    The fix is to inject a third Link alongside the NTH/STH siblings with no
    pre-existing capacity. The IASR's ``Development path`` column distinguishes
    NTH-specific, STH-specific, and "new corridor" options at the per-option level,
    but threading that through reliably wasn't straightforward and we chose
    simplicity: collapse all of them onto one new parallel link. This loses the
    directional preference between NTH/STH/new-build — fine until a custom
    constraint actually differentiates them.

    Limits on the new link are explicit zeros, not NaN. NaN means "translator
    applies default capacity" downstream (see ``_collapse_paths_with_no_limits``),
    which would let the model dispatch flow across a corridor that doesn't yet
    exist. Explicit zero forbids dispatch until the augmentation is actually built.

    See Open-ISP/ISPyPSA#96 for the original modelling decision.

    I/O Example:
        paths:
            path_id         geo_from  geo_to  carrier
            CNSW-SNW_NTH    CNSW      SNW     AC
            CNSW-SNW_STH    CNSW      SNW     AC

        limits (existing siblings, abbreviated; timeslices already region-prefixed):
            path_id         direction  timeslice        capacity
            CNSW-SNW_NTH    forward    nsw_peak_demand  900
            CNSW-SNW_STH    forward    nsw_peak_demand  800

        flow_path_options keys: {"CNSW-SNW"}   # un-suffixed corridor

        returns paths (new row appended):
            path_id         geo_from  geo_to  carrier
            CNSW-SNW_NTH    CNSW      SNW     AC
            CNSW-SNW_STH    CNSW      SNW     AC
            CNSW-SNW        CNSW      SNW     AC

        returns limits (six explicit-zero rows appended for CNSW-SNW: 2 directions x 3 timeslices.
        CNSW and SNW are both NSW, so all six carry the nsw prefix):
            path_id         direction  timeslice             capacity
            CNSW-SNW_NTH    forward    nsw_peak_demand       900
            CNSW-SNW_STH    forward    nsw_peak_demand       800
            CNSW-SNW        forward    nsw_peak_demand       0
            CNSW-SNW        forward    nsw_summer_typical    0
            CNSW-SNW        forward    nsw_winter_reference  0
            CNSW-SNW        reverse    nsw_peak_demand       0
            CNSW-SNW        reverse    nsw_summer_typical    0
            CNSW-SNW        reverse    nsw_winter_reference  0
    """
    new_paths, new_limits = _new_parallel_path_rows(
        flow_path_options, set(paths["path_id"]), region_lookup
    )
    paths = pd.concat([paths, new_paths], ignore_index=True)
    limits = pd.concat([limits, new_limits], ignore_index=True)
    return paths, limits


def _new_parallel_path_rows(
    flow_path_options: dict[str, pd.DataFrame],
    existing_path_ids: set[str],
    region_lookup: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Builds topology + zero-capacity limit rows for augmentation keys without an existing path.

    Limits are explicit zeros (not NaN) because these paths physically don't exist
    yet — NaN in this schema means "translator applies default capacity", which
    would let the model dispatch flow on a Link that hasn't been built. Timeslices
    are region-prefixed by ``_add_region_to_timeslices`` so the new corridor matches
    the rest of the limits table.

    I/O Example:
        flow_path_options keys: {"CQ-NQ", "CNSW-SNW"}
        existing_path_ids: {"CQ-NQ", "CNSW-SNW_NTH", "CNSW-SNW_STH"}
        region_lookup: {"CNSW": "NSW", "SNW": "NSW", ...}

        returns:
            paths:
                path_id    geo_from  geo_to  carrier
                CNSW-SNW   CNSW      SNW     AC
            limits (6 rows: 2 directions x 3 timeslices, all 0 MW, nsw-prefixed):
                path_id    direction  timeslice            capacity
                CNSW-SNW   forward    nsw_peak_demand      0
                CNSW-SNW   forward    nsw_summer_typical   0
                ... etc
    """
    new_keys = sorted(set(flow_path_options.keys()) - existing_path_ids)
    paths = pd.DataFrame(
        [_parse_path_key(k) for k in new_keys],
        columns=["path_id", "geo_from", "geo_to", "carrier"],
    )
    # `capacity` must be explicitly typed so the empty-keys case still produces
    # a float64 column. Without this, an empty `new_keys` yields an object-dtype
    # `capacity`, and concatenating it onto the populated (float64) limits frame
    # downstream both raises a pandas FutureWarning and silently coerces the
    # result column to object. The string columns don't need typing — they're
    # object on both sides regardless.
    limits = pd.DataFrame(
        [
            {"path_id": k, "direction": d, "timeslice": t, "capacity": 0.0}
            for k in new_keys
            for d in _NEW_PATH_DIRECTIONS
            for t in _CANONICAL_TIMESLICES
        ],
        columns=["path_id", "direction", "timeslice", "capacity"],
    ).astype({"capacity": "float64"})
    limits = _add_region_to_timeslices(limits, paths, region_lookup)
    return paths, limits


def _parse_path_key(key: str) -> dict:
    """Parses a flow-path key like 'CNSW-SNW' into topology fields. Splits on the first hyphen."""
    geo_from, geo_to = key.split("-", 1)
    return {"path_id": key, "geo_from": geo_from, "geo_to": geo_to, "carrier": "AC"}
