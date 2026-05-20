import logging

import pandas as pd

from .helpers import (
    _financial_year_string_to_end_year_int,
    _fuzzy_match_names,
    _looks_like_financial_year,
    _snakecase_string,
)

_FLOW_PATH_FORWARD_MW_COL = (
    "Notional transfer level increase (MW) Note: Same increase applies to all transfer "
    "limit conditions (Peak demand, Summer typical and Winter reference)_Forward direction"
)
_FLOW_PATH_REVERSE_MW_COL = _FLOW_PATH_FORWARD_MW_COL.replace(
    "Forward direction", "Reverse direction"
)


# --- Extraction from iasr_tables dict ---


def _extract_flow_path_options_from_iasr(
    iasr_tables: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Discovers flow_path_augmentation_options_<path> tables and keys them by path_id."""
    return _iasr_tables_with_prefix(iasr_tables, "flow_path_augmentation_options_")


def _extract_flow_path_costs_from_iasr(
    iasr_tables: dict[str, pd.DataFrame], scenario: str
) -> dict[str, pd.DataFrame]:
    """Discovers flow_path_augmentation_costs_<scenario>_<path> tables and keys them by path_id."""
    snake = _snakecase_string(scenario)
    return _iasr_tables_with_prefix(
        iasr_tables,
        (
            f"flow_path_augmentation_costs_{snake}_",
            # v7.5 workbook has one mistyped table (singular "cost"); drop the
            # second prefix once Open-ISP/isp-workbook-parser#80 is fixed.
            f"flow_path_augmentation_cost_{snake}_",
        ),
    )


def _extract_rez_options_from_iasr(
    iasr_tables: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Discovers rez_augmentation_options_<state> tables and keys them by state code."""
    return _iasr_tables_with_prefix(iasr_tables, "rez_augmentation_options_")


def _extract_rez_costs_from_iasr(
    iasr_tables: dict[str, pd.DataFrame], scenario: str
) -> dict[str, pd.DataFrame]:
    """Discovers rez_augmentation_costs_<scenario>_<state> tables and keys them by state code."""
    return _iasr_tables_with_prefix(
        iasr_tables, f"rez_augmentation_costs_{_snakecase_string(scenario)}_"
    )


def _iasr_tables_with_prefix(
    iasr_tables: dict[str, pd.DataFrame], prefixes: str | tuple[str, ...]
) -> dict[str, pd.DataFrame]:
    """Returns the subset of iasr_tables whose names start with any of ``prefixes``, stripped of the matching prefix.

    Accepts a single prefix or a tuple of alternative prefixes (used to absorb
    upstream typos — see Open-ISP/isp-workbook-parser#80).

    I/O Example:
        iasr_tables (keys only):
            flow_path_augmentation_options_CQ-NQ
            flow_path_augmentation_options_NNSW-SQ
            rez_augmentation_options_NSW
            renewable_energy_zones

        prefixes: "flow_path_augmentation_options_"

        returns (keys only):
            CQ-NQ
            NNSW-SQ
    """
    if isinstance(prefixes, str):
        prefixes = (prefixes,)
    result = {}
    for name, df in iasr_tables.items():
        for prefix in prefixes:
            if name.startswith(prefix):
                result[name[len(prefix) :]] = df
                break
    return result


# --- Granularity-aware filtering of flow-path augmentations ---


def _filter_flow_path_augmentations_to_granularity(
    augmentations: dict[str, pd.DataFrame],
    regional_granularity: str,
    region_lookup: dict[str, str],
) -> dict[str, pd.DataFrame]:
    """Filters/re-keys flow-path augmentation tables to match the aggregated network paths.

    Flow-path augmentation tables come from IASR keyed by sub-region path IDs. When
    `network_transmission_paths` is aggregated to a coarser granularity, augmentation
    entries for paths that no longer exist would point at non-existent expansion_ids.

    sub_regions: returned unchanged.
    nem_regions: intra-region keys dropped; cross-region keys re-keyed
        (NNSW-SQ → NSW-QLD), preserving any '_suffix'. The "Flow path" column
        inside each DataFrame is also rewritten so it stays aligned with the dict key.
    single_region: returns an empty dict — flow paths don't exist at this granularity.

    I/O Example:
        augmentations:
            "CQ-NQ":   <DataFrame with "Flow path" column = "CQ-NQ">           # intra-QLD
            "NNSW-SQ": <DataFrame with "Flow path" column = "NNSW-SQ">         # NSW <-> QLD

        region_lookup: {"CQ": "QLD", "NQ": "QLD", "NNSW": "NSW", "SQ": "QLD"}

        regional_granularity = "nem_regions" returns:
            "NSW-QLD": <DataFrame with "Flow path" column = "NSW-QLD">

        regional_granularity = "single_region" returns: {}
    """
    if regional_granularity == "sub_regions":
        return augmentations
    if regional_granularity == "single_region":
        return {}
    if regional_granularity == "nem_regions":
        return _aggregate_flow_path_augmentations_to_nem_regions(
            augmentations, region_lookup
        )
    raise ValueError(f"Unknown regional_granularity: {regional_granularity!r}")


def _aggregate_flow_path_augmentations_to_nem_regions(
    augmentations: dict[str, pd.DataFrame],
    region_lookup: dict[str, str],
) -> dict[str, pd.DataFrame]:
    """Drops intra-region augmentation entries and re-keys cross-region ones.

    I/O Example:
        augmentations:
            "CQ-NQ":             <DataFrame with "Flow path" column = "CQ-NQ">             # intra-QLD
            "NNSW-SQ":           <DataFrame with "Flow path" column = "NNSW-SQ">           # NSW <-> QLD
            "NNSW-SQ_Terranora": <DataFrame with "Flow path" column = "NNSW-SQ_Terranora"> # parallel suffix

        region_lookup: {"CQ": "QLD", "NQ": "QLD", "NNSW": "NSW", "SQ": "QLD"}

        returns:
            "NSW-QLD":           <DataFrame with "Flow path" column = "NSW-QLD">           # CQ-NQ dropped (intra-QLD)
            "NSW-QLD_Terranora": <DataFrame with "Flow path" column = "NSW-QLD_Terranora"> # suffix preserved
    """
    result = {}
    for old_key, df in augmentations.items():
        new_key = _rekey_augmentation_path_to_region(old_key, region_lookup)
        if new_key is None:
            continue
        new_df = df.copy()
        new_df["Flow path"] = new_key
        result[new_key] = new_df
    return result


def _rekey_augmentation_path_to_region(
    path_key: str, region_lookup: dict[str, str]
) -> str | None:
    """Aggregates a sub-region augmentation key to its NEM-region key, or None if intra-region.

    Splits the key into base and optional suffix, maps each base endpoint to its
    region via ``region_lookup``, and rebuilds. Returns ``None`` when both endpoints
    sit in the same region (intra-region augmentations don't survive aggregation).

    I/O Example:
        region_lookup: {"NNSW": "NSW", "SQ": "QLD", "CQ": "QLD", "NQ": "QLD",
                        "CNSW": "NSW", "SNW": "NSW"}

        "NNSW-SQ"             -> "NSW-QLD"             # cross-region
        "NNSW-SQ_Terranora"   -> "NSW-QLD_Terranora"  # suffix preserved
        "CQ-NQ"               -> None                  # intra-QLD
        "CNSW-SNW"            -> None                  # intra-NSW
    """
    base, sep, suffix = path_key.partition("_")
    geo_from, geo_to = base.split("-", 1)
    region_from = region_lookup[geo_from]
    region_to = region_lookup[geo_to]
    if region_from == region_to:
        return None
    return f"{region_from}-{region_to}{sep}{suffix}"


# --- Orchestrator ---


def _template_network_expansion(
    flow_path_options: dict[str, pd.DataFrame],
    flow_path_costs: dict[str, pd.DataFrame],
    rez_options: dict[str, pd.DataFrame],
    rez_costs: dict[str, pd.DataFrame],
    network_transmission_paths: pd.DataFrame,
    rez_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process flow path and REZ augmentation options and cost forecasts to find the least
    cost option for each path, return results in `ISPyPSA` format as two tables: the
    selected options split by direction, and a long-form cost trajectory in $/MW.

    Args:
        flow_path_options: dict mapping path_id to pd.DataFrame of flow path
            augmentation options. DataFrames include columns 'Flow path',
            'Option name', and the notional transfer level increase columns for
            forward and reverse directions.
        flow_path_costs: dict mapping path_id to pd.DataFrame of flow path
            augmentation costs for the selected scenario. DataFrames include
            columns 'Flow path', 'Option', and one column per financial year
            (e.g., '2024-25', '2025-26', ...).
        rez_options: dict mapping state code to pd.DataFrame of REZ augmentation
            options. DataFrames include columns 'REZ / constraint ID', 'Option',
            'Additional network capacity (MW)', and 'Additional import capacity (MW)'.
        rez_costs: dict mapping state code to pd.DataFrame of REZ augmentation costs
            for the selected scenario. DataFrames include columns 'REZ / Constraint
            ID', 'Option', and one column per financial year.
        network_transmission_paths: pd.DataFrame templated network paths table with
            columns 'path_id', 'geo_from', etc. Used to discriminate physical paths
            from constraint groups in the output (constraint-group IDs are absent
            from this table and emit a single ``constraint_relaxation`` row).
        rez_ids: set of REZ IDs (from ``renewable_energy_zones["ID"]``). Used to
            filter ``network_transmission_paths`` down to REZ rows when building
            the REZ-ID -> path_id lookup.

    Returns:
        Tuple of (options, costs) pd.DataFrames.

        options: one row per (expansion_id, expansion_type). Physical paths emit
        forward + reverse; constraint groups emit a single constraint_relaxation
        row. Columns:
            - expansion_id
            - expansion_type ('forward', 'reverse', or 'constraint_relaxation')
            - allowed_expansion (MW)
            - expansion_option (name of the selected augmentation option,
              retained for traceability)

        costs: long-form cost trajectory. Columns:
            - expansion_id
            - year (financial-year ending year as int, e.g. 2025 for FY 2024-25)
            - cost ($/MW)

    I/O Example:
        Inputs (abbreviated):

            flow_path_options["CQ-NQ"]:
                Flow path  Option name    Fwd_MW  Rev_MW  Indicative cost  ...
                CQ-NQ      CQ-NQ Option 1 1000    1200    500
                CQ-NQ      CQ-NQ Option 2 1500    1500    1000

            flow_path_costs["CQ-NQ"]:
                Flow path  Option           2024-25      2025-26      ...
                CQ-NQ      CQ-NQ Option 1   500000000    510000000
                CQ-NQ      CQ-NQ Option 2   1200000000   1220000000

            rez_options["NSW"]:
                REZ / constraint ID  Option    Additional_net_MW  Additional_imp_MW
                N1                   Option 1  1660               1660

            rez_costs["NSW"]:
                REZ / Constraint ID  Option    2024-25       2025-26       ...
                N1                   Option 1  5875680000    5964045000

            network_transmission_paths:
                path_id  geo_from  geo_to  carrier
                CQ-NQ    CQ        NQ      AC
                N1-CNSW  N1        CNSW    AC

            rez_ids: {"N1"}   # used to filter network_transmission_paths to REZ-only rows

        Outputs:

            options (network_expansion_options):
                expansion_id  expansion_type  allowed_expansion  expansion_option
                CQ-NQ         forward         1000               CQ-NQ Option 1  # better $/MW than Option 2
                CQ-NQ         reverse         1200               CQ-NQ Option 1
                N1-CNSW       forward         1660               Option 1
                N1-CNSW       reverse         1660               Option 1

            costs (network_transmission_path_expansion_costs), in $/MW:
                expansion_id  year  cost
                CQ-NQ         2025  416666.67              # 500M / 1200
                CQ-NQ         2026  425000.00
                N1-CNSW       2025  3539566.27             # 5.88B / 1660
                N1-CNSW       2026  3593401.81
    """
    rez_paths = network_transmission_paths[
        network_transmission_paths["geo_from"].isin(rez_ids)
    ]
    options = _load_all_options(flow_path_options, rez_options, rez_paths)
    costs = _load_all_costs(flow_path_costs, rez_costs, rez_paths, options)
    selected = _select_least_cost_option_per_expansion(options, costs)
    options_table = _build_options_table(options, selected, network_transmission_paths)
    costs_table = _build_costs_table(costs, options, selected)
    return options_table, costs_table


# --- Options loading ---


def _load_all_options(
    flow_path_options: dict[str, pd.DataFrame],
    rez_options: dict[str, pd.DataFrame],
    rez_paths: pd.DataFrame,
) -> pd.DataFrame:
    """Concats flow-path and REZ augmentation options into one normalised frame."""
    flow = _extract_flow_path_options(flow_path_options)
    rez = _extract_rez_options(rez_options, rez_paths)
    return pd.concat([flow, rez], ignore_index=True)


def _extract_flow_path_options(
    flow_path_options: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Normalises flow-path augmentation options to a standard schema.

    I/O Example:
        flow_path_options["CQ-NQ"]:
            Flow path  Option name     Fwd_MW  Rev_MW
            CQ-NQ      CQ-NQ Option 1  1000    1200
            CQ-NQ      CQ-NQ Option 2  text    text  # non-numeric, dropped

        returns:
            expansion_id  option_name     forward_mw  reverse_mw
            CQ-NQ         CQ-NQ Option 1  1000.0      1200.0
    """
    if not flow_path_options:
        return _empty_options_frame()
    frames = [
        _normalise_flow_path_option_frame(df) for df in flow_path_options.values()
    ]
    return pd.concat(frames, ignore_index=True)


def _normalise_flow_path_option_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns, parses MW values, drops rows with no numeric capacity.

    I/O Example:
        df (real IASR column names abbreviated — see ``_FLOW_PATH_FORWARD_MW_COL``):
            Flow path  Option name     Fwd_MW  Rev_MW
            CQ-NQ      CQ-NQ Option 1  1000    1200
            CQ-NQ      CQ-NQ Option 2  text    text

        returns:
            expansion_id  option_name     forward_mw  reverse_mw
            CQ-NQ         CQ-NQ Option 1  1000.0      1200.0       # Option 2 dropped
    """
    normalised = pd.DataFrame(
        {
            "expansion_id": df["Flow path"],
            "option_name": df["Option name"],
            "forward_mw": _parse_numeric(df[_FLOW_PATH_FORWARD_MW_COL]),
            "reverse_mw": _parse_numeric(df[_FLOW_PATH_REVERSE_MW_COL]),
        }
    )
    return _drop_options_with_no_capacity(normalised)


def _extract_rez_options(
    rez_options: dict[str, pd.DataFrame],
    rez_paths: pd.DataFrame,
) -> pd.DataFrame:
    """Normalises REZ augmentation options and maps REZ IDs to their path_id.

    I/O Example:
        rez_options["NSW"]:
            REZ / constraint ID  Option    Additional_net_MW  Additional_imp_MW
            N1                   Option 1  1660               1660
            SWNSW1               Option 1  "Removes limit"    ""  # dropped

        rez_paths:
            path_id   geo_from
            N1-CNSW   N1

        returns:
            expansion_id  option_name  forward_mw  reverse_mw
            N1-CNSW       Option 1     1660.0      1660.0
    """
    if not rez_options:
        return _empty_options_frame()
    geo_from_to_path_id = _build_geo_from_to_path_id_map(rez_paths)
    frames = [
        _normalise_rez_option_frame(df, geo_from_to_path_id)
        for df in rez_options.values()
    ]
    return pd.concat(frames, ignore_index=True)


def _normalise_rez_option_frame(
    df: pd.DataFrame, geo_from_to_path_id: dict[str, str]
) -> pd.DataFrame:
    """Renames columns, maps REZ ID to expansion_id, parses MW values, drops no-capacity rows.

    I/O Example:
        df:
            REZ / constraint ID  Option    Additional_net_MW  Additional_imp_MW
            N1                   Option 1  1660               1660
            SWQLD1               Option 1  150                NaN              # constraint group
            SWNSW1               Option 1  "Removes limit"    NaN              # dropped

        geo_from_to_path_id: {"N1": "N1-NNSW"}

        returns:
            expansion_id  option_name  forward_mw  reverse_mw
            N1-NNSW       Option 1     1660.0      1660.0
            SWQLD1        Option 1     150.0       NaN               # passed through unchanged
    """
    normalised = pd.DataFrame(
        {
            "expansion_id": _map_rez_id_to_expansion_id(
                df["REZ / constraint ID"], geo_from_to_path_id
            ),
            "option_name": df["Option"],
            "forward_mw": _parse_numeric(df["Additional network capacity (MW)"]),
            "reverse_mw": _parse_numeric(df["Additional import capacity (MW)"]),
        }
    )
    normalised = normalised.dropna(subset=["expansion_id"])
    return _drop_options_with_no_capacity(normalised)


def _map_rez_id_to_expansion_id(
    rez_id: pd.Series, geo_from_to_path_id: dict[str, str]
) -> pd.Series:
    """Maps REZ IDs to their path_id (which is their expansion_id); constraint group
    IDs (e.g. SWQLD1, SEVIC1) aren't in the map and pass through unchanged. NaN stays
    NaN so the caller can drop it.

    I/O Example:
        rez_id:
            N1
            SWQLD1
            NaN

        geo_from_to_path_id: {"N1": "N1-NNSW", "Q1": "Q1-NQ"}

        returns:
            N1-NNSW    # REZ mapped to its path_id
            SWQLD1     # constraint group passes through
            None       # NaN → None for caller to drop
    """

    def convert(r):
        if pd.isna(r):
            return None
        return geo_from_to_path_id.get(r, r)

    return rez_id.map(convert)


def _build_geo_from_to_path_id_map(
    rez_paths: pd.DataFrame,
) -> dict[str, str]:
    """Maps each REZ ID (geo_from) to its path_id. Expects REZ-only rows so that
    each geo_from appears once.

    I/O Example:
        rez_paths:
            path_id   geo_from  geo_to  carrier
            Q1-NQ     Q1        NQ      AC
            N1-NNSW   N1        NNSW    AC

        returns:
            {"Q1": "Q1-NQ", "N1": "N1-NNSW"}
    """
    return dict(zip(rez_paths["geo_from"], rez_paths["path_id"]))


def _drop_options_with_no_capacity(options: pd.DataFrame) -> pd.DataFrame:
    """Drops rows where both forward and reverse capacity are non-numeric, with logging.

    I/O Example:
        options:
            expansion_id  option_name  forward_mw  reverse_mw
            CQ-NQ         Option 1     1000.0      1200.0
            CQ-NQ         Option 2     NaN         NaN          # dropped, log emitted
            SWQLD1        Option 1     150.0       NaN          # kept (forward is numeric)

        returns:
            expansion_id  option_name  forward_mw  reverse_mw
            CQ-NQ         Option 1     1000.0      1200.0
            SWQLD1        Option 1     150.0       NaN
    """
    no_capacity = options["forward_mw"].isna() & options["reverse_mw"].isna()
    if no_capacity.any():
        dropped = options.loc[no_capacity, ["expansion_id", "option_name"]]
        for _, row in dropped.iterrows():
            logging.info(
                f"Skipping option '{row['option_name']}' for '{row['expansion_id']}': "
                "no numeric capacity in forward or reverse direction."
            )
    return options.loc[~no_capacity].reset_index(drop=True)


# --- Costs loading ---


def _load_all_costs(
    flow_path_costs: dict[str, pd.DataFrame],
    rez_costs: dict[str, pd.DataFrame],
    rez_paths: pd.DataFrame,
    options: pd.DataFrame,
) -> pd.DataFrame:
    """Concats flow-path and REZ cost tables and aligns option_names to ``options``."""
    flow = _extract_flow_path_costs(flow_path_costs)
    rez = _extract_rez_costs(rez_costs, rez_paths)
    costs = pd.concat([flow, rez], ignore_index=True)
    return _align_option_names_to_options(costs, options)


def _extract_flow_path_costs(
    flow_path_costs: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Melts flow-path cost tables from wide (year columns) to long format.

    I/O Example:
        flow_path_costs["CQ-NQ"]:
            Flow path  Option           2024-25      2025-26
            CQ-NQ      CQ-NQ Option 1   500000000    510000000

        returns:
            expansion_id  option_name     year  cost
            CQ-NQ         CQ-NQ Option 1  2025  500000000
            CQ-NQ         CQ-NQ Option 1  2026  510000000
    """
    if not flow_path_costs:
        return _empty_costs_frame()
    frames = [
        _normalise_cost_frame(df, id_col="Flow path", option_col="Option")
        for df in flow_path_costs.values()
    ]
    return pd.concat(frames, ignore_index=True)


def _extract_rez_costs(
    rez_costs: dict[str, pd.DataFrame],
    rez_paths: pd.DataFrame,
) -> pd.DataFrame:
    """Melts REZ cost tables and maps REZ ID (or constraint group ID) to expansion_id.

    I/O Example:
        rez_costs["NSW"]:
            REZ / Constraint ID  Option    2024-25     2025-26
            N1                   Option 1  5875000000  5964000000
            SWQLD1               Option 1  500000      505000

        rez_paths:
            path_id   geo_from
            N1-NNSW   N1

        returns:
            expansion_id  option_name  year  cost
            N1-NNSW       Option 1     2025  5875000000
            N1-NNSW       Option 1     2026  5964000000
            SWQLD1        Option 1     2025  500000       # constraint group passes through
            SWQLD1        Option 1     2026  505000
    """
    if not rez_costs:
        return _empty_costs_frame()
    geo_from_to_path_id = _build_geo_from_to_path_id_map(rez_paths)
    # IASR uses "REZ / Constraint ID" (capital C) in cost tables but "REZ / constraint ID"
    # (lowercase c) in the options tables — preserved verbatim from the source workbook.
    frames = [
        _normalise_cost_frame(df, id_col="REZ / Constraint ID", option_col="Option")
        for df in rez_costs.values()
    ]
    long = pd.concat(frames, ignore_index=True)
    long["expansion_id"] = _map_rez_id_to_expansion_id(
        long["expansion_id"], geo_from_to_path_id
    )
    return long.reset_index(drop=True)


def _normalise_cost_frame(
    df: pd.DataFrame, id_col: str, option_col: str
) -> pd.DataFrame:
    """Melts a wide cost frame to (expansion_id, option_name, year, cost), dropping non-numeric costs.

    Year column names (e.g. '2024-25') are converted to the financial-year ending
    year as an int (2025) — see :func:`_financial_year_string_to_end_year_int`.

    I/O Example:
        df (with id_col="Flow path", option_col="Option"):
            Flow path  Option    Status  2024-25      2025-26
            CQ-NQ      Option 1          500000000    510000000
            CQ-NQ      Option 2          "N/A"        1220000000

        returns:
            expansion_id  option_name  year  cost
            CQ-NQ         Option 1     2025  500000000
            CQ-NQ         Option 1     2026  510000000
            CQ-NQ         Option 2     2026  1220000000   # 2025 dropped (non-numeric)
    """
    year_cols = [c for c in df.columns if _looks_like_financial_year(c)]
    long = df[[id_col, option_col] + year_cols].melt(
        id_vars=[id_col, option_col], var_name="year", value_name="cost"
    )
    long = long.rename(columns={id_col: "expansion_id", option_col: "option_name"})
    long["year"] = long["year"].map(_financial_year_string_to_end_year_int)
    long["cost"] = pd.to_numeric(long["cost"], errors="coerce")
    return long.dropna(subset=["cost"]).reset_index(drop=True)


# --- Selection ---


def _select_least_cost_option_per_expansion(
    options: pd.DataFrame, costs: pd.DataFrame
) -> pd.DataFrame:
    """Selects the lowest $/MW option per expansion_id using the first year with complete costs.

    I/O Example:
        options:
            expansion_id  option_name     forward_mw  reverse_mw
            CQ-NQ         CQ-NQ Option 1  1000        1200       # $/MW = 500M/1200 = 416,667
            CQ-NQ         CQ-NQ Option 2  1500        1500       # $/MW = 1.2B/1500 = 800,000

        costs (first year with complete costs = 2025):
            expansion_id  option_name     year  cost
            CQ-NQ         CQ-NQ Option 1  2025  500000000
            CQ-NQ         CQ-NQ Option 2  2025  1200000000

        returns:
            expansion_id  option_name
            CQ-NQ         CQ-NQ Option 1
    """
    anchor_costs = _first_year_with_complete_costs_per_expansion(costs)
    scored = _score_options(options, anchor_costs)
    return _pick_min_per_expansion(scored)


def _align_option_names_to_options(
    costs: pd.DataFrame, options: pd.DataFrame
) -> pd.DataFrame:
    """Fuzzy-matches each expansion's cost option_names to its option_names in the options frame.

    Bridges systematic mismatches between the two source tables (em-dash vs hyphen,
    path prefix variations, optional ``(Project Marinus …)``-style annotations).

    I/O Example:
        options:
            expansion_id  option_name
            NNSW-SQ       NNSW-SQ Option 1
            TAS-SEV       TAS-SEV Option 1 (Project Marinus Stage 1)

        costs:
            expansion_id  option_name       year  cost
            NNSW-SQ       NNSW–SQ Option 1  2025  500000000      # em-dash
            TAS-SEV       TAS-SEV Option 1  2025  3750000000     # no annotation

        returns:
            expansion_id  option_name                                year  cost
            NNSW-SQ       NNSW-SQ Option 1                           2025  500000000
            TAS-SEV       TAS-SEV Option 1 (Project Marinus Stage 1) 2025  3750000000
    """
    aligned = []
    for expansion_id, cost_group in costs.groupby("expansion_id"):
        option_names = options.loc[
            options["expansion_id"] == expansion_id, "option_name"
        ].unique()
        if len(option_names) == 0:
            logging.warning(
                f"No options for expansion_id '{expansion_id}'; "
                f"dropping {len(cost_group)} orphaned cost row(s). "
                "Expected when all options for the expansion_id were dropped for "
                "non-numeric capacity; otherwise indicates the upstream options "
                "table is missing for this id."
            )
            continue
        cost_group = cost_group.copy()
        cost_group["option_name"] = _fuzzy_match_names(
            cost_group["option_name"],
            option_names,
            task_desc=f"matching cost option_names to options for {expansion_id}",
            threshold=60,
        )
        aligned.append(cost_group)
    return pd.concat(aligned, ignore_index=True) if aligned else costs.iloc[0:0]


def _first_year_with_complete_costs_per_expansion(costs: pd.DataFrame) -> pd.DataFrame:
    """For each expansion_id, returns costs from the earliest year where all options have a cost.

    I/O Example:
        costs:
            expansion_id  option_name  year  cost
            CQ-NQ         Option 1     2025  500000000
            CQ-NQ         Option 1     2026  510000000
            CQ-NQ         Option 2     2026  1220000000    # Option 2 missing 2025

        returns (2026 is the first year with complete costs for CQ-NQ):
            expansion_id  option_name  year  cost
            CQ-NQ         Option 1     2026  510000000
            CQ-NQ         Option 2     2026  1220000000
    """
    selected = []
    for expansion_id, group in costs.groupby("expansion_id"):
        year = _earliest_complete_year(group)
        if year is None:
            logging.warning(
                f"No year has costs for all options of expansion '{expansion_id}'; "
                "dropping the expansion. Likely indicates gaps in the upstream cost table."
            )
            continue
        selected.append(group.loc[group["year"] == year])
    return pd.concat(selected, ignore_index=True) if selected else costs.iloc[0:0]


def _earliest_complete_year(path_costs: pd.DataFrame) -> int | None:
    """Finds the earliest year where every option in this path has a cost, else None.

    I/O Example:
        path_costs:
            option_name  year  cost
            Option 1     2025  500000000
            Option 1     2026  510000000
            Option 2     2026  1220000000

        returns: 2026      # 2025 missing Option 2 cost
    """
    option_count = path_costs["option_name"].nunique()
    per_year = path_costs.groupby("year")["option_name"].nunique()
    complete_years = per_year[per_year == option_count].index
    return min(complete_years) if len(complete_years) else None


def _score_options(options: pd.DataFrame, anchor_costs: pd.DataFrame) -> pd.DataFrame:
    """Joins options to their anchor-year cost and computes $/MW using max directional MW.

    I/O Example:
        options:
            expansion_id  option_name  forward_mw  reverse_mw
            CQ-NQ         Option 1     1000        1200
            CQ-NQ         Option 2     1500        1500

        anchor_costs:
            expansion_id  option_name  cost
            CQ-NQ         Option 1     500000000
            CQ-NQ         Option 2     1200000000

        returns (max_mw = max(forward, reverse); cost_per_mw = cost / max_mw):
            expansion_id  option_name  forward_mw  reverse_mw  cost         max_mw  cost_per_mw
            CQ-NQ         Option 1     1000        1200        500000000    1200    416666.67
            CQ-NQ         Option 2     1500        1500        1200000000   1500    800000.00
    """
    merged = options.merge(
        anchor_costs[["expansion_id", "option_name", "cost"]],
        on=["expansion_id", "option_name"],
        how="inner",
    )
    merged["max_mw"] = merged[["forward_mw", "reverse_mw"]].max(axis=1)
    merged["cost_per_mw"] = merged["cost"] / merged["max_mw"]
    return merged


def _pick_min_per_expansion(scored: pd.DataFrame) -> pd.DataFrame:
    """Returns the (expansion_id, option_name) of the minimum $/MW option per expansion.

    I/O Example:
        scored:
            expansion_id  option_name  cost_per_mw
            CQ-NQ         Option 1     416666.67     # winner
            CQ-NQ         Option 2     800000.00
            N1-NNSW       Option 1     3539566.27    # winner (only option)

        returns:
            expansion_id  option_name
            CQ-NQ         Option 1
            N1-NNSW       Option 1
    """
    winners = scored.loc[scored.groupby("expansion_id")["cost_per_mw"].idxmin()]
    return winners[["expansion_id", "option_name"]].reset_index(drop=True)


# --- Output shaping ---


def _build_options_table(
    options: pd.DataFrame,
    selected: pd.DataFrame,
    network_transmission_paths: pd.DataFrame,
) -> pd.DataFrame:
    """Filters to selected options and emits (expansion_id, expansion_type, allowed_expansion, expansion_option).

    Physical paths emit two rows (``forward`` + ``reverse``); constraint groups
    (expansion_ids absent from ``network_transmission_paths``) emit one
    ``constraint_relaxation`` row.

    I/O Example:
        options:
            expansion_id  option_name  forward_mw  reverse_mw
            CQ-NQ         Option 1     1000        1200
            CQ-NQ         Option 2     1500        1500           # not selected
            SWQLD1        Option 1     330         NaN            # constraint group

        selected:
            expansion_id  option_name
            CQ-NQ         Option 1
            SWQLD1        Option 1

        network_transmission_paths:
            path_id  geo_from  geo_to  carrier
            CQ-NQ    CQ        NQ      AC

        returns:
            expansion_id  expansion_type          allowed_expansion  expansion_option
            CQ-NQ         forward                 1000               Option 1
            CQ-NQ         reverse                 1200               Option 1
            SWQLD1        constraint_relaxation   330                Option 1
    """
    selected_options = options.merge(
        selected, on=["expansion_id", "option_name"], how="inner"
    )
    path_ids = set(network_transmission_paths["path_id"])
    is_path = selected_options["expansion_id"].isin(path_ids)
    path_rows = _melt_path_options_to_directions(selected_options[is_path])
    constraint_rows = _build_constraint_relaxation_rows(selected_options[~is_path])
    combined = pd.concat([path_rows, constraint_rows], ignore_index=True)
    return (
        combined[
            ["expansion_id", "expansion_type", "allowed_expansion", "expansion_option"]
        ]
        .sort_values(["expansion_id", "expansion_type"])
        .reset_index(drop=True)
    )


def _melt_path_options_to_directions(options: pd.DataFrame) -> pd.DataFrame:
    """For physical paths, melts forward/reverse capacity columns into expansion_type rows.

    Blank source values are emitted as 0 — for paths, a missing direction
    means "this option provides no expansion in that direction".

    I/O Example:
        options:
            expansion_id  option_name  forward_mw  reverse_mw
            CQ-NQ         Option 1     1000        1200
            DN1-CNSW      Option 2a    500         NaN          # no reverse expansion in source

        returns:
            expansion_id  expansion_option  expansion_type  allowed_expansion
            CQ-NQ         Option 1          forward         1000
            CQ-NQ         Option 1          reverse         1200
            DN1-CNSW      Option 2a         forward         500
            DN1-CNSW      Option 2a         reverse         0          # NaN -> 0
    """
    long = options.melt(
        id_vars=["expansion_id", "option_name"],
        value_vars=["forward_mw", "reverse_mw"],
        var_name="expansion_type",
        value_name="allowed_expansion",
    )
    long["expansion_type"] = long["expansion_type"].str.replace("_mw", "", regex=False)
    long["allowed_expansion"] = long["allowed_expansion"].fillna(0)
    return long.rename(columns={"option_name": "expansion_option"})


def _build_constraint_relaxation_rows(options: pd.DataFrame) -> pd.DataFrame:
    """For constraint groups, emits one row per expansion_id using whichever direction has capacity.

    I/O Example:
        options:
            expansion_id  option_name  forward_mw  reverse_mw
            SWQLD1        Option 1     330         NaN

        returns:
            expansion_id  expansion_type         allowed_expansion  expansion_option
            SWQLD1        constraint_relaxation  330                Option 1
    """
    return pd.DataFrame(
        {
            "expansion_id": options["expansion_id"].values,
            "expansion_type": "constraint_relaxation",
            "allowed_expansion": options["forward_mw"]
            .combine_first(options["reverse_mw"])
            .values,
            "expansion_option": options["option_name"].values,
        }
    )


def _build_costs_table(
    costs: pd.DataFrame, options: pd.DataFrame, selected: pd.DataFrame
) -> pd.DataFrame:
    """Filters costs to selected options and divides by max directional MW to get $/MW.

    I/O Example:
        costs:
            expansion_id  option_name  year  cost
            CQ-NQ         Option 1     2025  500000000
            CQ-NQ         Option 1     2026  510000000
            CQ-NQ         Option 2     2025  1200000000     # not selected

        options:
            expansion_id  option_name  forward_mw  reverse_mw
            CQ-NQ         Option 1     1000        1200
            CQ-NQ         Option 2     1500        1500

        selected:
            expansion_id  option_name
            CQ-NQ         Option 1

        returns (cost is divided by max_mw = max(1000, 1200) = 1200):
            expansion_id  year  cost
            CQ-NQ         2025  416666.67
            CQ-NQ         2026  425000.00
    """
    selected_costs = costs.merge(
        selected, on=["expansion_id", "option_name"], how="inner"
    )
    selected_with_mw = selected_costs.merge(
        options[["expansion_id", "option_name", "forward_mw", "reverse_mw"]],
        on=["expansion_id", "option_name"],
        how="inner",
    )
    selected_with_mw["max_mw"] = selected_with_mw[["forward_mw", "reverse_mw"]].max(
        axis=1
    )
    selected_with_mw["cost"] = selected_with_mw["cost"] / selected_with_mw["max_mw"]
    return (
        selected_with_mw[["expansion_id", "year", "cost"]]
        .sort_values(["expansion_id", "year"])
        .reset_index(drop=True)
    )


# --- Utilities ---


def _parse_numeric(series: pd.Series) -> pd.Series:
    """Parses a series to floats; non-numeric becomes NaN. Handles comma-thousands."""
    cleaned = series.astype(str).str.replace(",", "", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")


def _empty_options_frame() -> pd.DataFrame:
    """Schema-shaped empty DataFrame for the internal options representation."""
    return pd.DataFrame(
        {
            "expansion_id": pd.Series(dtype="object"),
            "option_name": pd.Series(dtype="object"),
            "forward_mw": pd.Series(dtype="float64"),
            "reverse_mw": pd.Series(dtype="float64"),
        }
    )


def _empty_costs_frame() -> pd.DataFrame:
    """Schema-shaped empty DataFrame for the internal costs representation."""
    return pd.DataFrame(
        {
            "expansion_id": pd.Series(dtype="object"),
            "option_name": pd.Series(dtype="object"),
            "year": pd.Series(dtype="int64"),
            "cost": pd.Series(dtype="float64"),
        }
    )
