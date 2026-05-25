# File holds functions for templating CONNECTION AND BUILD COST tables that hold
# data with time-varying (spanning multiple years) values. These tables contain
# data that applies to both generation and storage (new entrant).
import logging
import re

import numpy as np
import pandas as pd

from ispypsa.templater.helpers import (
    _financial_year_string_to_end_year_int,
    _fuzzy_map_to_canonical,
    _where_any_substring_appears,
)
from ispypsa.templater.lists import _ISP_SCENARIOS_NEW

_VRE_COLUMN_RENAMES = {
    "REZ ID": "geo_id",
    "Connection capacity (MVA)": "connection_capacity",
}

_VRE_TECHNOLOGY_STRINGS = ["solar", "wind"]
_IBR_TECHNOLOGY_STRINGS = _VRE_TECHNOLOGY_STRINGS + ["battery", "batteries"]


def _template_connection_costs(
    connection_cost_forecast_vre: pd.DataFrame,
    connection_costs_for_vre: pd.DataFrame,
    system_strength_cost_table: pd.DataFrame,
    scenario: str,
    generators_new_entrant: pd.DataFrame,
) -> pd.DataFrame:
    """Combines VRE and non-VRE connection costs with system strength costs.

    Produces a long-format table with columns (geo_id, technology, year,
    connection_cost, system_strength_cost) from three IASR source tables.
    VRE (wind/solar) costs are templated and system strength costs are merged
    in to return a single ISPyPSA format table: ``costs_connection``.

    Note: non-VRE connection costs will be added in a subsequent PR.

    Args:
        connection_cost_forecast_vre: IASR ``connection_cost_forecast_wind_and_solar``
            table. Columns: 'REZ ID', 'Scenario', one column per financial year.
        connection_costs_for_vre: IASR ``connection_costs_for_wind_and_solar`` table.
            Columns: 'REZ ID', 'Connection capacity (MVA)'.
        system_strength_cost_table: IASR ``efficient_level_of_system_strength_cost``
            table. Single row with one column per financial year (cost in $/kW).
        scenario: ISP scenario name, e.g. "Step Change".
        generators_new_entrant: templated new entrant generators table.
            Columns used: 'geo_id' and 'technology'.

    Returns:
        One row per (geo_id, technology, year). connection_cost and
        system_strength_cost are in $/MW; either may be NaN where no cost
        applies for that combination.

    I/O Example:
        Inputs (abbreviated):

            connection_cost_forecast_vre:
                REZ ID  REZ names       Scenario    Notes   2024-25     2025-26
                N1      North West NSW  Step Change         73000000    74000000
                Q9      Banana          Step Change         854000000   867000000

            connection_costs_for_vre:
                REZ ID  REZ names       Connection capacity (MVA)
                N1      North West NSW  400
                Q9      Banana          1800

            system_strength_cost_table:
                                        2024-25  2025-26
                IBR remediation $/kW    163.24   148.88

            scenario: "Step Change"

            generators_new_entrant:
                name                    technology              geo_id
                N1_WH_North West NSW    Wind                    N1
                N1_SAT_North West NSW   Large scale Solar PV    N1
                Q9_WH_Banana            Wind                    Q9
                Q9_SAT_Banana           Large scale Solar PV    Q9

        Returns:
            geo_id  technology              year  connection_cost  system_strength_cost
            N1      Wind                    2025  182500.00        163240.00  # VRE IBR: 163.24 $/kW * 1000
            N1      Wind                    2026  185000.00        148880.00
            N1      Large scale Solar PV    2025  182500.00        163240.00  # VRE IBR
            N1      Large scale Solar PV    2026  185000.00        148880.00
            Q9      Wind                    2025  474444.44        163240.00
            Q9      Wind                    2026  481666.67        148880.00
            Q9      Large scale Solar PV    2025  474444.44        163240.00
            Q9      Large scale Solar PV    2026  481666.67        148880.00
    """

    system_strength_costs = _normalise_system_strength_cost_frame(
        system_strength_cost_table
    )

    vre_technologies_by_geo_id = _get_unique_vre_geo_id_rows(generators_new_entrant)
    vre_connection_costs = _template_vre_connection_costs(
        connection_cost_forecast_vre,
        connection_costs_for_vre,
        scenario,
        vre_technologies_by_geo_id,
    )

    # note: I started writing up non-VRE connection cost pipeline but there's some
    # extra complexity there with batteries in REZs/BOTN being fussy/canonicalisation
    # SO to keep this PR scope tight I've shifted that to a separate PR (next up).
    non_vre_connection_costs = pd.DataFrame(
        {
            "geo_id": pd.Series(dtype="object"),
            "technology": pd.Series(dtype="object"),
            "year": pd.Series(dtype="int64"),
            "connection_cost": pd.Series(dtype="float64"),
        }
    )

    combined_connection_costs = pd.concat(
        [vre_connection_costs, non_vre_connection_costs], axis=0, ignore_index=True
    )

    connection_costs = _merge_and_filter_system_strength_costs(
        combined_connection_costs, system_strength_costs
    )
    return connection_costs


# --- VRE connection costs ---


def _template_vre_connection_costs(
    connection_cost_forecast_vre: pd.DataFrame,
    connection_costs_for_vre: pd.DataFrame,
    scenario: str,
    vre_technologies_by_geo_id: pd.DataFrame,
) -> pd.DataFrame:
    """Templates connection costs for VRE (wind and solar) new entrant technologies.

    Filters the cost forecast to the given scenario, merges in connection capacity,
    normalises to long format and $/MW units, then expands to all VRE
    technology/geo_id pairs from the new entrant generators table. REZs absent
    from the cost forecast (e.g. offshore wind) are included with NaN
    connection_cost — system strength costs are still applied to them downstream.
    NaN connection costs for REZs that ARE in the forecast are logged as a WARNING.

    I/O Example:
        Inputs:

            connection_cost_forecast_vre:
                REZ ID  REZ names       Scenario        2024-25     2025-26
                N1      North West NSW  Step Change     73000000    74000000
                Q9      Banana          Step Change     854000000   867000000
                # V8 absent: offshore REZs have no connection cost forecast

            connection_costs_for_vre:
                REZ ID  REZ names       Connection capacity (MVA)
                N1      North West NSW  400
                Q9      Banana          1800

            scenario: "Step Change"

            vre_technologies_by_geo_id:
                geo_id  technology
                N1      Wind
                N1      Large scale Solar PV
                Q9      Large scale Solar PV
                V8      Wind - offshore (fixed)     # included despite no cost forecast

        returns:
            geo_id  technology              year  connection_cost
            N1      Wind                    2025  182500.00    # 73000000 / 400
            N1      Wind                    2026  185000.00    # 74000000 / 400
            N1      Large scale Solar PV    2025  182500.00
            N1      Large scale Solar PV    2026  185000.00
            Q9      Large scale Solar PV    2025  474444.44    # 854000000 / 1800
            Q9      Large scale Solar PV    2026  481666.67    # 867000000 / 1800
            V8      Wind - offshore (fixed) 2025  NaN          # no forecast: expected
            V8      Wind - offshore (fixed) 2026  NaN          # system strength applied downstream
    """
    scenario_cost_forecast = _filter_table_by_isp_scenario(
        connection_cost_forecast_vre,
        scenario,
        "Scenario",
        "VRE connection cost forecast",
    )
    merged_df = _merge_connection_cost_and_capacity_frames(
        scenario_cost_forecast, connection_costs_for_vre, ["REZ ID"]
    )
    normalised_df = _normalise_connection_cost_forecast_frame(
        merged_df, id_cols_rename=_VRE_COLUMN_RENAMES
    )
    costs_per_mw = _calculate_connection_cost_per_mw(normalised_df)
    _warn_nan_connection_costs(costs_per_mw, id_cols=["geo_id"])
    return _build_vre_cost_rows(costs_per_mw, vre_technologies_by_geo_id)


def _get_unique_vre_geo_id_rows(generators_new_entrant: pd.DataFrame) -> pd.DataFrame:
    """Extracts unique (geo_id, technology) rows for non-distributed VRE technologies
    from the templated new entrant generators table.

    Returns only rows where technology contains 'solar' or 'wind' (see ``_VRE_TECHNOLOGY_STRINGS``),
    but not 'distributed', with duplicate (geo_id, technology) pairs removed.

    I/O Example:
        generators_new_entrant:
            geo_id  technology
            N1      Large scale Solar PV
            N1      Large scale Solar PV            # duplicate: dropped
            N1      Wind
            NNSW    OCGT                            # non-VRE: excluded
            NNSW    Distributed Resources Solar     # 'distributed': excluded
            Q9      Large scale Solar PV
            V8      Wind - offshore (fixed)

        returns:
            geo_id  technology
            N1      Large scale Solar PV
            N1      Wind
            Q9      Large scale Solar PV
            V8      Wind - offshore (fixed)
    """
    vre_technologies = _where_any_substring_appears(
        generators_new_entrant["technology"], _VRE_TECHNOLOGY_STRINGS
    )
    distributed_resources = _where_any_substring_appears(
        generators_new_entrant["technology"], ["distributed"]
    )
    rows_to_keep = vre_technologies & ~distributed_resources
    return (
        generators_new_entrant.loc[rows_to_keep, ["geo_id", "technology"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )


def _build_vre_cost_rows(
    costs_per_mw: pd.DataFrame,
    vre_technologies: pd.DataFrame,
) -> pd.DataFrame:
    """Creates one row per (geo_id, technology, year) for all VRE technologies.

    Cross-joins ``vre_technologies`` with the set of years from ``costs_per_mw``,
    then left-joins connection costs. REZs absent from the cost forecast (e.g.
    offshore wind) receive NaN connection_cost for all years — this is expected,
    and system strength costs are still applied to them downstream.

    REZ IDs in ``costs_per_mw`` that have no matching VRE technology entry are
    dropped.

    I/O Example:
        costs_per_mw:
            geo_id  year  connection_cost
            N1      2025  182500.0
            N1      2026  185000.0
            Q9      2025  474444.4
            Q9      2026  481666.7
            # V8 absent: no cost forecast defined for offshore REZs

        vre_technologies:
            geo_id  technology
            N1      Wind
            N1      Large scale Solar PV
            Q9      Large scale Solar PV
            V8      Wind - offshore (fixed)     # included despite no cost forecast

        returns:
            geo_id  technology              year  connection_cost
            N1      Wind                    2025  182500.0
            N1      Wind                    2026  185000.0
            N1      Large scale Solar PV    2025  182500.0
            N1      Large scale Solar PV    2026  185000.0
            Q9      Large scale Solar PV    2025  474444.4
            Q9      Large scale Solar PV    2026  481666.7
            V8      Wind - offshore (fixed) 2025  NaN      # no forecast: NaN preserved
            V8      Wind - offshore (fixed) 2026  NaN
    """
    unique_years = pd.DataFrame({"year": costs_per_mw["year"].unique()})
    all_combinations = vre_technologies.merge(unique_years, how="cross")
    return all_combinations.merge(
        costs_per_mw[["geo_id", "year", "connection_cost"]],
        how="left",
        on=["geo_id", "year"],
    )


# --- system strength costs ---


def _merge_and_filter_system_strength_costs(
    connection_costs: pd.DataFrame,
    system_strength_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Left-merges system strength costs into the combined connection costs table,
    then zeros out system strength costs for non-IBR technologies.

    System strength costs apply only to IBR technologies (wind, solar, battery).
    Solar thermal contains "solar" so matches ``_IBR_TECHNOLOGY_STRINGS`` but does
    not incur a system strength cost, so it is zeroed separately.

    I/O Example:
        connection_costs:
            geo_id  technology    year  connection_cost
            N1      Solar         2025  1825000.0
            N1      Wind          2025  1825000.0
            QLD     Small OCGT    2025  500000.0

        system_strength_costs:
            year  system_strength_cost
            2025  10000.0

        returns:
            geo_id  technology    year  connection_cost  system_strength_cost
            N1      Solar         2025  1825000.0        10000.0
            N1      Wind          2025  1825000.0        10000.0
            QLD     Small OCGT    2025  500000.0         0.0
    """
    costs = connection_costs.merge(system_strength_costs, how="left", on=["year"])
    costs = _set_non_ibr_system_strength_cost_to_zero(costs)
    return _set_solar_thermal_system_strength_costs_to_zero(costs)


def _normalise_system_strength_cost_frame(
    system_strength_cost_table: pd.DataFrame,
) -> pd.DataFrame:
    """Reshapes the wide system strength cost table to long format and converts
    units from $/kW to $/MW.

    Year column names (e.g. '2024-25') are converted to the financial-year ending
    year as an int (2025) — see :func:`_financial_year_string_to_end_year_int`.
    Any non-year columns (columns not containing cost data) are dropped.

    I/O Example:
        system_strength_cost_table:
                                    2025-26     2026-27
            IBR remediation $/kW    10          12

        returns:
            year  system_strength_cost
            2025  10000.0    # 10 $/kW * 1000 = 10000 $/MW
            2026  12000.0
    """
    year_cols = [
        c for c in system_strength_cost_table.columns if _looks_like_financial_year(c)
    ]
    long_system_strength_costs = system_strength_cost_table.melt(
        value_vars=year_cols, value_name="system_strength_cost", var_name="year"
    )
    long_system_strength_costs["system_strength_cost"] = pd.to_numeric(
        long_system_strength_costs["system_strength_cost"], errors="coerce"
    )
    long_system_strength_costs["system_strength_cost"] *= 1000  # convert to $/MW
    long_system_strength_costs["year"] = long_system_strength_costs["year"].map(
        _financial_year_string_to_end_year_int
    )
    return long_system_strength_costs[["year", "system_strength_cost"]]


def _set_non_ibr_system_strength_cost_to_zero(
    connection_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Sets system_strength_cost to 0.0 for non-IBR technologies.

    IBR technologies are those whose name contains 'solar', 'wind', 'battery',
    or 'batteries' (see ``_IBR_TECHNOLOGY_STRINGS``). All others are zeroed.
    """
    included_technologies = _where_any_substring_appears(
        connection_costs["technology"], _IBR_TECHNOLOGY_STRINGS
    )
    connection_costs.loc[~included_technologies, "system_strength_cost"] = 0.0
    return connection_costs


def _set_solar_thermal_system_strength_costs_to_zero(
    connection_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Sets system_strength_cost to 0.0 for solar thermal technologies.

    Solar thermal matches 'solar' in ``_IBR_TECHNOLOGY_STRINGS`` but is not
    an inverter-based resource, so its system strength cost is zeroed separately
    after the IBR filter.
    """
    solar_thermal_rows = _where_any_substring_appears(
        connection_costs["technology"], ["solar thermal"]
    )
    connection_costs.loc[solar_thermal_rows, "system_strength_cost"] = 0.0
    return connection_costs


# --- shared helpers ---


def _normalise_connection_cost_forecast_frame(
    connection_cost_forecast: pd.DataFrame, id_cols_rename: dict[str, str]
) -> pd.DataFrame:
    """Reshapes wide multi-year cost frame to long format.

    Year column names (e.g. '2024-25') are converted to the financial-year ending
    year as an int (2025) — see :func:`_financial_year_string_to_end_year_int`.

    Empty/null/NaN values are retained.

    I/O Example:
        connection_cost_forecast (VRE example):
            REZ ID  REZ names       Connection capacity (MVA)   2024-25     2025-26
            N1      North West NSW  400                         730000000   740000000
            Q9      Banana          1800                        8540000000  8670000000
            R12     New REZ         150                         NaN         NaN

        id_cols_rename:
            {
                "REZ ID" : "geo_id",
                "Connection capacity (MVA)": "connection_capacity"
            }

        returns:
            geo_id  year    connection_capacity     connection_cost
            N1      2025    400                     730000000
            N1      2026    400                     740000000
            Q9      2025    1800                    8540000000
            Q9      2026    1800                    8670000000
            R12     2025    150                     NaN                # R12 NaN value returned
            R12     2026    150                     NaN                # R12 NaN value returned
    """

    year_cols = [
        c for c in connection_cost_forecast.columns if _looks_like_financial_year(c)
    ]
    melted = connection_cost_forecast.melt(
        id_vars=list(id_cols_rename.keys()),
        value_vars=year_cols,
        var_name="year",
        value_name="connection_cost",
        ignore_index=True,
    )
    melted = melted.rename(columns=id_cols_rename)
    melted = _enforce_numeric_cols(melted, ["connection_cost", "connection_capacity"])
    melted["year"] = melted["year"].map(_financial_year_string_to_end_year_int)
    return melted[
        ["geo_id", "year", "connection_capacity", "connection_cost"]
    ].reset_index(drop=True)


def _enforce_numeric_cols(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    """Applies pd.to_numeric() with arg ``errors='coerce'`` to each ``df`` column in ``numeric_cols``."""
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _merge_connection_cost_and_capacity_frames(
    cost_df: pd.DataFrame, capacity_df: pd.DataFrame, merge_cols: list[str]
) -> pd.DataFrame:
    """Left-merges cost forecast with connection capacity on ``merge_cols``.

    Note: for VRE connection costs, REZ IDs present only in ``capacity_df`` are
    dropped; offshore REZs with no cost forecast are handled downstream by
    ``_build_vre_cost_rows``.
    """
    return cost_df.merge(capacity_df, how="left", on=merge_cols)


def _calculate_connection_cost_per_mw(
    cost_and_capacity_df: pd.DataFrame,
) -> pd.DataFrame:
    """Divides total connection cost by connection capacity to return $/MW connection cost.

    Division by zero (inf) is replaced with pd.NA. NaN cost inputs are preserved.

    I/O Example:
        cost_and_capacity_df:
            geo_id  connection_capacity  year  connection_cost
            N1      400                  2025  730000000.0
            Q9      1800                 2025  8540000000.0
            R12     150                  2025  NaN            # NaN: no cost data
            R13     0                    2025  120000000.0    # Becomes NaN - capacity = 0.0

        returns:
            geo_id  year  connection_cost
            N1      2025  1825000.0      # $73M / 400
            Q9      2025  4744444.4      # $854M / 1800
            R12     2025  NaN            # NaN / 150 -> NaN
            R13     2025  NaN            # $12M / 0.0 -> set to NaN
    """
    costs_per_mw = cost_and_capacity_df.copy()
    costs_per_mw["connection_cost"] = (
        costs_per_mw["connection_cost"]
        .div(costs_per_mw["connection_capacity"])
        .replace([np.inf, -np.inf], np.nan)  # use np.nan for float64 col values
    )
    return costs_per_mw.drop(columns=["connection_capacity"])


def _warn_nan_connection_costs(
    costs_per_mw: pd.DataFrame,
    id_cols: list[str],
) -> None:
    """Logs a WARNING for any (id_cols, year) combinations where connection_cost is NaN.

    Note: A NaN (or zero) connection cost means no additional cost above the base build cost
    is applied for that generator/geo_id/year combination. It does not imply the absence
    of connection or build constraints — those are managed separately via explicit build
    limits and network capacity constraints in the model.
    """
    nan_rows = costs_per_mw[costs_per_mw["connection_cost"].isna()]
    if nan_rows.empty:
        return
    log_cols = id_cols + ["year"]
    identifiers = sorted(
        nan_rows[log_cols].drop_duplicates().itertuples(index=False, name=None)
    )
    for id_vals in identifiers:
        id_val_string = ", ".join(f"{col}={val}" for col, val in zip(log_cols, id_vals))
        logging.warning(
            f"NaN connection cost after per-MW calculation for: ({id_val_string}) "
            "— no additional connection cost will be applied here"
        )


# temp define here helper function for scenario filtering
# todo: consider move into helper.py file if useful in other places?
def _filter_table_by_isp_scenario(
    table: pd.DataFrame, scenario: str, scenario_col_name: str, table_desc: str
) -> pd.DataFrame:
    """Filters an IASR table to rows matching ``scenario`` and drops the scenario column.

    Fuzzy-matches scenario names to handle minor spelling variations in the raw data.

    I/O Example:
        table:
            Region      Scenario        2025-26     2026-27
            NSW         Step Change     100         150
            NSW         Slower Growth   200         300

        scenario = "Step Change", scenario_col_name = "Scenario"

        returns:
            Region      2025-26     2026-27
            NSW         100         300
    """
    table = table.copy()
    table[scenario_col_name] = _fuzzy_map_to_canonical(
        table[scenario_col_name],
        _ISP_SCENARIOS_NEW,
        task_desc=f"filtering {table_desc} table by ISP scenario",
        threshold=80,
    )
    filtered = (
        table.loc[table[scenario_col_name] == scenario]
        .drop(columns=[scenario_col_name])
        .reset_index(drop=True)
    )
    _warn_if_no_scenario_rows(filtered, scenario, table_desc)
    return filtered


def _warn_if_no_scenario_rows(
    filtered_table: pd.DataFrame, scenario: str, table_desc: str
) -> None:
    """Logs a WARNING if filtering a table by scenario produced no matching rows."""
    if filtered_table.empty:
        logging.warning(
            f"No rows matched scenario '{scenario}' in {table_desc} table "
            "— filtered table will be empty"
        )


# temp define here - copied over from https://github.com/Open-ISP/ISPyPSA/pull/102/changes#top `network_expansion.py`
# todo: move to helpers.py / just pull in once that PR gets merged
def _looks_like_financial_year(col: str) -> bool:
    """True if column name matches a financial year pattern like '2024-25'."""
    return bool(re.match(r"^\d{4}-\d{2}$", str(col)))
