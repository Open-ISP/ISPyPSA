# File holds functions for templating CONNECTION AND BUILD COST tables that hold
# data with time-varying (spanning multiple years) values. These tables contain
# data that applies to both generation and storage (new entrant).
import logging

import numpy as np
import pandas as pd

from ispypsa.templater.helpers import (
    _assert_table_valid,
    _best_fuzzy_match,
    _build_geo_region_lookup,
    _collapse_geo_id_and_average,
    _convert_values_to_numeric,
    _filter_table_by_isp_scenario,
    _melt_financial_year_columns,
    _set_geo_id,
    _where_any_substring_appears,
)
from ispypsa.templater.mappings import (
    _COST_TABLE_COLUMN_RENAMES,
    _SUMMARY_COLUMN_RENAMES,
)
from ispypsa.templater.new_entrants import _override_botn_technology

# NOTE: the VRE / non-VRE split below is approximate, not strict. The IASR
# "connection_cost_forecast_other" (non-VRE) table actually includes "distributed
# solar", so "non-VRE" really means "everything templated outside the wind/solar
# REZ-connection path". This is a known wart — revisit how distributed resources
# are handled once more of the templater (and schema validation) is in place.
_VRE_TECHNOLOGY_STRINGS = ["solar", "wind"]
_IBR_TECHNOLOGY_STRINGS = _VRE_TECHNOLOGY_STRINGS + ["battery", "batteries"]
_EXCLUDED_TECHNOLOGIES = {"Alkaline Electrolyser"}
# The v7.5 system strength cost table covers 2026-2054, one year short at each end
# of the connection cost forecasts (2025-2055). Maps each missing year to the
# nearest covered year, whose cost it takes.
_KNOWN_MISSING_SYSTEM_STRENGTH_COST_YEARS = {2025: 2026, 2055: 2054}


def _template_connection_costs(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Combines VRE and non-VRE connection costs with system strength costs.

    Produces a long-format table with columns (geo_id, technology, year,
    connection_cost, system_strength_cost), with a row for each new entrant option in
    ``new_entrants_summary`` (see _extract_new_entrant_pairs). VRE (wind/solar) costs
    are templated per REZ. Non-VRE costs are published per region: each new entrant
    option takes its region's cost, and geo_id is then collapsed to
    ``regional_granularity`` (see _template_non_vre_connection_costs). Non-VRE
    technologies in _EXCLUDED_TECHNOLOGIES are dropped, and the rest are respelt to
    match the new entrant technologies (see _canonicalise_technologies). System
    strength costs ($/MW) are merged in for all IBR technologies (wind, solar,
    battery).

    Args:
        iasr_tables: dict containing the following keys:
            ``new_entrants_summary``: columns IASR ID / DLT names, Technology Type,
                REZ ID, Sub-region.
            ``connection_cost_forecast_wind_and_solar``: columns REZ ID, Scenario,
                one column per financial year (total connection cost in $).
            ``connection_costs_for_wind_and_solar``: columns REZ ID, Connection
                capacity (MVA).
            ``connection_cost_forecast_other``: columns Generator Type, Region,
                Scenario, one column per financial year (total connection cost in $).
            ``connection_capacity_non_vre``: columns Region, Generator Type,
                Connection capacity (MVA). Manually extracted — see
                ``manually_extracted_template_tables/``.
            ``efficient_level_of_system_strength_cost``: single row with one column
                per financial year (IBR remediation cost in $/kW).
        scenario: ISP scenario name, e.g. "Step Change".
        regional_granularity: "sub_regions", "nem_regions", or "single_region".
        sub_regional_geography: network_geography templated at "sub_regions"
            granularity; columns used: 'geo_id', 'geo_type', 'region_id'.

    Returns:
        One row per (geo_id, technology, year). connection_cost and
        system_strength_cost are in $/MW; system_strength_cost is 0.0 for
        non-IBR technologies and solar thermal. The years the system strength table
        is known not to cover take the nearest covered year's cost (see
        _add_known_missing_system_strength_cost_years); for IBR technologies it is
        NaN in any other year the table doesn't cover.

    I/O Example:
        Inputs (abbreviated; regional_granularity="nem_regions"):

            new_entrants_summary:
                IASR ID / DLT names  Technology Type       Sub-region  REZ ID
                N1 Wind              Wind                  NNSW        N1
                N1 Solar             Large scale Solar PV  NNSW        N1
                CNSW CCGT            CCGT                  CNSW        Not Applicable
                SNW CCGT             CCGT                  SNW         Not Applicable

            connection_cost_forecast_wind_and_solar:
                REZ ID  Scenario      2024-25   2025-26
                N1      Step Change   73000000  74000000

            connection_costs_for_wind_and_solar:
                REZ ID  Connection capacity (MVA)
                N1      400

            connection_cost_forecast_other:
                Generator Type  Region  Scenario      2024-25   2025-26
                CCGT            NSW     Step Change   40000000  42000000

            connection_capacity_non_vre:
                Region  Generator Type  Connection capacity (MVA)
                NSW     CCGT            400

            efficient_level_of_system_strength_cost:
                Unnamed: 1                  2025-26   ...  2053-54
                IBR remediation cost $/kW   163.24    ...  60.45

        Returns:
            geo_id  technology              year  connection_cost  system_strength_cost
            N1      Wind                    2025  182500.0         163240.0  # 2025 takes 2026's cost
            N1      Wind                    2026  185000.0         163240.0  # VRE IBR
            N1      Large scale Solar PV    2025  182500.0         163240.0
            N1      Large scale Solar PV    2026  185000.0         163240.0
            NSW     CCGT                    2025  100000.0         0.0       # CNSW, SNW -> NSW
            NSW     CCGT                    2026  105000.0         0.0       # non-VRE, non-IBR
    """
    logging.info("Creating a template for new entrant connection costs.")
    new_entrant_pairs = _extract_new_entrant_pairs(iasr_tables["new_entrants_summary"])
    system_strength_costs = _normalise_system_strength_cost_frame(
        _read_cost_table(iasr_tables, "efficient_level_of_system_strength_cost")
    )
    system_strength_costs = _add_known_missing_system_strength_cost_years(
        system_strength_costs
    )
    vre_connection_costs = _template_vre_connection_costs(
        iasr_tables, scenario, new_entrant_pairs
    )
    non_vre_connection_costs = _template_non_vre_connection_costs(
        iasr_tables,
        scenario,
        new_entrant_pairs,
        regional_granularity,
        sub_regional_geography,
    )
    combined_connection_costs = pd.concat(
        [vre_connection_costs, non_vre_connection_costs], axis=0, ignore_index=True
    )
    return _add_system_strength_costs(combined_connection_costs, system_strength_costs)


def _extract_new_entrant_pairs(new_entrants_summary: pd.DataFrame) -> pd.DataFrame:
    """Returns the deduplicated (geo_id, technology) pair of every new entrant option
    in ``new_entrants_summary``, at sub-region/REZ level.

    The pairs decide which cost rows the connection costs keep, and their technologies
    are the spellings both cost tables are matched to. Connection costs need the
    sub-region detail so that they can be aggregated the same way as the other
    new entrant properties (see _template_non_vre_connection_costs).

    geo_id comes from _set_geo_id. BOTN - Cethana takes its own technology, as it does
    in storage_new_entrant (see _override_botn_technology). Otherwise it would be listed
    as the generic PHES 24h option in TAS.

    I/O Example:
        new_entrants_summary (abbreviated):
            IASR ID / DLT names   Technology Type               Sub-region  REZ ID
            N1 Wind               Wind                          NNSW        N1
            CNSW OCGT Small       OCGT (small GT)               CNSW        Not Applicable
            WOO OCGT Small        OCGT (small GT)               CNSW        Not Applicable  # same pair
            BOTN - Cethana - 20h  Pumped Hydro (24hrs storage)  TAS         Not Applicable

        returns:
            geo_id  technology
            N1      Wind
            CNSW    OCGT (small GT)
            TAS     BOTN - Cethana
    """
    summary = _set_geo_id(new_entrants_summary.rename(columns=_SUMMARY_COLUMN_RENAMES))
    summary["technology"] = _override_botn_technology(summary)
    return summary[["geo_id", "technology"]].drop_duplicates().reset_index(drop=True)


# --- VRE connection costs ---


def _template_vre_connection_costs(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    new_entrant_pairs: pd.DataFrame,
) -> pd.DataFrame:
    """Templates connection costs for VRE (wind and solar) new entrant technologies.

    Reads the cost forecast (``connection_cost_forecast_wind_and_solar``), filters it
    to the given scenario and melts it to long format, then merges in connection
    capacity (``connection_costs_for_wind_and_solar``), converts to $/MW, and expands to all VRE (geo_id, technology) pairs in
    ``new_entrant_pairs``. REZs absent from the cost forecast (e.g. offshore wind) are
    included with NaN connection_cost — system strength costs are still applied to them
    downstream. NaN connection costs for REZs that ARE in the forecast are logged as a
    WARNING.

    I/O Example:
        Inputs:

            iasr_tables["connection_cost_forecast_wind_and_solar"]:
                REZ ID  REZ names       Scenario        2024-25     2025-26
                N1      North West NSW  Step Change     73000000    74000000
                Q9      Banana          Step Change     854000000   867000000
                # V8 absent: offshore REZs have no connection cost forecast

            iasr_tables["connection_costs_for_wind_and_solar"]:
                REZ ID  REZ names       Connection capacity (MVA)
                N1      North West NSW  400
                Q9      Banana          1800

            scenario: "Step Change"

            new_entrant_pairs:
                geo_id  technology
                NNSW    CCGT
                NNSW    Battery Storage (4h)
                N1      Wind
                N1      Large scale Solar PV
                Q9      Large scale Solar PV
                V8      Wind - offshore (fixed)

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
    vre_technologies_by_geo_id = _get_unique_vre_geo_id_rows(new_entrant_pairs)
    forecast_name = "connection_cost_forecast_wind_and_solar"
    forecast = _read_cost_table(iasr_tables, forecast_name)
    forecast = _filter_table_by_isp_scenario(forecast, scenario, forecast_name)
    costs = _melt_financial_year_columns(
        forecast, ["geo_id"], "connection_cost", forecast_name
    )
    capacity = _read_connection_capacity(
        iasr_tables, "connection_costs_for_wind_and_solar", ["geo_id"]
    )
    costs = costs.merge(capacity, how="left", on=["geo_id"])
    costs_per_mw = _calculate_connection_cost_per_mw(costs)
    _warn_nan_connection_costs(costs_per_mw, id_cols=["geo_id"])
    return _build_vre_cost_rows(costs_per_mw, vre_technologies_by_geo_id)


def _get_unique_vre_geo_id_rows(new_entrant_pairs: pd.DataFrame) -> pd.DataFrame:
    """Extracts (geo_id, technology) rows for non-distributed VRE technologies
    from the new entrant pairs.

    Returns only rows where technology contains 'solar' or 'wind' (see
    ``_VRE_TECHNOLOGY_STRINGS``), but not 'distributed'. Deduplication is the
    caller's responsibility — the pairs are already deduplicated upstream by
    ``_extract_new_entrant_pairs``.

    I/O Example:
        new_entrant_pairs:
            geo_id  technology
            N1      Large scale Solar PV
            N1      Wind
            NNSW    OCGT                            # non-VRE: excluded
            NNSW    Distributed Resources Solar     # 'distributed': excluded

        returns:
            geo_id  technology
            N1      Large scale Solar PV
            N1      Wind
    """
    vre_technologies = _where_any_substring_appears(
        new_entrant_pairs["technology"], _VRE_TECHNOLOGY_STRINGS
    )
    distributed_resources = _where_any_substring_appears(
        new_entrant_pairs["technology"], ["distributed"]
    )
    rows_to_keep = vre_technologies & ~distributed_resources
    return new_entrant_pairs.loc[rows_to_keep, ["geo_id", "technology"]].reset_index(
        drop=True
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
            R99     2025  474444.4  # R99 dropped - no VRE technology in the pairs
            R99     2026  481666.7  # R99 dropped
            # V8 absent: no cost forecast defined for offshore REZs

        vre_technologies:
            geo_id  technology
            N1      Wind
            N1      Large scale Solar PV
            V8      Wind - offshore (fixed)

        returns:
            geo_id  technology              year  connection_cost
            N1      Wind                    2025  182500.0
            N1      Wind                    2026  185000.0
            N1      Large scale Solar PV    2025  182500.0
            N1      Large scale Solar PV    2026  185000.0
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


# --- Non-VRE connection costs ---


def _template_non_vre_connection_costs(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    new_entrant_pairs: pd.DataFrame,
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Templates connection costs for non-VRE new entrant options, at
    ``regional_granularity``.

    IASR publishes non-VRE connection costs per NEM region and technology. The cost
    forecast (``connection_cost_forecast_other``) is filtered to ``scenario`` and
    melted to long format, then connection capacity (``connection_capacity_non_vre``)
    is merged in and costs are converted to $/MW. Technologies in
    _EXCLUDED_TECHNOLOGIES are dropped, and the rest are respelt to match
    ``new_entrant_pairs`` (see _canonicalise_technologies).

    Each new entrant option then takes its region's cost (see
    _merge_region_costs_onto_new_entrant_pairs), so only options that exist get a row,
    and REZ batteries take their parent region's cost. Finally, geo_id is collapsed to
    ``regional_granularity`` and costs are averaged over the options that now share a
    geo_id (see _collapse_geo_id_and_average). Each sub-region option counts once, the
    same way the other new entrant properties and fuel prices are averaged.

    I/O Example (regional_granularity="single_region"):
        iasr_tables["connection_cost_forecast_other"]:
            Generator Type        Region  Scenario     2024-25
            CCGT                  NSW     Step Change  40000000
            CCGT                  QLD     Step Change  39000000
            Battery Storage (4h)  SA      Step Change  27000000

        iasr_tables["connection_capacity_non_vre"]:
            Region  Generator Type        Connection capacity (MVA)
            NSW     CCGT                  400
            QLD     CCGT                  300
            SA      Battery Storage (4h)  300

        scenario: "Step Change"

        new_entrant_pairs:
            geo_id  technology
            CNSW    CCGT
            SNW     CCGT
            NQ      CCGT
            S1      Battery Storage (4h)    # REZ battery
            N1      Wind                    # VRE: templated separately, dropped here

        sub_regional_geography:
            geo_id  geo_type    region_id
            CNSW    subregion   NSW
            SNW     subregion   NSW
            NQ      subregion   QLD
            S1      rez         SA
            N1      rez         NSW

        returns:
            geo_id  technology            year  connection_cost
            NEM     CCGT                  2025  110000.0    # mean(100000, 100000, 130000)
            S1      Battery Storage (4h)  2025  90000.0     # REZ: SA cost, geo_id not remapped
    """
    forecast_name = "connection_cost_forecast_other"
    forecast = _read_cost_table(iasr_tables, forecast_name)
    forecast = _filter_table_by_isp_scenario(forecast, scenario, forecast_name)
    costs = _melt_financial_year_columns(
        forecast, ["region_id", "technology"], "connection_cost", forecast_name
    )
    capacity = _read_connection_capacity(
        iasr_tables, "connection_capacity_non_vre", ["region_id", "technology"]
    )
    costs = costs.merge(capacity, how="left", on=["region_id", "technology"])
    costs = _canonicalise_technologies(
        costs,
        set(new_entrant_pairs["technology"]),
        "templating non-VRE connection costs",
    )
    costs = _calculate_connection_cost_per_mw(costs)
    _warn_nan_connection_costs(costs, id_cols=["region_id", "technology"])
    costs = _merge_region_costs_onto_new_entrant_pairs(
        costs, new_entrant_pairs, sub_regional_geography
    )
    return _collapse_geo_id_and_average(
        costs,
        ["geo_id", "technology", "year"],
        "connection_cost",
        regional_granularity,
        sub_regional_geography,
    )


def _merge_region_costs_onto_new_entrant_pairs(
    region_costs: pd.DataFrame,
    new_entrant_pairs: pd.DataFrame,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Gives each new entrant (geo_id, technology) pair the cost rows of its region.

    Each geo_id, whether a sub-region or a REZ, is looked up to its region with
    _build_geo_region_lookup and then inner-merged on (region_id, technology). Pairs with
    no regional cost (e.g. VRE, which is costed per REZ) and regional costs with no new
    entrant are both dropped.

    I/O Example:
        region_costs:
            region_id  technology  year  connection_cost
            NSW        CCGT        2025  100000.0
            QLD        CCGT        2025  130000.0    # no QLD CCGT new entrant: dropped
            SA         Battery     2025  90000.0

        new_entrant_pairs:
            geo_id  technology
            CNSW    CCGT
            SNW     CCGT
            S1      Battery     # REZ: takes SA's cost
            N1      Wind        # no regional cost for Wind: dropped

        sub_regional_geography:
            geo_id  geo_type   region_id
            CNSW    subregion  NSW
            SNW     subregion  NSW
            S1      rez        SA
            N1      rez        NSW

        returns:
            geo_id  technology  year  connection_cost
            CNSW    CCGT        2025  100000.0
            SNW     CCGT        2025  100000.0
            S1      Battery     2025  90000.0
    """
    region_by_geo_id = _build_geo_region_lookup(sub_regional_geography)
    pairs = new_entrant_pairs.assign(
        region_id=new_entrant_pairs["geo_id"].map(region_by_geo_id)
    )
    costs = pairs.merge(region_costs, how="inner", on=["region_id", "technology"])
    return costs.drop(columns="region_id")


# --- system strength costs ---


def _add_system_strength_costs(
    connection_costs: pd.DataFrame,
    system_strength_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Left-merges system strength costs into the combined connection costs table,
    then zeros out system strength costs for non-IBR technologies.

    System strength costs apply only to IBR technologies (wind, solar, battery).
    Solar thermal contains "solar" so matches ``_IBR_TECHNOLOGY_STRINGS`` but does
    not incur a system strength cost, so it is zeroed separately.

    Years missing from ``system_strength_costs`` get NaN for IBR technologies, and
    are logged (see _warn_years_without_system_strength_cost). The years known to be
    missing are filled before this runs (see
    _add_known_missing_system_strength_cost_years), so a year logged here is an
    unexpected gap.

    I/O Example:
        connection_costs:
            geo_id  technology              year  connection_cost
            N1      Large scale Solar PV    2025  182500.0
            N1      Wind                    2025  182500.0
            NSW     OCGT (small GT)         2025  100000.0

        system_strength_costs:
            year  system_strength_cost
            2025  163240.0

        returns:
            geo_id  technology              year  connection_cost  system_strength_cost
            N1      Large scale Solar PV    2025  182500.0         163240.0  # IBR: cost applied
            N1      Wind                    2025  182500.0         163240.0  # IBR: cost applied
            NSW     OCGT (small GT)         2025  100000.0         0.0       # non-IBR: zeroed
    """
    costs = connection_costs.merge(system_strength_costs, how="left", on=["year"])
    costs = _set_non_ibr_system_strength_cost_to_zero(costs)
    costs = _set_solar_thermal_system_strength_costs_to_zero(costs)
    _warn_years_without_system_strength_cost(costs)
    return costs


def _normalise_system_strength_cost_frame(
    system_strength_cost_table: pd.DataFrame,
) -> pd.DataFrame:
    """Melts the wide system strength cost table to one row per year and converts
    costs from $/kW to $/MW.

    The financial-year columns are melted by _melt_financial_year_columns, which
    drops the row-label column.

    I/O Example:
        system_strength_cost_table:
            Unnamed: 1                 2025-26  2026-27
            IBR remediation cost $/kW  163.24   148.88

        returns:
            year  system_strength_cost
            2026  163240.0    # 163.24 $/kW * 1000
            2027  148880.0
    """
    long = _melt_financial_year_columns(
        system_strength_cost_table,
        [],
        "system_strength_cost",
        "efficient_level_of_system_strength_cost",
    )
    long["system_strength_cost"] *= 1000  # convert to $/MW
    return long


def _add_known_missing_system_strength_cost_years(
    system_strength_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Adds a row for each year in _KNOWN_MISSING_SYSTEM_STRENGTH_COST_YEARS, copying
    the system_strength_cost of its nearest covered year.

    Raises a ValueError if a known missing year is already in
    ``system_strength_costs``, since the known list is then out of date. Any other
    year the table doesn't cover stays missing, and is reported after the merge (see
    _warn_years_without_system_strength_cost).

    I/O Example:
        system_strength_costs:
            year  system_strength_cost
            2026  163240.0
            2027  148880.0
            2054  60450.0

        returns:
            year  system_strength_cost
            2025  163240.0    # copied from 2026
            2026  163240.0
            2027  148880.0
            2054  60450.0
            2055  60450.0     # copied from 2054
    """
    _raise_if_known_missing_years_present(system_strength_costs)
    cost_by_year = system_strength_costs.set_index("year")["system_strength_cost"]
    missing_years = list(_KNOWN_MISSING_SYSTEM_STRENGTH_COST_YEARS)
    nearest_years = list(_KNOWN_MISSING_SYSTEM_STRENGTH_COST_YEARS.values())
    filled_years = pd.DataFrame(
        {
            "year": missing_years,
            "system_strength_cost": cost_by_year.loc[nearest_years].to_numpy(),
        }
    )
    all_years = pd.concat([system_strength_costs, filled_years], ignore_index=True)
    return all_years.sort_values("year").reset_index(drop=True)


def _raise_if_known_missing_years_present(system_strength_costs: pd.DataFrame) -> None:
    """Raises if a year in _KNOWN_MISSING_SYSTEM_STRENGTH_COST_YEARS is already in
    ``system_strength_costs``.
    """
    present = sorted(
        set(_KNOWN_MISSING_SYSTEM_STRENGTH_COST_YEARS)
        & set(system_strength_costs["year"])
    )
    if present:
        raise ValueError(
            f"System strength costs for years {present} are now in the "
            "'efficient_level_of_system_strength_cost' table; remove them from "
            "_KNOWN_MISSING_SYSTEM_STRENGTH_COST_YEARS."
        )


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
    """Sets system_strength_cost to 0.0 for solar thermal technologies."""
    solar_thermal_rows = _where_any_substring_appears(
        connection_costs["technology"], ["solar thermal"]
    )
    connection_costs.loc[solar_thermal_rows, "system_strength_cost"] = 0.0
    return connection_costs


def _warn_years_without_system_strength_cost(connection_costs: pd.DataFrame) -> None:
    """Logs one WARNING listing the years where system_strength_cost is NaN.

    Runs after non-IBR and solar thermal rows are zeroed, so any NaN left is an IBR
    technology in a year the system strength table doesn't cover. Listing the years
    also shows up a year header the melt skipped, e.g. a '2032-2033' header.
    """
    nan_rows = connection_costs["system_strength_cost"].isna()
    missing_years = sorted(connection_costs.loc[nan_rows, "year"].unique().tolist())
    if missing_years:
        logging.warning(
            f"No system strength cost for years {missing_years} in "
            "'efficient_level_of_system_strength_cost' table; IBR technologies will "
            "be NaN in those years."
        )


# ----------- Build Costs --------------


def _template_costs_new_entrant_build(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
) -> pd.DataFrame:
    """Templates new entrant build costs: one cost per technology and year, in $/MW.

    Filters the IASR build_costs table to ``scenario``, then drops the technologies in
    _EXCLUDED_TECHNOLOGIES and maps the rest to match the new entrant technologies in
    ``new_entrants_summary`` (see _extract_new_entrant_pairs and
    _canonicalise_technologies). Finally, the financial-year columns are melted to one
    row per year, and costs are converted from $/kW to $/MW.

    I/O Example (scenario="Step Change"):
        iasr_tables["build_costs"] ($/kW):
            Technology                      IASR Scenario  2024-25
            Wind                            Step Change    3412
            Wind                            Slower Growth  3500    # other scenario
            Solar Thermal (16hrs Storage)   Step Change    6892    # mapped to canonical
            Battery storage (2hrs storage)  Step Change    1238
            Alkaline Electrolyser           Step Change    2755    # excluded

        iasr_tables["new_entrants_summary"] (abbreviated):
            IASR ID / DLT names  Technology Type                 Sub-region  REZ ID
            N1 Wind              Wind                            NNSW        N1
            Q4 Solar Thermal     Solar Thermal (16hrs storage)   CQ          Q4
            N1 Battery - 2h      Battery storage (2hrs storage)  NNSW        N1

        returns ($/MW):
            technology                      year  cost
            Wind                            2025  3412000
            Solar Thermal (16hrs storage)   2025  6892000   # canonical - lower case 'storage'
            Battery storage (2hrs storage)  2025  1238000
    """
    logging.info("Creating a template for new entrant build costs.")
    build_costs = _read_cost_table(iasr_tables, "build_costs")
    build_costs = _filter_table_by_isp_scenario(build_costs, scenario, "build_costs")
    new_entrant_pairs = _extract_new_entrant_pairs(iasr_tables["new_entrants_summary"])
    build_costs = _canonicalise_technologies(
        build_costs,
        set(new_entrant_pairs["technology"]),
        "templating new entrant build costs",
    )
    build_costs = _melt_financial_year_columns(
        build_costs, ["technology"], "cost", "build_costs"
    )
    build_costs["cost"] *= 1000  # $/kW -> $/MW
    return build_costs


# --- shared helpers ---


def _read_cost_table(
    iasr_tables: dict[str, pd.DataFrame], table_name: str
) -> pd.DataFrame:
    """Returns ``iasr_tables[table_name]`` with its headers renamed to schema names,
    after checking it has every header the renames need and isn't empty.

    The headers and their new names come from _COST_TABLE_COLUMN_RENAMES. Raises a
    ValueError naming the table if a header is missing or the table is empty (see
    _assert_table_valid).

    I/O Example:
        iasr_tables["build_costs"]:
            Technology  IASR Scenario  2024-25
            Wind        Step Change    3412

        table_name: "build_costs"

        returns:
            technology  scenario     2024-25
            Wind        Step Change  3412
    """
    table = iasr_tables[table_name]
    renames = _COST_TABLE_COLUMN_RENAMES[table_name]
    _assert_table_valid(table, table_name, set(renames))
    return table.rename(columns=renames)


def _read_connection_capacity(
    iasr_tables: dict[str, pd.DataFrame], table_name: str, key_columns: list[str]
) -> pd.DataFrame:
    """Returns ``key_columns`` and a numeric connection_capacity column from the
    connection capacity table ``table_name``.

    Read with _read_cost_table. Raises a ValueError naming ``table_name`` if a
    capacity isn't numeric (see _convert_values_to_numeric).

    I/O Example (key_columns=["geo_id"]):
        iasr_tables["connection_costs_for_wind_and_solar"]:
            REZ ID  REZ names       Connection capacity (MVA)
            N1      North West NSW  400
            Q9      Banana          1800

        returns:
            geo_id  connection_capacity
            N1      400.0
            Q9      1800.0
    """
    capacity = _read_cost_table(iasr_tables, table_name)
    capacity = capacity[[*key_columns, "connection_capacity"]].copy()
    capacity["connection_capacity"] = _convert_values_to_numeric(
        capacity["connection_capacity"], table_name
    )
    return capacity


def _calculate_connection_cost_per_mw(
    cost_and_capacity_df: pd.DataFrame,
) -> pd.DataFrame:
    """Divides total connection cost by connection capacity to return $/MW connection cost.

    Division by zero (inf) is replaced with NaN. NaN cost inputs are preserved.

    I/O Example:
        cost_and_capacity_df:
            geo_id  connection_capacity  year  connection_cost
            N1      400                  2025  73000000.0
            Q9      1800                 2025  854000000.0
            R98     150                  2025  NaN            # no cost data → NaN preserved
            R99     0                    2025  12000000.0     # capacity = 0 → inf → NaN

        returns:
            geo_id  year  connection_cost
            N1      2025  182500.0      # 73000000 / 400
            Q9      2025  474444.4      # 854000000 / 1800
            R98     2025  NaN           # NaN / 150 -> NaN
            R99     2025  NaN           # 12000000 / 0 -> inf -> NaN
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
    """Logs one WARNING listing every (id_cols, year) combination where
    connection_cost is NaN.

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
    all_nan_costs = []
    for id_vals in identifiers:
        id_val_string = ", ".join(f"{col}={val}" for col, val in zip(log_cols, id_vals))
        all_nan_costs.append(id_val_string)

    logging.warning(
        f"NaN connection cost after per-MW calculation for: {all_nan_costs} "
        "— no additional connection cost will be applied here."
    )


def _canonicalise_technologies(
    df: pd.DataFrame, canonical: set[str], task_desc: str
) -> pd.DataFrame:
    """Drops _EXCLUDED_TECHNOLOGIES from ``df``, then maps its remaining
    technologies to match ``canonical`` (see _map_to_canonical_technologies).

    Exclusions go first because they are known to have no counterpart in ``canonical``;
    matching them would always raise.

    I/O Example (canonical={"Wind", "Solar Thermal (16hrs storage)"}):
        df:
            technology                     2024-25
            Wind                           3412
            Solar Thermal (16hrs Storage)  6892     # fuzzy matched -> replaced
            Alkaline Electrolyser          2755     # excluded

        returns:
            technology                     2024-25
            Wind                           3412
            Solar Thermal (16hrs storage)  6892
    """
    clean = _drop_excluded_technologies(df, _EXCLUDED_TECHNOLOGIES, task_desc)
    clean["technology"] = _map_to_canonical_technologies(
        clean["technology"], canonical, task_desc
    )
    return clean


def _map_to_canonical_technologies(
    technologies: pd.Series, canonical: set[str], task_desc: str
) -> pd.Series:
    """Replaces each technology with no exact match in ``canonical`` by its closest
    fuzzy match, logging each replacement as a FUZZY MATCH warning.

    Interim handling until the team discusses how far fuzzy matching may 'decide'
    (Open-ISP/ISPyPSA#TBD). Each warning marks a spelling difference that could
    become an IASR table edit.

    A technology with no match meeting a fuzz.ratio threshold of 90 or more raises.

    I/O Example:
        technologies = ["Wind", "Solar Thermal (16hrs Storage)"]
        canonical = {"Wind", "Solar Thermal (16hrs storage)"}

        returns:
            ["Wind",                            # exact match, unchanged
             "Solar Thermal (16hrs storage)"]   # FUZZY MATCH warning logged
    """
    misses = sorted(set(technologies) - canonical)
    matches = {
        name: _best_fuzzy_match(name, canonical, threshold=90) for name in misses
    }
    unmatched = [name for name, match in matches.items() if match is None]
    if unmatched:
        raise ValueError(
            f"Could not fuzzy match to an allowed value whilst {task_desc}: {unmatched}"
        )
    for name, match in matches.items():
        logging.warning(
            f"FUZZY MATCH: no exact match for '{name}' found whilst {task_desc}. "
            f"Replaced by best fuzzy match '{match}'."
        )
    return technologies.replace(matches)


def _drop_excluded_technologies(
    df: pd.DataFrame, exclusions: set[str], task_desc: str
) -> pd.DataFrame:
    """Drops the rows whose technology is in ``exclusions``, logging the dropped
    technologies once at INFO.

    I/O Example (exclusions={"Alkaline Electrolyser"}):
        df:
            region_id  technology
            NSW        CCGT
            NSW        Alkaline Electrolyser  # dropped
            QLD        Alkaline Electrolyser  # dropped, logged once

        returns:
            region_id  technology
            NSW        CCGT
    """
    to_exclude = df["technology"].isin(exclusions)
    dropped = sorted(df.loc[to_exclude, "technology"].unique())
    if dropped:
        logging.info(
            f"KNOWN EXCLUSION: technology(s) {dropped} excluded while {task_desc}."
        )
    return df.loc[~to_exclude].copy()
