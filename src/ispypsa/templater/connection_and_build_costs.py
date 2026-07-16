# File holds functions for templating CONNECTION AND BUILD COST tables that hold
# data with time-varying (spanning multiple years) values. These tables contain
# data that applies to both generation and storage (new entrant).
import logging
import re

import numpy as np
import pandas as pd

from ispypsa.templater.helpers import (
    _financial_year_string_to_end_year_int,
    _fuzzy_map_to_allowed_values,
    _looks_like_financial_year,
    _where_any_substring_appears,
)
from ispypsa.templater.lists import _ISP_SCENARIOS_NEW

_VRE_COLUMN_RENAMES = {
    "REZ ID": "geo_id",
    "Connection capacity (MVA)": "connection_capacity",
}
_NON_VRE_COLUMN_RENAMES = {
    "Region": "region_id",
    "Generator Type": "technology",
    "Connection capacity (MVA)": "connection_capacity",
}

# NOTE: the VRE / non-VRE split below is approximate, not strict. The IASR
# "connection_cost_forecast_other" (non-VRE) table actually includes "distributed
# solar", so "non-VRE" really means "everything templated outside the wind/solar
# REZ-connection path". This is a known wart — revisit how distributed resources
# are handled once more of the templater (and schema validation) is in place.
_VRE_TECHNOLOGY_STRINGS = ["solar", "wind"]
_IBR_TECHNOLOGY_STRINGS = _VRE_TECHNOLOGY_STRINGS + ["battery", "batteries"]
_NON_VRE_EXCLUDED_TECHNOLOGIES = ["Alkaline Electrolyser", "BOTN - Cethana"]


def _template_connection_costs(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    regional_granularity: str,
    generators_new_entrant: pd.DataFrame,
    storage_new_entrant: pd.DataFrame,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Combines VRE and non-VRE connection costs with system strength costs.

    Produces a long-format table with columns (geo_id, technology, year,
    connection_cost, system_strength_cost). VRE (wind/solar) costs are templated
    per-REZ; non-VRE costs are templated per-region and expanded to geo_id level
    based on ``regional_granularity``. System strength costs ($/MW) are merged in
    for all IBR technologies (wind, solar, battery).

    Args:
        iasr_tables: dict containing the following keys:
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
        generators_new_entrant: templated new entrant generators table;
            columns used: 'geo_id' and 'technology'.
        storage_new_entrant: templated new entrant storage table;
            columns used: 'geo_id' and 'technology'.
        sub_regional_geography: templated network geography table;
            columns used: 'geo_id', 'geo_type', 'region_id'.

    Returns:
        One row per (geo_id, technology, year). connection_cost and
        system_strength_cost are in $/MW; system_strength_cost is 0.0 for
        non-IBR technologies and solar thermal.

    I/O Example:
        Inputs (abbreviated; regional_granularity="nem_regions"):

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
                                        2024-25   2025-26
                IBR remediation $/kW    163.24    148.88

            generators_new_entrant:
                geo_id  technology
                N1      Wind
                N1      Large scale Solar PV
                NNSW    CCGT

            storage_new_entrant: (empty)

        Returns:
            geo_id  technology              year  connection_cost  system_strength_cost
            N1      Wind                    2025  182500.0         163240.0  # VRE IBR
            N1      Wind                    2026  185000.0         148880.0
            N1      Large scale Solar PV    2025  182500.0         163240.0  # VRE IBR
            N1      Large scale Solar PV    2026  185000.0         148880.0
            NSW     CCGT                    2025  100000.0         0.0       # non-VRE, non-IBR
            NSW     CCGT                    2026  105000.0         0.0
    """

    system_strength_costs = _normalise_system_strength_cost_frame(
        iasr_tables["efficient_level_of_system_strength_cost"]
    )

    canonical_technology_geo_id_pairs = _get_canon_technology_and_geo_id_pairs(
        generators_new_entrant, storage_new_entrant
    )

    vre_connection_costs = _template_vre_connection_costs(
        iasr_tables["connection_cost_forecast_wind_and_solar"],
        iasr_tables["connection_costs_for_wind_and_solar"],
        scenario,
        canonical_technology_geo_id_pairs,
    )
    non_vre_connection_costs = _template_non_vre_connection_costs(
        iasr_tables["connection_cost_forecast_other"],
        iasr_tables["connection_capacity_non_vre"],
        scenario,
        canonical_technology_geo_id_pairs,
        regional_granularity,
        sub_regional_geography,
    )

    combined_connection_costs = pd.concat(
        [vre_connection_costs, non_vre_connection_costs], axis=0, ignore_index=True
    )
    return _merge_and_filter_system_strength_costs(
        combined_connection_costs, system_strength_costs
    )


def _get_canon_technology_and_geo_id_pairs(
    generators_new_entrant: pd.DataFrame, storage_new_entrant: pd.DataFrame
) -> pd.DataFrame:
    """Combines and deduplicates (geo_id, technology) pairs from new entrant generators
    and storage tables."""
    generators = generators_new_entrant[["geo_id", "technology"]].copy()
    storage = storage_new_entrant[["geo_id", "technology"]].copy()
    combined = pd.concat([generators, storage], axis=0, ignore_index=True)
    return combined.drop_duplicates().reset_index(drop=True)


# --- VRE connection costs ---


def _template_vre_connection_costs(
    connection_cost_forecast_vre: pd.DataFrame,
    connection_costs_for_vre: pd.DataFrame,
    scenario: str,
    canonical_technology_geo_id_pairs: pd.DataFrame,
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

            canonical_technology_geo_id_pairs:
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
    vre_technologies_by_geo_id = _get_unique_vre_geo_id_rows(
        canonical_technology_geo_id_pairs
    )
    scenario_cost_forecast = _filter_table_by_isp_scenario(
        connection_cost_forecast_vre,
        scenario,
        "Scenario",
        "VRE connection cost forecast",
    )
    merged_df = scenario_cost_forecast.merge(
        connection_costs_for_vre, how="left", on=["REZ ID"]
    )
    normalised_df = _normalise_connection_cost_forecast_frame(
        merged_df, id_cols_rename=_VRE_COLUMN_RENAMES
    )
    costs_per_mw = _calculate_connection_cost_per_mw(normalised_df)
    _warn_nan_connection_costs(costs_per_mw, id_cols=["geo_id"])
    return _build_vre_cost_rows(costs_per_mw, vre_technologies_by_geo_id)


def _get_unique_vre_geo_id_rows(canonical_tech_geo_ids: pd.DataFrame) -> pd.DataFrame:
    """Extracts (geo_id, technology) rows for non-distributed VRE technologies
    from the canonical technology/geo_id pairs.

    Returns only rows where technology contains 'solar' or 'wind' (see
    ``_VRE_TECHNOLOGY_STRINGS``), but not 'distributed'. Deduplication is the
    caller's responsibility — the canonical pairs are already deduplicated upstream
    by ``_get_canon_technology_and_geo_id_pairs``.

    I/O Example:
        canonical_tech_geo_ids:
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
        canonical_tech_geo_ids["technology"], _VRE_TECHNOLOGY_STRINGS
    )
    distributed_resources = _where_any_substring_appears(
        canonical_tech_geo_ids["technology"], ["distributed"]
    )
    rows_to_keep = vre_technologies & ~distributed_resources
    return canonical_tech_geo_ids.loc[
        rows_to_keep, ["geo_id", "technology"]
    ].reset_index(drop=True)


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
            R99     2025  474444.4  # R99 dropped - no VRE technology in canonical pairs
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
    connection_cost_forecast_other: pd.DataFrame,
    connection_capacity_df: pd.DataFrame,
    scenario: str,
    canon_tech_geo_id_pairs: pd.DataFrame,
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Templates connection costs for non-VRE new entrant technologies.

    Filters the cost forecast to the given scenario, merges in connection capacity,
    normalises to long format and $/MW units, canonicalises technology names, then
    expands costs to geo_id level based on ``regional_granularity``. Batteries
    located in REZs receive additional rows keyed to their REZ geo_id using costs
    from their parent NEM region — see :func:`_create_additional_rez_battery_rows`.

    ``canonical_technologies`` should be the union of technology names from the
    templated new entrant generators and storage tables.

    NOTE: 'BOTN - Cethana' is excluded (see ``_NON_VRE_EXCLUDED_TECHNOLOGIES``).
    The IASR workbook defines a $0.0 connection cost for BOTN - Cethana, and it is
    the only entry identified by name rather than technology type — TBD how to
    handle it in the translator.

    I/O Example:
        connection_cost_forecast_other:
            Generator Type          Region  Scenario        2024-25
            CCGT                    NSW     Step Change     40000000
            CCGT                    QLD     Step Change     36000000
            Battery Storage (4h)    SA      Step Change     27000000

        connection_capacity_df:
            Region  Generator Type          Connection capacity (MVA)
            NSW     CCGT                    400
            QLD     CCGT                    300
            SA      Battery Storage (4h)    300

        scenario: "Step Change"

        canon_tech_geo_id_pairs:
            geo_id  technology
            CNSW    CCGT
            SNW     CCGT
            NQ      CCGT
            CSA     Battery Storage (4h)
            S1      Battery Storage (4h)

        regional_granularity: "sub_regions"

        sub_regional_geography:
            geo_id  geo_type    region_id
            CNSW    subregion   NSW
            SNW     subregion   NSW
            NQ      subregion   QLD
            CSA     subregion   SA
            S1      rez         SA

        returns:
            geo_id  technology              year    connection_cost
            CNSW    CCGT                    2025    100000.0
            SNW     CCGT                    2025    100000.0
            NQ      CCGT                    2025    120000.0
            CSA     Battery Storage (4h)    2025    90000.0
            S1      Battery Storage (4h)    2025    90000.0
    """

    scenario_cost_forecast = _filter_table_by_isp_scenario(
        connection_cost_forecast_other,
        scenario,
        "Scenario",
        "Non-VRE connection cost forecast",
    )
    merged_df = scenario_cost_forecast.merge(
        connection_capacity_df, how="left", on=["Region", "Generator Type"]
    )
    normalised_df = _normalise_connection_cost_forecast_frame(
        merged_df, id_cols_rename=_NON_VRE_COLUMN_RENAMES
    )
    canonical_technologies = set(canon_tech_geo_id_pairs["technology"])
    canonicalised_df = _canonicalise_non_vre_technologies(
        normalised_df, canonical_technologies
    )
    costs_per_mw = _calculate_connection_cost_per_mw(canonicalised_df)
    _warn_nan_connection_costs(costs_per_mw, id_cols=["region_id", "technology"])
    return _filter_connection_costs_by_regional_granularity(
        costs_per_mw,
        regional_granularity,
        canon_tech_geo_id_pairs,
        sub_regional_geography,
    )


def _canonicalise_non_vre_technologies(
    df: pd.DataFrame, canonical_technologies: set[str]
) -> pd.DataFrame:
    """Drops unsupported technology rows and maps IASR names to canonical ISPyPSA names.

    Removes rows for technologies listed in ``_NON_VRE_EXCLUDED_TECHNOLOGIES`` (special
    cases, e.g. 'BOTN - Cethana' or not-yet-handled technologies). Remaining values in the
    ``technology`` column are fuzzy-matched to ``canonical_technologies`` at
    threshold=85; any value that cannot be matched raises ValueError.

    NOTE: An empty canonical set means no new-entrant technologies are being
    modelled (e.g. a brownfield-only run) — there is nothing to map costs onto,
    so an empty df (with the same columns and dtypes as input df) is returned.

    I/O Example:
        df:
            region_id  technology          year  connection_capacity  connection_cost
            NSW        CCGT                2025  400                  40000000.0
            NSW        OCGT small GT       2025  400                  32000000.0   # slight variation
            TAS        BOTN - Cethana      2025  250                  0.0          # excluded explicitly
            QLD        CCGT                2025  300                  36000000.0

        canonical_technologies: {"CCGT", "OCGT (small GT)"}

        returns:
            region_id  technology       year  connection_capacity  connection_cost
            NSW        CCGT             2025  400                  40000000.0
            NSW        OCGT (small GT)  2025  400                  32000000.0  # fuzzy-matched
            QLD        CCGT             2025  300                  36000000.0
            # BOTN - Cethana row dropped (listed in ``_NON_VRE_EXCLUDED_TECHNOLOGIES``)
    """
    # NOTE: an only-VRE canonical set (non-empty, but no non-VRE techs) still raises
    # via _fuzzy_map_to_allowed_values when the forecast has non-VRE rows.
    # Intentional (for now) — kinda related to https://github.com/Open-ISP/ISPyPSA/discussions/103 and the final role(s) of validator.
    if not canonical_technologies:
        return pd.DataFrame(columns=df.columns).astype(df.dtypes)

    excluded = _where_any_substring_appears(
        df["technology"], _NON_VRE_EXCLUDED_TECHNOLOGIES
    )
    result = df.loc[~excluded].copy()
    result["technology"] = _fuzzy_map_to_allowed_values(
        result["technology"],
        canonical_technologies,
        task_desc="canonicalising non-VRE connection cost `technology` values",
    )
    return result


def _create_non_vre_rez_cost_rows(
    canon_tech_geo_id_pairs: pd.DataFrame,
    sub_regional_geography: pd.DataFrame,
    connection_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Creates connection cost rows for non-VRE technology located in REZs, keyed by
    the REZ geo_id.

    At IASR workbook v7.5 this only includes 2h, 4h and 8h battery storage
    technologies. Any non-VRE technologies defined in REZs that are not also
    represented in the ``connection_costs`` dataframe will not have rows created.

    I/O Example:
        canon_tech_geo_id_pairs:
            geo_id  technology
            S1      Battery Storage (4h)    # REZ, non-VRE: included
            S1      Wind                    # REZ, but VRE: excluded
            NQ      CCGT                    # subregion, non-VRE: excluded

        sub_regional_geography:
            geo_id  geo_type  region_id
            S1      rez       SA
            NQ      subregion QLD

        connection_costs:
            region_id   technology              year    connection_cost
            SA          Battery Storage (4h)    2025    90000.0
            SA          Battery Storage (4h)    2026    94500.0
            QLD         CCGT                    2025    100000.0
            QLD         CCGT                    2026    120000.0

        returns:
            geo_id  technology              year  connection_cost
            S1      Battery Storage (4h)    2025  90000.0   # SA cost applied to REZ S1
            S1      Battery Storage (4h)    2026  94500.0
    """
    rezs_by_region = sub_regional_geography.loc[
        sub_regional_geography["geo_type"] == "rez", ["geo_id", "region_id"]
    ]
    in_rez = canon_tech_geo_id_pairs["geo_id"].isin(set(rezs_by_region["geo_id"]))
    non_vre = ~_where_any_substring_appears(
        canon_tech_geo_id_pairs["technology"], _VRE_TECHNOLOGY_STRINGS
    )
    rez_technologies = canon_tech_geo_id_pairs[in_rez & non_vre].merge(
        rezs_by_region, how="left", on="geo_id"
    )
    additional_rows = rez_technologies.merge(
        connection_costs, how="inner", on=["region_id", "technology"]
    )
    return additional_rows.drop(columns=["region_id"])


def _filter_connection_costs_by_regional_granularity(
    non_vre_connection_costs: pd.DataFrame,
    regional_granularity: str,
    canon_tech_geo_id_pairs: pd.DataFrame,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Converts region-level non-VRE costs to geo_id-level based on granularity, and
    appends additional rows for batteries located in REZs.

    Three granularity branches are supported:
    - ``"nem_regions"``:   one row per NEM region (region_id renamed to geo_id)
    - ``"sub_regions"``:   one row per sub-region — see
      :func:`_expand_non_vre_connection_costs_to_subregions`
    - ``"single_region"``: all regions averaged to a single "NEM" geo_id — see
      :func:`_average_connection_costs_across_regions`

    Non-VRE technologies defined in REZs (from ``canon_tech_geo_id_pairs``) are
    appended in all branches with their REZ geo_id and costs inherited from the
    parent NEM region — see :func:`_create_non_vre_rez_cost_rows`.

    I/O Example:

        Inputs:
            non_vre_connection_costs:
                region_id  technology              year  connection_cost
                NSW        CCGT                    2025  100000.0    # 40000000 / 400
                NSW        Battery Storage (4h)    2025  50000.0     # 20000000 / 400
                QLD        CCGT                    2025  120000.0    # 36000000 / 300
                QLD        Battery Storage (4h)    2025  60000.0     # 18000000 / 300

            canon_tech_geo_id_pairs:
                geo_id  technology
                N1      Battery Storage (4h)    # REZ: extra row created for N1
                SNW     Battery Storage (4h)
                SNW     CCGT
                CQ      Battery Storage (4h)
                CQ      CCGT
                NQ      Battery Storage (4h)
                NQ      CCGT

            sub_regional_geography:
                geo_id  geo_type    region_id
                N1      rez         NSW
                SNW     subregion   NSW
                CQ      subregion   QLD
                NQ      subregion   QLD

        Returns:
            regional_granularity = "nem_regions":
                geo_id  technology              year  connection_cost
                NSW     CCGT                    2025  100000.0    # region_id -> geo_id
                NSW     Battery Storage (4h)    2025  50000.0
                QLD     CCGT                    2025  120000.0
                QLD     Battery Storage (4h)    2025  60000.0
                N1      Battery Storage (4h)    2025  50000.0     # REZ row appended (NSW cost)

            regional_granularity = "sub_regions":
                geo_id  technology              year  connection_cost
                SNW     CCGT                    2025  100000.0
                SNW     Battery Storage (4h)    2025  50000.0
                CQ      CCGT                    2025  120000.0    # QLD costs applied
                CQ      Battery Storage (4h)    2025  60000.0
                NQ      CCGT                    2025  120000.0
                NQ      Battery Storage (4h)    2025  60000.0
                N1      Battery Storage (4h)    2025  50000.0     # REZ row appended (NSW cost)

            regional_granularity = "single_region":
                geo_id  technology              year  connection_cost
                NEM     CCGT                    2025  110000.0    # mean(100000, 120000)
                NEM     Battery Storage (4h)    2025  55000.0     # mean(50000, 60000)
                N1      Battery Storage (4h)    2025  50000.0     # REZ row: NSW cost, not averaged
    """
    rez_non_vre_rows = _create_non_vre_rez_cost_rows(
        canon_tech_geo_id_pairs, sub_regional_geography, non_vre_connection_costs
    )

    if regional_granularity == "nem_regions":
        connection_costs = non_vre_connection_costs.rename(
            columns={"region_id": "geo_id"}
        )
    elif regional_granularity == "sub_regions":
        connection_costs = _expand_non_vre_connection_costs_to_subregions(
            non_vre_connection_costs, sub_regional_geography
        )
    elif regional_granularity == "single_region":
        connection_costs = _average_connection_costs_across_regions(
            non_vre_connection_costs
        )
    else:
        raise ValueError(f"Unknown regional_granularity: {regional_granularity!r}")

    combined = pd.concat(
        [connection_costs, rez_non_vre_rows], axis=0, ignore_index=True
    )
    return combined[["geo_id", "technology", "year", "connection_cost"]]


def _expand_non_vre_connection_costs_to_subregions(
    non_vre_connection_costs: pd.DataFrame, sub_regional_geography: pd.DataFrame
) -> pd.DataFrame:
    """Expands region-level connection costs to one row per subregion by merging
    with the subregion rows from the network geography table.

    I/O Example:
        non_vre_connection_costs:
            region_id  technology       year  connection_cost
            QLD        OCGT (small GT)  2025  120000.0    # 36000000 / 300
            NSW        OCGT (small GT)  2025  100000.0    # 40000000 / 400

        sub_regional_geography:
            geo_id  geo_type    region_id
            Q9      rez         QLD          # REZ rows ignored (inner merge on subregion only)
            NQ      subregion   QLD
            CQ      subregion   QLD
            NNSW    subregion   NSW
            SNW     subregion   NSW

        returns:
            geo_id  technology       year  connection_cost
            NQ      OCGT (small GT)  2025  120000.0
            CQ      OCGT (small GT)  2025  120000.0
            NNSW    OCGT (small GT)  2025  100000.0
            SNW     OCGT (small GT)  2025  100000.0
    """
    subregion_geo_type = sub_regional_geography.loc[
        sub_regional_geography["geo_type"] == "subregion", ["geo_id", "region_id"]
    ]
    connection_costs_by_subregion = subregion_geo_type.merge(
        non_vre_connection_costs, how="inner", on=["region_id"]
    )
    return connection_costs_by_subregion.drop(columns=["region_id"])


def _average_connection_costs_across_regions(
    non_vre_connection_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Averages connection costs across NEM regions and assigns geo_id="NEM".

    Used for the single_region granularity where all regions and subregions are
    collapsed to one aggregate "NEM".

    I/O Example:
        non_vre_connection_costs:
            region_id  technology       year  connection_cost
            NSW        OCGT (small GT)  2025  100000.0
            QLD        OCGT (small GT)  2025  120000.0
            VIC        OCGT (small GT)  2025  80000.0

        returns:
            geo_id  technology       year  connection_cost
            NEM     OCGT (small GT)  2025  100000.0    # mean(100000, 120000, 80000)
    """
    return (
        non_vre_connection_costs.assign(geo_id="NEM")
        .groupby(["geo_id", "technology", "year"])["connection_cost"]
        .mean()
        .reset_index()
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

    # NOTE: ``efficient_level_of_system_strength_cost`` table direct from IASR
    # has some 'year' columns that are formatted as '2032-2033' (those years onwards)
    # manual fix here:
    year_cols = {
        c: c.replace("-20", "-", 1)
        for c in system_strength_cost_table.columns
        if _looks_like_financial_year(c.replace("-20", "-", 1))
    }
    system_strength_cost_table = system_strength_cost_table.rename(columns=year_cols)
    long_system_strength_costs = system_strength_cost_table.melt(
        value_vars=list(year_cols.values()),
        value_name="system_strength_cost",
        var_name="year",
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
    """Sets system_strength_cost to 0.0 for solar thermal technologies."""
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
        connection_cost_forecast (VRE):
            REZ ID  Connection capacity (MVA)   2024-25
            N1      400                         73000000
            Q9      1800                        854000000
            R99     150                         NaN         # R99: fictional REZ, no cost data

        id_cols_rename: {"REZ ID": "geo_id", "Connection capacity (MVA)": "connection_capacity"}

        returns:
            geo_id  year  connection_capacity  connection_cost
            N1      2025  400                  73000000
            Q9      2025  1800                 854000000
            R99     2025  150                  NaN          # NaN preserved
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
    return melted


def _enforce_numeric_cols(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    """Applies pd.to_numeric() with arg ``errors='coerce'`` to each ``df`` column in ``numeric_cols``."""
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


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
    all_nan_costs = []
    for id_vals in identifiers:
        id_val_string = ", ".join(f"{col}={val}" for col, val in zip(log_cols, id_vals))
        all_nan_costs.append(id_val_string)

    logging.warning(
        f"NaN connection cost after per-MW calculation for: {all_nan_costs} "
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
            NSW         100         150
    """
    table = table.copy()
    table[scenario_col_name] = _fuzzy_map_to_allowed_values(
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
