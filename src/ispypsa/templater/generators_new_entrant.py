# Templates the `generators_new_entrant` table: one row per new entrant generating
# unit, with storage technologies excluded (those are templated separately into the
# storage new-entrants table). See schemas/generators_new_entrant.yaml for the target.
import logging

import pandas as pd

from ispypsa.templater.helpers import _where_any_substring_appears

_IDENTITY_COLUMNS = [
    "name",
    "technology",
    "resource_type",
    "geo_id",
    "fuel_type",
    "fuel_price_mapping",
]

_STORAGE_TECHNOLOGY_STRINGS = ["battery", "batteries", "pumped hydro"]

# Source (IASR new_entrants_summary) column names → schema output column names.
# The summary's own values are treated as canonical; no cross-table canonicalisation
# is applied here. "IASR ID / DLT names" is an existing unique identifier per row.
_SUMMARY_COLUMN_RENAMES = {
    "IASR ID / DLT names": "name",
    "Technology Type": "technology",
    "Fuel type": "fuel_type",
    "Fuel cost mapping": "fuel_price_mapping",
}

# TODO(revisit): Distributed Resources Solar currently gets no resource_type; add a
# mapping for it if/when resource_limits templating requires one.
_RESOURCE_QUALITY_CODE_TO_TYPE = {
    "WH": "wind_high",
    "WM": "wind_medium",
    "WFX": "wind_offshore_fixed",
    "WFL": "wind_offshore_floating",
    "SAT": "solar",
    "CST": "solar",
}


# NOTE: partial scope intentional - other columns to be added in next PRs!
def _template_generators_new_entrant(
    new_entrants_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Templates the new entrant generators identity table from the IASR summary.

    Drops storage, renames the carried-over summary columns to schema names, derives
    geo_id (REZ ID or sub-region) and resource_type (from the VRE resource code in
    the IASR ID), and returns the identity columns.

    Args:
        new_entrants_summary: IASR ``new_entrants_summary`` table.

    Returns:
        One row per generating unit with columns ``_IDENTITY_COLUMNS``.
    """
    logging.info("Creating a template for new entrant generators")
    gens = _drop_storage_technologies(new_entrants_summary)
    gens = _rename_summary_columns(gens)
    gens = _set_geo_id(gens)
    gens = _add_resource_type(gens)
    return gens[_IDENTITY_COLUMNS]


def _drop_storage_technologies(new_entrants_summary: pd.DataFrame) -> pd.DataFrame:
    """Drops storage rows from the new entrants summary, keeping only generators.

    Storage (batteries, distributed batteries, pumped hydro) is templated into the
    storage new-entrants table, so it is removed here. Matching is case-insensitive
    on the "Technology Type" column (see ``_STORAGE_TECHNOLOGY_STRINGS``).

    I/O Example:
        new_entrants_summary:
            Technology Type                  REZ ID
            Wind                             N3
            Large scale Solar PV             N3
            Battery Storage (2hrs storage)   N3       # storage: dropped
            Distributed Resources Batteries  Not Applicable  # storage: dropped
            Pumped Hydro (24hrs storage)     Not Applicable  # storage: dropped
            OCGT (small GT)                  Not Applicable

        returns:
            Technology Type                  REZ ID
            Wind                             N3
            Large scale Solar PV             N3
            OCGT (small GT)                  Not Applicable
    """
    is_storage = _where_any_substring_appears(
        new_entrants_summary["Technology Type"], _STORAGE_TECHNOLOGY_STRINGS
    )
    return new_entrants_summary.loc[~is_storage].reset_index(drop=True)


def _rename_summary_columns(gens: pd.DataFrame) -> pd.DataFrame:
    """Renames the summary's identifier, technology and fuel columns to schema names.

    See ``_SUMMARY_COLUMN_RENAMES``. Other columns (e.g. "REZ ID", "Sub-region",
    still needed to derive geo_id) pass through untouched.

    I/O Example:
        gens:
            IASR ID / DLT names   Technology Type   Fuel type   Fuel cost mapping   REZ ID
            Q1_WH_Far North QLD   Wind              Wind        Wind                Q1
            CNSW OCGT Small       OCGT (small GT)   Gas         NSW new OCGT        Not Applicable

        returns:
            name                  technology        fuel_type   fuel_price_mapping  REZ ID
            Q1_WH_Far North QLD   Wind              Wind        Wind                Q1
            CNSW OCGT Small       OCGT (small GT)   Gas         NSW new OCGT        Not Applicable
    """
    return gens.rename(columns=_SUMMARY_COLUMN_RENAMES)


def _set_geo_id(gens: pd.DataFrame) -> pd.DataFrame:
    """Sets ``geo_id`` from the row's REZ ID, falling back to its Sub-region.

    REZ-located generators (VRE) carry a real "REZ ID"; thermal and distributed
    resource rows have "REZ ID" == "Not Applicable" and sit at the sub-region, so
    they take their "Sub-region" value instead. Non-REZ IDs (e.g. N0, V0) flow
    through unchanged as REZ IDs.

    I/O Example:
        gens:
            technology             REZ ID           Sub-region
            Wind                   N3               CNSW
            Large scale Solar PV   N0               CNSW       # Non-REZ: kept as-is
            OCGT (small GT)        Not Applicable   NQ
            Distributed Resources Solar  Not Applicable  SQ

        returns (adds geo_id):
            technology             REZ ID           Sub-region  geo_id
            Wind                   N3               CNSW        N3
            Large scale Solar PV   N0               CNSW        N0
            OCGT (small GT)        Not Applicable   NQ          NQ
            Distributed Resources Solar  Not Applicable  SQ     SQ
    """
    gens = gens.copy()
    gens["geo_id"] = gens["REZ ID"].where(
        gens["REZ ID"] != "Not Applicable", gens["Sub-region"]
    )
    return gens


def _add_resource_type(gens: pd.DataFrame) -> pd.DataFrame:
    """Adds the VRE ``resource_type`` column from the resource code in ``name``.

    VRE IASR IDs embed a resource-quality code between underscores — e.g. the "WH"
    in "Q1_WH_Far North QLD" (wind high) or "SAT" in "DREZ_SAT_Dubbo" (solar). The
    code is extracted and mapped via ``_RESOURCE_QUALITY_CODE_TO_TYPE``. IDs with
    no matching code — the underscore-free thermal and distributed-resource rows —
    get NaN, meaning no VRE build-limit applies.

    I/O Example:
        gens:
            name                              technology
            Q1_WH_Far North QLD               Wind
            Q1_WM_Far North QLD               Wind
            N10_WFX_Hunter Coast              Wind - offshore (fixed)
            DREZ_SAT_Dubbo                    Large scale Solar PV
            N0_CST_NSW                        Solar Thermal (16hrs storage)
            CNSW SAT - Distributed Resources  Distributed Resources Solar
            CNSW OCGT Small                   OCGT (small GT)

        returns (adds resource_type):
            name                              technology                     resource_type
            Q1_WH_Far North QLD               Wind                           wind_high
            Q1_WM_Far North QLD               Wind                           wind_medium
            N10_WFX_Hunter Coast              Wind - offshore (fixed)        wind_offshore_fixed
            DREZ_SAT_Dubbo                    Large scale Solar PV           solar
            N0_CST_NSW                        Solar Thermal (16hrs storage)  solar  # CST -> solar
            CNSW SAT - Distributed Resources  Distributed Resources Solar   NaN  # no _ token
            CNSW OCGT Small                   OCGT (small GT)                NaN  # no _ token
    """
    gens = gens.copy()
    resource_code = gens["name"].str.extract(r"_(WH|WM|WFX|WFL|SAT|CST)_", expand=False)
    gens["resource_type"] = resource_code.map(_RESOURCE_QUALITY_CODE_TO_TYPE)
    return gens
