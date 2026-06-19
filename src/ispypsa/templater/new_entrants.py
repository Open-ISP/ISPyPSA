"""Templates the new entrant generator and storage identity tables.

Both tables are currently built from a single IASR input, the ``new_entrants_summary``
table. This module splits that table into its two subsets and shapes each into the
identity columns of its target schema (see schemas/generators_new_entrant.yaml and
schemas/storage_new_entrant.yaml).

There are two independent public orchestrators, one per output table, each taking
the full summary. They share the same shape:
    1. Filter the summary to the relevant technology group
    2. Rename the carried-over summary columns to their schema names
    3. Derive geo_id
    4. (Generators only) Derive resource_type
    5. Select the table's group-specific identity columns.
"""

import logging

import pandas as pd

from ispypsa.templater.helpers import _where_any_substring_appears

_GENERATOR_IDENTITY_COLUMNS = [
    "name",
    "technology",
    "resource_type",
    "geo_id",
    "fuel_type",
    "fuel_price_mapping",
]

_STORAGE_IDENTITY_COLUMNS = [
    "name",
    "technology",
    "geo_id",
    "fuel_type",
]

_STORAGE_TECHNOLOGY_STRINGS = ["battery", "batteries", "pumped hydro"]

# Source (IASR new_entrants_summary) column names → schema output column names.
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

# Extraction pattern for the resource-quality code embedded between underscores in
# a VRE IASR ID, e.g. "WFX" in "N10_WFX_Hunter Coast".
_RESOURCE_CODE_PATTERN = "_({})_".format(
    "|".join(sorted(_RESOURCE_QUALITY_CODE_TO_TYPE, key=len, reverse=True))
)


# --- public orchestrators ---


# NOTE: partial scope intentional - other columns to be added in next PRs!
def _template_generators_new_entrant(
    new_entrants_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Templates the new entrant generators identity table from the IASR summary.

    Keeps only generator rows, renames the carried-over summary columns to schema
    names, derives geo_id (REZ ID or sub-region) and resource_type (from the VRE
    resource code in the IASR ID), and returns the identity columns.

    I/O Example:
        new_entrants_summary (abbr.):
            IASR ID     Power Station   Technology Type REZ ID          Sub-region  Fuel type   Fuel cost mapping
            N3_WH_rez   N3_WH_rez       Wind            N3              NNSW        Wind        Wind
            N3 Battery  N3 Battery      Battery (2hrs)  N3              NNSW        Battery     Battery
            SQ CCGT     SQ CCGT         CCGT            Not Applicable  SQ          Gas         QLD new CCGT

    Returns:
        name        technology  resource_type   geo_id  fuel_type   fuel_price_mapping
        N3_WH_rez   Wind        wind_high       N3      Wind        Wind
        SQ CCGT     CCGT                        SQ      Gas         QLD new CCGT

    """
    logging.info("Creating a template for new entrant generators")
    gens = _filter_to_technology_group(new_entrants_summary, "generators")
    gens = gens.rename(columns=_SUMMARY_COLUMN_RENAMES)
    gens = _set_geo_id(gens)
    gens = _add_resource_type(gens)
    return gens[_GENERATOR_IDENTITY_COLUMNS]


# NOTE: partial scope intentional - other columns to be added in next PRs!
def _template_storage_new_entrant(
    new_entrants_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Templates the new entrant storage identity table from the IASR summary.

    Keeps only storage rows, renames the carried-over summary columns to schema
    names, derives geo_id (REZ ID or sub-region), and returns the identity columns.

    I/O Example:
        new_entrants_summary (abbr.):
            IASR ID     Power Station   Technology Type REZ ID          Sub-region  Fuel type   Fuel cost mapping
            N3_WH_rez   N3_WH_rez       Wind            N3              NNSW        Wind        Wind
            N3 Battery  N3 Battery      Battery (2hrs)  N3              NNSW        Battery     Battery
            SQ CCGT     SQ CCGT         CCGT            Not Applicable  SQ          Gas         QLD new CCGT

    Returns:
        name        technology      geo_id  fuel_type
        N3 Battery  Battery (2hrs)  N3      Battery
    """
    logging.info("Creating a template for new entrant storage")
    storage = _filter_to_technology_group(new_entrants_summary, "storage")
    storage = storage.rename(columns=_SUMMARY_COLUMN_RENAMES)
    storage = _set_geo_id(storage)
    return storage[_STORAGE_IDENTITY_COLUMNS]


# --- shared helpers ---


def _filter_to_technology_group(
    new_entrants_summary: pd.DataFrame, group: str
) -> pd.DataFrame:
    """Returns the summary rows for one technology group: generators or storage.

    Storage rows are those whose "Technology Type" contains a
    ``_STORAGE_TECHNOLOGY_STRINGS`` substring (battery, pumped hydro), matched
    case-insensitively; generators are every other row. The two groups partition
    the summary, so this single predicate is the only place the generator/storage
    boundary is defined.

    Args:
        new_entrants_summary: the IASR ``new_entrants_summary`` table
        group: "generators" or "storage".

    I/O Example:
        new_entrants_summary:
            Technology Type                  REZ ID
            Wind                             N3
            Battery Storage (2hrs storage)   N3
            Pumped Hydro (24hrs storage)     Not Applicable
            OCGT (small GT)                  Not Applicable

        group="generators" returns:
            Technology Type                  REZ ID
            Wind                             N3
            OCGT (small GT)                  Not Applicable

        group="storage" returns:
            Technology Type                  REZ ID
            Battery Storage (2hrs storage)   N3
            Pumped Hydro (24hrs storage)     Not Applicable
    """
    is_storage = _where_any_substring_appears(
        new_entrants_summary["Technology Type"], _STORAGE_TECHNOLOGY_STRINGS
    )
    if group == "storage":
        return new_entrants_summary.loc[is_storage].reset_index(drop=True)
    if group == "generators":
        return new_entrants_summary.loc[~is_storage].reset_index(drop=True)
    raise ValueError(
        "Filtering new entrants table to technology group: "
        f"group must be 'generators' or 'storage', got {group!r}"
    )


def _set_geo_id(new_entrants: pd.DataFrame) -> pd.DataFrame:
    """Sets ``geo_id`` from the row's REZ ID, falling back to its Sub-region.

    I/O Example:
        new_entrants:
            technology                       REZ ID           Sub-region
            Wind                             N3               CNSW
            Large scale Solar PV             N0               CNSW       # Non-REZ: kept as-is
            OCGT (small GT)                  Not Applicable   NQ
            Pumped Hydro (24hrs storage)     Not Applicable   SNW

        returns (adds geo_id):
            technology                       REZ ID           Sub-region  geo_id
            Wind                             N3               CNSW        N3
            Large scale Solar PV             N0               CNSW        N0
            OCGT (small GT)                  Not Applicable   NQ          NQ
            Pumped Hydro (24hrs storage)     Not Applicable   SNW         SNW
    """
    new_entrants = new_entrants.copy()
    new_entrants["geo_id"] = new_entrants["REZ ID"].where(
        new_entrants["REZ ID"] != "Not Applicable", new_entrants["Sub-region"]
    )
    return new_entrants


# --- generator-specific helpers ---


def _add_resource_type(gens: pd.DataFrame) -> pd.DataFrame:
    """Adds the VRE ``resource_type`` column from the resource code in ``name``.

    VRE IASR IDs embed a resource-quality code between underscores — e.g. the "WH"
    in "Q1_WH_Far North QLD". The code is extracted and mapped via
    ``_RESOURCE_QUALITY_CODE_TO_TYPE``. IDs with no matching code — the underscore-
    free thermal and distributed-resource rows — get NaN.

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
    resource_code = gens["name"].str.extract(_RESOURCE_CODE_PATTERN, expand=False)
    gens["resource_type"] = resource_code.map(_RESOURCE_QUALITY_CODE_TO_TYPE)
    return gens
