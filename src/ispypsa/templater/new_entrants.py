"""Templates the new entrant generator and storage identity tables.

Both tables are built from the IASR ``new_entrants_summary`` table (for identity
columns) plus per-technology property tables. This module splits the summary into
its two subsets and shapes each into the columns of its target schema (see
schemas/generators_new_entrant.yaml and schemas/storage_new_entrant.yaml).

There are two independent public orchestrators, one per output table. Each one:
    1. Filters the summary to its technology group (generators or storage)
    2. Renames the carried-over summary columns to their schema names
    3. Derives geo_id (REZ ID or sub-region)
    4. (Generators only) Derives resource_type from the VRE code in the IASR ID
    5. Merges in per-technology property values — each a single number looked up by
       technology, via _merge_technology_property (see the property merge maps in
       mappings.py, e.g. _GENERATORS_NEW_ENTRANT_PROPERTY_MAP)
    6. Selects the table's schema columns.
"""

import logging

import pandas as pd

from ispypsa.templater.helpers import (
    _is_storage_row,
    _pick_location,
)
from ispypsa.templater.mappings import _GENERATORS_NEW_ENTRANT_PROPERTY_MAP

_GENERATOR_IDENTITY_COLUMNS = [
    "name",
    "technology",
    "resource_type",
    "geo_id",
    "fuel_type",
    "fuel_price_mapping",
]

_GENERATOR_PROPERTY_COLUMNS = list(_GENERATORS_NEW_ENTRANT_PROPERTY_MAP)

_STORAGE_IDENTITY_COLUMNS = [
    "name",
    "technology",
    "geo_id",
    "fuel_type",
]

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

# Regex extracting the resource-quality code embedded between underscores in a VRE                                                                                                                                                  # IASR ID, e.g. "WFX" in "N10_WFX_Hunter Coast". Derived from the code map, it
# expands to "_(WFX|WFL|SAT|...)_" — one capture group over the known codes                                                                                                                                                      # sorted longest-first so a short code can't shadow a longer one it prefixes.
_RESOURCE_CODE_PATTERN = "_({})_".format(
    "|".join(sorted(_RESOURCE_QUALITY_CODE_TO_TYPE, key=len, reverse=True))
)


# --- public orchestrators ---


# NOTE: partial scope intentional - lcf_* columns added in a later PR!
def _template_generators_new_entrant(
    iasr_tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Templates the new entrant generators table from the IASR summary and properties.

    Keeps only generator rows, renames the carried-over summary columns to schema
    names, derives geo_id (REZ ID or sub-region) and resource_type (from the VRE
    resource code in the IASR ID), merges in the per-technology property columns
    (see ``_GENERATORS_NEW_ENTRANT_PROPERTY_MAP``), and returns the identity +
    property columns.

    Args:
        iasr_tables: IASR tables; uses ``new_entrants_summary`` plus the property
            tables named in ``_GENERATORS_NEW_ENTRANT_PROPERTY_MAP``.

    I/O Example (identity columns abbreviated to name/technology):
        new_entrants_summary:
            IASR ID / DLT names  Technology Type  ...
            N3_WH_rez            Wind             ...
            SQ CCGT              CCGT             ...

        property tables (one value per technology), e.g. heat_rates_new_entrants:
            Technology  Heat rate (GJ/MWh)
            Wind        0.0
            CCGT        7.25

        returns (property columns shown; identity columns also present):
            name       technology  fom      vom    lifetime_technical  ...  heat_rate  minimum_stable_level
            N3_WH_rez  Wind        18000.0  0.0    40                  ...  0.0        0.0
            SQ CCGT    CCGT        15303.0  4.18   40                  ...  7.25       46.0
    """
    logging.info("Creating a template for new entrant generators")
    gens = new_entrants_summary[~_is_storage_row(new_entrants_summary)].copy()
    gens = gens.rename(columns=_SUMMARY_COLUMN_RENAMES)
    gens = _set_geo_id(gens)
    gens = _add_resource_type(gens)
    for new_col, attrs in _GENERATORS_NEW_ENTRANT_PROPERTY_MAP.items():
        _assert_property_table_attrs(
            table=iasr_tables[attrs["table"]], attrs=attrs, property_name=new_col
        )
        gens = _merge_technology_keyed_property(
            gens,
            iasr_tables[attrs["table"]],
            technology_col=attrs["technology_col"],
            value_col=attrs["value_col"],
            new_col=new_col,
            scale=attrs.get("scale", 1.0),
        )
    return gens[_GENERATOR_IDENTITY_COLUMNS + _GENERATOR_PROPERTY_COLUMNS]


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
    storage = new_entrants_summary[_is_storage_row(new_entrants_summary)].copy()
    storage = storage.rename(columns=_SUMMARY_COLUMN_RENAMES)
    storage = _set_geo_id(storage)
    return storage[_STORAGE_IDENTITY_COLUMNS]


# --- shared helpers ---


def _merge_technology_keyed_property(
    new_entrants: pd.DataFrame,
    property_table: pd.DataFrame,
    technology_col: str,
    value_col: str,
    new_col: str,
    scale: float = 1.0,
) -> pd.DataFrame:
    """Adds ``new_col``: one numeric property value per row, looked up by technology.

    The property table holds a single value per technology (``value_col`` keyed by
    ``technology_col``). To manage typos/capitalisation differences each ``new_entrants``
    technology is fuzzy-matched to the property table's key before lookup. The
    matched values are optionally rescaled (e.g. ``scale=1000`` to convert $/kW → $/MW).
    NaN property values are retained untouched.

    Raises if a technology has no match in the property table.

    I/O Example:
        new_entrants:
            name  technology
            A     Wind
            B     CCGT
            C     Wind

        property_table:
            Technology       Base value
            Wind             2.0
            CCGT             5.0

        technology_col: "Technology"
        value_col: "Base value"
        new_col: "fom"
        scale: 1000.0           # $/kW -> $/MW scaling

        returns (new col "fom"):
            name  technology  fom
            A     Wind        2000.0
            B     CCGT        5000.0
            C     Wind        2000.0
    """
    values_by_technology = pd.to_numeric(
        property_table.set_index(technology_col)[value_col], errors="coerce"
    )
    values_by_technology *= scale
    matched_technology_name = _fuzzy_map_to_allowed_values(
        new_entrants["technology"],
        values_by_technology.index,
        task_desc=f"merging new entrant '{new_col}' by technology",
    )
    new_entrants = new_entrants.copy()
    new_entrants[new_col] = matched_technology_name.map(values_by_technology)
    return new_entrants


def _assert_property_table_attrs(
    table: pd.DataFrame, attrs: dict[str, str | float], property_name: str
) -> None:
    """Asserts that a property table has the required columns and isn't empty.

    Guards against two ways a property table can silently break the downstream
    merge: missing required columns, or has no rows. Failing this assertion flags
    a change in input IASR table structure that needs to be addressed.

    Args:
        table: the property table to validate, e.g.
            ``iasr_tables["fixed_opex_new_entrants"]``.
        attrs: this property's entry from a property map (e.g.
            ``_GENERATORS_NEW_ENTRANT_PROPERTY_MAP["fom"]``), giving the source
            table's name plus its ``technology_col``/``value_col``.
        property_name: the schema column name being merged (e.g. "fom"); used only
            to name the property in the "table is empty" error.

    Raises:
        ValueError: if ``table`` is missing ``technology_col`` and/or ``value_col``,
            or if ``table`` has no rows.
    """
    missing_cols = set([attrs["technology_col"], attrs["value_col"]]) - set(
        table.columns
    )
    if missing_cols:
        raise ValueError(
            f"'{attrs['table']}' table missing required columns: {sorted(missing_cols)}"
        )
    if table.empty:
        raise ValueError(
            f"'{attrs['table']}' table is empty - cannot merge property '{property_name}'"
        )


def _set_geo_id(new_entrants: pd.DataFrame) -> pd.DataFrame:
    """Adds 'geo_id' column to new_entrants containing REZ ID with Sub-region fallback.

    Applies ``_pick_location`` helper to each row of the new_entrants table to
    set their 'geo_id'. Simple wrapper for readability.
    """
    new_entrants["geo_id"] = new_entrants.apply(_pick_location, axis=1)
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
