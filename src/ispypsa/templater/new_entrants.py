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
       technology, via _merge_properties (see the property merge maps in mappings.py,
       e.g. _GENERATORS_NEW_ENTRANT_PROPERTY_MAP). Generators and storage share a common
       set of these (_COMMON_NEW_ENTRANT_PROPERTY_MAP). Storage additionally splits
       into battery and pumped-hydro (PHES) rows, which take their storage-specific
       properties from different IASR tables, then recombines them before merging
       the common properties.
    6. Selects the table's schema columns.
"""

import logging

import pandas as pd

from ispypsa.templater.helpers import (
    _fuzzy_map_to_allowed_values,
    _is_battery_row,
    _is_pumped_hydro_row,
    _is_storage_row,
    _pick_location,
)
from ispypsa.templater.mappings import (
    _COMMON_NEW_ENTRANT_PROPERTY_MAP,
    _GENERATORS_NEW_ENTRANT_PROPERTY_MAP,
    _STORAGE_BATTERY_PROPERTY_MAP,
    _STORAGE_PHES_PROPERTY_MAP,
)

_GENERATOR_IDENTITY_COLUMNS = [
    "name",
    "technology",
    "resource_type",
    "geo_id",
    "fuel_type",
    "fuel_price_mapping",
]

# Explicit output order (schema order)
_GENERATOR_PROPERTY_COLUMNS = [
    "fom",
    "vom",
    "lifetime_technical",
    "lifetime_economic",
    "heat_rate",
    "minimum_stable_level",
]

_STORAGE_IDENTITY_COLUMNS = [
    "name",
    "technology",
    "geo_id",
    "fuel_type",
]

# Explicit output order (schema order)
_STORAGE_PROPERTY_COLUMNS = [
    "storage_hours",
    "fom",
    "efficiency_charge",
    "efficiency_discharge",
    "soc_max",
    "soc_min",
    "minimum_stable_level",
    "lifetime_technical",
    "lifetime_economic",
    "degradation_annual",
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

# BOTN - Cethana is the one named, site-specific PHES project among the generic
# technologies. This mapping assists this special case handling through templating.
_BOTN_CETHANA_DETAILS = {
    # common prefix of the two spellings used for this project:
    # 'BOTN - Cethana - 20h' and 'BOTN - Cethana'
    "name": "BOTN - Cethana",
    "technology": "Pumped Hydro (24hrs storage)",
}


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
    new_entrants_summary = iasr_tables["new_entrants_summary"]
    gens = new_entrants_summary[~_is_storage_row(new_entrants_summary)].copy()
    gens = gens.rename(columns=_SUMMARY_COLUMN_RENAMES)
    gens = _set_geo_id(gens)
    gens = _add_resource_type(gens)
    gens = _merge_properties(gens, iasr_tables, _GENERATORS_NEW_ENTRANT_PROPERTY_MAP)
    return gens[_GENERATOR_IDENTITY_COLUMNS + _GENERATOR_PROPERTY_COLUMNS]


# NOTE: partial scope intentional - lcf_* columns added in a later PR!
def _template_storage_new_entrant(
    iasr_tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Templates the new entrant storage table from the IASR summary and properties.

    Keeps only storage rows, renames the carried-over summary columns to schema names
    and derives geo_id (REZ ID or sub-region). Battery and pumped-hydro (PHES) rows draw
    their storage-specific properties from different IASR tables, so each subset is merged
    separately and recombined; the shared properties (see
    ``_COMMON_NEW_ENTRANT_PROPERTY_MAP``) are then merged onto the combined set.

    Args:
        iasr_tables: IASR tables; uses ``new_entrants_summary`` plus the property tables
            named in the storage property maps and ``_COMMON_NEW_ENTRANT_PROPERTY_MAP``.

    I/O Example (identity columns abbreviated to name/technology):
        new_entrants_summary:
            IASR ID / DLT names  Technology Type                ...
            NQ Battery - 2h      Battery Storage (2hrs storage)  ...
            NQ Pumped Hydro-10h  Pumped Hydro (10hrs storage)    ...
            SQ CCGT              CCGT                            ...   # generator, dropped

        returns (property columns shown; identity columns also present):
            name                 technology                      storage_hours  efficiency_charge  ...
            NQ Battery - 2h      Battery Storage (2hrs storage)   2.0           92.0               ...
            NQ Pumped Hydro-10h  Pumped Hydro (10hrs storage)     10.0          87.2               ...
    """
    logging.info("Creating a template for new entrant storage")
    new_entrants_summary = iasr_tables["new_entrants_summary"]
    storage = new_entrants_summary[_is_storage_row(new_entrants_summary)].copy()
    storage = storage.rename(columns=_SUMMARY_COLUMN_RENAMES)
    storage = _set_geo_id(storage)
    batteries = _merge_battery_properties(
        storage[_is_battery_row(storage, col_to_check="technology")], iasr_tables
    )
    phes = _merge_phes_properties(
        storage[_is_pumped_hydro_row(storage, col_to_check="technology")], iasr_tables
    )
    storage = pd.concat([batteries, phes], ignore_index=True)
    _assert_botn_cethana_values_match_technology(iasr_tables)
    storage = _merge_properties(storage, iasr_tables, _COMMON_NEW_ENTRANT_PROPERTY_MAP)
    return storage[_STORAGE_IDENTITY_COLUMNS + _STORAGE_PROPERTY_COLUMNS]


# --- shared helpers ---


def _merge_properties(
    new_entrants: pd.DataFrame,
    iasr_tables: dict[str, pd.DataFrame],
    property_map: dict[str, dict],
    key_col: str = "technology",
) -> pd.DataFrame:
    """Merges every property in ``property_map`` onto ``new_entrants``.

    For each (new column, attrs) entry: validates the source IASR table, then looks up
    one numeric value per row via ``_merge_technology_keyed_property``. ``key_col`` names
    the column whose value is matched against each table's technology key (default
    "technology"; PHES passes a name-or-technology key instead — see
    ``_phes_lookup_key``).

    I/O Example (property_map = _STORAGE_BATTERY_PROPERTY_MAP, abbreviated):
        new_entrants:
            name             technology
            NQ Battery - 2h  Battery Storage (2hrs storage)

        returns (adds one column per map key):
            name             technology                     storage_hours  efficiency_charge  ...
            NQ Battery - 2h  Battery Storage (2hrs storage)  2.0           92.0               ...
    """
    for new_col, attrs in property_map.items():
        _assert_property_table_attrs(
            table=iasr_tables[attrs["table"]], attrs=attrs, property_name=new_col
        )
        new_entrants = _merge_technology_keyed_property(
            new_entrants,
            iasr_tables[attrs["table"]],
            technology_col=attrs["technology_col"],
            value_col=attrs["value_col"],
            new_col=new_col,
            scale=attrs.get("scale", 1.0),
            key_col=key_col,
        )
    return new_entrants


def _merge_technology_keyed_property(
    new_entrants: pd.DataFrame,
    property_table: pd.DataFrame,
    technology_col: str,
    value_col: str,
    new_col: str,
    scale: float = 1.0,
    key_col: str = "technology",
) -> pd.DataFrame:
    """Adds ``new_col``: one numeric property value per row, looked up by technology.

    The property table holds a single value per technology (``value_col`` keyed by
    ``technology_col``). Each ``new_entrants`` row is matched on ``key_col`` (its
    technology by default) — fuzzy-matched to the property table's key to manage
    typos/capitalisation differences before lookup. The matched values are optionally
    rescaled (e.g. ``scale=1000`` to convert $/kW → $/MW). NaN property values are
    retained untouched.

    Raises if a key value has no match in the property table.

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
        key_col: "technology"

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
        new_entrants[key_col],
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


# --- storage-specific helpers ---


def _merge_battery_properties(
    batteries: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Merges the battery-only storage properties onto the battery rows.

    Thin wrapper over ``_merge_properties`` for ``_STORAGE_BATTERY_PROPERTY_MAP`` — every
    battery property (storage_hours, charge/discharge efficiency, soc_max/min, annual
    degradation) is looked up by technology from the ``battery_properties`` table.
    """
    return _merge_properties(batteries, iasr_tables, _STORAGE_BATTERY_PROPERTY_MAP)


def _merge_phes_properties(
    phes: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Merges the pumped-hydro storage properties onto the PHES rows.

    PHES properties are keyed by name-or-technology (see ``_phes_lookup_key``). The table
    gives storage_hours and a single round-trip efficiency directly; charge/discharge
    efficiency are then derived from it (see ``_derive_phes_symmetric_efficiency``). The
    temporary key and round-trip columns are dropped by the orchestrator's final select.
    """
    phes = phes.copy()
    phes["phes_key"] = _phes_lookup_key(phes)
    phes = _merge_properties(
        phes, iasr_tables, _STORAGE_PHES_PROPERTY_MAP, key_col="phes_key"
    )
    phes = _derive_phes_symmetric_efficiency(phes)
    return phes


def _phes_lookup_key(phes):
    """Each PHES row's key into the pumped-hydro table: its 'name' for declared named
    projects, its 'technology' otherwise.

    Currently: only 'BOTN - Cethana' is a declared named project; sometimes
    spelled as 'BOTN - Cethana - 20h'.
    """
    is_named_project = phes["name"].str.startswith(_BOTN_CETHANA_DETAILS["name"])
    return phes["name"].where(is_named_project, phes["technology"])


def _derive_phes_symmetric_efficiency(phes: pd.DataFrame) -> pd.DataFrame:
    """Splits the round-trip 'round_trip_efficiency' (%) into charge and discharge legs.

    The IASR PHES table gives only a single round-trip efficiency. Assuming symmetric
    legs, each one-way efficiency is its square root, so e.g. a 76% round trip becomes
    ~87.2% charge and ~87.2% discharge (sqrt(0.76) ≈ 0.872).

    I/O Example:
        phes:
            name                 round_trip_efficiency
            NQ Pumped Hydro-10h  76.0

        returns (adds the two efficiency columns):
            name                 round_trip_efficiency  efficiency_charge  efficiency_discharge
            NQ Pumped Hydro-10h  76.0                   87.18              87.18
    """
    phes = phes.copy()
    one_way_efficiency = (phes["round_trip_efficiency"] / 100) ** 0.5 * 100
    phes["efficiency_charge"] = one_way_efficiency
    phes["efficiency_discharge"] = one_way_efficiency
    return phes


def _assert_botn_cethana_values_match_technology(iasr_tables):
    """Guard the assumption that BOTN - Cethana's value matches its `technology`'s
    (Pumped Hydro (24hrs storage)) in each common property table.

    BOTN is keyed by name in these tables but merged via its technology (see the common
    merge in _template_storage_new_entrant), so the two must agree. If a table diverges
    them, raise — BOTN then needs explicit name-keyed handling rather than silently taking
    the technology value. Both rows must be present: their absence is itself an unexpected
    change in the IASR table structure, so a missing-key lookup is left to raise. The
    common tables key BOTN by the bare 'BOTN - Cethana'.
    """
    name, tech = _BOTN_CETHANA_DETAILS["name"], _BOTN_CETHANA_DETAILS["technology"]
    for attrs in _COMMON_NEW_ENTRANT_PROPERTY_MAP.values():
        values = iasr_tables[attrs["table"]].set_index(attrs["technology_col"])[
            attrs["value_col"]
        ]
        botn_value = pd.to_numeric(values[name], errors="coerce")
        tech_value = pd.to_numeric(values[tech], errors="coerce")
        if pd.notna(botn_value) and pd.notna(tech_value) and botn_value != tech_value:
            raise ValueError(
                f"'{name}' diverges from its technology '{tech}' for "
                f"'{attrs['value_col']}' in '{attrs['table']}'."
            )


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
