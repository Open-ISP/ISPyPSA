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
    6. Merges in the two locational cost factors: lcf_build (per geo_id + technology)
       and lcf_om (per geo_id alone) — see _merge_lcf_build and _merge_lcf_om.
    7. Selects the table's schema columns.
    8. Collapses geo_id to the model's regional_granularity — see _collapse_geo_id_to_granularity.
       REZ-located (VRE) rows are left untouched at every granularity.
       Sub-region-located (thermal/storage) rows with the same technology are merged
       into one row per collapsed geo_id, taking the mean of every numeric property.

Note: lcf_build is taken directly from IASR's precomputed technology_specific_lcfs table,
rather than recomputed from the more granular cost-component IASR tables at this
stage.
"""

import logging

import pandas as pd

from ispypsa.templater.geography import _build_geo_region_lookup
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
    _SINGLE_REGION_ID,
    _STORAGE_BATTERY_PROPERTY_MAP,
    _STORAGE_PHES_PROPERTY_MAP,
)

_GENERATOR_IDENTITY_COLUMNS = [
    "name",
    "technology",
    "resource_type",
    "geo_id",
    "fuel_type",
]

# Explicit output order (schema order)
_GENERATOR_PROPERTY_COLUMNS = [
    "fom",
    "vom",
    "lcf_build",
    "lcf_om",
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
    "lcf_build",
    "lcf_om",
    "lifetime_technical",
    "lifetime_economic",
    "degradation_annual",
]

# Columns that define a distinct sub-region-located technology option for granularity
# collapse (see _collapse_geo_id_to_granularity).
_GENERATOR_GEO_ID_GROUP_KEYS = ["technology", "resource_type", "fuel_type"]
_STORAGE_GEO_ID_GROUP_KEYS = ["technology", "fuel_type"]

# Source (IASR new_entrants_summary) column names → schema output column names.
_SUMMARY_COLUMN_RENAMES = {
    "IASR ID / DLT names": "name",
    "Technology Type": "technology",
    "Fuel type": "fuel_type",
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

# Regex extracting the resource-quality code embedded between underscores in a VRE
# IASR ID, e.g. "WFX" in "N10_WFX_Hunter Coast". Derived from the code map, it
# expands to "_(WFX|WFL|SAT|...)_" — one capture group over the known codes
# sorted longest-first so a short code can't shadow a longer one it prefixes.
_RESOURCE_CODE_PATTERN = "_({})_".format(
    "|".join(sorted(_RESOURCE_QUALITY_CODE_TO_TYPE, key=len, reverse=True))
)

# BOTN - Cethana is the one named, site-specific PHES project among the generic
# technologies. These two mappings assist this special case handling through templating.
_BOTN_CETHANA_DETAILS = {
    "name": "BOTN - Cethana",
    "full_name": "BOTN - Cethana - 20h",  # pumped_hydro_new_entrant_properties keys it this way
    "technology": "Pumped Hydro (24hrs storage)",  # its generic tech in new_entrants_summary
}

_PHES_PROPERTY_KEY_RENAMES = {
    _BOTN_CETHANA_DETAILS["full_name"]: _BOTN_CETHANA_DETAILS["name"]
}

# Typo(?) in 'Regional build cost zone' for PHES rows in NSA subregion: see Open-ISP/ISPyPSA#131.
_KNOWN_BUILD_COST_ZONE_TYPOS = {
    ("NSA", "CSA"),  # (geo_id, Regional build cost zone)
}

# Data scale diff for 'BOTN - Cethana' in LCF table: see first comment on Open-ISP/ISPyPSA#131.
_LCF_COLUMNS_IN_PERCENT = ["BOTN - Cethana"]


# --- public orchestrators ---


def _template_generators_new_entrant(
    iasr_tables: dict[str, pd.DataFrame],
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Templates the new entrant generators table from the IASR summary and properties.

    Keeps only generator rows, renames the carried-over summary columns to schema
    names, derives geo_id (REZ ID or sub-region) and resource_type (from the VRE
    resource code in the IASR ID), merges in the per-technology property columns
    (see ``_GENERATORS_NEW_ENTRANT_PROPERTY_MAP``) and the two locational cost factors
    (``lcf_build`` per geo_id+technology, ``lcf_om`` per geo_id), collapses geo_id to
    ``regional_granularity`` (see ``_collapse_geo_id_to_granularity``), and returns
    the identity + property columns.

    Args:
        iasr_tables: IASR tables; uses ``new_entrants_summary`` plus the property
            tables named in ``_GENERATORS_NEW_ENTRANT_PROPERTY_MAP``.
        regional_granularity: "sub_regions", "nem_regions", or "single_region".
        sub_regional_geography: network_geography templated at "sub_regions"
            granularity; columns used: 'geo_id', 'geo_type', 'region_id'.

    I/O Example (identity columns abbreviated to name/technology; regional_granularity="sub_regions"):
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
    _assert_build_cost_zone_matches_geo_id(gens)
    gens = _merge_lcf_build(gens, iasr_tables["technology_specific_lcfs"])
    gens = _merge_lcf_om(gens, iasr_tables["locational_cost_factors"])
    gens = gens[_GENERATOR_IDENTITY_COLUMNS + _GENERATOR_PROPERTY_COLUMNS]
    return _collapse_geo_id_to_granularity(
        gens,
        regional_granularity,
        sub_regional_geography,
        _GENERATOR_GEO_ID_GROUP_KEYS,
        _GENERATOR_PROPERTY_COLUMNS,
    )


def _template_storage_new_entrant(
    iasr_tables: dict[str, pd.DataFrame],
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Templates the new entrant storage table from the IASR summary and properties.

    Keeps only storage rows, renames the carried-over summary columns to schema names
    and derives geo_id (REZ ID or sub-region). Battery and pumped-hydro (PHES) rows draw
    their storage-specific properties from different IASR tables, so each subset is merged
    separately and recombined; the shared properties (see
    ``_COMMON_NEW_ENTRANT_PROPERTY_MAP``) and the two locational cost factors
    (``lcf_build`` per geo_id+technology, ``lcf_om`` per geo_id) are then merged onto the
    combined set, and geo_id is collapsed to ``regional_granularity`` (see
    ``_collapse_geo_id_to_granularity``).

    Args:
        iasr_tables: IASR tables; uses ``new_entrants_summary`` plus the property tables
            named in the storage property maps and ``_COMMON_NEW_ENTRANT_PROPERTY_MAP``.
        regional_granularity: "sub_regions", "nem_regions", or "single_region".
        sub_regional_geography: network_geography templated at "sub_regions"
            granularity; columns used: 'geo_id', 'geo_type', 'region_id'.

    I/O Example (identity columns abbreviated to name/technology; regional_granularity="sub_regions"):
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
    batteries = _merge_properties(
        storage[_is_battery_row(storage, col_to_check="technology")],
        iasr_tables,
        _STORAGE_BATTERY_PROPERTY_MAP,
    )
    phes = _merge_phes_properties(
        storage[_is_pumped_hydro_row(storage, col_to_check="technology")], iasr_tables
    )
    storage = pd.concat([batteries, phes], ignore_index=True)
    storage = _merge_properties(storage, iasr_tables, _COMMON_NEW_ENTRANT_PROPERTY_MAP)
    _assert_build_cost_zone_matches_geo_id(storage)
    storage = _merge_lcf_build(storage, iasr_tables["technology_specific_lcfs"])
    storage = _merge_lcf_om(storage, iasr_tables["locational_cost_factors"])
    storage = storage[_STORAGE_IDENTITY_COLUMNS + _STORAGE_PROPERTY_COLUMNS]
    return _collapse_geo_id_to_granularity(
        storage,
        regional_granularity,
        sub_regional_geography,
        _STORAGE_GEO_ID_GROUP_KEYS,
        _STORAGE_PROPERTY_COLUMNS,
    )


# --- shared helpers ---


def _merge_properties(
    new_entrants: pd.DataFrame,
    iasr_tables: dict[str, pd.DataFrame],
    property_map: dict[str, dict],
) -> pd.DataFrame:
    """Merges every property in ``property_map`` onto ``new_entrants``.

    Groups properties by their source (table, technology_col) — see
    ``_group_by_source_key`` — so a table that contributes several properties (e.g.
    ``battery_properties`` feeds six) is validated and fuzzy-matched against
    ``new_entrants``' 'technology' once per property map.

    I/O Example (property_map = _STORAGE_BATTERY_PROPERTY_MAP, abbreviated):
        new_entrants:
            name             technology
            NQ Battery - 2h  Battery Storage (2hrs storage)

        returns (adds one column per map key):
            name             technology                      storage_hours  efficiency_charge  ...
            NQ Battery - 2h  Battery Storage (2hrs storage)   2.0            92.0               ...
    """
    new_entrants = new_entrants.copy()
    for (table_name, technology_col), props in _group_by_source_key(
        property_map
    ).items():
        table = iasr_tables[table_name]
        _assert_table_valid(
            table,
            table_name,
            _required_property_columns(props),
            f"{sorted(props.keys())}",
        )
        matched_technology = _fuzzy_map_to_allowed_values(
            new_entrants["technology"],
            table[technology_col],
            task_desc=f"merging new entrant properties from '{table_name}'",
        )
        for new_col, attrs in props.items():
            property_values = _get_property_value_map(table, attrs)
            new_entrants[new_col] = matched_technology.map(property_values)
    return new_entrants


def _group_by_source_key(property_map: dict[str, dict]) -> dict[tuple[str, str], dict]:
    """Groups a property map's entries by their source (table, technology_col).

    I/O Example:
        property_map:
            storage_hours:
                {table: battery_properties, technology_col: Technology, value_col: Energy capacity_Hours}
            efficiency_charge:
                {table: battery_properties, technology_col: Technology, value_col: Charge efficiency_%}
            lifetime_technical:
                {table: lead_time_and_project_life, technology_col: Technology, value_col: Technical life (years)}

        returns:
            (battery_properties, Technology): {
                storage_hours: { ... },
                efficiency_charge: { ... },
            }
            (lead_time_and_project_life, Technology): {
                lifetime_technical: { ... },
            }
            where { ... } indicates contents remain unchanged from inputs.
    """
    groups = {}
    for property_name, attrs in property_map.items():
        source_key = (attrs["table"], attrs["technology_col"])
        groups.setdefault(source_key, {})[property_name] = attrs
    return groups


def _required_property_columns(props: dict[str, dict]) -> set[str]:
    """Returns every ``value_col``/``technology_col`` named across a source's properties.

    I/O Example:
        props:
            fom: {table: fixed_opex_new_entrants, technology_col: Technology, value_col: Base value}
            vom: {table: variable_opex_new_entrants, technology_col: Generator, value_col: Base value}

        returns:
            {"Technology", "Generator", "Base value"}
    """
    return {d[col] for col in ["value_col", "technology_col"] for d in props.values()}


def _get_property_value_map(
    table: pd.DataFrame, attrs: dict[str, str | float]
) -> pd.Series:
    """Returns one property's value, keyed by technology and scaled.

    Raises:
        ValueError: if ``value_col`` contains anything ``pd.to_numeric`` can't parse,
            e.g. a stray typo in the IASR table.

    I/O Example:
        table:
            Technology  Base value
            Wind        2.0
            CCGT        5.0

        attrs: {technology_col: Technology, value_col: Base value, scale: 1000.0}

        returns (indexed by Technology):
            Wind    2000.0
            CCGT    5000.0
    """
    value_map = pd.to_numeric(
        table.set_index(attrs["technology_col"])[attrs["value_col"]], errors="raise"
    )
    value_map *= float(attrs.get("scale", 1.0))
    return value_map


def _assert_table_valid(
    table: pd.DataFrame, table_name: str, required_cols: set[str], merge_desc: str
) -> None:
    """Asserts a source table has every required column and isn't empty.

    Shared precondition check for every IASR table merged in this module — guards
    against two silent-failure modes: a missing column producing a KeyError, and
    an empty table merges to an all-NaN column with no warning.

    Args:
        table: the source table to validate, e.g. ``iasr_tables["battery_properties"]``.
        table_name: ``table``'s IASR table name, used to name it in error messages.
        required_cols: every column the downstream merge reads from ``table``.
        merge_desc: short description of what would be merged, named in the
            empty-table error, e.g. ``"properties '['fom']'"`` or ``"'lcf_build'"``.

    Raises:
        ValueError: if any of ``required_cols`` is missing from ``table``, or if
            ``table`` has no rows.

    I/O Example:
        table:
            Technology  Base value  Extra Column
            Wind        2.0         unused_info

        table_name: "fixed_opex_new_entrants"
        required_cols: {"Technology", "Base value"}
        merge_desc: "properties '['fom']'"

        # No ValueError raised: table has rows, both required columns present.
    """
    missing_cols = required_cols - set(table.columns)
    if missing_cols:
        raise ValueError(
            f"'{table_name}' table missing required columns: {sorted(missing_cols)}"
        )
    if table.empty:
        raise ValueError(f"'{table_name}' table is empty - cannot merge {merge_desc}")


def _set_geo_id(new_entrants: pd.DataFrame) -> pd.DataFrame:
    """Adds 'geo_id' column to new_entrants containing REZ ID with Sub-region fallback.

    Applies ``_pick_location`` helper to each row of the new_entrants table to
    set their 'geo_id'. Simple wrapper for readability.
    """
    new_entrants["geo_id"] = new_entrants.apply(_pick_location, axis=1)
    return new_entrants


# --- regional granularity collapse ---


def _collapse_geo_id_to_granularity(
    new_entrants: pd.DataFrame,
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
    group_key_columns: list[str],
    value_columns: list[str],
) -> pd.DataFrame:
    """Aggregates subregional options sharing ``group_key_columns`` to ``regional_granularity``.

    No-op at "sub_regions" (already the finest granularity). Otherwise:
        1. Splits ``new_entrants`` into REZ rows (left untouched) and subregion rows.
        2. Subregion rows get grouped by ``group_key_columns`` + the re-keyed geo_id and
            averaged over ``value_columns``.
        3. Aggregated rows' 'name' set to "{geo_id} {technology}" (except BOTN - see
            ``_name_collapsed_rows``).
        4. Returns concatted REZ rows and aggregated rows.

    Args:
        new_entrants: identity + property columns, one row per subregion/REZ
            technology option.
        regional_granularity: "sub_regions", "nem_regions", or "single_region".
        sub_regional_geography: network_geography templated at "sub_regions"
            granularity; columns used: 'geo_id', 'geo_type', 'region_id'.
        group_key_columns: identity columns (besides geo_id and name) that define a
            distinct technology option.
        value_columns: every property column to average through the merge.

    I/O Example (regional_granularity="nem_regions"; two sub-regions in one region):
        new_entrants:
            name             technology       geo_id  lcf_build
            CNSW OCGT Small  OCGT (small GT)  CNSW    104.0
            SNW OCGT Small   OCGT (small GT)  SNW     100.0

        sub_regional_geography:
            geo_id  geo_type   region_id
            CNSW    subregion  NSW
            SNW     subregion  NSW

        returns:
            name                 technology       geo_id  lcf_build
            NSW OCGT (small GT)  OCGT (small GT)  NSW     102.0  # mean(104, 100)
    """
    if regional_granularity == "sub_regions":
        return new_entrants

    is_subregion = _is_subregion_geo_id(new_entrants["geo_id"], sub_regional_geography)
    unchanged = new_entrants[~is_subregion].copy()
    to_collapse = new_entrants[is_subregion].copy()
    if to_collapse.empty:
        return new_entrants

    to_collapse["geo_id"] = _map_geo_id_to_granularity(
        to_collapse["geo_id"], regional_granularity, sub_regional_geography
    )
    collapsed = _aggregate_by_geo_id(to_collapse, group_key_columns, value_columns)
    collapsed = _name_collapsed_rows(collapsed)

    return pd.concat([unchanged, collapsed], ignore_index=True)[new_entrants.columns]


# NOTE: maybe move to helpers.py in future?
def _is_subregion_geo_id(
    geo_id: pd.Series, sub_regional_geography: pd.DataFrame
) -> pd.Series:
    """Boolean mask of ``geo_id`` values that are sub-region-located (not REZ)."""
    geo_type_by_geo_id = sub_regional_geography.set_index("geo_id")["geo_type"]
    return geo_id.map(geo_type_by_geo_id) == "subregion"


def _aggregate_by_geo_id(
    new_entrants: pd.DataFrame,
    group_key_columns: list[str],
    value_columns: list[str],
) -> pd.DataFrame:
    """Groups by ``group_key_columns`` + 'geo_id' and averages ``value_columns``."""
    # 'dropna=False' set to keep thermal generator rows (w/ NaN 'resource_type')
    return new_entrants.groupby(
        group_key_columns + ["geo_id"], dropna=False, as_index=False
    )[value_columns].mean()


def _map_geo_id_to_granularity(
    geo_id: pd.Series, regional_granularity: str, sub_regional_geography: pd.DataFrame
) -> pd.Series:
    """Maps sub-region geo_ids to their region_id ("nem_regions") or "NEM" ("single_region")."""
    if regional_granularity == "single_region":
        return pd.Series(_SINGLE_REGION_ID, index=geo_id.index)
    return geo_id.map(_build_geo_region_lookup(sub_regional_geography))


def _name_collapsed_rows(collapsed: pd.DataFrame) -> pd.DataFrame:
    """Sets 'name' on merged rows to "{geo_id} {technology}".

    The lone documented exception is BOTN - Cethana (see ``_BOTN_CETHANA_DETAILS``):
    a named, site-specific project rather than a generic technology archetype, which
    keeps its original 'name'.

    I/O Example:
        collapsed:
            technology       geo_id
            OCGT (small GT)  NSW
            BOTN - Cethana   TAS

        returns:
            technology       geo_id  name
            OCGT (small GT)  NSW     NSW OCGT (small GT)
            BOTN - Cethana   TAS     BOTN - Cethana - 20h  # original name kept
    """
    fresh_name = collapsed["geo_id"] + " " + collapsed["technology"]
    is_botn = collapsed["technology"] == _BOTN_CETHANA_DETAILS["name"]
    collapsed["name"] = fresh_name.mask(is_botn, _BOTN_CETHANA_DETAILS["full_name"])
    return collapsed


# --- locational cost factor (LCF) helpers ---


def _assert_build_cost_zone_matches_geo_id(new_entrants: pd.DataFrame) -> None:
    """Asserts the LCF lookup key (geo_id) matches the IASR's 'Regional build cost zone'.

    LCFs are keyed on geo_id here, which equals each unit's cost zone. v7.5 (and 7.8) breaks
    that rule for three NSA pumped-hydro rows, mislabelling them with the CSA cost zone
    (``_KNOWN_BUILD_COST_ZONE_TYPOS``); we accept that known typo and key on geo_id (NSA)
    regardless. Any *other* divergence is unexpected — possibly a real cost-zone split
    rather than a typo — so this function raises if any other diffs are seen.

    Raises:
        ValueError: if geo_id and 'Regional build cost zone' diverge for any
            (geo_id, cost zone) pair not in ``_KNOWN_BUILD_COST_ZONE_TYPOS``.
    """
    divergent = new_entrants[
        new_entrants["geo_id"] != new_entrants["Regional build cost zone"]
    ]
    unexpected = (
        set(zip(divergent["geo_id"], divergent["Regional build cost zone"]))
        - _KNOWN_BUILD_COST_ZONE_TYPOS
    )
    if unexpected:
        raise ValueError(
            "Unexpected divergence between geo_id and 'Regional build cost zone' in "
            f"new_entrants_summary: {sorted(unexpected, key=str)}."
        )


def _merge_lcf_build(
    new_entrants: pd.DataFrame, technology_specific_lcfs: pd.DataFrame
) -> pd.DataFrame:
    """Merges the build/connection locational cost factor (``lcf_build``, %) per unit.

    Looks up each unit's precomputed build/connection locational cost factor (LCF) from
    ``technology_specific_lcfs``, merged on (geo_id, technology). Units with no entry for
    their (geo_id, technology) pair get NaN (a default is applied downstream — see the
    schema ``nan_fill``).

    I/O Example:
        new_entrants (BOTN 'technology' already overridden):
            name                  technology                    geo_id
            Q1_WH_Far North QLD   Wind                          Q1
            NQ OCGT Small         OCGT (small GT)               NQ
            BOTN - Cethana - 20h  BOTN - Cethana                TAS

        technology_specific_lcfs:
            Cost zone / REZ ID  REZ name / Description  Wind            OCGT (small GT)  BOTN - Cethana
            Q1                  Far North QLD           1.0860          Not Applicable   Not Applicable
            NQ                  Subregional Ref Node    Not Applicable  1.0801           Not Applicable
            TAS                 Subregional Ref Node    1.0325          Not Applicable   100

        returns (adds lcf_build):
            name                  ...  lcf_build
            Q1_WH_Far North QLD   ...  108.60
            NQ OCGT Small         ...  108.01
            BOTN - Cethana - 20h  ...  100.0
    """
    lcf_by_geo_id_and_technology = _reshape_technology_specific_lcfs(
        technology_specific_lcfs
    )
    new_entrants = new_entrants.copy()
    new_entrants["lcf_technology"] = _fuzzy_map_to_allowed_values(
        new_entrants["technology"],
        lcf_by_geo_id_and_technology["lcf_technology"].unique(),
        task_desc="merging new entrant 'lcf_build' by technology",
    )
    return new_entrants.merge(
        lcf_by_geo_id_and_technology, how="left", on=["geo_id", "lcf_technology"]
    ).drop(columns="lcf_technology")


def _merge_lcf_om(
    new_entrants: pd.DataFrame, locational_cost_factors: pd.DataFrame
) -> pd.DataFrame:
    """Merges the O&M locational cost factor (``lcf_om``, %) per unit, looked up by geo_id.

    The O&M LCF is a single per-zone factor (technology-independent), already a percentage,
    held in the ``O&M costs 3`` column of ``locational_cost_factors``. Units whose geo_id
    has no entry get NaN (a 100% default is applied downstream).

    Raises:
        ValueError: if 'O&M costs 3' column contains anything ``pd.to_numeric`` can't parse,
            e.g. a stray typo in the IASR table.

    I/O Example:
        new_entrants:
            name                 geo_id
            Q1_WH_Far North QLD  Q1
            NQ OCGT Small        NQ

        locational_cost_factors (relevant cols):
            Cost zone / REZ ID  O&M costs 3
            Q1                  122.27
            NQ                  114.997

        returns (adds lcf_om):
            name                 geo_id  lcf_om
            Q1_WH_Far North QLD  Q1      122.27
            NQ OCGT Small        NQ      114.997
    """
    # "Cost zone / REZ ID" and "O&M costs 3": literal v7.5 IASR workbook column names.
    zone_col = "Cost zone / REZ ID"
    om_col = "O&M costs 3"
    _assert_table_valid(
        locational_cost_factors,
        "locational_cost_factors",
        {zone_col, om_col},
        "'lcf_om'",
    )
    om_by_geo_id = pd.to_numeric(
        locational_cost_factors.set_index(zone_col)[om_col], errors="raise"
    )
    new_entrants = new_entrants.copy()
    new_entrants["lcf_om"] = new_entrants["geo_id"].map(om_by_geo_id)
    return new_entrants


def _reshape_technology_specific_lcfs(
    technology_specific_lcfs: pd.DataFrame,
) -> pd.DataFrame:
    """Reshapes the wide ``technology_specific_lcfs`` table to long (geo_id, technology, %).

    Each source row is a cost zone (a REZ ID or sub-region — i.e. a geo_id) and each column
    after the description is a technology's precomputed LCF. Factors are converted to
    percentages (×100), except the bespoke columns already published as percentages
    (``_LCF_COLUMNS_IN_PERCENT``). "Not Applicable"/blank cells become NaN and are dropped,
    so only fully defined (geo_id, technology) pairs remain.

    I/O Example:
        technology_specific_lcfs:
            Cost zone / REZ ID  REZ name / Description  Wind    BOTN - Cethana
            Q1                  Far North QLD           1.0860  Not Applicable
            TAS                 Subregional Ref Node    1.0325  100

        returns:
            geo_id  lcf_technology  lcf_build
            Q1      Wind            108.60       # factor ×100
            TAS     Wind            103.25
            TAS     BOTN - Cethana  100.0        # already % -> left unscaled
    """
    # "Cost zone / REZ ID" / "REZ name / Description": literal v7.5 IASR column names.
    zone_col = "Cost zone / REZ ID"
    description_col = "REZ name / Description"
    _assert_table_valid(
        technology_specific_lcfs,
        "technology_specific_lcfs",
        {zone_col, description_col},
        "'lcf_build'",
    )
    technology_cols = technology_specific_lcfs.columns.difference(
        [zone_col, description_col]
    )
    long = technology_specific_lcfs.melt(
        id_vars=zone_col,
        value_vars=list(technology_cols),
        var_name="lcf_technology",
        value_name="lcf_build",
    ).rename(columns={zone_col: "geo_id"})
    long["lcf_build"] = pd.to_numeric(
        long["lcf_build"].replace("Not Applicable", pd.NA), errors="raise"
    )
    in_factor_form = ~long["lcf_technology"].isin(_LCF_COLUMNS_IN_PERCENT)
    long.loc[in_factor_form, "lcf_build"] *= 100
    return long.dropna(subset="lcf_build").reset_index(drop=True)


# --- storage-specific helpers ---


def _merge_phes_properties(
    phes: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Merges the pumped-hydro storage properties onto the PHES rows.

    BOTN - Cethana's 'technology' is first overridden to its own name so it draws its own
    published property rows rather than the generic PHES archetype's (see
    ``_override_botn_technology``). The pumped-hydro table is the lone table that keys BOTN
    by its full spelling, so its key is normalised to the bare name (see
    ``_normalise_phes_botn_key``) before a plain technology-keyed merge. The table gives
    storage_hours and a single round-trip efficiency directly; charge/discharge efficiency
    are then derived from it (see ``_derive_phes_symmetric_efficiency``). The round-trip
    column is dropped by the orchestrator's final select.
    """
    phes = phes.copy()
    phes["technology"] = _override_botn_technology(phes)
    phes = _merge_properties(
        phes, _normalise_phes_botn_key(iasr_tables), _STORAGE_PHES_PROPERTY_MAP
    )
    phes = _derive_phes_symmetric_efficiency(phes)
    return phes


def _normalise_phes_botn_key(
    iasr_tables: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Returns ``iasr_tables`` with the pumped-hydro table's BOTN key set to the bare name.

    Renames the lone full-spelling BOTN key (see ``_PHES_PROPERTY_KEY_RENAMES``) in the
    pumped-hydro table so it matches BOTN's overridden 'technology'. Returns a shallow copy
    of the dict with only that table replaced — the shared ``iasr_tables`` is left untouched.
    """
    table_name = "pumped_hydro_new_entrant_properties"
    key_col = _STORAGE_PHES_PROPERTY_MAP["storage_hours"]["technology_col"]
    normalised = iasr_tables[table_name].replace({key_col: _PHES_PROPERTY_KEY_RENAMES})
    return {**iasr_tables, table_name: normalised}


def _override_botn_technology(phes: pd.DataFrame) -> pd.Series:
    """Returns PHES 'technology' with named project 'BOTN - Cethana' set to its own name.

    This is an **opinionated** manual override: the property tables key the named project
    'BOTN - Cethana' by its own name rather than a generic technology archetype, and the
    schema's canonical technology (from costs_new_entrant_build) is likewise the bare name.
    Overriding here lets BOTN merge its own published rows everywhere downstream. Checks the
    incoming 'technology' is the expected archetype first (see
    ``_assert_botn_technology_expected``).
    """
    _assert_botn_technology_expected(phes)
    return phes["technology"].mask(_is_botn_row(phes), _BOTN_CETHANA_DETAILS["name"])


def _is_botn_row(df: pd.DataFrame) -> pd.Series:
    """Boolean mask of the BOTN - Cethana rows, matched by name (which carries the
    '- 20h' suffix, so a literal substring test rather than an exact match)."""
    return df["name"].str.contains(_BOTN_CETHANA_DETAILS["name"], regex=False)


def _assert_botn_technology_expected(phes: pd.DataFrame) -> None:
    """Checks BOTN - Cethana's incoming 'technology' is the expected archetype before override.

    Any BOTN row whose 'technology' isn't the expected
    ``_BOTN_CETHANA_DETAILS["technology"]`` signals a change in new_entrants_summary that
    this override would silently mishandle, so raise. When BOTN is absent (e.g. a scenario
    with no PHES) there is nothing to check and this passes.
    """
    expected = _BOTN_CETHANA_DETAILS["technology"]
    unexpected = set(phes[_is_botn_row(phes)]["technology"].unique()) - {expected}
    if unexpected:
        raise ValueError(
            f"'BOTN - Cethana' technology should be '{expected}': "
            f"got {sorted(unexpected, key=str)} in 'new_entrants_summary' table."
        )


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
