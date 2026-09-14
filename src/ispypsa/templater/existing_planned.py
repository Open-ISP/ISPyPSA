"""Splits the existing, committed, anticipated and additional (ECAA) technologies
summary into generator and storage tables, and builds the generator identity and
property columns.

Both target tables — see schemas/generators_existing_planned.yaml and
schemas/storage_existing_planned.yaml — are built from the single IASR
existing_committed_anticipated_additional_generator_summary table, which already lists
one row per real generating/storage unit (DUID-level). TODO: finish templating storage.

    existing_committed_anticipated_additional_generator_summary:
        IASR ID / DLT names  Power Station  Technology Type      REZ ID  Sub-region  Fuel type  Fuel cost mapping
        BW01                 Bayswater      Steam Sub Critical   NA      CNSW        Coal       Bayswater
        Q8 Battery - 2h      Q8 Battery     Battery Storage...   Q8      SQ          -          -

    generators_existing_planned (partial):
        name  power_station  technology           geo_id  fuel_type  fuel_price_mapping  capacity
        BW01  Bayswater      Steam Sub Critical    CNSW    Coal       Bayswater           660.0

    storage_existing_planned (partial, identity only):
        name             power_station  technology
        Q8 Battery - 2h  Q8 Battery     Battery Storage (2hrs storage)

Building generators_existing_planned:
    1. Splits the summary's rows into generators and storage — see
       _is_existing_planned_storage_row.
    2. Renames the carried-over spine/identity columns to their schema names, derives
       geo_id (REZ ID with Sub-region fallback — see helpers._set_geo_id) and relabels
       it to ``regional_granularity`` (REZ-located rows stay untouched at every
       granularity).
    3. Merges in unit-level properties (mappings.py). Each generator's
       ``name`` is resolved against a source table's own IASR ID column — exact
       matches first, small typos fuzzy-corrected. Every existing/planned unit is
       expected to resolve to a real row in each of these tables; an unresolved name raises.
    4. Merges in minimum_load: coal's Typical Lowest Band, then gas overlaid
       (see _merge_minimum_load) — the only two technologies with published minimum
       stable levels, so every other row is left NaN (expected).
"""

import logging

import pandas as pd

from ispypsa.templater.helpers import (
    _apply_known_value_replacement,
    _assert_table_valid,
    _fuzzy_match_names,
    _get_property_value_map,
    _group_properties_by_source,
    _is_storage_row,
    _map_geo_id_to_granularity,
    _required_property_columns,
    _set_geo_id,
)
from ispypsa.templater.mappings import _GENERATORS_EXISTING_PLANNED_PROPERTY_MAP

# Source (IASR existing_committed_anticipated_additional_generator_summary) column
# names → schema output column names.
_SUMMARY_COLUMN_RENAMES = {
    "IASR ID / DLT names": "name",
    "Power Station": "power_station",
    "Technology Type": "technology",
    "Fuel type": "fuel_type",
    "Fuel cost mapping": "fuel_price_mapping",
}

# Explicit output order (schema order).
_GENERATOR_COLUMNS = [
    "name",
    "power_station",
    "technology",
    "geo_id",
    "fuel_type",
    "fuel_price_mapping",
    "capacity",
    "vom",
    "heat_rate",
    "commissioning_date",
    "closure_year",
    "minimum_load",
]

_COMMISSIONING_DATE_SCHEMA_FORMAT = "%d/%m/%Y"

# minimum_load property table spec:
_COAL_MINIMUM_LOAD_PROPERTY = dict(
    table="coal_minimum_stable_level",
    key_col="IASR ID",
    # OPINIONATED: see Open-ISP/ISPyPSA#142
    value_col="Minimum Stable Level (MW)_Typical Lowest Band",
)
_GAS_MINIMUM_LOAD_PROPERTY = dict(
    table="gpg_min_stable_level_existing_generators",
    key_col="IASR ID",
    value_col="Min Stable Level (MW)",
)

# The PHES properties table keys Borumba by its short project name; the summary lists
# it under its full project name. TODO: implement as a 'known_value_replacement' when
# storage property merge is implemented.
_BORUMBA_FULL_NAME_MAP = {"Borumba": "QEJP - Borumba"}

# Tumut 3 has a real pump/non-pump unit split that the summary and phes_properties
# tables can't currently be reconciled on by name. _validate_phes_routing tolerates
# this as known unmatched (rather than renaming units to assign as PHES) as part of
# an interim simplification to treat all Tumut 3 units as generators.
# See Open-ISP/ISPyPSA#131 comment thread.
_KNOWN_UNMATCHED_PHES_STATIONS = {"Lower Tumut"}

# Case mismatch between maximum_capacity's IASR ID and the summary's: the 'safe'
# fuzzy-matching threshold (90) would miss (fuzz.ratio("KiataWF1", "KIATAWF1") == 50).
# TODO: rename to use a generic name like IASR_TYPO_FIXES per comment on #143.
_MAXIMUM_CAPACITY_ID_TYPO_FIX = dict(
    table_name="maximum_capacity_existing_committed_anticipated_additional_generators",
    column="IASR ID",
    replacements={"KiataWF1": "KIATAWF1"},
)


# --- public orchestrators ---


def _template_generators_existing_planned(
    iasr_tables: dict[str, pd.DataFrame],
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Templates the existing and planned (ECAA) generators table from the IASR summary.

    Args:
        iasr_tables: IASR tables; uses
            existing_committed_anticipated_additional_generator_summary,
            pumped_hydro_existing_committed_anticipated_additional_properties, plus
            every table named in ``_GENERATORS_EXISTING_PLANNED_PROPERTY_MAP``,
            coal_minimum_stable_level and gpg_min_stable_level_existing_generators.
        regional_granularity: "sub_regions", "nem_regions", or "single_region".
        sub_regional_geography: network_geography templated at "sub_regions"
            granularity; columns used: 'geo_id', 'geo_type', 'region_id'.

    I/O Example (subset of columns):
        iasr_tables:
            existing_committed_anticipated_additional_generator_summary:
                IASR ID / DLT names  Power Station  Technology Type      REZ ID  Sub-region
                BW01                 Bayswater      Steam Sub Critical   NA      CNSW

            maximum_capacity_existing_committed_anticipated_additional_generators:
                IASR ID  Installed capacity (MW)  Commissioning date
                BW01     660.0                    NaN

            ... plus the other property tables (see _GENERATORS_EXISTING_PLANNED_PROPERTY_MAP)

        regional_granularity: "nem_regions"

        sub_regional_geography:
            geo_id  geo_type    region_id
            CNSW    subregion   NSW

        returns:
            name  power_station  geo_id  capacity  commissioning_date
            BW01  Bayswater      NSW     660.0     NaN      # CNSW -> NSW via sub_regional_geography
    """
    logging.info("Creating a template for existing and planned generators")
    summary = iasr_tables["existing_committed_anticipated_additional_generator_summary"]
    phes_properties = iasr_tables[
        "pumped_hydro_existing_committed_anticipated_additional_properties"
    ]

    is_storage = _is_existing_planned_storage_row(summary, phes_properties)
    summary = summary.rename(columns=_SUMMARY_COLUMN_RENAMES)
    generators = summary[~is_storage].copy()
    generators = _set_geo_id(generators)
    generators["geo_id"] = _map_geo_id_to_granularity(
        generators["geo_id"], regional_granularity, sub_regional_geography
    )

    non_generator_names = set(summary.loc[is_storage, "name"])
    generators = _merge_unit_keyed_properties(
        generators,
        _apply_known_value_replacement(iasr_tables, _MAXIMUM_CAPACITY_ID_TYPO_FIX),
        _GENERATORS_EXISTING_PLANNED_PROPERTY_MAP,
        non_generator_names,
    )
    generators = _format_commissioning_date(generators)
    generators = _merge_minimum_load(generators, iasr_tables)
    return generators[_GENERATOR_COLUMNS]


def _template_storage_existing_planned(
    iasr_tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Templates the existing and planned (ECAA) storage table from the IASR summary.

    Currently just the generator/storage split and spine rename — TODO add properties.

    Args:
        iasr_tables: IASR tables; uses
            existing_committed_anticipated_additional_generator_summary and
            pumped_hydro_existing_committed_anticipated_additional_properties.

    I/O Example (spine columns shown; every other summary column passes through
    unchanged):
        existing_committed_anticipated_additional_generator_summary:
            IASR ID / DLT names  Power Station  Technology Type
            BW01                 Bayswater      Steam Sub Critical   # generator, dropped
            Q8 Battery - 2h      Q8 Battery     Battery Storage (2hrs storage)

        returns:
            name             power_station  technology
            Q8 Battery - 2h  Q8 Battery     Battery Storage (2hrs storage)
    """
    logging.info("Creating a template for existing and planned storage")
    summary = iasr_tables["existing_committed_anticipated_additional_generator_summary"]
    phes_properties = iasr_tables[
        "pumped_hydro_existing_committed_anticipated_additional_properties"
    ]
    is_storage = _is_existing_planned_storage_row(summary, phes_properties)
    storage = summary[is_storage].copy()
    return storage.rename(columns=_SUMMARY_COLUMN_RENAMES)


# --- generator/storage split ---


def _is_existing_planned_storage_row(
    summary: pd.DataFrame, phes_properties: pd.DataFrame
) -> pd.Series:
    """Boolean mask selecting storage rows: batteries plus stations with real PHES properties.

    Batteries and technology-labelled PHES (Borumba, Kidston, Snowy 2.0, Phoenix) are
    matched by ``_is_storage_row`` (Technology Type). Two more existing PHES stations
    — Wivenhoe, Shoalhaven — are labelled plain "Hydro" in the summary instead, so a
    station also counts as PHES storage if it appears by name in ``phes_properties``.

    ``_validate_phes_routing`` raises if a ``phes_properties`` station's name
    doesn't exist anywhere in the summary at all, with some known exceptions.

    I/O Example:
        summary:
            Power Station   Technology Type
            Wivenhoe        Hydro                            # PHES by table presence
            QEJP - Borumba  Pumped Hydro (24hrs storage)     # PHES by technology
            Tarong          Steam Sub Critical
            Q8 Battery      Battery Storage (2hrs storage)   # Battery by technology

        phes_properties:
            Power Station
            Wivenhoe
            Borumba

        returns: pd.Series([True, True, False, True])
    """
    is_storage = _is_storage_row(summary) | summary["Power Station"].isin(
        phes_properties["Power Station"]
    )
    _validate_phes_routing(summary, phes_properties)
    return is_storage


def _validate_phes_routing(
    summary: pd.DataFrame, phes_properties: pd.DataFrame
) -> None:
    """Raises if a phes_properties station's name doesn't exist anywhere in the summary.

    Checked after correcting the known Borumba name mismatch
    (``_BORUMBA_FULL_NAME_MAP``) and excusing the one known, documented gap
    (``_KNOWN_UNMATCHED_PHES_STATIONS`` — Tumut 3's "Lower Tumut"). Any other
    unmatched name means a real PHES station would otherwise be silently
    misclassified as a generator.

    I/O Example:
        summary:
            Power Station
            Wivenhoe
            QEJP - Borumba
            Tarong

        phes_properties:
            Power Station
            Wivenhoe        # matches
            Borumba         # matches after _BORUMBA_FULL_NAME_MAP
            Lower Tumut     # excused by _KNOWN_UNMATCHED_PHES_STATIONS

        -> no error

        A "Some New Station" row in phes_properties instead raises:
            ValueError: PHES properties station(s) not found in the summary:
                        ['Some New Station']
    """

    phes_stations = set(
        phes_properties["Power Station"].replace(_BORUMBA_FULL_NAME_MAP)
    )
    known_stations = set(summary["Power Station"]) | _KNOWN_UNMATCHED_PHES_STATIONS
    unmatched = phes_stations - known_stations
    if unmatched:
        raise ValueError(
            f"PHES properties station(s) not found in the summary: {sorted(unmatched)}"
        )


# --- property merges ---


def _merge_unit_keyed_properties(
    generators: pd.DataFrame,
    iasr_tables: dict[str, pd.DataFrame],
    property_map: dict[str, dict],
    exclude_unit_keys: set[str] = set(),
) -> pd.DataFrame:
    """Merges every property in ``property_map`` onto ``generators``, keyed on IASR ID = name.

    Groups properties by source (table, key_col) — see ``_group_properties_by_source``
    — so a table contributing several columns (e.g. maximum_capacity_... feeds
    capacity and commissioning_date) is validated and key-resolved once. Each
    generator's ``name`` is resolved against the property table's own 'key' column (see
    ``_resolve_unit_keys``); the resolved series becomes the key on which property
    values are mapped.

    I/O Example:
        generators:
            name     power_station
            BW01     Bayswater
            HUNTER1  Hunter Power Station

        property_map (abbr.):
            capacity:           table="maximum_capacity_...",
                                key_col="IASR ID",
                                value_col="Installed capacity (MW)"
            commissioning_date: table="maximum_capacity_...",
                                key_col="IASR ID",
                                value_col="Commissioning date"
            heat_rate:          table="heat_rates_...",
                                key_col="IASR ID",
                                value_col="Heat rate (GJ/MWh)"

        iasr_tables:
            maximum_capacity_...:   # one source, two properties -> resolved once
                IASR ID  Installed capacity (MW)  Commissioning date
                BW01     660.0                    NaN
                HUNTER1  375.0                    2025-08-01
                ORANA    100.0                    2027-08-1

            heat_rates_...:
                IASR ID  Heat rate (GJ/MWh)
                BW01     10.05
                HUNTER1  10.93

        exclude_unit_keys: {"ORANA"}    # storage unit name - excluded from the set
                                        # of unit names in maximum_capacity_...
                                        # when fuzzy-matching generator 'name's

        returns (one new column per property_map key):
            name     power_station         capacity  commissioning_date  heat_rate
            BW01     Bayswater             660.0     NaN                 10.05
            HUNTER1  Hunter Power Station  375.0     2025-08-01          10.93
    """
    if generators.empty:
        # Make sure all expected columns still get added
        # Leaving defensive check for empty df ATM -> because it's a subset of a
        # templater input table that **could** be empty after splitting. See comments
        # on #143.
        return generators.assign(
            **{new_col: pd.Series(dtype="object") for new_col in property_map}
        )

    generators = generators.copy()
    for (table_name, key_col), props in _group_properties_by_source(
        property_map
    ).items():
        table = iasr_tables[table_name]
        _assert_table_valid(
            table,
            table_name,
            _required_property_columns(props),
            f"{sorted(props.keys())}",
        )
        resolved_keys = _resolve_unit_keys(
            generators["name"], table[key_col], table_name, exclude_unit_keys
        )
        for new_col, attrs in props.items():
            property_values = _get_property_value_map(table, attrs)
            generators[new_col] = resolved_keys.map(property_values)
    return generators


def _resolve_unit_keys(
    names: pd.Series,
    table_keys: pd.Series,
    table_name: str,
    exclude_unit_keys: set[str] = set(),
) -> pd.Series:
    """Fuzzy-resolves ``names`` to ``table_keys``' strings; raises on any miss.

    Standardises small differences (e.g. a single typo'd character) between a
    unit's ``name`` and the spelling used in a property table's 'key' column.
    ``table_keys`` is a lookup pool, so its order is irrelevant; the result carries
    one value per name, in ``names``' order, spelled as in ``table_keys``. A set
    of unit keys (names) that are known to be out of scope for a given property
    merge can be passed to tighten the fuzzy-matching.

    Raises:
        ValueError: if any unit has no plausible match (above a fuzz ratio threshold
            of 90) — indicating that the table is likely missing a row for the unit.

    I/O Example:
        names:      pd.Series(["HUNTER1", "BW01", "SOMESTATION1"])
        table_keys: pd.Series(["SOMESTATIONl", "BW01", "HUNTER1", "B001"])
        table_name: "heat_rates_..."
        exclude_unit_keys: {"B001"}    # removed from table_keys before fuzzy-matching

        returns:    pd.Series(["HUNTER1", "BW01", "SOMESTATIONl"])
    """
    table_keys_minus_exclusions = set(table_keys) - exclude_unit_keys
    resolved = _fuzzy_match_names(
        names,
        table_keys_minus_exclusions,
        task_desc=f"merging existing/planned properties from '{table_name}'",
        threshold=90,
    )
    unmatched = resolved[~resolved.isin(table_keys_minus_exclusions)]
    if not unmatched.empty:
        raise ValueError(
            f"'{table_name}' table missing a row for generator(s): {sorted(unmatched)}"
        )
    return resolved


def _format_commissioning_date(generators: pd.DataFrame) -> pd.DataFrame:
    """Reformats commissioning_date from the IASR's ISO string to the schema's %d/%m/%Y."""
    generators = generators.copy()
    generators["commissioning_date"] = pd.to_datetime(
        generators["commissioning_date"]
    ).dt.strftime(_COMMISSIONING_DATE_SCHEMA_FORMAT)
    return generators


def _merge_minimum_load(
    generators: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Merges technology-specific minimum_load property for coal and gas generators.

    Coal and gas are the only technologies with published minimum stable levels, so
    every other row is legitimately left NaN (schema nan_fill: 0.0 applies
    downstream). Each is merged separately (see ``_merge_minimum_load_property``).

    I/O Example:
        generators:
            name    technology
            BW01    Steam Sub Critical
            ANGAS1  Reciprocating engine
            Q1G1    Large scale Solar PV

        iasr_tables:
            coal_minimum_stable_level:
                IASR ID  Technology Type     Minimum Stable Level (MW)_Typical Lowest Band
                BW01     Steam Sub Critical  260

            gpg_min_stable_level_existing_generators:
                IASR ID  Technology Type       Min Stable Level (MW)
                ANGAS1   Reciprocating engine  3.0

        returns (adds minimum_load):
            name    technology            minimum_load
            BW01    Steam Sub Critical    260.0
            ANGAS1  Reciprocating engine  3.0
            Q1G1    Large scale Solar PV  NaN   # neither coal nor gas
    """
    if generators.empty:
        return generators.assign(minimum_load=pd.Series(dtype="float64"))

    generators = generators.copy()
    generators["minimum_load"] = float("nan")
    generators = _merge_minimum_load_property(
        generators, iasr_tables, _COAL_MINIMUM_LOAD_PROPERTY
    )
    generators = _merge_minimum_load_property(
        generators, iasr_tables, _GAS_MINIMUM_LOAD_PROPERTY
    )
    return generators


def _merge_minimum_load_property(
    generators: pd.DataFrame,
    iasr_tables: dict[str, pd.DataFrame],
    property_spec: dict,
) -> pd.DataFrame:
    """Assigns minimum_load from one technology-specific table, bounded to its own technologies.

    Only generators whose ``technology`` appears in the table's own 'Technology Type'
    column are fuzzy-matched and have values mapped in the new 'minimum_load' column.

    I/O Example (coal):
        generators:
            name    technology            minimum_load
            BW01    Steam Sub Critical    NaN
            ANGAS1  Reciprocating engine  NaN
            Q1G1    Large scale Solar PV  NaN

        iasr_tables:
            coal_minimum_stable_level:
                IASR ID  Technology Type     Minimum Stable Level (MW)_Typical Lowest Band
                BW01     Steam Sub Critical  260
                ER01     Steam Sub Critical  182

        property_spec:  # _COAL_MINIMUM_LOAD_PROPERTY
            table="coal_minimum_stable_level"
            key_col="IASR ID"
            value_col="Minimum Stable Level (MW)_Typical Lowest Band"

        returns:
            name    technology            minimum_load
            BW01    Steam Sub Critical    260.0
            ANGAS1  Reciprocating engine  NaN   # not a coal technology
            Q1G1    Large scale Solar PV  NaN   # not a coal technology
    """
    table = iasr_tables[property_spec["table"]]
    _assert_table_valid(
        table,
        property_spec["table"],
        _required_property_columns({"minimum_load": property_spec})
        | {"Technology Type"},
        "'minimum_load'",
    )
    is_candidate = generators["technology"].isin(table["Technology Type"])
    resolved_keys = _resolve_unit_keys(
        generators[is_candidate]["name"],
        table[property_spec["key_col"]],
        property_spec["table"],
    )
    values = _get_property_value_map(table, property_spec)
    generators.loc[is_candidate, "minimum_load"] = resolved_keys.map(values)
    return generators
