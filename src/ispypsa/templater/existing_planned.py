"""Splits the existing, committed, anticipated and additional (ECAA) technologies
summary into generator and storage tables, and builds the generator identity and
property columns.

Both target tables — see schemas/generators_existing_planned.yaml and
schemas/storage_existing_planned.yaml — are built from the single IASR
existing_committed_anticipated_additional_generator_summary table, which already lists
one row per real generating/storage unit (DUID-level).

    existing_committed_anticipated_additional_generator_summary:
        IASR ID / DLT names  Power Station  Technology Type      REZ ID  Sub-region  Fuel type  Fuel cost mapping
        BW01                 Bayswater      Steam Sub Critical   NA      CNSW        Coal       Bayswater
        DALNTH1              Dalrymple BESS Battery Storage...   S4      CSA         -          -

    generators_existing_planned (partial):
        name  power_station  technology            geo_id  fuel_type  fuel_price_mapping  capacity
        BW01  Bayswater      Steam Sub Critical    CNSW    Coal       Bayswater           660.0

    storage_existing_planned (partial):
        name       power_station    technology          geo_id  fuel_type   capacity    storage_capacity
        DALNTH1    Dalrymple BESS   Battery Storage...  S4      Battery     30          9

Building {generators/storage}_existing_planned:
    1.  Splits the summary's rows into generators and storage — see
        _is_existing_planned_storage_row.
    2.  Renames the carried-over spine/identity columns to their schema names, derives
        geo_id (REZ ID with Sub-region fallback — see helpers._set_geo_id) and relabels
        it to ``regional_granularity`` (REZ-located rows stay untouched at every
        granularity).
    3.  Merges in unit-level properties (mappings.py). Each unit's ``name`` is resolved
        against a source table's own IASR ID column — exact matches first, small
        typos fuzzy-corrected. Every existing/planned unit is expected to resolve to
        a real row in each of these tables; an unresolved name raises.
    4.  For storage units - then merges in category-level properties: ``power_station`` or
        ``technology`` summary columns are resolved against equivalent columns in
        the property table, using those categories to map values onto.
    5.  For generators - merges in minimum_load: coal's Typical Lowest Band, then gas overlaid
        (see _merge_minimum_load) — the only two technologies with published minimum
        stable levels, so every other row is left NaN (expected).
    6.  Returns the filled out summary tables with only required columns present
        (see ``_GENERATOR_COLUMNS`` and ``_STORAGE_COLUMNS`` below).
"""

import logging

import pandas as pd

from ispypsa.templater.helpers import (
    _apply_iasr_table_replacements,
    _assert_table_valid,
    _derive_phes_symmetric_efficiency,
    _fuzzy_map_to_allowed_values,
    _fuzzy_match_names,
    _get_property_value_map,
    _group_properties_by_source,
    _is_battery_row,
    _is_storage_row,
    _map_geo_id_to_granularity,
    _required_property_columns,
    _set_geo_id,
)
from ispypsa.templater.mappings import (
    _BATTERY_EXISTING_PLANNED_TECH_PROPERTY_MAP,
    _GENERATORS_EXISTING_PLANNED_PROPERTY_MAP,
    _PHES_EXISTING_PLANNED_STATION_PROPERTY_MAP,
    _STORAGE_EXISTING_PLANNED_UNIT_PROPERTY_MAP,
)

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

_STORAGE_COLUMNS = [
    "name",
    "power_station",
    "technology",
    "geo_id",
    "fuel_type",
    "capacity",
    "storage_capacity",
    "efficiency_charge",
    "efficiency_discharge",
    "commissioning_date",
    "closure_year",
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

# Tumut 3 has a real pump/non-pump unit split that the summary and phes_properties
# tables can't currently be reconciled on by name. _validate_phes_routing tolerates
# this as known unmatched (rather than renaming units to assign as PHES) as part of
# an interim simplification to treat all Tumut 3 units as generators.
# See Open-ISP/ISPyPSA#131 comment thread.
_KNOWN_UNMATCHED_PHES_STATIONS = {"Lower Tumut"}


_IASR_TABLE_REPLACEMENTS = [
    # The PHES properties table keys Borumba by its short project name; the summary lists
    # it under its full project name. This is a mapping convenience/consistency fix
    dict(
        table_name="pumped_hydro_existing_committed_anticipated_additional_properties",
        column="Power Station",
        replacements={"Borumba": "QEJP - Borumba"},
    ),
    # Case mismatch between maximum_capacity's IASR ID and the summary's: the 'safe'
    # fuzzy-matching threshold (90) would miss (fuzz.ratio("KiataWF1", "KIATAWF1") == 50).
    dict(
        table_name="maximum_capacity_existing_committed_anticipated_additional_generators",
        column="IASR ID",
        replacements={"KiataWF1": "KIATAWF1"},
    ),
]

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
    iasr_tables = _apply_iasr_table_replacements(iasr_tables, _IASR_TABLE_REPLACEMENTS)
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
        iasr_tables,
        _GENERATORS_EXISTING_PLANNED_PROPERTY_MAP,
        exclude_unit_keys=non_generator_names,
    )
    generators = _format_commissioning_date(generators)
    generators = _merge_minimum_load(generators, iasr_tables)
    return generators[_GENERATOR_COLUMNS]


def _template_storage_existing_planned(
    iasr_tables: dict[str, pd.DataFrame],
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Templates the existing and planned (ECAA) storage table from the IASR summary.

    Args:
        iasr_tables: IASR tables; uses
            existing_committed_anticipated_additional_generator_summary,
            pumped_hydro_existing_committed_anticipated_additional_properties,
            maximum_capacity_existing_committed_anticipated_additional_generators,
            expected_closure_years and battery_properties.
        regional_granularity: "sub_regions", "nem_regions", or "single_region".
        sub_regional_geography: network_geography templated at "sub_regions"
            granularity; columns used: 'geo_id', 'geo_type', 'region_id'.

    I/O Example (subset of columns):
        existing_committed_anticipated_additional_generator_summary (abbr.):
            IASR ID / DLT names     Power Station   REZ ID      Sub-region  Fuel type
            BW01                    Bayswater       NA          CNSW        Coal
            Liddell BESS            Liddell BESS    N9          CNSW        Battery
            W/HOE#1                 Wivenhoe        NA          SQ          Water

        iasr_tables:
            battery_properties:
                Technology                      Charge efficiency_%     Discharge efficiency_%
                Battery Storage (4hrs storage)  92.5                    92.5

            pumped_hydro_existing_committed_anticipated_additional_properties:
                Power Station       Pumping efficiency (%)
                Wivenhoe            81.0

            ... plus the other tables in _STORAGE_EXISTING_PLANNED_UNIT_PROPERTY_MAP

        regional_granularity: "nem_regions"

        sub_regional_geography:
            geo_id  geo_type    region_id
            CNSW    subregion   NSW
            SQ      subregion   QLD

        returns:
            name            power_station   geo_id  fuel_type   efficiency_charge   efficiency_discharge
            Liddell BESS    Liddell BESS    N9      Battery     92.5                92.5
            W/HOE#1         Wivenhoe        QLD     Water       90.0                90.0
    """

    logging.info("Creating a template for existing and planned storage")
    iasr_tables = _apply_iasr_table_replacements(iasr_tables, _IASR_TABLE_REPLACEMENTS)
    summary = iasr_tables["existing_committed_anticipated_additional_generator_summary"]
    phes_properties = iasr_tables[
        "pumped_hydro_existing_committed_anticipated_additional_properties"
    ]
    is_storage = _is_existing_planned_storage_row(summary, phes_properties)
    summary = summary.rename(columns=_SUMMARY_COLUMN_RENAMES)

    storage = summary[is_storage].copy()
    storage = _set_geo_id(storage)
    storage["geo_id"] = _map_geo_id_to_granularity(
        storage["geo_id"], regional_granularity, sub_regional_geography
    )
    non_storage_names = set(summary.loc[~is_storage, "name"])
    storage = _merge_unit_keyed_properties(
        storage,
        iasr_tables,
        _STORAGE_EXISTING_PLANNED_UNIT_PROPERTY_MAP,
        exclude_unit_keys=non_storage_names,
    )
    storage = _merge_storage_type_split_properties(storage, iasr_tables)
    storage = _format_commissioning_date(storage)
    return storage[_STORAGE_COLUMNS]


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

    Checked after correcting the known Borumba name mismatch (see
    ``_apply_iasr_table_replacements``) and excusing the one known, documented gap
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
            QEJP - Borumba  # already 'fixed' (see _IASR_TABLE_REPLACEMENTS)
            Lower Tumut     # excused by _KNOWN_UNMATCHED_PHES_STATIONS

        -> no error

        A "Some New Station" row in phes_properties instead raises:
            ValueError: PHES properties station(s) not found in the summary:
                        ['Some New Station']
    """

    phes_stations = set(phes_properties["Power Station"])
    known_stations = set(summary["Power Station"]) | _KNOWN_UNMATCHED_PHES_STATIONS
    unmatched = phes_stations - known_stations
    if unmatched:
        raise ValueError(
            f"PHES properties station(s) not found in the summary: {sorted(unmatched)}"
        )


# --- property merges ---


def _merge_unit_keyed_properties(
    summary: pd.DataFrame,
    iasr_tables: dict[str, pd.DataFrame],
    property_map: dict[str, dict],
    exclude_unit_keys: set[str],
) -> pd.DataFrame:
    """Merges every property in ``property_map`` onto ``summary``, keyed on IASR ID = name.

    Groups properties by source (table, key_col) — see ``_group_properties_by_source``
    — so a table contributing several columns (e.g. maximum_capacity_... feeds
    capacity and commissioning_date) is validated and key-resolved once. Each
    generator's ``name`` is resolved against the property table's own 'key' column (see
    ``_resolve_unit_keys``); the resolved series becomes the key on which property
    values are mapped.

    I/O Example:
        summary:
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
    summary = summary.copy()
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
            summary["name"], table[key_col], table_name, exclude_unit_keys
        )
        for new_col, attrs in props.items():
            property_values = _get_property_value_map(table, attrs)
            summary[new_col] = resolved_keys.map(property_values)
    return summary


def _resolve_unit_keys(
    names: pd.Series,
    table_keys: pd.Series,
    table_name: str,
    exclude_unit_keys: set[str],
) -> pd.Series:
    """Fuzzy-resolves ``names`` to ``table_keys``' strings; raises on any miss.

    Standardises small differences (e.g. a single typo'd character) between a
    unit's ``name`` and the spelling used in a property table's 'key' column.
    ``table_keys`` is a lookup pool, so its order is irrelevant; the result carries
    one value per name, in ``names``' order, spelled as in ``table_keys``. A set
    of unit keys (names) that are known to be out of scope for a given property
    merge are passed as `exclude_unit_keys` to tighten the fuzzy-matching.

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
            f"'{table_name}' table missing a row for unit(s): {sorted(unmatched)}"
        )
    return resolved


def _merge_storage_type_split_properties(
    storage: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Merges technology-specific existing/planned storage properties into a summary
    table.

    This function splits an input ``storage`` summary table into battery and non-battery
    storage units (non-battery is currently PHES-only), merges technology-specific
    property values into the corresponding dataframe (see ``_merge_category_keyed_properties``),
    applies technology-specific transforms (see ``_derive_phes_symmetric_efficiency``),
    and returns a recombined all-storage summary table. Row order is not preserved
    by this function, instead (where they each exist) battery unit rows are returned
    above PHES unit rows due to the split-then-concat approach.

    I/O Example:
        storage (abbr.):
            name            power_station   technology      ...
            W/HOE#1         Wivenhoe        Hydro
            QEJP - Borumba  QEJP - Borumba  Pumped Hydro (24hrs storage)
            Liddell BESS    Liddell BESS    Battery Storage (4hrs storage)

        iasr_tables:
            battery_properties:
                Technology                     Charge efficiency_%  Discharge efficiency_%
                Battery Storage (4hrs storage) 92.5                 92.5

            pumped_hydro_existing_committed_anticipated_additional_properties:
                Power Station       Pumping efficiency (%)
                Wivenhoe            81.0
                QEJP - Borumba      81.0

        returns:
            name            power_station   technology  ...                 efficiency_charge   efficiency_discharge
            W/HOE#1         Wivenhoe        Hydro                           90.0                90.0
            QEJP - Borumba  QEJP - Borumba  Pumped Hydro (24hrs storage)    90.0                90.0
            Liddell BESS    Liddell BESS    Battery Storage (4hrs storage)  92.5                92.5
    """
    is_battery = _is_battery_row(storage, col_to_check="technology")
    battery_only = storage[is_battery].copy()
    phes_only = storage[~is_battery].copy()

    battery_only = _merge_category_keyed_properties(
        battery_only,
        iasr_tables,
        _BATTERY_EXISTING_PLANNED_TECH_PROPERTY_MAP,
        "technology",
    )
    phes_only = _merge_category_keyed_properties(
        phes_only,
        iasr_tables,
        _PHES_EXISTING_PLANNED_STATION_PROPERTY_MAP,
        "power_station",
    )
    phes_only = _derive_phes_symmetric_efficiency(phes_only)
    return pd.concat([battery_only, phes_only], axis=0, ignore_index=True)


# NOTE: plan to pull this out as a shareable helper for use here and
# by new_entrants.py to address Open-ISP/ISPyPSA#TBD.
def _merge_category_keyed_properties(
    summary: pd.DataFrame,
    iasr_tables: dict[str, pd.DataFrame],
    property_map: dict[str, dict],
    summary_key: str,
) -> pd.DataFrame:
    """Merges every non-unit-keyed property in ``property_map`` onto ``summary``.

    Groups properties by their source (table, key_col) — see
    ``_group_properties_by_source`` — so a table that contributes several properties
    (e.g. ``battery_properties`` feeds six) is validated and fuzzy-matched against
    ``summary_key`` values once per property map.

    I/O Example:
        property_map (abbr.):
            efficiency_charge:  table="battery_properties",
                                key_col="Technology",
                                value_col="Charge efficiency_%"
        summary:
            name             technology
            Liddell BESS     Battery Storage (4hrs storage)

        iasr_tables['battery_properties']:
            Technology                              Charge efficiency_%
            Battery Storage (4hrs storage)          92.5

        summary_key = "technology"

        returns (adds one column per map key):
            name             technology                       efficiency_charge  ...
            Liddell BESS     Battery Storage (4hrs storage)   92.5               ...
    """
    summary = summary.copy()
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
        matched_key_col = _fuzzy_map_to_allowed_values(
            summary[summary_key],
            table[key_col],
            task_desc=f"merging properties from '{table_name}'",
        )
        for new_col, attrs in props.items():
            property_values = _get_property_value_map(table, attrs)
            summary[new_col] = matched_key_col.map(property_values)
    return summary


def _format_commissioning_date(summary: pd.DataFrame) -> pd.DataFrame:
    """Reformats commissioning_date from the IASR's ISO string to the schema's %d/%m/%Y."""
    summary = summary.copy()
    summary["commissioning_date"] = pd.to_datetime(
        summary["commissioning_date"]
    ).dt.strftime(_COMMISSIONING_DATE_SCHEMA_FORMAT)
    return summary


def _merge_minimum_load(
    generators: pd.DataFrame,
    iasr_tables: dict[str, pd.DataFrame],
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
        exclude_unit_keys=set(),
    )
    values = _get_property_value_map(table, property_spec)
    generators.loc[is_candidate, "minimum_load"] = resolved_keys.map(values)
    return generators
