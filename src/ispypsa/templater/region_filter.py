import logging

import pandas as pd

# The regional granularities a filter can be applied at, keyed on the
# network_geography_hierarchy column the filter selects on: region_id for
# filter_by_nem_regions and subregion_id for filter_by_isp_sub_regions. At a
# coarser granularity one model geography holds the units and paths of
# several of the things being selected between, and they can't be separated.
_GRANULARITIES_A_FILTER_APPLIES_AT = {
    "region_id": ["sub_regions", "nem_regions"],
    "subregion_id": ["sub_regions"],
}

_TABLES_WITH_A_GEO_ID = [
    "network_geography",
    "generators_existing_planned",
    "generators_new_entrant",
    "storage_existing_planned",
    "storage_new_entrant",
    "costs_connection",
    "costs_fuel_prices",
]

_CUSTOM_CONSTRAINT_TABLES = [
    "custom_constraints",
    "custom_constraints_lhs",
    "custom_constraints_rhs",
]


def _filter_template_to_regions(
    template: dict[str, pd.DataFrame],
    regional_granularity: str,
    nem_regions: list[str] | None = None,
    isp_sub_regions: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Filters the templated tables to the selected NEM regions or ISP
    sub-regions.

    A template covers the whole NEM. The config's filter_by_nem_regions and
    filter_by_isp_sub_regions options ask for a model of part of it, say NSW
    alone. The selected geographies are the selected regions or sub-regions
    and everything inside them, and the rest are dropped. Dropping them leaves
    rows in most other tables that refer to something no longer there, a path
    to a dropped sub-region or a cost for a dropped path, so those rows are
    dropped too.

    Exactly one of nem_regions (e.g. ["NSW"]) and isp_sub_regions (e.g.
    ["CNSW", "SNW"]) is given. regional_granularity is the granularity the
    template was built at.

    Returns the template with the same tables and columns, and fewer rows in
    the tables listed below. The other tables are returned as they are:
    costs_new_entrant_build and emissions_reduction are not regional, timeslices
    is left whole, and carbon_budgets is for the whole NEM and is not scaled
    down (the translator warns of that).

    A row is dropped when it refers to something that was dropped. A blank key
    cell is a wildcard for "applies to all", refers to nothing, and so never
    causes a row to be dropped. What each table's rows refer to:

    - network_geography_hierarchy: a row is kept if it is a selected
      geography, or the NEM region or NEM that contains one. It is filtered
      because the translator places the demand of every sub-region in it at a
      model geography, so a sub-region left in it would bring its demand into
      the model.
    - network_geography, the four generator and storage tables,
      costs_connection and costs_fuel_prices: the geo_id, which must be in the
      filtered hierarchy. A fuel price row that names a power station also
      names its geo_id, so it goes with the station.
    - network_transmission_paths: geo_from and geo_to, both of which must be
      kept. A path from a kept geography to a dropped one is dropped.
    - network_transmission_path_limits: the path_id.
    - custom_constraints, custom_constraints_lhs and custom_constraints_rhs:
      the constraint, which is dropped whole if any of its LHS terms is on a
      dropped path, unit or geography. Its limit was set for all of its terms
      together, so it is not applied to the terms that are left.
    - network_expansion_options: the expansion_id, which must be a kept path
      or a kept custom constraint.
    - network_transmission_path_expansion_costs: the expansion_id, which must
      still have a row in network_expansion_options.

    Two things raise a ValueError, each because the model asked for can't be
    built:

    - isp_sub_regions at a regional_granularity of "nem_regions" or
      "single_region", or nem_regions at "single_region". The units and paths
      are then templated at a geography that holds more than the selection,
      NSW for a selection of CNSW, and can't be split.
    - A filter with no value in the hierarchy, say ["NSX"].

    Where some values are in the hierarchy and some are not, say ["NSW",
    "NSX"], the template is filtered to those that are and a warning names the
    rest. The dropped rows are not logged: they are what the filter asks for.

    I/O Example:
        nem_regions: ["NSW"]

        template["network_geography_hierarchy"]:
            geo_id  subregion_id  region_id  nem_id
            NEM                              NEM     # contains NSW: kept
            NSW                   NSW        NEM
            QLD                   QLD        NEM     # dropped
            CNSW    CNSW          NSW        NEM
            SQ      SQ            QLD        NEM     # dropped
            N3      CNSW          NSW        NEM

        template["network_geography"]:
            geo_id  geo_type   region_id  subregion_id
            CNSW    subregion  NSW        CNSW
            SQ      subregion  QLD        SQ            # dropped
            N3      rez        NSW        CNSW

        template["network_transmission_paths"]:
            path_id  geo_from  geo_to  carrier
            N3-CNSW  N3        CNSW    AC
            CNSW-SQ  CNSW      SQ      AC       # SQ is dropped, so is the path

        template["network_transmission_path_limits"]:
            path_id  direction  timeslice  capacity
            N3-CNSW  forward               500
            CNSW-SQ  forward               900       # dropped with its path
                                           1000      # a wildcard: kept

        returns["network_geography_hierarchy"]:
            geo_id  subregion_id  region_id  nem_id
            NEM                              NEM
            NSW                   NSW        NEM
            CNSW    CNSW          NSW        NEM
            N3      CNSW          NSW        NEM

        returns["network_geography"]:
            geo_id  geo_type   region_id  subregion_id
            CNSW    subregion  NSW        CNSW
            N3      rez        NSW        CNSW

        returns["network_transmission_paths"]:
            path_id  geo_from  geo_to  carrier
            N3-CNSW  N3        CNSW    AC

        returns["network_transmission_path_limits"]:
            path_id  direction  timeslice  capacity
            N3-CNSW  forward               500
                                           1000
    """
    level = "region_id" if nem_regions else "subregion_id"
    if regional_granularity not in _GRANULARITIES_A_FILTER_APPLIES_AT[level]:
        raise ValueError(
            f"A template with regional_granularity '{regional_granularity}' can't "
            f"be filtered by {level}: its geographies are coarser than the filter."
        )
    filtered = template.copy()

    hierarchy = _filter_hierarchy(
        template["network_geography_hierarchy"], level, nem_regions or isp_sub_regions
    )
    filtered["network_geography_hierarchy"] = hierarchy
    for table in _TABLES_WITH_A_GEO_ID:
        filtered[table] = _keep_rows_referring_to(
            template[table], "geo_id", hierarchy["geo_id"]
        )

    paths = template["network_transmission_paths"]
    paths = _keep_rows_referring_to(paths, "geo_from", hierarchy["geo_id"])
    paths = _keep_rows_referring_to(paths, "geo_to", hierarchy["geo_id"])
    filtered["network_transmission_paths"] = paths
    filtered["network_transmission_path_limits"] = _keep_rows_referring_to(
        template["network_transmission_path_limits"], "path_id", paths["path_id"]
    )

    constraint_ids = _list_constraints_with_every_term_kept(template, filtered)
    for table in _CUSTOM_CONSTRAINT_TABLES:
        filtered[table] = _keep_rows_referring_to(
            template[table], "constraint_id", constraint_ids
        )

    options = _keep_rows_referring_to(
        template["network_expansion_options"],
        "expansion_id",
        pd.concat([paths["path_id"], constraint_ids]),
    )
    filtered["network_expansion_options"] = options
    filtered["network_transmission_path_expansion_costs"] = _keep_rows_referring_to(
        template["network_transmission_path_expansion_costs"],
        "expansion_id",
        options["expansion_id"],
    )
    return filtered


def _filter_hierarchy(
    hierarchy: pd.DataFrame, level: str, selected_ids: list[str]
) -> pd.DataFrame:
    """Filters network_geography_hierarchy to the selected geographies and the
    NEM region and NEM that contain them.

    level is the column the selection is made on: region_id to select NEM
    regions, subregion_id to select ISP sub-regions. A row is selected when
    its value in that column is one of selected_ids, so a selected region
    brings its sub-regions and REZs with it.

    Raises a ValueError when no value of selected_ids is in the level column,
    and logs a warning naming the values that are not when only some are.

    I/O Example:
        level: "subregion_id"
        selected_ids: ["CNSW"]

        hierarchy:
            geo_id  subregion_id  region_id  nem_id
            NEM                              NEM     # contains CNSW
            NSW                   NSW        NEM     # contains CNSW
            QLD                   QLD        NEM
            CNSW    CNSW          NSW        NEM
            NNSW    NNSW          NSW        NEM
            N3      CNSW          NSW        NEM     # a REZ in CNSW
            N1      NNSW          NSW        NEM

        returns:
            geo_id  subregion_id  region_id  nem_id
            NEM                              NEM
            NSW                   NSW        NEM
            CNSW    CNSW          NSW        NEM
            N3      CNSW          NSW        NEM
    """
    not_found = sorted(set(selected_ids) - set(hierarchy[level]))
    if len(not_found) == len(set(selected_ids)):
        raise ValueError(
            f"None of the region filter values is a {level} in "
            f"network_geography_hierarchy: {not_found}"
        )
    if not_found:
        logging.warning(
            f"Region filter values that are not a {level} in "
            f"network_geography_hierarchy: {not_found}"
        )
    is_selected = hierarchy[level].isin(selected_ids)
    selected = hierarchy[is_selected]
    contains_a_selected = hierarchy["geo_id"].isin(selected["region_id"]) | hierarchy[
        "geo_id"
    ].isin(selected["nem_id"])
    return hierarchy[is_selected | contains_a_selected]


def _keep_rows_referring_to(
    table: pd.DataFrame, column: str, kept_ids: pd.Series
) -> pd.DataFrame:
    """Keeps the rows of table whose value in column is one of kept_ids, or
    is blank.

    The templated tables refer to one another by id: a path limit names its
    path in path_id, a unit names its geography in geo_id. Once the region
    filter has dropped some paths or geographies, the rows that refer to them
    have to go as well. In several tables the id may also be left blank, as a
    wildcard for "applies to all", and such a row refers to nothing that was
    dropped.

    kept_ids holds the ids that are still there, e.g. the path_id column of
    the filtered network_transmission_paths.

    Returns table with its columns unchanged and only the rows whose column
    value is in kept_ids or is blank. Where column has no blanks, as with a
    unit's geo_id, this is a plain filter to kept_ids.

    I/O Example:
        column: "path_id"
        kept_ids: ["CQ-NQ"]

        table:
            path_id  direction  capacity
            CQ-NQ    forward    1200
            SQ-CQ    forward    1100      # SQ-CQ is not kept
                     reverse    500       # a wildcard: kept

        returns:
            path_id  direction  capacity
            CQ-NQ    forward    1200
                     reverse    500
    """
    return table[table[column].isna() | table[column].isin(kept_ids)]


def _list_constraints_with_every_term_kept(
    template: dict[str, pd.DataFrame], filtered: dict[str, pd.DataFrame]
) -> pd.Series:
    """Lists the custom constraints with no LHS term on a dropped path, unit
    or geography.

    The constraints and their terms are read from template, and what is kept
    from the geography, path and unit tables of filtered. A term's
    variable_name is looked up in the table its term_type refers to: a path_id
    for link_flow, a generator name for generator_output and
    generator_capacity, a storage name for storage_output and a geo_id for
    load. A constraint with no LHS terms at all has none dropped, so it is
    listed.

    I/O Example:
        template["custom_constraints"]:
            constraint_id  direction
            SWQLD1         <=
            NQ1            <=
            UNUSED         <=         # no terms, so none dropped

        template["custom_constraints_lhs"]:
            constraint_id  term_type         variable_name  coefficient
            SWQLD1         link_flow         SQ-CQ          1.0
            SWQLD1         generator_output  BW01           0.5          # BW01 was dropped
            NQ1            link_flow         SQ-CQ          1.0

        filtered["network_transmission_paths"]:
            path_id  geo_from  geo_to
            SQ-CQ    SQ        CQ

        filtered["generators_existing_planned"]:
            name  geo_id

        the other generator, storage and geography tables of filtered: no rows

        returns:
            NQ1
            UNUSED
    """
    lhs = template["custom_constraints_lhs"]
    generators = pd.concat(
        [filtered["generators_existing_planned"], filtered["generators_new_entrant"]]
    )
    storage = pd.concat(
        [filtered["storage_existing_planned"], filtered["storage_new_entrant"]]
    )
    paths = filtered["network_transmission_paths"]
    geography = filtered["network_geography"]

    term_type = lhs["term_type"]
    name = lhs["variable_name"]
    is_kept = (
        ((term_type == "link_flow") & name.isin(paths["path_id"]))
        | ((term_type == "generator_output") & name.isin(generators["name"]))
        | ((term_type == "generator_capacity") & name.isin(generators["name"]))
        | ((term_type == "storage_output") & name.isin(storage["name"]))
        | ((term_type == "load") & name.isin(geography["geo_id"]))
    )
    with_a_dropped_term = lhs.loc[~is_kept, "constraint_id"]
    constraint_ids = template["custom_constraints"]["constraint_id"]
    return constraint_ids[~constraint_ids.isin(with_a_dropped_term)]
