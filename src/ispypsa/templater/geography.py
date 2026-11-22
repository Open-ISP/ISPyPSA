import logging

import pandas as pd

from ispypsa.templater.mappings import (
    _NEM_REGION_IDS,
    _NON_REZ_GEOGRAPHIES,
    _SINGLE_REGION_ID,
)


def _template_network_geography(
    sub_regional_reference_nodes: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
    regional_granularity: str,
) -> pd.DataFrame:
    """Creates the network_geography table from IASR workbook tables.

    The shape of the output depends on ``regional_granularity``:

    - ``sub_regions``: one row per ISP sub-region (geo_type=subregion) plus
      one row per REZ (geo_type=rez). Includes a ``subregion_id`` column.
    - ``nem_regions``: one row per NEM region (geo_type=region) plus one
      row per REZ. No ``subregion_id`` column.
    - ``single_region``: a single row with geo_id="NEM" plus one row per
      REZ. No ``subregion_id`` column.

    Args:
        sub_regional_reference_nodes: pd.DataFrame IASR table of ISP sub-regional
            reference nodes.
        renewable_energy_zones: pd.DataFrame IASR table of renewable energy zone
            identities.
        regional_granularity: one of "sub_regions", "nem_regions", or
            "single_region".

    I/O Example:
        Inputs:

            sub_regional_reference_nodes:
                NEM region,       ISP sub-region,            Sub-regional reference node
                Queensland,       Northern Queensland (NQ),  Ross 275 kV
                New South Wales,  Central NSW (CNSW),        Wellington 330 kV

            renewable_energy_zones:
                ID,  Name,           NEM region,  ISP sub-region
                Q1,  Far North,      QLD,         NQ
                N3,  Central-West,   NSW,         CNSW

        regional_granularity = "sub_regions":
                geo_id  geo_type   region_id  subregion_id
                NQ      subregion  QLD        NQ
                CNSW    subregion  NSW        CNSW
                Q1      rez        QLD        NQ
                N3      rez        NSW        CNSW

        regional_granularity = "nem_regions":
                geo_id  geo_type  region_id
                QLD     region    QLD
                NSW     region    NSW
                Q1      rez       QLD
                N3      rez       NSW

        regional_granularity = "single_region":
                geo_id  geo_type  region_id
                NEM     region    NEM
                Q1      rez       NEM
                N3      rez       NEM
    """
    if regional_granularity == "sub_regions":
        return _build_sub_regional_geography(
            sub_regional_reference_nodes, renewable_energy_zones
        )
    if regional_granularity == "nem_regions":
        return _build_nem_regional_geography(
            sub_regional_reference_nodes, renewable_energy_zones
        )
    if regional_granularity == "single_region":
        return _build_single_region_geography(renewable_energy_zones)
    raise ValueError(f"Unknown regional_granularity: {regional_granularity!r}")


# --- sub_regions ---


def _build_sub_regional_geography(
    sub_regional_reference_nodes: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
) -> pd.DataFrame:
    """Builds geography for sub_regions: subregion rows + REZ rows with subregion_id."""
    return pd.concat(
        [
            _subregion_rows(sub_regional_reference_nodes),
            _rez_rows_with_subregion(renewable_energy_zones),
        ],
        ignore_index=True,
    )


def _subregion_rows(sub_regional_reference_nodes: pd.DataFrame) -> pd.DataFrame:
    """Builds one geography row per ISP sub-region (geo_type=subregion).

    I/O Example:
        sub_regional_reference_nodes:
            NEM region,       ISP sub-region,            Sub-regional reference node
            Queensland,       Northern Queensland (NQ),  Ross 275 kV
            New South Wales,  Central NSW (CNSW),        Wellington 330 kV

        returns:
            geo_id  geo_type   region_id  subregion_id
            NQ      subregion  QLD        NQ
            CNSW    subregion  NSW        CNSW
    """
    subregion_id = _extract_subregion_id(sub_regional_reference_nodes["ISP sub-region"])
    region_id = sub_regional_reference_nodes["NEM region"].map(_NEM_REGION_IDS)
    return pd.DataFrame(
        {
            "geo_id": subregion_id,
            "geo_type": "subregion",
            "region_id": region_id,
            "subregion_id": subregion_id,
        }
    )


def _rez_rows_with_subregion(renewable_energy_zones: pd.DataFrame) -> pd.DataFrame:
    """Builds one geography row per REZ, retaining its parent sub-region.

    I/O Example:
        renewable_energy_zones:
            ID,  Name,         NEM region,  ISP sub-region
            Q1,  Far North,    QLD,         NQ
            N3,  Central-West, NSW,         CNSW

        returns:
            geo_id  geo_type  region_id  subregion_id
            Q1      rez       QLD        NQ
            N3      rez       NSW        CNSW
    """
    return pd.DataFrame(
        {
            "geo_id": renewable_energy_zones["ID"],
            "geo_type": "rez",
            "region_id": renewable_energy_zones["NEM region"],
            "subregion_id": renewable_energy_zones["ISP sub-region"],
        }
    )


# --- nem_regions ---


def _build_nem_regional_geography(
    sub_regional_reference_nodes: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
) -> pd.DataFrame:
    """Builds geography for nem_regions: region rows + REZ rows keyed to regions."""
    return pd.concat(
        [
            _region_rows(sub_regional_reference_nodes),
            _rez_rows_no_subregion(renewable_energy_zones),
        ],
        ignore_index=True,
    )


def _region_rows(sub_regional_reference_nodes: pd.DataFrame) -> pd.DataFrame:
    """Builds one geography row per unique NEM region (geo_type=region).

    Maps full NEM region names to short codes via ``_NEM_REGION_IDS``, then
    de-duplicates so each region appears once even though multiple sub-regions
    share it.

    I/O Example:
        sub_regional_reference_nodes:
            NEM region,       ISP sub-region,            Sub-regional reference node
            Queensland,       Northern Queensland (NQ),  Ross 275 kV
            Queensland,       Central Queensland (CQ),   Stanwell 275 kV
            New South Wales,  Central NSW (CNSW),        Wellington 330 kV

        returns:
            geo_id  geo_type  region_id
            QLD     region    QLD
            NSW     region    NSW
    """
    region_ids = (
        sub_regional_reference_nodes["NEM region"]
        .map(_NEM_REGION_IDS)
        .drop_duplicates()
    )
    return pd.DataFrame(
        {
            "geo_id": region_ids.values,
            "geo_type": "region",
            "region_id": region_ids.values,
        }
    )


def _rez_rows_no_subregion(renewable_energy_zones: pd.DataFrame) -> pd.DataFrame:
    """Builds REZ geography rows keyed to NEM regions (no subregion_id column).

    I/O Example:
        renewable_energy_zones:
            ID,  Name,         NEM region,  ISP sub-region
            Q1,  Far North,    QLD,         NQ
            N3,  Central-West, NSW,         CNSW

        returns:
            geo_id  geo_type  region_id
            Q1      rez       QLD
            N3      rez       NSW
    """
    return pd.DataFrame(
        {
            "geo_id": renewable_energy_zones["ID"],
            "geo_type": "rez",
            "region_id": renewable_energy_zones["NEM region"],
        }
    )


# --- single_region ---


def _build_single_region_geography(
    renewable_energy_zones: pd.DataFrame,
) -> pd.DataFrame:
    """Builds geography for single_region: one NEM row + REZ rows keyed to NEM."""
    return pd.concat(
        [_single_nem_row(), _rez_rows_for_single_region(renewable_energy_zones)],
        ignore_index=True,
    )


def _single_nem_row() -> pd.DataFrame:
    """Builds the single 'NEM' geography row used when granularity is single_region.

    I/O Example:
        returns:
            geo_id  geo_type  region_id
            NEM     region    NEM
    """
    return pd.DataFrame(
        {
            "geo_id": [_SINGLE_REGION_ID],
            "geo_type": ["region"],
            "region_id": [_SINGLE_REGION_ID],
        }
    )


def _rez_rows_for_single_region(
    renewable_energy_zones: pd.DataFrame,
) -> pd.DataFrame:
    """Builds REZ geography rows all pointing at the single 'NEM' region.

    I/O Example:
        renewable_energy_zones:
            ID,  Name,         NEM region,  ISP sub-region
            Q1,  Far North,    QLD,         NQ
            N3,  Central-West, NSW,         CNSW

        returns:
            geo_id  geo_type  region_id
            Q1      rez       NEM
            N3      rez       NEM
    """
    return pd.DataFrame(
        {
            "geo_id": renewable_energy_zones["ID"],
            "geo_type": "rez",
            "region_id": _SINGLE_REGION_ID,
        }
    )


# --- shared ---


def _extract_subregion_id(series: pd.Series) -> pd.Series:
    """Extract subregion ID from 'Name (ID)' format, e.g. 'Northern Queensland (NQ)' -> 'NQ'."""
    return series.str.extract(r"\(([A-Z]+)\)", expand=False)


# --- hierarchy ---


def _template_network_geography_hierarchy(
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    """Creates the network_geography_hierarchy table from the sub-regional
    network geography.

    The ISP's geographies nest: a REZ sits in a sub-region, a sub-region in a
    NEM region, and every NEM region in the NEM. network_geography lists only
    the geographies the model is built from, so at "nem_regions" and
    "single_region" granularity it no longer says which sub-regions make up a
    region. The hierarchy table records the nesting in full, and is the same
    whatever granularity the model is built at.

    ``sub_regional_geography`` is the network_geography table as templated at
    "sub_regions" granularity, whatever the model's granularity is. It already
    has a row for every sub-region and REZ. The hierarchy table keeps those
    rows and adds a row for each NEM region and one for the NEM.

    Returns the hierarchy table, with one row per geography:

    - geo_id: the geography.
    - subregion_id: the sub-region it is or sits inside. Blank for a NEM
      region and the NEM, which sit inside no sub-region.
    - region_id: the NEM region it is or sits inside. Blank for the NEM.
    - nem_id: "NEM" on every row, since every geography is or sits inside
      the NEM.

    The table has no column for the type of geography, because the level
    columns already say it: a sub-region is a row whose geo_id equals its
    subregion_id, a NEM region one whose geo_id equals its region_id, and the
    NEM the row whose geo_id equals its nem_id. A REZ is a row whose geo_id
    equals none of the three.

    Tasmania is a NEM region with a single sub-region, and the two share the
    id TAS. A geo_id can only have one row, so TAS has a single row whose
    subregion_id and region_id are both TAS: it is both a sub-region and a
    NEM region.

    The translator uses the hierarchy table to place data given at one level
    of the hierarchy at another, e.g. each sub-region's demand at the model geography
    that contains it.

    I/O Example:
        sub_regional_geography:
            geo_id  geo_type   region_id  subregion_id
            NQ      subregion  QLD        NQ
            CNSW    subregion  NSW        CNSW
            SNW     subregion  NSW        SNW
            TAS     subregion  TAS        TAS
            Q1      rez        QLD        NQ

        returns:
            geo_id  subregion_id  region_id  nem_id
            NEM                              NEM
            QLD                   QLD        NEM
            NSW                   NSW        NEM     # once, though two sub-regions share it
            NQ      NQ            QLD        NEM
            CNSW    CNSW          NSW        NEM
            SNW     SNW           NSW        NEM
            TAS     TAS           TAS        NEM     # both a sub-region and a region: one row
            Q1      NQ            QLD        NEM
    """
    region_ids = _find_regions_needing_a_row(sub_regional_geography)
    nem_and_region_rows = _build_nem_and_region_rows(region_ids)
    return _add_rows_to_sub_regional_geography(
        nem_and_region_rows, sub_regional_geography
    )


def _find_regions_needing_a_row(sub_regional_geography: pd.DataFrame) -> pd.Series:
    """Finds the NEM regions that need a row of their own in the hierarchy
    table: each region the sub-regions sit inside, listed once.

    A region that shares its id with a sub-region (TAS) is left out, because
    that sub-region's row stands for it.

    I/O Example:
        sub_regional_geography:
            geo_id  geo_type   region_id  subregion_id
            NQ      subregion  QLD        NQ
            CNSW    subregion  NSW        CNSW
            SNW     subregion  NSW        SNW
            TAS     subregion  TAS        TAS
            Q1      rez        QLD        NQ

        returns:
            pd.Series(["QLD", "NSW"])    # NSW once; TAS shares its sub-region's id
    """
    is_subregion = sub_regional_geography["geo_type"] == "subregion"
    subregions = sub_regional_geography[is_subregion]
    shares_id_with_subregion = subregions["region_id"].isin(subregions["geo_id"])
    region_ids = subregions.loc[~shares_id_with_subregion, "region_id"]
    return region_ids.drop_duplicates()


def _build_nem_and_region_rows(region_ids: pd.Series) -> pd.DataFrame:
    """Builds the hierarchy table rows for the NEM and for each NEM region.

    A region is its own region_id. Neither kind of row sits inside a
    sub-region, and the NEM sits inside no region, so those cells are blank.

    I/O Example:
        region_ids: pd.Series(["QLD", "NSW"])

        returns:
            geo_id  region_id
            NEM
            QLD     QLD
            NSW     NSW
    """
    nem_row = pd.DataFrame({"geo_id": [_SINGLE_REGION_ID]})
    region_rows = pd.DataFrame({"geo_id": region_ids, "region_id": region_ids})
    return pd.concat([nem_row, region_rows], ignore_index=True)


def _add_rows_to_sub_regional_geography(
    nem_and_region_rows: pd.DataFrame, sub_regional_geography: pd.DataFrame
) -> pd.DataFrame:
    """Adds the NEM and NEM region rows above the sub-region and REZ rows,
    and marks every geography as being inside the NEM.

    The geo_type column of sub_regional_geography is dropped: in the hierarchy
    table the level columns say what type each geography is.

    I/O Example:
        nem_and_region_rows:
            geo_id  region_id
            NEM
            NSW     NSW

        sub_regional_geography:
            geo_id  geo_type   region_id  subregion_id
            CNSW    subregion  NSW        CNSW
            N3      rez        NSW        CNSW

        returns:
            geo_id  subregion_id  region_id  nem_id
            NEM                              NEM
            NSW                   NSW        NEM
            CNSW    CNSW          NSW        NEM
            N3      CNSW          NSW        NEM
    """
    hierarchy = pd.concat(
        [nem_and_region_rows, sub_regional_geography], ignore_index=True
    )
    hierarchy["nem_id"] = _SINGLE_REGION_ID
    return hierarchy[["geo_id", "subregion_id", "region_id", "nem_id"]]


# --- non-REZ geographies ---


def _add_non_rez_geographies_to_rez_table(
    iasr_tables: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Returns a shallow copy of iasr_tables with non-REZ areas N0 and V0 appended
    to renewable_energy_zones.

    Raises:
        ValueError: if any non-REZ ID is already present in renewable_energy_zones.

    I/O Example:
        _NON_REZ_GEOGRAPHIES (constant, mappings.py):
            ID,  Name,                     NEM region,  ISP sub-region
            N0,  New South Wales Non-REZ,  NSW,         CNSW
            V0,  Victoria Non-REZ,         VIC,         SEV

        iasr_tables["renewable_energy_zones"]:
            ID,  Name,         NEM region,  ISP sub-region
            Q1,  Far North,    QLD,         NQ
            N3,  Central-West, NSW,         CNSW

        returns:
            iasr_tables["renewable_energy_zones"]:
                ID,  Name,                      NEM region,  ISP sub-region
                Q1,  Far North,                 QLD,         NQ
                N3,  Central-West,              NSW,         CNSW
                N0,  New South Wales Non-REZ,   NSW,         CNSW   # added
                V0,  Victoria Non-REZ,          VIC,         SEV    # added

            All other iasr_tables are returned unchanged.
    """
    _assert_non_rezs_dont_exist(iasr_tables["renewable_energy_zones"])
    table = pd.concat(
        [iasr_tables["renewable_energy_zones"], _NON_REZ_GEOGRAPHIES],
        axis=0,
        ignore_index=True,
    )
    _log_non_rezs_added_to_rez_table()
    return {**iasr_tables, "renewable_energy_zones": table}


def _assert_non_rezs_dont_exist(table: pd.DataFrame) -> None:
    """Raises if any non-REZ ID is already in the table, which would duplicate it."""
    additional_non_rez_ids = set(_NON_REZ_GEOGRAPHIES["ID"])
    existing_ids = set(table["ID"])
    already_present = additional_non_rez_ids & existing_ids
    if already_present:
        msg = (
            f"Non-REZ ID(s) {sorted(already_present)} are already defined in "
            "'renewable_energy_zones' IASR table. Check '_NON_REZ_GEOGRAPHIES' "
            "is up-to-date."
        )
        raise ValueError(msg)


def _log_non_rezs_added_to_rez_table() -> None:
    """Logs non-REZs added to the renewable_energy_zones table."""
    msg = (
        "Added non-REZ geographies to 'renewable_energy_zones' "
        f"IASR table: {sorted(_NON_REZ_GEOGRAPHIES['ID'])}"
    )
    logging.info(msg)
