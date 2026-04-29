import pandas as pd

from ispypsa.templater.mappings import _NEM_REGION_IDS, _SINGLE_REGION_ID


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
