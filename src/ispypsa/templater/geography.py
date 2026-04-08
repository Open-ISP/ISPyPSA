import pandas as pd

from ispypsa.templater.mappings import _NEM_REGION_IDS


def _template_network_geography(
    sub_regional_reference_nodes: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
) -> pd.DataFrame:
    """Creates the network_geography table from IASR workbook tables.

    Args:
        sub_regional_reference_nodes: pd.DataFrame IASR table of ISP sub-regional
            reference nodes.
        renewable_energy_zones: pd.DataFrame IASR table of renewable energy zone
            identities.

    Returns:
        `pd.DataFrame`: unified network_geography table with columns geo_id, geo_type,
            region_id, subregion_id.
    """
    return pd.concat(
        [
            _subregion_rows(sub_regional_reference_nodes),
            _rez_rows(renewable_energy_zones),
        ],
        ignore_index=True,
    )


def _subregion_rows(sub_regional_reference_nodes: pd.DataFrame) -> pd.DataFrame:
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


def _rez_rows(renewable_energy_zones: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "geo_id": renewable_energy_zones["ID"],
            "geo_type": "rez",
            "region_id": renewable_energy_zones["NEM region"],
            "subregion_id": renewable_energy_zones["ISP sub-region"],
        }
    )


def _extract_subregion_id(series: pd.Series) -> pd.Series:
    """Extract subregion ID from 'Name (ID)' format, e.g. 'Northern Queensland (NQ)' -> 'NQ'."""
    return series.str.extract(r"\(([A-Z]+)\)", expand=False)
