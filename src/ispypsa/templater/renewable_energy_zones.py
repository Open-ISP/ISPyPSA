import logging
from pathlib import Path

import pandas as pd

from .helpers import _snakecase_string


def template_renewable_energy_zones(
    parsed_workbook_path: Path | str, location_mapping_only: bool = True
) -> pd.DataFrame:
    """Creates a mapping table that specifies which regions and sub regions renewable energy zones belong to.

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.
        location_mapping_only: Whether to create a location mapping template that only
            contains columns that map REZ IDs to sub-regions and regions.
            Defaults to True.

    Returns:
        `pd.DataFrame`: ISPyPSA region and zone mapping table
    """
    logging.info("Creating a renewable_energy_zone_locations template")
    renewable_energy_zones = pd.read_csv(
        Path(parsed_workbook_path, "renewable_energy_zones.csv")
    )
    renewable_energy_zones.columns = [
        _snakecase_string(col_name) for col_name in renewable_energy_zones.columns
    ]
    renewable_energy_zones = renewable_energy_zones.rename(
        columns={
            "nem_region": "nem_region_id",
            "isp_sub_region": "isp_sub_region_id",
            "id": "rez_id",
        }
    )
    if location_mapping_only:
        renewable_energy_zones = renewable_energy_zones.loc[
            :,
            [
                "name",
                "nem_region_id",
                "isp_sub_region_id",
                "rez_id",
            ],
        ]
    renewable_energy_zones = renewable_energy_zones.set_index("rez_id", drop=True)
    return renewable_energy_zones
