import logging
from pathlib import Path

import pandas as pd


def template_renewable_energy_zone_locations(parsed_workbook_path: Path | str) -> pd.DataFrame:
    """Creates a mapping table that specifies which regions and sub regions renewable energy zones belong to.

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: ISPyPSA region and zone mapping table
    """
    logging.info("Creating a renewable_energy_zone_locations template")
    renewable_energy_zone_locations = pd.read_csv(
        Path(parsed_workbook_path, "renewable_energy_zones.csv")
    )
    renewable_energy_zone_locations = renewable_energy_zone_locations.loc[
        :, ["NEM Region", "ISP Sub-region", "ID"]
    ]
    renewable_energy_zone_locations.columns = ["nem_region_id", "isp_sub_region_id", "rez_id"]
    renewable_energy_zone_locations = renewable_energy_zone_locations.set_index("rez_id", drop=True)
    return renewable_energy_zone_locations
