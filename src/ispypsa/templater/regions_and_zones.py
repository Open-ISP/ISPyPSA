import logging
from pathlib import Path

import pandas as pd


def template_region_and_zone_mapping(
    parsed_workbook_path: Path | str
) -> pd.DataFrame:
    """Creates a mapping table that specifies which region sub regions belong too and which regions and subregions REZs
    belong to.

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: ISPyPSA region and zone mapping table
    """
    logging.info(f"Creating a node mapping template")
    region_and_zone_mapping = pd.read_csv(
        Path(parsed_workbook_path, "renewable_energy_zones.csv")
    )
    region_and_zone_mapping = region_and_zone_mapping.loc[:, ["NEM Region", "ISP Sub-region", "ID"]]
    region_and_zone_mapping.columns = ["nem_region_id", "isp_sub_region_id", "rez_id"]
    region_and_zone_mapping = region_and_zone_mapping.set_index("rez_id", drop=True)
    return region_and_zone_mapping