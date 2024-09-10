from pathlib import Path

import pandas as pd


def template_sub_regional_network_table(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Processes the 'Sub-regional network representation table' into an ISPyPSA template format"""
    sub_regional_df = pd.read_csv(
        Path(parsed_workbook_path, "sub_regional_reference_nodes.csv")
    )
    # Regular expression separates plain English names and capitalised sub-region IDs
    split_name_id = sub_regional_df["ISP Sub-region"].str.extract(
        r"([A-Za-z\s,]+)\s\(([A-Z]+)\)", expand=True
    )
    split_name_id.columns = ["isp_sub_region", "isp_sub_region_id"]
    # Regular expression separates node name and 2-3 digit voltage
    split_node_voltage = sub_regional_df["Sub-region Reference Node"].str.extract(
        r"([A-Za-z\s]+)\s([0-9]{2,3})\skV"
    )
    split_node_voltage.columns = [
        "sub_region_reference_node",
        "sub_region_reference_node_voltage_kV",
    ]
    sub_regional_network = pd.concat(
        [
            split_name_id,
            split_node_voltage,
            sub_regional_df["NEM Region"].rename("nem_region"),
        ],
        axis=1,
    )
    sub_regional_network = sub_regional_network.set_index("isp_sub_region")
    return sub_regional_network
