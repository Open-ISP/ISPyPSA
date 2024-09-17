import logging
from pathlib import Path

import pandas as pd
import requests
import xmltodict


def template_sub_regional_network_table(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Processes the 'Sub-regional network representation table' into an ISPyPSA template format

    Regular expressions are used to separate the sub-regional node name and ID, and the
    sub-regional reference node (i.e. substation) and its nominal voltage in kV.

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`
    Returns:
        ISPyPSA sub-regional network template in a :class:`pd.DataFrame`

    """
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


def _request_transmission_substation_coordinates() -> pd.DataFrame:
    """
    Obtains transmission substation coordinates from a Web Feature Service (WFS)
    source hosted as a dataset within the Australian Government's National Map:

    https://www.nationalmap.gov.au/#share=s-403jqUldEkbj6CwWcPZHefSgYeA

    The requested data is in Geography Markup Language (GML) format, which can be parsed
    using the same tools that are used to parse XML.

    Returns:
        Substation names, latitude and longitude within a :class:`pd.DataFrame`.
        If request error is encountered or the HTTP status of the request is not OK,
        then an empty DataFrame will be returned with a warning that network node data
        will be templated without coordinate data

    """
    params = dict(
        service="WFS",
        version="2.0.0",
        request="GetFeature",
        typeNames="Foundation_Electricity_Infrastructure:Transmission_Substations",
        maxFeatures=10000,
    )
    url = "https://services.ga.gov.au/gis/services/Foundation_Electricity_Infrastructure/MapServer/WFSServer"
    substation_coordinates = {}
    try:
        r = requests.get(url, params=params)
        if r.status_code == 200:
            data = xmltodict.parse(r.content)
            features = data["wfs:FeatureCollection"]["wfs:member"]
            for feature in features:
                substation = feature[
                    "Foundation_Electricity_Infrastructure:Transmission_Substations"
                ]
                name = substation.get("Foundation_Electricity_Infrastructure:NAME")
                coordinates = substation["Foundation_Electricity_Infrastructure:SHAPE"][
                    "gml:Point"
                ]["gml:pos"]
                lat, long = coordinates.split(" ")
                substation_coordinates[name] = {"latitude": lat, "longitude": long}
        else:
            logging.warning(
                f"Failed to fetch substation coordinates. HTTP Status code: {r.status_code}."
            )
    except requests.exceptions.RequestException as e:
        logging.error(f"Error requesting substation coordinate data:\n{e}.")
    if not substation_coordinates:
        logging.warning("Network node data will be templated without coordinate data")
    return pd.DataFrame(substation_coordinates).T
