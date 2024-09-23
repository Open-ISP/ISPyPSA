import logging
from pathlib import Path

import pandas as pd
import requests
import xmltodict
from thefuzz import process

from .helpers import _fuzzy_match_names, _snakecase_string, ModelConfigOptionError

_NEM_REGION_IDS = pd.Series(
    {
        "Queensland": "QLD",
        "New South Wales": "NSW",
        "Victoria": "VIC",
        "South Australia": "SA",
        "Tasmania": "TAS",
    },
    name="nem_region_id_mapping",
)


def template_nodes(parsed_workbook_path: Path | str, granularity: str = "sub_regional"):
    valid_granularity_options = ["sub_regional", "regional"]
    if granularity not in valid_granularity_options:
        raise ModelConfigOptionError(
            f"The option '{granularity}' is not a valid option for `granularity`. "
            + f"Select one of: {valid_granularity_options}"
        )
    elif granularity == "sub_regional":
        template = _template_sub_regional_node_table(parsed_workbook_path)
        index_col = "isp_sub_region_id"
    elif granularity == "regional":
        template = _template_regional_node_table(parsed_workbook_path)
        index_col = "nem_region_id"
    elif granularity == "copper_plate":
        # TODO: Decide how to implement copper plate
        print("Not implemented")
        return None
    # request and merge in substation coordinates for reference nodes
    substation_coordinates = _request_transmission_substation_coordinates()
    if not substation_coordinates.empty:
        reference_node_col = process.extractOne("reference_node", template.columns)[0]
        matched_subs = _fuzzy_match_names(
            template[reference_node_col], substation_coordinates.index
        )
        reference_node_coordinates = pd.merge(
            matched_subs,
            substation_coordinates,
            how="left",
            left_on=reference_node_col,
            right_index=True,
        )
        template = pd.concat(
            [
                template,
                reference_node_coordinates["substation_latitude"],
                reference_node_coordinates["substation_longitude"],
            ],
            axis=1,
        )
    template = template.set_index(index_col)
    return template


def _template_sub_regional_node_table(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Processes the 'Sub-regional network representation' table into an ISPyPSA template format

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`
    Returns:
        ISPyPSA sub-regional network template in a :class:`pandas.DataFrame`

    """
    sub_regional_df = pd.read_csv(
        Path(parsed_workbook_path, "sub_regional_reference_nodes.csv")
    )
    name_id_col = "ISP Sub-region"
    split_name_id = _split_name_id(sub_regional_df[name_id_col])
    split_name_id.columns = [
        _snakecase_string(name_id_col),
        _snakecase_string(name_id_col + " ID"),
    ]
    node_voltage_col = "Sub-region Reference Node"
    split_node_voltage = _split_node_voltage(sub_regional_df[node_voltage_col])
    split_node_voltage.columns = [
        _snakecase_string(node_voltage_col),
        _snakecase_string(node_voltage_col + " Voltage (kV)"),
    ]
    sub_regional_network = pd.concat(
        [
            split_name_id,
            split_node_voltage,
            sub_regional_df["NEM Region"].rename("nem_region"),
        ],
        axis=1,
    )
    sub_regional_network["nem_region"] = _fuzzy_match_names(
        sub_regional_network["nem_region"], _NEM_REGION_IDS.keys()
    )
    sub_regional_network["nem_region_id"] = sub_regional_network["nem_region"].replace(
        _NEM_REGION_IDS
    )
    return sub_regional_network


def _template_regional_node_table(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Processes the 'Regional reference nodes' table into an ISPyPSA template format

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`
    Returns:
        ISPyPSA regional network template in a :class:`pandas.DataFrame`

    """
    regional_df = pd.read_csv(
        Path(parsed_workbook_path, "regional_reference_nodes.csv")
    )
    name_id_col = "ISP Sub-region"
    split_name_id = _split_name_id(regional_df[name_id_col])
    split_name_id.columns = [
        _snakecase_string(name_id_col),
        _snakecase_string(name_id_col + " ID"),
    ]
    node_voltage_col = "Regional Reference Node"
    split_node_voltage = _split_node_voltage(regional_df[node_voltage_col])
    split_node_voltage.columns = [
        _snakecase_string(node_voltage_col),
        _snakecase_string(node_voltage_col + " Voltage (kV)"),
    ]
    regional_network = pd.concat(
        [
            regional_df["NEM Region"].rename("nem_region"),
            split_name_id,
            split_node_voltage,
        ],
        axis=1,
    )
    regional_network["nem_region"] = _fuzzy_match_names(
        regional_network["nem_region"], _NEM_REGION_IDS.keys()
    )
    regional_network["nem_region_id"] = regional_network["nem_region"].replace(
        _NEM_REGION_IDS
    )
    return regional_network


def _request_transmission_substation_coordinates() -> pd.DataFrame:
    """
    Obtains transmission substation coordinates from a Web Feature Service (WFS)
    source hosted as a dataset within the Australian Government's National Map:

    https://www.nationalmap.gov.au/#share=s-403jqUldEkbj6CwWcPZHefSgYeA

    The requested data is in Geography Markup Language (GML) format, which can be parsed
    using the same tools that are used to parse XML.

    Returns:
        Substation names, latitude and longitude within a :class:`pandas.DataFrame`.
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
                substation_coordinates[name] = {
                    "substation_latitude": lat,
                    "substation_longitude": long,
                }
        else:
            logging.warning(
                f"Failed to fetch substation coordinates. HTTP Status code: {r.status_code}."
            )
    except requests.exceptions.RequestException as e:
        logging.error(f"Error requesting substation coordinate data:\n{e}.")
    if not substation_coordinates:
        logging.warning(
            "Could not get substation coordinate data. "
            + "Network node data will be templated without coordinate data."
        )
    return pd.DataFrame(substation_coordinates).T


def _split_name_id(series: pd.Series) -> pd.DataFrame:
    """
    Separate the a name (plain English) and ID in parentheses (capitalised letters)
    using a regular expression
    """
    split_name_id = series.str.extract(
        r"(?P<name>[A-Za-z\s,]+)\s\((?P<id>[A-Z]+)\)", expand=True
    )
    return split_name_id


def _split_node_voltage(series: pd.Series) -> pd.DataFrame:
    """
    Separate the node name (plain English) and 2-3 digit voltage in kV using a regular
    expression
    """
    split_node_voltage = series.str.extract(
        r"(?P<name>[A-Za-z\s]+)\s(?P<voltage>[0-9]{2,3})\skV"
    )
    return split_node_voltage
