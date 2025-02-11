import logging
from pathlib import Path

import pandas as pd
import requests
import xmltodict
from thefuzz import process

from ispypsa.templater.mappings import _NEM_REGION_IDS, _NEM_SUB_REGION_IDS

from .helpers import (
    _fuzzy_match_names,
    _snakecase_string,
)


def _get_reference_node_locations(reference_nodes):
    # request and merge in substation coordinates for reference nodes
    substation_coordinates = _request_transmission_substation_coordinates()
    if not substation_coordinates.empty:
        reference_node_col = process.extractOne(
            "reference_node", reference_nodes.columns
        )[0]
        matched_subs = _fuzzy_match_names(
            reference_nodes[reference_node_col],
            substation_coordinates.index,
            "merging in substation coordinate data",
            threshold=85,
        )
        reference_node_coordinates = pd.merge(
            matched_subs,
            substation_coordinates,
            how="left",
            left_on=reference_node_col,
            right_index=True,
        )
        reference_nodes = pd.concat(
            [
                reference_nodes,
                reference_node_coordinates["substation_latitude"],
                reference_node_coordinates["substation_longitude"],
            ],
            axis=1,
        )
    return reference_nodes


def _template_sub_regions(
    sub_regional_reference_nodes: pd.DataFrame, mapping_only: bool = False
) -> pd.DataFrame:
    """Processes the 'Sub-regional network representation' table into an ISPyPSA template format

    Args:
        sub_regional_reference_nodes: pd.DataFrame specifying the NEM subregional
            reference nodes.
        mapping_only: boolean, when doing single region or region modelling this input
            is set to True so unnecessary information such sub_region_reference_node
            are latitude and longitude are not returned.
    Returns:
        `pd.DataFrame`: ISPyPSA sub-regional node template

    """
    sub_regional_df = sub_regional_reference_nodes
    sub_region_name_and_id = _split_out_sub_region_name_and_id(sub_regional_df)
    node_voltage_col = "Sub-region Reference Node"
    split_node_voltage = _extract_voltage(sub_regional_df, node_voltage_col)
    sub_regions = pd.concat(
        [
            sub_region_name_and_id,
            split_node_voltage,
            sub_regional_df["NEM Region"].rename("nem_region"),
        ],
        axis=1,
    )
    sub_regions = sub_regions.set_index("isp_sub_region_id")
    sub_regions = _match_region_name_and_id(sub_regions)

    if mapping_only:
        sub_regions = sub_regions[["nem_region_id"]]
    else:
        sub_regions = sub_regions[
            [
                "nem_region_id",
                "sub_region_reference_node",
                "sub_region_reference_node_voltage_kv",
            ]
        ]
        sub_regions = _get_reference_node_locations(sub_regions)
    return sub_regions


def _template_regions(regional_reference_nodes: pd.DataFrame) -> pd.DataFrame:
    """Processes the 'Regional reference nodes' table into an ISPyPSA template format

    Args:
        regional_reference_nodes: pd.DataFrame iasr workbook table specifying the NEM
            regional reference nodes

    Returns:
        `pd.DataFrame`: ISPyPSA regional node template

    """
    regional_df = regional_reference_nodes
    node_voltage_col = "Regional Reference Node"
    split_node_voltage = _extract_voltage(regional_df, node_voltage_col)
    sub_region_name_and_id = _split_out_sub_region_name_and_id(regional_df)
    regions = pd.concat(
        [
            regional_df["NEM Region"].rename("nem_region"),
            split_node_voltage,
            sub_region_name_and_id["isp_sub_region_id"],
        ],
        axis=1,
    )
    regions = _match_region_name_and_id(regions)
    regions = regions.set_index("nem_region_id")
    regions = regions[
        [
            "isp_sub_region_id",
            "regional_reference_node",
            "regional_reference_node_voltage_kv",
        ]
    ]
    regions = _get_reference_node_locations(regions)
    return regions


def _split_out_sub_region_name_and_id(data: pd.DataFrame):
    name_id_col = "ISP Sub-region"
    sub_region_name_and_id = _capture_just_name(data[name_id_col])
    sub_region_name_and_id["name"] = _fuzzy_match_names(
        sub_region_name_and_id["name"],
        _NEM_SUB_REGION_IDS.keys(),
        "determining the NEM subregion region",
    )
    sub_region_name_and_id.columns = [_snakecase_string(name_id_col)]
    sub_region_name_and_id[_snakecase_string(name_id_col + " ID")] = (
        sub_region_name_and_id[_snakecase_string(name_id_col)].replace(
            _NEM_SUB_REGION_IDS
        )
    )
    return sub_region_name_and_id


def _match_region_name_and_id(data: pd.DataFrame):
    data["nem_region"] = _fuzzy_match_names(
        data["nem_region"],
        _NEM_REGION_IDS.keys(),
        "determining the NEM region",
    )
    data["nem_region_id"] = data["nem_region"].replace(_NEM_REGION_IDS)
    return data


def _extract_voltage(data: pd.DataFrame, column: str):
    split_node_voltage = _split_node_voltage(data[column])
    split_node_voltage.columns = [
        _snakecase_string(column),
        _snakecase_string(column + " Voltage (kV)"),
    ]
    split_node_voltage[_snakecase_string(column + " Voltage (kV)")] = (
        split_node_voltage[_snakecase_string(column + " Voltage (kV)")].astype(int)
    )
    return split_node_voltage


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
        r = requests.get(url, params=params, timeout=60)
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
    substation_coordinates = pd.DataFrame(substation_coordinates).T
    substation_coordinates = substation_coordinates[
        substation_coordinates.index.notna()
    ]
    return substation_coordinates


def _capture_just_name(series: pd.Series) -> pd.DataFrame:
    """
    Capture the name (plain English) and not the ID in parentheses (capitalised letters)
    using a regular expression on a string `pandas.Series`.
    """
    split_name_id = series.str.strip().str.extract(
        r"(?P<name>[A-Za-z\s,]+)(?=\s\([A-Z]+\))"
    )
    return split_name_id


def _split_node_voltage(series: pd.Series) -> pd.DataFrame:
    """
    Capture the node name (plain English) and 2-3 digit voltage in kV using a regular
    expression on a string `pandas.Series`.
    """
    split_node_voltage = series.str.strip().str.extract(
        r"(?P<name>[A-Za-z\s]+)\s(?P<voltage>[0-9]{2,3})\skV"
    )
    return split_node_voltage
