import logging
from pathlib import Path

import pandas as pd
import requests
import xmltodict
from thefuzz import process

from .helpers import (
    _fuzzy_match_names,
    _snakecase_string,
)
from .mappings import _NEM_REGION_IDS, _NEM_SUB_REGION_IDS
from .renewable_energy_zones import template_renewable_energy_zones


def template_nodes(
    parsed_workbook_path: Path | str,
    regional_granularity: str = "sub_regions",
    rez_nodes: str = "discrete_nodes",
) -> pd.DataFrame:
    """Creates a node template that describes the nodes (i.e. buses) to be modelled

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.
        regional_granularity: Regional granularity of the nodes obtained from the model
            configuration. Defaults to "sub_regions".
        rez_nodes: How Renewable Energy Zones are modelled in the network. Obtained from
            the model configuration. Defaults to "discrete_nodes", which models REZs
            as network nodes.

    Returns:
        `pd.DataFrame`: ISPyPSA node template
    """
    logging.info(f"Creating a nodes template with {regional_granularity} as nodes")
    if regional_granularity == "sub_regions":
        template = _template_sub_regional_node_table(parsed_workbook_path)
    elif regional_granularity == "nem_regions":
        template = _template_regional_node_table(parsed_workbook_path)

    elif regional_granularity == "single_region":
        # TODO: Clarify `single_region`/`copper_plate` implementation
        template = {
            "name": "National Electricity Market single region",
            "type": "single_region",
            "reference_node": "Thomastown",
            "regional_reference_node_voltage_kv": 66,
        }
        template = pd.DataFrame(template, index=["NEM"])
    # request and merge in substation coordinates for reference nodes
    substation_coordinates = _request_transmission_substation_coordinates()
    if not substation_coordinates.empty:
        reference_node_col = process.extractOne("reference_node", template.columns)[0]
        matched_subs = _fuzzy_match_names(
            template[reference_node_col],
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
        template = pd.concat(
            [
                template,
                reference_node_coordinates["substation_latitude"],
                reference_node_coordinates["substation_longitude"],
            ],
            axis=1,
        )
    # add REZs as network nodes is "discrete_nodes" is provided
    if rez_nodes == "discrete_nodes":
        template = pd.concat(
            [
                template,
                _make_rezs_nodes(parsed_workbook_path).rename({"rez_id": "node_id"}),
            ],
            axis=0,
        )
    template.index = template.index.rename("node_id")
    return template


def template_sub_regions_to_nem_regions_mapping(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Processes the 'Sub-regional network representation' table into an ISPyPSA template
    format that maps sub-region IDs to NEM region IDs.

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`

    Returns:
        `pd.DataFrame`: ISPyPSA regional to subregional mapping template

    """
    sub_regions = pd.read_csv(
        Path(parsed_workbook_path, "sub_regional_reference_nodes.csv")
    )
    sub_region_name_and_id = _split_out_sub_region_name_and_id(sub_regions)
    sub_regions = pd.concat(
        [
            sub_region_name_and_id["isp_sub_region_id"],
            sub_regions["NEM Region"].rename("nem_region"),
        ],
        axis=1,
    )
    mapping = _match_region_name_and_id(sub_regions)
    mapping = mapping.drop(columns=["nem_region"]).set_index("isp_sub_region_id")
    return mapping


def template_region_to_single_sub_region_mapping(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Processes the 'Regional reference node' table into an ISPyPSA template
    format that maps each NEM region to a single sub-region that corresponds to the
    sub-region of the RRN.

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`

    Returns:
        pd.DataFrame: ISPyPSA region to single sub-region mapping template
    """
    regional_df = pd.read_csv(
        Path(parsed_workbook_path, "regional_reference_nodes.csv")
    )
    sub_region_name_and_id = _split_out_sub_region_name_and_id(regional_df)
    mapping = pd.concat(
        [
            regional_df["NEM Region"].rename("nem_region"),
            sub_region_name_and_id["isp_sub_region_id"],
        ],
        axis=1,
    )
    mapping = _match_region_name_and_id(mapping).drop(columns=["nem_region"])
    mapping = mapping.set_index("nem_region_id")
    return mapping


def _template_sub_regional_node_table(parsed_workbook_path: Path | str) -> pd.DataFrame:
    """Processes the 'Sub-regional network representation' table into an ISPyPSA template format

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`
    Returns:
        `pd.DataFrame`: ISPyPSA sub-regional node template

    """
    sub_regional_df = pd.read_csv(
        Path(parsed_workbook_path, "sub_regional_reference_nodes.csv")
    )
    sub_region_name_and_id = _split_out_sub_region_name_and_id(sub_regional_df)
    node_voltage_col = "Sub-region Reference Node"
    split_node_voltage = _extract_voltage(sub_regional_df, node_voltage_col)
    sub_regions = pd.concat(
        [
            sub_region_name_and_id,
            split_node_voltage,
        ],
        axis=1,
    )
    sub_regions = sub_regions.rename(columns={"isp_sub_region": "name"}).set_index(
        "isp_sub_region_id"
    )
    sub_regions["type"] = "sub_region"
    return sub_regions[
        [
            "name",
            "type",
            "sub_region_reference_node",
            "sub_region_reference_node_voltage_kv",
        ]
    ]


def _template_regional_node_table(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Processes the 'Regional reference nodes' table into an ISPyPSA template format

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`

    Returns:
        `pd.DataFrame`: ISPyPSA regional node template

    """
    regional_df = pd.read_csv(
        Path(parsed_workbook_path, "regional_reference_nodes.csv")
    )
    node_voltage_col = "Regional Reference Node"
    split_node_voltage = _extract_voltage(regional_df, node_voltage_col)
    regions = pd.concat(
        [
            regional_df["NEM Region"].rename("nem_region"),
            split_node_voltage,
        ],
        axis=1,
    )
    regions = _match_region_name_and_id(regions)
    regions = regions.rename(columns={"nem_region": "name"}).set_index("nem_region_id")
    regions["type"] = "nem_region"
    return regions[
        [
            "name",
            "type",
            "regional_reference_node",
            "regional_reference_node_voltage_kv",
        ]
    ]


def _make_rezs_nodes(parsed_workbook_path: Path | str) -> pd.DataFrame:
    rezs = template_renewable_energy_zones(
        parsed_workbook_path, location_mapping_only=False
    )
    rezs["type"] = "rez"
    rezs = rezs[["name", "type"]]
    return rezs


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
        sub_region_name_and_id[
            _snakecase_string(name_id_col)
        ].replace(_NEM_SUB_REGION_IDS)
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
