import pandas as pd

from ispypsa.templater.mappings import _NEM_REGION_IDS, _NEM_SUB_REGION_IDS

from .helpers import (
    _fuzzy_match_names,
    _snakecase_string,
)


def _template_sub_regions(
    sub_regional_reference_nodes: pd.DataFrame, mapping_only: bool = False
) -> pd.DataFrame:
    """Processes the 'Sub-regional network representation' table into an ISPyPSA template format

    Args:
        sub_regional_reference_nodes: pd.DataFrame specifying the NEM subregional
            reference nodes.
        mapping_only: boolean, when doing single region or region modelling this input
            is set to True so unnecessary information such as sub_region_reference_node
            is not returned.
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
    sub_regions = _match_region_name_and_id(sub_regions)

    if mapping_only:
        sub_regions = sub_regions[["isp_sub_region_id", "nem_region_id"]]
    else:
        sub_regions = sub_regions[
            [
                "isp_sub_region_id",
                "nem_region_id",
                "sub_region_reference_node",
                "sub_region_reference_node_voltage_kv",
            ]
        ]
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

    regions = regions[
        [
            "nem_region_id",
            "isp_sub_region_id",
            "regional_reference_node",
            "regional_reference_node_voltage_kv",
        ]
    ]
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
