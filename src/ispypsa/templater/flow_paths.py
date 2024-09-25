import re
from pathlib import Path

import pandas as pd

from .helpers import (
    _HVDC_FLOW_PATHS,
    _snakecase_string,
)
from ..config.validators import validate_granularity


def template_flow_paths(
    parsed_workbook_path: Path | str, granularity: str = "sub_regional"
) -> pd.DataFrame:
    """
    Creates a flow path template that describes the flow paths (i.e. lines)
    that will be modelled using ISPyPSA based on the `granularity`
    specified in the model configuration.

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
          outputs from the `isp-workbook-parser`.
        granularity: Geographical granularity obtained from the model configuration

    Returns:
        Flow path template as a `pd.DataFrame`
    """
    validate_granularity(granularity)
    if granularity == "sub_regional":
        template = _template_sub_regional_flow_paths(parsed_workbook_path)
    elif granularity == "regional":
        template = _template_regional_interconnectors(parsed_workbook_path)
    elif granularity == "single_region":
        template = pd.DataFrame()
    if not template.empty:
        template = template.set_index("flow_path_name")
    return template


def _template_regional_interconnectors(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    interconnector_capabilities = pd.read_csv(
        Path(parsed_workbook_path, "interconnector_transfer_capability.csv")
    )
    from_to_carrier = _get_flow_path_name_from_to_carrier(
        interconnector_capabilities.iloc[:, 0], granularity="regional"
    )
    capability_columns = _clean_capability_columns(interconnector_capabilities)
    regional_capabilities = pd.concat([from_to_carrier, capability_columns], axis=1)
    return regional_capabilities


def _template_sub_regional_flow_paths(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    flow_path_capabilities = pd.read_csv(
        Path(parsed_workbook_path, "flow_path_transfer_capability.csv")
    )
    from_to_carrier = _get_flow_path_name_from_to_carrier(
        flow_path_capabilities.iloc[:, 0], granularity="sub_regional"
    )
    capability_columns = _clean_capability_columns(flow_path_capabilities)
    sub_regional_capabilities = pd.concat([from_to_carrier, capability_columns], axis=1)
    return sub_regional_capabilities


def _get_flow_path_name_from_to_carrier(
    flow_path_name_series: pd.Series, granularity: str
) -> pd.DataFrame:
    """
    Capture the name, from-node ID, the to-node ID and determines a name
    for a flow path using regular expressions on a string `pandas.Series`
    that contains the flow path name in the forward power flow direction.

    A carrier ('AC' or 'DC') is determined based on whether the flow path descriptor
    is in _HVDC_FLOW_PATHS or goes from TAS to VIC.
    """

    from_to_desc = flow_path_name_series.str.strip().str.extract(
        # capture 2-4 capital letter code that is the from-node
        r"^(?P<node_from>[A-Z]{2,4})"
        # match em or en dashes, or hyphens and soft hyphens surrounded by spaces
        + r"\s*[\u2014\u2013\-\u00ad]+\s*"
        # capture 2-4 captial letter code that is the to-node
        + r"(?P<node_to>[A-Z]{2,4})"
        # capture optional descriptor (e.g. '("Heywood")')
        + r"\s*(?P<descriptor>.*)"
    )
    from_to_desc["carrier"] = from_to_desc.apply(
        lambda row: "DC"
        if any(
            [
                dc_line in row["descriptor"]
                for dc_line in _HVDC_FLOW_PATHS["flow_path_name"]
            ]
        )
        # manually detect Basslink since the name is not in the descriptor
        or (row["node_from"] == "TAS" and row["node_to"] == "VIC")
        else "AC",
        axis=1,
    )
    from_to_desc["flow_path_name"] = from_to_desc.apply(
        lambda row: _determine_flow_path_name(
            row.node_from, row.node_to, row.descriptor, row.carrier, granularity
        ),
        axis=1,
    )
    return from_to_desc.drop(columns=["descriptor"])


def _determine_flow_path_name(
    node_from: str, node_to: str, descriptor: str, carrier: str, granularity: str
) -> str:
    """
    Constructs flow path name
      - If the carrier is `DC`, looks for the name in `ispypsa.templater.helpers._HVDC_FLOW_PATHS`
      - Else if there is a descriptor, uses a regular expression to extract the name
      - Else constructs a name using typical NEM naming conventing based on `granularity`
        - First letter of `node_from`, first of `node_to` followed by "I" (interconnector)
          if `granularity` is `regional`
        - `<node_from>-<node_to> if `granularity` is `sub_regional`
    """
    if carrier == "DC":
        name = _HVDC_FLOW_PATHS.loc[
            (_HVDC_FLOW_PATHS.node_from == node_from)
            & (_HVDC_FLOW_PATHS.node_to == node_to),
            "flow_path_name",
        ].iat[0]
    elif descriptor and (
        match := re.search(
            # unicode characters here refer to quotation mark and left/right
            # quotation marks
            r"\(([\w\u0022\u201c\u201d]+)\)",
            descriptor,
        )
    ):
        name = match.group(1).strip('"').lstrip("\u201c").rstrip("\u201d")
    else:
        if granularity == "regional":
            name = node_from[0] + node_to[0] + "I"
        elif granularity == "sub_regional":
            name = node_from + "-" + node_to
    return name


def _clean_capability_columns(capability_df: pd.DataFrame) -> dict:
    """
    Cleans flow path capability column names (e.g. drops references to notes) and
    converts column values with notes or string-like value (e.g. "1,250") to integers
    """
    capabilities = []
    for direction in ("Forward direction", "Reverse direction"):
        direction_cols = [
            col for col in capability_df.columns if direction in col and "(MW)" in col
        ]
        for col in direction_cols:
            qualifier = re.search(r".*_([A-Za-z\s]+)$", col).group(1)
            col_name = _snakecase_string(direction + " (MW) " + qualifier)
            capabilities.append(_get_mw_capability(capability_df[col]).rename(col_name))
    return pd.concat(capabilities, axis=1)


def _get_mw_capability(mw_capability_series: pd.Series) -> pd.Series:
    """
    Capture the MW capability approximation from a string `pandas.Series` that contains
    a MW value and may also contain a note or qualification, and then convert values
    to integers. The returned `pandas.Series` has the name
    '<column_qualifier>_capability_approximation_mw'.
    """
    mw_capability = mw_capability_series.str.extract(
        r"^(?P<capability_approximation_mw>[0-9\,]+).*", expand=False
    )
    mw_capability = mw_capability.str.replace(",", "")
    return pd.to_numeric(mw_capability, downcast="integer")