import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .helpers import _snakecase_string


def new_function_to_test_code_cove(a):
    return a * 2


def template_renewable_energy_zones_locations(
    parsed_workbook_path: Path | str, location_mapping_only: bool = True
) -> pd.DataFrame:
    """Creates a mapping table that specifies which regions and sub regions renewable
       energy zones belong to.

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


def template_rez_build_limits(
    parsed_workbook_path: Path | str,
    expansion_on: bool,
) -> pd.DataFrame:
    """Create a template for renewable energy zones that contains data on resource and
    transmission limits and transmission expansion costs.

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.
        expansion_on: bool indicating if transmission line expansion is considered.

    Returns:
        `pd.DataFrame`: REZ table resource and transmission limits table
    """
    logging.info("Creating a rez_build_limits template")
    rez_build_limits = pd.read_csv(
        Path(parsed_workbook_path, "initial_build_limits.csv")
    )
    rez_build_limits.columns = [
        _snakecase_string(col) for col in rez_build_limits.columns
    ]
    rez_build_limits = rez_build_limits.rename(
        columns={
            "isp_sub_region": "isp_sub_region_id",
        }
    )
    cols_to_pass_to_float = [
        col
        for col in rez_build_limits.columns
        if col not in ["rez_id", "isp_sub_region_id"]
    ]
    for col in cols_to_pass_to_float:
        rez_build_limits[col] = pd.to_numeric(rez_build_limits[col], errors="coerce")
    cols_where_zero_goes_to_nan = [
        "rez_resource_limit_violation_penalty_factor_$m/mw",
        "indicative_transmission_expansion_cost_$m/mw",
        "indicative_transmission_expansion_cost_$m/mw_tranche_2",
        "indicative_transmission_expansion_cost_$m/mw_tranche_3",
    ]
    for col in cols_where_zero_goes_to_nan:
        rez_build_limits.loc[rez_build_limits[col] == 0.0, col] = np.nan
    rez_build_limits = combine_transmission_expansion_cost_to_one_column(
        rez_build_limits
    )
    rez_build_limits = process_transmission_limit(rez_build_limits)
    cols_where_nan_goes_to_zero = [
        "wind_generation_total_limits_mw_high",
        "wind_generation_total_limits_mw_medium",
        "wind_generation_total_limits_mw_offshore_floating",
        "wind_generation_total_limits_mw_offshore_fixed",
        "solar_pv_plus_solar_thermal_limits_mw_solar",
    ]
    for col in cols_where_nan_goes_to_zero:
        rez_build_limits[col] = rez_build_limits[col].fillna(0.0)
    rez_build_limits = convert_cost_units(
        rez_build_limits, "rez_resource_limit_violation_penalty_factor_$m/mw"
    )
    rez_build_limits = convert_cost_units(
        rez_build_limits, "indicative_transmission_expansion_cost_$m/mw"
    )
    rez_build_limits = rez_build_limits.rename(
        columns={
            "indicative_transmission_expansion_cost_$m/mw": "indicative_transmission_expansion_cost_$/mw",
            "rez_resource_limit_violation_penalty_factor_$m/mw": "rez_solar_resource_limit_violation_penalty_factor_$/mw",
        }
    )
    rez_build_limits = rez_build_limits.loc[
        :,
        [
            "rez_id",
            "isp_sub_region_id",
            "wind_generation_total_limits_mw_high",
            "wind_generation_total_limits_mw_medium",
            "wind_generation_total_limits_mw_offshore_floating",
            "wind_generation_total_limits_mw_offshore_fixed",
            "solar_pv_plus_solar_thermal_limits_mw_solar",
            "rez_solar_resource_limit_violation_penalty_factor_$/mw",
            # Remove while not being used.
            # "rez_transmission_network_limit_peak_demand",
            "rez_transmission_network_limit_summer_typical",
            # Remove while not being used.
            # "rez_transmission_network_limit_winter_reference",
            "indicative_transmission_expansion_cost_$/mw",
        ],
    ]
    if not expansion_on:
        rez_build_limits = rez_build_limits.drop(
            columns=["indicative_transmission_expansion_cost_$/mw"]
        )
    rez_build_limits = rez_build_limits.set_index("rez_id", drop=True)
    return rez_build_limits


def process_transmission_limit(data):
    """Replace 0.0 MW Transmission limits with nan if there is not a cost given for
    expansion.
    """
    cols = [
        "rez_transmission_network_limit_peak_demand",
        "rez_transmission_network_limit_summer_typical",
        "rez_transmission_network_limit_winter_reference",
    ]
    for col in cols:
        replacement_check = data[
            "indicative_transmission_expansion_cost_$m/mw"
        ].isna() & (data[col] == 0.0)
        data.loc[replacement_check, col] = np.nan
    return data


def combine_transmission_expansion_cost_to_one_column(data):
    """The model can only utilise a single transmission expansion cost. If the tranche
    1 column is nan then this function adopts the tranche 2 cost if it is not
    nan. The process is repeated with tranche 3 if the cost is still nan.
    """
    tranche_one = "indicative_transmission_expansion_cost_$m/mw"
    tranche_two = "indicative_transmission_expansion_cost_$m/mw_tranche_2"
    tranche_three = "indicative_transmission_expansion_cost_$m/mw_tranche_3"

    first_replacement_check = data[tranche_one].isna() & ~data[tranche_two].isna()
    data.loc[first_replacement_check, tranche_one] = data.loc[
        first_replacement_check, tranche_two
    ]
    second_replacement_check = data[tranche_one].isna() & ~data[tranche_three].isna()
    data.loc[second_replacement_check, tranche_one] = data.loc[
        second_replacement_check, tranche_three
    ]
    return data


def convert_cost_units(data, column):
    """Convert cost from millions of dollars per MW to $/MW"""
    data[column] = data[column] * 1e6
    return data
