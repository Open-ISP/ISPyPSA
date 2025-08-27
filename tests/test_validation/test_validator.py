"""Test the validation module for ISPyPSA templater outputs."""

from pathlib import Path

import pandas as pd
import pytest

from ispypsa.validation import validate_ispypsa_inputs


def test_validate_ispypsa_inputs_success(csv_str_to_df):
    """Test successful validation of all three tables."""

    # Create valid sub_regions table
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    NQ,                 QLD
    CQ,                 QLD
    SQ,                 QLD
    VIC,                VIC
    """

    # Create valid flow_paths table
    flow_paths_csv = """
    flow_path,  node_from,  node_to,  carrier,  forward_direction_mw_summer_typical
    NQ-CQ,      NQ,         CQ,       AC,       1000
    CQ-SQ,      CQ,         SQ,       AC,       1500
    SQ-VIC,     SQ,         VIC,      AC,       2000
    """

    # Create valid renewable_energy_zones table
    rez_csv = """
    rez_id,  isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_solar_resource_limit_violation_penalty_factor_$/mw,  rez_transmission_network_limit_summer_typical
    Q1,      NQ,                 AC,       570.0,                                  1710.0,                                   0.0,                                                 0.0,                                              1100.0,                                        288711.0,                                                 750.0
    Q2,      NQ,                 AC,       4700.0,                                 13900.0,                                  0.0,                                                 0.0,                                              0.0,                                           288711.0,                                                 NaN
    Q3,      CQ,                 AC,       0.0,                                    0.0,                                      0.0,                                                 0.0,                                              3400.0,                                        NaN,                                                      1000.0
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
        "flow_paths": csv_str_to_df(flow_paths_csv),
        "renewable_energy_zones": csv_str_to_df(rez_csv),
    }

    # Should not raise any exception
    validate_ispypsa_inputs(ispypsa_tables)


def test_validate_ispypsa_inputs_missing_table(csv_str_to_df):
    """Test validation fails when a table that has a config is missing."""

    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    NQ,                 QLD
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv)
        # Missing flow_paths and renewable_energy_zones which have configs
    }

    with pytest.raises(ValueError, match="has a validation config but is not found"):
        validate_ispypsa_inputs(ispypsa_tables)


def test_validate_ispypsa_inputs_duplicate_rows(csv_str_to_df):
    """Test validation fails when there are duplicate sub_region_ids."""

    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    NQ,                 QLD
    NQ,                 QLD
    CQ,                 QLD
    """

    flow_paths_csv = """
    flow_path,  node_from,  node_to,  carrier,  forward_direction_mw_summer_typical
    NQ-CQ,      NQ,         CQ,       AC,       1000
    """

    rez_csv = """
    rez_id,  isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_solar_resource_limit_violation_penalty_factor_$/mw,  rez_transmission_network_limit_summer_typical
    Q1,      NQ,                 AC,       570.0,                                  1710.0,                                   0.0,                                                 0.0,                                              1100.0,                                        288711.0,                                                 750.0
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
        "flow_paths": csv_str_to_df(flow_paths_csv),
        "renewable_energy_zones": csv_str_to_df(rez_csv),
    }

    with pytest.raises(ValueError, match="Validation failed for table 'sub_regions'"):
        validate_ispypsa_inputs(ispypsa_tables)


def test_validate_ispypsa_inputs_invalid_nem_region(csv_str_to_df):
    """Test validation fails when nem_region_id has invalid value."""

    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    NQ,                 QLD
    XX,                 INVALID
    """

    flow_paths_csv = """
    flow_path,  node_from,  node_to,  carrier,  forward_direction_mw_summer_typical
    NQ-XX,      NQ,         XX,       AC,       1000
    """

    rez_csv = """
    rez_id,  isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_solar_resource_limit_violation_penalty_factor_$/mw,  rez_transmission_network_limit_summer_typical
    Q1,      NQ,                 AC,       570.0,                                  1710.0,                                   0.0,                                                 0.0,                                              1100.0,                                        288711.0,                                                 750.0
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
        "flow_paths": csv_str_to_df(flow_paths_csv),
        "renewable_energy_zones": csv_str_to_df(rez_csv),
    }

    with pytest.raises(ValueError, match="Validation failed for table 'sub_regions'"):
        validate_ispypsa_inputs(ispypsa_tables)


def test_validate_ispypsa_inputs_referential_integrity(csv_str_to_df):
    """Test validation fails when flow_paths references non-existent sub_region."""

    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    NQ,                 QLD
    CQ,                 QLD
    """

    # XX is not in sub_regions
    flow_paths_csv = """
    flow_path,  node_from,  node_to,  carrier,  forward_direction_mw_summer_typical
    NQ-CQ,      NQ,         CQ,       AC,       1000
    CQ-XX,      CQ,         XX,       AC,       1500
    """

    rez_csv = """
    rez_id,  isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_solar_resource_limit_violation_penalty_factor_$/mw,  rez_transmission_network_limit_summer_typical
    Q1,      NQ,                 AC,       570.0,                                  1710.0,                                   0.0,                                                 0.0,                                              1100.0,                                        288711.0,                                                 750.0
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
        "flow_paths": csv_str_to_df(flow_paths_csv),
        "renewable_energy_zones": csv_str_to_df(rez_csv),
    }

    with pytest.raises(
        ValueError, match="Referential integrity violation.*node_to.*invalid values.*XX"
    ):
        validate_ispypsa_inputs(ispypsa_tables)


def test_validate_ispypsa_inputs_negative_capacity(csv_str_to_df):
    """Test validation fails when capacity values are negative."""

    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    NQ,                 QLD
    CQ,                 QLD
    """

    flow_paths_csv = """
    flow_path,  node_from,  node_to,  carrier,  forward_direction_mw_summer_typical
    NQ-CQ,      NQ,         CQ,       AC,       -1000
    """

    rez_csv = """
    rez_id,  isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_solar_resource_limit_violation_penalty_factor_$/mw,  rez_transmission_network_limit_summer_typical
    Q1,      NQ,                 AC,       570.0,                                  1710.0,                                   0.0,                                                 0.0,                                              1100.0,                                        288711.0,                                                 750.0
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
        "flow_paths": csv_str_to_df(flow_paths_csv),
        "renewable_energy_zones": csv_str_to_df(rez_csv),
    }

    with pytest.raises(ValueError, match="Validation failed for table 'flow_paths'"):
        validate_ispypsa_inputs(ispypsa_tables)


def test_validate_ispypsa_inputs_ignores_tables_without_configs(csv_str_to_df):
    """Test that tables without validation configs are ignored."""

    # Create valid tables that have configs
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    NQ,                 QLD
    CQ,                 QLD
    """

    flow_paths_csv = """
    flow_path,  node_from,  node_to,  carrier,  forward_direction_mw_summer_typical
    NQ-CQ,      NQ,         CQ,       AC,       1000
    """

    rez_csv = """
    rez_id,  isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_solar_resource_limit_violation_penalty_factor_$/mw,  rez_transmission_network_limit_summer_typical
    Q1,      NQ,                 AC,       570.0,                                  1710.0,                                   0.0,                                                 0.0,                                              1100.0,                                        288711.0,                                                 750.0
    """

    # Add a table that doesn't have a validation config
    some_other_table_csv = """
    id,  value
    1,   100
    2,   200
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
        "flow_paths": csv_str_to_df(flow_paths_csv),
        "renewable_energy_zones": csv_str_to_df(rez_csv),
        "some_other_table": csv_str_to_df(some_other_table_csv),  # No config for this
    }

    # Should not raise any exception - the table without config should be ignored
    validate_ispypsa_inputs(ispypsa_tables)


def test_validate_ispypsa_inputs_yaml_driven_referential_integrity(csv_str_to_df):
    """Test that referential integrity checks are driven by YAML definitions."""

    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id
    NQ,                 QLD
    CQ,                 QLD
    """

    # REZ that references non-existent sub-region
    rez_csv = """
    rez_id,  isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_solar_resource_limit_violation_penalty_factor_$/mw,  rez_transmission_network_limit_summer_typical
    Q1,      XX,                 AC,       570.0,                                  1710.0,                                   0.0,                                                 0.0,                                              1100.0,                                        288711.0,                                                 750.0
    """

    # Valid flow_paths to avoid other failures
    flow_paths_csv = """
    flow_path,  node_from,  node_to,  carrier,  forward_direction_mw_summer_typical
    NQ-CQ,      NQ,         CQ,       AC,       1000
    """

    ispypsa_tables = {
        "sub_regions": csv_str_to_df(sub_regions_csv),
        "flow_paths": csv_str_to_df(flow_paths_csv),
        "renewable_energy_zones": csv_str_to_df(rez_csv),
    }

    # Should fail on REZ referential integrity check defined in YAML
    with pytest.raises(
        ValueError,
        match="Referential integrity violation.*renewable_energy_zones.isp_sub_region_id.*XX.*REZ must be located in a valid sub-region",
    ):
        validate_ispypsa_inputs(ispypsa_tables)
