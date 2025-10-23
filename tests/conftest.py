import io
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture(scope="module")
def workbook_table_cache_test_path():
    return Path("tests", "test_workbook_table_cache")


@pytest.fixture
def csv_str_to_df():
    def func(csv_str, **kwargs):
        """Helper function to convert a CSV string to a DataFrame."""
        # Remove spaces and tabs that have been included for readability.
        csv_str = csv_str.replace(" ", "").replace("\t", "").replace("__", " ")
        return pd.read_csv(io.StringIO(csv_str), **kwargs)

    return func


@pytest.fixture
def sample_ispypsa_tables(csv_str_to_df):
    """
    Fixture that returns a dictionary of dataframes needed to run create_pypsa_friendly_inputs.

    This example set has 2 subregions (CNSW and NNSW) each with one REZ.
    All tables from _BASE_TEMPLATE_OUTPUTS are included with appropriate columns and sample data.
    """
    tables = {}

    # Sub-regions table
    sub_regions_csv = """
    isp_sub_region_id,  nem_region_id,  sub_region_reference_node,         sub_region_reference_node_voltage_kv,  substation_latitude,  substation_longitude
    CNSW,               NSW1,           Sydney__West__330__kV,             330,                                   -33.8,                150.8
    NNSW,               NSW1,           Tamworth__330__kV,                 330,                                   -31.1,                150.9
    """
    tables["sub_regions"] = csv_str_to_df(sub_regions_csv)

    # NEM regions table
    nem_regions_csv = """
    NEM__Region,  Regional__Reference__Node,    ISP__Sub-region
    NSW1,         Sydney__West__330__kV,        Central__NSW__(CNSW)
    """
    tables["nem_regions"] = csv_str_to_df(nem_regions_csv)

    # Renewable energy zones table
    renewable_energy_zones_csv = """
    rez_id,                    isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_solar_resource_limit_violation_penalty_factor_$/mw,  rez_transmission_network_limit_summer_typical
    Central-West__Orana__REZ,  CNSW,               AC,       3000,                                  2000,                                    0,                                                   0,                                                1500,                                          10000,                                                    4500
    New__England__REZ,         NNSW,               AC,       5000,                                  3500,                                    0,                                                   0,                                                2000,                                          10000,                                                    6000
    """
    tables["renewable_energy_zones"] = csv_str_to_df(renewable_energy_zones_csv)

    # Flow paths table
    flow_paths_csv = """
    flow_path,    carrier,  node_from,  node_to,  forward_direction_mw_summer_typical,  reverse_direction_mw_summer_typical
    CNSW-NNSW,    AC,       CNSW,       NNSW,     1000,                                 1000
    """
    tables["flow_paths"] = csv_str_to_df(flow_paths_csv)

    # ECAA generators table
    ecaa_generators_csv = """
    generator,                        sub_region_id,  region_id,  fuel_type,    maximum_capacity_mw
    Bayswater,                        CNSW,           NSW1,       Black__Coal,  2640
    Eraring,                          CNSW,           NSW1,       Black__Coal,  2880
    Central-West__Orana__REZ__Wind,   CNSW,           NSW1,       Wind,         0
    Central-West__Orana__REZ__Solar,  CNSW,           NSW1,       Solar,        0
    New__England__REZ__Wind,          NNSW,           NSW1,       Wind,         0
    New__England__REZ__Solar,         NNSW,           NSW1,       Solar,        0
    """
    tables["ecaa_generators"] = csv_str_to_df(ecaa_generators_csv)

    # REZ group constraints LHS table
    custom_constraints_lhs = """
    constraint_id,                     term_type,           variable_name,                      coefficient
    Central-West__Orana__REZ_Custom,   generator_capacity,  Central-West__Orana__REZ__Wind,     1.0
    Central-West__Orana__REZ_Custom,   generator_capacity,  Central-West__Orana__REZ__Solar,    1.0
    New__England__REZ_Custom,          generator_capacity,  New__England__REZ__Wind,            1.0
    New__England__REZ_Custom,          generator_capacity,  New__England__REZ__Solar,           1.0
    """
    tables["custom_constraints_lhs"] = csv_str_to_df(custom_constraints_lhs)

    # REZ group constraints RHS table
    custom_constraints_rhs = """
    constraint_id,                     summer_typical
    Central-West__Orana__REZ_Custom,   4500
    New__England__REZ_Custom,          6000
    """
    tables["custom_constraints_rhs"] = csv_str_to_df(custom_constraints_rhs)

    # Optional tables that might be needed for transmission expansion
    flow_path_expansion_costs_csv = """
    flow_path,    additional_network_capacity_mw,  2025_26_$/mw,  2026_27_$/mw,  2027_28_$/mw
    CNSW-NNSW,    500,                             1200,          1250,          1300
    """
    tables["flow_path_expansion_costs"] = csv_str_to_df(flow_path_expansion_costs_csv)

    rez_transmission_expansion_costs_csv = """
    rez_constraint_id,                 additional_network_capacity_mw,  2025_26_$/mw,  2026_27_$/mw,  2027_28_$/mw
    Central-West__Orana__REZ,          1000,                            2000,          2100,          2200
    New__England__REZ,                 1500,                            2500,          2600,          2700
    Central-West__Orana__REZ_Custom,   1000,                            2000,          2100,          2200
    New__England__REZ_Custom,          1500,                            2500,          2600,          2700
    """
    tables["rez_transmission_expansion_costs"] = csv_str_to_df(
        rez_transmission_expansion_costs_csv
    )

    return tables


@pytest.fixture
def sample_model_config():
    """
    Fixture that returns a sample ModelConfig for testing.

    This config uses:
    - Sub-regional granularity with discrete REZ nodes
    - Financial year type
    - 2 investment periods (2024, 2030)
    - REZ transmission expansion disabled to avoid empty dataframe issues
    - Unserved energy enabled
    """
    from ispypsa.config import ModelConfig
    from ispypsa.config.validators import (
        NetworkConfig,
        NodesConfig,
        PathsConfig,
        TemporalAggregationConfig,
        TemporalCapacityInvestmentConfig,
        TemporalConfig,
        TemporalOperationalConfig,
        TemporalRangeConfig,
        UnservedEnergyConfig,
    )

    return ModelConfig(
        paths=PathsConfig(
            ispypsa_run_name="test_run",
            parsed_traces_directory="NOT_SET_FOR_TESTING",
            parsed_workbook_cache="",
            workbook_path="",
            run_directory="",
        ),
        scenario="Step Change",
        wacc=0.06,
        discount_rate=0.05,
        iasr_workbook_version="6.0",
        solver="highs",
        temporal=TemporalConfig(
            year_type="fy",
            range=TemporalRangeConfig(start_year=2026, end_year=2028),
            capacity_expansion=TemporalCapacityInvestmentConfig(
                reference_year_cycle=[2024],
                resolution_min=30,
                aggregation=TemporalAggregationConfig(representative_weeks=None),
                investment_periods=[2026, 2028],
            ),
            operational=TemporalOperationalConfig(
                reference_year_cycle=[2024],
                resolution_min=30,
                aggregation=TemporalAggregationConfig(representative_weeks=None),
                horizon=24,
                overlap=0,
            ),
        ),
        network=NetworkConfig(
            nodes=NodesConfig(
                regional_granularity="sub_regions", rezs="discrete_nodes"
            ),
            annuitisation_lifetime=25,
            transmission_expansion=True,
            rez_transmission_expansion=True,
            rez_to_sub_region_transmission_default_limit=1000000.0,
        ),
        unserved_energy=UnservedEnergyConfig(cost=10000, max_per_node=10000),
    )
