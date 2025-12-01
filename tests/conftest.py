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
        csv_str = csv_str.replace("__", " ")
        return pd.read_csv(
            io.StringIO(csv_str),
            # Sep matches: <optional space> + <COMMA> + <optional space>
            sep=r"\s*,\s*",
            # Engine must be python to use multi-char/regex separators
            engine="python",
            **kwargs,
        )

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
    CNSW,               NSW,            Sydney__West__330__kV,             330,                                   -33.8,                150.8
    NNSW,               NSW,            Tamworth__330__kV,                 330,                                   -31.1,                150.9
    """
    tables["sub_regions"] = csv_str_to_df(sub_regions_csv)

    # NEM regions table
    nem_regions_csv = """
    NEM__Region,  Regional__Reference__Node,    ISP__Sub-region
    NSW,          Sydney__West__330__kV,        Central__NSW__(CNSW)
    """
    tables["nem_regions"] = csv_str_to_df(nem_regions_csv)

    # Renewable energy zones table
    renewable_energy_zones_csv = """
    rez_id,                    isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,  wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_resource_limit_violation_penalty_factor_$/mw,  rez_transmission_network_limit_summer_typical
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
    generator,                        technology_type,            region_id,  sub_region_id,   fuel_type,     fuel_cost_mapping,  minimum_load_mw,  vom_$/mwh_sent_out,  heat_rate_gj/mwh,  commissioning_date,  closure_year,      maximum_capacity_mw,   rez_id
    Bayswater,                        Steam__Sub__Critical,       NSW,        CNSW,            Black__Coal,   Bayswater,          250,              5.0,                 10.0,              NaN,                 2033,              2640,                  NaN
    Eraring,                          Steam__Sub__Critical,       NSW,        CNSW,            Black__Coal,   Eraring,            210,              5.0,                 10.0,              NaN,                 2029,              2880,                  NaN
    Bodangora__Wind__Farm,            Wind,                       NSW,        CNSW,            Wind,          Wind,               0,                0.0,                 0.0,               NaN,                 2045,              250,                   N3
    Central-West__Orana__REZ__Solar,  Large__scale__Solar__PV,    NSW,        CNSW,            Solar,         Solar,              0,                0.0,                 0.0,               2025,                2055,              200,                   N3
    New__England__REZ__Wind,          Wind,                       NSW,        NNSW,            Wind,          Wind,               0,                0.0,                 0.0,               2028,                2058,              500,                   N2
    Moree__Solar__Farm,               Large__scale__Solar__PV,    NSW,        NNSW,            Solar,         Solar,              0,                0.0,                 0.0,               NaN,                 2065,              320,                   N2
    """
    tables["ecaa_generators"] = csv_str_to_df(ecaa_generators_csv)

    # New entrant generators table
    new_entrant_generators_df = pd.DataFrame(
        {
            "generator_name": [
                "CCGT",
                "OCGT (small GT)",
                "Large scale Solar PV",
                "Wind",
                "Large scale Solar PV",
                "Wind",
            ],
            "technology_type": [
                "CCGT",
                "OCGT (small GT)",
                "Large scale Solar PV",
                "Wind",
                "Large scale Solar PV",
                "Wind",
            ],
            "region_id": ["NSW", "NSW", "NSW", "NSW", "NSW", "NSW"],
            "sub_region_id": ["CNSW", "CNSW", "CNSW", "CNSW", "NNSW", "NNSW"],
            "fuel_type": ["Gas", "Gas", "Solar", "Wind", "Solar", "Wind"],
            "fuel_cost_mapping": [
                "NSW new CCGT",
                "NSW new OCGT",
                "Solar",
                "Wind",
                "Solar",
                "Wind",
            ],
            "minimum_stable_level_%": [46, 0, 0, 0, 0, 0],
            "vom_$/mwh_sent_out": [4.0, 15.0, 0, 0, 0, 0],
            "heat_rate_gj/mwh": [7.0, 10.0, 0, 0, 0, 0],
            "maximum_capacity_mw": [400, 250, None, None, None, None],
            "unit_capacity_mw": [100, 50, None, None, None, None],
            "lifetime": [40, 40, 30, 30, 30, 30],
            "connection_cost_technology": [
                "CCGT",
                "Small OCGT2",
                "Large scale Solar PV",
                "Wind",
                "Large scale Solar PV",
                "Wind",
            ],
            "connection_cost_rez/_region_id": [
                "NSW",
                "NSW",
                "Central-West Orana",
                "Central-West Orana",
                "New England",
                "New England",
            ],
            "fom_$/kw/annum": [12.0, 15.0, 20.0, 30.0, 20.0, 30.0],
            "technology_specific_lcf_%": [100.0, 103.0, 107.0, 105.0, 101.0, 99.0],
            "isp_resource_type": [
                "CCGT",
                "OCGT (small GT)",
                "SAT",
                "WH",
                "SAT",
                "WH",
            ],
            "rez_id": [None, None, "N3", "N3", "N2", "N2"],
            "generator": [
                "ccgt_cnsw",
                "ocgt_small_gt_cnsw",
                "large_scale_solar_pv_n3_sat",
                "wind_n3_wh",
                "large_scale_solar_pv_n2_sat",
                "wind_n2_wh",
            ],
        }
    )
    tables["new_entrant_generators"] = new_entrant_generators_df

    # Additional tables needed for new entrant generators:
    new_entrant_build_costs_csv = """
    technology,                         2023_24_$/mw,   2024_25_$/mw,   2025_26_$/mw,   2026_27_$/mw,   2027_28_$/mw,   2028_29_$/mw
    CCGT,                               1900000,        1850000,        1800000,        1750000,        1700000,        1650000
    OCGT__(small__GT),                  1600000,        1700000,        1650000,        1700000,        1750000,        1800000
    Large__scale__Solar__PV,            1700000,        1600000,        1500000,        1400000,        1300000,        1200000
    Wind,                               2900000,        2800000,        2700000,        2600000,        2500000,        2400000
    Battery__Storage__(2hrs__storage),  3000000,        2900000,        2800000,        2700000,        2600000,        2500000
    Battery__Storage__(4hrs__storage),  4000000,        3900000,        3800000,        3700000,        3600000,        3500000
    """
    tables["new_entrant_build_costs"] = csv_str_to_df(new_entrant_build_costs_csv)

    new_entrant_wind_and_solar_connection_costs_csv = """
    REZ__names,               2023_24_$/mw,  2024_25_$/mw,  2025_26_$/mw, 2026_27_$/mw,  2027_28_$/mw,   2028_29_$/mw,  system_strength_connection_cost_$/mw
    Central-West__Orana,      150000,        140000,       130000,        120000,         110000,        100000,        137000
    New__England,             120000,        120000,       120000,        120000,         120000,        120000,        137000
    Wide__Bay,                120000,        120000,       120000,        120000,         120000,        120000,        137000
    """
    tables["new_entrant_wind_and_solar_connection_costs"] = csv_str_to_df(
        new_entrant_wind_and_solar_connection_costs_csv
    )

    new_entrant_non_vre_connection_costs_csv = """
    Region,  ccgt_$/mw,  small_ocg_t2_$/mw
    NSW,     85000,      85000
    """
    tables["new_entrant_non_vre_connection_costs"] = csv_str_to_df(
        new_entrant_non_vre_connection_costs_csv
    )

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

    # Coal prices
    coal_prices_csv = """
    generator,        2023_24_$/gj,  2024_25_$/gj,  2025_26_$/gj,  2026_27_$/gj,  2027_28_$/gj,  2028_29_$/gj
    Eraring,          1.0,           2.0,           3.0,           4.0,           5.0,           6.0
    Bayswater,        1.0,           2.0,           3.0,           4.0,           5.0,           6.0
    """
    tables["coal_prices"] = csv_str_to_df(coal_prices_csv)

    gas_prices_csv = """
    generator,        2023_24_$/gj,  2024_25_$/gj,  2025_26_$/gj,  2026_27_$/gj,  2027_28_$/gj,  2028_29_$/gj
    NSW__new__CCGT,   20.0,          21.0,          22.0,          23.0,          24.0,          25.0
    NSW__new__OCGT,   20.0,          21.0,          22.0,          23.0,          24.0,          25.0
    """
    tables["gas_prices"] = csv_str_to_df(gas_prices_csv)

    # Biomethane prices
    biomethane_prices_csv = """
    2023_24_$/gj,  2024_25_$/gj,  2025_26_$/gj,  2026_27_$/gj,  2027_28_$/gj,  2028_29_$/gj
    40.0,          35.0,          30.0,          30.0,          25.0,          20.0
    """
    tables["biomethane_prices"] = csv_str_to_df(biomethane_prices_csv)

    # GPG emissions reduction biomethane
    gpg_emissions_reduction_biomethane_csv = """
    2023_24_%,  2024_25_%,  2025_26_%,  2026_27_%,  2027_28_%,  2028_29_%
    90,         80,         70,         60,         50,         40
    """
    tables["gpg_emissions_reduction_biomethane"] = csv_str_to_df(
        gpg_emissions_reduction_biomethane_csv
    )

    # ECAA battery table:
    ecaa_batteries_csv = """
    storage_name, sub_region_id, region_id, rez_id, commissioning_date, closure_year, maximum_capacity_mw,  charging_efficiency_%,  discharging_efficiency_%,   storage_duration_hours, fuel_type,  isp_resource_type
    Battery1,     CNSW,          NSW,       ,       2020-01-01,         2040,         100,                  95.0,                   95.0,                       4,                      Battery,    Battery__Storage__4h
    Battery2,     NNSW,          NSW,       ,       2022-07-1,          2042,         200,                  90.0,                   90.0,                       2,                      Battery,    Battery__Storage__2h
    """
    tables["ecaa_batteries"] = csv_str_to_df(ecaa_batteries_csv)

    # New entrant battery table:
    new_entrant_batteries_csv = """
    storage_name,          sub_region_id, region_id,       rez_id,     technology_type,                     lifetime,    charging_efficiency_%,  discharging_efficiency_%,   storage_duration_hours, fuel_type,  technology_specific_lcf_%,     connection_cost_$/mw,   fom_$/kw/annum,     isp_resource_type
    NewBattery1,           CNSW,          NSW,             ,           Battery__Storage__(2hrs__storage),   20,          90.0,                   90.0,                       2,                      Battery,    100.0,                         55000.0,                10.0,               Battery__Storage__2h
    NewBattery2,           CNSW,          NSW,             ,           Battery__Storage__(2hrs__storage),   20,          95.0,                   95.0,                       2,                      Battery,    100.0,                         55000.0,                7.0,                Battery__Storage__2h
    """
    tables["new_entrant_batteries"] = csv_str_to_df(new_entrant_batteries_csv)

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


@pytest.fixture
def sample_generator_translator_tables(csv_str_to_df):
    """
    Fixture that returns a dictionary of dataframes needed to test individual generator
    translator functions. Contains sample data for gas, coal, biomass, hydrogen, biomethane,
    and liquid fuel prices, as well as emissions reduction data and a sample generators dataframe.
    """
    tables = {}

    # Gas prices
    gas_prices_csv = """
    generator,        2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj
    Bairnsdale,       20.0,          21.0,          22.0
    SA__new__CCGT,    20.0,          21.0,          22.0
    QLD__new__OCGT,   20.0,          21.0,          22.0
    """
    tables["gas_prices"] = csv_str_to_df(gas_prices_csv)

    # Coal prices
    coal_prices_csv = """
    generator,        2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj
    Eraring,          1.0,           2.0,           3.0
    """
    tables["coal_prices"] = csv_str_to_df(coal_prices_csv)

    # Biomass prices
    biomass_prices_csv = """
    2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj
    0.5,           0.4,           0.3
    """
    tables["biomass_prices"] = csv_str_to_df(biomass_prices_csv)

    # Hydrogen prices
    hydrogen_prices_csv = """
    2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj
    45.0,          43.0,          41.0
    """
    tables["hydrogen_prices"] = csv_str_to_df(hydrogen_prices_csv)

    # Biomethane prices
    biomethane_prices_csv = """
    2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj
    40.0,          35.0,          30.0
    """
    tables["biomethane_prices"] = csv_str_to_df(biomethane_prices_csv)

    # Liquid fuel prices
    liquid_fuel_prices_csv = """
    2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj
    25.0,          26.0,          27.0
    """
    tables["liquid_fuel_prices"] = csv_str_to_df(liquid_fuel_prices_csv)

    # GPG emissions reduction H2
    gpg_emissions_reduction_h2_csv = """
    generator,     2022_23_%,  2023_24_%,  2024_25_%
    Kogan__Gas,    100,        98,         95
    """
    tables["gpg_emissions_reduction_h2"] = csv_str_to_df(gpg_emissions_reduction_h2_csv)

    # GPG emissions reduction biomethane
    gpg_emissions_reduction_biomethane_csv = """
    2022_23_%,  2023_24_%,  2024_25_%
    90,         80,         70
    """
    tables["gpg_emissions_reduction_biomethane"] = csv_str_to_df(
        gpg_emissions_reduction_biomethane_csv
    )

    # Sample new entrant build costs
    new_entrant_build_costs = """
    technology,                    2022_23_$/mw,  2023_24_$/mw,  2024_25_$/mw
    Large__scale__Solar__PV,       1600000,       1500000,       1400000
    CCGT,                          2000000,       1950000,       1900000
    Wind,                          1800000,       1700000,       1500000
    Wind__-__offshore__(floating), 4500000,       4400000,       4300000
    Biomass,                       3100000,       2900000,       2500000
    """
    tables["new_entrant_build_costs"] = csv_str_to_df(new_entrant_build_costs)

    # Sample new entrant connection costs for VRE
    new_entrant_wind_and_solar_connection_costs = """
    REZ__names,               2022_23_$/mw,  2023_24_$/mw, 2024_25_$/mw, system_strength_connection_cost_$/mw
    Far__North__Queensland,   150000,        140000,       130000,       137000
    Leigh__Creek,             120000,        120000,       120000,       137000
    Tumut,                    200000,        210000,       220000,       137000
    Wide__Bay,                120000,        120000,       120000,       137000
    """
    tables["new_entrant_wind_and_solar_connection_costs"] = csv_str_to_df(
        new_entrant_wind_and_solar_connection_costs
    )

    # Sample new entrant connection costs for non-VRE
    new_entrant_non_vre_connection_costs = """
    Region,   ccgt_$/mw,    biomass_$/mw
    QLD,      110000,       120000
    NSW,      85000,        100000
    SA,       110000,       130000
    """
    tables["new_entrant_non_vre_connection_costs"] = csv_str_to_df(
        new_entrant_non_vre_connection_costs
    )

    # Generators dataframe
    generators_df_csv = """
    name,                      carrier,       isp_fuel_cost_mapping
    Bairnsdale,                Gas,           Bairnsdale
    Eraring,                   Black__Coal,   Eraring
    SA__new__CCGT,             Gas,           SA__new__CCGT
    Kogan__Gas,                Hyblend,       QLD__new__OCGT
    Large__Scale__Solar__PV,   Solar,         Large__Scale__Solar__PV
    """
    tables["translated_generators_df"] = csv_str_to_df(generators_df_csv)

    return tables


@pytest.fixture
def translated_generator_column_order():
    translated_column_orders = dict(
        ecaa_column_order=[
            "name",
            "bus",
            "p_nom",
            "p_nom_extendable",
            "p_min_pu",
            "carrier",
            "marginal_cost",
            "build_year",
            "lifetime",
            "capital_cost",
            "isp_technology_type",
            "isp_fuel_cost_mapping",
            "isp_vom_$/mwh_sent_out",
            "isp_heat_rate_gj/mwh",
            "isp_rez_id",
        ],
        new_entrant_column_order=[
            "name",
            "bus",
            "p_nom",
            # "p_nom_mod",
            "p_nom_extendable",
            # "p_nom_max",
            "p_min_pu",
            "carrier",
            "marginal_cost",
            "build_year",
            "lifetime",
            "capital_cost",
            "isp_technology_type",
            "isp_fuel_cost_mapping",
            "isp_vom_$/mwh_sent_out",
            "isp_heat_rate_gj/mwh",
            "isp_resource_type",
            "isp_rez_id",
        ],
    )

    return translated_column_orders
