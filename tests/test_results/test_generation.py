from unittest.mock import Mock

import pandas as pd
import pytest

from ispypsa.results.generation import (
    extract_demand,
    extract_generation_expansion_results,
    extract_generator_dispatch,
)


def test_extract_generator_dispatch(csv_str_to_df):
    """Test extraction of generator dispatch results."""

    # 1. Setup Generator Data
    # - gen1: standard generator
    # - gen2: another generator
    # - custom_gen: should be filtered out
    generators_csv = """
    name,         bus,                             carrier,  isp_technology_type
    gen1,         NSW1,                            gas,      Gas - CCGT
    gen2,         VIC1,                            wind,     Wind - Onshore
    custom_gen,   bus_for_custom_constraint_gens,  dummy,    Dummy
    """
    generators = csv_str_to_df(generators_csv).set_index("name")

    # 2. Setup Dispatch Data (generators_t.p)
    # Columns match generator names, index is time/period (period, timestep)

    dispatch_data_csv = """
    period,  timestep,            gen1,  gen2,  custom_gen
    2030,    2030-01-01 00:00:00, 100,   50,    10
    2030,    2030-01-01 01:00:00, 110,   60,    10
    2040,    2040-01-01 00:00:00, 120,   70,    10
    """
    dispatch_df = csv_str_to_df(dispatch_data_csv)
    # Set multi-index to match PyPSA structure
    dispatch_t = dispatch_df.set_index(["period", "timestep"])

    # 3. Mock Network
    network = Mock()
    network.generators = generators
    network.generators_t.p = dispatch_t

    # 4. Expected Output
    # - custom_gen should be removed
    # - columns renamed and reordered (carrier -> fuel_type, isp_technology_type -> technology_type)
    expected_csv = """
    generator, node, fuel_type, technology_type, investment_period, timestep,             dispatch_mw
    gen1,      NSW1, gas,       Gas - CCGT,      2030,              2030-01-01 00:00:00,  100
    gen1,      NSW1, gas,       Gas - CCGT,      2030,              2030-01-01 01:00:00,  110
    gen1,      NSW1, gas,       Gas - CCGT,      2040,              2040-01-01 00:00:00,  120
    gen2,      VIC1, wind,      Wind - Onshore,  2030,              2030-01-01 00:00:00,  50
    gen2,      VIC1, wind,      Wind - Onshore,  2030,              2030-01-01 01:00:00,  60
    gen2,      VIC1, wind,      Wind - Onshore,  2040,              2040-01-01 00:00:00,  70
    """
    expected_df = csv_str_to_df(expected_csv)

    # 5. Run Function
    result = extract_generator_dispatch(network)

    # Sort both for consistent comparison
    sort_cols = ["generator", "investment_period", "timestep"]
    result = result.sort_values(sort_cols).reset_index(drop=True)
    expected_df = expected_df.sort_values(sort_cols).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df)


def test_extract_demand(csv_str_to_df):
    """Test extraction of demand/load results."""

    # 1. Setup Load Data
    # - load1: load in NSW
    # - load2: load in VIC
    loads_csv = """
    name,   bus
    load1,  NSW1
    load2,  VIC1
    """
    loads = csv_str_to_df(loads_csv).set_index("name")

    # 2. Setup Demand Data (loads_t.p_set)
    # Columns match load names, index is time/period (period, timestep)
    demand_data_csv = """
    period,  timestep,            load1,  load2
    2030,    2030-01-01 00:00:00, 200,    150
    2030,    2030-01-01 01:00:00, 210,    160
    2040,    2040-01-01 00:00:00, 220,    170
    """
    demand_df = csv_str_to_df(demand_data_csv)
    # Set multi-index to match PyPSA structure
    demand_t = demand_df.set_index(["period", "timestep"])

    # 3. Mock Network
    network = Mock()
    network.loads = loads
    network.loads_t.p_set = demand_t

    # 4. Expected Output
    # - columns renamed and reordered
    expected_csv = """
    node,  investment_period, timestep,             demand_mw
    NSW1,  2030,              2030-01-01 00:00:00,  200
    NSW1,  2030,              2030-01-01 01:00:00,  210
    NSW1,  2040,              2040-01-01 00:00:00,  220
    VIC1,  2030,              2030-01-01 00:00:00,  150
    VIC1,  2030,              2030-01-01 01:00:00,  160
    VIC1,  2040,              2040-01-01 00:00:00,  170
    """
    expected_df = csv_str_to_df(expected_csv)

    # 5. Run Function
    result = extract_demand(network)

    # Sort both for consistent comparison
    sort_cols = ["node", "investment_period", "timestep"]
    result = result.sort_values(sort_cols).reset_index(drop=True)
    expected_df = expected_df.sort_values(sort_cols).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df)


def test_extract_generation_expansion_results(csv_str_to_df):
    """Test extraction of generation expansion results."""

    # 1. Setup Generator Data
    # - gen1: existing generator (build_year 2025, lifetime 10 -> closure 2035)
    # - gen2: new generator (build_year 2030, lifetime 25 -> closure 2055)
    # - custom_gen: should be filtered out (bus_for_custom_constraint_gens)
    generators_csv = """
    Generator,    bus,                             carrier,  isp_technology_type,  build_year,  lifetime,  p_nom_opt
    gen1,         NSW1,                            Gas,      Gas - CCGT,           2025,        10,        500.0
    gen2,         VIC1,                            Solar,    Solar - Fixed,        2030,        25,        200.0
    gen3,         NSW1,                            Wind,     Wind - Onshore,       2025,        30,        100.0
    custom_gen,   bus_for_custom_constraint_gens,  dummy,    Dummy,                2025,        10,        0.0
    """
    generators = csv_str_to_df(generators_csv).set_index("Generator")

    # 2. Mock Network
    network = Mock()
    network.generators = generators

    # 3. Expected Output
    # - custom_gen should be removed
    # - closure_year = build_year + lifetime
    # - isp_technology_type renamed to technology_type
    expected_csv = """
    generator,  fuel_type,  technology_type,  node,  capacity_mw,  investment_period,  closure_year
    gen1,       Gas,        Gas - CCGT,       NSW1,  500.0,        2025,               2035
    gen2,       Solar,      Solar - Fixed,    VIC1,  200.0,        2030,               2055
    gen3,       Wind,       Wind - Onshore,   NSW1,  100.0,        2025,               2055
    """
    expected_df = csv_str_to_df(expected_csv)

    # 4. Run Function
    result = extract_generation_expansion_results(network)

    # Sort both for consistent comparison
    sort_cols = ["generator"]
    result = result.sort_values(sort_cols).reset_index(drop=True)
    expected_df = expected_df.sort_values(sort_cols).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected_df)
