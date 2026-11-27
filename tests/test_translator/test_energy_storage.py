import pandas as pd

from ispypsa.translator.energy_storage import (
    _add_efficiencies_and_cyclic_state_of_charge,
    _map_existing_storage_columns,
    _translate_storage,
)


def test_map_existing_storage_columns(csv_str_to_df):
    storage = csv_str_to_df("""
        name,      power_station,          technology,                      fuel_type,  capacity,  storage_capacity
        VBB1,      Victorian Big Battery,  Battery storage (2hrs storage),  Battery,    300.0,     470.0
        DALNTH1,   Dalrymple BESS,         Battery storage (1hr storage),   Battery,    30.0,      9.0
        SNO2PH01,  Snowy 2.0,              Pumped Hydro (1wk storage),      Water,      366.0,     58330.0
    """)

    result = _map_existing_storage_columns(storage)

    # max_hours is storage_capacity / capacity (MWh / MW).
    expected = csv_str_to_df("""
        name,      isp_name,               isp_technology_type,             carrier,  p_nom,  storage_capacity,  max_hours
        VBB1,      Victorian Big Battery,  Battery storage (2hrs storage),  Battery,  300.0,  470.0,             1.566667
        DALNTH1,   Dalrymple BESS,         Battery storage (1hr storage),   Battery,  30.0,   9.0,               0.3
        SNO2PH01,  Snowy 2.0,              Pumped Hydro (1wk storage),      Water,    366.0,  58330.0,           159.371585
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False)


def test_add_efficiencies_and_cyclic_state_of_charge(csv_str_to_df):
    storage = csv_str_to_df("""
        name,      efficiency_charge,  efficiency_discharge
        BATTERY1,  95.0,               90.0
        SNO2PH01,  87.178,             87.178
    """)

    result = _add_efficiencies_and_cyclic_state_of_charge(storage)

    expected = csv_str_to_df("""
        name,      efficiency_charge,  efficiency_discharge,  efficiency_store,  efficiency_dispatch,  cyclic_state_of_charge
        BATTERY1,  95.0,               90.0,                  0.95,              0.90,                 True
        SNO2PH01,  87.178,             87.178,                0.87178,           0.87178,              True
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_translate_storage(csv_str_to_df, sample_model_config):
    ispypsa_tables = {
        "storage_existing_planned": csv_str_to_df("""
            name,      power_station,          technology,  fuel_type,  capacity,  storage_capacity,  efficiency_charge,  efficiency_discharge,  geo_id,  commissioning_date,  closure_year
            VBB1,      Victorian Big Battery,  Battery 2h,  Battery,    300.0,     470.0,             92.0,               92.0,                  MEL,     ,                    2041
            OLDBESS1,  Old BESS,               Battery 2h,  Battery,    30.0,      60.0,              92.0,               92.0,                  CNSW,    01/07/2015,          2024
        """),
        "storage_new_entrant": csv_str_to_df("""
            name,             technology,  geo_id,  fuel_type,  storage_hours,  fom,      efficiency_charge,  efficiency_discharge,  lcf_build,  lcf_om,  lifetime_technical,  lifetime_economic
            CNSW Battery 2h,  Battery 2h,  CNSW,    Battery,    2.0,            10000.0,  92.0,               92.0,                  100.0,      100.0,   20.0,                20.0
        """),
        "costs_new_entrant_build": csv_str_to_df("""
            technology,  year,  cost
            Battery 2h,  2026,  1200000.0
            Battery 2h,  ,      1000000.0
        """),
        "costs_connection": csv_str_to_df("""
            geo_id,  technology,  year,  connection_cost,  system_strength_cost
            CNSW,    Battery 2h,  2026,  50000.0,          0.0
            CNSW,    Battery 2h,  2028,  50000.0,          0.0
        """),
        "network_geography": csv_str_to_df("""
            geo_id,  geo_type,   region_id,  subregion_id
            MEL,     subregion,  VIC,        MEL
            CNSW,    subregion,  NSW,        CNSW
        """),
    }

    result = _translate_storage(ispypsa_tables, sample_model_config)

    # The sample config starts in FY2026, with investment periods 2026 and 2028,
    # discrete_nodes and a wacc of 6%. OLDBESS1 closes in 2024, before the first
    # investment period, so it is dropped. VBB1 is already built (build year
    # 2025) and runs through 2041 (lifetime 17), with max_hours 470 / 300. The
    # new-entrant option gets one unit per investment period, with max_hours
    # from storage_hours and the blank-year build cost for 2028. Its
    # capital_cost is the annuity over 20 years of build_cost + 50000, plus
    # fom: 1250000 gives 118980.70, and 1050000 gives 101543.78.
    expected = csv_str_to_df("""
        isp_name,               name,                  carrier,  bus,   p_nom,  p_nom_extendable,  max_hours,  build_year,  lifetime,  capital_cost,  efficiency_store,  efficiency_dispatch,  cyclic_state_of_charge,  isp_technology_type
        Victorian Big Battery,  VBB1,                  Battery,  MEL,   300.0,  False,             1.566667,   2025,        17.0,      0.0,           0.92,              0.92,                 True,                    Battery 2h
        CNSW Battery 2h,        CNSW Battery 2h_2026,  Battery,  CNSW,  0.0,    True,              2.0,        2026,        20.0,      118980.70,     0.92,              0.92,                 True,                    Battery 2h
        CNSW Battery 2h,        CNSW Battery 2h_2028,  Battery,  CNSW,  0.0,    True,              2.0,        2028,        20.0,      101543.78,     0.92,              0.92,                 True,                    Battery 2h
    """)
    pd.testing.assert_frame_equal(result, expected, rtol=1e-6)
