import pandas as pd

from ispypsa.translator.generation import (
    _GENERATOR_COLUMNS,
    _map_existing_generator_columns,
    _map_new_entrant_generator_columns,
    _translate_generators,
)


def test_map_existing_generator_columns(csv_str_to_df):
    generators = csv_str_to_df("""
        name,  technology,          fuel_type,      capacity
        BW01,  Steam Sub Critical,  Black Coal,     660.0
    """)

    result = _map_existing_generator_columns(generators)

    expected = csv_str_to_df("""
        name,  isp_technology_type,     carrier,    p_nom,  isp_name,  p_nom_extendable,  capital_cost,  marginal_cost
        BW01,  Steam Sub Critical,      Black Coal, 660.0,  BW01,      False,             0.0,           BW01
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_map_new_entrant_generator_columns(csv_str_to_df):
    generators = csv_str_to_df("""
        name,        technology,  fuel_type,  build_year,  lifetime_technical
        CNSW__OCGT,  OCGT,        Gas,        2026,        25.0
        CNSW__OCGT,  OCGT,        Gas,        2030,        25.0
    """)

    result = _map_new_entrant_generator_columns(generators)

    expected = csv_str_to_df("""
        name,             isp_technology_type,  carrier,  build_year,  lifetime,  isp_name,    p_nom,  p_nom_extendable,  marginal_cost
        CNSW__OCGT_2026,  OCGT,                 Gas,      2026,        25.0,      CNSW__OCGT,  0.0,    True,              CNSW__OCGT
        CNSW__OCGT_2030,  OCGT,                 Gas,      2030,        25.0,      CNSW__OCGT,  0.0,    True,              CNSW__OCGT
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_translate_generators(csv_str_to_df, sample_model_config):
    ispypsa_tables = {
        "generators_existing_planned": csv_str_to_df("""
            name,      technology,            fuel_type,    capacity,  geo_id,  commissioning_date,  closure_year,  emissions_intensity
            BW01,      Steam__Sub__Critical,  Black__Coal,  660.0,     CNSW,    ,                    2033,          0.9
            LIDDELL1,  Steam__Sub__Critical,  Black__Coal,  500.0,     CNSW,    ,                    2023,          0.9
            PLANNED1,  Wind,                  Wind,         100.0,     N3,      01/08/2029,          ,              0.0
        """),
        "generators_new_entrant": csv_str_to_df("""
            name,        technology,  geo_id,  fuel_type,  fom,      lcf_build,  lcf_om,  lifetime_technical,  lifetime_economic,  emissions_intensity
            CNSW__OCGT,  OCGT,        CNSW,    Gas,        20000.0,  110.0,      105.0,   25.0,                25.0,               0.6
            N3__Wind,    Wind,        N3,      Wind,       30000.0,  100.0,      100.0,   30.0,                30.0,               0.0
        """),
        "costs_new_entrant_build": csv_str_to_df("""
            technology,  year,  cost
            OCGT,        ,      1400000.0
            Wind,        2026,  3000000.0
            Wind,        2028,  2900000.0
        """),
        "costs_connection": csv_str_to_df("""
            geo_id,  technology,  year,  connection_cost,  system_strength_cost
            CNSW,    OCGT,        2026,  100000.0,         0.0
            CNSW,    OCGT,        2028,  100000.0,         0.0
            N3,      Wind,        2026,  150000.0,         60000.0
            N3,      Wind,        2028,  140000.0,         55000.0
        """),
        "network_geography": csv_str_to_df("""
            geo_id,  geo_type,   region_id,  subregion_id
            CNSW,    subregion,  NSW,        CNSW
            N3,      rez,        NSW,        CNSW
        """),
    }

    result = _translate_generators(ispypsa_tables, sample_model_config)

    # The helper tests cover the content. With investment periods 2026 and 2028,
    # LIDDELL1 closes before the first and PLANNED1 is built after the last
    # (FY2030), so BW01 is the only existing unit kept. The two new-entrant
    # options get one generator per investment period, and OCGT's blank-year
    # build cost covers both.
    assert list(result.columns) == _GENERATOR_COLUMNS
    assert len(result) == 5
