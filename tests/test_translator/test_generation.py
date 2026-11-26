import pandas as pd

from ispypsa.translator.generation import (
    _map_existing_generator_columns,
    _map_new_entrant_generator_columns,
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
