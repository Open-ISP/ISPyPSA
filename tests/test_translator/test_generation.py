import pandas as pd

from ispypsa.translator.generation import _map_existing_generator_columns


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
