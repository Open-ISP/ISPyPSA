import pandas as pd

from ispypsa.templater.geography import (
    _extract_subregion_id,
    _template_network_geography,
)


def test_extract_subregion_id_with_comma_in_name():
    series = pd.Series(
        ["Sydney, Newcastle, Wollongong (SNW)", "Northern Queensland (NQ)"]
    )
    result = _extract_subregion_id(series)
    expected = pd.Series(["SNW", "NQ"])
    pd.testing.assert_series_equal(result, expected)


def test_template_network_geography(csv_str_to_df):
    sub_regional_reference_nodes = csv_str_to_df("""
        NEM region,  ISP sub-region,                        Sub-regional reference node
        Queensland,  Northern Queensland (NQ),              Ross 275 kV
        New South Wales,  Central New South Wales (CNSW),   Wellington 330 kV
    """)

    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,              NEM region,  ISP sub-region
        Q1,   Far North QLD,     QLD,         NQ
        N3,   Central-West Orana, NSW,         CNSW
    """)

    expected = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        NQ,      subregion,  QLD,        NQ
        CNSW,    subregion,  NSW,        CNSW
        Q1,      rez,        QLD,        NQ
        N3,      rez,        NSW,        CNSW
    """)

    result = _template_network_geography(
        sub_regional_reference_nodes, renewable_energy_zones
    )

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )
