import re

import pandas as pd
import pytest

from ispypsa.templater.geography import (
    _add_non_rez_geographies_to_rez_table,
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


def test_template_network_geography_sub_regions(csv_str_to_df):
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

    result = _template_network_geography(
        sub_regional_reference_nodes, renewable_energy_zones, "sub_regions"
    )

    expected = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        NQ,      subregion,  QLD,        NQ
        CNSW,    subregion,  NSW,        CNSW
        Q1,      rez,        QLD,        NQ
        N3,      rez,        NSW,        CNSW
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_template_network_geography_nem_regions(csv_str_to_df):
    sub_regional_reference_nodes = csv_str_to_df("""
        NEM region,  ISP sub-region,                        Sub-regional reference node
        Queensland,  Northern Queensland (NQ),              Ross 275 kV
        Queensland,  Central Queensland (CQ),               Stanwell 275 kV
        New South Wales,  Central New South Wales (CNSW),   Wellington 330 kV
    """)

    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,              NEM region,  ISP sub-region
        Q1,   Far North QLD,     QLD,         NQ
        N3,   Central-West Orana, NSW,         CNSW
    """)

    result = _template_network_geography(
        sub_regional_reference_nodes, renewable_energy_zones, "nem_regions"
    )

    expected = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        QLD,     region,     QLD
        NSW,     region,     NSW
        Q1,      rez,        QLD
        N3,      rez,        NSW
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_template_network_geography_single_region(csv_str_to_df):
    sub_regional_reference_nodes = csv_str_to_df("""
        NEM region,  ISP sub-region,            Sub-regional reference node
        Queensland,  Northern Queensland (NQ),  Ross 275 kV
    """)

    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,              NEM region,  ISP sub-region
        Q1,   Far North QLD,     QLD,         NQ
        N3,   Central-West Orana, NSW,         CNSW
    """)

    result = _template_network_geography(
        sub_regional_reference_nodes, renewable_energy_zones, "single_region"
    )

    expected = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        NEM,     region,     NEM
        Q1,      rez,        NEM
        N3,      rez,        NEM
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


# --- _add_non_rez_geographies_to_rez_table ---


def test_add_non_rez_geographies_to_rez_table(csv_str_to_df, caplog):
    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,                 NEM region,  ISP sub-region
        Q1,   Far North QLD,        QLD,         NQ
        N3,   Central-West Orana,   NSW,         CNSW
    """)

    iasr_tables = {
        "renewable_energy_zones": renewable_energy_zones,
        "another_table": csv_str_to_df("""
            First Column,   Second Column
            number_one,     number_two
        """),
    }

    with caplog.at_level("INFO"):
        updated_tables = _add_non_rez_geographies_to_rez_table(iasr_tables)

    # check that returned renewable_energy_zones table has extra rows
    expected_rez = csv_str_to_df("""
        ID,  Name,                      NEM region,  ISP sub-region
        Q1,  Far North QLD,             QLD,         NQ
        N3,  Central-West Orana,        NSW,         CNSW
        N0,  New South Wales Non-REZ,   NSW,         CNSW
        V0,  Victoria Non-REZ,          VIC,         SEV
    """)
    pd.testing.assert_frame_equal(
        updated_tables["renewable_energy_zones"], expected_rez
    )

    # and that the additions were logged:
    msg = (
        "IASR TABLE EDIT: Added non-REZ geographies to 'renewable_energy_zones' "
        "IASR table: ['N0', 'V0']"
    )
    assert msg in caplog.text

    # and that the other table passed through untouched
    assert updated_tables["another_table"] is iasr_tables["another_table"]
    assert iasr_tables["renewable_energy_zones"] is renewable_energy_zones


def test_add_non_rez_geographies_to_rez_table_guard_raises(csv_str_to_df):
    # check that the guard raises correctly based on ID presence, even if other
    # column values for that non-REZ row are different.
    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,                 NEM region, ISP sub-region
        N3,   Central-West Orana,   NSW,        CNSW
        N0,   New Non-REZ Name,     NSW,        SNSW
    """)

    iasr_tables = {
        "renewable_energy_zones": renewable_energy_zones,
    }
    expected_msg = re.escape("Non-REZ ID(s) ['N0'] are already defined")
    with pytest.raises(ValueError, match=expected_msg):
        _add_non_rez_geographies_to_rez_table(iasr_tables)
