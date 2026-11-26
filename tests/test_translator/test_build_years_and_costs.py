import pandas as pd

from ispypsa.translator.build_years_and_costs import (
    _build_years_from_commissioning_dates,
    _buses_for_geo_ids,
    _drop_units_built_after_last_investment_period,
    _drop_units_closed_before_first_investment_period,
    _lifetimes_from_closure_years,
)


def test_build_years_from_commissioning_dates():
    commissioning_dates = pd.Series(
        [
            "01/08/2025",  # FY 2026
            "01/07/2027",  # FY 2028
            "01/01/2020",  # before model start
            "06/07/2029",  # day-month format: July + 'fy' -> 2030
            "07/06/2029",  #                   June + 'fy' -> 2029
            float("nan"),  # blank -> start_year - 1 -> 2025
        ],
    )
    result = _build_years_from_commissioning_dates(commissioning_dates, "fy", 2026)

    expected = pd.Series([2026, 2028, 2025, 2030, 2029, 2025])
    pd.testing.assert_series_equal(result, expected)


def test_lifetimes_from_closure_years():
    closure_years = pd.Series(
        [
            2033.0,  # built before the model starts, runs through 2033
            2040.0,  # commissioned in 2028
            float("nan"),  # blank: not retired during the horizon
        ]
    )
    build_years = pd.Series([2025, 2028, 2025])

    result = _lifetimes_from_closure_years(closure_years, build_years)

    expected = pd.Series([9.0, 13.0, float("nan")])
    pd.testing.assert_series_equal(result, expected)


def test_drop_units_closed_before_first_investment_period(csv_str_to_df, caplog):
    units = csv_str_to_df("""
        name,      closure_year
        TORRB1,    2026
        LIDDELL1,  2023
        LONGGEN,   2050
        NEWGEN1,
    """)

    with caplog.at_level("INFO"):
        result = _drop_units_closed_before_first_investment_period(
            units, 2026, "generators_existing_planned"
        )

    # TORRB1 runs through 2026, LONGGEN closes long after the first investment period,
    # and NEWGEN1 has no closure year, so all three are kept. LIDDELL1 drops.
    expected = csv_str_to_df("""
        name,     closure_year
        TORRB1,   2026
        LONGGEN,  2050
        NEWGEN1,
    """)
    pd.testing.assert_frame_equal(result, expected)
    assert (
        "Units in generators_existing_planned dropped because they close before "
        "the first investment period (2026): ['LIDDELL1']"
    ) in caplog.text


def test_drop_units_closed_before_first_investment_period_no_drops(
    csv_str_to_df, caplog
):
    units = csv_str_to_df("""
        name,    closure_year
        BW01,    2033
        TORRB1,  2026
    """)

    with caplog.at_level("INFO"):
        result = _drop_units_closed_before_first_investment_period(
            units, 2026, "generators_existing_planned"
        )

    pd.testing.assert_frame_equal(result, units)
    assert "dropped because they close before" not in caplog.text


def test_drop_units_built_after_last_investment_period(csv_str_to_df, caplog):
    units = csv_str_to_df("""
        name,      build_year
        BW01,      2025
        PLANNED1,  2030
        PLANNED2,  2031
    """)

    with caplog.at_level("INFO"):
        result = _drop_units_built_after_last_investment_period(
            units, 2030, "generators_existing_planned"
        )

    # PLANNED1 is built in the last investment period, so it is kept.
    expected = csv_str_to_df("""
        name,      build_year
        BW01,      2025
        PLANNED1,  2030
    """)
    pd.testing.assert_frame_equal(result, expected)
    assert (
        "Units in generators_existing_planned dropped because they are built after "
        "the last investment period (2030): ['PLANNED2']"
    ) in caplog.text


def test_drop_units_built_after_last_investment_period_no_drops(csv_str_to_df, caplog):
    units = csv_str_to_df("""
        name,      build_year
        BW01,      2025
        PLANNED1,  2030
    """)

    with caplog.at_level("INFO"):
        result = _drop_units_built_after_last_investment_period(
            units, 2030, "generators_existing_planned"
        )

    pd.testing.assert_frame_equal(result, units)
    assert "dropped because they are built after" not in caplog.text


def test_buses_for_geo_ids_discrete_nodes(csv_str_to_df):
    geo_ids = pd.Series(["CNSW", "N3"])
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        N3,      rez,        NSW,        CNSW
    """)

    result = _buses_for_geo_ids(
        geo_ids, network_geography, "discrete_nodes", "sub_regions"
    )

    expected = pd.Series(["CNSW", "N3"])
    pd.testing.assert_series_equal(result, expected)


def test_buses_for_geo_ids_attached_sub_regions(csv_str_to_df):
    geo_ids = pd.Series(["CNSW", "N3"])
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        N3,      rez,        NSW,        CNSW
    """)

    result = _buses_for_geo_ids(
        geo_ids, network_geography, "attached_to_parent_node", "sub_regions"
    )

    # N3's parent sub-region is CNSW (its subregion_id), not its region NSW.
    expected = pd.Series(["CNSW", "CNSW"])
    pd.testing.assert_series_equal(result, expected)


def test_buses_for_geo_ids_attached_nem_regions(csv_str_to_df):
    # The templater has already mapped sub-region geo_ids to their region.
    geo_ids = pd.Series(["NSW", "N3"])
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,  region_id
        NSW,     region,    NSW
        N3,      rez,       NSW
    """)

    result = _buses_for_geo_ids(
        geo_ids, network_geography, "attached_to_parent_node", "nem_regions"
    )

    expected = pd.Series(["NSW", "NSW"])
    pd.testing.assert_series_equal(result, expected)


def test_buses_for_geo_ids_attached_single_region(csv_str_to_df):
    geo_ids = pd.Series(["NEM", "N3"])
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,  region_id
        NEM,     region,    NEM
        N3,      rez,       NEM
    """)

    result = _buses_for_geo_ids(
        geo_ids, network_geography, "attached_to_parent_node", "single_region"
    )

    expected = pd.Series(["NEM", "NEM"])
    pd.testing.assert_series_equal(result, expected)
