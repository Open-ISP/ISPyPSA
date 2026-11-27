import re

import pandas as pd
import pytest

from ispypsa.translator.build_years_and_costs import (
    _annuitised_capital_costs,
    _build_existing_units,
    _build_new_entrant_units,
    _build_years_from_commissioning_dates,
    _buses_for_geo_ids,
    _drop_units_built_after_last_investment_period,
    _drop_units_closed_before_first_investment_period,
    _lifetimes_from_closure_years,
    _merge_build_costs,
    _merge_connection_costs,
    _raise_on_missing_build_costs,
    _raise_on_missing_connection_costs,
    _resolve_build_costs,
)


def test_build_existing_units(csv_str_to_df, sample_model_config):
    units = csv_str_to_df("""
        name,      power_station,  capacity,  geo_id,  commissioning_date,  closure_year
        BW01,      Bayswater,      660.0,     CNSW,    ,                    2033
        LIDDELL1,  Liddell,        500.0,     CNSW,    ,                    2023
        PLANNED1,  Wind Farm A,    100.0,     N3,      01/08/2026,
        PLANNED2,  Wind Farm B,    200.0,     N3,      01/08/2028,
    """)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        N3,      rez,        NSW,        CNSW
    """)

    result = _build_existing_units(
        units, network_geography, sample_model_config, "generators_existing_planned"
    )

    # The sample config starts in FY2026, with investment periods 2026 and 2028.
    # LIDDELL1 closes before 2026, and PLANNED2 is built in FY2029, after 2028,
    # so both are dropped. With discrete_nodes, each bus is the unit's geo_id.
    expected = csv_str_to_df("""
        name,      power_station,  capacity,  geo_id,  commissioning_date,  closure_year,  build_year,  lifetime,  bus,   p_nom_extendable,  capital_cost
        BW01,      Bayswater,      660.0,     CNSW,    ,                    2033,          2025,        9.0,       CNSW,  False,             0.0
        PLANNED1,  Wind Farm A,    100.0,     N3,      01/08/2026,          ,              2027,        ,          N3,    False,             0.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_build_new_entrant_units(csv_str_to_df, sample_model_config):
    units = csv_str_to_df("""
        name,       technology,  geo_id,  fom,      lcf_build,  lcf_om,  lifetime_technical,  lifetime_economic
        CNSW OCGT,  OCGT,        CNSW,    20000.0,  110.0,      105.0,   30.0,                25.0
    """)
    build_costs = csv_str_to_df("""
        technology,  year,  cost
        OCGT,        2026,  1000000.0
        OCGT,        ,      900000.0
    """)
    connection_costs = csv_str_to_df("""
        geo_id,  technology,  year,  connection_cost,  system_strength_cost
        CNSW,    OCGT,        2026,  100000.0,         50000.0
        CNSW,    OCGT,        2028,  100000.0,         50000.0
    """)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
    """)

    result = _build_new_entrant_units(
        units, build_costs, connection_costs, network_geography, sample_model_config
    )

    # One unit per investment period (2026 and 2028 in the sample config). The
    # blank-year build cost (900000) is used for 2028. capital_cost is the
    # annuity at the sample config's wacc of 6% over the 25-year economic
    # lifetime of (build_cost + connection_cost) * 1.10 + system_strength_cost,
    # plus fom * 1.05: 1260000 gives 119565.66, and 1150000 gives 110960.73.
    expected = csv_str_to_df("""
        name,            technology,  geo_id,  fom,      lcf_build,  lcf_om,  lifetime_technical,  lifetime_economic,  build_year,  build_cost,  connection_cost,  system_strength_cost,  capital_cost,  bus,   isp_name,   p_nom,  p_nom_extendable
        CNSW OCGT_2026,  OCGT,        CNSW,    20000.0,  110.0,      105.0,   30.0,                25.0,               2026,        1000000.0,   100000.0,         50000.0,               119565.66,     CNSW,  CNSW OCGT,  0.0,    True
        CNSW OCGT_2028,  OCGT,        CNSW,    20000.0,  110.0,      105.0,   30.0,                25.0,               2028,        900000.0,    100000.0,         50000.0,               110960.73,     CNSW,  CNSW OCGT,  0.0,    True
    """)
    pd.testing.assert_frame_equal(result, expected, rtol=1e-6)


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


def test_resolve_build_costs(csv_str_to_df):
    build_costs = csv_str_to_df("""
        technology,  year,  cost
        OCGT,        2026,  1400000.0
        OCGT,        ,      1300000.0
        OCGT,        2027,  1350000.0
        CCGT,        ,      2500000.0
    """)

    result = _resolve_build_costs(build_costs, [2026, 2030])

    # OCGT's 2026 row takes priority over its blank-year row, which fills 2030.
    # The 2027 row isn't an investment period, so it's filtered out. CCGT's
    # blank-year row fills both investment periods.
    expected = csv_str_to_df("""
        technology,  year,  cost
        OCGT,        2026,  1400000.0
        OCGT,        2030,  1300000.0
        CCGT,        2026,  2500000.0
        CCGT,        2030,  2500000.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_resolve_build_costs_no_blank_years(csv_str_to_df):
    build_costs = csv_str_to_df("""
        technology,  year,  cost
        OCGT,        2025,  1450000.0
        OCGT,        2026,  1400000.0
        OCGT,        2030,  1300000.0
    """)

    result = _resolve_build_costs(build_costs, [2026, 2030])

    expected = csv_str_to_df("""
        technology,  year,  cost
        OCGT,        2026,  1400000.0
        OCGT,        2030,  1300000.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_merge_build_costs(csv_str_to_df):
    units = csv_str_to_df("""
        name,        technology,  build_year
        CNSW__OCGT,  OCGT,        2026
        CNSW__OCGT,  OCGT,        2030
        NQ__CCGT,    CCGT,        2026
    """)
    build_costs = csv_str_to_df("""
        technology,  year,  cost
        OCGT,        2026,  1400000.0
        OCGT,        2030,  1300000.0
        CCGT,        2030,  2500000.0
    """)

    result = _merge_build_costs(units, build_costs)

    # There is no CCGT row for 2026, so NQ CCGT gets a NaN build_cost.
    expected = csv_str_to_df("""
        name,        technology,  build_year,  build_cost
        CNSW__OCGT,  OCGT,        2026,        1400000.0
        CNSW__OCGT,  OCGT,        2030,        1300000.0
        NQ__CCGT,    CCGT,        2026,
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_merge_connection_costs(csv_str_to_df):
    units = csv_str_to_df("""
        name,        geo_id,  technology,     build_year
        N1__Wind,    N1,      Wind,           2026
        N10__WOF,    N10,     Wind__offshore, 2026
        CNSW__OCGT,  CNSW,    OCGT,           2026
    """)
    connection_costs = csv_str_to_df("""
        geo_id,  technology,      year,  connection_cost,  system_strength_cost
        N1,      Wind,            2026,  150000.0,         60000.0
        N1,      Wind,            2030,  140000.0,         55000.0
        N10,     Wind__offshore,  2026,  ,                 160000.0
    """)

    result = _merge_connection_costs(units, connection_costs)

    # N10's blank connection_cost becomes 0, and CNSW OCGT has no
    # costs_connection row, so both its costs are NaN.
    expected = csv_str_to_df("""
        name,        geo_id,  technology,      build_year,  connection_cost,  system_strength_cost
        N1__Wind,    N1,      Wind,            2026,        150000.0,         60000.0
        N10__WOF,    N10,     Wind__offshore,  2026,        0.0,              160000.0
        CNSW__OCGT,  CNSW,    OCGT,            2026,        ,
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_raise_on_missing_build_costs(csv_str_to_df):
    # The two CCGT rows for 2026 are options in different places, which share
    # one missing build cost, so the pair is named once.
    units = csv_str_to_df("""
        technology,  build_year,  build_cost
        OCGT,        2026,        1400000.0
        CCGT,        2026,
        CCGT,        2026,
        CCGT,        2030,
    """)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "New entrants with no build cost (technology, build_year): "
            "[('CCGT', 2026), ('CCGT', 2030)]"
        ),
    ):
        _raise_on_missing_build_costs(units)


def test_raise_on_missing_connection_costs(csv_str_to_df):
    units = csv_str_to_df("""
        geo_id,  technology,  build_year,  connection_cost
        N1,      Wind,        2026,        150000.0
        CNSW,    OCGT,        2026,
        CNSW,    OCGT,        2030,
    """)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "New entrants with no connection cost (geo_id, technology, build_year): "
            "[('CNSW', 'OCGT', 2026), ('CNSW', 'OCGT', 2030)]"
        ),
    ):
        _raise_on_missing_connection_costs(units)


def test_annuitised_capital_costs(csv_str_to_df):
    units = csv_str_to_df("""
        build_cost,  connection_cost,  system_strength_cost,  lcf_build,  fom,      lcf_om,  lifetime_economic,  lifetime_technical
        1000000.0,   100000.0,         50000.0,               110.0,      20000.0,  105.0,   25.0,               30.0
        1000000.0,   0.0,              0.0,                   ,           20000.0,  ,        ,                   30.0
    """)

    result = _annuitised_capital_costs(units, 0.07)

    # First row: (1000000 + 100000) x 1.1 + 50000 (not scaled) = 1260000,
    # annuitised over 25 years = 108121.25, plus fom 20000 x 1.05 = 21000.
    # Second row: the blank LCFs mean no scaling, and the blank lifetime_economic
    # falls back to the 30-year lifetime_technical. 1000000 annuitised over
    # 30 years = 80586.40, plus fom 20000.
    expected = pd.Series([129121.25, 100586.40])
    pd.testing.assert_series_equal(result, expected, rtol=1e-6)
